"""Collect per-stock investor supply/demand (개인·외국인·기관 순매수) into SQLite.

Source: ``ka10059`` (투자자기관별종목별요청) — one request per stock returns up to
100 daily rows. Because this is a single TR, the kiwoom-client per-TR rate
limiter throttles to ~1 req/s; a full KOSPI+KOSDAQ common-stock sweep
(~2,600 stocks) takes roughly 45 minutes for one page each.

Multi-page backfill (⚠️ important, discovered 2026-07-09): ka10059 supports
continuation well beyond 100 days — Kiwoom returns ``cont-yn``/``next-key`` in
the **HTTP response headers**, not the JSON body. ``kiwoom_rest_api``'s
``BaseClient.request()`` returns only ``resp.json()`` and silently drops
response headers, so every collector in this repo that checked
``resp.get("cont_yn")`` (this file previously did not even try; ``daily_bars.py``
does, but its check is a no-op against this library version for the same
reason) never actually continued past page 1 — not because the API is capped
at ~100 days, but because the header was never read. Manually reading
``httpx.Response.headers`` for a real stock confirmed 6 pages reaches
2024-01-19 with more still available (``cont-yn: Y``). ``--max-pages`` here
uses :func:`_fetch_investor_flow_pages`, which talks to the underlying HTTP
client directly (bypassing the header-dropping wrapper) to unlock this.

CLI:
    kq-collect --market all --days 30                 # mock server, 1 page
    kq-collect --market all --days 30 --prod           # real data, 1 page
    kq-collect --resume                                # skip already-collected
    kq-collect --prod --max-pages 30 --days 0          # deep backfill (~years)
"""

from __future__ import annotations

import argparse
import sqlite3
import time

from typing import Any

from kiwoom_rest_api import KiwoomAPI
from kiwoom_rest_api.base import KiwoomAPIError

from .kiwoom_cli import add_common_args, build_universe, open_session, print_banner
from .storage import (
    INVESTOR_COLUMNS,
    days_ago,
    fetchone,
    progress_line,
    to_float,
    to_int,
    upsert_supply_demand,
)

# ka10099 (종목정보 리스트) market-type codes.
MARKETS: dict[str, str] = {"kospi": "0", "kosdaq": "10"}


def is_common_stock(row: dict) -> bool:
    """True for common shares only.

    Excludes ETF/ETN/REITs (``market`` is not 거래소/코스닥) and preferred
    shares (KRX common-stock codes end in ``0``; preferred end in 5/7/K/...).
    """
    return row["market"] in ("거래소", "코스닥") and row["code"].endswith("0")


def fetch_stock_list(api: KiwoomAPI, markets: list[str]) -> list[dict]:
    """Fetch and normalize the stock master for the given markets."""
    out: list[dict] = []
    for market in markets:
        resp = api.stock_info.stock_info_list(mrkt_tp=MARKETS[market])
        for row in resp.get("list", []):
            out.append(
                {
                    "code": row.get("code", "").strip(),
                    "name": row.get("name", "").strip(),
                    "market": row.get("marketName", "").strip(),
                    "sector": row.get("upName", "").strip(),
                    "kind": row.get("kind", "").strip(),
                }
            )
    return out


def _has_recent_rows(con: Any, code: str, cutoff: str) -> bool:
    return fetchone(
        con, "SELECT 1 FROM supply_demand WHERE code=? AND date>=? LIMIT 1", (code, cutoff)
    ) is not None


def build_sd_records(code: str, resp: dict, cutoff: str) -> list[tuple]:
    """Map a ka10059 response into supply_demand rows newer than ``cutoff``."""
    records: list[tuple] = []
    for row in resp.get("stk_invsr_orgn", []) or []:
        date = row.get("dt", "")
        if date < cutoff:
            continue
        records.append(
            (
                code,
                date,
                # cur_prc 의 부호는 전일대비 등락 방향이므로 절댓값(가격)으로 저장.
                abs(to_int(row.get("cur_prc"))),
                to_float(row.get("flu_rt")),
                to_int(row.get("acc_trde_qty")),
                *[to_int(row.get(src)) for src in INVESTOR_COLUMNS.values()],
            )
        )
    return records


def _fetch_investor_flow_pages(
    api: KiwoomAPI,
    code: str,
    dt: str,
    *,
    max_pages: int = 1,
    stop_at: str = "",
    page_sleep_s: float = 1.1,
) -> list[dict]:
    """Fetch up to ``max_pages`` of ka10059 rows for ``code``, following real
    continuation via HTTP response headers (see module docstring for why the
    normal ``api.stock_info.investor_institution_by_stock()`` wrapper can't
    do this — it never sees ``cont-yn``/``next-key``, which Kiwoom returns as
    response headers, not JSON body fields).

    Args:
        api: Authenticated ``KiwoomAPI`` instance.
        code: Stock code.
        dt: Anchor date (``YYYYMMDD``) for the first page, same as the
            existing single-page call.
        max_pages: Upper bound on pages to fetch (each page is ~100 days
            older than the last). ``1`` reproduces the previous behavior.
        stop_at: If given (``YYYYMMDD``), stop once a page's oldest row is
            older than this date — lets a resumed backfill stop once it
            reaches data it already has, instead of always re-walking to
            ``max_pages``.
        page_sleep_s: Delay between pages (the shared per-TR rate limiter
            already throttles the underlying HTTP client to ~1 req/s via
            ``_rate_limiter.acquire`` below, but the explicit sleep keeps
            this multi-page loop from bursting ahead of the collector's
            outer per-stock rate limit).

    Returns:
        Concatenated ``stk_invsr_orgn`` rows across all fetched pages
        (newest first, per Kiwoom's own ordering — duplicates across page
        boundaries are not expected but harmless since storage upserts on
        the natural key).
    """
    base = api.stock_info._client
    resource_url = api.stock_info.RESOURCE_URL
    cont_yn, next_key = "N", ""
    all_rows: list[dict] = []

    for page in range(max_pages):
        # ``_build_headers(api_id, token, cont_yn, next_key, extra_headers)``.
        # 2026-08-15 까지 이 호출이 옛 인자 순서(api_id, cont_yn, next_key, extra)
        # 그대로여서 token 자리에 "N" 이 들어가고 next_key=None 이 헤더 값이 되어
        # ``AttributeError: 'NoneType' object has no attribute 'encode'`` 로 죽었다.
        # 아무도 못 본 이유는 **이 경로가 도달 불가능**했기 때문이다 — 일간 수급은
        # ``collectors.combined`` 가 받고 어떤 DAG 도 이 모듈을 부르지 않는다.
        #
        # 토큰은 매 페이지 ``_current_token()`` 으로 새로 받는다. 다중 페이지 백필은
        # 오래 도는데 그 사이 토큰이 갱신되면 캐시해 둔 값은 만료된다.
        headers = base._build_headers("ka10059", base._current_token(), cont_yn, next_key)
        for attempt in range(base._max_retries + 1):
            if base._rate_limiter is not None:
                base._rate_limiter.acquire("ka10059")
            http_resp = base._client.post(
                resource_url,
                headers=headers,
                json={
                    "dt": dt,
                    "stk_cd": code,
                    "amt_qty_tp": "2",
                    "trde_tp": "0",
                    "unit_tp": "1",
                },
            )
            if http_resp.status_code == 429 and attempt < base._max_retries:
                time.sleep(base._retry_backoff * (attempt + 1))
                continue
            http_resp.raise_for_status()
            data = http_resp.json()
            return_code = data.get("return_code", 0)
            if return_code == 5 and attempt < base._max_retries:
                time.sleep(base._retry_backoff * (attempt + 1))
                continue
            if return_code == 3 and attempt < base._max_retries:
                # Access token expired mid-run (discovered 2026-07-09: a
                # multi-hour deep backfill outlives the token, and this
                # library issues a token once at login() with no auto-
                # refresh anywhere in its request path — every call after
                # expiry fails the same way, cascading into hundreds of
                # stocks silently "failing" for a reason that has nothing to
                # do with that stock). Re-login and retry this same page
                # instead of giving up on the stock.
                api.login()
                headers = base._build_headers("ka10059", base._current_token(), cont_yn, next_key)
                continue
            if return_code != 0:
                raise KiwoomAPIError(
                    code=return_code,
                    message=data.get("return_msg", "Unknown error"),
                    response=data,
                )
            break
        else:
            break  # exhausted retries without a clean response

        page_rows = data.get("stk_invsr_orgn") or []
        all_rows.extend(page_rows)

        if stop_at and page_rows and min(r.get("dt", "") for r in page_rows) <= stop_at:
            break

        resp_cont = http_resp.headers.get("cont-yn", "N")
        resp_next = http_resp.headers.get("next-key", "")
        if page == max_pages - 1 or resp_cont != "Y" or not resp_next:
            break
        cont_yn, next_key = "Y", resp_next
        time.sleep(page_sleep_s)

    return all_rows


def _latest_sd_date(con: Any, code: str) -> str | None:
    """수급(supply_demand)의 최신 저장일 (``YYYYMMDD``), 없으면 None.

    ``con.execute`` 를 직접 쓰던 sqlite 전용 구현이었다 — ``combined --resume`` 은
    이 함수를 Postgres DSN 으로 부르므로 `AttributeError: 'connection' object has
    no attribute 'execute'` 가 나기 직전이었고, 그건 ``storage.fetchone`` 의
    docstring 이 "잠복" 이라고 이름까지 적어둔 바로 그 자리다.

    Postgres 는 DATE 컬럼을 ``datetime.date`` 로 돌려주는데 호출부는 응답의
    ``dt``(``YYYYMMDD`` 문자열)와 부등호로 비교한다 — 문자열로 맞춰야 date vs str
    TypeError 가 안 난다.
    """
    row = fetchone(con, "SELECT MAX(date) FROM supply_demand WHERE code=?", (code,))
    v = row[0] if row else None
    if v is None:
        return None
    return v.strftime("%Y%m%d") if hasattr(v, "strftime") else str(v)


def collect(
    api: KiwoomAPI,
    con: sqlite3.Connection,
    stocks: list[dict],
    *,
    days: int = 30,
    resume: bool = False,
    max_pages: int = 1,
    progress_every: int = 50,
) -> dict[str, int]:
    """Collect supply/demand for ``stocks`` into the DB. Returns a summary dict.

    Args:
        days: Keep only rows within the last ``days`` (0 = keep everything a
            page returns, no cutoff — use with ``max_pages`` > 1 for a deep
            backfill).
        max_pages: Pages per stock to fetch via
            :func:`_fetch_investor_flow_pages` (each ~100 days older than the
            last). ``1`` is the original single-page behavior. When
            ``resume`` is also set, pagination stops early once it reaches a
            stock's already-stored latest date, instead of always walking
            all ``max_pages``.
    """
    cutoff = days_ago(days)
    today = time.strftime("%Y%m%d")
    stats = {"done": 0, "skipped": 0, "failed": 0, "rows": 0}
    started = time.monotonic()

    for i, stock in enumerate(stocks, 1):
        code = stock["code"]
        if resume and max_pages <= 1 and _has_recent_rows(con, code, cutoff):
            stats["skipped"] += 1
            continue
        try:
            stop_at = _latest_sd_date(con, code) if resume and max_pages > 1 else ""
            rows = _fetch_investor_flow_pages(
                api, code, today, max_pages=max_pages, stop_at=stop_at or ""
            )
            records = build_sd_records(code, {"stk_invsr_orgn": rows}, cutoff)
            stats["rows"] += upsert_supply_demand(con, records)
            stats["done"] += 1
        except KiwoomAPIError as e:
            stats["failed"] += 1
            print(f"  ⚠️ {code} {stock['name']}: rc={e.code} {e.message[:50]}")
        except Exception as e:  # noqa: BLE001 — isolate per-stock failures
            stats["failed"] += 1
            print(f"  💥 {code} {stock['name']}: {type(e).__name__}: {e}")

        if i % progress_every == 0 or i == len(stocks):
            print(progress_line(
                i, len(stocks), started, stats, f"{stats['rows']:,} rows"))
    return stats


def main() -> int:
    parser = add_common_args(
        argparse.ArgumentParser(description="키움 수급 데이터 SQLite 수집기"))
    parser.add_argument("--days", type=int, default=30, help="최근 N일 (0=페이지가 주는 전체, cutoff 없음)")
    parser.add_argument(
        "--resume", action="store_true",
        help="max-pages=1이면 최근 데이터 있는 종목 건너뜀; max-pages>1이면 종목별 저장된 "
        "최신 날짜에 도달하는 즉시 페이지네이션 중단(전체 재백필 방지)",
    )
    parser.add_argument(
        "--max-pages", type=int, default=1,
        help="종목당 연속조회 페이지 수 (페이지당 ~100일). 1은 기존 동작과 동일, "
        "깊은 백필은 --max-pages 30 --days 0 같은 식으로 사용",
    )
    args = parser.parse_args()

    con, api = open_session(args)
    stocks = build_universe(api, con, args)
    print_banner(args, stocks, f"최근 {args.days}일")

    stats = collect(
        api, con, stocks, days=args.days, resume=args.resume, max_pages=args.max_pages
    )

    api.close()
    con.close()
    print(
        f"\n✅ 완료: done={stats['done']} skip={stats['skipped']} "
        f"fail={stats['failed']} rows={stats['rows']:,}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
