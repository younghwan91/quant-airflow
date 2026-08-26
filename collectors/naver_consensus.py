"""Collect analyst consensus (목표주가·투자의견) from Naver Finance.

Kiwoom's broker API has no analyst consensus; Naver Finance exposes it (FnGuide-
sourced) via the mobile integration endpoint, no auth required. This is the
forward-looking signal PEAD lacks — target-price implied upside and, once a time
series accumulates, **consensus revisions** (the re-rating that drives mega-caps
where post-earnings drift is arbitraged away). See docs/pead-strategy.md.

The endpoint is a **current snapshot**, so this collector is meant to run daily,
appending a date-stamped row per code to build the revision time series over
time. ``parse_consensus`` is pure (JSON in → numbers out) and unit-tested.
Writes CSV: date,code,target_mean,recomm_mean,base_date.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import csv
import json
import os
import time
import urllib.request
from datetime import date

from .storage import to_float_or_none, universe_query

BASE = "https://m.stock.naver.com/api/stock"
UA = ("Mozilla/5.0 (iPhone; CPU iPhone OS 15_0 like Mac OS X) "
      "AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148")


#: 콤마 섞인 숫자 문자열 → float|None. 정본은 storage 하나다 — 이 6줄이
#: dart_earnings 와 naver_consensus 에 한 벌씩 있었다.
_to_float = to_float_or_none


def parse_consensus(payload: dict) -> tuple[float | None, float | None, str | None]:
    """Extract (target_price_mean, recomm_mean, base_date) from an integration response.

    Args:
        payload: Parsed Naver ``/stock/{code}/integration`` JSON.

    Returns:
        ``(target_mean, recomm_mean, base_date)`` from ``consensusInfo``, or
        ``(None, None, None)`` when the stock has no analyst coverage.
        ``recomm_mean`` is Naver's 1–5 scale (5 = strong buy). ``target_mean`` is
        the mean 12m target price (원). ``base_date`` is the consensus as-of date.
    """
    ci = payload.get("consensusInfo") or {}
    return (
        _to_float(ci.get("priceTargetMean")),
        _to_float(ci.get("recommMean")),
        ci.get("createDate") or None,
    )


def parse_estimate(payload: dict) -> tuple[float | None, float | None, str | None]:
    """Extract forward EPS consensus from a ``finance/annual`` response.

    Naver marks future periods with ``isConsensus == "Y"`` and fills in analyst
    estimates. This returns next year's **estimated EPS** and the most recent
    **actual EPS** (prior year), so the caller can form an *expected growth*
    signal — the forward-looking analogue of PEAD that (unlike backward earnings)
    can work in mega-caps, where the market prices future expectations.

    Returns:
        ``(fwd_eps, prev_eps, est_year)`` — estimated EPS for the consensus year,
        the latest actual EPS, and the estimate year key (e.g. "202612"); or
        ``(None, None, None)`` if no consensus year / EPS row is present.
    """
    fi = payload.get("financeInfo") or {}
    titles = fi.get("trTitleList") or []
    cons = [t.get("key") for t in titles if t.get("isConsensus") == "Y"]
    actuals = [t.get("key") for t in titles if t.get("isConsensus") != "Y"]
    if not cons:
        return None, None, None
    est_year = cons[0]

    def _row_name(r: dict) -> str:
        t = r.get("title")
        return t.get("name", "") if isinstance(t, dict) else str(t or "")

    eps_row = next((r for r in fi.get("rowList", []) if _row_name(r) == "EPS"), None)
    if eps_row is None:
        return None, None, None
    cols = eps_row.get("columns") or {}
    fwd = _to_float((cols.get(est_year) or {}).get("value"))
    prev = _to_float((cols.get(actuals[-1]) or {}).get("value")) if actuals else None
    return fwd, prev, est_year


def _get_json(url: str, *, retries: int = 3) -> dict | None:
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    for _ in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=20) as r:
                return json.loads(r.read().decode())
        except Exception:
            time.sleep(1.0)
    return None


def fetch_consensus(code: str) -> tuple[float | None, float | None, str | None]:
    """Fetch (target_mean, recomm_mean, base_date) for one code from Naver."""
    d = _get_json(f"{BASE}/{code}/integration")
    return parse_consensus(d) if d else (None, None, None)


def fetch_estimate(code: str) -> tuple[float | None, float | None, str | None]:
    """Fetch (fwd_eps, prev_eps, est_year) forward consensus for one code."""
    d = _get_json(f"{BASE}/{code}/finance/annual")
    return parse_estimate(d) if d else (None, None, None)


def _fetch_both(
    code: str, sleep: float,
) -> tuple[str, float | None, float | None, str | None, float | None, float | None, str | None]:
    """Both endpoints for one code — the unit of work handed to the thread pool.

    sleep 은 **두 요청 사이에만** 둔다. 작업 단위 마지막에 한 번 더 자는 건
    순수 낭비였다 — 다음 요청은 자기 앞의 sleep 을 이미 갖고 있어서 그 잠이
    아무것도 늦추지 않고 워커만 놀렸다(2,627종목 × 0.2초 ÷ 4워커 = 131초,
    전체 실행 시간의 15%).
    """
    tm, rm, bd = fetch_consensus(code)
    time.sleep(sleep)
    fe, pe, ey = fetch_estimate(code)
    return code, tm, rm, bd, fe, pe, ey


def _universe_query(args: argparse.Namespace) -> tuple[str, dict]:
    """SQL (+ params) selecting the code universe: all ``daily_bars`` codes or top-N liquid.

    ``--all-codes`` uses a plain ``DISTINCT code`` scan with no recency window, so a
    stock that just IPO'd today (and so has only today's row in ``daily_bars``) is
    included from day one — no special-casing needed for newly listed codes.

    ``--covered-days N`` 은 **애널리스트 커버리지가 있는 종목만** 고른다.
    전종목(2,627개)을 매일 도는 건 요청의 73% 가 헛돈다는 뜻이다 — 실제 적재는
    하루 652~660행이고 나머지 ~1,930 종목은 세 필드가 전부 None 이라 조용히
    버려진다. 커버리지는 하루아침에 생기지 않으므로 매일 확인할 이유가 없다.
    **새로 커버리지가 붙은 종목은 주 1회 전종목 스윕이 잡는다** — 그 스윕이
    없으면 이 필터는 한 번 빠진 종목을 영원히 놓친다.
    """
    covered_days = getattr(args, "covered_days", 0)
    if covered_days:
        return (
            "SELECT DISTINCT code FROM consensus "
            "WHERE date >= CURRENT_DATE - make_interval(days => %(d)s) ORDER BY code",
            {"d": covered_days},
        )
    return universe_query(all_codes=args.all_codes, top_n=args.top_n)


def main() -> int:
    ap = argparse.ArgumentParser(description="네이버 애널리스트 컨센서스 수집 (목표주가·투자의견, 일별 스냅샷)")
    ap.add_argument("--out", default=None, help="출력 CSV (일별 append)")
    ap.add_argument("--top-n", type=int, default=800, help="유동성 상위 N종목")
    ap.add_argument("--all-codes", action="store_true", help="유동성 상위 N 대신 daily_bars 전종목 사용")
    ap.add_argument(
        "--covered-days", type=int, default=0,
        help="최근 N일 안에 컨센서스가 실제로 잡힌 종목만 조회 (0=끔). "
             "전종목의 73%%는 매일 빈 응답이라 헛돈다 — 단 주 1회는 --all-codes 로 "
             "전종목을 훑어야 신규 커버리지를 놓치지 않는다.",
    )
    ap.add_argument("--db-table", action="store_true", help="CSV 대신 consensus 테이블에 직접 upsert")
    ap.add_argument("--db", default=None)
    ap.add_argument("--sleep", type=float, default=0.2)
    ap.add_argument(
        "--workers", type=int, default=8,
        help="동시 fetch 스레드 수. 네이버는 무인증·독립 레이트리밋이라 "
             "4는 과보수적이었다.",
    )
    args = ap.parse_args()
    if not args.db_table and not args.out:
        ap.error("--out is required unless --db-table is set")

    import pandas as pd
    from .storage import connect, default_db_path, upsert_consensus
    con = connect(args.db or str(default_db_path()))
    sql, params = _universe_query(args)
    top = pd.read_sql_query(sql, con, params=params)
    codes = top["code"].tolist()

    today = date.today().isoformat()
    done: set[str] = set()
    if args.db_table:
        existing = pd.read_sql_query(
            "SELECT code FROM consensus WHERE date = %(d)s", con, params={"d": today})
        done = set(existing["code"])
    else:
        con.close()
        if os.path.exists(args.out):
            for r in csv.reader(open(args.out)):
                if r and r[0] == today:
                    done.add(r[1])  # (date, code) already collected today

    pending = [c for c in codes if c not in done]

    f = open(args.out, "a", newline="") if args.out else None
    w = csv.writer(f) if f else None
    n = 0
    # DB 경로: future 완료마다 개별 upsert(~2,600회 라운드트립) 대신 _CHUNK_SIZE개
    # 완료마다 배치 upsert. 루프 종료 후 1회(메가배치)는 크래시 시 전체 진행분을
    # 잃으므로 청크 단위 중간 커밋으로 손실을 최대 1청크 분량으로 제한한다.
    # as_completed 순서라 청크 경계는 종목 순서가 아니라 완료 순서 기준이지만,
    # 각 레코드가 독립적이라(같은 (code,date) 재수집이 아님) 문제 없다.
    _CHUNK_SIZE = 100
    db_buffer: list[tuple] = []

    def flush_db() -> None:
        nonlocal n
        if db_buffer:
            upsert_consensus(con, db_buffer)
            n += len(db_buffer)
            db_buffer.clear()

    # 종목당 2개 독립 엔드포인트(integration/finance-annual) 순차 호출 + sleep이라
    # ~2,600종목이면 sleep만 수십 분 걸림 — 스레드풀로 종목 단위 fetch를 겹쳐서
    # wall-clock을 줄인다. DB/CSV 쓰기는 메인 스레드에서만(순차) 수행해 커넥션을
    # 스레드 간 공유하지 않는다.
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {ex.submit(_fetch_both, code, args.sleep): code for code in pending}
        for i, fut in enumerate(concurrent.futures.as_completed(futures), 1):
            code, tm, rm, bd, fe, pe, ey = fut.result()
            if tm is None and rm is None and fe is None:
                continue

            if args.db_table:
                db_buffer.append((code, today, tm, rm, bd, fe, pe, ey))
                if len(db_buffer) >= _CHUNK_SIZE:
                    flush_db()
            else:
                def _s(x: object) -> object:
                    return x if x is not None else ""
                w.writerow([today, code, _s(tm), _s(rm), bd or "", _s(fe), _s(pe), ey or ""])
                n += 1
            if i % 50 == 0:
                if f:
                    f.flush()
                print(f"[{i}/{len(pending)}] rows={n}", flush=True)
    flush_db()
    if f:
        f.close()
    if args.db_table:
        con.close()
    print(f"DONE date={today} rows={n}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
