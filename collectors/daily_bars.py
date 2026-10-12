"""Collect per-stock daily OHLCV bars (전종목 일봉) into SQLite.

Source: ``ka10081`` (주식일봉차트요청) — one request returns up to ~600 daily
candles (about 2.5 years), so a single call backfills full history for most
stocks. Because this is a single TR, the kiwoom-client per-TR rate limiter
throttles to ~1 req/s; a full KOSPI+KOSDAQ common-stock sweep (~2,600 stocks)
takes roughly 45 minutes. Threads do **not** help — the per-TR bucket
serializes them — so the win is making each call count (600 rows/call) and
``--resume`` to skip stocks already backfilled.

CLI:
    kq-collect-daily --market all                 # mock, full history backfill
    kq-collect-daily --market all --prod          # real-server keys
    kq-collect-daily --resume                      # resume interrupted backfill
    kq-collect-daily --update                      # daily incremental (append new bars)
    kq-collect-daily --days 120                    # keep only last 120 days
"""

from __future__ import annotations

import argparse
import sqlite3
import time

from typing import Any

from kiwoom_client import KiwoomAPI
from kiwoom_client.base import KiwoomAPIError

from .kiwoom_cli import add_common_args, build_universe, open_session, print_banner
from .storage import (
    days_ago,
    fetchall,
    fetchone,
    progress_line,
    to_int,
    upsert_corporate_actions,
    upsert_daily_bars,
)

# ka10081 response: list key and per-row field map → DB columns.
_CHART_KEY = "stk_dt_pole_chart_qry"


def _has_any_rows(con: Any, code: str, table: str = "daily_bars") -> bool:
    return fetchone(con, f"SELECT 1 FROM {table} WHERE code=? LIMIT 1", (code,)) is not None  # noqa: S608 — 리터럴


def codes_current_as_of(con: Any, table: str, market_latest: str) -> set[str]:
    """``market_latest`` 이후 행이 있는 코드 집합 — `--update` 의 대량 판정.

    **종목별 ``MAX(date)`` 조회를 대체한다.** 그 방식은 종목당 한 번씩
    515개 청크를 가로질러야 해서 **건당 845ms** 다(실측). 2,628종목 × 2테이블
    = 5,256번이면 API 를 한 번도 안 불러도 16분이 걸린다 — 실제로 2026-08-26
    catchup 이 `done=0 skip=2628` 로 API 호출이 0이었는데도 16분을 썼다.

    같은 판정을 집합 하나로 받으면 **전체 47ms** 다. 날짜 조건이 최신 청크
    하나만 타기 때문이다.

    반환 집합에 없는 코드는 정의상 낡은 것이다(그 날짜 이후 행이 없다) —
    `MAX(date) >= market_latest` 와 의미가 정확히 같다.
    """
    sql = f"SELECT DISTINCT code FROM {table} WHERE date >= ?"  # noqa: S608 — table 은 호출부 리터럴
    return {r[0] for r in fetchall(con, sql, (market_latest,))}


def _latest_date(con: Any, code: str, table: str = "daily_bars") -> str | None:
    """Most recent stored bar date for ``code`` (YYYYMMDD), or None if empty."""
    row = fetchone(con, f"SELECT MAX(date) FROM {table} WHERE code=?", (code,))  # noqa: S608 — 리터럴
    v = row[0] if row else None
    if v is None:
        return None
    # sqlite는 date를 TEXT("20260716")로 보관하지만 Postgres는 date 타입이라
    # psycopg2가 datetime.date를 돌려준다. 호출부가 market_latest("20260716")와
    # 부등호 비교하므로 YYYYMMDD 문자열로 맞춘다 — 안 그러면 date vs str TypeError.
    return v.strftime("%Y%m%d") if hasattr(v, "strftime") else str(v)


# Liquid reference stock (삼성전자) used to learn the market's latest bar date.
_REF_CODE = "005930"


#: 장 마감(15:30 KST) 뒤 데이터 확정까지의 여유. 이 시각 전에는 오늘 봉을
#: **미완성**으로 본다. daily_collection 이 16:00 에 도는 근거와 같은 값이다.
_SESSION_CLOSE_HHMM = "1540"


def _market_latest_date(
    api: KiwoomAPI,
    base_dt: str,
    ref_code: str = _REF_CODE,
    *,
    now_hhmm: str | None = None,
) -> str:
    """서버가 가진 **완료된** 최신 거래일. 진행 중인 오늘 봉은 세지 않는다.

    ``--update`` 가 "이미 최신인 종목"을 건너뛰는 기준점이다.

    **왜 첫 행을 그대로 쓰면 안 되는가.** ka10081 은 장중에도 오늘 봉을
    첫 행으로 준다 — 09:00 부터 지금까지의 **미완성 캔들**이다. 10:05 에 도는
    catchup 이 그걸 최신 거래일로 삼으면, 전날 16:00 수집분(어제까지)은 전
    종목에서 "낡음"으로 판정돼 **스킵이 한 건도 안 걸린다.** 실측: 2026-08-24
    catchup 이 `done=2626 skip=0 | 일봉 1,503,880행` 으로 16:00 수집을 통째로
    재실행했고 48.6분을 썼다.

    덤으로 그 미완성 캔들이 확정 일봉과 구분 없이 `daily_bars` 에 upsert 돼,
    16:00 수집이 덮어쓰기 전까지 이 테이블을 읽는 쪽은 부분 봉을 확정 봉으로
    읽었다.

    프로브가 실패하면 ``base_dt`` 로 물러난다(예전과 동일 — 모르면 전부 받는다).
    """
    now_hhmm = now_hhmm or time.strftime("%H%M")
    today = time.strftime("%Y%m%d")
    try:
        resp = api.chart.stock_daily_chart(
            stk_cd=ref_code, base_dt=base_dt, upd_stkpc_tp="1"
        )
        rows = resp.get(_CHART_KEY) or []
        for row in rows:
            dt = row.get("dt")
            if not dt:
                continue
            if dt == today and now_hhmm < _SESSION_CLOSE_HHMM:
                continue  # 진행 중인 캔들 — 완료된 직전 거래일로 내려간다
            return dt
    except Exception:  # noqa: BLE001 — fall back to today on any probe failure
        pass
    return base_dt


def _row_to_record(code: str, row: dict) -> tuple:
    # Prices may carry a direction sign (e.g. cur_prc); store absolute price.
    return (
        code,
        row.get("dt", ""),
        abs(to_int(row.get("open_pric"))),
        abs(to_int(row.get("high_pric"))),
        abs(to_int(row.get("low_pric"))),
        abs(to_int(row.get("cur_prc"))),
        to_int(row.get("trde_qty")),
        to_int(row.get("trde_prica")),
    )


#: ka10081 ``upd_stkpc_tp`` 비트 → 종류. 키움 OpenAPI 의 수정주가구분 관례다. 실측으로 확인한 것은
#: 8(005930 2018-05-04 50:1 액면분할, ``upd_rt=-98.00``) 하나뿐 — 나머지는 원주가 점프와 대조해 확인한다.
EVENT_BITS = {
    1: "rights",              # 유상증자
    2: "bonus",               # 무상증자
    4: "dividend",            # 배당락
    8: "split",               # 액면분할
    16: "reverse_split",      # 액면병합
    32: "merger",             # 기업합병
    64: "capital_reduction",  # 감자
    256: "rights_ex",         # 권리락
}


def event_kind(tp: str) -> str:
    """``"10"`` → ``"bonus+split"``. 숫자가 아니거나 모르는 비트는 그대로 남긴다(버리지 않는다)."""
    if not tp.isdigit():
        return f"raw:{tp}"
    v = int(tp)
    names = [name for bit, name in EVENT_BITS.items() if v & bit]
    rest = v & ~sum(EVENT_BITS)
    if rest:
        names.append(f"bit{rest}")
    return "+".join(names) or f"raw:{tp}"


def _row_to_event(code: str, row: dict) -> tuple | None:
    """수정 이벤트가 찍힌 행이면 (code, dt, tp, upd_rt, kind), 아니면 None (027)."""
    tp = (row.get("upd_stkpc_tp") or "").strip()
    if not tp:
        return None
    rt = (row.get("upd_rt") or "").strip()
    try:
        upd_rt = float(rt) if rt else None
    except ValueError:
        upd_rt = None
    return (code, row.get("dt", ""), tp, upd_rt, event_kind(tp))


def collect(
    api: KiwoomAPI,
    con: sqlite3.Connection,
    stocks: list[dict],
    *,
    days: int = 0,
    max_pages: int = 1,
    resume: bool = False,
    update: bool = False,
    progress_every: int = 50,
    raw: bool = False,
) -> dict[str, int]:
    """Collect daily bars for ``stocks`` into the DB. Returns a summary dict.

    Args:
        days: Keep only the most recent N days. 0 = keep everything returned.
        max_pages: Continuation pages per stock (each ~600 bars). 1 is plenty
            for ~2.5y; raise to go deeper into history.
        resume: Skip stocks that already have at least one stored bar.
        update: Incremental mode — per stock, skip if already current and
            otherwise append only bars newer than the latest stored one
            (still one call/stock; a single call backfills multi-day gaps).
        raw: 무수정 주가(``upd_stkpc_tp="0"``)를 ``daily_bars_raw`` 에 쓴다(migration 026).
            기본(수정주가 → ``daily_bars``)은 수집 시점보다 앞선 병합·분할만큼 과거가 소급 조정돼
            "그날 실제 가격 × 그날 주식수" 시총이 틀린다.
    """
    table = "daily_bars_raw" if raw else "daily_bars"
    upd = "0" if raw else "1"
    cutoff = days_ago(days)
    base_dt = time.strftime("%Y%m%d")
    # In update mode, learn the newest trading day once so we can skip stocks
    # already current (makes same-day re-runs near-instant).
    market_latest = _market_latest_date(api, base_dt) if update else base_dt
    if update:
        print(f"📅 시장 최신 거래일: {market_latest}")
    stats = {"done": 0, "skipped": 0, "failed": 0, "rows": 0, "events": 0}
    started = time.monotonic()

    for i, stock in enumerate(stocks, 1):
        code = stock["code"]
        if resume and _has_any_rows(con, code, table):
            stats["skipped"] += 1
            continue
        # Incremental: stop pulling at the last bar we already have; skip
        # entirely if already at the latest trading day (nothing new).
        lower = ""
        if update:
            latest = _latest_date(con, code, table)
            if latest is not None and latest >= market_latest:
                stats["skipped"] += 1
                continue
            lower = latest or ""
        try:
            records: list[tuple] = []
            events: list[tuple] = []
            cont_yn, next_key = "N", ""
            for _ in range(max_pages):
                resp = api.chart.stock_daily_chart(
                    cont_yn=cont_yn,
                    next_key=next_key,
                    stk_cd=code,
                    base_dt=base_dt,
                    upd_stkpc_tp=upd,
                )
                stop = False
                for row in resp.get(_CHART_KEY, []) or []:
                    dt = row.get("dt", "")
                    if not dt:  # malformed row — skip, don't poison the batch insert
                        continue
                    if lower and dt <= lower:  # already stored — stop (newest-first)
                        stop = True
                        break
                    if cutoff and dt < cutoff:
                        stop = True
                        break
                    records.append(_row_to_record(code, row))
                    ev = _row_to_event(code, row)
                    if ev is not None:
                        events.append(ev)
                resp_cont = resp.get("cont_yn") or resp.get("cont-yn", "N")
                resp_next = resp.get("next_key") or resp.get("next-key", "")
                if stop or resp_cont != "Y" or not resp_next:
                    break
                cont_yn, next_key = "Y", resp_next
            stats["rows"] += upsert_daily_bars(con, records, table)
            # 수정주가 이벤트(분할·병합·증자…)는 수정·무수정 응답에 똑같이 실린다 — 어느 경로든 남긴다(027).
            stats["events"] += upsert_corporate_actions(con, events)
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
        argparse.ArgumentParser(description="키움 전종목 일봉 SQLite 수집기"))
    parser.add_argument(
        "--days", type=int, default=0,
        help="최근 N일만 저장 (0=콜이 주는 전체 ~2.5년, 기본 0)",
    )
    parser.add_argument(
        "--max-pages", type=int, default=1,
        help="종목당 연속조회 페이지 수 (페이지당 ~600봉). 더 깊은 과거는 ↑",
    )
    parser.add_argument(
        "--resume", action="store_true", help="이미 봉이 있는 종목 건너뜀 (백필 재개)"
    )
    parser.add_argument(
        "--update", action="store_true",
        help="증분: 종목별 최신봉 이후만 추가, 이미 최신이면 건너뜀 (일일 갱신)",
    )
    parser.add_argument(
        "--raw", action="store_true",
        help="무수정 주가(upd_stkpc_tp=0)를 daily_bars_raw 에 (migration 026)",
    )
    parser.add_argument(
        "--codes", default="",
        help="쉼표로 준 코드만(ETF 등) — 상장 종목 유니버스·stocks 마스터를 건드리지 않는다",
    )
    args = parser.parse_args()

    con, api = open_session(args)
    if args.codes:
        stocks = [{"code": c.strip(), "name": c.strip()} for c in args.codes.split(",") if c.strip()]
    else:
        stocks = build_universe(api, con, args)
    print_banner(args, stocks,
                 "전체(~2.5년)" if args.days == 0 else f"최근 {args.days}일")

    stats = collect(
        api, con, stocks, days=args.days, max_pages=args.max_pages,
        resume=args.resume, update=args.update, raw=args.raw,
    )

    api.close()
    con.close()
    print(
        f"\n✅ 완료: done={stats['done']} skip={stats['skipped']} "
        f"fail={stats['failed']} rows={stats['rows']:,} events={stats['events']:,}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
