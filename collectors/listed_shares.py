"""Collect per-stock listed shares outstanding (상장주식수) into SQLite.

Source: ``ka10001`` (종목기본정보요청 / basic stock info), exposed as
``api.stock_info.basic_stock_info(stk_cd=code)``. Only current values are
returned — see the caveat below.

Field-name resolution (✅ VERIFIED 2026-07-09 via a real ``--prod`` call for
005930 삼성전자 from inside the quant-airflow scheduler container, using
credentials from Airflow's Variables store):
    ``flo_stk`` is confirmed correct and its unit is **thousands of shares**.
    The real response returned ``"flo_stk": "5846279"``; ×1,000 =
    5,846,279,000, which is in the right ballpark for Samsung Electronics'
    actual ~5.97B shares outstanding (the small gap is plausibly treasury
    shares / reporting-date drift, not a unit error — raw or ×1,000,000 would
    be off by orders of magnitude, so ×1,000 is unambiguously the right unit).

Historical backfill caveat:
    ka10001 appears to return only the *current* shares-outstanding snapshot,
    not a time series. Unlike ``daily_bars`` (which can backfill ~2.5 years
    of history from a single ``ka10081`` call), this collector cannot
    backfill ``shares_outstanding_history`` to 2024 — the table will only
    start accumulating data from whenever this collector is first run in
    production. Corporate actions (splits, buybacks) that happened before
    the first run will not be reflected in earlier ``market_cap_asof``
    lookups.

CLI:
    kq-collect-shares --market all                # mock server
    kq-collect-shares --market all --prod          # real-server keys
    kq-collect-shares --limit 10                   # test on a few stocks
"""

from __future__ import annotations

import argparse
import sqlite3
import time

from kiwoom_rest_api import KiwoomAPI
from kiwoom_rest_api.base import KiwoomAPIError

from .config import make_api, mask_dsn
from .storage import connect, default_db_path, to_int, upsert_shares_outstanding, upsert_stocks
from .supply_demand import fetch_stock_list, is_common_stock

# ka10001 response field holding 상장주식수, in thousands of shares — verified
# 2026-07-09 against a real API response (005930), see module docstring.
_SHARES_FIELD = "flo_stk"
_SHARES_UNIT_MULTIPLIER = 1000

# 종목별 즉시 upsert(~2,600회 DB 라운드트립) 대신 CHUNK_SIZE개마다 배치 upsert.
# 루프 종료 후 1회 upsert(메가배치)는 크래시 시 전체 진행분을 잃고 레코드 1건
# 오류가 전체를 롤백시키므로 대신 청크 단위 중간 커밋을 쓴다 — 크래시 손실을
# 최대 1청크(~2분) 분량으로 제한하면서 라운드트립은 ~2,600 → ~26회로 줄인다.
_CHUNK_SIZE = 100


def collect(
    api: KiwoomAPI,
    con: sqlite3.Connection,
    stocks: list[dict],
    *,
    progress_every: int = 50,
) -> dict[str, int]:
    """Collect current shares-outstanding snapshots for ``stocks``. Returns a summary dict."""
    today = time.strftime("%Y%m%d")
    stats = {"done": 0, "failed": 0, "rows": 0}
    started = time.monotonic()
    buffer: list[tuple] = []

    def flush() -> None:
        if buffer:
            stats["rows"] += upsert_shares_outstanding(con, buffer, source="kiwoom")
            buffer.clear()

    for i, stock in enumerate(stocks, 1):
        code = stock["code"]
        try:
            resp = api.stock_info.basic_stock_info(stk_cd=code)
            shares = to_int(resp.get(_SHARES_FIELD)) * _SHARES_UNIT_MULTIPLIER
            buffer.append((code, today, shares))
            stats["done"] += 1
        except KiwoomAPIError as e:
            stats["failed"] += 1
            print(f"  ⚠️ {code} {stock['name']}: rc={e.code} {e.message[:50]}")
        except Exception as e:  # noqa: BLE001 — isolate per-stock failures
            stats["failed"] += 1
            print(f"  💥 {code} {stock['name']}: {type(e).__name__}: {e}")

        if len(buffer) >= _CHUNK_SIZE:
            flush()

        if i % progress_every == 0 or i == len(stocks):
            elapsed = time.monotonic() - started
            rate = i / elapsed if elapsed else 0
            eta = (len(stocks) - i) / rate / 60 if rate else 0
            print(
                f"  [{i}/{len(stocks)}] done={stats['done']} fail={stats['failed']} "
                f"| {stats['rows']:,} rows | {rate:.1f} stk/s | ETA {eta:.1f}m"
            )
    flush()
    return stats


def main() -> int:
    parser = argparse.ArgumentParser(description="키움 상장주식수 SQLite 수집기")
    parser.add_argument("--prod", action="store_true", help="실서버 사용 (기본: 모의)")
    parser.add_argument("--market", choices=["kospi", "kosdaq", "all"], default="all")
    parser.add_argument("--limit", type=int, default=0, help="앞에서 N종목만 (테스트)")
    parser.add_argument("--db", default=str(default_db_path()))
    parser.add_argument(
        "--rate", type=float, default=0.9,
        help="TR당 요청 속도(req/s). 긴 전수 수집의 429 방지를 위해 기본 0.9",
    )
    args = parser.parse_args()

    con = connect(args.db)
    api = make_api(is_mock=not args.prod, rate_limit=args.rate, max_retries=5)

    markets = ["kospi", "kosdaq"] if args.market == "all" else [args.market]
    stocks = fetch_stock_list(api, markets)
    stocks = [s for s in stocks if is_common_stock(s)]
    if args.limit:
        stocks = stocks[: args.limit]

    upsert_stocks(con, stocks)
    server = "모의" if not args.prod else "실서버"
    print(f"🔌 {server} | 시장={args.market} | 종목 {len(stocks)}개")
    print(f"💾 {mask_dsn(args.db)}\n")

    stats = collect(api, con, stocks)

    api.close()
    con.close()
    print(f"\n✅ 완료: done={stats['done']} fail={stats['failed']} rows={stats['rows']:,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
