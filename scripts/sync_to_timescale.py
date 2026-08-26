"""One-way sync: kr-quant's local sqlite -> shared TimescaleDB.

Collectors (kr-quant) stay sqlite-only; this pushes a recent window of rows
into the LAN-exposed TimescaleDB so another host can query current data
without touching the sqlite file directly.

Column lists and the upsert itself come from ``collectors.storage`` — the
schema's single source of truth. They used to be re-declared here, justified as
insulation from *kr-quant*'s internals; that reason went stale once
``collectors/storage.py`` moved into this repo, and what was left was six lists
silently drifting from the DDL they mirror (an insert is positional, so a drift
puts values in the wrong column rather than erroring).

Usage:
    python sync_to_timescale.py --sqlite /path/to/kr_quant.db --days 7
    python sync_to_timescale.py --sqlite /path/to/kr_quant.db --full
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import psycopg2
import psycopg2.extras

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from collectors.storage import (  # noqa: E402
    DAILY_BAR_COLUMNS as DAILY_BAR_COLS,
    SUPPLY_DEMAND_COLUMNS as SUPPLY_DEMAND_COLS,
    _CREDIT_BALANCE_COLS as CREDIT_BALANCE_COLS,
    _SECTOR_INDEX_COLS as SECTOR_INDEX_COLS,
    _SHORT_SELLING_COLS as SHORT_SELLING_COLS,
    _STOCKS_COLS as STOCKS_COLS,
    _upsert,
)

TABLES: dict[str, list[str]] = {
    "daily_bars": DAILY_BAR_COLS,
    "supply_demand": SUPPLY_DEMAND_COLS,
    "short_selling": SHORT_SELLING_COLS,
    "credit_balance": CREDIT_BALANCE_COLS,
    "sector_index": SECTOR_INDEX_COLS,
}


def pg_dsn() -> str:
    return (
        f"host={os.environ['TIMESCALE_HOST']} port={os.environ.get('TIMESCALE_PORT', '5432')} "
        f"dbname={os.environ['TIMESCALE_DB']} user={os.environ['TIMESCALE_USER']} "
        f"password={os.environ['TIMESCALE_PASSWORD']}"
    )


def _to_date(value: str) -> date:
    """sqlite stores dates as 'YYYYMMDD' text; TimescaleDB columns are DATE."""
    return date(int(value[:4]), int(value[4:6]), int(value[6:8]))


def sync_table(
    sq: sqlite3.Connection,
    pg: psycopg2.extensions.connection,
    table: str,
    cols: list[str],
    cutoff: str | None,
    *,
    pk_cols: tuple[str, ...] = ("code", "date"),
) -> int:
    """Copy one table's rows into Postgres, upserting on its natural key.

    ``cutoff`` is a ``YYYYMMDD`` lower bound on ``date``; ``None`` reads the whole
    table (used for ``stocks``, which has no date column — that case used to be a
    second near-identical function).
    """
    select = f"SELECT {','.join(cols)} FROM {table}"  # noqa: S608 — cols/table 은 모듈 상수
    rows = (sq.execute(select).fetchall() if cutoff is None
            else sq.execute(f"{select} WHERE date >= ?", (cutoff,)).fetchall())
    if not rows:
        return 0
    # sqlite stores dates as 'YYYYMMDD' text; the TimescaleDB columns are DATE.
    converted = [
        tuple(_to_date(v) if c == "date" else v for c, v in zip(cols, row))
        for row in rows
    ]
    # storage._upsert builds the same ON CONFLICT statement this file used to
    # hand-roll — plus the page_size fix (so rowcount reflects every batch) and
    # the rollback-on-failure that keeps one bad row from aborting the whole
    # transaction for every later table.
    return _upsert(pg, table, cols, converted, pk_cols=pk_cols)


def main() -> int:
    parser = argparse.ArgumentParser(description="sqlite -> TimescaleDB 증분 동기화")
    parser.add_argument("--sqlite", default=os.environ.get("KR_QUANT_SQLITE_PATH"))
    parser.add_argument(
        "--days", type=int, default=7,
        help="최근 N일만 동기화 (기본 7 — 재시도/backfill 여유 포함, upsert라 겹쳐도 안전)",
    )
    parser.add_argument("--full", action="store_true", help="전체 히스토리 동기화 (최초 1회 부트스트랩용)")
    args = parser.parse_args()

    if not args.sqlite:
        raise SystemExit("--sqlite 또는 KR_QUANT_SQLITE_PATH 환경변수가 필요합니다.")

    cutoff = "19000101" if args.full else (date.today() - timedelta(days=args.days)).strftime("%Y%m%d")

    sq = sqlite3.connect(args.sqlite)
    pg = psycopg2.connect(pg_dsn())
    started = time.monotonic()
    try:
        n_stocks = sync_table(sq, pg, "stocks", STOCKS_COLS, None, pk_cols=("code",))
        totals = {table: sync_table(sq, pg, table, cols, cutoff) for table, cols in TABLES.items()}
    finally:
        sq.close()
        pg.close()

    elapsed = time.monotonic() - started
    print(f"✅ sync 완료 ({elapsed:.1f}s) stocks={n_stocks} " + " ".join(f"{t}={n}" for t, n in totals.items()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
