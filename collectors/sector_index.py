"""Collect per-industry daily index bars (업종 일봉) into SQLite.

Source: ``ka20003`` (전업종지수요청) for the industry code+name list, then
``ka20006`` (업종일봉차트요청) per industry for historical daily bars. Only
~65 industries total (31 KOSPI + 34 KOSDAQ), so a full sweep is fast — no
resume/backfill machinery needed like the per-stock collectors.

CLI:
    kq-collect-sector --prod              # 실서버, 업종지수 일봉 전체 수집
    kq-collect-sector --days 120          # 최근 120일만 저장
"""

from __future__ import annotations

import argparse
import sqlite3
import time

from kiwoom_rest_api import KiwoomAPI
from kiwoom_rest_api.base import KiwoomAPIError

from .config import make_api, mask_dsn
from .storage import connect, days_ago, default_db_path, to_int, upsert_sector_index

# ka20006 response: list key for daily bars.
_CHART_KEY = "inds_dt_pole_qry"

# ka20003 scopes that together cover every industry code Kiwoom exposes.
_SCOPES = ("001", "101")  # KOSPI, KOSDAQ


def fetch_sector_list(api: KiwoomAPI) -> list[dict]:
    """All industry codes+names across the KOSPI/KOSDAQ scopes."""
    seen: dict[str, str] = {}
    for scope in _SCOPES:
        resp = api.sector.all_industry_index(inds_cd=scope)
        for row in resp.get("all_inds_idex", []) or []:
            seen[row["stk_cd"]] = row["stk_nm"]
    return [{"code": code, "name": name} for code, name in seen.items()]


def _row_to_record(code: str, name: str, row: dict) -> tuple:
    return (
        code,
        name,
        row.get("dt", ""),
        abs(to_int(row.get("open_pric"))),
        abs(to_int(row.get("high_pric"))),
        abs(to_int(row.get("low_pric"))),
        abs(to_int(row.get("cur_prc"))),
        to_int(row.get("trde_qty")),
        to_int(row.get("trde_prica")),
    )


def collect(
    api: KiwoomAPI,
    con: sqlite3.Connection,
    sectors: list[dict],
    *,
    days: int = 0,
    progress_every: int = 10,
) -> dict[str, int]:
    """Collect daily index bars for ``sectors`` into the DB. Returns a summary dict."""
    base_dt = time.strftime("%Y%m%d")
    cutoff = days_ago(days)
    stats = {"done": 0, "failed": 0, "rows": 0}
    started = time.monotonic()

    for i, sector in enumerate(sectors, 1):
        code, name = sector["code"], sector["name"]
        try:
            resp = api.chart.industry_daily_chart(inds_cd=code, base_dt=base_dt)
            records = [
                _row_to_record(code, name, row)
                for row in resp.get(_CHART_KEY, []) or []
                if not cutoff or row.get("dt", "") >= cutoff
            ]
            stats["rows"] += upsert_sector_index(con, records)
            stats["done"] += 1
        except KiwoomAPIError as e:
            stats["failed"] += 1
            print(f"  ⚠️ {code} {name}: rc={e.code} {e.message[:50]}")
        except Exception as e:  # noqa: BLE001 — isolate per-sector failures
            stats["failed"] += 1
            print(f"  💥 {code} {name}: {type(e).__name__}: {e}")

        if i % progress_every == 0 or i == len(sectors):
            elapsed = time.monotonic() - started
            print(
                f"  [{i}/{len(sectors)}] done={stats['done']} fail={stats['failed']} "
                f"| {stats['rows']:,} rows | {elapsed:.1f}s"
            )
    return stats


def main() -> int:
    parser = argparse.ArgumentParser(description="키움 업종지수 일봉 SQLite 수집기")
    parser.add_argument("--prod", action="store_true", help="실서버 사용 (기본: 모의)")
    parser.add_argument(
        "--days", type=int, default=0, help="최근 N일만 저장 (0=콜이 주는 전체, 기본 0)"
    )
    parser.add_argument("--db", default=str(default_db_path()))
    parser.add_argument(
        "--rate", type=float, default=0.9, help="TR당 요청 속도(req/s). 429 방지용 기본값"
    )
    args = parser.parse_args()

    con = connect(args.db)
    api = make_api(is_mock=not args.prod, rate_limit=args.rate, max_retries=5)

    sectors = fetch_sector_list(api)
    server = "모의" if not args.prod else "실서버"
    window = "전체" if args.days == 0 else f"최근 {args.days}일"
    print(f"🔌 {server} | 업종 {len(sectors)}개 | {window}")
    print(f"💾 {mask_dsn(args.db)}\n")

    stats = collect(api, con, sectors, days=args.days)

    api.close()
    con.close()
    print(f"\n✅ 완료: done={stats['done']} fail={stats['failed']} rows={stats['rows']:,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
