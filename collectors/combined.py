"""Collect daily bars **and** investor supply/demand in a single sweep.

Per stock this issues two requests: ``ka10081`` (주식일봉차트, ~600 bars) and
``ka10059`` (투자자기관별종목별, ~100 recent days). Because Kiwoom rate-limits
**per TR (api_id)** and these are different TRs with independent buckets, the
two calls do not throttle each other — interleaving them in one loop collects
both datasets in roughly the time of one (~45 min for ~2,600 stocks), not the
~90 min you'd get running the two collectors back to back.

CLI:
    kq-collect-both --market all                  # mock, both datasets
    kq-collect-both --market all --prod           # real-server keys
    kq-collect-both --resume                       # skip stocks already done
    kq-collect-both --update                       # skip daily-bar call if already current
    kq-collect-both --sd-days 60 --daily-days 0    # SD last 60d, daily full
"""

from __future__ import annotations

import argparse
import sqlite3
import time

from kiwoom_rest_api import KiwoomAPI
from kiwoom_rest_api.base import KiwoomAPIError

from .kiwoom_cli import add_common_args, build_universe, open_session, print_banner
from .storage import (
    date_days_ago,
    days_ago,
    progress_line,
    upsert_daily_bars,
    upsert_supply_demand,
)
from .daily_bars import (
    _CHART_KEY,
    _has_any_rows,
    _market_latest_date,
    _row_to_record,
    codes_current_as_of,
)
from .supply_demand import (
    _has_recent_rows,
    build_sd_records,
)


# 종목별 즉시 upsert(~2,600회×2 라운드트립) 대신 CHUNK_SIZE개 종목마다 배치 upsert.
# `_upsert` 는 문장마다 `con.commit()` 하므로 종목별 호출은 Postgres fsync 를
# 실행당 ~5,250번 부른다 — short_credit·listed_shares 는 이미 이 청크 패턴을 쓰고
# 있었고, 종목 루프를 도는 콜렉터 중 여기만 남아 있었다. 루프 종료 후 1회
# (메가배치)로 미루지 않는 이유도 그 둘과 같다: 크래시 손실을 최대 1청크로 제한한다.
_CHUNK_SIZE = 100


def collect(
    api: KiwoomAPI,
    con: sqlite3.Connection,
    stocks: list[dict],
    *,
    sd_days: int = 100,
    daily_days: int = 0,
    resume: bool = False,
    update: bool = False,
    progress_every: int = 50,
) -> dict[str, int]:
    """Collect daily bars + supply/demand for ``stocks``. Returns a summary dict.

    Args:
        sd_days: Supply/demand window in days (ka10059 returns ~100 max).
        daily_days: Daily-bar window in days. 0 = everything the call returns.
        resume: Skip stocks that already have both a daily bar and recent SD.
        update: Skip the per-stock calls for stocks already at the market's
            latest **completed** trading day (self-heals prior-day gaps cheaply
            — a stock still missing yesterday's bar looks not-current and gets
            a full re-fetch, one already caught up costs zero API calls).
            일봉과 수급을 **따로** 판정한다: 한쪽만 낡았으면 그쪽 TR 만 부른다.

            수급 가드가 없던 시절엔 ka10059 가 "롤링 100일 창만 주고 단일일을
            안 준다"는 이유로 항상 재수집됐다. 그 논리가 틀렸다 — 필요한 건
            단일일 조회 API 가 아니라 "이 종목의 최신 거래일 행이 DB 에 이미
            있나" 한 줄이고, 그게 `_sd_latest_date` 다. 그 전까지는 새 데이터가
            존재할 수 없는 일요일에도 175,266행을 다시 쓰며 48.6분을 태웠다.
    """
    base_dt = time.strftime("%Y%m%d")
    # date_days_ago(=항상 날짜): 여기는 원래 days>0 가드가 없었다. daily_cutoff 와
    # 달리 sd_days=0 은 "창 없음"이 아니라 "오늘"이다.
    sd_cutoff = date_days_ago(sd_days)
    daily_cutoff = days_ago(daily_days)
    market_latest = _market_latest_date(api, base_dt) if update else base_dt
    # 최신 여부를 **한 번에** 받아둔다 — 종목별 MAX(date) 는 건당 845ms 라
    # (515개 청크를 가로지른다) 5,256번이면 API 를 안 불러도 16분이다.
    daily_current_codes = codes_current_as_of(con, "daily_bars", market_latest) if update else set()
    sd_current_codes = codes_current_as_of(con, "supply_demand", market_latest) if update else set()
    if update:
        print(
            f"📅 시장 최신 거래일: {market_latest} "
            f"(이미 최신: 일봉 {len(daily_current_codes):,} · 수급 {len(sd_current_codes):,})"
        )
    stats = {"done": 0, "skipped": 0, "failed": 0, "daily_rows": 0, "sd_rows": 0}
    started = time.monotonic()
    daily_buffer: list[tuple] = []
    sd_buffer: list[tuple] = []

    def flush() -> None:
        if daily_buffer:
            stats["daily_rows"] += upsert_daily_bars(con, daily_buffer)
            daily_buffer.clear()
        if sd_buffer:
            stats["sd_rows"] += upsert_supply_demand(con, sd_buffer)
            sd_buffer.clear()

    for i, stock in enumerate(stocks, 1):
        code = stock["code"]
        if resume and _has_any_rows(con, code) and _has_recent_rows(con, code, sd_cutoff):
            stats["skipped"] += 1
            continue
        daily_current = code in daily_current_codes
        sd_current = code in sd_current_codes
        if daily_current and sd_current:
            stats["skipped"] += 1
            continue
        try:
            if not daily_current:
                # 일봉 (ka10081) — own rate-limit bucket.
                d_resp = api.chart.stock_daily_chart(
                    stk_cd=code, base_dt=base_dt, upd_stkpc_tp="1"
                )
                bars = [
                    _row_to_record(code, r)
                    for r in d_resp.get(_CHART_KEY, []) or []
                    if r.get("dt")
                    and (not daily_cutoff or r["dt"] >= daily_cutoff)
                    # 진행 중인 오늘 캔들을 확정 일봉으로 쓰지 않는다 —
                    # market_latest 는 완료된 최신 거래일이다.
                    and r["dt"] <= market_latest
                ]
                daily_buffer.extend(bars)

            if not sd_current:
                # 수급 (ka10059) — separate TR, separate bucket (no extra throttle).
                s_resp = api.stock_info.investor_institution_by_stock(
                    dt=base_dt, stk_cd=code, amt_qty_tp="2", trde_tp="0", unit_tp="1"
                )
                sd_buffer.extend(build_sd_records(code, s_resp, sd_cutoff))
            stats["done"] += 1
        except KiwoomAPIError as e:
            stats["failed"] += 1
            print(f"  ⚠️ {code} {stock['name']}: rc={e.code} {e.message[:50]}")
        except Exception as e:  # noqa: BLE001 — isolate per-stock failures
            stats["failed"] += 1
            print(f"  💥 {code} {stock['name']}: {type(e).__name__}: {e}")

        if stats["done"] % _CHUNK_SIZE == 0:
            flush()

        if i % progress_every == 0 or i == len(stocks):
            print(progress_line(
                i, len(stocks), started, stats,
                f"일봉 {stats['daily_rows']:,} / 수급 {stats['sd_rows']:,}"))
    flush()
    return stats


def main() -> int:
    parser = add_common_args(
        argparse.ArgumentParser(description="키움 일봉+수급 통합 SQLite 수집기"))
    parser.add_argument(
        "--sd-days", type=int, default=100, help="수급 최근 N일 (ka10059 최대 ~100)"
    )
    parser.add_argument(
        "--daily-days", type=int, default=0,
        help="일봉 최근 N일만 저장 (0=콜이 주는 전체 ~2.5년, 기본 0)",
    )
    parser.add_argument(
        "--resume", action="store_true", help="일봉+최근수급 둘 다 있는 종목 건너뜀"
    )
    parser.add_argument(
        "--update", action="store_true",
        help="일봉: 이미 시장 최신 거래일이면 API 호출 스킵 (수급은 매번 재수집)",
    )
    args = parser.parse_args()

    con, api = open_session(args)
    stocks = build_universe(api, con, args)
    daily_win = "전체(~2.5년)" if args.daily_days == 0 else f"최근 {args.daily_days}일"
    print_banner(args, stocks, f"일봉 {daily_win} + 수급 최근 {args.sd_days}일")

    stats = collect(
        api, con, stocks,
        sd_days=args.sd_days, daily_days=args.daily_days,
        resume=args.resume, update=args.update,
    )

    api.close()
    con.close()
    print(
        f"\n✅ 완료: done={stats['done']} skip={stats['skipped']} "
        f"fail={stats['failed']} | 일봉 {stats['daily_rows']:,}행 "
        f"수급 {stats['sd_rows']:,}행"
    )

    # 개별 종목 실패는 collect()가 이미 격리해서 처리하지만, 실패율이 높으면
    # (예: 스키마 드리프트처럼 전종목이 같은 이유로 실패) 파이프라인 전체가
    # 실패했다고 봐야 함 — Airflow 등 호출자가 감지할 수 있도록 exit code로 신호.
    attempted = stats["done"] + stats["failed"]
    if attempted and stats["failed"] / attempted > 0.2:
        print(f"❌ 실패율 {stats['failed']}/{attempted} > 20% — 비정상 종료")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
