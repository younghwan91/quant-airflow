"""Collect per-stock short selling (공매도) and credit balance (신용잔고) into SQLite.

Sources:
- ``ka10014`` 공매도추이 — daily short selling qty, outstanding short balance,
  short ratio, avg price. Params: stk_cd, tm_tp="1", strt_dt, end_dt.
- ``ka10013`` 신용매매동향 — daily new/repay/balance qty and ratio.
  Params: stk_cd, dt="0" (all available), qry_tp="1" (주식신용).

Both TRs have independent per-TR rate-limit buckets, so interleaving them in
one loop collects both datasets in roughly the time of one (~45 min full sweep).

CLI:
    kq-collect-sc --market all              # mock, both datasets
    kq-collect-sc --market all --prod       # real-server keys
    kq-collect-sc --resume                   # skip stocks already done
    kq-collect-sc --days 60                  # keep last 60 days only
"""

from __future__ import annotations

import argparse
import sqlite3
import time

from typing import Any

from kiwoom_rest_api import KiwoomAPI
from kiwoom_rest_api.base import KiwoomAPIError

from .config import make_api, mask_dsn
from .storage import (
    connect,
    fetchone,
    default_db_path,
    to_float,
    to_int,
    upsert_credit_balance,
    upsert_short_selling,
    upsert_stocks,
)
from .supply_demand import fetch_stock_list, is_common_stock


def _has_recent_ss(con: Any, code: str, cutoff: str) -> bool:
    """이 종목에 ``cutoff`` **이후** 공매도 행이 있나 — 일일 수집의 resume 기준."""
    return fetchone(
        con, "SELECT 1 FROM short_selling WHERE code=? AND date>=? LIMIT 1",
        (code, cutoff),
    ) is not None


def _has_history_back_to(con: Any, code: str, depth_cutoff: str) -> bool:
    """이 종목의 공매도가 이미 ``depth_cutoff`` **이전**까지 닿아 있나.

    주간 깊이 백필(`weekly_history_backfill`)의 스킵 기준이다. "최근 행이
    있나"(`_has_recent_ss`)로는 깊이를 판정할 수 없다 — 매일 수집이 최근
    구간을 채워두므로 전 종목이 항상 참이 되어 스킵이 무의미해진다.

    **왜 깊이로 판정해야 하는가.** 백필은 키움 TR 상한(공매도 ~336일)까지
    긁는 게 목적인데, 실측 2,545 종목 중 2,463개(96.8%)가 이미 330일 이전까지
    확보돼 있다. 실제로 얕은 건 82종목뿐인데 매주 5,090 요청(49분)을 전부
    다시 보내고 ~107만 행을 같은 값으로 덮어썼다.
    """
    return fetchone(
        con, "SELECT 1 FROM short_selling WHERE code=? AND date<=? LIMIT 1",
        (code, depth_cutoff),
    ) is not None


def _build_ss_records(code: str, resp: dict, cutoff: str) -> list[tuple]:
    records = []
    for row in resp.get("shrts_trnsn", []) or []:
        dt = row.get("dt", "")
        if dt < cutoff:
            continue
        records.append((
            code,
            dt,
            abs(to_int(row.get("close_pric"))),
            to_int(row.get("trde_qty")),
            to_int(row.get("shrts_qty")),        # 당일 공매도 수량
            to_int(row.get("ovr_shrts_qty")),    # 공매도 잔고 수량
            to_float(row.get("trde_wght")),      # 공매도 비중 %
            to_int(row.get("shrts_avg_pric")),   # 공매도 평균가
            to_int(row.get("shrts_trde_prica")), # 공매도 거래대금
        ))
    return records


def _build_cb_records(code: str, resp: dict, cutoff: str) -> list[tuple]:
    records = []
    for row in resp.get("crd_trde_trend", []) or []:
        dt = row.get("dt", "")
        if dt < cutoff:
            continue
        records.append((
            code,
            dt,
            abs(to_int(row.get("cur_prc"))),
            to_int(row.get("new")),       # 신규
            to_int(row.get("rpya")),      # 상환
            to_int(row.get("remn")),      # 신용잔고 수량
            to_int(row.get("amt")),       # 신용잔고 금액
            to_float(row.get("remn_rt")), # 신용잔고율 %
            to_float(row.get("shr_rt")),  # 신용비율 %
        ))
    return records


# 종목별 즉시 upsert(~2,600회×2 DB 라운드트립) 대신 CHUNK_SIZE개 종목마다 배치
# upsert. 루프 종료 후 1회 upsert(메가배치)는 크래시 시 전체 진행분을 잃고
# 레코드 1건 오류가 전체를 롤백시키므로 대신 청크 단위 중간 커밋을 쓴다 —
# 크래시 손실을 최대 1청크(~2분) 분량으로 제한하면서 라운드트립을 크게 줄인다.
_CHUNK_SIZE = 100


def collect(
    api: KiwoomAPI,
    con: sqlite3.Connection,
    stocks: list[dict],
    *,
    days: int = 100,
    resume: bool = False,
    resume_depth: int = 0,
    progress_every: int = 50,
) -> dict[str, int]:
    """공매도+신용잔고를 종목별로 수집한다.

    Args:
        days: 받아서 **쓰는** 창의 길이. 기본 100 은 키움이 주는 전량에 가깝다.
        resume: `cutoff` 이후 행이 있는 종목을 건너뛴다(중단된 런 재개용).
        resume_depth: 0 이 아니면 **깊이 기반 스킵**을 켠다 — 이미 `오늘 -
            resume_depth일` 이전까지 이력이 닿아 있는 종목을 건너뛴다. 주간
            깊이 백필 전용이다. `resume` 과 달리 "얼마나 과거까지 있나"를 보므로,
            매일 수집이 최근 구간을 채워둔 상태에서도 의미 있게 걸러진다.
    """
    today = time.strftime("%Y%m%d")
    cutoff = time.strftime("%Y%m%d", time.localtime(time.time() - days * 86400))
    depth_cutoff = (
        time.strftime("%Y%m%d", time.localtime(time.time() - resume_depth * 86400))
        if resume_depth > 0
        else ""
    )
    start_dt = cutoff
    stats = {"done": 0, "skipped": 0, "failed": 0, "ss_rows": 0, "cb_rows": 0}
    started = time.monotonic()
    ss_buffer: list[tuple] = []
    cb_buffer: list[tuple] = []

    def flush() -> None:
        if ss_buffer:
            stats["ss_rows"] += upsert_short_selling(con, ss_buffer)
            ss_buffer.clear()
        if cb_buffer:
            stats["cb_rows"] += upsert_credit_balance(con, cb_buffer)
            cb_buffer.clear()

    for i, stock in enumerate(stocks, 1):
        code = stock["code"]
        if resume and _has_recent_ss(con, code, cutoff):
            stats["skipped"] += 1
            continue
        if depth_cutoff and _has_history_back_to(con, code, depth_cutoff):
            stats["skipped"] += 1
            continue
        try:
            # 공매도 (ka10014) — independent TR bucket
            ss_resp = api.short_selling.short_selling_trend(
                stk_cd=code, tm_tp="1", strt_dt=start_dt, end_dt=today
            )
            ss_buffer.extend(_build_ss_records(code, ss_resp, cutoff))

            # 신용잔고 (ka10013) — independent TR bucket
            cb_resp = api.stock_info.credit_trading_trend(
                stk_cd=code, dt="0", qry_tp="1"
            )
            cb_buffer.extend(_build_cb_records(code, cb_resp, cutoff))
            stats["done"] += 1
        except KiwoomAPIError as e:
            stats["failed"] += 1
            print(f"  ⚠️ {code} {stock['name']}: rc={e.code} {e.message[:50]}")
        except Exception as e:  # noqa: BLE001
            stats["failed"] += 1
            print(f"  💥 {code} {stock['name']}: {type(e).__name__}: {e}")

        if stats["done"] % _CHUNK_SIZE == 0:
            flush()

        if i % progress_every == 0 or i == len(stocks):
            elapsed = time.monotonic() - started
            rate = i / elapsed if elapsed else 0
            eta = (len(stocks) - i) / rate / 60 if rate else 0
            print(
                f"  [{i}/{len(stocks)}] done={stats['done']} skip={stats['skipped']} "
                f"fail={stats['failed']} | 공매도 {stats['ss_rows']:,} / "
                f"신용 {stats['cb_rows']:,} | {rate:.1f} stk/s | ETA {eta:.1f}m"
            )
    flush()
    return stats


def main() -> int:
    parser = argparse.ArgumentParser(description="키움 공매도+신용잔고 SQLite 수집기")
    parser.add_argument("--prod", action="store_true", help="실서버 사용 (기본: 모의)")
    parser.add_argument("--market", choices=["kospi", "kosdaq", "all"], default="all")
    parser.add_argument("--days", type=int, default=100, help="최근 N일 (기본 100)")
    parser.add_argument("--limit", type=int, default=0, help="앞에서 N종목만 (테스트)")
    parser.add_argument("--db", default=str(default_db_path()))
    parser.add_argument("--resume", action="store_true", help="최근 데이터 있는 종목 건너뜀")
    parser.add_argument(
        "--resume-depth", type=int, default=0,
        help="이미 최근 N일 이전까지 이력이 닿아 있는 종목은 건너뜀 "
             "(주간 깊이 백필용 — 0=끔). 공매도 TR 상한이 ~336일이므로 330 이 실용값이다.",
    )
    parser.add_argument(
        "--all-kinds", action="store_true",
        help="ETF/ETN/리츠/우선주 등 모두 포함 (기본: 보통주만)",
    )
    parser.add_argument("--rate", type=float, default=0.9, help="TR당 요청 속도(req/s)")
    args = parser.parse_args()

    con = connect(args.db)
    api = make_api(is_mock=not args.prod, rate_limit=args.rate, max_retries=5)

    markets = ["kospi", "kosdaq"] if args.market == "all" else [args.market]
    stocks = fetch_stock_list(api, markets)
    if not args.all_kinds:
        stocks = [s for s in stocks if is_common_stock(s)]
    if args.limit:
        stocks = stocks[: args.limit]

    upsert_stocks(con, stocks)
    server = "모의" if not args.prod else "실서버"
    print(f"🔌 {server} | 시장={args.market} | 종목 {len(stocks)}개 | 최근 {args.days}일")
    print(f"💾 {mask_dsn(args.db)}\n")

    stats = collect(
        api, con, stocks, days=args.days, resume=args.resume,
        resume_depth=args.resume_depth,
    )

    api.close()
    con.close()
    print(
        f"\n✅ 완료: done={stats['done']} skip={stats['skipped']} "
        f"fail={stats['failed']} | 공매도 {stats['ss_rows']:,}행 "
        f"신용 {stats['cb_rows']:,}행"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
