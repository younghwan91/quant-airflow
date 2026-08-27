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

from .kiwoom_cli import add_common_args, build_universe, open_session, print_banner
from .storage import (
    date_days_ago,
    days_ago,
    fetchall,
    progress_line,
    to_float,
    to_int,
    upsert_credit_balance,
    upsert_short_selling,
)


def codes_with_history_back_to(con: Any, depth_cutoff: str) -> set[str]:
    """공매도 이력이 이미 ``depth_cutoff`` **이전**까지 닿아 있는 코드 집합.

    주간 깊이 백필(`weekly_history_backfill`)의 스킵 기준이다. "최근 행이
    있나"(:func:`codes_with_rows_since`)로는 깊이를 판정할 수 없다 — 매일 수집이
    최근 구간을 채워두므로 전 종목이 항상 참이 되어 스킵이 무의미해진다.

    **왜 깊이로 판정해야 하는가.** 백필은 키움 TR 상한(공매도 ~336일)까지 긁는 게
    목적인데, 실측 2,545 종목 중 2,463개(96.8%)가 이미 330일 이전까지 확보돼 있다.
    실제로 얕은 건 82종목뿐인데 매주 5,090 요청(49분)을 전부 다시 보내고 ~107만
    행을 같은 값으로 덮어썼다.

    **집합으로 받는 이유.** 종목마다 EXISTS 를 부르면 2,545 왕복인데, 이 조건은
    **오래된** 청크를 봐야 해서 하이퍼테이블 대부분을 가로지른다 —
    `daily_bars.codes_current_as_of` 가 실측한 건당 845ms 짜리 모양 그대로다.
    그러면 API 를 한 번도 안 불러도 스킵 판정에만 ~35분이 든다. 집합 하나면 1회다.
    """
    return {r[0] for r in fetchall(
        con, "SELECT DISTINCT code FROM short_selling WHERE date<=?", (depth_cutoff,))}


def codes_with_rows_since(con: Any, cutoff: str) -> set[str]:
    """``cutoff`` 이후 공매도 행이 있는 코드 집합 — ``--resume`` 의 대량 판정."""
    return {r[0] for r in fetchall(
        con, "SELECT DISTINCT code FROM short_selling WHERE date>=?", (cutoff,))}


#: 미확정 세션 거부의 근거. 공매도(ka10014)·신용잔고(ka10013) 는 둘 다 거래소가
#: **T+1 이후**에 확정 공시한다 — `daily_short_credit` 이 화~토 10:00 에 도는 이유가
#: 그것이다(그 DAG docstring 참고). 따라서 "오늘 날짜" 행은 정의상 확정값이 아니다.
#:
#: **왜 가드가 필요한가 — ka10013 은 그 미확정 행을 빈손으로 안 준다.** 0 으로 채운
#: 스텁을 주고, 가격 자리에는 조회 시점의 장중 가격을 넣는다. 실측(2026-08-27):
#:
#:     date        rows   전부 0(잔고=신규=상환=0)
#:     2026-08-26  2,627  440   (17% — 실제로 신용잔고 없는 종목들)
#:     2026-08-27  2,627  2,627 (100% — 전부 스텁)
#:
#:     credit_balance.close  vs  daily_bars.close(확정)
#:     005930  265,000           266,000
#:     000660  1,710,000         1,730,000
#:
#: 그대로 적재하면 ~24시간 동안 **전 종목 신용잔고가 0으로 보인다.** 다음날
#: `--days 10` 창이 덮어써 자가치유되지만, 그 사이 이 테이블을 읽는 쪽은 "신용잔고가
#: 0으로 붕괴했다"를 읽는다. `daily_bars` 의 진행 중 캔들(_SESSION_CLOSE_HHMM)과
#: 같은 계열이고, 그쪽엔 있는 상한이 여기엔 없었다.
#:
#: ka10014 는 애초에 T+0 행을 안 주므로 이 가드가 no-op 이지만, 두 빌더에 같은
#: 규약을 두어 "미확정 세션은 저장하지 않는다"를 한 곳에서 읽히게 한다.
def _is_settled(dt: str, today: str) -> bool:
    """``dt`` 가 확정된 세션인가 — 오늘(미확정) 이전이어야 한다."""
    return bool(dt) and dt < today


def _build_ss_records(code: str, resp: dict, cutoff: str, today: str) -> list[tuple]:
    records = []
    for row in resp.get("shrts_trnsn", []) or []:
        dt = row.get("dt", "")
        if dt < cutoff or not _is_settled(dt, today):
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


def _build_cb_records(code: str, resp: dict, cutoff: str, today: str) -> list[tuple]:
    records = []
    for row in resp.get("crd_trde_trend", []) or []:
        dt = row.get("dt", "")
        if dt < cutoff or not _is_settled(dt, today):
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
    # date_days_ago(=항상 날짜): 원래 이 자리는 days>0 가드가 없었다. days=0 이면
    # 오늘이고, 그게 strt_dt 로도 나간다 — 빈 문자열로 바꾸면 창이 풀린다.
    cutoff = date_days_ago(days)
    depth_cutoff = days_ago(resume_depth)
    start_dt = cutoff
    # 스킵 판정을 **한 번에** 받아둔다 — 종목별 EXISTS 는 건당 왕복이고, 특히 깊이
    # 조건은 오래된 청크를 가로질러 건당 ~845ms 다(codes_with_history_back_to 참고).
    resume_codes = codes_with_rows_since(con, cutoff) if resume else set()
    deep_codes = codes_with_history_back_to(con, depth_cutoff) if depth_cutoff else set()
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
        if code in resume_codes or code in deep_codes:
            stats["skipped"] += 1
            continue
        try:
            # 공매도 (ka10014) — independent TR bucket
            ss_resp = api.short_selling.short_selling_trend(
                stk_cd=code, tm_tp="1", strt_dt=start_dt, end_dt=today
            )
            ss_buffer.extend(_build_ss_records(code, ss_resp, cutoff, today))

            # 신용잔고 (ka10013) — independent TR bucket
            cb_resp = api.stock_info.credit_trading_trend(
                stk_cd=code, dt="0", qry_tp="1"
            )
            cb_buffer.extend(_build_cb_records(code, cb_resp, cutoff, today))
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
            print(progress_line(
                i, len(stocks), started, stats,
                f"공매도 {stats['ss_rows']:,} / 신용 {stats['cb_rows']:,}"))
    flush()
    return stats


def main() -> int:
    parser = add_common_args(
        argparse.ArgumentParser(description="키움 공매도+신용잔고 SQLite 수집기"))
    parser.add_argument("--days", type=int, default=100, help="최근 N일 (기본 100)")
    parser.add_argument("--resume", action="store_true", help="최근 데이터 있는 종목 건너뜀")
    parser.add_argument(
        "--resume-depth", type=int, default=0,
        help="이미 최근 N일 이전까지 이력이 닿아 있는 종목은 건너뜀 "
             "(주간 깊이 백필용 — 0=끔). 공매도 TR 상한이 ~336일이므로 330 이 실용값이다.",
    )
    args = parser.parse_args()

    con, api = open_session(args)
    stocks = build_universe(api, con, args)
    print_banner(args, stocks, f"최근 {args.days}일")

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
