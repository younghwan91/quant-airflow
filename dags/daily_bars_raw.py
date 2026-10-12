"""무수정 주가 일봉(daily_bars_raw, migration 026) + 지정 ETF 일봉 — 평일 19:00.

``daily_bars`` 는 수정주가(upd_stkpc_tp=1)라 과거가 수집 시점 기준으로 소급 조정돼 있어 "그날 가격 × 그날
주식수" 시총이 틀린다(026 의 "왜"). 무수정(upd_stkpc_tp=0) 일봉을 따로 쌓는다. 최초 전량(2016-09-09~)은
2026-10-12 밤 수동 백필로 넣었고, 이 DAG 는 매일 **최근 5일을 다시 받아 덮는다** — 19:00 의 그날 값은
KRX 애프터마켓(16:00~20:00)이 진행 중인 잠정치라(daily_finalize docstring) 다음 날 런이 확정치로 고친다.
확정치도 2026-09-14 부터는 애프터마켓 포함 통합값이다 — 고가·저가·종가·거래량은 정규장 값이 아니고 시가만 정규장이다.

ETF(KODEX 200TR 278530 · KODEX 200 069500 · KODEX 코스닥150 229200)는 상장 종목 유니버스(보통주만)에
없어서 ``--codes`` 로 따로 받는다 — 수정·무수정 둘 다(swing-it 코어·위성 모의매매의 코어 다리).

**왜 19:00 인가.** 키움 앱키는 1세션이라 다른 키움 사용자와 겹치면 앞 토큰이 죽는다. 16:00 daily_collection
(~48분)·16:55 조정주가·17:45/18:30 kqc nightly 가 끝난 뒤이고, 다음 키움 사용자는 06:30 daily_finalize 다.
~2,630요청 × ~1초 ≈ 45분 → 19:50 전후 종료. 세 태스크는 직렬이다(병렬이면 나중 로그인이 앞 토큰을 무효화).
"""

from __future__ import annotations

import sys

import pendulum
from airflow.decorators import dag, task

from _common import DAG_DEFAULT_ARGS, DEFAULT_TASK_KW, kiwoom_env, run_collector, timescale_dsn

ETF_CODES = "278530,069500,229200"


@dag(
    dag_id="daily_bars_raw",
    default_args=DAG_DEFAULT_ARGS,
    schedule="0 19 * * 1-5",  # 평일 19:00 KST — 저녁 키움 사용자들이 끝난 뒤
    # 첫 런 10-13 19:00 — 첫 구간 [10-12 19:00, 10-13 19:00) 이 끝날 때(CLAUDE.md §4). 10-13 으로 두면
    # 10-14 에야 뜬다. 10-12 밤엔 수동 전량 백필이 키움을 쓰므로 그날 런은 없어야 한다(겹치면 토큰 충돌).
    start_date=pendulum.datetime(2026, 10, 12, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["kr-quant", "daily", "kiwoom"],
)
def daily_bars_raw():

    @task(**DEFAULT_TASK_KW)
    def collect_raw() -> None:
        run_collector([
            sys.executable, "-m", "collectors.daily_bars",
            "--market", "all", "--prod", "--rate", "0.9", "--raw", "--days", "5",
            "--db", timescale_dsn(),
        ], env=kiwoom_env())

    @task(**DEFAULT_TASK_KW)
    def collect_etf_raw() -> None:
        run_collector([
            sys.executable, "-m", "collectors.daily_bars",
            "--prod", "--rate", "0.9", "--raw", "--days", "5", "--codes", ETF_CODES,
            "--db", timescale_dsn(),
        ], env=kiwoom_env())

    @task(**DEFAULT_TASK_KW)
    def collect_etf_adjusted() -> None:
        # daily_bars(수정주가) 쪽 — daily_collection 과 같은 15일 재수집 창.
        run_collector([
            sys.executable, "-m", "collectors.daily_bars",
            "--prod", "--rate", "0.9", "--days", "15", "--codes", ETF_CODES,
            "--db", timescale_dsn(),
        ], env=kiwoom_env())

    collect_raw() >> collect_etf_raw() >> collect_etf_adjusted()


daily_bars_raw()
