"""신용잔고(short_credit) 일일 수집 → TimescaleDB 직접 저장.

daily_collection과 스케줄 분리 — 거래소 신용잔고 공시는 T+1~2 지연되는
경우가 잦아, 일봉/수급과 같은 시각에 돌리면 최신 데이터가 아직 안 나온
상태로 수집될 수 있다. 다음날 오전으로 스케줄을 늦춰 잡는다.

storage.py가 Postgres DSN을 받으면 TimescaleDB에 직접 upsert하므로 별도
sync 스텝이 필요 없다.
"""

from __future__ import annotations

import sys

from datetime import timedelta

import pendulum
from airflow.decorators import dag, task

from _common import kiwoom_env, run_collector, timescale_dsn


@dag(
    dag_id="daily_short_credit",
    schedule="0 10 * * 2-6",  # 화~토 10:00 KST (전날 공시 데이터 반영 이후)
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["kr-quant", "collection"],
)
def daily_short_credit():

    @task(retries=1, retry_delay=timedelta(minutes=10))
    def collect_short_credit() -> None:
        # --days 10: **쓰는 창을 자른다.** 기본 100 은 매 실행 334,183행을
        # 썼는데 그중 실제 새 행은 하루치 ~4,600 행 = 98.6% 가 같은 값 재기록이다.
        # short_selling·credit_balance 는 청크 대부분이 압축 상태라 그 재기록이
        # 압축 세그먼트 해제→갱신→재압축을 부른다(pg_stat 실측: 압축 청크에서
        # n_tup_ins ≈ n_tup_del 로 churn 이 그대로 보인다).
        #
        # 10일이면 연휴가 낀 주에도 여유가 있고, 런이 하루 실패해도 다음 런의
        # 창이 그 구멍을 덮는다 — 이 자가치유가 고정 창의 유일한 장점이라
        # 7일까지 줄이지는 않는다. 깊이는 weekly_history_backfill 의 몫이다.
        run_collector([
            sys.executable, "-m", "collectors.short_credit",
            "--market", "all", "--prod", "--days", "10", "--db", timescale_dsn(),
        ], env=kiwoom_env())

    collect_short_credit()


daily_short_credit()
