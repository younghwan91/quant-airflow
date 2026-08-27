"""주간 상장주식수(listed_shares) 수집 → TimescaleDB 직접 저장.

ka10001은 현재 시점 스냅샷만 반환하고(과거 이력 백필 불가), 상장주식수는
분할/자사주 등 기업행위가 없는 한 자주 바뀌지 않으므로 매일이 아닌 주 1회
(**화요일**)만 수집한다. 컨테이너 기동(10:00) 10분 후.

**10:05 와의 5분 간격은 장식이 아니라 토큰 가드다 — 줄이면 안 된다.**
화요일 아침에는 daily_short_credit(10:00) · daily_collection_catchup(10:05) ·
이 DAG(10:10) 셋이 동시에 돈다. TR 버킷이 전부 달라 겹치는 것 자체는 공짜지만
(아래 schedule 주석 참고), **셋 다 같은 KIWOOM_APP_KEY 로 각각 로그인한다.**
로그인이 같은 순간에 겹치면 나중 쪽이 앞 토큰을 무효화한다
(`8005:Token이 유효하지 않습니다`) — daily_collection 이 실측 40런 중 4런을 그렇게
잃었다. 지금 이 셋에서 8005 가 안 나는 건 이 5분 간격이 막아주고 있어서다.
"시작 시각을 예쁘게 맞추자"고 셋을 같은 분에 모으면 그 사고가 아침 창에서
재현된다.

월요일이 아니라 화요일인 이유는 아래 ``schedule`` 옆 주석에 있다.

storage.py가 Postgres DSN을 받으면 TimescaleDB에 직접 upsert하므로 별도
sync 스텝이 필요 없다.
"""

from __future__ import annotations

import sys

import pendulum
from airflow.decorators import dag, task

from _common import DEFAULT_TASK_KW, kiwoom_env, run_collector, timescale_dsn


@dag(
    dag_id="weekly_listed_shares",
    # 매주 **화요일** 10:10 KST — 컨테이너 기동(10:00) 10분 후.
    #
    # 월요일이 아니라 화요일인 이유: 이 태스크는 종목당 ka10001 한 번이라
    # 2,628 요청 / 실측 48분 38초가 통째로 레이트리밋 대기다. 화~토 10:00 에는
    # `daily_short_credit`(ka10014/ka10013, 실측 48분 40초)이 이미 스택을 잡고
    # 있고 **TR 버킷이 달라 서로 throttle 하지 않는다** — 겹쳐 돌리면 두 작업이
    # 한 작업 시간에 끝난다. 월요일에 두면 그 48분이 월요일 오전 창을 혼자
    # 결정해 주당 48분이 추가된다.
    schedule="10 10 * * 2",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["kr-quant", "collection"],
)
def weekly_listed_shares():

    @task(**DEFAULT_TASK_KW)
    def collect_listed_shares() -> None:
        run_collector([
            sys.executable, "-m", "collectors.listed_shares",
            "--market", "all", "--prod", "--db", timescale_dsn(),
        ], env=kiwoom_env())

    collect_listed_shares()


weekly_listed_shares()
