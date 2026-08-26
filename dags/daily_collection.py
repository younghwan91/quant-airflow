"""Daily 일봉+수급+업종지수 수집 → TimescaleDB 직접 저장.

storage.py가 Postgres DSN을 받으면 TimescaleDB에 직접 upsert하므로(ON
CONFLICT DO UPDATE — sqlite의 INSERT OR REPLACE와 동일한 자연키 upsert),
별도 sync 스텝이 필요 없다. 신용잔고는 보통 T+1~2 지연 공시라 별도
DAG(daily_short_credit)로 분리했다.
"""

from __future__ import annotations

import sys

import pendulum
from airflow.decorators import dag, task

from _common import DEFAULT_TASK_KW, kiwoom_env, run_collector, timescale_dsn


@dag(
    dag_id="daily_collection",
    schedule="0 16 * * 1-5",  # 평일 16:00 KST — 장마감(15:30) 직후, 데이터 확정 대기 30분
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["kr-quant", "collection"],
)
def daily_collection():

    @task(**DEFAULT_TASK_KW)
    def collect_both() -> None:
        # --prod: 실데이터. 모의서버 기본값은 실제 시세/수급이 아님
        # (kr-quant/README.md 참고).
        # --daily-days/--sd-days: **쓰는 창을 자른다.** 기본값(일봉 0=전량)은
        # ka10081 이 주는 ~573봉을 매일 전 종목에 upsert 했다 — 하루 150만 행을
        # 쓰면서 실제 새 행은 2,626개다. daily_bars 는 청크 515개 중 514개가
        # 압축 상태라, 그 재기록의 대부분이 압축 세그먼트 해제→갱신→재압축이다
        # (pg_stat 실측: n_tup_ins 1,696,027 ≈ n_tup_del 1,693,401 = churn).
        #
        # 깊은 구멍은 여기서 메우지 않는다 — daily_collection_catchup 이
        # --update 로 낡은 종목만 골라 전량 재수집한다. 그게 그 DAG 의 존재
        # 이유이고, 여기서 매일 전 이력을 다시 쓸 이유가 없다.
        run_collector([
            sys.executable, "-m", "collectors.combined",
            "--market", "all", "--prod", "--rate", "0.9",
            "--daily-days", "15", "--sd-days", "15",
            "--db", timescale_dsn(),
        ], env=kiwoom_env())

    @task(**DEFAULT_TASK_KW)
    def collect_sector() -> None:
        # 별도 TR(ka20003/ka20006)이라 collect_both와 레이트리밋 버킷이 안
        # 겹침. TimescaleDB는 MVCC라 두 태스크가 동시에 써도 안전(sqlite와
        # 달리 단일 writer 제약 없음).
        #
        # **그런데 병렬로 두면 안 된다 — 토큰이 겹친다.** 두 태스크가 각각
        # 별도 프로세스로 `make_api(login=True)` 를 부르는데 앱키가 같아서,
        # 나중에 로그인한 쪽이 먼저 받은 토큰을 무효화한다:
        #
        #     KiwoomAPIError: [3] 인증에 실패했습니다[8005:Token이 유효하지 않습니다]
        #
        # 실측 40런 중 4런(10%)이 이렇게 3초 만에 죽고 retry_delay 10분을
        # 통째로 물었다(2026-07-16, 08-21, 08-25 등). 직렬화하면 로그인이
        # 겹칠 수가 없다. 비용은 71초뿐이다.
        run_collector([
            sys.executable, "-m", "collectors.sector_index",
            "--prod", "--days", "10", "--db", timescale_dsn(),
        ], env=kiwoom_env())

    # 순서가 sector 먼저인 이유: 71초짜리라 자격증명이 깨졌으면 **48분을 태우기
    # 전에** 알 수 있다. 어차피 저녁 창은 collect_both 가 끝나야 닫히므로 총
    # 소요는 어느 순서든 같다.
    collect_sector() >> collect_both()


daily_collection()
