"""네이버 증권사 리포트 일일 증분 → naver_research_reports (032·035, collectors/naver_mstock.py).

전 종목 리포트 목록(``/api/research/company``, 최신순)을 최근 10일치만 넘기며 새 research_id 의 상세(의견·목표가)를
받는다 — 종목별 목록 3,700회를 도는 백필(``naver_mstock research``)과 달리 목록 몇 페이지 + 새 리포트 수(하루 ~100).
10일인 이유: 네이버가 늦게 올리는 리포트·연휴를 덮는다(이미 있는 id 는 상세를 다시 받지 않아 비용은 목록 몇 장).

05:40 인 이유: swing-it 전진 팔(leader_M_ana)이 매일 07:43 에 전날 리포트까지 읽는다 — 06:30 daily_finalize 전에
끝낸다. 리포트는 주말에도 가끔 올라와 매일 돈다. 네이버라 키움 토큰 간격 규약과 무관하다.
새 리포트 0건(휴일)은 정상이지만 목록 자체가 비면 예외(CLAUDE.md §5).
"""

from __future__ import annotations

import sys

import pendulum
from airflow.decorators import dag, task

from _common import DAG_DEFAULT_ARGS, DEFAULT_TASK_KW, run_collector, timescale_dsn


@dag(
    dag_id="daily_naver_research",
    default_args=DAG_DEFAULT_ARGS,
    schedule="40 5 * * *",  # 매일 05:40 KST
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["kr-quant", "collection", "naver"],
)
def daily_naver_research():

    @task(**DEFAULT_TASK_KW)
    def collect_research() -> None:
        run_collector([
            sys.executable, "-m", "collectors.naver_mstock", "research-daily",
            "--days", "10", "--db", timescale_dsn(),
        ])

    collect_research()


daily_naver_research()
