"""장전 뉴스/공시 판단 — 08:45 KST, 09:00 시가 전 시가 진입 판단용.

daily_news와 같은 수집 태스크(news_toss.py/news_dart.py, 자연키 upsert라
몇 번을 다시 돌아도 안전)를 재사용하고, 그 뒤에 news_judge.py로 판단한다.
daily_news(10:05·16:05)가 나중에 같은 항목을 다시 봐도 news_judgments의
(source_type, source_id, ticker, prompt_version) upsert 키 덕에 중복
판단되지 않는다.

만쥬 인터뷰(scalp-it 레포 docs/research/manju/46-manju-answers.md §3)의
"08:50까지 테마 매핑 완성" 워크플로우와 맞추려고 08:45로 잡았다 — 설계 근거는
docs/superpowers/specs/2026-09-06-news-llm-judgments-design.md 참고.
"""

from __future__ import annotations

import sys

import pendulum
from airflow.decorators import dag, task

from _common import DEFAULT_TASK_KW, claude_env, dart_env, run_collector, timescale_dsn


@dag(
    dag_id="premarket_news_judgment",
    schedule="45 8 * * 1-5",  # 평일 08:45 KST
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["kr-quant", "collection", "news", "llm"],
)
def premarket_news_judgment():

    @task(**DEFAULT_TASK_KW)
    def collect_toss_news() -> None:
        run_collector([
            sys.executable, "-m", "collectors.news_toss",
            "--db", timescale_dsn(),
        ])

    @task(**DEFAULT_TASK_KW)
    def collect_dart_disclosures() -> None:
        run_collector([
            sys.executable, "-m", "collectors.news_dart",
            "--db", timescale_dsn(),
        ], env=dart_env())

    @task(**DEFAULT_TASK_KW)
    def judge_news() -> None:
        run_collector([
            sys.executable, "-m", "collectors.news_judge",
            "--db", timescale_dsn(),
        ], env=claude_env())

    [collect_toss_news(), collect_dart_disclosures()] >> judge_news()


premarket_news_judgment()
