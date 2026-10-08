"""토스 종목별 뉴스 피드 → TimescaleDB (news_articles + news_company_feed).

전 거래일 거래대금 상위 300종목(069500 제외)의 토스 종목별 뉴스를, 실패 없이 끝난
마지막 런의 날짜부터 다시 받는다(collectors/news_toss_company.py, migrations/015).
daily_news 의 하이라이트 피드는 최신 수십 건뿐이라 종목별 "그날 기사 전부" 를 못
주고, 스윙 연구·daytrade-it 전진 홀드아웃이 그걸 필요로 한다.

이 DAG 가 대체하는 것: daytrade-it-rlnews 분리 워크트리(아카이브 태그에 고정)의
비관리 크론(평일 16:30, collect_toss_daily.sh) — 기사를 파일에만 쌓았다. 둘이
같은 기사를 받는지 며칠 나란히 확인한 뒤 크론을 지운다(그때까지 토스에 요청이
겹치지 않게 크론 종료(~16:45) 뒤인 17:10 에 돈다).

17:10 인 이유: 기사 하루치는 장 마감 뒤에야 다 나온다(daytrade-it 은 15:20 이후
시작한 런만 그날을 "완결" 로 쳤다). 토스는 키움이 아니라 키움 토큰 간격 규약과
무관하다. 실측 런 시간 ~14분(크론 원장, 300~350종목).
"""

from __future__ import annotations

import sys

import pendulum
from airflow.decorators import dag, task

from _common import DAG_DEFAULT_ARGS, DEFAULT_TASK_KW, run_collector, timescale_dsn


@dag(
    dag_id="daily_toss_company_news",
    default_args=DAG_DEFAULT_ARGS,
    schedule="10 17 * * 1-5",  # 평일 17:10 KST
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["kr-quant", "collection", "news"],
)
def daily_toss_company_news():

    @task(**DEFAULT_TASK_KW)
    def collect_company_news() -> None:
        run_collector([
            sys.executable, "-m", "collectors.news_toss_company",
            "--db", timescale_dsn(),
        ])

    collect_company_news()


daily_toss_company_news()
