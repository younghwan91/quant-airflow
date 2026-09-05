"""토스증권 뉴스 수집 → TimescaleDB 직접 저장 (news_articles/news_article_tickers).

krx-news-client(pip, 서버 아님 — kiwoom-client와 같은 클라이언트 라이브러리)가
호출 시점에 토스 API를 직접 불러 최신 뉴스 스냅샷(피드 3개 합쳐 최대
50+20+10건)을 돌려준다. Toss가 페이지네이션 없이 "최근 것"만 주므로, 창 사이
공백이 길수록 그 사이 쌓인 기사가 스냅샷 밖으로 밀려 유실될 수 있다 — 오전
10:05/저녁 16:05 두 창 모두에 걸어 공백을 줄인다(기존 오전·저녁 가동 창
안이라 새 창을 열 필요는 없다, docs/operations.md 참고). 트레이딩 판단에
분 단위 최신성이 필요해지면 이 배치 방식 자체를 재검토해야 한다.

kiwoom_env()/dart_env() 같은 자격증명 주입이 필요 없다 — 토스 쪽은 공개
엔드포인트라 앱키가 없다.
"""

from __future__ import annotations

import sys

import pendulum
from airflow.decorators import dag, task

from _common import DEFAULT_TASK_KW, run_collector, timescale_dsn


@dag(
    dag_id="daily_news",
    schedule="5 10,16 * * 1-5",  # 평일 10:05 · 16:05 KST — 기존 오전/저녁 창 안
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["kr-quant", "collection", "news"],
)
def daily_news():

    @task(**DEFAULT_TASK_KW)
    def collect_toss_news() -> None:
        run_collector([
            sys.executable, "-m", "collectors.news_toss",
            "--db", timescale_dsn(),
        ])

    collect_toss_news()


daily_news()
