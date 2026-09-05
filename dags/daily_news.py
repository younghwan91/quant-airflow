"""뉴스·공시 수집 → TimescaleDB 직접 저장 (news_articles/news_article_tickers, disclosures).

krx-news-client(pip, 서버 아님 — kiwoom-client와 같은 클라이언트 라이브러리)가
호출 시점에 소스를 직접 불러 정규화된 결과를 돌려준다.

- 토스 뉴스: 최신 스냅샷(피드 3개 합쳐 최대 50+20+10건)만 주고 페이지네이션이
  없어, 창 사이 공백이 길수록 그 사이 쌓인 기사가 밀려 유실될 수 있다 — 오전
  10:05/저녁 16:05 두 창 모두에 걸어 공백을 줄인다(기존 오전·저녁 가동 창 안이라
  새 창을 열 필요는 없다, docs/operations.md 참고). 트레이딩 판단에 분 단위
  최신성이 필요해지면 이 배치 방식 자체를 재검토해야 한다. 공개 엔드포인트라
  앱키가 없다.
- DART 공시: dart_env()로 재무제표 수집(dart_earnings/dart_shares)과 같은 키
  풀을 주입한다 — collectors/news_dart.py가 한 키가 일한도면 다음 키로 순환한다.
"""

from __future__ import annotations

import sys

import pendulum
from airflow.decorators import dag, task

from _common import DEFAULT_TASK_KW, dart_env, run_collector, timescale_dsn


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

    @task(**DEFAULT_TASK_KW)
    def collect_dart_disclosures() -> None:
        run_collector([
            sys.executable, "-m", "collectors.news_dart",
            "--db", timescale_dsn(),
        ], env=dart_env())

    collect_toss_news()
    collect_dart_disclosures()


daily_news()
