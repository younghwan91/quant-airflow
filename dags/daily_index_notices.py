"""KRX 지수운영공지 → index_notices·index_changes·index_constituents (migrations/021).

대표지수(KOSPI 200·KOSDAQ 150·KRX 300) 정기변경은 6·12월 동시만기일 다음 날 시행되고, 공지는
그 16~24일 전에 index.krx.co.kr 지수운영공지 게시판에 엑셀과 함께 올라온다(2016~2026 실측).
swing-it 이 편입 전진 모의매매(발표 다음 거래일 종가 진입, 2026-12 변경부터)를 하므로 **발표 당일 밤**에
들어와야 한다 — 평일 매일 18:30(발표는 장 마감 뒤, 2016~2026 실측). 2026-10-12 사용자 승인으로 주 1회에서 바꿨다. 수시변경(특례편입·분할·합병)도 원문(index_notices)으로 같이 쌓인다.

이 게시판은 data.krx.co.kr 와 달리 로그인이 필요 없다. 수집기는 이미 있는 글을 건너뛰므로
평소엔 목록 페이지(~75쪽 × 0.4초)와 새 글 몇 개만 부른다. 키움을 쓰지 않아 다른 태스크와 겹쳐도 된다.
"""

from __future__ import annotations

import sys

import pendulum
from airflow.decorators import dag, task

from _common import DAG_DEFAULT_ARGS, DEFAULT_TASK_KW, run_collector, timescale_dsn


@dag(
    dag_id="daily_index_notices",
    default_args=DAG_DEFAULT_ARGS,
    # 평일 18:30 KST — 키움 무관(KRX 지수 사이트, 1회 ~30초). 발표 당일 안에 들어온다.
    schedule="30 18 * * 1-5",
    # catchup=False 라도 첫 구간이 끝나야 첫 런이 뜬다(CLAUDE.md §4) — 10-12 로 두었다가 생성 당일 런이 빠졌다.
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["kr-quant", "index", "reference", "daily"],
)
def daily_index_notices():

    @task(**DEFAULT_TASK_KW)
    def collect_index_notices() -> None:
        run_collector([
            sys.executable, "-m", "collectors.krx_index_changes",
            "--db", timescale_dsn(),
        ])

    collect_index_notices()


daily_index_notices()
