"""KRX 지수운영공지 → index_notices·index_changes·index_constituents (migrations/021).

대표지수(KOSPI 200·KOSDAQ 150·KRX 300) 정기변경은 6·12월 동시만기일 다음 날 시행되고, 공지는
그 16~24일 전에 index.krx.co.kr 지수운영공지 게시판에 엑셀과 함께 올라온다(2016~2026 실측).
swing-it 이 편입·편출 수급(공지일 +1 거래일 기준)을 쓰므로 공지가 올라온 주 안에 들어와야 한다 —
주 1회면 충분하다. 수시변경(특례편입·분할·합병)도 원문(index_notices)으로 같이 쌓인다.

이 게시판은 data.krx.co.kr 와 달리 로그인이 필요 없다. 수집기는 이미 있는 글을 건너뛰므로
평소엔 목록 페이지(~75쪽 × 0.4초)와 새 글 몇 개만 부른다. 키움을 쓰지 않아 다른 태스크와 겹쳐도 된다.
"""

from __future__ import annotations

import sys

import pendulum
from airflow.decorators import dag, task

from _common import DAG_DEFAULT_ARGS, DEFAULT_TASK_KW, run_collector, timescale_dsn


@dag(
    dag_id="weekly_index_notices",
    default_args=DAG_DEFAULT_ARGS,
    # 월요일 17:30 KST — 키움 무관(KRX 지수 사이트). 정기변경 공지는 시행 16~24일 전이라 주 1회로 충분하다.
    schedule="30 17 * * 1",
    start_date=pendulum.datetime(2026, 10, 12, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["kr-quant", "index", "reference"],
)
def weekly_index_notices():

    @task(**DEFAULT_TASK_KW)
    def collect_index_notices() -> None:
        run_collector([
            sys.executable, "-m", "collectors.krx_index_changes",
            "--db", timescale_dsn(),
        ])

    collect_index_notices()


weekly_index_notices()
