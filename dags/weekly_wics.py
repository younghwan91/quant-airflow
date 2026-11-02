"""WICS 섹터·산업군 주간 스냅샷 → wics_membership (collectors/wics.py, migrations/020) — 화요일 18:25.

2026-10-12 백필(2019~2026-10-06) 뒤 **정기 수집이 없었다** — 10-13·10-20·10-27 세 주가 비어 swing-it 전진 팔
(pead_rank_F1)의 업종 마스크가 10-06 스냅숏을 이어 쓰고 그 뒤 상장 종목은 유니버스에 못 들어왔다(2026-11-02 발견).

수집기는 ``daily_bars`` 005930 거래일로 매주 첫 거래일을 고르고 이미 있는 날짜는 건너뛴다 — 그래서 매주 한 번
돌면 빠진 주까지 이어 받는다. 그 거래일에 섹터 응답이 통째로 비면 예외다(CLAUDE.md §5).

화요일 18:25 인 이유: 주의 첫 거래일이 월요일이면 이미 있고, 월요일이 휴장이면 화요일 일봉(16:00
daily_collection)이 들어온 뒤다. 수·목까지 휴장인 주는 다음 화요일 런이 받는다. wiseindex 는 키움이 아니라
키움 토큰 간격 규약과 무관하다. 한 스냅숏 ≈ 섹터 10 + 중분류 + WI26 요청 × 0.3초 ≈ 30초.
"""

from __future__ import annotations

import sys

import pendulum
from airflow.decorators import dag, task

from _common import DAG_DEFAULT_ARGS, DEFAULT_TASK_KW, run_collector, timescale_dsn


@dag(
    dag_id="weekly_wics",
    default_args=DAG_DEFAULT_ARGS,
    schedule="25 18 * * 2",  # 화요일 18:25 KST
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["kr-quant", "collection", "wics"],
)
def weekly_wics():

    @task(**DEFAULT_TASK_KW)
    def collect_wics() -> None:
        run_collector([
            sys.executable, "-m", "collectors.wics",
            "--db", timescale_dsn(),
            "--since", "2026-10-01",  # 백필은 끝났다 — 최근 창만 보고 빠진 주를 이어 받는다
        ])

    collect_wics()


weekly_wics()
