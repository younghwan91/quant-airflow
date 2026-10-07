"""새벽 확정치 재수집 — 전 거래일의 일봉·수급·업종지수를 **확정값으로 덮고** 조정주가를 다시 만든다.

16:00 ``daily_collection`` 이 받는 **그날(T) 값은 잠정치다.** 2026-10-08 실측(새벽 05:40 키움 재조회·
네이버 일봉과 대조, 40종목):

    daily_bars.close   09-14~10-06 은 네이버(KRX 공식 종가)와 40/40 일치, **10-07(T) 은 10/40**
                       예) 005930 DB 270,000 · 확정 269,000 / 거래량 15,964,739 · 확정 16,395,504
    supply_demand      10-06 은 확정값과 전 컬럼 일치, **10-07(T) 은 개인·외인·기관 전부 다름**
                       예) 005930 개인 892,207 → 확정 957,691

키움은 16:00 시점의 오늘 봉에 시간외 체결이 반영되는 중인 값을 주고, 투자자별 수급도 장 마감 직후엔
잠정 집계다. 다음 날 16:00 런이 ``--daily-days 15`` 창으로 다시 덮어 고쳐지므로 **이력은 맞다** —
틀린 건 "T 의 저녁부터 T+1 16:00 까지" 이 테이블을 읽는 쪽이다. 저녁에 신호를 만드는 스윙 연구
(swing-it)와 16:55 의 조정주가 재계산이 정확히 그 구간에서 읽는다.

(같은 날 다른 세션이 "09-14 부터 daily_bars.close 가 NXT 20:00 종가로 오염됐다" 고 적었는데 —
daytrade-it docs/research/2026-10-05-cross-repo-synthesis.md — 그건 비교 기준으로 쓴
``minute_bars`` 15:30 봉이 공식 종가와 다른 것이었다. daily_bars 는 T 하루만 잠정이다.)

**왜 06:30 인가.** 05:40 재조회에서 이미 확정값이었다. 장 시작 전이어야 하고, 키움 앱키 1세션 제약
때문에 다른 키움 사용자와 겹치면 안 된다 — 07:50 pull-all(키움 안 씀) 전에 끝나고 08:45 개장 전
점검·08:55 감지기 로그인보다 한참 앞이다. 실측 ~48분(daily_collection 과 같은 요청 수) + 조정주가
6분 23초 → 07:30 전후 종료.

**왜 화~토인가.** 월~금 거래일의 다음 날 아침이다. 창은 달력 3일이라 휴장일 다음 날에도 직전
거래일을 덮는다. 토요일 런은 금요일 치를 확정하고, 그 뒤 10:40 ``weekly_price_adjust`` 와도 안 겹친다.

쓰는 창은 3일 — 압축 경계(30일) 안이다(CLAUDE.md §3).
"""

from __future__ import annotations

import os
import sys

import pendulum
from airflow.decorators import dag, task

from _common import DAG_DEFAULT_ARGS, DEFAULT_TASK_KW, kiwoom_env, run_collector, timescale_dsn


@dag(
    dag_id="daily_finalize",
    default_args=DAG_DEFAULT_ARGS,
    schedule="30 6 * * 2-6",  # 화~토 06:30 KST — 전 거래일 확정치(위 docstring)
    start_date=pendulum.datetime(2026, 10, 8, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["kr-quant", "collection", "finalize"],
)
def daily_finalize():

    @task(**DEFAULT_TASK_KW)
    def finalize_sector() -> None:
        # daily_collection 과 같은 순서·같은 이유: 71초짜리를 먼저 돌려 자격증명이 깨졌으면 일찍 죽는다.
        # 두 키움 태스크는 직렬이어야 한다 — 병렬이면 나중 로그인이 앞 토큰을 무효화한다(8005).
        run_collector([
            sys.executable, "-m", "collectors.sector_index",
            "--prod", "--days", "3", "--db", timescale_dsn(),
        ], env=kiwoom_env())

    @task(**DEFAULT_TASK_KW)
    def finalize_both() -> None:
        run_collector([
            sys.executable, "-m", "collectors.combined",
            "--market", "all", "--prod", "--rate", "0.9",
            "--daily-days", "3", "--sd-days", "3",
            "--db", timescale_dsn(),
        ], env=kiwoom_env())

    @task(**DEFAULT_TASK_KW)
    def rebuild_adjusted() -> None:
        # daily_price_adjust 와 같은 커맨드. 그쪽(16:55)은 T 를 잠정치로 만들었으므로 여기서 다시 만든다.
        run_collector(
            [
                sys.executable, "-m", "swing_it.price_adjust",
                "--rebuild-db", "--db", timescale_dsn(),
            ],
            env={**os.environ, "PYTHONPATH": "/opt/swing-it/src"},
            cwd="/opt/swing-it",
        )

    finalize_sector() >> finalize_both() >> rebuild_adjusted()


daily_finalize()
