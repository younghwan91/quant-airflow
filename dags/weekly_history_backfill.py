"""주간 심층 백필 — 증분 DAG가 못 채우는 '과거 깊이'를 주기적으로 메운다.

``daily_collection``(daily_bars/supply_demand)은 ``combined --update``로 2.5년을
백필하지만, ``sector_index``와 ``short_credit``(short_selling/credit_balance)은
오랫동안 증분(``--days 10`` / 기본 100) 전용이라 시작일 이후만 쌓여 히스토리가
얕았다 — 2026-07-18 발견: sector_index 29일(06-08~), short_selling/credit_balance
82일(03-19~). daily_bars가 2.5~10년인 것과 대조적이었다.

원인은 "매일 증분"만 있고 "가끔 깊이 재수집(백필)"이 스케줄에 없었던 것.
이 DAG가 두 소스를 주기적으로 깊게 재수집해, **신규 상장 종목·신규 업종지수·
DB 리셋 이후에도 히스토리가 다시 비지 않게** 한다. 키움 TR이 주는 최대 깊이는
실측(2026-07-18):
- sector_index ``--days 0``: 업종당 ~600거래일(2.3년) 전체.
- short_credit ``--days 800``: 종목당 공매도 ~336일, 신용 ~100일(키움 제공 한계).
  credit_balance는 이 한계 탓에 백필로도 100일까지만 — 그 이상은 증분 축적뿐.

**idempotent:** 두 콜렉터 모두 (code, date) 단위 upsert(ON CONFLICT DO UPDATE)라
매주 전체를 다시 받아도 안전하다. 이미 있는 깊이는 같은 값으로 덮어써질 뿐이고,
얕았던 신규 종목만 실질적으로 채워진다.

**일요일 11:00 KST 이유:** 장이 안 서는 날 + 스택 가동 창(10:00~) 안. short_credit
전체 재수집이 ~1시간이라 평일 수집과 겹치지 않게 주말에 둔다. earnings_backfill
(일요일 10:00, DART)과는 1시간 분리 — 소스(키움 vs DART)가 달라 자원 경합은 없지만
실패 blast radius를 나눈다. 두 태스크는 같은 키움 TR 레이트리밋 버킷을 공유하므로
병렬이 아니라 순차로 둔다(sector 먼저, 빠름 → short_credit).
"""

from __future__ import annotations

import sys

import pendulum
from airflow.decorators import dag, task

from _common import DEFAULT_TASK_KW, kiwoom_env, run_collector, timescale_dsn


@dag(
    dag_id="weekly_history_backfill",
    schedule="0 11 * * 0",  # 일요일 11:00 KST — earnings_backfill(10:00)과 1시간 분리
    # start_date는 직전 일요일 인터벌 시작(07-12)보다 앞에 둬야 catchup=False에서
    # 이번 주 일요일(07-19)이 첫 런으로 잡힌다 — 07-18로 두면 한 주 밀려 07-26이 됐다.
    start_date=pendulum.datetime(2026, 7, 11, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["kr-quant", "collection", "backfill"],
)
def weekly_history_backfill():

    @task(**DEFAULT_TASK_KW)
    def backfill_sector() -> None:
        # --days 0: 키움 지수 TR이 주는 전체 히스토리(~600거래일)를 upsert.
        # 평일 daily_collection은 --days 10 증분만 받으므로 여기서 깊이를 채운다.
        run_collector([
            sys.executable, "-m", "collectors.sector_index",
            "--prod", "--days", "0", "--db", timescale_dsn(),
        ], env=kiwoom_env())

    @task(**DEFAULT_TASK_KW)
    def backfill_short_credit() -> None:
        # --days 800: 종목당 공매도 ~336일 / 신용 ~100일(키움 한계)까지 깊게 받는다.
        # daily_short_credit은 --days 10 증분 — 신규 상장 종목의 과거는 이 백필로 채운다.
        #
        # --resume-depth 330: **이미 깊은 종목은 건너뛴다.** 이게 없을 때
        # 이 태스크는 6주 연속 49~50분으로 붙박이였다 — 일이 줄지 않는다는 건
        # 구멍만 메우는 구조가 아니라는 뜻이다. 실측으로 2,545 종목 중
        # 2,463개(96.8%)가 이미 330일 이전까지 확보돼 있고 실제로 얕은 건
        # 82종목뿐인데, 매주 5,090 요청을 전부 다시 보내고 ~107만 행을 같은
        # 값으로 덮어썼다.
        #
        # 330 인 이유: 공매도 TR 상한이 ~336일이라 그 언저리가 "받을 수 있는
        # 만큼 다 받은 상태"다. 800 을 기준으로 삼으면 어떤 종목도 만족할 수
        # 없어 스킵이 영원히 안 걸린다.
        run_collector([
            sys.executable, "-m", "collectors.short_credit",
            "--market", "all", "--prod", "--days", "800",
            "--resume-depth", "330",
            "--db", timescale_dsn(),
        ], env=kiwoom_env())

    backfill_sector() >> backfill_short_credit()


weekly_history_backfill()
