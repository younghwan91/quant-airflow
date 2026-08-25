"""daily_bars 기업행동(액면분할·무상증자) 백조정 → daily_bars_adjusted 테이블 재생성.

research/HANDOFF.md가 지적한 갭: `kr_quant.price_adjust.adjust_prices()`는
sepa_experiment.py 전략 하나에서만 호출되고 다른 여러 연구 스크립트는
daily_bars 원자료(미조정)를 직접 읽는다 — 분할이 가짜 −68% 손실로 잡혀
백테스트 절대수익률이 왜곡되는 정확히 그 버그(리더 시스템 CAGR +20.9%→
조정 +14.0%, GOAL 루프 54-59 진단)에 계속 노출된다.

이 DAG는 daily_bars_adjusted 테이블(PK: code,date)에 조정가를 채워서, 새
코드든 기존 연구 스크립트든 daily_bars 대신 이 테이블만 쓰면 자동으로
분할조정된 값을 받게 한다 — 원자료(daily_bars)는 그대로 보존.

**전체 재계산(매주) 이유:** back-adjust는 종목별 *전체* 이력을 봐야 정확하다
— 오늘 새로 감지된 분할이 그 종목의 과거 모든 조정값을 바꾼다. 그래서
증분이 아니라 매번 daily_bars 전체를 다시 읽어 재계산·upsert한다(자연키
(code,date) upsert라 기존 행은 덮어써짐). 분할은 드물어서(전체 이력 통틀어
~44건) 매일 돌릴 필요는 없고, daily_bars 규모(수백만 행)에서도 주간 배치로
충분히 저렴하다.

무인증(DB만), Kiwoom/DART 자격증명 불필요.

kr_quant.price_adjust의 핵심 로직(adjust_prices/diagnose)은 kr-quant의 백테스트
전략들이 in-process import하므로 kr-quant에 계속 남아 있다 — 콜렉터 이전과 무관.
그래서 이 DAG는 /opt/kr-quant 마운트를 통해 계속 kr_quant를
실행한다. 다만 collectors/ 이전 이후 kr-quant의 editable pip install은 더 이상 하지
않으므로(entrypoint-wrapper.sh), PYTHONPATH로 대신 kr_quant를 찾게 한다.
"""

from __future__ import annotations

import os
import sys

from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.sensors.external_task import ExternalTaskSensor

from _common import run_collector, timescale_dsn


@dag(
    dag_id="weekly_price_adjust",
    # 토요일 10:40 KST — 스택 기동(cron 0 10 * * *) 이후. 기존 05:00은 머신이
    # 꺼져 있는 시간이라(스택 가동 창 10:00~) 제 시각에 돌 수 없었다.
    #
    # **순서 보장은 이제 시계가 아니라 센서다.** weekly_delisted_stocks(10:05)가
    # 새 폐지 종목 시세를 daily_bars 에 넣은 뒤에 조정가를 재생성해야 하는데,
    # 그 보장이 35분 간격뿐이었다. 실측으로 2026-08-15 에 그 DAG 의 한 태스크가
    # **175.3분** 걸린 적이 있다 — 마침 price_adjust 가 안 기다려도 되는 마지막
    # 태스크(수급)라 사고가 안 났을 뿐이다. backfill_delisted_bars 가 35분을
    # 넘기는 날엔 반쯤 채워진 daily_bars 로 조정가가 만들어지고, 조용히 틀린
    # 결과가 다음 주까지 간다. 생존편향 제거가 핵심 가치인 레포에서 정확히 그
    # 가치를 갉는 실패 모드다. 아래 ExternalTaskSensor 가 그걸 닫는다.
    schedule="40 10 * * 6",
    start_date=pendulum.datetime(2026, 7, 12, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["kr-quant", "maintenance", "price-adjust"],
)
def weekly_price_adjust():

    # 폐지 종목 시세 백필이 끝났는지 **직접 확인한다.** 두 DAG 는 같은 토요일에
    # 돌지만 스케줄 시각이 달라(10:05 vs 10:40) 데이터 인터벌이 어긋나므로,
    # execution_delta 로 그 35분을 명시한다.
    wait_for_delisted_bars = ExternalTaskSensor(
        task_id="wait_for_delisted_bars",
        external_dag_id="weekly_delisted_stocks",
        external_task_id="backfill_delisted_bars",
        execution_delta=timedelta(minutes=35),
        # poke 는 워커 슬롯을 잡고 있으므로 reschedule 로 놓아준다 —
        # PARALLELISM 이 3 인 서버에서 슬롯 하나를 90분 물고 있으면 안 된다.
        mode="reschedule",
        poke_interval=120,
        timeout=90 * 60,
        # 센서가 시간 초과하면 조정가를 만들지 **않는다**. 반쯤 채워진
        # daily_bars 로 만드느니 이번 주를 거르는 게 낫다 — 다음 주 런이 어차피
        # 전 종목 전 기간을 재계산한다.
        soft_fail=False,
    )

    @task(retries=1, retry_delay=timedelta(minutes=10))
    def rebuild_adjusted() -> None:
        run_collector(
            [
                sys.executable, "-m", "kr_quant.price_adjust",
                "--rebuild-db", "--db", timescale_dsn(),
            ],
            # editable install 없이 kr_quant 패키지를 찾도록 PYTHONPATH 주입 (src/ 레이아웃)
            env={**os.environ, "PYTHONPATH": "/opt/kr-quant/src"},
            cwd="/opt/kr-quant",
        )

    wait_for_delisted_bars >> rebuild_adjusted()


weekly_price_adjust()
