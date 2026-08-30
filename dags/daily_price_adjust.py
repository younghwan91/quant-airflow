"""평일 조정주가 갱신 — daily_bars_adjusted 에 **이번 주 행**을 채운다.

``weekly_price_adjust`` 는 토요일에만 돈다. 그래서 월~금 내내 조정가 테이블에
그 주의 행이 **아예 없다** — 금요일이면 5거래일이 비어 있다. 실측 2026-08-27(목):

    daily_bars          최신 2026-08-27
    daily_bars_adjusted 최신 2026-08-21   ← 4거래일 결측

**주간 주기의 근거가 다른 걸 증명하고 있었다.** ``weekly_price_adjust`` docstring 은
"분할은 드물어서(전체 이력 통틀어 ~44건) 매일 돌릴 필요는 없다"고 적는데, 그건
*과거 조정계수가 잘 안 변한다*는 뜻이지 *이번 주 행이 없어도 된다*는 뜻이 아니다.
문제는 계수가 낡은 게 아니라 **행 자체가 없는 것**이다.

그 테이블은 애초에 "연구 스크립트가 미조정 원자료를 읽어 분할이 가짜 −68% 손실로
잡히던" 버그를 막으려고 만들었다. 주간 주기는 그 스크립트들에게 "조정가를 쓰되
이번 주는 포기해라"를 요구한다 — 스윙·모멘텀이면 거기가 전략의 작동 구간이다.

**비용(2026-08-27 실측): 6분 23초, 피크 RSS 5.19GB.** DB 전용이라 키움 토큰도
벤더 레이트리밋도 안 쓴다 — 경합 대상이 아무것도 없다. 16:55 는 daily_collection
(16:01~16:50)이 끝난 직후이자 daily_sharadar(17:30~, DuckDB 로 메모리를 쓴다)
앞이라, 저녁 창에서 이 작업이 혼자 도는 유일한 구간이다.

⚠️ 5.19GB 는 이 머신(15GB)에서 작은 값이 아니다. 2026-08-08 에 이 커맨드가
SIGKILL(OOM 추정)로 죽은 적이 있다 — 그래서 다른 메모리 사용자와 겹치지 않는
자리에 두었다. 증분 모드는 없다: ``rebuild_adjusted_table`` 은 back-adjust 가
종목별 전체 이력을 봐야 하므로 매번 daily_bars 전량(572만 행)을 읽는다.

**그 "겹치지 않는 자리"를 시계가 지키고 있었다 — 이제 센서가 지킨다.** 16:55 는
``daily_collection`` 이 실측 16:50 에 끝난다는 데 기댄 값인데, 그건 정상 경로의
값이다. 재시도가 걸리면 그 DAG 는 최대 16:59 까지 가고(중앙 48.7분 + retry_delay
10분), 늦게 실패한 날은 그보다 더 간다 — 40분째에 죽으면 재실행이 17:38 에
끝난다. 그 구간이 정확히 이 태스크의 5.19GB 와 겹치고, 겹치는 상대는 48분짜리
전종목 수집이다.

이 레포의 규칙은 그 경우 이미 정해져 있다(``weekly_price_adjust``): **DAG 사이
순서는 시계가 아니라 ``ExternalTaskSensor`` 로 보장한다.** 35분 간격이라는 암묵
가정이 앞 태스크가 175분 걸린 날 깨졌던 그 자리다. 여기도 같은 모양이라 같은
약을 쓴다.

센서가 시간 초과하면 **오늘은 만들지 않는다**(``soft_fail=False`` — 빨갛게
남는다). 조정가는 매번 전량 재계산이라 다음 평일 런이 오늘 치까지 다시 만든다.
하루 늦는 대신 OOM 위험과 ``daily_sharadar``(17:30, DuckDB) 와의 메모리 충돌을
피한다. ``soft_fail=True`` 로 조용히 스킵하지 않는 이유는 이 레포의 실패 모드가
늘 "초록불인데 데이터가 없다" 쪽이기 때문이다 — ``wait_and_stop.sh`` 의
``report_failures()`` 에 걸려야 다음날 사람이 본다.

**토요일 런은 그대로 둔다.** 그쪽은 ``weekly_delisted_stocks`` 의 폐지 시세
백필을 기다린다 — 새로 폐지된 종목의 이력까지 조정가에 반영해야 하기 때문이고,
평일에는 그 백필이 안 돈다. 즉 두 DAG 는 **기다리는 대상이 서로 다르다**:
평일은 그날의 일봉(``daily_collection``), 토요일은 폐지 시세 백필.
"""

from __future__ import annotations

import os
import sys

from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.sensors.external_task import ExternalTaskSensor

from _common import DEFAULT_TASK_KW, run_collector, timescale_dsn


@dag(
    dag_id="daily_price_adjust",
    # 평일 16:55 — daily_collection(16:00 시작, 실측 16:50 종료) 직후.
    # daily_consensus(17:00, 45초)와 겹치지만 그쪽은 네이버 HTTP 라 자원이 안 겹친다.
    #
    # 이 시각은 이제 "가장 이른 시작 가능 시각"이지 순서 보장이 아니다 — 보장은
    # 아래 ExternalTaskSensor 가 한다(위 docstring).
    schedule="55 16 * * 1-5",
    start_date=pendulum.datetime(2026, 8, 27, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["kr-quant", "maintenance", "price-adjust"],
)
def daily_price_adjust():

    # 일봉 수집이 **끝났는지** 직접 확인한다. 두 DAG 는 같은 평일에 돌지만 시각이
    # 달라(16:00 vs 16:55) 데이터 인터벌이 어긋나므로 execution_delta 로 그 55분을
    # 명시한다 — weekly_price_adjust 가 35분에 하는 것과 같다.
    wait_for_daily_bars = ExternalTaskSensor(
        task_id="wait_for_daily_bars",
        external_dag_id="daily_collection",
        external_task_id="collect_both",
        execution_delta=timedelta(minutes=55),
        # poke 는 워커 슬롯을 잡고 있으므로 reschedule 로 놓아준다 —
        # PARALLELISM 이 3 인 서버에서 슬롯을 물고 기다리면 안 된다.
        mode="reschedule",
        poke_interval=120,
        # 20분 = 17:15 까지. daily_sharadar(17:30) 앞에서 끊는 값이다 — 6분 23초
        # 짜리 재계산이 17:15 에 시작해도 17:22 에 끝나 그쪽 DuckDB 빌드와 안
        # 겹친다. 더 길게 잡으면 OOM 을 피하려고 만든 센서가 OOM 을 부른다.
        timeout=20 * 60,
        soft_fail=False,
    )

    @task(**DEFAULT_TASK_KW)
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

    wait_for_daily_bars >> rebuild_adjusted()


daily_price_adjust()
