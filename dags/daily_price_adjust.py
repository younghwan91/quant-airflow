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

**토요일 런은 그대로 둔다.** 그쪽은 ``weekly_delisted_stocks`` 의 폐지 시세
백필을 ``ExternalTaskSensor`` 로 기다렸다가 돌아, 새로 폐지된 종목의 이력까지
조정가에 반영한다. 평일에는 기다릴 대상이 없으므로 센서도 없다.
"""

from __future__ import annotations

import os
import sys

import pendulum
from airflow.decorators import dag, task

from _common import DEFAULT_TASK_KW, run_collector, timescale_dsn


@dag(
    dag_id="daily_price_adjust",
    # 평일 16:55 — daily_collection(16:00 시작, 실측 16:50 종료) 직후.
    # daily_consensus(17:00, 45초)와 겹치지만 그쪽은 네이버 HTTP 라 자원이 안 겹친다.
    schedule="55 16 * * 1-5",
    start_date=pendulum.datetime(2026, 8, 27, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["kr-quant", "maintenance", "price-adjust"],
)
def daily_price_adjust():

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

    rebuild_adjusted()


daily_price_adjust()
