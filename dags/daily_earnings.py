"""일일 증분 DART 분기 실적(순이익·매출·영업이익) 수집 → earnings 테이블 upsert.

DART OpenAPI ``fnlttSinglAcnt``에서 분기 순이익·매출·영업이익을 당기/전년동기
쌍으로 받아 lookahead-safe ``avail_date``(분기말+공시지연)와 함께 ``earnings``
테이블에 upsert한다. 이 데이터가 PEAD(실적 YoY)와 미너비니 SEPA **Code 33**
(EPS·매출·마진 3분기 연속 가속, ``features.fundamentals.code33_panel``)의
펀더멘털 입력이다.

**당기+전기만 갱신하는 이유:** ``--recent-quarters 2``로 전체 ~2,600개
daily_bars 종목 중 당기+전분기(N-1)만 재확인한다 — (code, period) 단위 resume가
있으므로 이미 DB에 있는 조합은 skip되고, 대부분의 실행일은 사실상 no-op에
가깝다. 실적 시즌(분기말+45~90일 공시 몰림 구간)에만 실제로 새 데이터를 받는다.
2,600 종목 × 2분기 ≈ 5,200콜/일로 DART 일 한도(2만/키)에 안전하게 들어간다 —
과거 ``weekly_earnings.py``처럼 top-500 전체 히스토리를 매주 재수집하는 방식과
달리, 매일 가볍게 최신 분기만 스치듯 확인하는 접근이다.

**별도 DAG로 분리한 이유:** ``daily_collection``과 같은 흐름/시각(평일 16:00
KST, 장마감+데이터 확정 대기)에 편입하되, 같은 DAG에 태스크로 얹지 않고 별도
DAG로 둔다 — DART 수집 실패/재시도가 Kiwoom 일봉·수급 수집의 blast radius에
섞이지 않게 하기 위함(반대 방향도 마찬가지).

DART 키는 Airflow의 Fernet 암호화 Variables에만 있고(컨테이너 평문 env 아님),
수집 subprocess에만 주입한다 — Kiwoom 자격증명과 동일 패턴(daily_collection).

**퇴역 이력:** 이 DAG와 ``earnings_backfill.py``가 ``earnings`` 테이블을 함께
커버하게 되면서, 기존 ``weekly_earnings.py``(주간 전체 CSV 재생성)는 superseded되어
제거되었다.
"""

from __future__ import annotations

import sys

import pendulum
from airflow.decorators import dag, task

from _common import DEFAULT_TASK_KW, dart_env, run_collector, timescale_dsn


@dag(
    dag_id="daily_earnings",
    schedule="0 16 * * 1-5",  # 평일 16:00 KST — daily_collection과 동일 흐름/시각
    start_date=pendulum.datetime(2026, 7, 12, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["kr-quant", "collection", "earnings"],
)
def daily_earnings():

    @task(**DEFAULT_TASK_KW)
    def collect_earnings() -> None:
        run_collector(
            [
                sys.executable, "-m", "collectors.dart_earnings",
                # --multi-batch: 100종목씩 묶어 조회(fnlttMultiAcnt). 비수기엔 전부
                # done_periods로 스킵돼 차이가 없지만, 실적 시즌엔 종목별 루프가
                # 2,635종목×2분기 ≈ 5,270콜이 되는 걸 ~54콜로 줄인다.
                "--db-table", "--multi-batch", "--all-codes", "--recent-quarters", "2",
                "--db", timescale_dsn(),
            ],
            env=dart_env(),
        )

    collect_earnings()


daily_earnings()
