"""애널리스트 컨센서스(목표주가·투자의견·추정실적) 일일 수집 → TimescaleDB 직접 저장.

키움 브로커 API엔 컨센서스가 없고, 네이버 금융(FnGuide 출처)에 목표주가·
투자의견·추정 EPS가 있다. 이 DAG는 매 거래일 스냅샷을 한 줄씩 upsert하여
**컨센서스 개정(revision) 시계열**을 축적한다 — 후행 PEAD가 무력한 초대형주의
재평가를 미리 잡는 forward-looking 신호(docs/pead-strategy.md).

네이버는 인증이 필요 없으므로 Kiwoom 자격증명이 불필요하다.

**DB 직접 저장(--db-table)으로 전환한 이유:** 원래는 CSV 전용이라 daily_bars·
earnings 등 다른 데이터와 SQL로 조인이 안 됐다 — "다른 데이터랑 같이"라는
프로젝트 목표(README)에 맞춰 consensus 테이블(PK: code, date)에 직접 upsert.
sql/init_timescale.sql에 스키마 추가됨.

**유니버스: 평일은 커버리지 종목만, 월요일은 전종목.** 원래는 매일 전종목
(~2,627개)을 훑었다. "커버리지 없는 소형주는 자연히 스킵되니 무해하다"는
전제였는데, 실측이 그 전제를 깼다 — 요청은 일 5,254건인데 실제 적재는 하루
652~660행이다. **~1,930 종목(73%)이 매일 빈 응답을 받고 버려진다.** 무해한 게
아니라 실행 시간의 대부분이다.

커버리지는 하루아침에 생기지 않으므로 매일 확인할 이유가 없다. 대신 주 1회
(월요일) 전종목을 훑어 신규 커버리지 편입을 잡는다 — 그 스윕이 없으면 한 번
빠진 종목을 영원히 놓친다.

**스케줄이 18:00 이 아닌 이유.** 예전 주석은 "장 마감 후 컨센서스 갱신 반영"
이라고만 적혀 있었고 검증 흔적이 없었다. DB 실측은 정반대다 — `base_date`
(FnGuide 기준일)가 **항상 전 영업일**이다:

    date        base_date
    2026-08-24  2026-08-21
    2026-08-21  2026-08-20
    2026-08-20  2026-08-19

당일 갱신분이 애초에 없으므로 언제 받든 같은 데이터다. 그래서 daily_collection
(16:00, 실측 48.7분)이 끝난 뒤로 붙여 스택 가동 시간을 줄인다.
"""

from __future__ import annotations

import sys

import pendulum
from airflow.decorators import dag, task

from _common import DEFAULT_TASK_KW, run_collector, timescale_dsn


@dag(
    dag_id="daily_consensus",
    # 평일 17:00 KST — 네이버 컨센서스는 T-1 기준이라 시각이 데이터에 영향을
    # 주지 않는다(위 docstring 실측). daily_collection(16:00) 이 중앙 48.7분,
    # 재시도 경로 최대 58.8분이라 17:00 이면 겹치지 않는다.
    schedule="0 17 * * 1-5",
    start_date=pendulum.datetime(2026, 7, 10, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["kr-quant", "collection", "consensus"],
)
def daily_consensus():

    @task(**DEFAULT_TASK_KW)
    def collect_consensus() -> None:
        # 월요일만 전종목 스윕(신규 커버리지 편입 탐지), 화~금은 최근 90일 안에
        # 실제로 컨센서스가 잡힌 종목만. 90일인 이유는 분기 실적 시즌을 한 번은
        # 포함해야 일시적으로 조용했던 종목이 탈락하지 않기 때문이다.
        is_monday = pendulum.now("Asia/Seoul").weekday() == 0
        universe = ["--all-codes"] if is_monday else ["--covered-days", "90"]
        run_collector([
            sys.executable, "-m", "collectors.naver_consensus",
            "--db-table", *universe, "--db", timescale_dsn(),
        ])

    collect_consensus()


daily_consensus()
