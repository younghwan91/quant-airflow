"""일별 상장주식수(KRX MDCSTAT01501) 수집 → TimescaleDB 직접 저장.

⛔ **2026-08-15부터 paused — 소스가 막혔다.** KRX 가 MDCSTAT 계열에 회원 로그인을
걸면서 OTP 발급이 ``'LOGOUT'`` 을 돌려준다(실측). 그런데 수집기가 그 응답을 그대로
다운로드에 넘기고 빈 결과를 빈 리스트로 반환해, **22회 실행 내내 rows=0 이면서 한 번도
실패로 잡히지 않았다**(전 실행 로그 확인). 초록불이 아무 의미가 없던 DAG다.

이제 ``krx_shares.fetch_snapshot`` 이 그 응답에 RuntimeError 를 낸다. 소스가 살아나기
전까지 이 DAG 를 켜면 매일 빨갛게 터지므로 paused 로 둔다 — 지우지 않는 이유는 KRX 가
정책을 되돌릴 수 있고, 이 소스가 유일한 **날짜지정·전종목** 경로이기 때문이다.

대체 현황: 현재 상장 종목은 ``weekly_listed_shares``(키움 ka10001, 주간 스냅샷)가
채운다 — 과거 백필은 안 되지만 앞으로는 쌓인다. 상장폐지 종목은
``collectors/dart_shares.py``(DART stockTotqySttus)가 담당한다.


ka10001(weekly_listed_shares)은 현재 스냅샷만 주어 과거 백필이 불가하고, 그
때문에 market_cap_asof가 과거 백테스트 날짜에 '오늘의 주식수'를 소급 적용하는
lookahead 버그가 있었다. KRX MDCSTAT01501은 무인증·전종목·날짜지정(trdDd)이라
과거 임의 거래일을 백필할 수 있어 point-in-time 시총/수급비율 분모를 정확히
만든다 — 이 DAG가 그 authoritative 소스다.

- 매 거래일 장 마감·시세확정 후(18:30 KST) 당일 상장주식수를 append.
- 최초 과거 백필은 수동 트리거로 range 실행:
    docker exec quant-airflow-airflow-scheduler-1 \
      python -m collectors.krx_shares \
      --from 2024-01-08 --to <today> --db <dsn>
- Kiwoom 자격증명이 불필요(네이버/DART와 동일하게 무인증).
"""

from __future__ import annotations

import sys

import pendulum
from airflow.decorators import dag, task

from _common import DEFAULT_TASK_KW, run_collector, timescale_dsn


@dag(
    dag_id="daily_krx_shares",
    # ⛔ schedule=None — **수동 트리거 전용이다.**
    #
    # 원래는 `"30 18 * * 1-5"` + `is_paused_upon_creation=True` 였는데, 그 플래그는
    # **DAG 가 메타DB 에 처음 등록될 때만** 적용된다. 이 DAG 는 start_date 2026-07-10
    # 부터 이미 등록돼 있어 플래그가 통째로 무시됐고, `is_paused=false` 인 채로
    # 8/17 이후 평일마다 100% 실패했다(6연속). 0.6초짜리 실패 두 번에
    # retry_delay 10분이 붙어 **평일 스택 종료가 18:40 으로 못박혔다** —
    # cron_updown.log 의 모든 평일 종료가 18:40:4x 다.
    #
    # 소스가 살아나면 여기에 cron 을 되돌린다. 그 전에 반드시 아래 두 가지를
    # 확인할 것:
    #   1. `collectors/krx_shares.py` 가 `source="krx"` 로 쓴다(고쳐놨다)
    #   2. `weekly_listed_shares` 와의 중복 — 둘 다 shares_outstanding_history 의
    #      같은 (code,date) PK 에 쓴다. KRX 는 요청 2회로 우선주까지 + 과거
    #      날짜지정이 되는 상위호환이라, 살아나면 키움 쪽(2,628요청/48분)을 끈다.
    schedule=None,
    start_date=pendulum.datetime(2026, 7, 10, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["kr-quant", "collection", "shares"],
)
def daily_krx_shares():

    @task(**DEFAULT_TASK_KW)
    def collect_krx_shares() -> None:
        # KST 당일(거래일이면 데이터 존재, 휴장일이면 collector가 codes=0로 무해 처리)
        today = pendulum.now("Asia/Seoul").to_date_string()
        run_collector([
            sys.executable, "-m", "collectors.krx_shares",
            "--date", today, "--db", timescale_dsn(),
        ])

    collect_krx_shares()


daily_krx_shares()
