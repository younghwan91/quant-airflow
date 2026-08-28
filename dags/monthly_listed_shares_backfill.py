"""상장 종목 과거 상장주식수 백필(DART) — 시총 분모가 다시 비지 않게 한다.

키움 ``ka10001``(``weekly_listed_shares``)은 **현재 스냅샷만** 준다. 그래서
``shares_outstanding_history`` 는 그 수집이 시작된 2026년부터만 쌓이고, 그 이전
구간은 폐지 종목 백필분(``source='dart'``)밖에 없었다. 실측 2026-08-27:

    SELECT count(*) FROM stocks k WHERE EXISTS (
      SELECT 1 FROM shares_outstanding_history s
      WHERE s.code=k.code AND s.date <= '2018-06-01' AND s.shares_outstanding > 0)
    → 5

연도별로 190~261종목이 있긴 했지만 **전부 폐지 종목**이라 상장 마스터와 교집합이
없었다. 결과적으로 시총을 분모로 쓰는 백테스트는 2026년 이전에서 대부분 NaN 이
되고, 값이 나오는 소수는 폐지 종목 쪽으로 치우친 표본이었다 — 에러가 아니라
**조용히 틀리는** 종류다(kr-quant 기관수급 알파가 이걸로 VOID 됐다).

2026-08-28 에 전량 백필해 그 시점 거래 종목 대비 93.6~95.1% 로 채웠다. 이 DAG 는
**그 상태를 유지하는 쪽**이다.

**무엇을 잡고 무엇을 안 잡나.** 2026년 이후 신규 상장분은 이 DAG 의 몫이 아니다 —
``weekly_listed_shares`` 가 주간 스냅샷으로 그때부터 쌓는다. 여기가 잡는 건
**2016~2025 구간에 거래 이력이 새로 생긴 코드**다:

- ``combined --update`` 가 신규 편입 종목의 과거 일봉을 깊게 받아오면, 그 코드는
  그제서야 2016~2025 구간에 daily_bars 행을 갖는다 → 주식수도 필요해진다.
- DB 를 다시 만들었거나 이력을 복구한 경우.

정상 상태에서는 대상이 0이라 **몇 초 만에 끝난다.** ``backfill_markers`` 덕에
"DART 에 보고서가 아예 없는" 106종목을 매번 다시 조회하지도 않는다 — 그 마커가
없던 시절엔 종목당 최대 `4보고서 × 연수` 만큼의 호출을 회차마다 태웠다.

무인증 아님(DART 키 필요). 키움 토큰을 안 쓰므로 아침 창의 키움 DAG 들과 겹쳐
돌아도 서로 막지 않는다. DART 일한도는 키 로테이션(`collectors/dart.py`)이 받는다.
"""

from __future__ import annotations

import sys

import pendulum
from airflow.decorators import dag, task

from _common import DEFAULT_TASK_KW, dart_env, run_collector, timescale_dsn

#: 백필 구간. 2016 은 daily_bars 이력의 시작(2016-09-09)에 맞춘 값이고, 상한을
#: 2025 로 둔 건 2026년부터는 weekly_listed_shares 가 주간 스냅샷으로 채우기
#: 때문이다 — 그 구간까지 DART 로 받으면 같은 사실을 두 소스로 중복 수집한다.
FROM_YEAR = "2016"
TO_YEAR = "2025"


@dag(
    dag_id="monthly_listed_shares_backfill",
    # 매월 1일 10:20 KST — 스택 오전 창(10:00~11:30) 안. DART 전용이라 같은 창의
    # 키움 DAG(short_credit 10:00 · catchup 10:05 · listed_shares 화 10:10)와
    # 토큰도 TR 버킷도 겹치지 않는다.
    #
    # 월 1회인 이유: 정상 상태에서 대상이 0이다(위 docstring). 신규 편입 종목의
    # 과거 일봉이 들어오는 빈도가 이 작업의 실제 주기이고, 그건 월 단위로 충분하다.
    schedule="20 10 1 * *",
    start_date=pendulum.datetime(2026, 8, 28, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["kr-quant", "collection", "shares", "backfill"],
)
def monthly_listed_shares_backfill():

    @task(**DEFAULT_TASK_KW)
    def backfill_listed_shares() -> None:
        run_collector(
            [
                sys.executable, "-m", "collectors.dart_shares",
                "--listed", "--from-year", FROM_YEAR, "--to-year", TO_YEAR,
                "--db", timescale_dsn(),
            ],
            env=dart_env(),
        )

    backfill_listed_shares()


monthly_listed_shares_backfill()
