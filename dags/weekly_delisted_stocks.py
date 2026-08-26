"""KRX 상장폐지종목 마스터 + 그 종목들의 과거 일봉 수집 (생존편향 보정).

``features/universe.py``가 미해결로 남겨둔 갭: point-in-time 유니버스를 지금
거래되는 종목만으로 구성하면 상장폐지된 종목이 통째로 빠져 생존편향이 생긴다.

KRX의 일반 통계 리포트(MDCSTAT류, ``daily_krx_shares`` DAG가 쓰는 것)는 최근
회원 로그인을 요구하도록 바뀌어 막혔지만, 이 DAG가 쓰는 ``finder_listdelisu``
(종목 검색 자동완성 위젯 API)는 로그인 없이 그대로 동작한다 — 실제 라이브
호출로 확인됨. 날짜는 안 주므로 daily_bars의 종목별 마지막 거래일로
상장폐지일을 근사한다(상폐 종목은 보통 상폐일 직전까지 거래되므로 근접치).

**두 번째 태스크(2026-08-15 추가):** 마스터 리스트만으로는 편향이 안 풀린다.
폐지 종목의 과거 시세가 daily_bars 에 있어야 백테스트 유니버스에 들어간다.
키움은 폐지 코드에 빈 응답(return_code=0)을 주므로 네이버 siseJson 으로 받는다 —
자세한 근거는 ``collectors/naver_delisted_bars.py`` docstring.

**주간 스케줄인 이유:** 상장폐지는 매일 몇 건씩 나는 이벤트가 아니라 드물게
발생하므로, price_adjust와 같은 주간 배치로 충분하다.

**price_adjust 보다 앞서 도는 이유(2026-08-15 순서 교체):** 이 DAG 가 새 폐지
종목의 시세를 daily_bars 에 넣으면, daily_bars_adjusted 는 그걸 본 뒤에
재생성돼야 한다. 반대 순서면 새로 받은 종목이 조정가 테이블에 일주일 늦게
반영된다. 그래서 delisted 10:05 → price_adjust 10:40 으로 바꿨다.

**세·네 번째 태스크(2026-08-15 추가):** 상장주식수와 수급. KRX MDCSTAT 계열이 로그인 장벽으로
막혀(실측 0행) DART ``stockTotqySttus`` 로 받는다 — 근거는
``collectors/dart_shares.py`` docstring.

수급은 **부분만** 채운다 — 네이버가 기관·외국인 순매매만 주므로 개인·기관세부는
NULL 로 남는다(migration 006 의 source 컬럼으로 구분).

마스터·시세·수급 수집은 무인증, 주식수 백필만 DART 키가 필요하다.
"""

from __future__ import annotations

import sys

from datetime import timedelta

import pendulum
from airflow.decorators import dag, task

from _common import DEFAULT_TASK_KW, dart_env, run_collector, timescale_dsn


@dag(
    dag_id="weekly_delisted_stocks",
    # 토요일 10:05 KST — 스택 가동 창(10:00~) 직후. price_adjust(10:40)보다 앞:
    # 새 폐지 종목 시세가 daily_bars 에 들어간 뒤 조정가가 재생성돼야 한다.
    schedule="5 10 * * 6",
    start_date=pendulum.datetime(2026, 7, 12, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["kr-quant", "maintenance", "delisted"],
)
def weekly_delisted_stocks():

    @task(**DEFAULT_TASK_KW)
    def collect_delisted() -> None:
        run_collector([
            sys.executable, "-m", "collectors.krx_delisted",
            "--db", timescale_dsn(),
        ])

    @task(retries=1, retry_delay=timedelta(minutes=20))
    def backfill_delisted_bars() -> None:
        """폐지 종목의 과거 일봉 백필 (네이버).

        이미 있는 (code, date)는 ON CONFLICT DO NOTHING 이라 매주 전종목을 훑어도
        새로 폐지된 것만 실제로 쌓인다 — 멱등이라 재실행이 안전하다. 우리 시세 구간
        이전에 폐지된 종목은 응답이 비어 비용 없이 넘어간다.
        """
        run_collector([
            sys.executable, "-m", "collectors.naver_delisted_bars",
            "--db", timescale_dsn(),
        ])

    @task(retries=1, retry_delay=timedelta(minutes=20))
    def backfill_delisted_shares() -> None:
        """폐지 종목 상장주식수 백필 (DART).

        시가총액의 분모다 — 이게 없으면 cap 기반 유니버스가 폐지 종목을 못 담아,
        시세를 아무리 메워도 그 경로엔 생존편향이 남는다.

        시세 백필 **뒤**에 둔다: 대상 선정이 ``daily_bars.source='naver'`` 기준이라
        그 주에 새로 들어온 폐지 종목을 같은 실행에서 처리하려면 순서가 필요하다.
        이미 주식수가 있는 코드는 건너뛰므로 매주 돌아도 신규분만 조회한다.
        """
        run_collector(
            [sys.executable, "-m", "collectors.dart_shares", "--db", timescale_dsn()],
            env=dart_env(),
        )

    @task(retries=1, retry_delay=timedelta(minutes=20))
    def backfill_delisted_flow() -> None:
        """폐지 종목 수급 **부분** 백필 (네이버).

        키움 ka10059 는 폐지 코드에 return_code=0(성공) + 0행을 준다(실측). 네이버는
        주지만 기관·외국인 순매매만 있어, 개인·기관세부는 NULL 로 남는다 —
        개인 순매매를 쓰는 연구는 이 데이터로 재현할 수 없다.

        페이지 단위 조회라 신규 폐지분 기준으로도 종목당 수십 요청이 든다. 이미 수급이
        있는 코드는 건너뛰므로 매주 도는 비용은 신규분에 비례한다.
        """
        run_collector([
            sys.executable, "-m", "collectors.naver_supply_demand",
            "--db", timescale_dsn(),
        ])

    (collect_delisted() >> backfill_delisted_bars()
     >> backfill_delisted_shares() >> backfill_delisted_flow())


weekly_delisted_stocks()
