"""아침 누락분 자동 복구 — 전날 daily_collection에서 실패한 종목만 재수집.

daily_collection(16:00)은 매일 전종목 일봉을 통째로 재수집해서 시간이
오래 걸리고(~45분), 도중 에러가 나면 그 시점 이후 종목들이 그날치를
아예 못 받는다(2026-07-08 트랜잭션 연쇄 실패 사례). 그 실패분을 다음날
16:00까지 기다리지 않고, 컨테이너가 켜지는 아침 시간대에 값싸게 먼저
복구한다.

``combined.py --update``는 종목별로 이미 시장 최신 **완료** 거래일 데이터가
있으면 API 호출 자체를 건너뛴다 — 일봉과 수급을 따로 판정하므로 한쪽만 낡은
종목은 그쪽 TR 만 부른다. 전날 정상 수집된 종목은 DB 조회 두 번으로 스킵되고,
실패해서 뒤처진 종목만 실제로 재수집된다. 뒤처진 종목은 `--daily-days` 없이
전 이력을 받으므로 깊은 구멍도 여기서 메워진다.

**2026-08-25 이전에는 이 전제가 거짓이었다.** 실측 2,924초 — 16:00 본
수집(2,918초)의 완전한 중복이었고 이유가 둘이었다:

1. `_market_latest_date` 가 10:05 **장중**에 프로브를 던져 진행 중인 오늘
   캔들을 최신 거래일로 잡았다. 전날 수집분(어제까지)은 전 종목에서 낡음
   판정을 받아 `skip=0` 이 됐다.
2. 수급(ka10059)에 증분 가드가 아예 없어 항상 전 종목 재수집이었다.

둘 다 collectors 쪽에서 고쳤다. 이 docstring 의 "몇 초 안에 끝난다" 는 이제
실제로 참이다.
"""

from __future__ import annotations

import sys

import pendulum
from airflow.decorators import dag, task

from _common import DEFAULT_TASK_KW, kiwoom_env, run_collector, timescale_dsn


@dag(
    dag_id="daily_collection_catchup",
    # 평일 10:05 KST — 컨테이너 기동(10:00) 직후. **본 수집(daily_collection)이
    # 평일만 도는데 그 복구용이 매일 돌 이유가 없다.** 주말엔 복구할 실패분이
    # 원리적으로 존재하지 않는다(실측: 2026-08-22 토 / 08-23 일 런이 `일봉 0행`
    # 을 쓰고도 각각 48.6분을 태웠다 — 수급에 증분 가드가 없어서였고 그건
    # collectors/combined.py 에서 고쳤지만, 애초에 돌 이유가 없는 날이다).
    schedule="5 10 * * 1-5",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["kr-quant", "collection"],
)
def daily_collection_catchup():

    @task(**DEFAULT_TASK_KW)
    def catchup_both() -> None:
        run_collector([
            sys.executable, "-m", "collectors.combined",
            "--market", "all", "--prod", "--rate", "0.9", "--update",
            "--db", timescale_dsn(),
        ], env=kiwoom_env())

    catchup_both()


daily_collection_catchup()
