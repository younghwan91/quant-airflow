"""Sharadar(미국) 스토어 일일 재구축 — 벌크 스냅샷 → 빌드 → 검증 → 원자적 공개.

**이 레포에서 유일한 비(非)한국 DAG다.** 이 머신에서 살아 있는 스케줄러가
여기뿐이라 미국 데이터도 여기서 돈다 — 레포를 kr-quant-airflow 에서
quant-airflow 로 개명한 이유다.

설계 근거: `docs/superpowers/specs/2026-08-15-sharadar-bulk-rebuild-design.md`

**왜 증분이 아니라 재구축인가.** 처음엔 API 증분(종목 22,000개를 30개씩 ~730회
순회)으로 만들었다가 실측에서 버렸다:

- 벤더 티커 제한이 개수(30)가 아니라 **문자열 200자**라, 우선주 티커 30개면
  240자가 되어 `400` 이다 — fundamentals 는 매 실행 실패했다
- 소켓 타임아웃이 재시도되지 않아 70분짜리 작업이 딸꾹질 한 번에 전멸했다
- 스토어를 직접 upsert 하므로 연구(`opt-factor optimize`)와 DuckDB 락이 충돌했다

벌크는 테이블당 요청 1회라 앞의 둘이 해당 없고, 새 파일에 지어 갈아끼우므로
셋째도 사라진다. 덤으로 구독 중이던 데이터셋이 전부 아카이브된다.

**태스크가 둘인 이유**: 다운로드(네트워크, 최대 17분)와 빌드(CPU·디스크, 약
20분)는 실패 성격이 다르다. 나뉘어 있어야 Airflow UI 에서 어디서 끊겼는지 보이고,
재시도도 그 지점만 된다 — 벤더가 느린 날 빌드까지 되돌릴 이유가 없다.
"""

from __future__ import annotations

import sys

from datetime import timedelta

import pendulum

from airflow.decorators import dag, task

from _common import run_collector, sharadar_env

RAW_DIR = "/opt/us-data/sharadar/raw"
STORE = "/opt/us-data/us_micro.duckdb"


@dag(
    dag_id="daily_sharadar",
    # 17:30 KST — **벤더가 테이블마다 다른 시각에 올린다**(2026-08-15 실측
    # manifest): holdings_ticker 01:39, insiders 09:48, daily 12:56, 그런데
    # 정작 가장 중요한 stocks(주가) 16:40, fundamentals 16:49, funds 16:54 다.
    # 처음엔 daily 만 보고 13:15 로 잡았는데, 그러면 주가·재무는 매일 전날
    # 드롭을 받는다. 가장 늦은 16:54 뒤로 여유를 둔 시각이 17:30 이다.
    #
    # 화~토인 이유: 미국 장이 없는 날은 새로 받을 게 없다. 금요일 세션은
    # 토요일 드롭에 실려 오므로 토요일 런이 받는다(daily_short_credit 과 같은 패턴).
    #
    # ⚠️ **이 DAG 가 평일 저녁 창의 길이를 혼자 정한다.** 예전 주석은 "평일은
    # daily_consensus(18:00)가 이미 스택을 잡아두므로 추가 가동시간이 거의 없다"
    # 고 적었는데 두 군데가 틀렸다: consensus 는 18:00 이 아니라 **17:00** 이고,
    # 49초면 끝난다(실측 08-26 17:00:01→17:00:50, 08-27 17:00:02→17:00:47).
    # 즉 17:01~18:23 은 이 DAG 혼자 쓰는 시간이고, 평일 스택을 약 53분 늘린다.
    #
    # 그래도 17:30 을 앞당길 수 없다 — 벤더 드롭이 그날 다 끝나야 받을 게 있고,
    # 관측된 최악값이 17:28 이었다(2026-08-25). 앞당기면 그날 치를 통째로 놓친다.
    # 토요일은 이 DAG 때문에 스택이 10:40 대신 18:15 까지 뜬다.
    schedule="30 17 * * 2-6",
    start_date=pendulum.datetime(2026, 8, 15, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["us", "sharadar", "factor"],
)
def daily_sharadar():

    @task(retries=2, retry_delay=timedelta(minutes=10))
    def download() -> None:
        """구독 14개를 벤더와 대조. `modified` 가 그대로면 받지 않는다."""
        run_collector(
            [
                sys.executable, "-m", "collectors.sharadar_bulk",
                "--raw-dir", RAW_DIR,
            ],
            env=sharadar_env(),
        )

    @task(retries=1, retry_delay=timedelta(minutes=15))
    def rebuild() -> None:
        """새 스토어를 짓고, 게이트를 통과하면 제자리에 갈아끼운다.

        실패하면 아무것도 안 바꾼다 — 기존 스토어가 계속 서비스된다.
        """
        run_collector(
            [
                sys.executable, "-m", "collectors.sharadar_build",
                "--raw-dir", RAW_DIR, "--store", STORE,
            ],
            env=sharadar_env(),
        )

    download() >> rebuild()


daily_sharadar()
