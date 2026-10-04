"""DART 분기 실적(순이익·매출·영업이익) 전체 이력 백필 → ``earnings`` 테이블 upsert.

이 DAG는 ``--all-codes``로 daily_bars 전 종목(~2,600개)을, ``--from-year 2016``부터(주1)
전체 이력으로 한 번에 ``earnings`` 테이블에 upsert하는 **백필 전용** 파이프라인이다.
일상적인 최신 분기 반영(최신 유지)은 ``daily_earnings``가 담당한다 — 두 목적(최신
유지 vs. 과거 이력 채우기)이 달라 별도 DAG로 분리했다.

(주1) 2016년은 EARNINGS_PIPELINE_PLAN.md 핸드오프 문서의 권장치이며 아직 사용자
확인은 안 된 기본값이다 — 필요 시 조정할 것.

**월 1회(매월 1일 10:00 KST)로 내린 이유 — "수렴 후 no-op"은 사실이 아니었다:**
과거 이 DAG는 평일 매일 18:00에 돌았고, 근거는 "백필이 끝나면 전부 스킵되는
no-op이라 계속 켜둬도 무해하다"였다. 2026-07-17 실측 결과 그 전제가 틀렸다.

- 이력은 이미 수렴했다: 2016~2025년 종목당 4분기가 모두 차 있고 2026년은 Q1만
  존재한다(반기보고서 공시 전이므로 정상). 즉 받을 게 없다.
- 그런데도 매 실행이 3시간 35분 걸렸고 `earnings` 테이블 쓰기는 사실상 없었다.
  ``pending`` 계산이 **현재 상장 종목 전체**에서 DB에 있는 (code, period)만 빼기
  때문이다 — 2016년에 상장도 안 했던 ~947개 종목은 "아직 안 채운 조합"으로 남아
  매 실행마다 영원히 재조회된다. 채워질 수 없으므로 수렴하지 않는다.
- 진짜 비용은 DART 호출이 아니라 **머신 가동시간**이었다. ``wait_and_stop.sh``가
  스택을 내리기 전 이 DAG가 21:35까지 붙잡고 있어 TimescaleDB/스케줄러/웹서버가
  매일 4시간 이상 더 떠 있었다.
- 게다가 ``wait_and_stop.sh``의 대기 목록에 이 DAG가 없어 22:00 안전장치에
  ``docker compose stop``으로 런이 통째로 잘렸다(2026-07-16 런이 1235분으로
  기록된 원인).

신규 상장 종목의 과거 이력은 여전히 누군가 채워야 하므로 삭제하지 않고 월 1회만
남긴다. 일상적인 최신 분기 반영은 ``daily_earnings``(평일 16:00, ``--recent-quarters 2``)가
담당한다. 즉시 필요하면 ``airflow dags trigger earnings_backfill``로 수동 실행.

**재실행 안전(resume) 이유:** ``--db-table`` 모드는 ``earnings`` 테이블에 이미 있는
(code, period) 조합을 건너뛰므로, DART 일한도(status 020, 전 키 소진)로 중단되어도
다음 실행이 자동으로 이어받는다 — 별도의 "resume" 플래그가 필요 없다. 즉 이 DAG는
수렴할 때까지 반복 트리거해도 안전(idempotent)하다.

**retries=2 (30분 간격) 이유:** corp_map 부트스트랩(load_corp_map_with_rotation)이
일시적 네트워크 오류로 실패하면 재시도로 복구된다. 반면 실제 원인이 일한도(020)
소진이면 재시도해도 다시 실패할 뿐이지만 손해는 없다(어차피 다음날 18:00 스케줄이
이어받음) — 두 경우를 구분해 특수 처리하기보다 재시도 자체를 안전하게 둔다.

DART 키는 Airflow의 Fernet 암호화 Variables에만 있고(컨테이너 평문 env 아님),
수집 subprocess에만 주입한다 — Kiwoom 자격증명과 동일 패턴(daily_collection).
"""

from __future__ import annotations

import sys

from datetime import timedelta

import pendulum
from airflow.decorators import dag, task

from _common import DEFAULT_TASK_KW, dart_env, run_collector, timescale_dsn

FROM_YEAR = "2016"  # EARNINGS_PIPELINE_PLAN.md 권장치 — 사용자 확인 필요한 기본값


@dag(
    dag_id="earnings_backfill",
    # 매주 일요일 10:00 KST — 스택 기동(cron 10:00) 직후, 장이 안 서는 날이라
    # 다른 수집과 경합하지 않는다. --multi-batch 적용 후 전 종목 전 이력 재확인이
    # 2분 24초로 끝나므로(과거 215분) 주 1회로 돌려 신규 상장 종목의 과거 이력을
    # 일주일 안에 따라잡는다. 폐지된 weekly_earnings(일요일)의 자리를 대신한다.
    schedule="0 10 * * 0",
    start_date=pendulum.datetime(2026, 7, 12, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["kr-quant", "collection", "earnings", "backfill"],
)
def earnings_backfill():

    @task(**{**DEFAULT_TASK_KW, "retries": 2, "retry_delay": timedelta(minutes=30)})
    def collect_earnings_backfill() -> None:
        run_collector(
            [
                sys.executable, "-m", "collectors.dart_earnings",
                # --multi-batch: fnlttMultiAcnt로 100종목씩 묶어 조회한다. 없으면
                # 종목별 fnlttSinglAcnt 루프로 떨어져 전 종목×전 분기가 ~21,000콜/
                # 215분이 된다(실측). 콜렉터엔 2026-07-12부터 있었으나 DAG가 플래그를
                # 안 넘겨 계속 느린 경로를 타고 있었다.
                "--db-table", "--multi-batch", "--all-codes", "--from-year", FROM_YEAR,
                # --knowledge-date avail: 이건 과거 백필이다. 기본값 today 로 두면
                # 오래전 공시된 값에 "오늘 알게 됨"이 찍혀, knowledge_date <= asof 로
                # 읽는 과거 시점 백테스트에서 그 행이 통째로 안 보인다. 기존 행은
                # upsert_earnings 가 건너뛰므로 무해해 보이지만, 새로 편입된 종목
                # (예: 폐지 종목 백필분)이 조용히 그 상태가 된다.
                # 위 --multi-batch 와 같은 실수의 반복이었다 — 콜렉터에 플래그를
                # 만들고 DAG 가 안 넘기는 것.
                "--knowledge-date", "avail",
                "--db", timescale_dsn(),
            ],
            env=dart_env(),
        )
        print("earnings 전체 이력 백필 실행 완료 (DB upsert, DART 일한도 도달 시 다음 트리거에서 자동 재개)")

    collect_earnings_backfill()


earnings_backfill()
