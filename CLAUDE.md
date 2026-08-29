# CLAUDE.md — 이 저장소에서 일하는 규칙

> **데이터 저장 방식과 DAG 가 이 프로젝트의 목숨이다.** 코드는 다시 쓰면 되지만
> 오염된 이력과 놓친 수집 창은 되돌리기 비싸다. 아래 규칙은 그 전제에서 나온다.

## 0. 손대기 전에 읽을 것

- 아키텍처·수집 목록: [`README.md`](README.md)
- 테이블·PK·압축 설계·마이그레이션: [`docs/schema.md`](docs/schema.md)
- 가동 창·시크릿·저장소 구조: [`docs/operations.md`](docs/operations.md)
- 미국 파이프라인: [`docs/sharadar.md`](docs/sharadar.md)

이 레포의 DAG·마이그레이션 주석에는 **실측 근거**가 들어 있다("실측 40런 중 4런",
"pg_stat 에 churn 이 그대로"). 그건 장식이 아니라 그 설정을 그렇게 둔 이유다.
바꾸기 전에 그 주석부터 읽고, 근거가 여전히 유효한지 확인한다.

## 1. 승인 없이 하지 않는 것

- 테이블 `DROP` / `TRUNCATE` / 대량 `DELETE`
- PK·upsert 키 변경, 컬럼 삭제, 타입 축소
- DAG `schedule` 변경, DAG 삭제, 수집 대상(종목·기간·컬럼) 축소
- 압축 정책·청크 인터벌 변경
- 스케줄러/컨테이너 기동·종료(운영 중인 창을 자를 수 있다)

바꿔야 한다고 판단되면 **먼저 무엇이·왜·기존 데이터에 어떤 영향인지** 말하고
승인을 받는다. "고쳐두는 게 낫겠다" 는 이 항목들에서는 통하지 않는다.

## 2. 스키마의 정본은 한 곳이 아니다 — 둘 다 고쳐야 한다

| 대상 | 정본 |
|---|---|
| **Postgres/TimescaleDB** (운영 DB) | `sql/init_timescale.sql` + `sql/migrations/*.sql` |
| sqlite (로컬·테스트 경로) | `collectors/storage.py` 의 `SCHEMA` 상수 |

`storage.connect()` 는 **Postgres 경로에서 DDL 을 돌리지 않는다.** 그래서
`storage.py` 에만 테이블을 추가하면 운영 DB 에는 영원히 안 생긴다 —
`backfill_markers` 가 실제로 그렇게 빠져 있었다(2026-08-30 수정).

테이블·컬럼을 더할 때 체크리스트:

1. `sql/migrations/NNN_*.sql` 새로 작성 — 상단 주석에 **왜**(실측 근거 포함),
   하단에 **검증 쿼리**와 **롤백**을 적는다(기존 파일들이 전부 그 형식이다).
2. `sql/init_timescale.sql` 에 같은 것을 반영 — 새 DB 를 세울 때의 정본이다.
3. sqlite 경로도 쓰면 `collectors/storage.py` 의 `SCHEMA` 에 반영.
4. `docs/schema.md` 표와 마이그레이션 목록 갱신.

타입 주의: Postgres 쪽 수량 컬럼은 `BIGINT` 다. `INTEGER`(32bit) 로 두면
삼성전자 발행주식수(58억주)가 오버플로우한다.

## 3. 데이터 규약

- **모든 수집기는 `(code, date)` upsert 로 멱등이어야 한다.** 재실행이 안전하지
  않은 수집기는 만들지 않는다.
- **행마다 `source` 를 남긴다.** kiwoom / naver / krx / dart 는 정의와 정확도가
  다르다(네이버 폐지분은 거래대금이 근사, 수급은 기관·외국인만).
- **NULL 과 0 을 구분한다.** NULL = 모름, 0 = 없음. 네이버 수급의 `individual`
  은 NULL 로 남아야 하고, 0 으로 채우면 신호가 조용히 오염된다.
- **point-in-time 을 깨지 않는다.** `earnings` 는 `knowledge_date` 로 정정 이력을
  쌓고(덮어쓰지 않는다), `shares_outstanding_history` 도 마찬가지다. 과거 백필은
  `--knowledge-date avail` 을 넘겨야 한다 — 기본값 `today` 면 그 행이 과거 시점
  백테스트에서 통째로 안 보인다.
- **오늘 값을 과거로 복사하지 않는다.** 그 lookahead 로 2017~2026 구간 시총이
  전부 틀렸던 사고가 있다(migration 008 ③). **틀린 값보다 없는 값이 낫다.**
- 쓰는 창(`--daily-days` 등)은 압축 경계(30일) 안에 둔다. 넘기면 upsert 마다
  압축해제→갱신→재압축이 돈다.

## 4. DAG 규약

- 새 DAG·태스크는 `dags/_common.py` 의 `DEFAULT_TASK_KW`·`run_collector()`·
  `timescale_dsn()`·`*_env()` 를 쓴다. 자격증명은 Airflow Fernet Variables 에만
  있고 수집 subprocess 에만 주입된다 — 태스크 로그에 DSN 을 찍지 않는다
  (`_masked()` 가 막고 있다).
- **키움 태스크를 같은 시각에 겹치지 않는다.** TR 버킷이 달라도 같은
  `KIWOOM_APP_KEY` 로 각각 로그인하면 나중 쪽이 앞 토큰을 무효화한다
  (`8005:Token이 유효하지 않습니다`). 화요일 아침 10:00 / 10:05 / 10:10 의 5분
  간격은 장식이 아니라 그 가드다.
- **DAG 사이 순서는 시계가 아니라 `ExternalTaskSensor` 로 보장한다.**
  `weekly_price_adjust` 가 그 예다 — 35분 간격이라는 암묵 가정은 앞 태스크가
  175분 걸린 날 깨졌다.
- 스케줄을 정할 때 **가동 창**을 함께 본다(`docs/operations.md`). 창 밖으로
  삐져나가는 DAG 는 22:00 안전장치에 잘린다(`earnings_backfill` 전례).
- 새 DAG 를 추가·unpause 하면 `scripts/wait_and_stop.sh` 가 그걸 기다리는지
  확인한다(메타DB 를 직접 물으므로 보통 자동이지만, 창의 **지평선** 안에
  들어오는지는 사람이 확인해야 한다).

## 5. "초록불 = 성공" 이 아니다

이 레포에서 반복된 실패 모드다. 무언가를 고쳤다고 말하기 전에 확인한다:

- `daily_krx_shares` 는 **22회 연속 rows=0 이면서 한 번도 실패하지 않았다.**
  빈 응답을 성공으로 처리하던 수집기 때문이다 — 새 수집기는 빈 응답에
  예외를 던진다.
- `wait_and_stop.sh` 의 종료 조건은 "예정된 런이 더 없다" 이지 "다 성공했다" 가
  아니다. `report_failures()` 가 남기는 `⚠️ 오늘 실패한 태스크` 줄을 본다.
- 커버리지 점검 6개 테이블에 `earnings`·`consensus` 는 없다 — 그쪽 실패는
  태스크 상태로만 잡힌다.

## 6. 개발

이 레포는 배포되는 패키지가 아니다 — `pyproject.toml` 에 `[project]` 가 없고
도구 설정(ruff·pytest)만 들어 있다. 런타임 의존은 `docker/requirements.txt` 다.
CI 와 같은 방식으로 돌린다:

```bash
pip install -r docker/requirements.txt
pip install pytest 'ruff==0.14.0'     # ruff 버전은 핀한다 (CI 와 한 쌍)

ruff check collectors/ dags/ tests/ scripts/
pytest
```

CI 는 여기에 더해 **DAG AST 파싱**(스케줄 오타·깨진 import)과 **시크릿 스캔**,
**커밋 신원 가드**(회사 이메일이 섞이면 실패)를 돌린다. 이 레포는 공개다 — 키
모양 문자열을 추적 대상 파일에 넣지 않는다.

수집기를 고치면 대응하는 `tests/test_*.py` 를 함께 고친다. DB 를 실제로 때리는
검증이 필요하면 마이그레이션 하단의 검증 쿼리 형식을 따른다.

커밋 신원은 CI 가드가 검사한다 — 회사 이메일이 섞이면 실패한다.
