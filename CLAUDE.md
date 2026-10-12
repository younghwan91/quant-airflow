# CLAUDE.md — quant-airflow

kr_quant(TimescaleDB) 수집·운영 레포. 다른 레포(swing-it·scalp-it·daytrade-it)가 이 DB 와 이
레포의 `.env` 를 그대로 쓴다. **데이터 저장 방식과 DAG 가 목숨이다** — 코드는 다시 쓰면 되지만
오염된 이력·놓친 수집 창은 되돌리기 비싸고, DB 사본은 하나다(두 번째는 구글 드라이브 백업뿐).

## 0. 문서 — 언제 읽나
- `docs/schema.md` — 테이블·PK·단위·마이그레이션 목록. 테이블을 읽거나 바꾸기 전에.
- `docs/operations.md` — 24/7 단일 호스트, 배포 경로, 알림, 백업, swing-it 마운트 결합. 운영·크론·백업을 만지기 전에.
- `README.md` — 아키텍처·수집 목록. `docs/sharadar.md` — 미국(구독 해지로 꺼져 있다).
- DAG·마이그레이션 주석의 실측 근거("실측 40런 중 4런")는 그 설정의 이유다. 바꾸기 전에 읽고 아직 유효한지 확인한다.
- 섹션 번호 §0~§6 은 코드·마이그레이션·compose 주석이 `CLAUDE.md §N` 으로 참조한다 — 번호를 바꾸지 않는다(새 섹션은 뒤에).

## 1. 승인 받고 하는 것 / 묻지 않고 하는 것
먼저 **무엇이·왜·기존 데이터에 어떤 영향인지** 말하고 승인을 받는다("고쳐두는 게 낫겠다"는 안 통한다):
- 테이블 `DROP`/`TRUNCATE`/대량 `DELETE`, PK·upsert 키 변경, 컬럼 삭제, 타입 축소
- DAG `schedule` 변경·삭제, 수집 대상(종목·기간·컬럼) 축소, 압축 정책·청크 인터벌 변경
- 스케줄러·DB 컨테이너 기동·종료·재생성. **호스트 재부팅은 제안도 하지 않는다**(24/7, 사용자 결정 2026-10-08)

행을 지우거나 고칠 때(승인 뒤): `BEGIN` → 예상 행 수를 `DO $$ ... RAISE EXCEPTION` 으로 단언 → `COMMIT`.
지울 행은 버리지 말고 `legacy` 스키마로 옮긴다(migration 018·022 형식).

묻지 않고 하고 결과만 보고: 문서·주석 정정, 거짓 경보 제거, CI 초록인 디펜다봇·자기 PR 머지, 드라이브 백업
정리(휴지통행). 남은 질문 목록을 돌려주지 않는다 — 꼭 물을 것만 한 줄로 좁혀 묻는다.

## 2. 스키마 정본은 둘 — 둘 다 고친다
| 대상 | 정본 |
|---|---|
| Postgres/TimescaleDB(운영) | `sql/init_timescale.sql` + `sql/migrations/NNN_*.sql` |
| sqlite(로컬·테스트) | `collectors/storage.py` 의 `SCHEMA` |

`storage.connect()` 는 Postgres 에서 DDL 을 돌리지 않는다 — `storage.py` 에만 넣으면 운영 DB 엔 영원히 없다
(`backfill_markers` 전례, 4af34cd). 테이블·컬럼 추가 체크리스트:
1. `sql/migrations/NNN_*.sql` — 번호는 `git fetch` 후 origin/main 최댓값+1. 상단에 **왜**(실측), 하단에 **검증 쿼리·롤백**.
2. `sql/init_timescale.sql` 반영(새 DB 정본). 3. sqlite 도 쓰면 `SCHEMA` 반영. 4. `docs/schema.md` 갱신.

적용 기록 테이블이 없다 — 적용 여부는 하단 검증 쿼리로 본다. 호스트에 `psql` 이 없으니 파일 안 `psql "$KR_QUANT_DB"` 대신:
`docker exec -i quant-airflow-timescaledb-replica-1 psql -U "$TIMESCALE_USER" -d "$TIMESCALE_DB" -v ON_ERROR_STOP=1 < sql/migrations/NNN_x.sql`

수량 컬럼은 `BIGINT`(삼성전자 발행주식수 58억주가 `INTEGER` 를 넘친다). `ticks`·`minute_bars`·`quotes`·`quote_events`·
`vi_events`·`theme_*`(scalp-it), `kqc_runs`(krx-quant-core) 는 같은 DB 에 있지만 DDL 이 다른 레포에 있다 — 여기서 손대지 않는다.

## 3. 데이터 규약
- 모든 수집기는 `(code, date)` 류 upsert 로 **멱등**. 행마다 `source`(kiwoom/naver/krx/dart — 정의·정확도가 다르다).
- **NULL ≠ 0.** NULL = 모름, 0 = 없음(네이버 수급 `individual` 은 NULL 로 남긴다). **틀린 값보다 없는 값이 낫다** —
  원천이 틀린 값은 "고쳐" 넣지 말고 빼서 `legacy` 로(022).
- **point-in-time.** `earnings`·`shares_outstanding_history` 는 `knowledge_date` 로 정정 이력을 쌓는다(덮어쓰지 않는다).
  과거 백필은 `--knowledge-date avail` — 기본 `today` 면 과거 시점 백테스트에서 통째로 안 보인다.
  오늘 값을 과거로 복사하지 않는다(2017~2026 시총 전부 틀렸던 사고, migration 008).
- 쓰는 창(`--daily-days` 등)은 압축 경계 30일 안. 넘기면 upsert 마다 압축해제→재압축.
- 읽을 때 함정(상세 `docs/schema.md`): `daily_bars.trade_value` 는 **백만원**(`minute_bars` 는 원 — 1e6 배 문턱
  사고 2회), `supply_demand` 는 수량(주), `flu_rt` 는 수익률로 쓰지 않는다. `daily_bars` 는 수집 당시 값이라 기업행동
  이음매가 있다 — 연속 시계열은 `daily_bars_adjusted`. T 당일 16:00 값은 잠정, `daily_finalize`(화~토 06:30)가 확정치로 덮는다.
- **2026-09-14~ 일봉 3종·`supply_demand.close`·`acc_trde_qty` 의 고가·저가·종가·거래량은 KRX 애프터마켓(16:00~20:00) 포함 통합값,
  시가만 정규장**(NXT 아님, 네이버도 같은 통합값). **정규장 종가는 `regular_close` 뷰**(031 — 09-14 전 `supply_demand.close`,
  뒤는 틱반올림(`close`(t+1)/(1+`flu_rt`(t+1)/1e4)), t+1 수급 뒤에 채워지고 NULL 이면 `src` 가 이유) — `flu_rt` 기준가는 그대로다. `daily_finalize.check_data_quality`(`collectors/data_checks.py`)가 분봉 세션 범위·이 항등식·원주가 카나리아를 매일 검사한다.
- 소스 호출·파싱은 클라이언트 라이브러리(`~/git/kiwoom-client`·`krx-fundamentals-client`·`krx-news-client`)에 넣고,
  `collectors/` 는 조립·적재만 한다. 라이브러리 변경은 이미지 재빌드 전엔 안 들어온다.
- 분석·연구·매매 판정 코드는 여기 두지 않는다(swing-it 등 트레이더 레포). 이 레포는 원본 수집·적재만.
- 기사 LLM 은 전역 규칙을 따른다. 이 레포 구현은 `collectors/news_judge.py`(배치 원장 `news_judge_batches`,
  create 재시도 금지). Haiku 4.5 판정은 `legacy` 스키마 보관분 — 운영 테이블로 되돌리지 않는다.

## 4. DAG 규약
- `@dag(default_args=DAG_DEFAULT_ARGS)`(최종 실패 알림 콜백), 태스크는 `DEFAULT_TASK_KW`·`run_collector()`·`timescale_dsn()`·
  `*_env()`(`dags/_common.py`). `subprocess.run` 직접 호출 금지 — 로그가 통째로 유실되고 DSN 마스킹(`_masked`)을 우회한다.
- `start_date` 는 `catchup=False` 에서 **첫 구간이 끝나야** 첫 런이 뜬다 — 넉넉히 과거로(`daily_finalize` 10-08→10-01, cbd4e56).
- **키움 태스크 시각을 겹치지 않는다.** 같은 `KIWOOM_APP_KEY` 로 따로 로그인하면 뒤쪽이 앞 토큰을 무효화한다
  (`8005:Token이 유효하지 않습니다`). 화요일 10:00/10:05/10:10 간격이 그 가드다. scalp-it 크론도 같은 키를 쓴다 —
  시각을 정할 때 `grep -n KIWOOM ~/git/*/deploy/crontab.simnode` 로 확인.
- DAG 사이 순서는 시계가 아니라 `ExternalTaskSensor`(`*_price_adjust` 전례 — 35분 간격 가정이 175분짜리 날에 깨졌다).
- **켜고 끄는 상태는 코드에 둔다.** `airflow-init` 이 기동마다 `dags/*.py` 를 전부 unpause 한다. 안 돌릴 DAG 는
  `schedule=None`(`daily_krx_shares`·`daily_sharadar`). UI pause 는 다음 기동에 되살아나고, 반대로 paused 로 남은
  DAG 는 6일간 조용히 안 돌았다(2026-09-01).

## 5. "초록불 = 성공" 이 아니다
- **빈 응답을 성공으로 넘기는 수집기**가 반복 사고다: `daily_krx_shares` 22회 연속 rows=0 무실패, `daily_earnings`
  분기 창이 연 두 번 매일 `DONE rows=0`, DART corpCode 빈 응답 → rows=0 성공(be0bf6d). 새 수집기는 빈 응답에 예외를
  던지고, 매일 rows=0 은 장애로 본다.
- **실행 0회는 실패로 안 잡힌다**(paused·스케줄 누락). "고쳤다" 전에 그 DAG 가 실제로 돌았는지(`~/git/quant-airflow/logs/dag_id=<name>/`)
  와 대상 테이블 `max(date)` 가 움직였는지 본다.
- 헬스체크 커버리지는 daily_bars·supply_demand·credit_balance·sector_index·shares·short_selling 뿐 — `earnings`·`consensus`
  실패는 태스크 상태로만 잡힌다. 같은 응답이면 같은 곳에서 터지는 실패(`CardinalityViolation` 류, 4e2de05)는
  재시도로 안 낫는다 — 원인을 고친다.

## 6. 개발
런타임 의존은 `docker/requirements.txt`, `pyproject.toml` 은 ruff·pytest 설정 전용. 워크트리엔 `.venv` 가 없으니 운영 체크아웃 것을 쓴다:
```bash
V=~/git/quant-airflow/.venv/bin
$V/ruff check collectors/ dags/ tests/ scripts/   # ruff 0.14.0 핀 — CI 와 한 쌍
$V/python -m pytest -q
```
- CI(푸시 때 pre-push 가 로컬에서도 돌림)는 추가로 DAG AST 파싱·시크릿 스캔·커밋 신원 가드를 돈다.
- 개인 비영리 연구다(회사 파이프라인 아님, 2026-07-17 사용자 확인). 넘지 않는 선: 수집 데이터를 레포·공개 위치에 올리기,
  수집 데이터를 다시 공급하는 공개 API 띄우기, 로그인이 필요한 소스 긁기. 상업화(펀드·판매)하면 이 전제가 사라진다.
- 공개 레포다 — 키 모양 문자열·수집 데이터를 커밋하지 않는다. 스캐너(32자 hex·`api_key=` 16자+·DSN 비번 6자+)에 걸리는 테스트 가짜 시크릿은 그 줄에 `allowlist-secret`.
- 테스트는 sqlite·mock 경로라 **Postgres 전용 동작**(`ON CONFLICT` 한 문장 내 중복 키, `%s`/`?`, date 타입)을 못 잡는다
  (4e2de05·0068e64) — `storage.py` 의 PG 분기를 고치면 운영 DB 에서 읽기 전용 쿼리나 트랜잭션 롤백으로 확인한다.
- 수집기를 고치면 대응 `tests/test_*.py` 도. 커밋은 한국어 conventional(`fix(earnings): ...`), 개인 이메일 신원만.

## 7. 운영 체크아웃이 곧 운영 코드다
- `~/git/quant-airflow` 작업 트리를 Airflow 가 bind-mount 한다(`dags/`·`collectors/`·`scripts/`). 크론 줄도
  이 절대경로를 부른다. **거기서 브랜치 전환·미커밋 편집을 하지 않는다** — 작업은 워크트리에서.
- `../swing-it` 도 `/opt/swing-it:ro` 로 라이브 마운트된다(`*_price_adjust` 가 실행). swing-it 의
  `price_adjust.py` 가 numpy·pandas 밖을 import 하면 여기서만 죽는다(2026-09-14 사고).
- 반영 경로: main 푸시 → pre-push 가 `ci.yml` 을 로컬에서 그대로 돌림 → 이 호스트 `cron-install`.
  운영 체크아웃은 **07:50 `pull-all --ci-gate`** 때 ff 된다 — 워크트리에서 푸시했으면 그때까지 옛 코드·옛 크론이 돈다(푸시 직후 `cron-install` 은 운영 체크아웃의 파일을 읽는다).
  당장 반영하려면 `git -C ~/git/quant-airflow pull --ff-only`(깨끗할 때만).
- 자동 아님: compose env·`docker/requirements.txt`·`Dockerfile` → `docker compose -f docker-compose.airflow.yml
  up -d --build`(§1 승인), `sql/migrations/` → 손으로 적용. DAG 파일은 재파싱까지 최대 30분.
- 크론 정본은 `deploy/crontab.simnode` — `crontab -e` 로 고치면 다음 설치에 덮인다.
- 다른 레포 크론이 `export $(grep ... ../quant-airflow/.env | xargs)` 로 읽는다 — `.env` 키 이름을 바꾸거나
  지우지 않고, 값에 공백·따옴표를 넣지 않는다.

## 8. 상태 점검·DB 접속 (호스트에 psql 없음)
```bash
set -a; . ~/git/quant-airflow/.env; set +a
tail -40 ~/logs/quant-airflow/daily_health_check.log   # 11:35·18:10 헬스체크(⚠️ 줄)
tail ~/logs/quant-airflow/alerts.log                   # 웹훅 URL 이 없으면 알림은 여기에만 쌓인다
docker exec quant-airflow-airflow-scheduler-1 airflow dags list-import-errors
docker exec quant-airflow-timescaledb-replica-1 psql -U "$TIMESCALE_USER" -d "$TIMESCALE_DB" -Atc "select ..."
docker exec quant-airflow-airflow-meta-db-1 psql -U "$AIRFLOW_META_USER" -d "$AIRFLOW_META_DB" -Atc "select ..."
~/git/quant-airflow/.venv/bin/python   # psycopg2 있음 — psycopg2.connect(os.environ["KR_QUANT_DB"])
```
컨테이너 이름은 `...-replica-1` 이지만 **PRIMARY** 다(2026-09-11 `pg_promote`, 확인은 `select pg_is_in_recovery()` = f).
