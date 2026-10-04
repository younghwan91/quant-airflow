# 알림 통로 하나 — 사고를 로그 파일이 아니라 사람에게 보낸다

**결정일**: 2026-10-05
**상태**: 설계 승인(사용자: "계획을 마치면 자율적으로 실행해") — 구현 진행
**상위 맥락**: "두 노드 협업·자동화" 4개 하위 프로젝트 중 **1번**. 나머지는
2 배포=푸시, 3 DAG 실패 자가치유, 4 리플리카 자동 복구 — 셋 다 "보고할 곳"이
있어야 성립하므로 이게 먼저다.

## 왜 만드는가

이 레포의 사고는 전부 **사람이 로그를 읽어야만 발견된다.** 2026-10-04 실측:

- simnode 크론 35개, `daily_health_check.sh`, `backup_to_gdrive.sh`, 16개 DAG 의
  실패 — 종착지가 전부 `~/logs/**/*.log` 또는 Airflow 태스크 로그다.
- `dags/_common.py` 의 `DEFAULT_TASK_KW` 에 `on_failure_callback` 이 없다.
  Airflow 는 실패를 UI 에 빨간 네모로만 둔다.
- 양쪽 노드 어디에도 Discord/Telegram/Slack/ntfy 전송 코드가 없다(dotfiles 포함).

그 결과로 실제 놓친 것:

| 사고 | 발생 | 발견 | 발견 경위 |
|---|---|---|---|
| trader 리플리카 슬롯 `active=false`·`wal_status=extended` | 09-19~21 (사흘) | 09-23 | 세션에서 로그를 열어봄 |
| `daily_news.collect_dart_disclosures` 재시도까지 실패 (CardinalityViolation) | 09-29 16:05 | 10-04 | 사용자 부재 11일 뒤 세션 점검 |
| 헬스체크 커버리지 "전종목 2,651 누락" 오보 | 09-13~10-02 (매 영업일) | 09-23 | 같은 세션 |
| 백업 `cd: No such file` 조용한 실패 | 09-12 | 09-13 | 다음날 사람이 로그 확인 |

공통 분모는 사고 종류가 아니라 **발견 경로가 없다**는 것이다. 이 스펙은 그 경로
하나를 만든다. 사고 자체를 줄이는 건 3·4번의 몫이다.

## 범위

**안에 있는 것** — 알림을 **보내는** 발생지 5곳과 전송기 1개:

1. 전송기 `collectors/alert.py` (+ bash 래퍼 `scripts/alert.sh`, `scripts/cron_run.sh`)
2. Airflow 태스크 **최종 실패** (재시도 소진 후) — `dags/_common.py`
3. `scripts/daily_health_check.sh` 의 `⚠️` 줄 전부 + `daily_bars` 누락 ≥ 100
4. `scripts/backup_to_gdrive.sh` 비정상 종료
5. `deploy/crontab.simnode` 의 quant-airflow 크론 2줄(헬스체크·백업)의 **스크립트
   자체가 뜨기 전 실패**(09-12 유형)

**밖에 있는 것** (명시):

- trader 의 scalp-it/daytrade-it 크론 — 다른 레포. `scripts/alert.sh` 를 그쪽에서
  호출할 수 있게 만들어 두지만(그 레포들은 이미 `../quant-airflow/.env` 를
  source 한다) 적용은 각 레포에서 한다.
- 중복 억제·집계 — 헬스체크는 하루 2회라 같은 경보가 두 번 올 수 있다. 허용한다.
  실측으로 시끄러워지면 그때 넣는다(YAGNI).
- 커버리지 임계값 전반, "커버리지 점검을 실패하는 DAG 태스크로 승격" — 3번.
- Airflow `on_retry_callback`(재시도 시마다 알림) — 재시도로 낫는 실패는 알릴
  가치가 없다. 최종 실패만.
- `sla_miss_callback`, 성공 알림 — 안 한다.

## 접근 — 왜 "전송기 하나 + 발생지마다 명시적 훅"인가

| 접근 | 장점 | 단점 | 판정 |
|---|---|---|---|
| **A. 전송기 하나 + 명시적 훅** | 의존성 없음(stdlib·curl). 발생지마다 10줄 안팎. URL 없으면 조용히 통과. 이 레포 방식(명시적, 주석에 근거)과 맞음 | 발생지가 늘 때마다 한 줄씩 손대야 함 | **채택** |
| B. 전부 Airflow 로 모아 콜백 하나 | 발생지가 하나 | 백업은 호스트에서 돌아야 할 이유(tmpfs 스테이징·컨테이너 이름)가 있고, 구조를 흔든다 — 2~4번 범위 | 보류 |
| C. 로그 수집 데몬(vector 등) → 알림 | 코드 안 고침 | 두 노드에 데몬 하나씩 추가. `⚠️` 문자열 매칭이라 깨지기 쉬움. 디버깅 층이 하나 더 | 기각 |

채널은 **Discord 웹훅**. 근거: 사용자가 Discord 를 이미 쓴다(`~/.claude/channels/discord`,
2026-03). 웹훅은 봇 토큰·권한 없이 URL 하나로 POST 한 번이라 bash 크론·Airflow
콜백·헬스체크 어디서든 같은 코드로 쏜다. Claude 채널의 봇 토큰(trader 에만 있음)과
운영 알림을 섞지 않는다.

**URL 은 아직 없다**(사용자: "나중에"). 그래서 모든 경로는 `ALERT_WEBHOOK_URL`
이 비어 있으면 **로컬 로그에만 쓰고 종료코드 0** 이어야 한다 — 코드는 지금
배포되고, URL 은 들어오는 날 `.env` 한 줄로 켜진다.

## 구성 요소

### 1. `collectors/alert.py` — 유일한 전송기

```
notify(level: str, title: str, body: str = "") -> bool
    level: "warn" | "error" | "info"
    반환: 웹훅 전송 성공 여부. URL 없음·전송 실패 모두 False — 예외는 절대 안 올린다.

format_message(level, title, body, *, host) -> str
    순수 함수. 테스트 대상.

CLI:  python3 collectors/alert.py <level> <title> [body]    # body 없으면 stdin
```

규약:

- **절대 예외를 올리지 않는다.** 알림이 본작업(수집·백업)을 죽이면 알림이 사고가
  된다. 네트워크·JSON·env 어떤 실패도 `print` 한 줄로 삼키고 `False`.
- **stdlib 만** (`urllib.request`, `json`, `socket`, `os`). 호스트 python3(크론)와
  컨테이너 python 양쪽에서 같은 파일이 돈다. `requests` 를 쓰면 호스트에서 깨진다.
- **항상 로컬 로그에도 쓴다.** `ALERT_LOG`(기본 `~/logs/quant-airflow/alerts.log`)에
  한 줄 — URL 이 있든 없든. 이게 감사 추적이고, URL 없는 기간의 유일한 기록이다.
  디렉터리는 만든다. 쓰기 실패도 삼킨다.
- **시크릿 마스킹.** 본문은 `collectors.config.mask_secrets` 를 거친다 —
  CalledProcessError 메시지나 백업 로그 꼬리에 DSN 이 섞여 올 수 있다. 이 레포는
  공개 레포고 Discord 채널은 공개 레포보다도 넓게 복사된다.
- **Discord 한도.** `content` 2,000자. 본문은 1,800자에서 자르고 `…(잘림, 전체는
  로그)` 를 붙인다. 코드 블록(```)으로 감싼 뒤 자르면 블록이 안 닫히므로 **자른 뒤
  감싼다.**
- **메시지 모양.** `[{host}] {emoji} {title}` 첫 줄, 빈 줄, 본문 코드 블록. host 는
  `socket.gethostname()` — 두 노드가 한 채널을 쓰므로 어디서 난 일인지가 첫 글자다.
  emoji: warn ⚠️ / error 🔴 / info ℹ️.
- **타임아웃 10초.** 크론·콜백 안에서 도는 코드다. 매달리면 안 된다.
- `ALERT_WEBHOOK_URL` 은 `os.environ` 에서만 읽는다. `.env` 파싱은 하지 않는다 —
  호스트에서는 래퍼(`alert.sh`)가, 컨테이너에서는 compose 가 넣어준다.

`collectors/` 에 두는 이유: `dags/_common.py` 가 import 할 수 있고(이미
`collectors.proc`·`collectors.config` 를 import 한다), 테스트가 Airflow 없이 돈다
(CI 는 Airflow 를 설치하지 않는다 — `.github/workflows/ci.yml`). `proc.py` 가
같은 이유로 거기 내려간 전례가 있다.

### 2. `scripts/alert.sh`, `scripts/cron_run.sh` — bash 래퍼

`alert.sh <level> <title> [body]` (body 없으면 stdin):
- 레포 루트를 자기 위치에서 유도(`backup_to_gdrive.sh` 와 같은 수법 — 09-12 사고의
  교훈). `.env` 에서 `ALERT_WEBHOOK_URL`·`ALERT_LOG` **두 키만** `grep` 으로 읽어
  export (`daily_health_check.sh` 의 `env_get` 과 같은 이유로 `.env` 전체를 source
  하지 않는다 — 시크릿이 무관한 자식에 퍼진다).
- `exec python3 "$REPO/collectors/alert.py" "$@"`. 항상 0 으로 끝난다(`alert.py`
  규약). python3 가 없으면 로그 한 줄 남기고 0.

`cron_run.sh <이름> -- <명령...>`:
- 명령을 돌리고 출력을 **그대로 stdout 으로 흘린다**(크론의 `>> log` 리다이렉트가
  지금처럼 동작해야 한다). 동시에 임시 파일에 복사(`tee`).
- 종료코드 ≠ 0 이면 `alert.sh error "<이름> 실패 (rc=N)"` 에 출력 **꼬리 30줄**을
  본문으로. 그리고 **원래 종료코드로 끝난다** — 크론 로그 관점에선 아무것도 안
  바뀐다.
- 이게 잡는 것: 스크립트 안의 핸들러가 돌기 전에 죽는 경우(`cd` 실패, 인터프리터
  없음, 권한). 스크립트 안 핸들러(3·4)와 **겹치면 알림이 두 번** 간다 — 백업 EXIT
  트랩이 알리고 rc≠0 으로 나가면 `cron_run.sh` 가 또 알린다. 이걸 피하려고
  스크립트 쪽 핸들러를 빼지 않는다(핸들러는 **왜** 죽었는지 안다, 래퍼는 꼬리만
  안다). 대신 백업 트랩은 알린 뒤 **환경변수 `ALERT_SENT=1` 을 세울 수 없다**(자식
  → 부모로 못 넘긴다). 그래서 규칙은 단순하게: **`cron_run.sh` 는 꼬리 30줄 안에
  `[alert.py]` 마커가 있으면 중복으로 보고 보내지 않는다.** `alert.py` 는 로컬
  로그와 **stdout 에도** 마커 한 줄을 찍는다(`[alert.py] sent|logged-only <title>`).
  문자열 매칭이지만 자기 자신이 찍은 마커라 외부 포맷에 의존하지 않는다.

### 3. `dags/_common.py` — 최종 실패 콜백

```python
def alert_task_failure(context) -> None:   # on_failure_callback
DEFAULT_TASK_KW = {"retries": 1, "retry_delay": ..., "on_failure_callback": alert_task_failure}
```

- Airflow 는 `on_failure_callback` 을 **재시도를 다 쓴 뒤 최종 failed 로 갈 때만**
  부른다(재시도 시엔 `on_retry_callback`). 그래서 이것 하나로 "진짜 실패만 알림"이
  된다 — 09-29 공시 실패는 attempt 2 가 죽은 10:15 에 한 번 왔을 것이다.
- `upstream_failed` 는 콜백이 안 불린다(그 상태는 실행된 적이 없다). 09-29
  `judge_news` 가 그 예 — 상류가 알렸으니 맞다.
- 메시지 조립은 `collectors.alert.format_task_failure(dag_id, task_id, run_id,
  try_number, exc_text, log_url)` 순수 함수에 두고 `_common` 은 context 에서
  값만 꺼내 넘긴다 — Airflow 없이 테스트하기 위해서다(`_common` 은
  `airflow.models.Variable` 을 import 해 CI 에서 import 자체가 안 된다).
- `exc_text` 는 `str(context["exception"])` 첫 300자. `run_collector` 가 던지는
  `CalledProcessError` 는 이미 `_masked(cmd)` 로 만들어져 DSN 이 없지만, 어차피
  `alert.py` 가 한 번 더 마스킹한다.
- `log_url` 은 `context["task_instance"].log_url` — 웹서버는 LAN 안이라 밖에서는
  안 열리지만, 채널에서 바로 클릭해 들어가는 용도로 충분하다.
- 콜백 본문은 통째로 `try/except Exception: print(...)` — 콜백이 던지면 Airflow 는
  로그에 남기고 넘어가지만, 깔끔하게 삼킨다.
- **직접 `retries=` 를 적은 6개 태스크**(`daily_sharadar` 2, `earnings_backfill` 1,
  `weekly_delisted_stocks` 3)는 `@task(**{**DEFAULT_TASK_KW, "retries": 2,
  "retry_delay": timedelta(minutes=30)})` 꼴로 바꾼다. `DEFAULT_TASK_KW` 주석이
  말하는 "일부러 다른 값만 눈에 띈다"는 의도가 그대로 살고, 콜백이 빠지지 않는다.
  **retries/retry_delay 값은 하나도 바꾸지 않는다**(CLAUDE.md §1 — 스케줄·수집
  동작 변경 금지. 재시도 정책은 그 경계 안쪽으로 본다).
- `daily_krx_shares`·`daily_sharadar` 는 `schedule=None` 이라 안 돌지만 코드는
  같은 규약을 따른다 — 켜는 날 콜백도 같이 켜져야 한다.

### 4. `scripts/daily_health_check.sh` — ⚠️ 모아서 1건

- `warn()` 함수 추가: `log "⚠️ $*"` 하고 전역 배열 `WARNINGS` 에 push. 기존
  `log "⚠️ ..."` 호출 전부를 `warn "..."` 로 바꾼다(문자열은 그대로).
- 커버리지 표 뒤에 **`daily_bars` 누락 ≥ 100 이면 `warn`** 하나 추가. 100 인 이유:
  정상 범위는 한 자리(상폐 제외 후 09-22 실측 3), 거래정지가 몰려도 수십이다.
  2,600 전종목은 "16:00 수집이 통째로 안 됐다"는 뜻이고 그건 지금 표로만 찍히고
  끝난다. 다른 테이블 임계값은 3번에서.
- 스크립트 끝에서 `WARNINGS` 가 비어 있지 않으면 `alert.sh warn "헬스체크 경보
  N건"` 에 줄들을 본문으로 **한 번** 보낸다. 실행당 1건이 원칙 — 리플리카 경보처럼
  두 줄이 세트로 나오는 걸 쪼개면 채널이 시끄럽다.
- 주말·오전 "건너뜀" 은 `log` 그대로(경보 아님).

### 5. `scripts/backup_to_gdrive.sh` — 비정상 종료

- 현재 `trap 'rm -rf "$TMPDIR"' EXIT` 하나. 이걸 함수 `on_exit()` 로 바꿔 rc 를
  먼저 잡고 → 정리 → rc≠0 이면 `alert.sh error "백업 실패 (rc=N, BACKUP_ONLY=…)"`.
  본문은 **스크립트 자신의 stdout 을 모을 수 없으므로**(크론이 파일로 보낸다)
  `tail -n 30` 을 크론 로그 파일에서 읽는다 — 그 경로는 크론 라인이 정하므로
  `BACKUP_LOG` 환경변수로 받고 없으면 본문 없이 제목만. `set -e` 환경에서 트랩
  안의 실패가 다시 트랩을 부르지 않도록 트랩 안에서는 `|| true` 로 감싼다.
- 성공 알림은 없다. "오늘 백업이 왔나"는 로그가 답한다(3번에서 "N시간 안에 성공
  마커가 없으면 경보"로 뒤집을 수 있다 — 지금은 아니다).

### 6. 배선

- `docker-compose.airflow.yml` `x-airflow-common-env` 에 `ALERT_WEBHOOK_URL:
  ${ALERT_WEBHOOK_URL:-}` 과 `ALERT_LOG: /opt/airflow/logs/alerts.log` 추가.
  **컨테이너 재생성은 하지 않는다**(CLAUDE.md §1). URL 이 들어오는 날 승인 받고
  `docker compose -f docker-compose.airflow.yml up -d` 로 env 를 재적용한다. 그
  전까지 콜백은 "URL 없음 → 태스크 로그에 `[alert.py] logged-only`" 로 동작하고
  그건 의도된 상태다.
- `.env.example` 에 `ALERT_WEBHOOK_URL=` 블록(어디서 발급하는지 한 줄).
- `deploy/crontab.simnode` 의 헬스체크 2줄·백업 1줄을 `scripts/cron_run.sh <이름>
  -- <기존 명령>` 으로 감싼다. 리다이렉트(`>> log 2>&1`)는 바깥에 그대로. 백업
  줄에는 `BACKUP_LOG=<그 로그 경로>` 를 앞에 둔다 — cron-install 은 `VAR=value`
  **단독 줄**만 거부하고 명령 앞 접두는 받는다(기존 줄들이 `cd … && export …`
  를 쓰는 것과 같은 자리).
- `docs/operations.md` 에 "## 알림" 절: 채널·키·URL 없을 때 동작·발생지 5곳·
  URL 을 넣는 날 할 일(양쪽 `.env` + compose 재적용 승인).

## 데이터 흐름

```
[발생지]                          [전송기]                    [목적지]
Airflow task 최종 실패 ─ context ─┐
daily_health_check ⚠️ 모음 ──────┤
backup EXIT rc≠0 ────────────────┼→ alert.sh → alert.py ──┬→ ALERT_LOG (항상)
cron_run.sh rc≠0 (꼬리 30줄) ────┤   (컨테이너는 직접)     └→ Discord webhook (URL 있을 때)
                                 │
                 mask_secrets → 1,800자 절단 → [host] emoji title
```

## 오류 처리 — "알림이 사고가 되지 않는다"

| 상황 | 동작 |
|---|---|
| `ALERT_WEBHOOK_URL` 비어 있음 | 로컬 로그만, stdout 마커 `logged-only`, 반환 False / 종료 0 |
| 웹훅 4xx/5xx/타임아웃/DNS | `print` 한 줄, 로컬 로그에는 이미 씀, 반환 False / 종료 0 |
| 로컬 로그 디렉터리 못 만듦 | 삼킨다 (stdout 마커는 남는다) |
| 콜백 안 예외 | 삼키고 태스크 로그에 한 줄. 태스크 상태에 영향 없음 |
| `cron_run.sh` 의 `alert.sh` 자체 실패 | 원래 종료코드 그대로 반환 — 래퍼가 종료코드를 바꾸는 일은 없다 |
| 본문에 DSN/API 키 | `mask_secrets` — `postgresql://u:***@`, `api_key=***` |

## 테스트

- `tests/test_alert.py`
  - `format_message`: host 접두·emoji·코드블록·1,800자 절단 뒤 블록이 닫힘·
    `mask_secrets` 적용(DSN 이 든 본문).
  - `format_task_failure`: 필드 전부 포함, `exc_text` 300자 절단.
  - `notify` URL 없음: 웹훅 호출 0회, 로그 파일에 한 줄, False. (`monkeypatch` 로
    `ALERT_LOG` 를 tmp 로.)
  - `notify` URL 있음: `urllib.request.urlopen` 을 패치해 payload 모양(`content`
    키, 2,000자 이하) 확인. urlopen 이 예외를 던져도 False 만 돌아오고 안 올라감.
  - CLI: `subprocess.run([sys.executable, "collectors/alert.py", ...])` 종료 0,
    stdin 본문.
- `tests/test_cron_run.py` (bash 는 CI 러너·호스트 양쪽에 있다)
  - 성공 명령 → 종료 0, 알림 로그 없음, 출력이 stdout 에 그대로.
  - 실패 명령(`exit 3`) → 종료 **3**, `ALERT_LOG` 에 `error` 한 줄, 본문에 꼬리.
  - 꼬리에 `[alert.py]` 마커가 있는 실패 → 중복 알림 없음.
- `dags/` 변경은 CI 의 AST 파싱이 import 깨짐을 잡는다. 콜백이 실제로 불리는지는
  배포 후 **의도적으로 실패하는 1회성 수동 트리거**로 확인하지 않는다 — 운영
  스케줄러를 건드리는 일이라 §1 범위. 대신 다음 실제 실패 때 `alerts.log` 에 줄이
  생기는지 본다(URL 없는 기간에도 그 줄은 생긴다).
- 쉘: `bash -n` 두 스크립트, `ruff` 는 기존대로.

## 배포와 적용 순서

1. 이 브랜치 → `main` 푸시(pre-push ci-local 통과). simnode 는 동기화로 받는다.
   `collectors/`·`scripts/`·`dags/` 는 bind-mount 라 **다음 실행부터** 새 코드.
   `dags/_common.py` 변경은 스케줄러의 DAG 재파싱(최대 30분,
   `MIN_FILE_PROCESS_INTERVAL=1800`)으로 반영 — 재기동 없음.
2. `cron-install` 이 07:50 에 crontab 줄을 바꾼다(또는 손으로 한 번).
3. **URL 이 오는 날**: 양쪽 `.env` 에 `ALERT_WEBHOOK_URL=…` → simnode 에서
   `alert.sh info "알림 통로 개통"` 로 즉시 확인 → 사용자 승인 후 airflow compose
   `up -d`(env 재적용). 이 세 단계는 operations.md 에 그대로 적는다.

## 검증 — "초록불 = 성공" 이 아니다 (CLAUDE.md §5)

구현이 끝났다고 말하기 전에 확인할 것:

- simnode 에서 `scripts/alert.sh warn "테스트"` → `~/logs/quant-airflow/alerts.log`
  에 줄이 생기고 종료 0.
- simnode 에서 `scripts/cron_run.sh 테스트 -- false` → 종료 1, alerts.log 에
  `error` 줄.
- 다음 18:10 헬스체크 로그에 `[alert.py] logged-only` 마커가 있다(경보가 있는
  날) 또는 없다(없는 날) — 둘 중 하나가 **의도대로**.
- Airflow 웹 UI 의 DAG 코드 뷰에서 `_common.py` 가 새 내용으로 파싱됐고
  import error 가 0 이다(`import_error` 테이블).
