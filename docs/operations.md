# 운영 — 가동 창 · 시크릿 · 저장소 구조

[← README](../README.md)

## 머신 가동은 cron 이 관리한다

스페어 PC 를 24시간 켜 두지 않는다. **창 셋으로 나눠 띄운다** — 오전 창은 매일,
저녁 창은 평일용과 토요일용이 따로 있어 하루에 최대 두 창이 뜬다.

| 창 | 기동 | 대상 |
|---|---|---|
| 1 — 오전 수집 | 매일 10:00 (지평선 11:30) | catchup · short_credit · listed_shares · 주간/월간 백필 |
| 2 — 평일 저녁 | 평일 15:55 | daily_collection(16:00) · price_adjust(16:55) · consensus(17:00) · sharadar(화~금 17:30) |
| 3 — 토요일 저녁 | 토 17:20 | sharadar(17:30) 하나 |

각 창은 `scripts/wait_and_stop.sh` 가 "그 지평선까지 예정된 DAG 가 전부 끝났는가"를
Airflow 메타DB 에 물어 조기 종료한다(안전장치 포함).

**왜 한 창이 아닌가.** 예전엔 10:00 기동 → 마지막 DAG 까지 한 창이었는데, 실측 가동
521분 중 작업이 138분(27%)이고 **380분(73%)이 오전 수집과 저녁 수집 사이의 유휴**였다.
오전 DAG 는 전날 확정 데이터를 받으므로 미룰 수 없고, 저녁 DAG 는 장마감(15:30)
이후에만 데이터가 나오므로 앞당길 수 없다 — 스케줄을 붙이는 건 원리적으로 불가능하고
창을 나누는 게 유일한 답이다.

> **완료 ≠ 성공.** 종료 조건은 "예정된 런이 더 없다"라서 실패한 런도 끝난 런으로
> 세어진다. 실제로 실패한 런을 남긴 채 `모두 완료 — 컨테이너 종료` 가 찍힌 날이
> 있었다. 지금은 종료 직전 `report_failures()` 가 그날 실패한 (dag_id, task_id) 를
> 로그에 남긴다.
>
> ⚠️ `wait_and_stop.sh` 는 **airflow 4종(스케줄러·웹서버·init·메타DB)만 내린다.**
> `timescaledb` 는 이 저장소 전용이 아니다 — 다른 프로젝트의 장중 틱 수집이 같은
> 컨테이너를 쓰고, crontab 의 `db_guard.sh` 가 평일 08:00~15:55 살아 있는지 지킨다.
> `docker compose stop` 으로 전부 내리면 오전 창을 닫는 순간 **장중에 남의 수집 DB 를
> 죽인다.**

## 시크릿 처리

`KIWOOM_APP_KEY`/`KIWOOM_APP_SECRET`(실계좌 키)와 `DART_API_KEY`(_2/_3)는
`airflow-webserver`·`airflow-scheduler` 컨테이너에 평문 env 로 전달되지 **않는다.**
`airflow-init` 컨테이너가 이 값을 한 번만 읽어 `airflow variables set` 으로 Airflow
메타DB 에 **Fernet 암호화**해 저장하고, DAG 태스크는 실행 시점에 `Variable.get()` 으로
꺼내 수집기 서브프로세스 환경에만 주입한다(`dags/_common.py` 의
`kiwoom_env()`·`dart_env()`). 로그로 새는 것은 `collectors/proc.py` 의 줄 단위
마스킹과 `config.mask_dsn` 이 막는다.

TimescaleDB 접속 정보(`TIMESCALE_*`)는 LAN 내부용이라 평문 컨테이너 env 로 두었다.
필요하면 Airflow Connection 으로 옮길 수 있지만, 지금 범위에서는 과하다고 판단했다.

`.env` 는 절대 커밋하지 않는다(`.gitignore` 에 포함).

## 저장소 구조

```
dags/                  # 14개 DAG — run_collector()로 `python -m collectors.X` 실행
  _common.py           #   공유 헬퍼: timescale_dsn()/kiwoom_env()/dart_env()/run_collector()
collectors/            # 수집 로직 자체 보유 (kr_quant 런타임 의존 없음)
  storage.py           #   스키마 + upsert 전체 (sqlite/Postgres 듀얼 백엔드)
  config.py            #   자격증명 로딩 + 키움 클라이언트 생성 + DSN 마스킹 + DART 키 목록(정본)
  kiwoom_cli.py        #   전종목 스윕 콜렉터 공통 CLI(인자·세션·유니버스·배너)
  proc.py              #   자식 프로세스 스트리밍 + 줄 단위 시크릿 마스킹(정본)
  {daily_bars,supply_demand,short_credit,...}.py   # 소스별 수집기
  naver_delisted_bars.py  #   폐지 종목 과거 일봉(키움은 빈 응답을 '성공'으로 준다)
  dart_shares.py          #   상장주식수(DART). 기본은 폐지 종목, `--listed` 는 상장 종목 과거 백필
  sharadar_bulk.py        #   미국 벌크 스냅샷 동기화, sharadar_build.py 가 스토어 재구축
scripts/
  wait_and_stop.sh     # 지평선까지 예정 DAG 전부 끝나면 airflow 조기 종료 (--until HH:MM)
sql/init_timescale.sql # hypertable 스키마 + 청크/압축 정책 (신규 DB용)
sql/migrations/        # 기존 DB 변경분 — 001~009, docs/schema.md 참고
docker/Dockerfile      # collectors/ 의존성만 설치 (kr-quant editable install 없음)
docker-compose.yml     # Airflow(scheduler + 웹은 `profiles: ["ui"]`) + Airflow 메타 Postgres + TimescaleDB
```

**`dags/_common.py`** — DAG 마다 중복되던 DSN·자격증명 헬퍼를 한곳에 모았다.
`run_collector()` 는 수집기 stdout 을 태스크 로그로 실시간 스트리밍하고, 로그에 남을
수 있는 DSN 비밀번호를 마스킹한다(`collectors/config.py` 의 `mask_dsn` 을 재사용하는
단일 소스).

## kr-quant 와의 경계

수집(이 저장소)과 분석([kr-quant](https://github.com/younghwan91/kr-quant))은
프로세스도 저장소도 분리돼 있다. 분석 세션에서 실수로 수집기를 직접 실행해 DB
정합성이 깨지는 일을 막기 위해서다. `collectors/` 는 `kr_quant` 패키지에 대한 런타임
의존이 전혀 없다(자체 `storage.py`·`config.py` 를 갖는다).

> **예외** — kr-quant 는 여전히 `/opt/kr-quant` 에 읽기 전용으로 마운트된다.
> `weekly_price_adjust`(백조정 로직)가 kr-quant 의 분석 코드를 in-place 로 실행하기
> 때문이다(패키지 설치가 아니라 PYTHONPATH/sys.path 기반).
