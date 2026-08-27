# quant-airflow

[![CI](https://github.com/younghwan91/quant-airflow/actions/workflows/ci.yml/badge.svg)](https://github.com/younghwan91/quant-airflow/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-younghwan--chae-0A66C2?logo=linkedin&logoColor=white)](https://www.linkedin.com/in/younghwan-chae/)

퀀트 리서치용 시장 데이터를 매일 자동으로 수집·적재하는 Airflow 파이프라인이다.
**한국 주식**(코스피·코스닥)의 시세·수급·실적·컨센서스를 TimescaleDB에 쌓고,
**미국 주식**(Sharadar)은 벤더 벌크 스냅샷으로 DuckDB 스토어를 매일 재구축한다.
수집 로직(`collectors/`)과 스케줄링(`dags/`)을 모두 이 저장소가 자체 보유한다.

**상장폐지 종목까지 담는다.** 대부분의 수집기는 "현재 상장된 종목" 목록 위를 도므로
망한 회사가 통째로 빠지고, 그 데이터로 만든 백테스트는 살아남은 회사만 보고 성적을
잰다(생존편향). 이 파이프라인은 폐지 종목의 과거 시세·실적을 별도로 메우고
(`naver_delisted_bars`, `daily_bars.source`), 매주 새 폐지분을 따라간다.

- **오케스트레이션**: Airflow(LocalExecutor) — 13개 DAG, 한국은 매일 증분 + 주간 깊이 재수집, 미국(Sharadar)은 일일 스냅샷 재구축
- **데이터 소스**: DART(실적) · 키움 REST(시세·수급·공매도·신용·상장주식수) · KRX(상장주식수·상장폐지) · 네이버(컨센서스·폐지종목 시세)
- **스토어**: TimescaleDB(hypertable + 압축) — LAN에 열어 메인 PC가 읽기 전용으로 질의

---

## 목차

- [역할 분리](#역할-분리)
- [아키텍처](#아키텍처)
- [빠른 시작](#빠른-시작)
- [DAG 목록](#dag-목록)
- [데이터 스키마](#데이터-스키마)
- [저장소 구조](#저장소-구조)
- [스키마 마이그레이션](#스키마-마이그레이션)
- [TimescaleDB 설계 노트](#timescaledb-설계-노트)
- [메인 PC에서 데이터 읽기](#메인-pc에서-데이터-읽기)
- [시크릿 처리](#시크릿-처리)

---

## 역할 분리

*수집 로직이 왜 분석 저장소가 아니라 이곳에 있는가*

| | quant-airflow (이 저장소) | [kr-quant](https://github.com/younghwan91/kr-quant) |
|---|---|---|
| **역할** | 데이터 **수집·적재·스케줄링** | 전략·피처 **분석** (백테스트, PEAD 등) |
| **DB 접근** | 쓰기 (수집기가 upsert) | 읽기 전용 |
| **핵심 디렉터리** | `collectors/`, `dags/` | `kr_quant/` 라이브러리 |

수집 로직을 분석 저장소에서 떼어 이곳에 둔 이유는 두 가지다.

1. **사고 방지** — 분석 세션에서 실수로 수집기를 직접 실행해 DB 정합성이 깨지는
   일을 막는다. 분석(kr-quant)과 수집(이곳)은 프로세스도 저장소도 완전히 분리돼 있다.
2. **오픈소스 공유** — 두 저장소 모두 공개다. 수집(이곳)과 분석(kr-quant)을 나눠 각각 독립적으로 읽히게 한다. `collectors/`는 `kr_quant` 패키지에
   대한 런타임 의존이 전혀 없다(자체 `collectors/storage.py`·`collectors/config.py`를 갖는다).

> **예외** — kr-quant는 여전히 `/opt/kr-quant`에 읽기 전용으로 마운트된다.
> `weekly_price_adjust`(kr_quant.price_adjust 백조정 로직) DAG가 kr-quant의
> 분석 코드를 in-place로 실행하기 때문이다(패키지 설치가 아니라 PYTHONPATH/sys.path 기반).

## 아키텍처

데이터가 둘이고, **적재 방식이 서로 다르다.** 한국은 소스 API가 증분만 주므로
DB에 직접 upsert하고, 미국(Sharadar)은 벤더가 전체 스냅샷을 주므로 파일을 새로
지어 통째로 갈아끼운다.

```
spare PC (Ubuntu, 이 저장소)                                 main PC
┌──────────────────────────────────────────────┐
│ Airflow (LocalExecutor)  dags/*.py            │
│                                                │
│  ── 한국 ────────────────────────────────      │
│   -m collectors.X  ──upsert──►  TimescaleDB   │◄──psql───┐
│                                  (5432, LAN)   │          │
│                                                │     ┌─────────┴──────────┐
│  ── 미국 (Sharadar) ─────────────────────      │     │     분석/백테      │
│   -m collectors.sharadar_bulk                  │     │      kr-quant      │
│        │ bulk zip (modified 바뀐 것만)          │     │ portfolio-research │
│        ▼                                       │     └─────────┬──────────┘
│   sharadar/raw/  ──►  -m collectors.sharadar_build        │
│                            │ build → gate      │          │
│                            ▼ os.replace        │          │
│                       us_micro.duckdb          │◄──file───┘
└──────────────────────────────────────────────┘
```

미국 쪽 화살표가 `os.replace`인 게 핵심이다 — 연구가 스토어를 열고 있는 중에
배포해도 충돌하지 않는다(기존 리더는 옛 inode를 계속 읽는다). DuckDB는 단일
라이터라, 직접 upsert하면 연구와 수집이 상시 서로를 막는다.

머신 가동은 cron이 관리한다. **창 셋으로 나눠 띄운다** — 오전 창은 매일,
저녁 창은 평일용과 토요일용이 따로 있어 하루에 최대 두 창이 뜬다.

| 창 | 기동 | 대상 |
|---|---|---|
| 1 — 오전 수집 | 매일 10:00 (지평선 11:30) | catchup · short_credit · listed_shares · 주간 백필 |
| 2 — 평일 저녁 | 평일 15:55 | daily_collection(16:00) · price_adjust(16:55) · consensus(17:00) · sharadar(화~금 17:30) |
| 3 — 토요일 저녁 | 토 17:20 | sharadar(17:30) 하나 |

각 창은 `scripts/wait_and_stop.sh` 가 "그 지평선까지 예정된 DAG가 전부 끝났는가"를
Airflow 메타DB에 물어 조기 종료한다(안전장치 포함).

**왜 한 창이 아닌가.** 예전엔 10:00 기동 → 마지막 DAG까지 한 창이었는데, 실측
가동 521분 중 작업이 138분(27%)이고 **380분(73%)이 오전 수집과 저녁 수집 사이의
유휴**였다. 오전 DAG는 전날 확정 데이터를 받으므로 미룰 수 없고, 저녁 DAG는 장
마감(15:30) 이후에만 데이터가 나오므로 앞당길 수 없다 — 스케줄을 붙이는 건
원리적으로 불가능하고 창을 나누는 게 유일한 답이다.

> **완료 ≠ 성공.** 종료 조건은 "예정된 런이 더 없다"라서 실패한 런도 끝난 런으로
> 세어진다. 실제로 2026-08-27 에 `daily_earnings` 가 재시도까지 두 번 실패한 채
> `모두 완료 — 컨테이너 종료`가 찍혔다(그 전에도 `daily_minervini_scan` 10일,
> `daily_krx_shares` 6일이 같은 식으로 조용히 지나갔다). 지금은 종료 직전
> `report_failures()` 가 그날 실패한 (dag_id, task_id) 를 로그에 남긴다.
>
> ⚠️ `wait_and_stop.sh` 는 **airflow 4종(스케줄러·웹서버·init·메타DB)만
> 내린다.** `timescaledb` 는 이 저장소 전용이 아니다 — scalp-it 의 장중 틱 수집이
> 같은 컨테이너를 쓰고 crontab 의 `db_guard.sh` 가 평일 08:00~15:55 살아 있는지 지킨다.
> 예전처럼 `docker compose stop` 으로 전부 내리면 오전 창을 11:30에 닫는 순간
> **장중에 남의 수집 DB를 죽인다.**

## 빠른 시작

```bash
# 스페어 PC (Ubuntu)
git clone <this-repo> quant-airflow
git clone https://github.com/younghwan91/kr-quant.git ../kr-quant   # sibling — 두 DAG만 사용
cd quant-airflow

cp .env.example .env   # KIWOOM_APP_KEY/SECRET, DART_API_KEY(_2/_3), TIMESCALE_*, AIRFLOW_* 채우기
docker compose up -d                 # 스케줄러 + Airflow 메타DB + TimescaleDB
docker compose --profile ui up -d    # 웹 UI 까지 필요할 때만
```

- **Airflow 웹서버**: `http://<spare-pc-ip>:8080` — `profiles: ["ui"]` 라 기본
  기동에서는 뜨지 않는다. 볼 일이 있을 때 `--profile ui` 로 올린다
- **TimescaleDB**: `<spare-pc-ip>:5432` (LAN 오픈, 메인 PC가 질의)

## DAG 목록

데이터는 **매일 증분 + 주간 깊이 재수집**의 2단 구조로 채운다. 평일 DAG가 최신
데이터를 증분으로 쌓고, 주간 DAG가 히스토리 깊이를 유지해 신규 상장 종목·새 지수·DB
리셋 이후에도 과거가 비지 않도록 한다(모든 수집기가 `(code, date)` upsert라 idempotent).

**매일/평일 — 증분**

| DAG | 스케줄(KST) | 수집 대상 |
|---|---|---|
| `daily_collection` | 평일 16:00 | 일봉 + 수급(키움) + 업종지수. 쓰는 창은 최근 15일 — 깊은 구멍은 catchup 이 메운다 |
| `daily_collection_catchup` | 평일 10:05 | 전날 실패분만 값싸게 재수집(일봉·수급을 따로 판정, 최신이면 API 호출 0) |
| `daily_short_credit` | 화~토 10:00 | 공매도 + 신용잔고(키움, T+1~2 지연 고려). 쓰는 창 최근 10일 |
| `daily_earnings` | 평일 16:00 | DART 실적 증분(당기 + 전분기, `--multi-batch`) |
| `daily_price_adjust` | 평일 16:55 | `daily_bars_adjusted` 재생성 — **조정가 테이블에 이번 주 행을 채운다**. 토요일 런만 있던 시절엔 월~금 내내 그 주가 비어 있었다(실측 8/27 목: 원자료 8/27 vs 조정가 8/21, 4거래일 결측) |
| `daily_consensus` | 평일 17:00 | 네이버 애널리스트 컨센서스. 월요일만 전종목, 화~금은 최근 90일 커버리지 종목만 |
| ~~`daily_krx_shares`~~ | ~~수동 전용~~ | ⛔ **`schedule=None` (2026-08-25)** — KRX가 MDCSTAT 계열에 로그인을 걸어 OTP가 `LOGOUT`을 반환한다. 원래 `is_paused_upon_creation=True` 를 걸어뒀는데 **그 플래그는 최초 등록 때만 적용돼** 무시됐고, 8/17 이후 평일마다 실패하며 재시도 10분으로 저녁 창을 18:40까지 늘렸다. 대체: 상장분은 `weekly_listed_shares`, 폐지분은 `dart_shares` |

**주간 — 백필/스냅샷**

| DAG | 스케줄(KST) | 수집 대상 |
|---|---|---|
| `earnings_backfill` | 일 10:00 | DART 실적 전체 이력 백필(`--multi-batch`, resume) |
| `weekly_history_backfill` | 일 11:00 | 업종지수·공매도·신용잔고 히스토리 깊이 재수집. `--resume-depth 330` 으로 **이미 깊은 종목은 건너뛴다** |
| `weekly_listed_shares` | 화 10:10 | 키움 상장주식수 스냅샷. 화요일인 이유는 `daily_short_credit` 과 TR 버킷이 달라 겹쳐 돌려도 서로 안 막기 때문이다 |
| `weekly_delisted_stocks` | 토 10:05 | KRX 상장폐지종목 마스터 + **과거 일봉**(네이버) + **상장주식수**(DART) 백필 — 생존편향 보정 3층 |
| `weekly_price_adjust` | 토 10:40 | 같은 재생성을 **폐지 시세 백필 뒤에** 돌려 새로 폐지된 종목까지 조정가에 넣는다. `ExternalTaskSensor` 로 완료를 **직접 확인한 뒤** 시작한다(예전엔 35분 시계 간격이 유일한 보장이었다). 평일 갱신은 위 `daily_price_adjust` 담당 |

> **신뢰성** — 모든 DAG 태스크에 재시도를 걸어 두었다. 외부 API·수집 DAG는
> `retries=1, retry_delay=10분`, 전체 이력 백필은 `retries=2, 30분`이다. 일시적
> 네트워크 오류로 그날 데이터가 조용히 빠지는 것을 막기 위해서다.
>
> **신용잔고 한계** — 키움 API가 최근 100 거래일까지만 제공하므로, 그보다 깊은
> 신용잔고 히스토리는 채울 수 없다.

**미국(Sharadar) — 이 저장소에서 유일한 비한국 파이프라인**

| DAG | 스케줄(KST) | 하는 일 |
|---|---|---|
| `daily_sharadar` | 화~토 17:30 | 벌크 스냅샷 동기화 → 스토어 재구축 → 검증 → 원자적 공개. 테이블별로 실패를 격리해 **한 개가 죽어도 나머지 13개의 상태 표는 남는다** |

한국 파이프라인과 스케줄러·인프라를 공유한다 — 저장소 이름이 `quant-airflow`인 이유다.
설계 근거는
[`docs/superpowers/specs/2026-08-15-sharadar-bulk-rebuild-design.md`](docs/superpowers/specs/2026-08-15-sharadar-bulk-rebuild-design.md).

**증분이 아니라 재구축이다.** 처음엔 API 증분(종목 22,000개를 30개씩 ~730회
순회)으로 만들었다가 실측에서 버렸다 — 벤더 티커 제한이 개수(30)가 아니라
**문자열 200자**라 우선주 티커 30개면 무조건 `400`이었고(fundamentals 매번 실패),
소켓 타임아웃이 재시도되지 않아 70분짜리 작업이 딸꾹질 한 번에 전멸했으며,
스토어를 직접 upsert 하므로 연구(`opt-factor optimize`)와 DuckDB 락이 충돌했다.

벌크는 테이블당 요청 1회라 앞의 둘이 해당 없고, **새 파일에 지어 `os.replace`로
갈아끼우므로** 셋째도 사라진다 — 연구가 도는 중에 배포해도 기존 리더는 옛 inode를
계속 안전하게 읽는다.

```
① RAW      /opt/us-data/sharadar/raw/  ← 매일 14개 전부 대조, `modified` 가 그대로면 안 받는다
② BUILD    raw → .us_micro.duckdb.building   (검증된 --provider csv 경로 재사용)
③ GATE     테이블 결측·0행·행수 5% 이상 감소면 공개 중단
④ PUBLISH  os.replace → us_micro.duckdb   (직전 2세대는 .prev* 로 보존)
```

- **동기화 확인**: 구독 14개를 매일 전부 벤더 목록과 대조하고, 매 실행 끝에
  상태 표(벤더 타임스탬프·정체 횟수·판정)를 찍는다. 전송이 없어도 `checked_at`이
  갱신돼 "오늘 확인했다"가 남는다. 낡음(벤더 미갱신)은 막지 않는다 — 벌크가
  매번 전체 이력을 주므로 다음 실행에 저절로 채워진다. 막는 것은 손상(절단·
  체크섬 불일치·행수 급감·최신일 후퇴)뿐이다. 설계는
  [`2026-08-16-sharadar-sync-verification-design.md`](docs/superpowers/specs/2026-08-16-sharadar-sync-verification-design.md).
- **수집 로직은 이 저장소에 없다.** `portfolio-research`(sibling)의 `opt-factor ingest`를
  부른다 — `weekly_price_adjust`가 kr-quant를 쓰는 것과 같은 구조(ro 마운트 +
  `PYTHONPATH`, pip install 없음). `_csv_daily`(백만달러 환산)·`_csv_tickers`
  (`is_delisted` 리네임)·`_csv_fundamentals`(PIT 위반 제외)는 실제 버그에서 나온
  코드라 **DuckDB SQL로 재구현하지 않는다**(동등성 테스트 없이는 금지).
- **스토어**: `~/data/us_micro.duckdb`(2.7GB, 컨테이너에선 `/opt/us-data/`).
  가격 21,963종목·4,630만행, 1997~. 폐지 종목 포함. 경로는 `.env`의 `US_DATA_DIR`.
- **17:30인 이유**: 벤더 드롭이 그날 다 끝난 뒤라야 받을 게 있다. ⚠️ **테이블별
  시각은 믿을 게 못 된다** — 2026-08-15 실측(stocks 16:40 / fundamentals 16:49 /
  funds 16:54)이 08-27 재측정에서 크게 이동했다(stocks **12:40** / fundamentals
  12:25 / funds 12:47, 반대로 holdings_ticker 는 01:39 → 14:42). 개별 표가 아니라
  **그날의 마지막 드롭**이 기준이고, 관측된 최악값이 **17:28**(2026-08-25)로 현재
  스케줄보다 2분 이르다. 그래서 17:30 은 앞당기지 않는다(표본 3일이라 근거는 얇다).
  화~토인 건 미국 장이 없는 날엔 새로 받을 게 없어서다(금요일 세션은 토요일 드롭에
  실려 온다). 이 DAG 가 평일 저녁 창을 혼자 ~53분 늘린다 — `daily_consensus` 는
  17:00·49초라 스택을 잡아두지 않는다.
  ⚠️ 토요일은 이 DAG 하나 때문에 오전 창(11:30 종료)과 6시간 떨어진 저녁 창을
  따로 띄운다(17:20 기동) — 예전엔 이것 하나 때문에 스택이 18:15까지 통째로 떠 있었다.
- **실측 소요(2026-08-25 정기 런)**: DAG 전체 31.6분 — 다운로드 3,307MB 약 9분
  (14개 중 `새로 받음 10 · 확인만 4`, `modified` 스킵이 실제로 걸린다), 빌드·검증·공개
  22.2분. 9일 공백을 메운 날은 14개를 전부 받아 3,565MB / 15.3분이었다.
  `wait_and_stop.sh`가 실행 중인 런을 기다리므로 잘리지 않는다.
- **아직 스토어에 안 들어가는 것**: funds(SFP)·holdings(SF3)·holdings_investor
  (SF3B)·events. 구독분이라 raw 아카이브에는 받아두지만, `portfolio-research`에 테이블이
  없어 적재는 못 한다. `metrics`는 종목당 1행 최신 스냅샷뿐이라(히스토리 없음)
  백테스트에 쓰면 look-ahead다.

## 데이터 스키마

전체 정의는 [`sql/init_timescale.sql`](sql/init_timescale.sql)에 있다. 시계열 테이블은
TimescaleDB hypertable(PK `(code, date)`)이고, 그 외는 일반 테이블이다.

**시세·수급 (hypertable)**

| 테이블 | 내용 |
|---|---|
| `daily_bars` | 일봉 OHLCV + 거래대금. `source`='kiwoom'(상장 종목) / 'naver'(폐지 종목 백필 — 거래대금은 close×volume 근사) |
| `daily_bars_adjusted` | 액면분할 백조정 일봉. 평일 `daily_price_adjust` + 토요일 `weekly_price_adjust` 가 **전량 재계산**한다(back-adjust 는 종목별 전체 이력을 봐야 해서 증분이 불가능하다 — 실측 6분 23초 / 피크 RSS 5.2GB). `source`는 `daily_bars`에서 전파 |
| `supply_demand` | 투자자별 순매매 **수량(주)** — 금액이 아니다(`amt_qty_tp="2"`). `flu_rt` 는 **등락률 × 100(bp)**: 175 = +1.75%. `natn`(국가)은 실측상 늘 0(90일 157,532행 전부). `source`='kiwoom'(전체) / 'naver'(폐지 부분 백필 — 기관·외국인만, 개인·기관세부는 NULL → **지표마다 유니버스가 달라진다**) |
| `short_selling` | 공매도 추이(수량·잔고·비율·평균가) |
| `credit_balance` | 신용잔고(신규·상환·잔고·비율) |
| `sector_index` | 업종지수 OHLCV |
| `shares_outstanding_history` | 상장주식수 이력(point-in-time 시총 계산용). `source`가 kiwoom(주간 스냅샷)/krx(중단)/dart(과거 백필)를 구분. 키움은 현재 스냅샷만 주므로 **2016~2025 구간은 DART `--listed` 백필로 채운다** |
| `consensus` | 네이버 애널리스트 컨센서스(목표가·투자의견·EPS) |

**펀더멘털·마스터·스캐너 (일반 테이블)**

| 테이블 | 내용 |
|---|---|
| `stocks` | 종목 마스터(코드·이름·시장·섹터) |
| `earnings` | DART 분기 실적(순이익·매출·영업이익, 당기/전년동기), lookahead-safe `avail_date` + 정정 이력을 보존하는 `knowledge_date`(PK: code, period, knowledge_date) |
| `delisted_stocks` | 상장폐지 종목 마스터(생존편향 보정). 과거 시세는 `daily_bars`에 `source='naver'`로 들어간다 |

## 저장소 구조

```
dags/                  # 13개 DAG — run_collector()로 `python -m collectors.X` 실행
  _common.py           #   공유 헬퍼: timescale_dsn()/kiwoom_env()/dart_env()/run_collector()
collectors/            # 수집 로직 자체 보유 (kr_quant 런타임 의존 없음)
  storage.py           #   스키마 + upsert 전체 (sqlite/Postgres 듀얼 백엔드)
  config.py            #   자격증명 로딩 + 키움 클라이언트 생성 + DSN 마스킹 + DART 키 목록(정본)
  kiwoom_cli.py        #   전종목 스윕 콜렉터 공통 CLI(인자·세션·유니버스·배너)
  proc.py              #   자식 프로세스 스트리밍 + 줄 단위 시크릿 마스킹(정본)
  {daily_bars,supply_demand,short_credit,...}.py   # 소스별 수집기
  naver_delisted_bars.py  #   폐지 종목 과거 일봉(키움은 빈 응답을 '성공'으로 준다)
  dart_shares.py          #   상장주식수(DART). 기본은 폐지 종목, `--listed` 는 상장 종목 과거 백필
scripts/
  wait_and_stop.sh     # 지평선까지 예정 DAG 전부 끝나면 airflow 조기 종료 (--until HH:MM)
  sync_to_timescale.py # sqlite → TimescaleDB 증분 upsert (레거시 경로)
sql/init_timescale.sql # hypertable 스키마 + 청크/압축 정책 (신규 DB용)
sql/migrations/        # 기존 DB 변경분 — 001~008, README "스키마 마이그레이션" 참고
docker/Dockerfile      # collectors/ 의존성만 설치 (kr-quant editable install 없음)
docker-compose.yml     # Airflow(scheduler + 웹은 `profiles: ["ui"]`) + Airflow 메타 Postgres + TimescaleDB
```

**`dags/_common.py`** — DAG마다 중복되던 DSN·자격증명 헬퍼를 한곳에 모았다.
`run_collector()`는 수집기 stdout을 태스크 로그로 실시간 스트리밍하고, 로그에 남을 수
있는 DSN 비밀번호를 마스킹한다(`collectors/config.py`의 `mask_dsn`을 재사용하는 단일 소스).

## 스키마 마이그레이션

`sql/init_timescale.sql`은 **새 DB를 세우는** 스크립트다. 이미 데이터가 있는 DB는
`sql/migrations/`를 순서대로 적용한다.

```bash
psql "$KR_QUANT_DB" -v ON_ERROR_STOP=1 -f sql/migrations/001_earnings_knowledge_date.sql
```

| 마이그레이션 | 적용일 | 내용 |
|---|---|---|
| `001_earnings_knowledge_date` | 2026-08-13 | `earnings` PK를 `(code, period, knowledge_date)`로 확장 — 정정공시가 덮어쓰지 않고 새 버전으로 쌓인다. 기존 83,996행은 최초 보고치이므로 `knowledge_date = avail_date`로 백필 |
| `002_daily_bars_source` | 2026-08-15 | `daily_bars.source` 추가 — 폐지 종목 시세를 네이버에서 받으면서 행별 출처를 남긴다(거래대금이 실측/근사로 갈림) |
| `003_daily_bars_adjusted_source` | 2026-08-15 | `daily_bars_adjusted.source` 추가 — 002 의 짝. 백테스트가 읽는 건 조정가 테이블이라 거기까지 전파돼야 근사 거래대금을 식별할 수 있다. **적용 후 `python -m kr_quant.price_adjust --rebuild-db` 로 재생성해야 기존 행이 채워진다** |
| `004_delisted_naver_checked` | 2026-08-15 | `delisted_stocks.naver_checked` 추가 — 네이버에 구간 내 데이터가 없거나 이미 다 받은 코드를 표시해 주간 재조회를 막는다(실측 주간 1,758회 → 0회) |
| `005_shares_source_knowledge` | 2026-08-15 | `shares_outstanding_history`에 `source`·`knowledge_date` 추가 — 폐지 종목 주식수를 DART에서 받으면서 출처와 "언제 알 수 있었나"를 남긴다(기준일 ≠ 공시일) |
| `006_supply_demand_source` | 2026-08-15 | `supply_demand.source` 추가 — 폐지 종목 수급은 네이버에서만 받히는데 항목이 일부뿐이고(기관·외국인) '외국인'의 정의도 키움과 다르다. 출처를 남겨 읽는 쪽이 NULL(모름)과 0(순매매 없음)을 구분하게 한다 |
| `007_delisted_backfill_markers` | 2026-08-25 | `delisted_stocks.dart_checked`·`naver_sd_checked` 추가 — 004 와 같은 병. DART/네이버가 "자료 없음"을 준 폐지 종목이 어디에도 기록되지 않아 매주 다시 조회됐다(실측 42종목, 주당 2.2분). 다시 훑으려면 컬럼을 NULL 로 되돌리거나 수집기에 `--refetch` |
| `008_compression_and_lookahead_cleanup` | 2026-08-25 | ① 압축 경계 7일 → 30일(수집기가 쓰는 창이 15일/10일이라 매 upsert 가 압축해제→재압축을 돌렸다) ② `daily_bars_adjusted` 압축 영구 해제(실측 739MB → 901MB 로 음수 압축) ③ `shares_outstanding_history` 에서 오늘 스냅샷을 과거 3개 날짜로 복사해 둔 `source='kiwoom'` 행 삭제 — `market_cap_asof` 에 lookahead 가 살아 있었다 |

> ⚠️ 001은 코드(`collectors/storage.py`)가 먼저 나가고 DB 적용이 3일 늦었다. 그 사이
> `daily_earnings`가 초록불이었던 건 비수기라 `rows=0`이어서 DB를 건드리기 전에 빠져나갔기
> 때문이지, 스키마가 맞아서가 아니다. **스키마를 바꾸는 커밋은 DB 적용까지가 한 단위다.**

## TimescaleDB 설계 노트

- **청크 크기** — 모든 hypertable이 `chunk_time_interval = 1년`을 쓴다. 기본 7일 청크는
  이 데이터 볼륨(~2,600종목 × 250거래일/년 ≈ 65만 행/년)에 비해 지나치게 잘게 쪼개져
  청크 메타데이터 오버헤드가 커지고, 여러 해에 걸친 스캔이 느려진다.
- **압축** — 시세·수급·컨센서스 hypertable은 30일이 지난 청크를 컬럼형으로 자동 압축한다
  (`compress_segmentby = 'code'`). 최근 데이터는 행 기반으로 남겨 잦은 upsert를 빠르게 처리한다.
  **7일이 아니라 30일인 이유**(2026-08-25, `sql/migrations/008`): 수집기가 매일 쓰는 창이
  일봉·수급 15일, 공매도·신용 10일이라 경계가 7일이면 그 창의 절반 이상이 이미 압축된
  청크를 때린다 — upsert 마다 세그먼트 압축해제→갱신→재압축이 돌았다(pg_stat 실측:
  압축 청크에서 `n_tup_ins ≈ n_tup_del`, `n_live_tup = 0`).
- **`daily_bars_adjusted`는 압축 제외** — `weekly_price_adjust`가 매주 테이블 전체를
  upsert로 재작성하므로, 압축을 걸면 오래된 청크를 매주 압축 해제했다가 다시 압축하는
  순환만 반복된다. 라이브 DB 는 이 주석과 달리 515청크 중 512개가 압축돼 있었고(정책 없이
  수동 압축된 것으로 보인다) 실측 압축률이 739MB → 901MB 로 음수였다 — 008 에서 전량
  `decompress_chunk` + `compress = false` 로 영구 해제했다.
- **DB 쓰기** — 수집기는 `psycopg2.extras.execute_values`로 배치 upsert하고, 긴 전수
  수집은 청크 단위(100종목)로 중간 커밋해 크래시 시 손실을 제한한다.

## 메인 PC에서 데이터 읽기

```python
import psycopg2
conn = psycopg2.connect(
    host="<spare-pc-ip>", port=5432,
    dbname="kr_quant", user="kr_quant", password="...",
)
# 예: 최근 조정 일봉
df = pd.read_sql("SELECT * FROM daily_bars_adjusted WHERE code = %s ORDER BY date", conn, params=("005930",))
```

분석·백테스트 코드는 [kr-quant](https://github.com/younghwan91/kr-quant)에 있으며, 이
DB를 읽기 전용으로 사용한다.

## 시크릿 처리

`KIWOOM_APP_KEY`/`KIWOOM_APP_SECRET`(실계좌 키)와 `DART_API_KEY`(_2/_3)는
`airflow-webserver`·`airflow-scheduler` 컨테이너에 평문 env로 전달되지 **않는다**.
`airflow-init` 컨테이너가 이 값을 한 번만 읽어 `airflow variables set`으로 Airflow
메타DB에 **Fernet 암호화**해 저장하고, DAG 태스크는 실행 시점에 `Variable.get()`으로
꺼내 수집기 서브프로세스 환경에만 주입한다(`dags/_common.py`의 `kiwoom_env()`·`dart_env()`).

TimescaleDB 접속 정보(`TIMESCALE_*`)는 LAN 내부용이라 평문 컨테이너 env로 두었다.
필요하면 Airflow Connection으로 옮길 수 있지만, 지금 범위에서는 과하다고 판단했다.

`.env`는 절대 커밋하지 않는다(`.gitignore`에 포함).


---

## ⭐ 도움이 되셨다면

이 프로젝트가 유용했다면 우측 상단 **[⭐ Star](https://github.com/younghwan91/quant-airflow)** 를 눌러주세요. 검색·추천 노출이 올라가 더 많은 분들이 찾을 수 있습니다.

- 🐛 버그·질문 → [Issues](https://github.com/younghwan91/quant-airflow/issues)
- 📈 업데이트 소식 → [팔로우 @younghwan91](https://github.com/younghwan91)

## 관련 프로젝트 — 오픈소스 퀀트 스택

한국·미국 주식과 암호화폐를 아우르는 오픈소스 스택입니다. 각 저장소는 독립적으로 쓸 수 있습니다.

| 축 | 프로젝트 | 설명 |
|---|---|---|
| 🇰🇷 한국 주식 | **[kiwoom-rest-api](https://github.com/younghwan91/kiwoom-rest-api)** | 키움증권 REST API Python 라이브러리 — 국내주식 엔드포인트 전수·실시간 WebSocket, sync + async (`pip install kiwoom-client`) |
| 🇰🇷 한국 주식 | **[krx-fundamentals-api](https://github.com/younghwan91/krx-fundamentals-api)** | 국내 기업 펀더멘탈 REST API — 재무제표·투자지표·배당·종목 스크리닝 (DART + KRX + 네이버) |
| 🇰🇷 한국 주식 | **[krx-news-rest-api](https://github.com/younghwan91/krx-news-rest-api)** | 한국 주식 뉴스·공시 수집 REST API (FastAPI + Redis) |
| 🇰🇷 한국 주식 | **[kr-quant](https://github.com/younghwan91/kr-quant)** | 코스피·코스닥 알파 리서치 — walk-forward·랜덤 음성대조·purged CV·Deflated Sharpe 를 CI 가드레일로 강제 |
| 🇺🇸 미국 주식 | **[portfolio-research](https://github.com/younghwan91/portfolio-research)** | 미국주식 팩터 엔진 — point-in-time·생존편향 보정 데이터 위에서 walk-forward 를 Deflated Sharpe·PBO 로 게이팅 (+ ETF 전술배분 TAA — 9개 사전등록, 채택 0) |
| 🇺🇸 미국 주식 | **[automated-stock-trading-systems](https://github.com/younghwan91/automated-stock-trading-systems)** | Bensdorp 의 7개 비상관 트레이딩 시스템 백테스터 (교육용 재구현) |
| ₿ 암호화폐 | **[quantbox-engine](https://github.com/younghwan91/quantbox-engine)** | 암호화폐 선물 백테스트·실행 엔진 — 룩어헤드 0, 백테스트↔실거래 일체화 |

## 만든 사람

**채영환 (Younghwan Chae)** · [GitHub @younghwan91](https://github.com/younghwan91) · [LinkedIn](https://www.linkedin.com/in/younghwan-chae/)

전체 오픈소스 퀀트 스택은 [프로필](https://github.com/younghwan91)에서 한눈에 볼 수 있습니다.
