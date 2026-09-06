# 데이터 스키마 · TimescaleDB 설계 · 마이그레이션

[← README](../README.md)

전체 DDL 은 [`sql/init_timescale.sql`](../sql/init_timescale.sql) 에 있다. 시계열
테이블은 TimescaleDB hypertable(PK `(code, date)`)이고, 그 외는 일반 테이블이다.

## 시세·수급 (hypertable)

| 테이블 | 내용 |
|---|---|
| `daily_bars` | 일봉 OHLCV + 거래대금. `source`='kiwoom'(상장 종목) / 'naver'(폐지 종목 백필 — 거래대금은 close×volume 근사) |
| `daily_bars_adjusted` | 액면분할 백조정 일봉. 평일 `daily_price_adjust` + 토요일 `weekly_price_adjust` 가 **전량 재계산**한다(back-adjust 는 종목별 전체 이력을 봐야 해서 증분이 불가능하다 — 실측 6분 23초 / 피크 RSS 5.2GB). `source` 는 `daily_bars` 에서 전파 |
| `supply_demand` | 투자자별 순매매 **수량(주)** — 금액이 아니다(`amt_qty_tp="2"`). `flu_rt` 는 **등락률 × 100(bp)**: 175 = +1.75%. `natn`(국가)은 실측상 늘 0. `source`='kiwoom'(전체) / 'naver'(폐지 부분 백필 — 기관·외국인만, 개인·기관세부는 NULL → **지표마다 유니버스가 달라진다**) |
| `short_selling` | 공매도 추이(수량·잔고·비율·평균가) |
| `credit_balance` | 신용잔고(신규·상환·잔고·비율) |
| `sector_index` | 업종지수 OHLCV |
| `shares_outstanding_history` | 상장주식수 이력(point-in-time 시총 계산용). `source` 가 kiwoom(주간 스냅샷)/krx(중단)/dart(과거 백필)를 구분. 키움은 현재 스냅샷만 주므로 **2016~2025 구간은 DART `--listed` 백필로 채운다** |
| `consensus` | 네이버 애널리스트 컨센서스(목표가·투자의견·EPS) |
| `news_articles` | krx-news-client(pip)로 수집한 뉴스(현재 토스만). PK `(id, published_at)` — `id`=`make_article_id(source,url)`라 재크롤링해도 같은 행을 갱신한다. 백테스팅+실매매, 추후 LLM 매매판단용 |

## 관계형 (일반 테이블, 시계열 아님)

| 테이블 | 내용 |
|---|---|
| `news_article_tickers` | `news_articles` 관련 종목 — `(article_id, ticker)` 정규화 테이블. 종목별 뉴스 전체 조회용 |

## 펀더멘털·마스터 (일반 테이블)

| 테이블 | 내용 |
|---|---|
| `stocks` | 종목 마스터(코드·이름·시장·섹터) |
| `earnings` | DART 분기 실적(순이익·매출·영업이익, 당기/전년동기), lookahead-safe `avail_date` + 정정 이력을 보존하는 `knowledge_date`(PK: code, period, knowledge_date) |
| `delisted_stocks` | 상장폐지 종목 마스터(생존편향 보정). 과거 시세는 `daily_bars` 에 `source='naver'` 로 들어간다 |
| `backfill_markers` | `(code, source)` — "조회해봤는데 자료가 없더라"를 기록해 백필이 같은 코드를 매 회차 다시 훑지 않게 한다 |
| `news_judgments` | LLM이 news_articles/disclosures를 읽고 낸 판단. PK `(source_type, source_id, ticker, prompt_version)` — prompt_version이 바뀌면 새 행, 기존 행은 절대 안 고침(재현성). `confidence`(LLM 자체 확신도 0~100)·`judged_at`(응답 시각, 레이턴시 측정용)은 013 이후 행에만 있다(NULL=013 이전) |

## TimescaleDB 설계 노트

- **청크 크기** — 모든 hypertable 이 `chunk_time_interval = 1년` 을 쓴다. 기본 7일
  청크는 이 데이터 볼륨(~2,600종목 × 250거래일/년 ≈ 65만 행/년)에 비해 지나치게
  잘게 쪼개져 청크 메타데이터 오버헤드가 커지고, 여러 해에 걸친 스캔이 느려진다.
- **압축** — 시세·수급·컨센서스 hypertable 은 30일이 지난 청크를 컬럼형으로 자동
  압축한다(`compress_segmentby = 'code'`). 최근 데이터는 행 기반으로 남겨 잦은
  upsert 를 빠르게 처리한다. **7일이 아니라 30일인 이유**(`sql/migrations/008`):
  수집기가 매일 쓰는 창이 일봉·수급 15일, 공매도·신용 10일이라 경계가 7일이면 그
  창의 절반 이상이 이미 압축된 청크를 때린다 — upsert 마다 세그먼트
  압축해제→갱신→재압축이 돌았다(pg_stat 실측: 압축 청크에서 `n_tup_ins ≈
  n_tup_del`, `n_live_tup = 0`).
- **`daily_bars_adjusted` 는 압축 제외** — `daily_price_adjust`/`weekly_price_adjust`
  가 테이블 전체를 upsert 로 재작성하므로 압축을 걸면 압축해제→재압축 순환만
  반복된다. 라이브 DB 에서 실측 압축률이 739MB → 901MB 로 음수였다 — 008 에서
  전량 `decompress_chunk` + `compress = false` 로 영구 해제했다.
- **DB 쓰기** — 수집기는 `psycopg2.extras.execute_values` 로 배치 upsert 하고, 긴
  전수 수집은 청크 단위(100종목)로 중간 커밋해 크래시 시 손실을 제한한다.

## 스키마 마이그레이션

`sql/init_timescale.sql` 은 **새 DB 를 세우는** 스크립트다. 이미 데이터가 있는 DB 는
[`sql/migrations/`](../sql/migrations) 를 순서대로 적용한다.

```bash
psql "$KR_QUANT_DB" -v ON_ERROR_STOP=1 -f sql/migrations/001_earnings_knowledge_date.sql
```

| 마이그레이션 | 내용 |
|---|---|
| `001_earnings_knowledge_date` | `earnings` PK 를 `(code, period, knowledge_date)` 로 확장 — 정정공시가 덮어쓰지 않고 새 버전으로 쌓인다 |
| `002_daily_bars_source` | `daily_bars.source` 추가 — 폐지 종목 시세를 네이버에서 받으면서 행별 출처를 남긴다(거래대금이 실측/근사로 갈림) |
| `003_daily_bars_adjusted_source` | `daily_bars_adjusted.source` 추가 — 002 의 짝. 백테스트가 읽는 건 조정가 테이블이라 거기까지 전파돼야 근사 거래대금을 식별할 수 있다. **적용 후 조정가를 재생성해야 기존 행이 채워진다** |
| `004_delisted_naver_checked` | `delisted_stocks.naver_checked` — 네이버에 자료가 없는 코드를 표시해 주간 재조회를 막는다(실측 주간 1,758회 → 0회) |
| `005_shares_source_knowledge` | `shares_outstanding_history` 에 `source`·`knowledge_date` 추가 — 기준일 ≠ 공시일 |
| `006_supply_demand_source` | `supply_demand.source` 추가 — 네이버 폐지 수급은 항목이 일부뿐이라, 읽는 쪽이 NULL(모름)과 0(순매매 없음)을 구분해야 한다 |
| `007_delisted_backfill_markers` | `delisted_stocks.dart_checked`·`naver_sd_checked` — 004 와 같은 병 |
| `008_compression_and_lookahead_cleanup` | 압축 경계 7일 → 30일 · `daily_bars_adjusted` 압축 영구 해제 · `shares_outstanding_history` 의 lookahead 행 삭제 |
| `009_backfill_markers` | 004·007 의 마커 컬럼을 `backfill_markers(code, source)` 테이블로 옮긴다. 마커가 `delisted_stocks` 에 있으면 **상장 종목에는 쓸 수가 없었다** — 소스가 늘 때마다 컬럼을 붙이는 대신 자리를 바꿨다 |
| `010_news_articles` | `news_articles`/`news_article_tickers` 신설 — krx-news-client(토스) 뉴스를 백테스팅+실매매용으로 영속 저장. `id` 자연키 upsert라 krx-news-rest-api 옛 Redis 캐시(ZSET member=article JSON)가 갖던 dedup 버그가 없다 |
| `011_disclosures` | `disclosures` 신설 — krx-news-client(DART) 공시를 백테스팅+실매매용으로 영속 저장 |
| `012_news_judgments` | `news_judgments` 신설 — LLM 뉴스/공시 판단, 장전/장중 DAG가 채움 |
| `013_news_judgments_confidence_judged_at` | `news_judgments`에 `confidence`(LLM 확신도 0~100)·`judged_at`(응답 시각, UTC) 추가 — scalp-it 세션 요청(오탐 필터링·레이턴시 측정용), 둘 다 nullable(013 이전 행은 소급 불가) |

> ⚠️ 001 은 코드가 먼저 나가고 DB 적용이 3일 늦었다. 그 사이 `daily_earnings` 가
> 초록불이었던 건 비수기라 `rows=0` 이어서 DB 를 건드리기 전에 빠져나갔기 때문이지,
> 스키마가 맞아서가 아니다. **스키마를 바꾸는 커밋은 DB 적용까지가 한 단위다.**
