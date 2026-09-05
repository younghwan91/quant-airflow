-- Shared TimescaleDB schema for kr-quant data.
-- Mirrors kr_quant/storage.py's sqlite schema, but `date` is a real DATE
-- column (sqlite stores 'YYYYMMDD' TEXT) so hypertable chunking works.

CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS stocks (
    code   TEXT PRIMARY KEY,
    name   TEXT,
    market TEXT,
    sector TEXT,
    kind   TEXT
);

-- source: 'kiwoom' = 상장 종목(ka10081, trade_value 는 보고된 거래대금),
--         'naver'  = 상장폐지 종목 백필(siseJson, trade_value 는 close*volume/1e6 근사).
-- 폐지 종목이 이 테이블에 들어오는 이유는 생존편향이다 — 수집 소스가 현재 상장 종목만
-- 돌려주므로, 그냥 두면 백테스트가 살아남은 회사만 보고 성적을 잰다.
CREATE TABLE IF NOT EXISTS daily_bars (
    code        TEXT NOT NULL,
    date        DATE NOT NULL,
    open        INTEGER,
    high        INTEGER,
    low         INTEGER,
    close       INTEGER,
    volume      BIGINT,
    trade_value BIGINT,
    source      TEXT NOT NULL DEFAULT 'kiwoom',
    PRIMARY KEY (code, date)
);
-- 기본 7일 청크는 이 볼륨(~2,600종목×250거래일/년 ≈ 65만행/년)엔 과하게 잘게
-- 쪼갬 — 청크 메타데이터 오버헤드 + 백테스트의 여러 해 스캔이 느려짐. 1년 청크로.
SELECT create_hypertable('daily_bars', 'date', if_not_exists => TRUE, chunk_time_interval => INTERVAL '1 year');

CREATE TABLE IF NOT EXISTS supply_demand (
    code         TEXT NOT NULL,
    date         DATE NOT NULL,
    close        INTEGER,
    -- 등락률 × 100 (bp). 175 = +1.75%, -309 = -3.09% — **백분율이 아니다.**
    -- 벤더(ka10059) 표기를 그대로 저장한다. 그대로 % 로 읽으면 100배가 되고,
    -- 실제로 하류에서 +1301% 가 찍힌 적이 있다(2026-08-27, kr-quant 뷰어).
    flu_rt       REAL,
    acc_trde_qty BIGINT,        -- 거래량(주). daily_bars.volume 과 같은 값이다.
    -- 아래 투자자별 순매매는 전부 **수량(주)** 이지 금액이 아니다 — 수집기가
    -- ka10059 를 amt_qty_tp="2"(수량)로 부른다. 금액이 필요하면 종가를 곱해야
    -- 하고, 참값은 VWAP 가중이라 그 환산은 근사다.
    -- natn(국가)은 실측상 값이 들어온 적이 없다(최근 90일 157,532행 전부 0).
    individual   INTEGER,
    foreign_     INTEGER,
    institution  INTEGER,
    fnnc_invt    INTEGER,
    insrnc       INTEGER,
    invtrt       INTEGER,
    bank         INTEGER,
    penfnd_etc   INTEGER,
    samo_fund    INTEGER,
    natn         INTEGER,
    etc_corp     INTEGER,
    source       TEXT NOT NULL DEFAULT 'kiwoom',  -- kiwoom(전체) / naver(폐지 부분: 기관·외국인만)
    PRIMARY KEY (code, date)
);
SELECT create_hypertable('supply_demand', 'date', if_not_exists => TRUE, chunk_time_interval => INTERVAL '1 year');

CREATE TABLE IF NOT EXISTS short_selling (
    code            TEXT NOT NULL,
    date            DATE NOT NULL,
    close           INTEGER,
    volume          BIGINT,
    short_qty       BIGINT,
    short_balance   BIGINT,
    short_ratio     REAL,
    short_avg_price INTEGER,
    short_value     BIGINT,
    PRIMARY KEY (code, date)
);
SELECT create_hypertable('short_selling', 'date', if_not_exists => TRUE, chunk_time_interval => INTERVAL '1 year');

CREATE TABLE IF NOT EXISTS credit_balance (
    code        TEXT NOT NULL,
    date        DATE NOT NULL,
    close       INTEGER,
    new_qty     BIGINT,
    repay_qty   BIGINT,
    balance_qty BIGINT,
    balance_amt BIGINT,
    balance_rt  REAL,
    credit_rt   REAL,
    PRIMARY KEY (code, date)
);
SELECT create_hypertable('credit_balance', 'date', if_not_exists => TRUE, chunk_time_interval => INTERVAL '1 year');

CREATE TABLE IF NOT EXISTS sector_index (
    code        TEXT NOT NULL,
    name        TEXT,
    date        DATE NOT NULL,
    open        INTEGER,
    high        INTEGER,
    low         INTEGER,
    close       INTEGER,
    volume      BIGINT,
    trade_value BIGINT,
    PRIMARY KEY (code, date)
);
SELECT create_hypertable('sector_index', 'date', if_not_exists => TRUE, chunk_time_interval => INTERVAL '1 year');

CREATE TABLE IF NOT EXISTS shares_outstanding_history (
    code               TEXT NOT NULL,
    date               DATE NOT NULL,
    shares_outstanding BIGINT,  -- INTEGER(32bit, max~21억)로는 삼성전자 등 대형주 발행주식수(수십억주)가 오버플로우함
    source             TEXT NOT NULL DEFAULT 'kiwoom',  -- kiwoom/krx/dart(폐지 백필)
    knowledge_date     DATE,     -- DART 는 기준일(date)과 공시 접수일이 다르다
    PRIMARY KEY (code, date)
);
SELECT create_hypertable('shares_outstanding_history', 'date', if_not_exists => TRUE, chunk_time_interval => INTERVAL '1 year');
CREATE INDEX IF NOT EXISTS idx_sh_date ON shares_outstanding_history(date);

-- 일반 테이블(하이퍼테이블 아님): 자연키가 (code, period, knowledge_date)라
-- 파티션 후보인 avail_date가 PK에 없고, TimescaleDB는 파티션 컬럼이 빠진 유니크
-- 인덱스를 허용하지 않는다 — create_hypertable 조합은 생성 시 에러남(실제 DB로
-- 검증됨). 실적 데이터는 전종목 ~10년치도 수만~십만 행 규모라 압축/청크 이점이
-- 거의 없어 일반 테이블로 충분하다.
--
-- knowledge_date가 키에 들어가는 이유: DART 정정공시가 기존 행을 덮어쓰면 "그때
-- 알 수 있었던 값"이 사라져, 그 구간을 도는 백테스트가 사후 수정된 숫자를 당시
-- 알았던 것처럼 읽는다. 정정본은 새 행으로 쌓고, 읽는 쪽이 as-of로 고른다:
--
--   SELECT DISTINCT ON (code, period) *
--   FROM earnings WHERE knowledge_date <= :asof
--   ORDER BY code, period, knowledge_date DESC;
--
-- storage.upsert_earnings()는 값이 실제로 바뀐 행만 새 버전으로 넣는다(같은 값
-- 재수집은 행이 늘지 않음).
CREATE TABLE IF NOT EXISTS earnings (
    code            TEXT NOT NULL,
    period          TEXT NOT NULL,   -- e.g. '2020Q1'
    avail_date      DATE NOT NULL,   -- lookahead-safe availability date (period-end + filing lag)
    knowledge_date  DATE NOT NULL,   -- 이 값을 알게 된 날(수집일) — 정정공시는 새 행
    netinc          DOUBLE PRECISION,
    netinc_prior    DOUBLE PRECISION,
    revenue         DOUBLE PRECISION,
    revenue_prior   DOUBLE PRECISION,
    op_income       DOUBLE PRECISION,
    op_income_prior DOUBLE PRECISION,
    PRIMARY KEY (code, period, knowledge_date)
);
CREATE INDEX IF NOT EXISTS idx_earnings_avail_date ON earnings(avail_date);
CREATE INDEX IF NOT EXISTS idx_earnings_asof ON earnings(code, period, knowledge_date DESC);

CREATE TABLE IF NOT EXISTS consensus (
    code         TEXT NOT NULL,
    date         DATE NOT NULL,
    target_mean  DOUBLE PRECISION,
    recomm_mean  DOUBLE PRECISION,
    base_date    TEXT,
    fwd_eps      DOUBLE PRECISION,
    prev_eps     DOUBLE PRECISION,
    est_year     TEXT,
    PRIMARY KEY (code, date)
);
SELECT create_hypertable('consensus', 'date', if_not_exists => TRUE, chunk_time_interval => INTERVAL '1 year');
CREATE INDEX IF NOT EXISTS idx_consensus_date ON consensus(date);

CREATE TABLE IF NOT EXISTS daily_bars_adjusted (
    code        TEXT NOT NULL,
    date        DATE NOT NULL,
    open        DOUBLE PRECISION,  -- back-adjust 배수 적용 후라 daily_bars(INTEGER)와 달리 REAL/DOUBLE
    high        DOUBLE PRECISION,
    low         DOUBLE PRECISION,
    close       DOUBLE PRECISION,
    volume      BIGINT,            -- 미조정 원본 그대로(adjust_volume=False 기본값)
    trade_value BIGINT,
    source      TEXT NOT NULL DEFAULT 'kiwoom',  -- daily_bars.source 전파(근사 거래대금 식별)
    PRIMARY KEY (code, date)
);
SELECT create_hypertable('daily_bars_adjusted', 'date', if_not_exists => TRUE, chunk_time_interval => INTERVAL '1 year');
CREATE INDEX IF NOT EXISTS idx_dba_date ON daily_bars_adjusted(date);

-- 백필 소스가 "이 코드는 조회해봤고 자료가 없더라"를 기록하는 곳 (migration 009).
-- 마커가 없으면 자료 없는 코드는 결과 행이 안 생겨 대상 쿼리에 영원히 걸린다.
-- source 는 ``collectors.storage`` 의 CHECKED_* 상수 — 새 소스는 문자열 하나면
-- 되고 스키마를 안 건드린다(그 전에는 소스마다 delisted_stocks 에 컬럼을 붙였고,
-- 004 → 007 로 같은 병이 세 번 재발했다).
--
-- ⚠️ **이 테이블이 여기 없었다**(2026-08-30 발견). 009 마이그레이션이 만들고
-- ``collectors/storage.py`` 의 sqlite SCHEMA 에도 있지만, Postgres 스키마의 정본은
-- 이 파일 하나다(``storage.connect()`` 는 Postgres 경로에서 DDL 을 안 돌린다).
-- 즉 이 파일로 DB 를 새로 세우면 ``backfill_markers`` 가 없고,
-- ``weekly_delisted_stocks`` 의 세 백필 태스크와 ``monthly_listed_shares_backfill``
-- 이 전부 ``relation "backfill_markers" does not exist`` 로 죽는다.
CREATE TABLE IF NOT EXISTS backfill_markers (
    code         TEXT NOT NULL,
    source       TEXT NOT NULL,   -- collectors.storage 의 CHECKED_* 상수
    checked_date DATE NOT NULL,   -- 조회해봤고 자료가 없던 날
    PRIMARY KEY (code, source)
);

-- 일반 테이블: 종목당 1행뿐이고 시계열이 아니라 하이퍼테이블 대상 아님.
--
-- naver_checked / dart_checked / naver_sd_checked 는 DEPRECATED (009) — 위
-- backfill_markers 로 옮겼고 더 이상 갱신되지 않는다. 새 DB 에는 필요 없지만,
-- 기존 DB 와 컬럼 구성을 맞춰두려고 남긴다(009 도 원본을 안 지웠다).
CREATE TABLE IF NOT EXISTS delisted_stocks (
    code            TEXT NOT NULL,
    name            TEXT,
    market          TEXT,
    last_trade_date TEXT,   -- daily_bars 기준 마지막 거래일(상장폐지일 근사), 이력 없으면 NULL
    naver_checked   DATE,   -- DEPRECATED(009) → backfill_markers(source='naver_bars')
    dart_checked    DATE,   -- DEPRECATED(009) → backfill_markers(source='dart_shares_delisted')
    naver_sd_checked DATE,  -- DEPRECATED(009) → backfill_markers(source='naver_flow')
    PRIMARY KEY (code)
);

-- krx-news-client(pip, https://github.com/younghwan91/krx-news-client)로 수집한
-- 뉴스 히스토리 — 백테스팅+실매매, 추후 LLM 매매판단용(010 참고).
-- id 는 krx-news-client의 make_article_id(source, url)이라 안정적이다 — 같은
-- 기사가 여러 피드나 여러 크롤링 주기에서 다시 들어와도 upsert가 같은 행을 갱신한다.
-- published_at 을 파티션 컬럼으로 쓰므로 PK 에 포함한다(Timescale 은 파티션 컬럼을
-- 뺀 유니크 제약을 허용하지 않는다).
CREATE TABLE IF NOT EXISTS news_articles (
    id           TEXT NOT NULL,
    source       TEXT NOT NULL,
    category     TEXT,
    title        TEXT NOT NULL,
    url          TEXT NOT NULL,
    content      TEXT,
    summary      TEXT,
    author       TEXT,
    published_at TIMESTAMPTZ NOT NULL,
    collected_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (id, published_at)
);
SELECT create_hypertable(
    'news_articles', 'published_at',
    if_not_exists => TRUE, chunk_time_interval => INTERVAL '30 days'
);

-- 관련 종목 — 정규화된 별도 테이블(종목별 "이 종목 뉴스 전체" 조회가 핵심 사용처;
-- 배열 컬럼은 sqlite 쪽(collectors/storage.py)에서 못 쓰고 인덱싱도 안 된다).
CREATE TABLE IF NOT EXISTS news_article_tickers (
    article_id TEXT NOT NULL,
    ticker     TEXT NOT NULL,
    PRIMARY KEY (article_id, ticker)
);
CREATE INDEX IF NOT EXISTS idx_nat_ticker ON news_article_tickers(ticker);

-- DART 공시 히스토리(migrations/011 참고) — krx-news-client의 DartScraper가
-- DART API의 stock_code를 그대로 티커로 주므로, news_articles와 달리 정규화
-- 테이블 없이 ticker 컬럼 하나로 충분하다(공시 1건=발행사 1곳).
CREATE TABLE IF NOT EXISTS disclosures (
    id              TEXT NOT NULL,
    source          TEXT NOT NULL,
    title           TEXT NOT NULL,
    url             TEXT NOT NULL,
    company         TEXT,
    ticker          TEXT,
    disclosure_type TEXT,
    published_at    TIMESTAMPTZ NOT NULL,
    collected_at    TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (id, published_at)
);
SELECT create_hypertable(
    'disclosures', 'published_at',
    if_not_exists => TRUE, chunk_time_interval => INTERVAL '30 days'
);
CREATE INDEX IF NOT EXISTS idx_disclosures_ticker ON disclosures(ticker);

-- Recent rows stay row-oriented (frequent upserts); anything older than 30
-- days is compressed columnar in the background — cuts disk use and speeds
-- up the long-range scans backtest/screener code does.
--
-- **왜 7일이 아니라 30일인가 (2026-08-25).** 수집기가 쓰는 창은 일봉·수급 15일,
-- 공매도·신용 10일이다. 압축 경계가 7일이면 그 창의 절반 이상이 이미 압축된
-- 청크를 때려, upsert 마다 세그먼트 압축해제→갱신→재압축이 돈다. pg_stat 에
-- 그 흔적이 그대로 남아 있었다 — 압축 청크에서 n_tup_ins ≈ n_tup_del 이고
-- n_live_tup 이 0 이다(해제된 행이 힙에 얹혔다가 재압축될 때까지 남는다).
-- 경계를 쓰는 창 밖으로 밀면 그 순환이 사라진다.
ALTER TABLE daily_bars SET (timescaledb.compress, timescaledb.compress_segmentby = 'code');
ALTER TABLE supply_demand SET (timescaledb.compress, timescaledb.compress_segmentby = 'code');
ALTER TABLE short_selling SET (timescaledb.compress, timescaledb.compress_segmentby = 'code');
ALTER TABLE credit_balance SET (timescaledb.compress, timescaledb.compress_segmentby = 'code');
ALTER TABLE sector_index SET (timescaledb.compress, timescaledb.compress_segmentby = 'code');
ALTER TABLE shares_outstanding_history SET (timescaledb.compress, timescaledb.compress_segmentby = 'code');
ALTER TABLE consensus SET (timescaledb.compress, timescaledb.compress_segmentby = 'code');
ALTER TABLE news_articles SET (timescaledb.compress, timescaledb.compress_segmentby = 'source');
ALTER TABLE disclosures SET (timescaledb.compress, timescaledb.compress_segmentby = 'ticker');
-- daily_bars_adjusted는 압축 대상에서 제외 — weekly_price_adjust가 매주 전체를
-- upsert로 재작성하므로, 압축을 걸면 매주 오래된 청크를 압축해제→재압축하는
-- 순환이 반복돼 이득 없이 CPU/IO만 낭비된다(주간 전체재생성 테이블 특성).
--
-- ⚠️ **이 주석은 한동안 거짓이었다.** 라이브 DB 에서 515청크 중 512개가 압축된
-- 상태였고(정책 없이 수동 압축된 것으로 보인다), 실측 압축률이 739MB → 901MB 로
-- **음수**였다. 주간 전량 upsert 가 그 청크들을 전부 압축해제해 놓고 재압축할
-- 정책이 없어 빈 압축청크 껍데기만 남아 있었다. 2026-08-25 에 전량
-- decompress_chunk + compress=false 로 되돌렸다. 이 파일이 요구하는 상태와
-- 실제 DB 가 어긋날 수 있다는 게 교훈이다 — init 스크립트는 최초 1회만 돈다.

SELECT add_compression_policy('daily_bars', INTERVAL '30 days');
SELECT add_compression_policy('supply_demand', INTERVAL '30 days');
SELECT add_compression_policy('short_selling', INTERVAL '30 days');
SELECT add_compression_policy('credit_balance', INTERVAL '30 days');
SELECT add_compression_policy('sector_index', INTERVAL '30 days');
SELECT add_compression_policy('shares_outstanding_history', INTERVAL '30 days');
SELECT add_compression_policy('consensus', INTERVAL '30 days');
SELECT add_compression_policy('news_articles', INTERVAL '30 days');
SELECT add_compression_policy('disclosures', INTERVAL '30 days');
-- earnings는 일반 테이블이라 압축/보존 정책 대상 아님 (위 CREATE TABLE 주석 참고).
