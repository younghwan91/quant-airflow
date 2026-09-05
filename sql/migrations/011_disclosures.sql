-- disclosures: DART 공시 히스토리 (백테스팅+실매매, 추후 LLM 매매판단용)
--
-- krx-news-client(pip, https://github.com/younghwan91/krx-news-client)의
-- DartScraper.scrape_disclosures()가 DART Open API의 stock_code를 그대로 티커로
-- 돌려준다 — news_articles(migrations/010)와 달리 티커가 구조화 데이터에서 오므로
-- 정규화 테이블(news_article_tickers 같은)이 필요 없다. 공시 1건은 발행사 1곳이라
-- ticker 컬럼 하나로 충분하다.
--
-- id는 krx-news-client의 make_article_id(source, url) = f"{source}:{md5(url)[:12]}"
-- 라 안정적이다 — collectors/news_dart.py가 하루 두 번(daily_news DAG) 같은 날짜
-- 구간을 다시 조회해도 upsert가 같은 행을 갱신한다.
--
-- published_at을 hypertable 파티션 컬럼으로 쓰므로 PK에 함께 넣는다(Timescale은
-- 파티션 컬럼을 뺀 유니크 제약을 허용하지 않는다) — id가 이미 전역 유니크라
-- published_at을 더해도 실질적인 유니크 범위는 안 바뀐다(news_articles와 동일 패턴).
--
-- 적용:
--   psql "$DB_URL" -v ON_ERROR_STOP=1 -f sql/migrations/011_disclosures.sql

BEGIN;

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

ALTER TABLE disclosures SET (timescaledb.compress, timescaledb.compress_segmentby = 'ticker');
SELECT add_compression_policy('disclosures', INTERVAL '30 days');

COMMIT;

-- 검증:
--   SELECT count(*) FROM disclosures;
--   SELECT ticker, count(*) FROM disclosures GROUP BY ticker ORDER BY 2 DESC LIMIT 10;
--
-- 롤백:
--   DROP TABLE disclosures;
