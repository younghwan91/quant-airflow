-- news_articles: 트레이딩 판단(백테스팅+실매매, 추후 LLM 매매판단)용 뉴스 히스토리
--
-- krx-news-client(pip, https://github.com/younghwan91/krx-news-client)가 토스/
-- 한경/더벨/DART 뉴스·공시를 정규화된 형태로 반환하고, collectors/news_toss.py가
-- 그 결과를 여기 upsert한다. 지금까지 이 데이터는 별도 저장소(krx-news-rest-api)의
-- Redis 캐시에만 있었는데, TTL이 걸려 있고 dedup도 깨져 있어 백테스팅용 히스토리로
-- 못 썼다(같은 기사가 collected_at 차이로 계속 새 항목으로 쌓이는 버그).
--
-- id는 krx-news-client의 make_article_id(source, url) = f"{source}:{md5(url)[:12]}"
-- 라 안정적이다 — 같은 기사를 여러 피드(예: 토스의 ALL_HIGHLIGHT/HOT)나 여러 크롤링
-- 주기에서 다시 받아도 upsert가 같은 행을 갱신한다.
--
-- published_at 을 hypertable 파티션 컬럼으로 쓰므로, PK 에 함께 넣는다(Timescale은
-- 파티션 컬럼을 뺀 유니크 제약을 허용하지 않는다) — id 가 이미 전역 유니크라
-- published_at 을 더해도 실질적인 유니크 범위는 안 바뀐다.
--
-- 관련 종목(tickers)은 별도 테이블로 정규화한다 — 종목별 "이 종목 뉴스 전체" 조회가
-- 핵심 사용처인데, 배열 컬럼은 sqlite 쪽(collectors/storage.py)에서 못 쓰고 인덱싱도
-- 안 된다.
--
-- 적용:
--   psql "$DB_URL" -v ON_ERROR_STOP=1 -f sql/migrations/010_news_articles.sql

BEGIN;

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

CREATE TABLE IF NOT EXISTS news_article_tickers (
    article_id TEXT NOT NULL,
    ticker     TEXT NOT NULL,
    PRIMARY KEY (article_id, ticker)
);
CREATE INDEX IF NOT EXISTS idx_nat_ticker ON news_article_tickers(ticker);

ALTER TABLE news_articles SET (timescaledb.compress, timescaledb.compress_segmentby = 'source');
SELECT add_compression_policy('news_articles', INTERVAL '30 days');

COMMIT;

-- 검증:
--   SELECT source, count(*) FROM news_articles GROUP BY source;
--   SELECT count(DISTINCT article_id) FROM news_article_tickers;
--
-- 롤백:
--   DROP TABLE news_article_tickers;
--   DROP TABLE news_articles;
