-- news_company_feed / news_company_feed_fetches: 토스 종목별 뉴스 피드의 영속 저장
--
-- 왜: 스윙 연구(swing-it news_persistence_swing)와 daytrade-it 4단계 전진 홀드아웃이
-- 쓰는 3년치 종목별 뉴스(2023-03-16~, 기사 행 141만)가 지금까지 DB 밖에 있었다 —
-- daytrade-it/data/toss_universe_42m/<code>.jsonl 파일, 그리고 그걸 매일 늘리는
-- 비관리 크론(16:30)이 **아카이브 태그에 고정된 분리 워크트리**(daytrade-it-rlnews,
-- archive/feat-pipeline-timing)의 스크립트를 돌리는 구조였다(2026-10-08 확인). 그
-- 워크트리를 지우면 수집이 멈추고, 파일이라 이 DB 의 upsert·커버리지 점검을 못 받았다.
--
-- 기사 본체는 새 테이블을 만들지 않고 news_articles 에 넣는다. 토스 기사 id 는
-- krx-news-client 의 make_article_id('toss', build_article_url(newsId)) 라 daily_news
-- (토스 하이라이트 피드)와 **같은 기사면 같은 id** 가 된다 — 실측: 2026-09-01 이후
-- 파일 기사 36,387 중 1,171 이 DB 의 토스 행과 같은 id, 그중 1,154 는 published_at 도
-- 같았다. 나머지 17 은 토스가 기사를 고쳐 createdAt 이 바뀐 것이라, PK(id,
-- published_at) 로 그냥 넣으면 같은 기사가 두 행이 된다 → 콜렉터는 이미 있는 id 는
-- 건너뛴다(먼저 본 판을 남긴다 — 나중 판은 lookahead).
--
-- 여기 두 테이블이 담는 것은 기사가 아니라 **"어느 종목 피드에 떴나"** 와 **"어디까지
-- 받았나"** 다:
--   - news_company_feed: (기사, 종목) — 그 종목의 /news/companies/{code} 피드에 그
--     기사가 있었다. 토스가 태그한 종목(news_article_tickers, 대부분 null 인
--     stockCodes)과 정의가 달라 섞지 않는다. 연구의 "관련 종목·팬아웃" 이 이것이다.
--   - news_company_feed_fetches: 종목별 수집 원장. 피드는 최대 100페이지(1만 건)까지만
--     주므로(page_cap) 대형주는 오래된 기사가 없다 — "그날 그 종목 기사가 0건" 과
--     "그날은 못 받았다" 를 구분하려면 이 원장이 있어야 한다(NULL 과 0 의 구분).
--
-- 적용:
--   psql "$DB_URL" -v ON_ERROR_STOP=1 -f sql/migrations/015_news_company_feed.sql

BEGIN;

CREATE TABLE IF NOT EXISTS news_company_feed (
    article_id    TEXT NOT NULL,         -- news_articles.id
    code          TEXT NOT NULL,         -- 피드를 조회한 종목
    first_seen_at TIMESTAMPTZ NOT NULL,  -- 이 쌍을 처음 받은 시각(아카이브는 그 종목 수집 완료 시각)
    source        TEXT NOT NULL,         -- 'toss_company_feed' | 'toss_archive_import'
    -- 이 쌍을 넣을 때 news_articles 행도 새로 만들었나(이미 있던 기사면 FALSE).
    -- 롤백이 daily_news 가 넣은 기사를 건드리지 않게 하는 유일한 표시다 —
    -- daily_news 도 category='stock' 을 쓴다(SOARING_STOCK 피드, 2026-10-08 실측 433행).
    inserted_article BOOLEAN NOT NULL,
    PRIMARY KEY (article_id, code)
);
CREATE INDEX IF NOT EXISTS idx_ncf_code ON news_company_feed(code);

CREATE TABLE IF NOT EXISTS news_company_feed_fetches (
    code        TEXT NOT NULL,
    run_started TIMESTAMPTZ NOT NULL,
    since       DATE NOT NULL,           -- 요청한 하한(KST 날짜)
    status      TEXT NOT NULL,           -- done | page_cap | failed
    oldest_at   TIMESTAMPTZ,             -- 받은 가장 오래된 기사 — page_cap 이면 실제 하한
    n_rows      INTEGER NOT NULL,
    finished_at TIMESTAMPTZ NOT NULL,
    source      TEXT NOT NULL,           -- 'toss_company_feed' | 'toss_archive_import'
    PRIMARY KEY (code, run_started)
);

COMMIT;

-- 검증:
--   SELECT source, count(*), count(DISTINCT code) FROM news_company_feed GROUP BY source;
--   SELECT status, source, count(*) FROM news_company_feed_fetches GROUP BY 1, 2;
--   -- 피드 쌍의 기사가 전부 news_articles 에 있다(0 이어야 한다):
--   SELECT count(*) FROM news_company_feed f
--    WHERE NOT EXISTS (SELECT 1 FROM news_articles a WHERE a.id = f.article_id);
--
-- 롤백(이 파이프라인이 새로 만든 기사만 지운다 — 순서 주의, 피드 표를 먼저 지우면
-- 어떤 기사를 넣었는지 잃는다):
--   DELETE FROM news_articles a USING news_company_feed f
--    WHERE f.inserted_article AND a.id = f.article_id;
--   DROP TABLE news_company_feed_fetches;
--   DROP TABLE news_company_feed;
