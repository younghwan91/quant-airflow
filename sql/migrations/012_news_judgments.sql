-- news_judgments: LLM이 news_articles/disclosures를 읽고 낸 구조화된 판단.
--
-- 왜: daily_news가 쌓는 원문 텍스트만으로는 단기 트레이딩 신호로 못 쓴다.
-- event_type/sentiment/related_codes/is_stale_repeat를 LLM이 구조화해 이
-- 테이블에 남긴다. 설계 근거는
-- docs/superpowers/specs/2026-09-06-news-llm-judgments-design.md 참고 —
-- 특히 related_codes(동일 테마 페어트레이드용)와 is_stale_repeat(재탕 뉴스
-- 판별)는 scalp-it 세션의 실제 트레이더 인터뷰 피드백으로 추가됐다.
--
-- earnings와 같은 이유로 일반 테이블이다(hypertable 아님) — 물량이 하루
-- 수십~수백 건이라 압축 정책 대상이 아니다.
--
-- related_codes는 TEXT(JSON 인코딩 배열) — Postgres TEXT[]를 안 쓰는 이유는
-- collectors/storage.py의 _upsert()가 타입 어댑터 없이 파라미터를 그대로
-- 바인딩해서, 배열 타입을 쓰면 Postgres/sqlite 두 경로가 갈라지기 때문이다.
--
-- upsert 키에 prompt_version이 들어가는 이유: 프롬프트/모델이 바뀌면 새
-- 버전으로 새 행을 쌓고 기존 행은 절대 안 고친다 — earnings의
-- knowledge_date 정정 이력 규약과 같은 재현성 원칙(한 번 쓴 LLM 판단은
-- 그 시점의 사실로 고정, 나중 모델로 재해석해서 덮어쓰지 않는다).
--
-- 적용:
--   psql "$DB_URL" -v ON_ERROR_STOP=1 -f sql/migrations/012_news_judgments.sql

BEGIN;

CREATE TABLE IF NOT EXISTS news_judgments (
    source_type         TEXT NOT NULL,   -- 'news' | 'disclosure'
    source_id           TEXT NOT NULL,   -- news_articles.id 또는 disclosures.id
    ticker              TEXT NOT NULL,
    event_type          TEXT NOT NULL,   -- 실적/유상증자/자사주/최대주주변경/소송/가이던스/규제/기타
    sentiment_direction INTEGER NOT NULL,  -- -1/0/1
    related_codes       TEXT NOT NULL DEFAULT '[]',  -- JSON 배열 문자열
    is_stale_repeat     BOOLEAN NOT NULL DEFAULT FALSE,
    first_seen_date     DATE,            -- is_stale_repeat=true일 때만 채움
    price_impact_likely BOOLEAN NOT NULL DEFAULT FALSE,
    rationale           TEXT NOT NULL DEFAULT '',
    model_id            TEXT NOT NULL,
    prompt_version      TEXT NOT NULL,
    knowledge_date      DATE NOT NULL,   -- 판단이 실제로 이뤄진 날 (백필 없음)
    PRIMARY KEY (source_type, source_id, ticker, prompt_version)
);
CREATE INDEX IF NOT EXISTS idx_news_judgments_ticker ON news_judgments(ticker);
CREATE INDEX IF NOT EXISTS idx_news_judgments_knowledge_date ON news_judgments(knowledge_date);

COMMIT;

-- 검증:
--   SELECT source_type, source_id, ticker, event_type FROM news_judgments LIMIT 5;
--   -- upsert 멱등성: 재실행 후 (source_type,source_id,ticker,prompt_version) 중복 0행
--   SELECT source_type, source_id, ticker, prompt_version, count(*)
--   FROM news_judgments GROUP BY 1,2,3,4 HAVING count(*) > 1;
--
-- 롤백:
--   DROP TABLE news_judgments;
