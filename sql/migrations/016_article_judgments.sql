-- judges / article_judgments: 기사 LLM 판정 원장 — 모든 트레이더 레포가 읽고, 판정을 만든 레포가 쓴다
--
-- 왜: 2026-10-09 기준 같은 토스 기사에 대한 LLM 판정이 네 군데 흩어져 있었다 —
--   - news_judgments(012, Haiku 4.5 v1, 공시 포함, 약 1.4만) — daytrade-it 실시간 폴러·scalp-it 이 읽는다
--   - daytrade-it/data/eval/cache/universe_v2.jsonl (Haiku 4.5 v2, 종목 유니버스 백필 156,605)
--   - swing-it/data/eval/cache/haiku55/persistence_v2.jsonl (Haiku 5.5 v2+r3 지속성, 36,171, 늘어나는 중)
--   - daytrade-it 프롬프트 비교 캐시(v1~v4, 수백)
-- 파일들은 git 밖이고 swing-it 쪽은 드라이브 백업 대상도 아니었다. 기사는 015 로 이미 이 DB 에
-- 있으니 판정도 같은 곳에 두면 조인·백업·재판정 건너뛰기가 한 곳에서 된다.
--
-- 원칙:
--   - 판정 하나 = judges 의 이름 하나. 모델·system 프롬프트·user 템플릿·파라미터가 **한 글자라도
--     바뀌면 새 이름**이다. 같은 이름의 행은 같은 입력 규칙으로 만들어졌다는 보장.
--   - article_judgments 는 (기사, 종목, judge) 당 한 행. 성공한 판정은 **덮어쓰지 않는다**(재현성 —
--     news_judgments 의 prompt_version 규칙과 같다). 실패 행(output NULL)만 재시도가 성공으로 바꾼다.
--   - output 은 JSONB 통째로 — judge 마다 출력 형식이 달라 공통 컬럼에 억지로 맞추지 않는다.
--   - judged_at 을 남긴다. 3년치 백필 판정은 기사보다 한참 뒤에 만들어졌다 — 과거 시점 백테스트가
--     "그때 알았던 것" 을 따지려면 이 시각이 필요하다(earnings.knowledge_date 와 같은 원칙).
--     옛 배치 캐시는 결과 시각이 없어 배치 제출 시각을 넣고 judged_at_exact=FALSE 로 표시한다
--     (Batch API 결과는 제출 후 24시간 안).
--   - news_judgments(012) 는 그대로 둔다 — 실매매 경로가 읽고 있어 장중에 깨면 손해다.
--
-- 쓰기 권한 규약: 각 레포는 **자기가 등록한 judge 의 행만** 쓴다(owner_repo).
--
-- 적용:
--   psql "$DB_URL" -v ON_ERROR_STOP=1 -f sql/migrations/016_article_judgments.sql

BEGIN;

CREATE TABLE IF NOT EXISTS judges (
    judge          TEXT PRIMARY KEY,        -- 예: 'v2-haiku45', 'v2r3-haiku55'
    model          TEXT NOT NULL,
    system_sha256  TEXT NOT NULL,
    system_prompt  TEXT NOT NULL,
    user_template  TEXT NOT NULL,
    params         JSONB NOT NULL DEFAULT '{}'::jsonb,  -- max_tokens·temperature·thinking 등
    output_schema  TEXT NOT NULL,           -- 출력 형식 이름(예: 'v2')
    owner_repo     TEXT NOT NULL,           -- 이 judge 의 행을 쓰는 유일한 레포
    description    TEXT NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS article_judgments (
    article_id            TEXT NOT NULL,         -- news_articles.id
    code                  TEXT NOT NULL,
    judge                 TEXT NOT NULL REFERENCES judges(judge),
    input_hash            TEXT NOT NULL,         -- sha256(model, system, user) — 판정 입력의 지문
    output                JSONB,                 -- NULL = 실패(error 에 사유)
    error                 TEXT,
    judged_at             TIMESTAMPTZ NOT NULL,
    judged_at_exact       BOOLEAN NOT NULL,      -- FALSE = 배치 제출 시각(결과는 그 뒤 24h 안)
    batch_id              TEXT,
    input_tokens          INTEGER,               -- 실측 usage(옛 캐시는 NULL — 모름)
    cache_creation_tokens INTEGER,
    cache_read_tokens     INTEGER,
    output_tokens         INTEGER,
    PRIMARY KEY (article_id, code, judge),
    CHECK ((output IS NULL) <> (error IS NULL))
);
CREATE INDEX IF NOT EXISTS idx_aj_judge ON article_judgments(judge);
CREATE INDEX IF NOT EXISTS idx_aj_code ON article_judgments(code);

COMMIT;

-- 검증:
--   SELECT judge, count(*), count(output), count(error), min(judged_at), max(judged_at)
--     FROM article_judgments GROUP BY judge;
--   -- 판정 대상 기사가 전부 news_articles 에 있다(0 이어야 한다):
--   SELECT count(*) FROM article_judgments j
--    WHERE NOT EXISTS (SELECT 1 FROM news_articles a WHERE a.id = j.article_id);
--
-- 롤백:
--   DROP TABLE article_judgments;
--   DROP TABLE judges;
