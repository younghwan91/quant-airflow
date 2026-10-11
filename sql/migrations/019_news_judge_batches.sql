-- news_judge_batches: news_judge.py 의 Message Batches 원장 (2026-10-12)
--
-- 왜: 2026-10-12 사용자 규칙 — 기사 LLM 분석은 항상 Haiku 5.5 + 프롬프트 캐싱 + Batch API.
-- news_judge.py 를 동기 messages.create 루프(v1)에서 배치로 바꾸면서, 낸 배치를 기억할 곳이 필요하다:
--   - 배치 결과는 제출 후 최대 24시간 — 태스크가 못 기다리고 끝나면 다음 실행(08:45/10:05/16:05)이
--     그 배치를 수거해야 한다.
--   - DEFAULT_TASK_KW 가 retries=1 이라, 제출 뒤 태스크가 죽어 재시도되면 같은 기사를 다시 내
--     두 번 과금된다. 원장에 있는 미수거 배치의 기사는 다시 내지 않는다.
-- batch_id 는 create 가 돌아오자마자 쓴다. items 는 custom_id(r0, r1, …) → (source_type, source_id,
-- ticker) 매핑 — custom_id 는 [A-Za-z0-9_-]{1,64} 라 'toss:…'·'dart:…' id 를 그대로 못 쓴다.
--
-- 적용: psql "$KR_QUANT_DB" -v ON_ERROR_STOP=1 -f sql/migrations/019_news_judge_batches.sql

BEGIN;

CREATE TABLE IF NOT EXISTS news_judge_batches (
    batch_id        TEXT PRIMARY KEY,      -- Message Batches id — create 직후 곧바로 기록
    model_id        TEXT NOT NULL,
    prompt_version  TEXT NOT NULL,
    submitted_at    TIMESTAMPTZ NOT NULL,
    items           JSONB NOT NULL,        -- [[source_type, source_id, ticker], ...] — custom_id r{i} 의 i 번째
    collected_at    TIMESTAMPTZ            -- NULL = 아직 수거 안 함(news_judge.py 가 다음 실행에 먼저 수거)
);
CREATE INDEX IF NOT EXISTS idx_news_judge_batches_open ON news_judge_batches(submitted_at) WHERE collected_at IS NULL;

COMMIT;

-- 검증:
--   SELECT count(*) FROM news_judge_batches;                                   -- 적용 직후 0
--   SELECT batch_id, submitted_at, jsonb_array_length(items), collected_at
--     FROM news_judge_batches ORDER BY submitted_at DESC LIMIT 5;              -- 첫 실행 뒤 한 행, collected_at 채워짐
--
-- 롤백: DROP TABLE news_judge_batches;  (news_judge.py 를 v2 이전으로 되돌린 뒤에만 — v2 는 이 테이블 없이 못 돈다)
