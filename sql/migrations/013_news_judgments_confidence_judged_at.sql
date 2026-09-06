-- news_judgments에 confidence/judged_at 추가 — scalp-it 세션 요청(2026-09-06)
--
-- 왜: 012에서 news_judgments를 만들 때 스캘핑(단타) 컨슈머 요구사항을
-- 세션간으로 물어봤더니(quant-airflow-orca ↔ scalp-it) 두 가지가 빠져
-- 있다는 답이 왔다:
--
--   1. confidence(신뢰도 점수) 없이는 sentiment_direction만으로 오탐을
--      못 거른다 — "즉시 진입 트리거"가 아니라 "기존 신호에 곱하는
--      필터/가중치"로 쓰려면 sentiment_direction + confidence 조합이
--      최소 요구사항이라고 확인받았다.
--   2. knowledge_date가 DATE(일 단위)라 "뉴스 발행 후 몇 초 만에 판단이
--      나왔는지" 레이턴시를 못 잰다. scalp-it이 이미 체결 기반 신호에서
--      "이미 일어난 일을 후행으로 근사"해 기각한 사례가 있다고 지적했다 —
--      뉴스 판단도 늦으면 같은 함정이라 초 단위 judged_at이 필요하다.
--
-- confidence는 nullable — LLM이 매기는 값이라 012 시점에 이미 쌓인 행은
-- 소급 계산이 안 된다(재현 불가능한 회고적 판단, CLAUDE.md §3 "오늘 값을
-- 과거로 복사하지 않는다"와 같은 부류). judged_at도 마찬가지로 시스템이
-- 그 시점에 관측한 값이라 과거 행엔 못 채운다 — NULL이 "013 이전 행"이라는
-- 뜻이다(NULL=모름, CLAUDE.md §3).
--
-- judged_at은 LLM 응답 내용이 아니라 collectors/news_judge.py의 collect()가
-- generate() 호출이 돌아온 직후 시스템 시계로 찍는다(LLM이 스스로 주장하는
-- 시각이 아니다) — TIMESTAMPTZ로 둔다(knowledge_date/DATE와 달리 초 단위
-- 레이턴시 계산이 목적이라 시간대 포함 정밀도가 필요하다).
--
-- 적용:
--   psql "$DB_URL" -v ON_ERROR_STOP=1 -f sql/migrations/013_news_judgments_confidence_judged_at.sql

BEGIN;

ALTER TABLE news_judgments
    ADD COLUMN IF NOT EXISTS confidence INTEGER
        CHECK (confidence IS NULL OR (confidence BETWEEN 0 AND 100));
ALTER TABLE news_judgments
    ADD COLUMN IF NOT EXISTS judged_at TIMESTAMPTZ;

COMMENT ON COLUMN news_judgments.confidence IS
    'LLM 자체 확신도 0~100 (scalp-it 요청, 2026-09-06). NULL = 013 이전 행(소급 불가).';
COMMENT ON COLUMN news_judgments.judged_at IS
    'generate() 응답이 실제로 돌아온 시각(시스템 시계, UTC) — published_at 대비 레이턴시 측정용. '
    'NULL = 013 이전 행(소급 불가).';

COMMIT;

-- 검증:
--   SELECT column_name, data_type FROM information_schema.columns
--   WHERE table_name = 'news_judgments' AND column_name IN ('confidence', 'judged_at');
--
--   -- confidence 범위 위반 0행이어야 한다
--   SELECT count(*) FROM news_judgments WHERE confidence IS NOT NULL
--     AND (confidence < 0 OR confidence > 100);
--
-- 롤백:
--   ALTER TABLE news_judgments DROP COLUMN confidence, DROP COLUMN judged_at;
