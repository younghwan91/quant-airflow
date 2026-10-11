-- Haiku 4.5 판정을 운영 테이블에서 빼 legacy 스키마로 보관한다 (2026-10-12)
--
-- 왜: 2026-10-12 부터 뉴스 판정은 Haiku 5.5 만 쓴다(swing-it 이 먼저, 이제 news_judge.py 도 —
-- 단가가 4.5 의 1/10 이고, 4.5 는 캐시 최소 접두가 4,096 토큰이라 ~600~1,000 토큰짜리 우리 지시문은
-- 캐싱이 원리상 불가능했다). 4.5 결과는 다시 쓰지 않는다 — 데이트레이딩·스캘핑에서 뉴스가 크게
-- 도움되지 않았고, 5.5 판정(v2r3-haiku55 등)이 이미 원장에 있다. 다만 지우지 않고 보관한다.
--
-- 옮기는 것(적용 직전 실측):
--   news_judgments    model_id claude-haiku-4-5, prompt_version v1  14,534행 (2026-09-06 ~ 10-09)
--   article_judgments judge v2-haiku45                              157,834행 (daytrade-it 유니버스 백필)
--   judges            v2-haiku45                                    1행 (FK 때문에 article 행 다음에)
-- news_judgments 에는 4.5 외 행이 없었다 — 이 뒤로는 news_judge.py v2(Haiku 5.5)가 새로 채운다
-- (PENDING_LOOKBACK_DAYS=3 이라 첫 실행이 최근 3일치만 다시 판단한다. 과거분 재판단은 안 한다 —
-- 오늘 날짜 knowledge_date 로 과거를 채우면 lookahead, CLAUDE.md §3).
--
-- legacy.* 는 운영 테이블과 같은 모양(LIKE ... INCLUDING ALL — PK·기본값·인덱스, FK 는 빠진다)이고
-- 코드 어디서도 읽지 않는다. 드라이브 core 덤프에는 계속 포함된다.
--
-- 적용: psql "$KR_QUANT_DB" -v ON_ERROR_STOP=1 -f sql/migrations/018_archive_haiku45_judgments.sql

BEGIN;

CREATE SCHEMA IF NOT EXISTS legacy;

CREATE TABLE legacy.news_judgments_haiku45    (LIKE public.news_judgments    INCLUDING ALL);
CREATE TABLE legacy.article_judgments_haiku45 (LIKE public.article_judgments INCLUDING ALL);
CREATE TABLE legacy.judges_haiku45            (LIKE public.judges            INCLUDING ALL);

INSERT INTO legacy.news_judgments_haiku45
SELECT * FROM public.news_judgments WHERE model_id LIKE 'claude-haiku-4-5%';
INSERT INTO legacy.article_judgments_haiku45
SELECT * FROM public.article_judgments WHERE judge = 'v2-haiku45';
INSERT INTO legacy.judges_haiku45
SELECT * FROM public.judges WHERE judge = 'v2-haiku45';

DELETE FROM public.news_judgments    WHERE model_id LIKE 'claude-haiku-4-5%';
DELETE FROM public.article_judgments WHERE judge = 'v2-haiku45';
DELETE FROM public.judges            WHERE judge = 'v2-haiku45';

-- 옮긴 수가 실측과 다르면(그 사이 4.5 행이 더 생겼거나 덜 옮겨졌으면) 통째로 되돌린다.
DO $$
BEGIN
    IF (SELECT count(*) FROM legacy.news_judgments_haiku45)    <> 14534
    OR (SELECT count(*) FROM legacy.article_judgments_haiku45) <> 157834
    OR (SELECT count(*) FROM legacy.judges_haiku45)            <> 1 THEN
        RAISE EXCEPTION '018: legacy 행 수가 실측과 다르다 — 롤백';
    END IF;
END $$;

COMMIT;

-- 검증:
--   SELECT count(*) FROM news_judgments WHERE model_id LIKE 'claude-haiku-4-5%';     -- 0
--   SELECT count(*) FROM article_judgments WHERE judge = 'v2-haiku45';               -- 0
--   SELECT (SELECT count(*) FROM legacy.news_judgments_haiku45),                     -- 14534
--          (SELECT count(*) FROM legacy.article_judgments_haiku45),                  -- 157834
--          (SELECT count(*) FROM legacy.judges_haiku45);                             -- 1
--
-- 롤백(되살리기):
--   BEGIN;
--   INSERT INTO judges            SELECT * FROM legacy.judges_haiku45;
--   INSERT INTO article_judgments SELECT * FROM legacy.article_judgments_haiku45;
--   INSERT INTO news_judgments    SELECT * FROM legacy.news_judgments_haiku45;
--   DROP SCHEMA legacy CASCADE;
--   COMMIT;
--   단 news_judgments 는 v1 행이 돌아와도 news_judge.py 가 v2 만 보므로 다시 판단되지 않는다.
