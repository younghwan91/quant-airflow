-- 035: naver_research_reports.prev_goal_price 를 NULL 로 (사용자 승인 2026-10-13)
--
-- 왜: 네이버 상세 API 의 prevGoalPrice 는 이름과 달리 직전 목표가가 아니라 priceAtWriteDate(작성일 전 거래일
-- 종가)와 같은 값이다. 2026-10-13 실측: prev_goal_price 가 있는 37,518행 100% 가 price_at_write 와 같다
-- (005930 research_id 74557: goal 110,000 · prevGoalPrice 75,300 = priceAtWriteDate 75,300 = 2024-06-04 종가).
-- swing-it 이 이 열을 직전 목표가로 읽어 시험 31 첫 실행이 무효가 됐다. 직전 목표가는 원천에 없다 — 필요하면
-- 같은 (code, broker) 의 이전 날짜 리포트 goal_price 로 만든다. 같은 값이 price_at_write 에 남아 있어 legacy 로
-- 옮길 정보가 없다(CLAUDE.md §3 의 legacy 이동은 값이 사라질 때). 수집기도 이 열에 NULL 만 쓴다(naver_mstock).

BEGIN;
DO $$
DECLARE n bigint; m bigint;
BEGIN
  SELECT count(*) INTO n FROM naver_research_reports WHERE prev_goal_price IS NOT NULL;
  SELECT count(*) INTO m FROM naver_research_reports
   WHERE prev_goal_price IS NOT NULL AND prev_goal_price IS DISTINCT FROM price_at_write;
  IF m <> 0 THEN RAISE EXCEPTION 'prev_goal_price 가 price_at_write 와 다른 행 % — 정보가 사라지므로 중단', m; END IF;
  IF n NOT IN (0, 37518) THEN RAISE EXCEPTION '예상 37518행(또는 재실행 0), 실제 %', n; END IF;
END $$;
UPDATE naver_research_reports SET prev_goal_price = NULL WHERE prev_goal_price IS NOT NULL;
COMMENT ON COLUMN naver_research_reports.prev_goal_price IS
  '항상 NULL — 원천 prevGoalPrice 는 전 거래일 종가(price_at_write 와 같음)라 버린다(035). 직전 목표가는 원천에 없다';
COMMENT ON COLUMN naver_research_reports.price_at_write IS '작성일 전 거래일 종가(원천 priceAtWriteDate, 2020~ 만 있음)';
COMMIT;

-- 검증: 0 이어야 한다
--   SELECT count(*) FROM naver_research_reports WHERE prev_goal_price IS NOT NULL;
-- 롤백(값 복원): UPDATE naver_research_reports SET prev_goal_price = price_at_write WHERE price_at_write IS NOT NULL;
