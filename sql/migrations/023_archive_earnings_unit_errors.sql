-- earnings 의 DART 단위 오류 20행을 legacy 로 옮긴다 (2026-10-12, 사용자 승인)
--
-- 왜: 데이터 점검(하이쿠 감사)에서 순이익·매출·영업이익이 수천조~수경 원인 행이 나왔다
-- (예: 007720 2024Q3 매출 3.7e16 원 — 앞뒤 분기 3.5e10·1.3e11). 022 의 주식수와 같은 원천 문제로,
-- 회사가 단위를 잘못 적어 낸 수치를 fnlttMultiAcnt 가 그대로 준다.
--
-- 고른 기준(적용 직전 실측): 매출·순이익·영업이익 중 하나가 그 종목 중앙값의 500배 이상이고 1e11 원
-- 초과 — 23행. 앞뒤 분기와 한 행씩 대조해 20행을 확정했다. **뺀 3행**은 008700 2016~2018 Q4 로,
-- 오히려 정상값이다(그 종목은 2019 년부터 줄곧 1/1,000 로 들어가 중앙값이 거꾸로 작다 — 이런 "장기
-- 단위 오류" 종목은 별도 조사, 여기서는 건드리지 않는다).
--
-- 나눠서 고치지 않는다(022 와 같은 이유 — 틀린 값보다 없는 값). 그 분기는 earnings 에서 없어진다.
-- 다음 해 같은 분기 행의 *_prior 컬럼에 이 오류가 남아 있을 수 있다(전년 동기 값을 DART 가 같이 준다).
--
-- 적용: psql "$KR_QUANT_DB" -v ON_ERROR_STOP=1 -f sql/migrations/023_archive_earnings_unit_errors.sql

BEGIN;

CREATE SCHEMA IF NOT EXISTS legacy;
CREATE TABLE legacy.earnings_unit_errors (LIKE public.earnings INCLUDING DEFAULTS);

CREATE TEMP TABLE bad(code TEXT, period TEXT, knowledge_date DATE) ON COMMIT DROP;
INSERT INTO bad VALUES
    ('007720', '2024Q3', DATE '2024-11-14'),
    ('017680', '2023Q3', DATE '2023-11-14'),
    ('032080', '2019Q3', DATE '2019-11-14'),
    ('032680', '2022Q4', DATE '2023-03-31'),
    ('039230', '2021Q4', DATE '2022-03-31'),
    ('058820', '2024Q1', DATE '2024-05-15'),
    ('060310', '2022Q3', DATE '2022-11-14'),
    ('065150', '2023Q4', DATE '2024-03-30'),
    ('069330', '2020Q1', DATE '2020-05-15'),
    ('073540', '2024Q1', DATE '2024-05-15'),
    ('096040', '2025Q3', DATE '2025-11-14'),
    ('102260', '2017Q2', DATE '2017-08-14'),
    ('144620', '2020Q1', DATE '2020-05-15'),
    ('160600', '2025Q2', DATE '2025-08-14'),
    ('234080', '2021Q2', DATE '2021-08-14'),
    ('263540', '2022Q2', DATE '2022-08-14'),
    ('311390', '2022Q4', DATE '2023-03-31'),
    ('323350', '2024Q2', DATE '2024-08-14'),
    ('439250', '2024Q1', DATE '2024-05-15'),
    ('484130', '2025Q1', DATE '2025-05-15');

-- 아직 터무니없는 값인 행만 — 그 사이 다시 수집돼 바뀐 행은 건드리지 않는다.
INSERT INTO legacy.earnings_unit_errors
SELECT e.* FROM public.earnings e JOIN bad b USING (code, period, knowledge_date)
 WHERE greatest(abs(coalesce(e.revenue, 0)), abs(coalesce(e.netinc, 0)), abs(coalesce(e.op_income, 0))) > 1e11;

DELETE FROM public.earnings e USING bad b
 WHERE e.code = b.code AND e.period = b.period AND e.knowledge_date = b.knowledge_date
   AND greatest(abs(coalesce(e.revenue, 0)), abs(coalesce(e.netinc, 0)), abs(coalesce(e.op_income, 0))) > 1e11;

DO $$
BEGIN
    IF (SELECT count(*) FROM legacy.earnings_unit_errors) <> 20 THEN
        RAISE EXCEPTION '023: 옮긴 행이 20 이 아니다 — 롤백';
    END IF;
END $$;

COMMIT;

-- 검증:
--   SELECT count(*) FROM legacy.earnings_unit_errors;                                        -- 20
--   SELECT count(*) FROM earnings WHERE abs(revenue) > 1e15 OR abs(netinc) > 1e15;            -- 0
--
-- 롤백:
--   INSERT INTO earnings SELECT * FROM legacy.earnings_unit_errors;
--   DROP TABLE legacy.earnings_unit_errors;
