-- earnings 의 "장기 단위·통화 오류" 2종목 41행을 legacy 로 옮긴다 (2026-10-12, 사용자 승인)
--
-- 왜: 023 의 튀는 행 기준(종목 중앙값의 500배)으로는 **여러 해 줄곧** 틀린 종목이 거꾸로 걸린다.
-- 종목별 매출 시계열에서 중앙값이 ~10^2.4 이상 계단식으로 바뀌는 지점을 찾으니 둘뿐이었다:
--   008700 아남전자   2019Q1 부터 30분기 ≈ ×1/1,000 (2018Q4 1.9e11 → 2019Q1 3.8e7 원) — 천 원 단위 제출 추정
--   241560 두산밥캣   2023Q4 부터 11분기 ≈ ×1/1,300 (2023Q3 2.4e12 → 2023Q4 7.5e9) — 달러 보고 전환 추정(환율 비)
-- 원화 기준 분석엔 둘 다 틀린 값이다. 환산해 넣지 않는다(틀린 값보다 없는 값 — 022·023 과 같은 이유).
--
-- 적용: psql "$KR_QUANT_DB" -v ON_ERROR_STOP=1 -f sql/migrations/025_archive_earnings_scale_shift.sql

BEGIN;

CREATE TABLE legacy.earnings_scale_shift (LIKE public.earnings INCLUDING DEFAULTS);

INSERT INTO legacy.earnings_scale_shift
SELECT * FROM public.earnings
 WHERE (code = '008700' AND period >= '2019Q1') OR (code = '241560' AND period >= '2023Q4');

DELETE FROM public.earnings
 WHERE (code = '008700' AND period >= '2019Q1') OR (code = '241560' AND period >= '2023Q4');

DO $$
BEGIN
    IF (SELECT count(*) FROM legacy.earnings_scale_shift) <> 41 THEN
        RAISE EXCEPTION '025: 옮긴 행이 41 이 아니다 — 롤백';
    END IF;
END $$;

COMMIT;

-- 검증:  SELECT code, count(*) FROM legacy.earnings_scale_shift GROUP BY 1;   -- 008700 30, 241560 11
-- 주의: 이후 daily_earnings 가 이 분기들을 "없다" 로 보고 다시 받아 같은 값을 넣을 수 있다 —
--       collectors/dart_earnings.py 의 SCALE_SHIFT_EXCLUDE 가 막는다(같은 커밋).
-- 롤백: INSERT INTO earnings SELECT * FROM legacy.earnings_scale_shift; DROP TABLE legacy.earnings_scale_shift;
