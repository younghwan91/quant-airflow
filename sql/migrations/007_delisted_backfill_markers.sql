-- delisted_stocks: DART 주식수 조회가 "자료 없음"이었던 코드를 기록
--
-- 004(naver_checked)와 **같은 병, 같은 약이다.** `weekly_delisted_stocks` 의
-- backfill_delisted_shares 태스크는 "폐지 시세는 있는데 주식수가 없는 종목"을
-- 매주 다시 훑는다(collectors/dart_shares.py:_targets). 그런데 조회 결과가
-- `no_corp`(corp_code 매핑 없음) 이나 `missing`(DART 에 자료 없음) 이면 **어디에도
-- 기록하지 않아서**, 그 종목들은 주식수가 영원히 안 생기고 따라서 대상 쿼리에
-- 영원히 걸린다.
--
-- 실측(2026-08-25): 남은 대상 42종목이 전부 그 상태다. 주당 2.2분 = 연 약 1.9시간을
-- 성과 0행으로 쓴다. 종목당 최대 `4개 보고서 × 거래연수` 번의 DART 호출이
-- 붙으므로 일한도도 함께 갉는다.
--
-- 상장폐지는 과거 사실이라 한 번 "DART 에 자료 없음"이면 영원히 그렇다.
--
-- 적용:
--   psql "$DB_URL" -v ON_ERROR_STOP=1 -f sql/migrations/007_delisted_dart_checked.sql
--
-- 다시 훑고 싶으면: UPDATE delisted_stocks SET dart_checked = NULL, naver_sd_checked = NULL;
-- 또는 수집기에 --refetch.

BEGIN;

ALTER TABLE delisted_stocks ADD COLUMN IF NOT EXISTS dart_checked DATE;
ALTER TABLE delisted_stocks ADD COLUMN IF NOT EXISTS naver_sd_checked DATE;

COMMENT ON COLUMN delisted_stocks.dart_checked IS
    'DART stockTotqySttus 를 조회했으나 주식수 자료를 못 찾은 날(corp_code 매핑 없음 '
    '포함). NULL = 아직 확인 안 함. 자료를 받은 코드는 shares_outstanding_history 에 '
    'source=dart 로 남으므로 이 컬럼을 쓰지 않는다.';

-- naver_sd_checked 는 같은 병의 세 번째 사례다(naver_delisted_bars 는 004 에서 이미
-- 고쳤다). backfill_delisted_flow 는 "수급 행이 없는 코드"를 대상으로 삼는데, 네이버가
-- 빈 응답을 준 코드는 행이 안 생겨 영원히 재조회된다. 현재 대상 0건이라 실비용은
-- 없지만, 그런 종목이 하나라도 생기면 fetch_flow 가 종목당 최대 120페이지를 매주
-- 넘긴다(2026-08-15 첫 실행 175.3분이 그 페이지 비용의 크기다).
COMMENT ON COLUMN delisted_stocks.naver_sd_checked IS
    '네이버 수급(외국인·기관)을 조회했으나 빈 응답이던 날. NULL = 아직 확인 안 함.';

COMMIT;

-- 검증:
--   SELECT count(*) FILTER (WHERE dart_checked IS NULL) AS 미확인,
--          count(*) FILTER (WHERE dart_checked IS NOT NULL) AS 확인됨
--     FROM delisted_stocks;
--
-- 롤백:
--   ALTER TABLE delisted_stocks DROP COLUMN dart_checked, DROP COLUMN naver_sd_checked;
