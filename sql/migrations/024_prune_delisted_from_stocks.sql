-- stocks(상장 종목 마스터)에 남은 상장폐지 31종목을 legacy 로 옮긴다 (2026-10-12, 사용자 승인)
--
-- 왜: stocks 는 "지금 상장된 종목" 마스터로 읽힌다(monthly_listed_shares_backfill·dart_shares 의
-- 커버리지 점검이 이 테이블로 상장 종목을 센다). 그런데 upsert_stocks 는 넣기만 하고 빼지 않아,
-- 2026-06-26 ~ 10-07 에 폐지된 31종목이 그대로 남아 있었다(데이터 점검, 10-08 일봉 0행·
-- delisted_stocks 에 있음). 이름·시장은 delisted_stocks 에도 있어 잃는 정보가 없다.
-- 재발: upsert_stocks 는 여전히 빼지 않는다 — 폐지가 생길 때마다 다시 쌓인다(별도 처리 필요).
--
-- 적용: psql "$KR_QUANT_DB" -v ON_ERROR_STOP=1 -f sql/migrations/024_prune_delisted_from_stocks.sql

BEGIN;

CREATE SCHEMA IF NOT EXISTS legacy;
CREATE TABLE legacy.stocks_delisted_20261012 (LIKE public.stocks INCLUDING DEFAULTS);

CREATE TEMP TABLE gone(code TEXT) ON COMMIT DROP;
INSERT INTO gone VALUES
    ('006380'), ('008290'), ('008500'), ('012510'), ('031440'), ('032980'), ('043090'), ('046070'),
    ('057050'), ('060240'), ('082640'), ('082660'), ('084180'), ('096610'), ('101390'), ('106520'),
    ('121850'), ('196490'), ('203690'), ('222160'), ('227100'), ('230360'), ('269620'), ('299900'),
    ('451700'), ('464440'), ('465320'), ('467930'), ('469880'), ('471050'), ('472220');

-- 폐지 목록에 있고 최근 일봉이 없는 것만 — 그 사이 재상장 등으로 다시 거래되면 건드리지 않는다.
INSERT INTO legacy.stocks_delisted_20261012
SELECT s.* FROM public.stocks s JOIN gone g USING (code)
 WHERE EXISTS (SELECT 1 FROM delisted_stocks d WHERE d.code = s.code)
   AND NOT EXISTS (SELECT 1 FROM daily_bars b WHERE b.code = s.code AND b.date >= DATE '2026-10-08');

DELETE FROM public.stocks s USING legacy.stocks_delisted_20261012 l WHERE s.code = l.code;

DO $$
BEGIN
    IF (SELECT count(*) FROM legacy.stocks_delisted_20261012) <> 31 THEN
        RAISE EXCEPTION '024: 옮긴 행이 31 이 아니다 — 롤백';
    END IF;
END $$;

COMMIT;

-- 검증:
--   SELECT count(*) FROM stocks s JOIN delisted_stocks d USING (code);   -- 0
--   SELECT count(*) FROM stocks;                                         -- 2,625 (10-08 일봉 종목 수와 같다)
--
-- 롤백: INSERT INTO stocks SELECT * FROM legacy.stocks_delisted_20261012; DROP TABLE legacy.stocks_delisted_20261012;
