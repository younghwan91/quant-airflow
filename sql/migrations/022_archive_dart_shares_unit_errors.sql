-- shares_outstanding_history 의 DART 단위 오류 31행을 legacy 로 옮긴다 (2026-10-12, 사용자 승인)
--
-- 왜: swing-it 이 시총 계산이 깨지는 행을 보고했다(082640 2019 = 161,358,585,000,000주 등).
-- 우리 파싱이 아니라 **DART 원천**이 그렇다 — 회사가 사업보고서 "주식의 총수 현황"을 천 주·백만 주
-- 단위로 잘못 적어 냈고 stockTotqySttus API 가 그 숫자를 그대로 준다(2026-10-12 원본 재조회:
-- 082640 2019 istc_totqy "161,358,585,000,000", 063170 2023 "17,774,267,000" — 2022 는 "17,774,267").
--
-- 고르는 기준(적용 직전 실측): source='dart' 이고, 값이 그 종목 전체 행 중앙값의 500배 이상이며
-- 1,000 의 배수. 27종목 31행, 비율은 거의 정확히 ×1,000 또는 ×1,000,000 이다. 키움 행에는 없다.
-- 1e10 주 초과만 거르면 4행(140910·003240·005820·038070 — ×1,000 인데 수십억 주)이 빠진다.
--
-- 나눠서 "고치지" 않는다 — 원래 값이 거의 확실해도 원천과 다른 값을 만드는 셈이고, 틀린 값보다 없는 값이
-- 낫다(CLAUDE.md §3). 그 해는 행이 없어지고 as-of 조회는 앞 행(또는 키움 행)을 쓴다.
-- 재발은 collectors/dart_shares.py 의 가드가 막는다(같은 커밋).
--
-- 적용: psql "$KR_QUANT_DB" -v ON_ERROR_STOP=1 -f sql/migrations/022_archive_dart_shares_unit_errors.sql

BEGIN;

CREATE SCHEMA IF NOT EXISTS legacy;
CREATE TABLE legacy.shares_outstanding_dart_unit_errors (LIKE public.shares_outstanding_history INCLUDING DEFAULTS);

CREATE TEMP TABLE bad(code TEXT, date DATE, shares BIGINT) ON COMMIT DROP;
INSERT INTO bad VALUES
    ('003240', DATE '2020-12-31', 1113400000),
    ('005030', DATE '2020-12-31', 56849517000000),
    ('005820', DATE '2020-12-31', 2200000000),
    ('015260', DATE '2016-12-31', 48389938000000),
    ('018620', DATE '2018-12-31', 16217013000),
    ('023960', DATE '2022-12-31', 31249355000),
    ('031330', DATE '2017-12-31', 99995067000),
    ('038070', DATE '2025-12-31', 9100676000),
    ('046310', DATE '2023-12-31', 16418641000),
    ('054090', DATE '2022-12-31', 24959232000),
    ('054090', DATE '2023-12-31', 24959232000),
    ('063170', DATE '2023-12-31', 17774267000),
    ('068330', DATE '2020-12-31', 44216140000000),
    ('073240', DATE '2017-12-31', 157993158000),
    ('082640', DATE '2019-12-31', 161358585000000),
    ('091090', DATE '2018-12-31', 60990792000),
    ('092460', DATE '2017-12-31', 11459502000),
    ('092730', DATE '2025-12-31', 16027989000),
    ('117730', DATE '2020-12-31', 11853448000),
    ('119850', DATE '2021-12-31', 16448909000),
    ('130740', DATE '2018-12-31', 9450000000000),
    ('140910', DATE '2026-06-30', 4463032000),
    ('141020', DATE '2017-12-31', 43559591000),
    ('214310', DATE '2022-12-31', 73582603000000),
    ('214310', DATE '2023-12-31', 73582603000000),
    ('214310', DATE '2024-12-31', 3679130000000),
    ('229640', DATE '2025-12-31', 30624879000000),
    ('330350', DATE '2022-12-31', 13192778000),
    ('333430', DATE '2025-12-31', 30726747000),
    ('462520', DATE '2024-12-31', 11855168000),
    ('462520', DATE '2025-12-31', 11855168000);

-- 값까지 맞는 행만 — 그 사이 다시 수집돼 바뀐 행은 건드리지 않는다.
INSERT INTO legacy.shares_outstanding_dart_unit_errors
SELECT s.* FROM public.shares_outstanding_history s
JOIN bad b ON b.code = s.code AND b.date = s.date AND b.shares = s.shares_outstanding AND s.source = 'dart';

DELETE FROM public.shares_outstanding_history s USING bad b
 WHERE b.code = s.code AND b.date = s.date AND b.shares = s.shares_outstanding AND s.source = 'dart';

DO $$
BEGIN
    IF (SELECT count(*) FROM legacy.shares_outstanding_dart_unit_errors) <> 31 THEN
        RAISE EXCEPTION '022: 옮긴 행이 31 이 아니다 — 롤백';
    END IF;
END $$;

COMMIT;

-- 검증:
--   SELECT count(*) FROM legacy.shares_outstanding_dart_unit_errors;                    -- 31
--   SELECT count(*) FROM shares_outstanding_history WHERE shares_outstanding > 1e10;    -- 0 (삼성전자 ~5.97e9 가 최대)
--   SELECT * FROM shares_outstanding_history WHERE code = '082640' ORDER BY date;       -- 2019-12-31 행 없음
--
-- 롤백:
--   INSERT INTO shares_outstanding_history SELECT * FROM legacy.shares_outstanding_dart_unit_errors;
--   DROP TABLE legacy.shares_outstanding_dart_unit_errors;
