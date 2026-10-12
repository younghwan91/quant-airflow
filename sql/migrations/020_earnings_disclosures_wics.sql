-- earnings_disclosures·wics_membership 신설 (2026-10-12)
--
-- 왜: swing-it 결합 전략 연구(설계 구간 2020-01~2023-12)가 요청한 두 가지.
--
-- ① earnings_disclosures — 실적이 "실제로 알려진 날".
--   earnings.avail_date 는 법정 마감일 근사(분기 +45일·연간 +90일 → 05-15·08-14·11-14·03-31)라
--   실제 공시일보다 늦다. 실측 005930 2020Q1: avail 05-15, 잠정실적 공정공시 04-07, 분기보고서 05-15.
--   PEAD·실적 이벤트를 avail_date 에 두면 반응의 앞부분(잠정 발표 직후)이 통째로 빠진다.
--   earnings 행은 건드리지 않는다(knowledge_date 이력 규약 그대로) — (code, period) 로 조인하는 별 테이블.
--   kind:
--     prelim    영업(잠정)실적(공정공시)            — period 는 rcept_dt 직전 분기말
--     pl_change 매출액또는손익구조30%(15%)이상변경  — 연간 수치, period 는 rcept_dt 직전 분기말
--     periodic  사업·반기·분기보고서 (YYYY.MM)     — period 는 보고서명의 YYYY.MM
--   [기재정정]·[첨부추가] 등 괄호 접두 공시와 자회사 공시는 뺀다. 같은 (code, period, kind) 가 여럿이면
--   가장 이른 원본 하나(삼성전자는 분기마다 잠정이 두 번 — 초순 가이던스와 말일 확정). DART 는 날짜만 주고
--   잠정실적은 장 마감 뒤 제출이 많다 — 쓰는 쪽은 rcept_dt 다음 거래일부터 거래 가능으로 본다.
--   비12월 결산(보고서 월이 종류와 안 맞는 행)은 earnings 의 period 규약과 어긋나 넣지 않는다.
--
-- ② wics_membership — 종목의 WICS 섹터·산업군(중분류) 이력.
--   wiseindex GetIndexComponets 는 과거 날짜를 받고 그날 구성으로 답한다 — 오늘 분류를 과거에 덮은 게
--   아니다(실측: NC 036570 은 2019-01-02 G45 IT, 2026-10-08 G50 커뮤니케이션서비스. G5020 미디어와
--   엔터테인먼트는 2019 에 없다). 매주 첫 거래일 스냅샷. KRX 업종분류는 data.krx.co.kr 가 로그인을
--   요구해(2026-10-12 실측 "LOGOUT") 못 쓴다. WICS 지수에 없는 종목(스팩·리츠 등)은 행이 없다.
--   분류는 둘을 같이 둔다 — 어느 하나도 전 종목을 덮지 못한다(실측):
--     industry_cd(WICS 중분류 G1010..)  2019-01-02 섹터 구성 1,985 중 69 가 어느 중분류에도 없다
--                                       (미디어 61 — 2019 엔 G25 아래 중분류 지수가 없었다 — 와 금융 일부)
--     wi26_cd(WI26 WI100..WI800)        2019 은 1,985 전부를 덮지만 2026-10-08 은 2,458 중 2,368
--   섹터는 섹터 지수(G10..G55) 응답에서 받는다 — WI26 응답의 SEC_CD 는 종목이 아니라 지수의 섹터다
--   (2026 WI600 소프트웨어 전원이 G50 으로 나온다).
--
-- 적용: psql "$KR_QUANT_DB" -v ON_ERROR_STOP=1 -f sql/migrations/020_earnings_disclosures_wics.sql

BEGIN;

CREATE TABLE IF NOT EXISTS earnings_disclosures (
    code       TEXT NOT NULL,
    period     TEXT NOT NULL,   -- earnings.period 와 같은 'YYYYQn'
    kind       TEXT NOT NULL CHECK (kind IN ('prelim', 'pl_change', 'periodic')),
    rcept_dt   DATE NOT NULL,   -- DART 접수일(시각 없음)
    rcept_no   TEXT NOT NULL,
    report_nm  TEXT NOT NULL,
    PRIMARY KEY (code, period, kind)
);
CREATE INDEX IF NOT EXISTS idx_earnings_disclosures_rcept_dt ON earnings_disclosures(rcept_dt);

CREATE TABLE IF NOT EXISTS wics_membership (
    date         DATE NOT NULL,   -- 스냅샷 기준일(그 주 첫 거래일)
    code         TEXT NOT NULL,
    sector_cd    TEXT NOT NULL,   -- G10..G55
    sector_nm    TEXT,
    industry_cd  TEXT,            -- G1010..G5510 (중분류). NULL = 섹터엔 있는데 아는 산업군 어디에도 없었다
    industry_nm  TEXT,
    wi26_cd      TEXT,            -- WI100..WI800 (WI26 26개 업종). NULL = 그날 WI26 어디에도 없었다
    wi26_nm      TEXT,
    source       TEXT NOT NULL DEFAULT 'wiseindex',
    PRIMARY KEY (date, code)
);
CREATE INDEX IF NOT EXISTS idx_wics_membership_code ON wics_membership(code, date);

COMMIT;

-- 검증:
--   SELECT kind, count(*), count(DISTINCT code), min(rcept_dt), max(rcept_dt)
--     FROM earnings_disclosures GROUP BY kind;
--   SELECT e.period, e.avail_date, d.kind, d.rcept_dt FROM earnings e
--     JOIN earnings_disclosures d USING (code, period)
--    WHERE e.code = '005930' AND e.period = '2020Q1';           -- prelim 2020-04-07, periodic 2020-05-15
--   SELECT date, sector_cd FROM wics_membership WHERE code = '036570' ORDER BY date;   -- G45 → G50
--   SELECT date, count(*), count(industry_cd) FROM wics_membership GROUP BY date ORDER BY date DESC LIMIT 5;
--
-- 롤백: DROP TABLE earnings_disclosures; DROP TABLE wics_membership;  (읽는 코드는 swing-it 연구뿐)
