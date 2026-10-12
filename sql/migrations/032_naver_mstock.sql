-- naver_investor_trend · naver_research_reports: 네이버 m.stock JSON 의 2008~ 일별 외국인 보유율·투자자 순매수와
-- 증권사 리포트 목표가 (2026-10-13, swing-it 요청 — 사용자 "무료인 건 받아줘", "데이터가 없으면 만들어서라도")
--
-- 왜: 2008~2016 독립 표본에서 수급 신호를 시험할 자료가 없다. 키움 ka10008(외국인 보유)은 최근 50일, ka10059(투자자
-- 순매수)는 폐지 코드에 빈 응답이고, 옛 네이버 frgn.naver 는 2026 개편으로 표가 사라졌다(10-13 실측 행 0).
-- m.stock JSON 은 로그인 없이 2008·폐지 종목까지 준다(005930 2007-10-12 보유율 48.62%, 003450 2016-10-28 10.63%).
-- 기존 표(supply_demand 등)는 건드리지 않는다. 수집: collectors/naver_mstock.py.
--
-- naver_investor_trend: 종가·거래량은 **원 가격**. foreign_hold_ratio 는 % (소수 2자리). 순매수는 수량(주) — 네이버
--   정의(개인+외국인+기관+기타 합 0 으로 맞춤)라 키움 supply_demand.foreign_ 과 다르다. 옛 구간 개인 '-' → NULL.
-- naver_research_reports: 리포트 1건 1행. write_date 는 **날짜만**(게시 시각 없음 — as-of 는 다음 거래일로).
--   goal_price 0/NULL = 목표가 없음(의견만 낸 리포트). prev_goal_price 는 원천 값 그대로(검증 안 됨).
--
-- 적용: docker exec -i quant-airflow-timescaledb-replica-1 psql -U "$TIMESCALE_USER" -d "$TIMESCALE_DB" \
--         -v ON_ERROR_STOP=1 < sql/migrations/032_naver_mstock.sql

CREATE TABLE IF NOT EXISTS naver_investor_trend (
    code               TEXT NOT NULL,
    date               DATE NOT NULL,
    close              BIGINT,
    volume             BIGINT,
    foreign_hold_ratio NUMERIC,
    foreign_net        BIGINT,
    institution_net    BIGINT,
    individual_net     BIGINT,
    source             TEXT NOT NULL DEFAULT 'naver_mstock',
    fetched_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (code, date)
);
SELECT create_hypertable('naver_investor_trend', 'date', if_not_exists => TRUE, chunk_time_interval => INTERVAL '1 year');

CREATE TABLE IF NOT EXISTS naver_research_reports (
    research_id     BIGINT PRIMARY KEY,
    code            TEXT,
    item_name       TEXT,
    broker          TEXT,
    write_date      DATE,
    title           TEXT,
    category        TEXT,
    opinion         TEXT,
    goal_price      BIGINT,
    prev_goal_price BIGINT,
    price_at_write  BIGINT,
    attach_url      TEXT,
    source          TEXT NOT NULL DEFAULT 'naver_research',
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_naver_research_code_date ON naver_research_reports(code, write_date);

-- 검증:
--   SELECT count(DISTINCT code), min(date), max(date) FROM naver_investor_trend;
--   SELECT foreign_hold_ratio FROM naver_investor_trend WHERE code = '005930' AND date = '2016-12-29';   -- 50.76
--   SELECT count(*), min(write_date) FROM naver_research_reports;
--
-- 롤백: DROP TABLE naver_investor_trend; DROP TABLE naver_research_reports;
