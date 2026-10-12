-- daily_bars_delisted_naver · kind_delistings: 2006~ 폐지 종목 일봉 + KIND 상장폐지 목록 (2026-10-12, 사용자 승인)
--
-- 왜: 2006~2016 이력 확장(swing-it 요청 — 2017~23 설계 구간 소모, 독립 표본)은 폐지 종목이 없으면 생존편향이라
-- 쓸 수 없다. 키움은 폐지 코드에 빈 행 하나만 준다(2026-08-13 실측). 네이버 siseJson 은 2005 부터 폐지 전날까지
-- 준다(표본 5/5: 019640 2009 폐지 1,067일 … 032420 2015 폐지 2,499일).
--
-- daily_bars 에 넣지 않는 이유: 상장 종목의 2006~2016 원주가는 daily_bars_raw 로 간다 — daily_bars 의 2016 이전이
-- "폐지 종목만" 인 유니버스가 되어 그 구간을 읽는 쪽이 생존편향을 거꾸로 뒤집어쓴다.
-- 값은 **수정가격**(네이버 = 키움 수정주가)이고 폐지 종목은 수정 이벤트가 없어 원 가격으로 못 돌린다 → source='naver_adj'.
-- trade_value 는 close×volume/1e6 근사(백만원, 중앙값 오차 0.7%).
--
-- kind_delistings: KIND 상장폐지 현황(무인증) 원문 — 폐지일·사유. 2006~2008 이 해마다 16~35건뿐이라 완전하지
-- 않아 보인다(1999~2005 는 60~127건). 대상 선정은 KIND ∪ delisted_stocks(KRX finder) − 지금 상장 코드.
-- 같은 코드가 두 번 폐지될 수 있어(재상장·코드 재사용) PK 에 날짜를 넣는다.
--
-- 적용: psql "$KR_QUANT_DB" -v ON_ERROR_STOP=1 -f sql/migrations/028_delisted_history.sql

CREATE TABLE IF NOT EXISTS daily_bars_delisted_naver (
    code        TEXT NOT NULL,
    date        DATE NOT NULL,
    open        BIGINT,
    high        BIGINT,
    low         BIGINT,
    close       BIGINT,
    volume      BIGINT,
    trade_value BIGINT,
    source      TEXT NOT NULL DEFAULT 'naver_adj',
    PRIMARY KEY (code, date)
);
SELECT create_hypertable('daily_bars_delisted_naver', 'date', if_not_exists => TRUE, chunk_time_interval => INTERVAL '1 year');

CREATE TABLE IF NOT EXISTS kind_delistings (
    code        TEXT NOT NULL,
    delist_date DATE NOT NULL,
    name        TEXT,
    reason      TEXT,
    PRIMARY KEY (code, delist_date)
);

-- 검증:
--   SELECT count(*), min(delist_date), max(delist_date) FROM kind_delistings;            -- ~2,000, 1991~
--   SELECT count(DISTINCT code), min(date), max(date) FROM daily_bars_delisted_naver;
--   SELECT * FROM daily_bars_delisted_naver WHERE code = '032420' ORDER BY date DESC LIMIT 1;   -- 2015-01-26
--
-- 롤백: DROP TABLE daily_bars_delisted_naver; DROP TABLE kind_delistings;
