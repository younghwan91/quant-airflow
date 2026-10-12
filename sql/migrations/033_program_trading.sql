-- program_trading: 키움 ka90013 종목별 일별 프로그램 매매 (2026-10-13, swing-it 요청 — 2016-09~ 과거 백필)
--
-- 왜: 프로그램 매매(지수차익·바스켓)는 대형주 수급의 큰 몫인데 DB 에 없었다. ka90013 은 기준일(date)부터 20거래일씩
-- 과거로 준다(005930 2016-12-29 확인). 응답은 매수·매도·순매수 **합계**뿐이다 — 차익·비차익 분리는 이 TR 에 없다.
-- 금액은 백만원(005930 2016-12-29 매수 48,155주 ≈ 86,484백만원), 수량은 주.
-- **상장 생존 종목만** — 키움은 폐지 코드에 빈 응답을 준다. 수집: collectors/flow_history.py --what program.
--
-- 적용: docker exec -i quant-airflow-timescaledb-replica-1 psql -U "$TIMESCALE_USER" -d "$TIMESCALE_DB" \
--         -v ON_ERROR_STOP=1 < sql/migrations/033_program_trading.sql

CREATE TABLE IF NOT EXISTS program_trading (
    code         TEXT NOT NULL,
    date         DATE NOT NULL,
    close        BIGINT,
    volume       BIGINT,
    prm_sell_amt BIGINT,
    prm_buy_amt  BIGINT,
    prm_net_amt  BIGINT,
    prm_sell_qty BIGINT,
    prm_buy_qty  BIGINT,
    prm_net_qty  BIGINT,
    stex_tp      TEXT,
    source       TEXT NOT NULL DEFAULT 'kiwoom_ka90013',
    PRIMARY KEY (code, date)
);
SELECT create_hypertable('program_trading', 'date', if_not_exists => TRUE, chunk_time_interval => INTERVAL '1 year');

-- 검증:
--   SELECT count(DISTINCT code), min(date), max(date) FROM program_trading;
--   SELECT prm_buy_amt, prm_buy_qty FROM program_trading WHERE code = '005930' AND date = '2016-12-29';  -- 86484 / 48155
--
-- 롤백: DROP TABLE program_trading;
