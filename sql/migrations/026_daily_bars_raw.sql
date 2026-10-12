-- daily_bars_raw: 키움 **무수정 주가**(upd_stkpc_tp=0) 일봉 (2026-10-12, 사용자 승인)
--
-- 왜: daily_bars 는 "원 일봉" 이 아니다. 수집기가 ka10081 을 수정주가(upd_stkpc_tp=1)로 받아, 수집 시점보다
-- 앞선 병합·분할만큼 과거 가격이 소급 조정돼 있다(2016~ 초기 백필 전체 + 그 뒤 15일 재수집 창 —
-- 메모리 price-tables-adjustment-seams). swing-it 실측: 005930 2017-03-31 daily_bars 41,200 vs 실제 2,060,000,
-- 900300 2024-05-02 50,500(이후 병합 기준). 그 시점 실제 주식수(shares_outstanding_history)를 곱하면 시총이
-- 수 배로 부풀어 시총·거래대금 유니버스가 틀어진다. 월말 표본의 3~14% 종목-월이 1.5배 넘게 어긋났다.
--
-- daily_bars 를 덮지 않고(§1 — 그 테이블을 읽는 레포 전부가 영향) 새 테이블에 따로 받는다:
--   - 상장 종목 + 지정 ETF(278530·069500·229200), 2016-09-09~. 폐지 종목은 키움이 안 줘서 없다(생존편향 주의 —
--     폐지분의 원 종가는 supply_demand.close 가 대안).
--   - 매일 평일 19:00 daily_bars_raw DAG 가 최근 며칠을 다시 받는다(16:00 값은 잠정치 — daily_finalize 참고).
--   - 시총 = daily_bars_raw.close × 그날 주식수. 연속 수익률은 여전히 daily_bars_adjusted.
--
-- 적용: psql "$KR_QUANT_DB" -v ON_ERROR_STOP=1 -f sql/migrations/026_daily_bars_raw.sql

CREATE TABLE IF NOT EXISTS daily_bars_raw (
    code        TEXT NOT NULL,
    date        DATE NOT NULL,
    open        BIGINT,
    high        BIGINT,
    low         BIGINT,
    close       BIGINT,
    volume      BIGINT,
    trade_value BIGINT,
    source      TEXT NOT NULL DEFAULT 'kiwoom_raw',
    PRIMARY KEY (code, date)
);
SELECT create_hypertable('daily_bars_raw', 'date', if_not_exists => TRUE, chunk_time_interval => INTERVAL '1 year');
ALTER TABLE daily_bars_raw SET (timescaledb.compress, timescaledb.compress_segmentby = 'code');
SELECT add_compression_policy('daily_bars_raw', INTERVAL '30 days', if_not_exists => TRUE);

-- 검증:
--   SELECT count(DISTINCT code), min(date), max(date) FROM daily_bars_raw;
--   SELECT date, close FROM daily_bars_raw WHERE code = '005930' AND date = '2017-03-31';   -- ~2,060,000 (액면분할 전)
--   SELECT r.close, a.close FROM daily_bars_raw r JOIN daily_bars a USING (code, date)
--    WHERE code = '900300' AND date = '2024-05-02';                                          -- 원주가 ≠ 50,500
--
-- 롤백: DROP TABLE daily_bars_raw;
