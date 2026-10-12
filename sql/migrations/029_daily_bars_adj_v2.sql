-- daily_bars_adj_v2_build: 원주가 × 수정 이벤트 누적계수 + 2026-09-14~ KRX 분봉 OHLC (2026-10-13)
--
-- 왜: daily_bars_adjusted 는 수정주가 daily_bars 를 ±30% 점프 탐지로 이어 붙여 수집 시점 이음매가 남고
-- (swing-it 실측 가짜 이음매 423개), 2026-09-14 부터 키움 일봉 종가는 KRX 정규장 종가가 아니라 KRX 애프터마켓
-- (16:00~20:00, 2026-09-14~; NXT 아님) 20:00 마지막가다(종목-일 86%, 평균 0.7%; 005930 2026-10-08 263,000 vs KRX 262,000). 사용자가 2026-10-12 swing-it 경유로
-- 재구성 ①안(원주가 × corporate_actions 계수, 기존 표는 그대로)을 승인했다. 계산은 collectors/adjusted_v2.py.
--
-- 이름: 이 표는 **_build** 다. swing-it 사전등록 87955ba 의 합격 검사 4종(카나리아 · 가짜 이음매 423 ≤30% ·
-- |r|>30% 목록 · 이벤트↔점프 불일치)을 통과한 뒤에만 daily_bars_adj_v2 로 이름을 바꾼다(그건 다음 마이그레이션).
--
-- 컬럼:
--   open~close  수정가격(최신 기준). 2026-09-14~ 는 분봉 09:00~15:32, 분봉이 없거나 기준이 다르면 NULL.
--   volume      수정 거래량(원 거래량 ÷ factor). trade_value 는 백만원(daily_bars 와 같은 단위, 조정 안 함).
--   raw_close   그날 실제 KRX 종가(시총 = raw_close × 그날 주식수). 폐지 종목(네이버 수정가격)은 NULL.
--   factor      그날 원주가에 곱한 계수. 폐지 종목은 NULL.
--   close_src   kiwoom_raw | minute_krx | halted | naver_adj | no_krx_source | minute_basis_mismatch
--   source      kiwoom_raw | naver_adj
--
-- 압축하지 않는다 — 이벤트가 새로 찍히면 그 종목 전체 이력을 다시 쓴다(daily_bars_adjusted 와 같은 이유, 008).
--
-- 적용: docker exec -i quant-airflow-timescaledb-replica-1 psql -U "$TIMESCALE_USER" -d "$TIMESCALE_DB" \
--         -v ON_ERROR_STOP=1 < sql/migrations/029_daily_bars_adj_v2.sql

CREATE TABLE IF NOT EXISTS daily_bars_adj_v2_build (
    code        TEXT NOT NULL,
    date        DATE NOT NULL,
    open        DOUBLE PRECISION,
    high        DOUBLE PRECISION,
    low         DOUBLE PRECISION,
    close       DOUBLE PRECISION,
    volume      BIGINT,
    trade_value BIGINT,
    raw_close   BIGINT,
    factor      DOUBLE PRECISION,
    close_src   TEXT NOT NULL,
    source      TEXT NOT NULL,
    PRIMARY KEY (code, date)
);
SELECT create_hypertable('daily_bars_adj_v2_build', 'date', if_not_exists => TRUE,
                         chunk_time_interval => INTERVAL '1 year');

-- 검증:
--   SELECT close_src, count(*) FROM daily_bars_adj_v2_build GROUP BY 1;
--   SELECT raw_close, close FROM daily_bars_adj_v2_build WHERE code = '005930' AND date = '2017-03-31';  -- 2,060,000 / 41,200
--   SELECT raw_close FROM daily_bars_adj_v2_build WHERE code = '005930' AND date = '2026-10-08';          -- 262,000(KRX)
--
-- 롤백: DROP TABLE daily_bars_adj_v2_build;
