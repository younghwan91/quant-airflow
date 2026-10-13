-- daily_bars_adj_v2_build 다시 만들기 — 2008 구간 포함 재계산용 (2026-10-14)
--
-- 왜: daily_bars_raw 가 2007-11-22~ 로 늘었다(상장 생존 1,157종목이 같은 날 시작, 2008~2016 306만행, 이벤트 6,264).
-- swing-it 요청대로 운영 표(daily_bars_adj_v2 — swing-it 이 읽는 중)를 제자리 갱신하지 않고 029 와 같은 형식의
-- _build 에 전량 계산 → 합격 검사 4종(87955ba, scratchpad adjv2_check) → 이름 교체(037). 옛 표는 지우지 않는다.
--
-- 적용: docker exec -i quant-airflow-timescaledb-replica-1 psql -U "$TIMESCALE_USER" -d "$TIMESCALE_DB" \
--         -v ON_ERROR_STOP=1 < sql/migrations/036_daily_bars_adj_v2_build_2008.sql

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

-- 검증: SELECT min(date), count(*) FROM daily_bars_adj_v2_build;  -- 계산 뒤 2007-11-22~
-- 롤백(계산 전·검사 실패 시, 사용자 확인 뒤): DROP TABLE daily_bars_adj_v2_build;
