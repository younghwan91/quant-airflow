-- daily_bars_listed_naver: 상장 종목 네이버 수정주가 2007-11~2016-09 — v2 이벤트 추정의 기준 (2026-10-14)
--
-- 왜: 036 으로 만든 daily_bars_adj_v2_build(2007-11-22~) 합격 검사에서 2008~2016 구간에 키움이 수정 이벤트를
-- 안 준 날(대부분 거래정지 뒤 재개일의 감자·병합)이 원주가 |log| > 0.4 점프로 남았다 — 188건 중 2016-09 이후 14 를 뺀
-- ~170. 예: 003720 2011-05-04 원주가 38,200 → 4,390(v2 −88%), 네이버 수정가는 3,820 → 4,390(+14.9%).
-- 2016-09~ 는 키움 수정주가(daily_bars) ÷ 원주가 비율 계단으로 채우는데(adjusted_v2.infer_events), 그 앞에는 daily_bars 가
-- 없다. 네이버 siseJson 은 키움 수정주가와 같은 값을 준다(naver_delisted_bars 머리말) — 그 기준으로만 쓴다.
-- 수익률 원천이 아니다(가격은 daily_bars_raw × 계수). daily_bars 에 넣지 않는다 — 그 표의 2016 이전 구간을 만들지 않는다.
--
-- 적용: docker exec -i quant-airflow-timescaledb-replica-1 psql -U "$TIMESCALE_USER" -d "$TIMESCALE_DB" \
--         -v ON_ERROR_STOP=1 < sql/migrations/037_daily_bars_listed_naver.sql

CREATE TABLE IF NOT EXISTS daily_bars_listed_naver (
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
SELECT create_hypertable('daily_bars_listed_naver', 'date', if_not_exists => TRUE, chunk_time_interval => INTERVAL '1 year');

-- 검증: SELECT count(DISTINCT code), min(date), max(date) FROM daily_bars_listed_naver;
-- 롤백(사용자 확인 뒤): DROP TABLE daily_bars_listed_naver;
