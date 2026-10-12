-- corporate_actions: 키움 ka10081 수정주가 이벤트(분할·병합·증자…) (2026-10-12)
--
-- 왜: daily_bars_adjusted(swing-it price_adjust)는 ±30% 점프 탐지로 백조정해 가짜 이음매가 남는다(423 종목-일 —
-- 2019-02-27 92, 2021-07-29 87 …). 원주가(daily_bars_raw, 026) × 누적 수정계수로 다시 만들려면 이벤트 날짜·비율이
-- 필요하다. 2026-10-12 키움 프로브: ka10081 행에 `upd_stkpc_tp`·`upd_rt` 가 수정·무수정 응답 모두에 실린다 —
-- 005930 2018-05-04 `upd_stkpc_tp='8'`(액면분할), `upd_rt='-98.00'`(50:1 → 그 이전 가격 × 0.02).
--
-- 무엇을 담나: 이벤트가 찍힌 행만, 원천 값 그대로(upd_stkpc_tp 문자열·upd_rt %). kind 는 수집기가 비트로 붙인
-- 이름이다(collectors/daily_bars.py EVENT_BITS — 8 외에는 아직 원주가 점프와 대조 전). 배당락(4)도 버리지 않고
-- 남긴다 — 쓰는 쪽이 고른다(swing-it 은 배당 제외).
-- 채우는 경로: daily_bars.collect(키움 일봉 백필·daily_bars_raw DAG). 매일 16:00 combined 수집은 넣지 않는다.
-- 상장 종목만(폐지 종목은 키움이 일봉을 안 준다).
--
-- 적용: psql "$KR_QUANT_DB" -v ON_ERROR_STOP=1 -f sql/migrations/027_corporate_actions.sql

CREATE TABLE IF NOT EXISTS corporate_actions (
    code         TEXT NOT NULL,
    date         DATE NOT NULL,                 -- 새 기준이 적용된 첫 거래일(이 날부터 새 가격)
    upd_stkpc_tp TEXT NOT NULL,                 -- 키움 수정주가구분(비트 합)
    upd_rt       NUMERIC,                       -- 수정비율 % — 이 날 이전 가격 × (1 + upd_rt/100)
    kind         TEXT,
    source       TEXT NOT NULL DEFAULT 'kiwoom_ka10081',
    fetched_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (code, date)
);
CREATE INDEX IF NOT EXISTS idx_corporate_actions_date ON corporate_actions(date);

-- 검증:
--   SELECT * FROM corporate_actions WHERE code = '005930';                -- 2018-05-04 | 8 | -98.00 | split
--   SELECT kind, count(*) FROM corporate_actions GROUP BY 1 ORDER BY 2 DESC;
--
-- 롤백: DROP TABLE corporate_actions;
