-- daily_bars_adj_v2_build → daily_bars_adj_v2 (2026-10-13) — 029 의 합격 검사 4종(swing-it 87955ba) 통과
--
-- 결과(2016-09-05~2026-10-12, 5,904,126행 = 상장 2,628 + 폐지 592종목):
--   1. 카나리아: 005930 2017-03-31 raw_close 2,060,000 / close 41,200 · 2018-05-04 일간 −2.1%(50:1 분할 연속)
--      · 2026-10-08 raw_close 262,000(KRX, 일봉은 NXT 263,000).
--   2. 가짜 이음매(2019-02-27·2021-07-29·2024-01-08 에서 daily_bars_adjusted 와 10%p 넘게 다른 상장 종목-일 256):
--      v2 최대 |일간| 30.0% — 30% 넘는 것 0. 001230 2021-07-29 −16.7% → −2.1%.
--   3. |일간| > 30.1%(첫 행·거래정지 뒤 재개 제외): 상장 15종목-일(키움 수정주가도 같은 점프 — 이벤트 원천 없음),
--      폐지 539(531 은 폐지 전 마지막 10거래일 = 가격제한 없는 정리매매).
--   4. 이벤트 ↔ 키움 수정비율(daily_bars ÷ 원주가 계단): 2,400건 중 2,397 이 1% 안 일치(3건은 수집 이음매 날).
--      원주가 |log| > 0.4 인데 이벤트 없음 97 → 83 은 그 계단으로 채움(adjusted_v2.infer_events), 14 남음(3 의 목록).
--   전 구간 수익률: daily_bars_adjusted 와 0.1%p 안 일치 99.87%(상장)·99.94%(폐지).
--
-- 적용: docker exec -i quant-airflow-timescaledb-replica-1 psql -U "$TIMESCALE_USER" -d "$TIMESCALE_DB" \
--         -v ON_ERROR_STOP=1 < sql/migrations/030_daily_bars_adj_v2_rename.sql

ALTER TABLE daily_bars_adj_v2_build RENAME TO daily_bars_adj_v2;

-- 검증: SELECT count(*) FROM daily_bars_adj_v2;  -- 5,904,126
-- 롤백: ALTER TABLE daily_bars_adj_v2 RENAME TO daily_bars_adj_v2_build;
