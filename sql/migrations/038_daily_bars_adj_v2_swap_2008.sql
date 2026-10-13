-- daily_bars_adj_v2_build(2007-11-22~) → daily_bars_adj_v2, 옛 표(2016-09-05~)는 daily_bars_adj_v2_2016 으로 보관 (2026-10-14)
--
-- 036 _build 를 037(네이버 기준 이벤트 추정)·adbdb2d(상장 전 장외 시세 pre_listing) 반영해 다시 계산한 결과:
--   10,015,495행 = 상장 2,628 + 폐지 1,166종목, 2007-11-22~2026-10-13. 추정 이벤트 159(2016-09 이전 76 새로).
--   검사 4종(같은 스크립트를 옛 운영 표에도 돌려 대조):
--   1. 카나리아 그대로(005930 2017-03-31 2,060,000/41,200, 2018-05-04 −2.1%, 2026-10-08 262,000).
--   2. daily_bars_adjusted 와 10%p 넘게 다른 종목-일 754(옛 표 739 — 차이 15 는 새로 들어온 10-13), v2 |r|>30% 137 동일.
--   3. |일간|>30.1%(첫 행·거래정지 뒤 재개 제외) 상장 23(옛 표 18 + 10-13 1 + 2016 이전 4: 093190 2011-05-12 −30.1%,
--      094360 2015-08-05 +31.2%, 142760 2016-07-07 −30.6%, 180400 2015-11-20 −42.9%). 폐지 1,427 중 1,346 이 정리매매.
--      폐지 정리매매 밖 81 은 거의 2016 이전 — 네이버 수정가가 이벤트를 안 반영한 날(원주가가 없어 고칠 수 없다).
--   거래정지 뒤 재개일 |log|>0.4 는 2016 이전 50(재편·재상장 — 어느 원천도 이어 붙이지 않는다) → 읽는 쪽은 정지 뒤 첫날 수익률을 버릴 것.
--   pre_listing 2,606행(8종목) = KRX 상장 전 K-OTC·프리보드 시세, OHLCV NULL.
--
-- lock_timeout: 읽는 쿼리가 잡고 있으면 기다리지 않고 실패한다(2026-10-14 잠금 줄서기 장애 교훈) — 다시 실행.
-- 적용: docker exec -i quant-airflow-timescaledb-replica-1 psql -U "$TIMESCALE_USER" -d "$TIMESCALE_DB" \
--         -v ON_ERROR_STOP=1 < sql/migrations/038_daily_bars_adj_v2_swap_2008.sql

BEGIN;
SET LOCAL lock_timeout = '10s';
ALTER TABLE daily_bars_adj_v2 RENAME TO daily_bars_adj_v2_2016;
ALTER TABLE daily_bars_adj_v2_build RENAME TO daily_bars_adj_v2;
COMMIT;

-- 검증: SELECT min(date), count(*) FROM daily_bars_adj_v2;       -- 2007-11-22, 10,015,495
--       SELECT min(date), count(*) FROM daily_bars_adj_v2_2016;  -- 2016-09-05, 5,904,126
-- 롤백: BEGIN; ALTER TABLE daily_bars_adj_v2 RENAME TO daily_bars_adj_v2_build;
--       ALTER TABLE daily_bars_adj_v2_2016 RENAME TO daily_bars_adj_v2; COMMIT;
