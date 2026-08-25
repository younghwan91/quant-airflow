-- 압축 정책 교정 + shares_outstanding_history 의 lookahead 행 제거 (2026-08-25)
--
-- 라이브 DB 실측이 세 가지를 드러냈다.
--
-- ① **압축이 8개 테이블 중 7개에서 손해였다.**
--
--      테이블                       압축 전 → 압축 후
--      daily_bars                   686 MB → 611 MB   (-11%, 유일한 이득)
--      supply_demand                388 MB → 545 MB   (+40%)
--      short_selling                120 MB → 134 MB   (+12%)
--      daily_bars_adjusted          739 MB → 901 MB   (+22%)
--      shares_outstanding_history  3368 kB → 4448 kB  (+32%)
--      sector_index                  12 MB →  13 MB
--      consensus                    696 kB → 1184 kB
--      credit_balance                44 MB →  44 MB   (±0)
--
--    원인은 청크 크기다. `init_timescale.sql` 은 `chunk_time_interval => 1 year`
--    를 요구하는데, 테이블이 기본값(7일)으로 먼저 만들어져 `create_hypertable(...,
--    if_not_exists => TRUE)` 가 no-op 이 됐다. 나중에 인터벌을 바꿔도 **기존
--    범위에는 소급되지 않는다** — 그래서 2016~2026 전 구간이 7일 청크 514개다.
--    `compress_segmentby='code'` 인데 7일 청크에는 종목당 5행뿐이라 압축할 런이
--    없고, 세그먼트 오버헤드가 이득을 잡아먹는다.
--
--    현재 `dimensions.time_interval` 은 360일이므로 **앞으로 생기는 청크는
--    정상이다.** 이 마이그레이션은 그 위에서 정책을 맞춘다.
--
-- ② **압축 경계(7일)가 수집기가 쓰는 창 안에 있었다.** 일봉·수급 15일,
--    공매도·신용 10일을 매일 upsert 하는데 7일 넘은 부분은 이미 압축돼 있어,
--    쓰기마다 세그먼트 압축해제→갱신→재압축이 돌았다. pg_stat 에 그 흔적이
--    그대로다(압축 청크에서 n_tup_ins ≈ n_tup_del, n_live_tup = 0).
--
-- ③ **`shares_outstanding_history` 에 진짜 point-in-time 데이터가 없었다.**
--    2017-01-01 · 2024-01-08 · 2026-01-01 세 날짜(전부 휴장일)에 각각 2,628행이
--    있었고 전부 `source='kiwoom'` — 오늘 스냅샷을 과거로 복사한 것이다. 삼성전자
--    (005930)가 2017-01-01 에 5,846,279,000주로 적혀 있었는데, 2018년 50:1 분할
--    전이므로 실제로는 그 1/50 이다. `market_cap_asof` 가 2017~2026 구간에서
--    오늘의 주식수를 읽는 lookahead 가 살아 있었다.
--
--    진짜 point-in-time 은 `source='dart'` 행(2016-12-31 이후 연말 기준일, 폐지
--    종목 418개)뿐이다. 가짜 행을 지우면 그 구간의 상장 종목은 시총이 **없다** —
--    틀린 값보다 없는 값이 낫다. 앞으로는 weekly_listed_shares 가 2026-07-09
--    이후를 주 단위로 쌓는다.
--
-- 적용:
--   psql "$DB_URL" -v ON_ERROR_STOP=1 -f sql/migrations/008_compression_and_lookahead_cleanup.sql

-- ── ① daily_bars_adjusted: 압축 해제 + 영구 비활성 ────────────────────────
--
-- init_timescale.sql 은 처음부터 "이 테이블은 압축하지 말라"고 적어놨는데(주간
-- 전량 재작성이라 압축해제→재압축 순환만 돈다) 라이브 DB 는 512/515 청크가
-- 압축돼 있었다. 정책 없이 수동 압축된 것으로 보인다.
SELECT decompress_chunk(c, if_compressed => true) FROM show_chunks('daily_bars_adjusted') c;
ALTER TABLE daily_bars_adjusted SET (timescaledb.compress = false);

-- ── ② 압축 경계를 쓰는 창 밖으로 ──────────────────────────────────────────
DO $$
DECLARE t text;
BEGIN
  FOREACH t IN ARRAY ARRAY[
      'daily_bars', 'supply_demand', 'short_selling', 'credit_balance',
      'shares_outstanding_history', 'sector_index', 'consensus'
  ] LOOP
    PERFORM remove_compression_policy(t, if_exists => true);
    PERFORM add_compression_policy(t, INTERVAL '30 days');
  END LOOP;
END $$;

-- ── ③ 소급 복사된 상장주식수 스냅샷 제거 ──────────────────────────────────
--
-- source='kiwoom' 로 한정한다 — 같은 날짜에 DART 발 진짜 행이 있으면 남긴다.
BEGIN;
DELETE FROM shares_outstanding_history
 WHERE date IN (DATE '2017-01-01', DATE '2024-01-08', DATE '2026-01-01')
   AND source = 'kiwoom';
COMMIT;

-- 검증:
--   SELECT source, min(date), max(date), count(*) FROM shares_outstanding_history
--    GROUP BY 1;          -- kiwoom 의 min 이 2026-07-09 여야 한다
--   SELECT hypertable_name, config->>'compress_after' FROM timescaledb_information.jobs
--    WHERE proc_name='policy_compression';   -- 전부 30 days
--   SELECT count(*) FILTER (WHERE is_compressed) FROM timescaledb_information.chunks
--    WHERE hypertable_name='daily_bars_adjusted';   -- 0
--
-- 롤백: 압축 정책은 add_compression_policy(t, INTERVAL '7 days') 로 되돌린다.
-- 삭제한 행은 되살릴 이유가 없다(값 자체가 틀렸다).
