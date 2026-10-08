-- news_articles·disclosures 의 collected_at 이 2026-09-11 이전 행에서 9시간 일찍 찍혀 있던 것을 바로잡는다 (2026-10-08)
--
-- 왜: krx-news-client 가 collected_at 을 tz 없는 datetime.now() 로 만들었고(그 레포 86cc785,
-- 2026-09-13 "뉴스/공시 시각을 tz 있는 KST 로 — 저장 시 9시간 밀리던 버그" 에서 고침), 그 naive 값이
-- 다른 시간대로 해석돼 실제보다 9시간 이른 시각으로 저장됐다.
--
-- 실측(2026-10-08, 세션 TZ=Asia/Seoul 로 본 collected_at 시:분):
--   2026-09-07 ~ 09-11  23:45 · 01:05 · 07:05   ← DAG 실행 시각 08:45 · 10:05 · 16:05 에서 정확히 −9h
--   2026-09-14 이후      08:45 · 10:05 · 16:05   ← 정상
--   news_articles 928행 중 644행이 collected_at < published_at(수집이 발행보다 먼저 — 불가능).
--   +9h 하면 그 수가 0 이 된다(collected_at + 9h < published_at 인 행 0). disclosures 2,631행도 0.
--   published_at 과 news_judgments.judged_at 은 같은 구간에서도 정상(judged_at 08·10·16시) — 안 건드린다.
--
-- 경계: 저장값 기준 2026-09-11 08:00 KST 미만. 그 구간의 마지막 런은 저장값 07:05·07:15(=실제 16:05·16:15)이고
-- 다음 정상 행은 09-14 08:45 다(09-12·13 은 행 없음) — 경계가 두 무리 사이 빈 구간에 놓인다.
--
-- 적용: psql "$KR_QUANT_DB" -v ON_ERROR_STOP=1 -f sql/migrations/014_fix_collected_at_9h.sql

BEGIN;

UPDATE news_articles
   SET collected_at = collected_at + interval '9 hours'
 WHERE collected_at < timestamptz '2026-09-11 08:00:00+09';

UPDATE disclosures
   SET collected_at = collected_at + interval '9 hours'
 WHERE collected_at < timestamptz '2026-09-11 08:00:00+09';

COMMIT;

-- 검증(적용 뒤): 두 값 모두 0 이어야 한다. 시:분 분포는 08:45·10:05·16:05 계열만 남는다.
--   SELECT count(*) FILTER (WHERE collected_at < published_at) FROM news_articles;
--   SET timezone='Asia/Seoul';
--   SELECT date(collected_at), string_agg(DISTINCT to_char(collected_at,'HH24:MI'),' ')
--     FROM news_articles WHERE collected_at < '2026-09-14' GROUP BY 1 ORDER BY 1;
--
-- 롤백(적용 직후에만 — 고친 행은 저장값 2026-09-11 16:15 KST 이하, 다음 정상 행은 09-14):
--   BEGIN;
--   UPDATE news_articles SET collected_at = collected_at - interval '9 hours'
--    WHERE collected_at < timestamptz '2026-09-12 00:00:00+09';
--   UPDATE disclosures   SET collected_at = collected_at - interval '9 hours'
--    WHERE collected_at < timestamptz '2026-09-12 00:00:00+09';
--   COMMIT;
