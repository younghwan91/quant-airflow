-- news_articles 의 같은 id 두 행(토스가 고친 기사) 4건 — 먼저 본 판만 남긴다 (2026-10-09)
--
-- 왜: PK 가 (id, published_at) 라, 토스가 기사를 고쳐 createdAt 이 바뀌면 daily_news 의 다음 수집이
-- 같은 id 를 **새 행**으로 넣는다. 실측 4 id(2026-09-10 ~ 10-08, 전부 daily_news 가 넣은 행):
--   toss:5a97ecfe560e  09-10 10:21 → 18:08  제목도 고침("미코, …품었다" → "새 먹거리 찾는 미코, …")
--   toss:888892100dab  10-02 13:53 → 10-04 07:40  연재 ⑥ → ⑦ 로 내용이 바뀐 같은 id
--   toss:9c52885acce7  10-08 08:06 → 08:13
--   toss:cdaa89790c76  09-30 09:43:59 → 09:44:08
-- 015 의 종목별 수집기는 처음부터 "이미 있는 id 는 건너뛴다"(나중 판은 lookahead)로 막았고, 같은 날
-- news_toss.py 도 그 규칙으로 고쳤다 — 이 파일은 이미 생긴 4건만 정리한다.
--
-- 남기는 쪽 = id 별 가장 이른 published_at. news_judgments 는 source_id(=id)로만 걸려 있어 영향 없다
-- (9c52885acce7 에 판단 1건 — id 가 그대로라 계속 이어진다).
--
-- 적용: psql "$KR_QUANT_DB" -v ON_ERROR_STOP=1 -f sql/migrations/017_dedupe_news_articles_edited.sql

BEGIN;

DELETE FROM news_articles a
 USING (SELECT id, min(published_at) AS first_at
          FROM news_articles GROUP BY id HAVING count(*) > 1) d
 WHERE a.id = d.id AND a.published_at > d.first_at;

COMMIT;

-- 검증(0 이어야 한다):
--   SELECT count(*) FROM (SELECT id FROM news_articles GROUP BY id HAVING count(*) > 1) x;
--
-- 롤백: 지운 4행은 2026-10-08 19:00 core 덤프(드라이브 quant-airflow-backup/core/core-2026-10-08.sql.gz)에 있다 —
--   그 덤프의 news_articles COPY 블록에서 위 4 id 의 나중 published_at 행을 골라 다시 넣는다.
