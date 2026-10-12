-- regular_close 뷰 + KRX 호가단위 함수: 정규장 종가를 기준가 항등식으로 파생 (2026-10-13, 사용자 승인 "항등식 파생부터")
--
-- 왜: 2026-09-14 KRX 애프터마켓(16:00~20:00) 개장 뒤 daily_bars 3종·supply_demand.close 의 종가는 애프터마켓 포함
-- 통합값이다(종목-일 86% 가 정규장 종가와 다르다, 99793d2·docs/schema.md 애프터마켓 절). 정규장 종가를 얻는 길은
-- 분봉 15:30~15:32 마지막 봉(scalp-it 소관, 과거분은 뒤의 분할·병합이 소급된 수정주가)과 기준가 항등식뿐이다:
--
--     정규장 종가(t) = 틱반올림(supply_demand.close(t+1) / (1 + flu_rt(t+1)/1e4))
--
-- flu_rt 의 기준가는 애프터마켓 뒤에도 전일 정규장 종가이고, 키움 flu_rt 는 (close/기준가 − 1)×1e4 의 반올림이다
-- (2025-01~2026-09-11 1,056,997쌍 중 반올림 일치 1,056,152 = 99.92%, 버림은 54.5% — 나머지는 기준가 재설정·스팩).
--
-- 무엇을 만드나:
--   krx_tick_size(price, d)  d 시점 그 가격대의 **가장 굵은** 호가단위. 2023-01-25 개편 뒤는 코스피·코스닥 공통
--                            (1·5·10·50·100·500·1,000원, 경계 2천·5천·2만·5만·20만·50만), 그 전은 코스피 표
--                            (경계 1천·5천·1만·5만·10만·50만). 코스닥 개편 전 5만원 이상(100원)과 스팩(2025-03~ 1원
--                            단위로 거래 — 실측 82종목)은 표보다 가늘다 → krx_base_close 의 격자 폴백이 잡는다.
--   krx_tick_round(price, d) 위 단위로 반올림.
--   krx_base_close(close, flu_rt, d)  t+1 수급 한 행 → t(=d) 의 기준가. 원값을 표 단위부터 1원까지 더 가는 격자로
--                            반올림해 보며 flu_rt 를 ±0.5bp 안에서 재현하는 첫 값. 진짜 기준가가 굵은 격자 위에
--                            있으면 그 반올림이 곧 진짜 값이고, 아니면 굵은 후보가 2bp 넘게 어긋나 flu_rt 를 못 맞춘다
--                            — 그래서 시장 구분(stocks.market 은 현재 스냅샷이다) 없이 맞는다. 1원 격자로도 못 맞추거나
--                            이웃 격자값도 맞으면(flu_rt ≈ −100% 처럼 bp 정밀도가 호가단위보다 굵을 때) NULL.
--   regular_close(code, date, close, src)  뷰.
--
-- 범위 선택 — 전 기간, 단 2026-09-14 전은 supply_demand.close 그대로(src='sd_close'):
--   그 전에는 close 자체가 정규장 종가다(시간외 단일가는 일봉 종가를 바꾸지 않았다). 항등식은 기준가 재설정일
--   (분할·병합·권리락)에 수정 기준가를 내놓아 close 보다 나쁠 뿐이고, 그 구간 항등식은 검증용으로만 쓴다
--   (close 와 대조 99.97~99.995% — docs/schema.md regular_close 절).
--   한 뷰가 09-14 를 가로질러 같은 뜻의 열을 주면 소비자가 날짜로 분기하지 않아도 된다. 키움 행(source LIKE 'kiwoom%')만
--   — 네이버 폐지 부분 백필(source='naver')은 flu_rt 가 없고 close 정의도 대조 전이다.
--
-- 09-14~ src(close 가 NULL 인 이유를 같이 준다 — 틀린 값보다 없는 값):
--   identity    항등식 값
--   pending     t 가 supply_demand 마지막 거래일 — t+1 수급이 들어오면(T+1 16:00 잠정 수집) 채워진다
--   no_next     그 종목의 다음 행이 없다(폐지 직전 마지막 날 등)
--   gap         그 종목의 다음 행이 거래일 달력의 다음 날이 아니다(수집 누락·정지 중 행 없음 — 기준가가 어느 날 것인지 모른다)
--   base_reset  t+1 에 corporate_actions 이벤트(upd_rt ≠ 0, 병합·분할·권리락 등) — flu_rt 기준가가 수정 기준가다
--   unmatched   krx_base_close 가 NULL — 1원 격자로도 flu_rt 를 못 맞추거나(corporate_actions 에 없는 기준가 재설정) 유일하지 않다
--   거래일 달력 = 09-14~ supply_demand 키움 행의 날짜.
--
-- 일반 뷰 vs 머티리얼라이즈드 뷰 — 일반 뷰를 고른다:
--   date 조건은 lead() 창 함수 아래로 내려가지 않아(결과가 바뀌므로) 09-14~ 전체를 매번 창 계산한다(code 조건은
--   PARTITION BY 열이라 내려간다). 실측 2026-10-13(09-14~10-12, 44,614행):
--     운영 DB 같은 모양 질의(읽기 전용, krx_base_close 만 인라인 반올림으로 대신) — 하루 전 종목 0.06~0.08s,
--       09-14~ 전부 0.10s, 한 종목 전 기간 0.08s(첫 실행 0.58s), 09-14 전 한 달 0.05s.
--     스크래치 DB(같은 이미지 timescaledb 2.17.2-pg16, 수급 전량 복사, 이 파일 그대로 적용) — 하루 0.10s, 09-14~ 전부 0.24s;
--       경계를 2025-09-15 로 당긴 1년치(674,064행) 하루 0.44s, 1년 전부 3.1s.
--   해마다 그만큼 느려진다. 머티리얼라이즈드 뷰는 REFRESH 를 daily_finalize 뒤에 배선해야 하고(DAG 변경 = CLAUDE.md §1
--   승인 대상), 그 사이엔 T+1 16:00 잠정 수급이 들어와도 낡은 값을 준다. 하루 조회가 수 초를 넘으면 그때
--   MATERIALIZED + daily_finalize 뒤 REFRESH 태스크를 제안한다.
--
-- 지연: t 의 정규장 종가는 t+1 수급이 들어온 뒤(T+1 16:00 잠정 수집) 나온다. 잠정 행도 flu_rt·close 가 같은 응답이라
-- 항등식이 성립할 것이지만 잠정 행으로는 아직 대조하지 않았다(검증은 확정 행). 그날 저녁에 그날(t) 정규장 종가가
-- 필요하면 분봉 15:30~15:32 마지막 봉.
--
-- 번호: origin/main 최댓값 030(daily_bars_adj_v2) + 1. 이 브랜치는 027~028 기준에서 갈라져 029·030 파일이 없다.
--
-- 같은 계산의 파이썬 판: collectors/data_checks.py base_close·derive_regular_close(daily_finalize 의 항등식 검사가 쓴다).
-- 호가 표·격자·두 날짜는 tests/test_data_checks.py 가 이 파일을 읽어 파이썬 상수와 대조한다 — 한쪽만 고치면 CI 가 깨진다.
--
-- 적용: docker exec -i quant-airflow-timescaledb-replica-1 psql -U "$TIMESCALE_USER" -d "$TIMESCALE_DB" \
--         -v ON_ERROR_STOP=1 < sql/migrations/031_regular_close.sql

CREATE OR REPLACE FUNCTION krx_tick_size(p_price numeric, p_day date) RETURNS integer
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT CASE
        WHEN p_price IS NULL OR p_day IS NULL THEN NULL
        WHEN p_day >= DATE '2023-01-25' THEN
            CASE WHEN p_price < 2000 THEN 1 WHEN p_price < 5000 THEN 5 WHEN p_price < 20000 THEN 10
                 WHEN p_price < 50000 THEN 50 WHEN p_price < 200000 THEN 100 WHEN p_price < 500000 THEN 500
                 ELSE 1000 END
        ELSE
            CASE WHEN p_price < 1000 THEN 1 WHEN p_price < 5000 THEN 5 WHEN p_price < 10000 THEN 10
                 WHEN p_price < 50000 THEN 50 WHEN p_price < 100000 THEN 100 WHEN p_price < 500000 THEN 500
                 ELSE 1000 END
    END
$$;

CREATE OR REPLACE FUNCTION krx_tick_round(p_price numeric, p_day date) RETURNS numeric
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT round(p_price / krx_tick_size(p_price, p_day)) * krx_tick_size(p_price, p_day)
$$;

CREATE OR REPLACE FUNCTION krx_base_close(p_close numeric, p_flu_rt numeric, p_day date) RETURNS numeric
LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS $$
DECLARE
    raw  numeric;
    std  integer;
    g    integer;
    cand numeric;
BEGIN
    IF p_close IS NULL OR p_flu_rt IS NULL OR p_day IS NULL OR p_close <= 0 OR p_flu_rt <= -10000 THEN
        RETURN NULL;
    END IF;
    raw := p_close * 10000 / (10000 + p_flu_rt);
    std := krx_tick_size(raw, p_day);
    FOREACH g IN ARRAY ARRAY[1000, 500, 100, 50, 10, 5, 1] LOOP
        CONTINUE WHEN g > std;
        cand := round(raw / g) * g;
        IF cand > 0 AND abs((p_close / cand - 1) * 10000 - p_flu_rt) <= 0.5 THEN
            -- 이웃 격자값도 flu_rt 를 맞추면 정밀도가 모자란 것(flu_rt ≈ −100% 등) — 고르지 않는다
            IF abs((p_close / (cand + g) - 1) * 10000 - p_flu_rt) <= 0.5
               OR (cand > g AND abs((p_close / (cand - g) - 1) * 10000 - p_flu_rt) <= 0.5) THEN
                RETURN NULL;
            END IF;
            RETURN cand;
        END IF;
    END LOOP;
    RETURN NULL;
END
$$;

CREATE OR REPLACE VIEW regular_close AS
SELECT s.code, s.date, s.close::numeric AS close, 'sd_close'::text AS src
  FROM supply_demand s
 WHERE s.date < DATE '2026-09-14' AND s.source LIKE 'kiwoom%'
UNION ALL
SELECT y.code, y.date,
       CASE WHEN y.src = 'identity' THEN y.base END AS close,
       CASE WHEN y.src = 'identity' AND y.base IS NULL THEN 'unmatched' ELSE y.src END AS src
  FROM (
    SELECT x.code, x.date, x.src,
           CASE WHEN x.src = 'identity' THEN krx_base_close(x.next_close, x.next_flu_rt::numeric, x.date) END AS base
      FROM (
        SELECT w.code, w.date, w.next_close, w.next_flu_rt,
               CASE WHEN c.next_date IS NULL THEN 'pending'
                    WHEN w.next_date IS NULL THEN 'no_next'
                    WHEN w.next_date <> c.next_date THEN 'gap'
                    WHEN EXISTS (SELECT 1 FROM corporate_actions ca
                                  WHERE ca.code = w.code AND ca.date = w.next_date
                                    AND ca.upd_rt IS DISTINCT FROM 0) THEN 'base_reset'
                    ELSE 'identity' END AS src
          FROM (
            SELECT s.code, s.date,
                   lead(s.date)   OVER win AS next_date,
                   lead(s.close)  OVER win AS next_close,
                   lead(s.flu_rt) OVER win AS next_flu_rt
              FROM supply_demand s
             WHERE s.date >= DATE '2026-09-14' AND s.source LIKE 'kiwoom%'
            WINDOW win AS (PARTITION BY s.code ORDER BY s.date)
          ) w
          JOIN (
            SELECT d.date, lead(d.date) OVER (ORDER BY d.date) AS next_date
              FROM (SELECT DISTINCT s.date FROM supply_demand s
                     WHERE s.date >= DATE '2026-09-14' AND s.source LIKE 'kiwoom%') d
          ) c ON c.date = w.date
      ) x
  ) y;

COMMENT ON VIEW regular_close IS
    '정규장 종가. 2026-09-14 전 = supply_demand.close(sd_close), 그 뒤 = 기준가 항등식(t+1 close/(1+flu_rt/1e4) 틱반올림). '
    'close NULL 이면 src 가 이유(pending·no_next·gap·base_reset·unmatched). migration 031';

-- 검증(적용 뒤):
--   SELECT * FROM regular_close WHERE code = '005930' AND date = '2026-10-06';   -- 272000 | identity (통합 종가 273,000)
--   SELECT * FROM regular_close WHERE code = '005930' AND date = '2017-03-31';   -- 2060000 | sd_close
--   SELECT src, count(*), count(close) FROM regular_close WHERE date >= '2026-09-14' GROUP BY 1;   -- identity 가 대부분, pending = 마지막 날 종목 수
--   SELECT krx_tick_round(150250, '2023-01-24'), krx_tick_round(150250, '2023-01-25'),
--          krx_base_close(269000, -110, '2026-10-06');                           -- 150500 | 150300 | 272000
--   분봉 대조(마지막 봉 15:30~15:32, 마감 지연일 제외):
--   WITH m AS (SELECT DISTINCT ON (code, ts::date) code, ts::date AS date, close FROM minute_bars
--               WHERE ts >= '2026-09-14' AND ts::time BETWEEN '15:30' AND '15:32' ORDER BY code, ts::date, ts DESC)
--   SELECT count(*), count(*) FILTER (WHERE r.close = m.close) FROM regular_close r JOIN m USING (code, date)
--    WHERE r.src = 'identity';      -- 2026-10-13 실측(같은 계산의 파이썬 판): 36,001 | 35,990 — 11건은 분봉이 권리락으로 소급 수정
--   src 분포 2026-10-13 실측(09-14~10-12): identity 41,949 · pending 2,625 · base_reset 31 · no_next 8 · unmatched 1
--
-- 롤백: DROP VIEW regular_close; DROP FUNCTION krx_base_close(numeric, numeric, date);
--       DROP FUNCTION krx_tick_round(numeric, date); DROP FUNCTION krx_tick_size(numeric, date);
