-- shares_original_filing: 상장주식수의 "처음 공개된 날·그때 값" (2026-11-02)
--
-- 왜: DART stockTotqySttus 의 rcept_no 는 **마지막 정정본** 것이라 shares_outstanding_history.knowledge_date 가 늦다.
--   실측 2026-11-02(earnings_disclosures 원본 접수일과 비교 가능한 dart 17,425행): 늦은 행 3,397 · 30일 넘게 늦은 행 2,202(1,277종목).
--   017670 2021-12-31: 원본 2022-03-16, 저장 2022-08-12. 068270 2021-12-31: 원본 2022-03-08, 저장 2022-05-12.
--   본표는 (code, date) 한 행이고 수집은 ON CONFLICT DO NOTHING 이라 기존 행을 고치지 않는다 — 옆 표로 더한다.
--
-- 의미:
--   date              shares_outstanding_history.date 와 같은 기준일
--   first_public_date 원본 정기보고서 접수일(earnings_disclosures.rcept_dt) — 읽는 쪽은 다음 거래일부터 쓴다
--   rcept_no          원본 접수번호
--   shares_original   원본 원문 '주식의 총수' 의 보통주 발행주식총수(dart_doc.extract_shares)
--   행 없음           원문을 못 읽었거나 기준일·크기가 안 맞았다 — 본표의 늦은 도장이 남는다(틀린 값보다 없는 값)
--
-- 읽는 법(krx-quant-core load_risk_inputs(shares_basis="first_public")): 본표 coalesce(knowledge_date, date) UNION ALL 이 표 first_public_date.
-- 같은 기준일은 늦게 안 판이 덮는다(정정 반영). 일반 테이블 — 몇천 행이고 덮어쓰지 않는다.
--
-- 적용: docker exec -i quant-airflow-timescaledb-replica-1 psql -U "$TIMESCALE_USER" -d "$TIMESCALE_DB" \
--         -v ON_ERROR_STOP=1 < sql/migrations/039_shares_original_filing.sql

CREATE TABLE IF NOT EXISTS shares_original_filing (
    code              TEXT   NOT NULL,
    date              DATE   NOT NULL,
    first_public_date DATE   NOT NULL,
    rcept_no          TEXT   NOT NULL,
    shares_original   BIGINT NOT NULL,
    PRIMARY KEY (code, date)
);

-- 검증: SELECT count(*), min(first_public_date), max(first_public_date) FROM shares_original_filing;
--       SELECT first_public_date FROM shares_original_filing WHERE code = '017670' AND date = '2021-12-31';  -- 2022-03-16
-- 롤백(사용자 확인 뒤): DROP TABLE shares_original_filing;
