-- earnings_dart_doc: DART 정기보고서 원문에서 읽은 분기 순이익·발행주식총수 (2026-10-13 초안 — **미적용, 승인 대기**)
--
-- 왜: earnings 의 원천(fnlttMultiAcnt)은 2015 사업연도부터만 답한다. 2007~2015 는 사업·반기·분기보고서 원문 표를
-- 읽어야 한다(collectors/dart_doc.py, 2006~2016 이력 확장 완료 조건 C). earnings 에 섞지 않고 새 표에 두는 이유:
--   - 기준이 섞인다: K-GAAP 시절(~2010) 분기는 별도, 사업보고서만 연결이고 2011 K-IFRS 에서 연결로 바뀐다.
--     한 열(netinc)에 넣으면 같은 회사 시계열에 기준 이음매가 숨는다 → 기준별 열(ctrl·cfs·ofs)을 다 둔다.
--   - earnings 의 정의(연결 합계 → 별도, 비12월 결산사는 회계분기를 달력분기 라벨로)와 1:1 이 아니다
--     — 이 표는 12월 결산사만, 달력분기만.
-- 검증(2016 겹침, 손대지 않은 표본 60종목): earnings 와 대조 가능한 보고서 기준 97%+, 같은 공시끼리 값 오독 0.
--
-- 의미:
--   period          'YYYYQn' 달력분기(12월 결산사만)
--   knowledge_date  그 레코드를 실은 공시의 접수일(rcept_no 앞 8자리) — earnings.knowledge_date 와 같은 뜻.
--                   정정 레코드는 정정본 접수일. 읽는 쪽은 knowledge_date **다음 거래일부터** 쓴다.
--   rcept_no        그 공시 접수번호, is_correction = [기재정정]·[첨부추가] 등 정정본
--   netinc/basis    earnings 와 같은 정의(연결 합계 → 별도), 그 분기 값: Q1~Q3 = 3개월, Q4 = 사업연도 전체(원)
--   netinc_ctrl/basis_ctrl  지배기업 소유주 귀속 우선(ctrl → cfs → ofs)
--   derived*        3개월 값을 같은 기준의 누적 − 직전 분기 누적(그 접수일까지 알려진 판)으로 만들었다
--   {ctrl,cfs,ofs}_q / _ytd  기준별 3개월·누적(Q4 _ytd = 연간, ofs_ytd = 별도 연간). 기준이 다른 값끼리 빼지 않는다
--   shares_*        '주식의 총수 등' 발행주식총수(유통주식수 아님) — shares_asof 를 못 찾으면 NULL + shares_asof_inferred
--   flags           파서 표시(ctrl_inconsistent·unit_rescaled·*_unit_conflict 등)
--
-- 적용(승인 뒤): docker exec -i quant-airflow-timescaledb-replica-1 psql -U "$TIMESCALE_USER" -d "$TIMESCALE_DB" \
--                  -v ON_ERROR_STOP=1 < sql/migrations/034_earnings_dart_doc.sql
-- 적용과 함께: sql/init_timescale.sql·docs/schema.md 반영(CLAUDE.md §2) — 초안 단계라 아직 넣지 않았다.

CREATE TABLE IF NOT EXISTS earnings_dart_doc (
    code                  TEXT    NOT NULL,
    period                TEXT    NOT NULL,
    knowledge_date        DATE    NOT NULL,
    rcept_no              TEXT    NOT NULL,
    is_correction         BOOLEAN NOT NULL,
    basis                 TEXT,                 -- 'cfs' | 'ofs' | NULL
    netinc                DOUBLE PRECISION,
    derived               BOOLEAN NOT NULL DEFAULT FALSE,
    basis_ctrl            TEXT,                 -- 'ctrl' | 'cfs' | 'ofs' | NULL
    netinc_ctrl           DOUBLE PRECISION,
    derived_ctrl          BOOLEAN NOT NULL DEFAULT FALSE,
    ctrl_q                DOUBLE PRECISION,
    ctrl_ytd              DOUBLE PRECISION,
    ctrl_q_derived        BOOLEAN NOT NULL DEFAULT FALSE,
    cfs_q                 DOUBLE PRECISION,
    cfs_ytd               DOUBLE PRECISION,
    cfs_q_derived         BOOLEAN NOT NULL DEFAULT FALSE,
    ofs_q                 DOUBLE PRECISION,
    ofs_ytd               DOUBLE PRECISION,
    ofs_q_derived         BOOLEAN NOT NULL DEFAULT FALSE,
    shares_common         BIGINT,
    shares_preferred      BIGINT,
    shares_total          BIGINT,
    shares_asof           DATE,
    shares_asof_inferred  BOOLEAN NOT NULL DEFAULT FALSE,
    flags                 TEXT[]  NOT NULL DEFAULT '{}',
    source                TEXT    NOT NULL DEFAULT 'dart_doc',
    PRIMARY KEY (code, period, knowledge_date),
    CHECK (basis IS NULL OR basis IN ('cfs', 'ofs')),
    CHECK (basis_ctrl IS NULL OR basis_ctrl IN ('ctrl', 'cfs', 'ofs'))
);
CREATE INDEX IF NOT EXISTS earnings_dart_doc_period_idx ON earnings_dart_doc (period, knowledge_date);

-- 검증:
--   SELECT left(period, 4), count(*), count(netinc), count(*) FILTER (WHERE is_correction) FROM earnings_dart_doc GROUP BY 1 ORDER BY 1;
--   SELECT netinc, basis, netinc_ctrl FROM earnings_dart_doc WHERE code = '005930' AND period = '2013Q4' AND NOT is_correction;
--     -- 30,474,764 백만원 · cfs · 29,821,215 백만원(삼성전자 2013 사업보고서)
--
-- 롤백: DROP TABLE earnings_dart_doc;
