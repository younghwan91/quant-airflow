-- KOSPI 200·KOSDAQ 150 등 대표지수 구성종목 변경 이력 (2026-10-12)
--
-- 왜: swing-it 결합 전략 연구가 지수 편입·편출 수급(정기변경 전후 패시브 매매)을 검증하려고
-- 2017년부터의 변경 이력을 요청했다. data.krx.co.kr 는 로그인을 요구하지만(2026-10-12 실측 "LOGOUT"),
-- KRX 지수 사이트 index.krx.co.kr 의 "지수운영공지" 게시판(MKD01040000)은 로그인 없이 목록·본문을 주고
-- 첨부파일도 1회용 OTP 로 받는다. 2015-01 부터 723건이 있다.
--   정기변경 본 공지(2015-12 ~ 2026-05, 22건 — KOSPI 200 은 2016-06 부터)에는 엑셀이 붙는다 — 지수별 "교체종목/변경내역/편출입"
--   시트(편입·제외 ISIN)와 "구성종목" 시트(변경 후 전체 명단). 2019 년까지 KOSPI 200 은 6월만,
--   12월은 KOSDAQ 150(·2018~ KRX 300)만 바뀌었다.
--
-- 세 테이블:
--   index_notices       공지 원문 전부(제목·본문·첨부명). 특례편입·인적분할·합병·신규상장 같은 수시변경은
--                       공지마다 형식이 달라 구조화하지 않고 여기 원문으로 둔다.
--   index_changes       정기변경의 편입·편출. effective_date = 공지의 "정기변경일"(선물·옵션 동시만기일
--                       다음 거래일, 그날부터 새 구성) — 패시브 매매는 그 **전 거래일 종가**에 몰린다.
--                       announce_date = 공지 게시일(시각 없음 — 쓰는 쪽은 다음 거래일부터 안다고 본다).
--   index_constituents  정기변경 후 전체 구성종목 — 그 effective_date 부터 다음 정기변경 전까지의 명단
--                       (사이의 수시변경은 반영 안 됨, index_notices 참고).
--
-- 적용: psql "$KR_QUANT_DB" -v ON_ERROR_STOP=1 -f sql/migrations/021_index_changes.sql

BEGIN;

CREATE TABLE IF NOT EXISTS index_notices (
    bbs_seq      INTEGER PRIMARY KEY,   -- 게시판 글 번호
    posted_date  DATE NOT NULL,
    title        TEXT NOT NULL,
    body         TEXT,
    attachments  TEXT,                  -- 첨부 파일명(줄바꿈 구분)
    fetched_at   TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_index_notices_posted ON index_notices(posted_date);

CREATE TABLE IF NOT EXISTS index_changes (
    index_name      TEXT NOT NULL,      -- KOSPI200 · KOSDAQ150 · KRX300 · KOSPI100 · KOSPI50
    effective_date  DATE NOT NULL,
    action          TEXT NOT NULL CHECK (action IN ('add', 'drop')),
    code            TEXT NOT NULL,      -- 단축코드
    isin            TEXT,               -- NULL = 그 공지 엑셀이 단축코드만 줬다(2021-06)
    name            TEXT,
    announce_date   DATE NOT NULL,
    bbs_seq         INTEGER NOT NULL,   -- index_notices 의 근거 공지
    PRIMARY KEY (index_name, effective_date, action, code)
);
CREATE INDEX IF NOT EXISTS idx_index_changes_code ON index_changes(code, effective_date);

CREATE TABLE IF NOT EXISTS index_constituents (
    index_name      TEXT NOT NULL,
    effective_date  DATE NOT NULL,
    code            TEXT NOT NULL,
    isin            TEXT,
    name            TEXT,
    announce_date   DATE NOT NULL,
    bbs_seq         INTEGER NOT NULL,
    PRIMARY KEY (index_name, effective_date, code)
);
CREATE INDEX IF NOT EXISTS idx_index_constituents_code ON index_constituents(code, effective_date);

COMMIT;

-- 검증:
--   SELECT index_name, effective_date, count(*) FILTER (WHERE action='add') AS adds,
--          count(*) FILTER (WHERE action='drop') AS drops
--     FROM index_changes GROUP BY 1, 2 ORDER BY 2, 1;              -- 공지 본문의 교체종목수와 같다
--   SELECT index_name, effective_date, count(*) FROM index_constituents
--    GROUP BY 1, 2 ORDER BY 2, 1;                                   -- 200 · 150 · 300 · 100 · 50
--   SELECT * FROM index_changes WHERE effective_date = '2020-06-12' AND index_name = 'KOSPI200';
--
-- 롤백: DROP TABLE index_changes; DROP TABLE index_constituents; DROP TABLE index_notices;
