-- 백필 "조회해봤고 없더라" 마커를 소스별 컬럼에서 (code, source) 테이블로 옮긴다
--
-- **004 → 007 → (지금 넷째) 로 같은 병이 세 번 재발했다.** 007 주석이 스스로
-- "004(naver_checked)와 같은 병, 같은 약이다" 라고 적고 있다. 소스가 하나 늘 때마다
-- 마이그레이션 + 컬럼 + 함수 사본 + 대상쿼리의 NOT EXISTS 절이 같이 늘어난다.
--
-- 그리고 이번에 그 구조의 진짜 한계가 드러났다: 마커 컬럼이 `delisted_stocks` 에
-- 있어서 **상장 종목에는 쓸 수가 없다**(그 테이블에 상장 종목이 없다).
-- `dart_shares --listed` 가 "DART 에 보고서가 아예 없는" 60종목을 매 회차 다시
-- 조회하는데, 그걸 기록할 자리가 없었다. 넷째 컬럼을 붙이는 대신 자리를 바꾼다.
--
-- 적용:
--   psql "$DB_URL" -v ON_ERROR_STOP=1 -f sql/migrations/009_backfill_markers.sql

BEGIN;

CREATE TABLE IF NOT EXISTS backfill_markers (
    code         TEXT NOT NULL,
    source       TEXT NOT NULL,
    checked_date DATE NOT NULL,
    PRIMARY KEY (code, source)
);

COMMENT ON TABLE backfill_markers IS
    '백필 소스가 "이 코드는 조회해봤고 자료가 없더라"를 기록하는 곳. 마커가 없으면 '
    '자료 없는 코드는 결과 행이 안 생겨 대상 쿼리에 영원히 걸린다. source 는 '
    'collectors.storage 의 CHECKED_* 상수 — 새 소스는 문자열 하나면 되고 스키마를 '
    '건드리지 않는다.';

-- 기존 세 컬럼의 데이터를 그대로 옮긴다(이름은 storage.CHECKED_* 와 맞춘다).
INSERT INTO backfill_markers (code, source, checked_date)
SELECT code, 'naver_bars', naver_checked FROM delisted_stocks WHERE naver_checked IS NOT NULL
ON CONFLICT (code, source) DO NOTHING;

INSERT INTO backfill_markers (code, source, checked_date)
SELECT code, 'naver_flow', naver_sd_checked FROM delisted_stocks WHERE naver_sd_checked IS NOT NULL
ON CONFLICT (code, source) DO NOTHING;

INSERT INTO backfill_markers (code, source, checked_date)
SELECT code, 'dart_shares_delisted', dart_checked FROM delisted_stocks WHERE dart_checked IS NOT NULL
ON CONFLICT (code, source) DO NOTHING;

-- 옛 컬럼은 **이 마이그레이션에서 지우지 않는다.** 데이터를 복사한 직후에 원본을
-- 없애면 되돌릴 방법이 사라진다. 읽기·쓰기는 이미 새 테이블로 옮겨졌으므로 이
-- 컬럼들은 이 시점부터 갱신되지 않는다 — 다음 회차에 지운다.
COMMENT ON COLUMN delisted_stocks.naver_checked IS
    'DEPRECATED (009) — backfill_markers(source=naver_bars) 로 옮겼다. 더 이상 갱신되지 않는다.';
COMMENT ON COLUMN delisted_stocks.naver_sd_checked IS
    'DEPRECATED (009) — backfill_markers(source=naver_flow) 로 옮겼다. 더 이상 갱신되지 않는다.';
COMMENT ON COLUMN delisted_stocks.dart_checked IS
    'DEPRECATED (009) — backfill_markers(source=dart_shares_delisted) 로 옮겼다. 더 이상 갱신되지 않는다.';

COMMIT;

-- 검증 (옮긴 건수가 원본과 같아야 한다):
--   SELECT source, count(*) FROM backfill_markers GROUP BY source ORDER BY source;
--   SELECT count(*) FILTER (WHERE naver_checked IS NOT NULL)    AS naver_bars,
--          count(*) FILTER (WHERE naver_sd_checked IS NOT NULL) AS naver_flow,
--          count(*) FILTER (WHERE dart_checked IS NOT NULL)     AS dart_delisted
--     FROM delisted_stocks;
--
-- 롤백 (옛 컬럼이 아직 살아 있으므로 읽기만 되돌리면 된다):
--   DROP TABLE backfill_markers;
