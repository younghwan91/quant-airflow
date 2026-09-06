"""Storage layer for collected datasets — sqlite or Postgres/TimescaleDB (write side).

Defines the schema and upsert helpers used by ``collectors/*.py``. Collectors
produce plain records; this module persists them idempotently on natural
keys. ``connect()`` dispatches on the connection string: a
``postgresql://``/``postgres://`` DSN opens Postgres (psycopg2, imported
lazily so sqlite-only use never needs it installed); anything else opens a
local sqlite file exactly as before.

This is an intentionally independent copy of the write-side half of
kr-quant's ``kr_quant/storage.py`` (kr-quant keeps the read-side half:
``connect``/``market_cap_asof``/``market_cap_asof_bulk``) — small enough that
duplicating it is simpler than introducing a shared package between the two
repos.
"""

from __future__ import annotations

import sqlite3
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Any

_PG_PREFIXES = ("postgresql://", "postgres://")

# ka10059 (투자자기관별종목별) net-buy fields → DB columns.
# Order matters: it defines the column order for ``supply_demand`` inserts.
#
# **단위: 순매매 수량(주). 금액이 아니다.** 수집기가 amt_qty_tp="2"(수량)로
# 부르기 때문이다. 금액으로 쓰려면 종가를 곱하되, 참값은 VWAP 가중이므로 근사다.
#
# ``natn``(국가)은 실측상 값이 들어온 적이 없다 — 최근 90일 157,532행 전부 0이다
# (2026-08-27). 컬럼은 벤더 응답 모양을 보존하려고 남겨두지만, 이걸 화면에 그리면
# 항상 빈 값이다.
INVESTOR_COLUMNS: dict[str, str] = {
    "individual": "ind_invsr",   # 개인
    "foreign_": "frgnr_invsr",   # 외국인
    "institution": "orgn",       # 기관계
    "fnnc_invt": "fnnc_invt",    # 금융투자
    "insrnc": "insrnc",          # 보험
    "invtrt": "invtrt",          # 투신
    "bank": "bank",              # 은행
    "penfnd_etc": "penfnd_etc",  # 연기금 등
    "samo_fund": "samo_fund",    # 사모펀드
    "natn": "natn",              # 국가
    "etc_corp": "etc_corp",      # 기타법인
}

SUPPLY_DEMAND_COLUMNS: list[str] = [
    "code",
    "date",
    "close",
    "flu_rt",
    "acc_trde_qty",
    *INVESTOR_COLUMNS.keys(),
]

# ka10081 (주식일봉차트) candle fields → DB columns. Order defines insert order.
DAILY_BAR_COLUMNS: list[str] = [
    "code",
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "trade_value",
]

_INVESTOR_COL_DDL = ",\n            ".join(f"{c} INTEGER" for c in INVESTOR_COLUMNS)

SCHEMA = f"""
CREATE TABLE IF NOT EXISTS stocks (
    code   TEXT PRIMARY KEY,
    name   TEXT,
    market TEXT,
    sector TEXT,
    kind   TEXT
);
CREATE TABLE IF NOT EXISTS supply_demand (
    code         TEXT NOT NULL,
    date         TEXT NOT NULL,
    close        INTEGER,
    -- 등락률 × 100 (bp). 175 = +1.75%, -309 = -3.09% — **백분율이 아니다.**
    -- 벤더(ka10059) 표기를 그대로 저장한다. 그대로 % 로 읽으면 100배가 되고,
    -- 실제로 하류에서 +1301% 가 찍힌 적이 있다(2026-08-27, kr-quant 뷰어).
    flu_rt       REAL,
    acc_trde_qty INTEGER,       -- 거래량(주). daily_bars.volume 과 같은 값이다.
    -- 아래 투자자별 순매매는 전부 **수량(주)** 이지 금액이 아니다 — 수집기가
    -- ka10059 를 amt_qty_tp="2"(수량)로 부른다. 금액이 필요하면 종가를 곱해야
    -- 하고, 참값은 VWAP 가중이라 그 환산은 근사다.
    {_INVESTOR_COL_DDL},
    source       TEXT NOT NULL DEFAULT 'kiwoom',  -- kiwoom(전체) / naver(폐지 부분: 기관·외국인만)
    PRIMARY KEY (code, date)
);
CREATE INDEX IF NOT EXISTS idx_sd_date ON supply_demand(date);
-- source: 'kiwoom' = 상장 종목(ka10081, trade_value 는 보고된 거래대금),
--         'naver'  = 상장폐지 종목 백필(siseJson, trade_value 는 close*volume/1e6 근사).
-- 폐지 종목이 이 테이블에 들어오는 이유는 생존편향이다 — 수집 소스가 현재 상장 종목만
-- 돌려주므로, 그냥 두면 백테스트가 살아남은 회사만 보고 성적을 잰다.
CREATE TABLE IF NOT EXISTS daily_bars (
    code        TEXT NOT NULL,
    date        TEXT NOT NULL,
    open        INTEGER,
    high        INTEGER,
    low         INTEGER,
    close       INTEGER,
    volume      INTEGER,
    trade_value INTEGER,
    source      TEXT NOT NULL DEFAULT 'kiwoom',
    PRIMARY KEY (code, date)
);
CREATE INDEX IF NOT EXISTS idx_db_date ON daily_bars(date);
CREATE TABLE IF NOT EXISTS short_selling (
    code            TEXT NOT NULL,
    date            TEXT NOT NULL,
    close           INTEGER,
    volume          INTEGER,
    short_qty       INTEGER,   -- 당일 공매도 수량 (shrts_qty)
    short_balance   INTEGER,   -- 공매도 잔고 수량 (ovr_shrts_qty)
    short_ratio     REAL,      -- 공매도 비중 % (trde_wght)
    short_avg_price INTEGER,   -- 공매도 평균가 (shrts_avg_pric)
    short_value     INTEGER,   -- 공매도 거래대금 (shrts_trde_prica)
    PRIMARY KEY (code, date)
);
CREATE INDEX IF NOT EXISTS idx_ss_date ON short_selling(date);
CREATE TABLE IF NOT EXISTS credit_balance (
    code        TEXT NOT NULL,
    date        TEXT NOT NULL,
    close       INTEGER,
    new_qty     INTEGER,   -- 신규 신용매수 (new)
    repay_qty   INTEGER,   -- 상환 (rpya)
    balance_qty INTEGER,   -- 신용잔고 수량 (remn)
    balance_amt INTEGER,   -- 신용잔고 금액 (amt)
    balance_rt  REAL,      -- 신용잔고율 % (remn_rt)
    credit_rt   REAL,      -- 신용비율 % (shr_rt)
    PRIMARY KEY (code, date)
);
CREATE INDEX IF NOT EXISTS idx_cb_date ON credit_balance(date);
CREATE TABLE IF NOT EXISTS sector_index (
    code        TEXT NOT NULL,  -- 업종코드 (001=KOSPI 종합, 101=KOSDAQ 종합 등)
    name        TEXT,
    date        TEXT NOT NULL,
    open        INTEGER,
    high        INTEGER,
    low         INTEGER,
    close       INTEGER,
    volume      INTEGER,
    trade_value INTEGER,
    PRIMARY KEY (code, date)
);
CREATE INDEX IF NOT EXISTS idx_si_date ON sector_index(date);
CREATE TABLE IF NOT EXISTS shares_outstanding_history (
    code               TEXT NOT NULL,
    date               TEXT NOT NULL,
    shares_outstanding INTEGER,  -- sqlite INTEGER is dynamically 64-bit already;
    source             TEXT NOT NULL DEFAULT 'kiwoom',  -- kiwoom/krx/dart(폐지 백필)
    knowledge_date     TEXT,     -- DART 는 기준일(date)과 공시 접수일이 다르다
    PRIMARY KEY (code, date)     -- Postgres side (init_timescale.sql) must use BIGINT, not INTEGER(32bit) — 삼성전자(58억주) overflows it
);
CREATE INDEX IF NOT EXISTS idx_sh_date ON shares_outstanding_history(date);
CREATE TABLE IF NOT EXISTS earnings (
    code            TEXT NOT NULL,
    period          TEXT NOT NULL,   -- e.g. '2020Q1'
    avail_date      TEXT,            -- lookahead-safe availability date (period-end + filing lag)
    knowledge_date  TEXT NOT NULL,   -- 이 값을 우리가 알게 된 날 (수집일). 정정공시는 새 행으로 쌓인다.
    netinc          REAL,
    netinc_prior    REAL,
    revenue         REAL,
    revenue_prior   REAL,
    op_income       REAL,
    op_income_prior REAL,
    PRIMARY KEY (code, period, knowledge_date)
);
CREATE INDEX IF NOT EXISTS idx_earnings_avail_date ON earnings(avail_date);
CREATE INDEX IF NOT EXISTS idx_earnings_asof ON earnings(code, period, knowledge_date DESC);
CREATE TABLE IF NOT EXISTS consensus (
    code         TEXT NOT NULL,
    date         TEXT NOT NULL,   -- 스냅샷 수집일 (오늘)
    target_mean  REAL,            -- 목표주가 평균
    recomm_mean  REAL,            -- 투자의견 평균 (1~5, 5=강력매수)
    base_date    TEXT,            -- 컨센서스 기준일(네이버 createDate)
    fwd_eps      REAL,            -- 향후 컨센서스 EPS
    prev_eps     REAL,            -- 직전 확정 EPS
    est_year     TEXT,            -- fwd_eps가 가리키는 연도(예: '202612')
    PRIMARY KEY (code, date)
);
CREATE INDEX IF NOT EXISTS idx_consensus_date ON consensus(date);
CREATE TABLE IF NOT EXISTS daily_bars_adjusted (
    code        TEXT NOT NULL,
    date        TEXT NOT NULL,
    open        REAL,   -- price_adjust.adjust_prices()의 back-adjust 배수 적용 후이므로
    high        REAL,   -- daily_bars(원자료, INTEGER)와 달리 REAL — 분할비율이 실수라 정수로
    low         REAL,   -- 안 떨어짐(예: 1주→4주 분할이면 종가가 1/4배가 됨)
    close       REAL,
    volume      INTEGER,      -- 기본은 미조정 원본 거래량 그대로(adjust_volume=False)
    trade_value INTEGER,      -- 거래대금은 가격조정과 무관(가격×수량이 아니라 원 보고값)
    source      TEXT NOT NULL DEFAULT 'kiwoom',  -- daily_bars.source 전파(근사 거래대금 식별)
    PRIMARY KEY (code, date)
);
CREATE INDEX IF NOT EXISTS idx_dba_date ON daily_bars_adjusted(date);
-- 백필 소스가 "이 코드는 조회해봤고 자료가 없더라"를 기록하는 곳.
-- **소스마다 컬럼을 늘리지 않는다.** 예전엔 delisted_stocks 에 naver_checked(004)
-- → dart_checked·naver_sd_checked(007) 로 컬럼을 붙여갔고, 007 주석이 스스로
-- "004 와 같은 병, 같은 약"이라고 적고 있다. 소스가 하나 늘 때마다 마이그레이션 +
-- 컬럼 + 함수 사본 + 대상쿼리의 NOT EXISTS 절이 같이 늘었다.
-- 게다가 그 컬럼들은 delisted_stocks 에 있어서 **상장 종목에는 쓸 수가 없었다** —
-- dart_shares --listed 가 자료 없는 60종목을 매번 다시 조회하던 이유다.
-- 여기서는 (code, source) 한 쌍이라 새 소스가 문자열 하나로 끝난다.
CREATE TABLE IF NOT EXISTS backfill_markers (
    code         TEXT NOT NULL,
    source       TEXT NOT NULL,   -- storage.CHECKED_* 상수
    checked_date TEXT NOT NULL,   -- 조회해봤고 자료가 없던 날
    PRIMARY KEY (code, source)
);
CREATE TABLE IF NOT EXISTS delisted_stocks (
    code            TEXT NOT NULL,
    name            TEXT,
    market          TEXT,
    last_trade_date TEXT,   -- daily_bars 기준 마지막 거래일(상장폐지일 근사), 이력 없으면 NULL
    naver_checked   TEXT,   -- 네이버 조회했으나 우리 구간 내 데이터 없던 날(NULL=미확인)
    dart_checked    TEXT,   -- DART 조회했으나 주식수 자료가 없던 날(NULL=미확인)
    naver_sd_checked TEXT,  -- 네이버 수급을 조회했으나 빈 응답이던 날(NULL=미확인)
    PRIMARY KEY (code)
);
-- krx-news-client(pip)로 수집한 뉴스 히스토리 — 백테스팅+실매매, 추후 LLM
-- 매매판단용(migrations/010 참고). id 는 make_article_id(source, url)라 안정적이라
-- 같은 기사가 여러 피드/크롤링 주기에서 다시 들어와도 upsert가 같은 행을 갱신한다.
-- Postgres 쪽(init_timescale.sql)은 published_at 이 하이퍼테이블 파티션 컬럼이라
-- PK 에 포함하지만, sqlite 는 하이퍼테이블 제약이 없으므로 id 하나만 PK로 둔다.
CREATE TABLE IF NOT EXISTS news_articles (
    id           TEXT NOT NULL,
    source       TEXT NOT NULL,
    category     TEXT,
    title        TEXT NOT NULL,
    url          TEXT NOT NULL,
    content      TEXT,
    summary      TEXT,
    author       TEXT,
    published_at TEXT NOT NULL,
    collected_at TEXT NOT NULL,
    PRIMARY KEY (id)
);
CREATE INDEX IF NOT EXISTS idx_news_articles_published_at ON news_articles(published_at);
CREATE TABLE IF NOT EXISTS news_article_tickers (
    article_id TEXT NOT NULL,
    ticker     TEXT NOT NULL,
    PRIMARY KEY (article_id, ticker)
);
CREATE INDEX IF NOT EXISTS idx_nat_ticker ON news_article_tickers(ticker);
-- krx-news-client(pip)의 DartScraper.scrape_disclosures()로 수집한 DART 공시
-- (migrations/011 참고). NewsArticle과 필드가 달라(회사·티커·공시유형) 별도 테이블로
-- 둔다 — ticker가 DART API의 stock_code를 그대로 쓰므로 news_articles와 달리
-- 정규화 없이 컬럼 하나로 충분하다(공시 1건=발행사 1곳).
CREATE TABLE IF NOT EXISTS disclosures (
    id              TEXT NOT NULL,
    source          TEXT NOT NULL,
    title           TEXT NOT NULL,
    url             TEXT NOT NULL,
    company         TEXT,
    ticker          TEXT,
    disclosure_type TEXT,
    published_at    TEXT NOT NULL,
    collected_at    TEXT NOT NULL,
    PRIMARY KEY (id)
);
CREATE INDEX IF NOT EXISTS idx_disclosures_ticker ON disclosures(ticker);
CREATE INDEX IF NOT EXISTS idx_disclosures_published_at ON disclosures(published_at);
CREATE TABLE IF NOT EXISTS news_judgments (
    source_type         TEXT NOT NULL,
    source_id           TEXT NOT NULL,
    ticker              TEXT NOT NULL,
    event_type          TEXT NOT NULL,
    sentiment_direction INTEGER NOT NULL,
    related_codes       TEXT NOT NULL DEFAULT '[]',
    is_stale_repeat     BOOLEAN NOT NULL DEFAULT 0,
    first_seen_date     TEXT,
    price_impact_likely BOOLEAN NOT NULL DEFAULT 0,
    rationale           TEXT NOT NULL DEFAULT '',
    model_id            TEXT NOT NULL,
    prompt_version      TEXT NOT NULL,
    knowledge_date      TEXT NOT NULL,
    confidence          INTEGER,          -- LLM 자체 확신도 0~100 (013, NULL=013 이전 행)
    judged_at           TEXT,             -- generate() 응답 시각 UTC ISO (013, NULL=013 이전 행)
    PRIMARY KEY (source_type, source_id, ticker, prompt_version)
);
CREATE INDEX IF NOT EXISTS idx_news_judgments_ticker ON news_judgments(ticker);
CREATE INDEX IF NOT EXISTS idx_news_judgments_knowledge_date ON news_judgments(knowledge_date);
"""


def default_db_path() -> Path:
    """Default DB location: ``<repo>/data/kr_quant.db`` (gitignored)."""
    return Path(__file__).resolve().parents[1] / "data" / "kr_quant.db"


def connect(db_path: str | Path | None = None) -> Any:
    """Open a connection with row access.

    ``db_path`` starting with ``postgresql://``/``postgres://`` opens Postgres
    (e.g. TimescaleDB) via psycopg2. Anything else is treated as a sqlite file
    path (default: ``<repo>/data/kr_quant.db``, dirs created as needed).
    """
    if isinstance(db_path, str) and db_path.startswith(_PG_PREFIXES):
        import psycopg2  # noqa: PLC0415 — optional dep, only needed for this path

        con = psycopg2.connect(db_path)
        # Schema (tables, hypertables, compression policy) is provisioned by
        # sql/init_timescale.sql, not here — init_db() only applies to the
        # sqlite path.
        return con

    path = Path(db_path) if db_path else default_db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    init_db(con)
    return con


def init_db(con: sqlite3.Connection) -> None:
    con.executescript(SCHEMA)
    con.commit()


def _is_pg(con: Any) -> bool:
    return not isinstance(con, sqlite3.Connection)


def fetchone(con: Any, sql: str, params: tuple) -> tuple | None:
    """sqlite3/psycopg2 양쪽에서 한 행을 읽는다 (``?`` 파라미터로 통일).

    sqlite3 는 ``con.execute()`` 와 ``?`` 를, psycopg2 는 ``con.cursor().execute()``
    와 ``%s`` 를 쓴다. **이 차이가 이 레포에서 반복해서 사고를 냈다** — 콜렉터마다
    sqlite 전용 헬퍼를 따로 쓰다가 Postgres 에서 `AttributeError: 'connection'
    object has no attribute 'execute'` 로 죽는 패턴이다:

    - `daily_bars` 의 `--update` 경로 (2026-07-17, `daily_collection_catchup` 이
      paused 로 방치돼 있던 원인)
    - `supply_demand._has_recent_rows` (잠복이었다 — 두 DAG 가 `--resume` 을 안
      넘겨서 안 터졌을 뿐이고, `combined --resume` 은 Postgres 로 이걸 부른다.
      지금은 이 함수를 쓴다)

    그래서 구현을 여기 하나로 모은다. 새 콜렉터는 이걸 쓴다.
    """
    if _is_pg(con):
        with con.cursor() as cur:
            cur.execute(sql.replace("?", "%s"), params)
            return cur.fetchone()
    return con.execute(sql, params).fetchone()


def fetchall(con: Any, sql: str, params: tuple = ()) -> list[tuple]:
    """:func:`fetchone` 의 다행 짝 — 여러 행을 sqlite3/psycopg2 양쪽에서 읽는다.

    ``fetchone`` 만 있던 동안 콜렉터들은 다행 조회를 만날 때마다 ``if _is_pg(con):
    with con.cursor() ...`` 분기를 손으로 다시 썼다(daily_bars·dart_shares·
    naver_delisted_bars·naver_supply_demand·krx_delisted). ``krx_shares`` 는 아예
    **네 번째 백엔드 판정식**(``con.__class__.__module__.startswith("psycopg")``)까지
    직접 만들었다 — ``fetchone`` 의 docstring 이 막으려던 드리프트가 다행 쪽에서
    그대로 재발한 것이다. 그래서 여기 하나로 모은다.
    """
    if _is_pg(con):
        with con.cursor() as cur:
            cur.execute(sql.replace("?", "%s"), params)
            return cur.fetchall()
    return con.execute(sql, params).fetchall()


def execute(con: Any, sql: str, params: tuple = ()) -> None:
    """행을 돌려주지 않는 문장을 실행하고 커밋한다 (``?`` 파라미터로 통일)."""
    if _is_pg(con):
        with con.cursor() as cur:
            cur.execute(sql.replace("?", "%s"), params)
    else:
        con.execute(sql, params)
    con.commit()


#: ``backfill_markers.source`` 값들 — "이 소스로 조회해봤고 자료가 없더라".
#: 새 백필 소스는 여기에 문자열 하나만 더하면 된다(스키마 변경 없음).
CHECKED_NAVER_BARS = "naver_bars"
CHECKED_NAVER_FLOW = "naver_flow"
CHECKED_DART_SHARES = "dart_shares_delisted"
CHECKED_DART_SHARES_LISTED = "dart_shares_listed"


def mark_checked(con: Any, source: str, codes: list[str], today: str) -> None:
    """``codes`` 에 "이 소스로 조회해봤고 자료가 없더라" 마커를 남긴다.

    마커가 없으면 자료가 없는 코드는 결과 행이 안 생겨 제외 조건에도 안 걸리고
    **영원히 재조회된다.** 상장폐지는 과거 사실이라 한 번 없으면 영원히 없고,
    상장 종목의 상장 이전 연도도 마찬가지다.

    ``(code, source)`` 키라 새 소스가 문자열 하나로 끝난다 — 예전처럼
    ``delisted_stocks`` 에 컬럼을 붙이지 않는다. 그 방식은 소스마다 마이그레이션·
    컬럼·함수 사본·대상쿼리 절이 같이 늘었고, 무엇보다 **상장 종목에는 쓸 수가
    없었다**(그 테이블에 상장 종목이 없다).
    """
    if not codes:
        return
    ph = "%s" if _is_pg(con) else "?"
    rows = [(c, source, today) for c in codes]
    if _is_pg(con):
        import psycopg2.extras  # noqa: PLC0415

        with con.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                "INSERT INTO backfill_markers(code,source,checked_date) VALUES %s "
                "ON CONFLICT (code,source) DO UPDATE SET checked_date=EXCLUDED.checked_date",
                rows, page_size=max(len(rows), 100))
    else:
        con.executemany(
            f"INSERT OR REPLACE INTO backfill_markers(code,source,checked_date) "
            f"VALUES({ph},{ph},{ph})", rows)
    con.commit()


def checked_codes(con: Any, source: str) -> set[str]:
    """``source`` 로 이미 조회해봤고 자료가 없던 코드 집합."""
    return {r[0] for r in fetchall(
        con, "SELECT code FROM backfill_markers WHERE source=?", (source,))}


def universe_query(*, all_codes: bool, top_n: int) -> tuple[str, dict]:
    """수집 대상 종목 유니버스를 고르는 SQL(+파라미터).

    ``all_codes`` 는 ``daily_bars`` 전 종목을 되돌린다 — 최근성 창이 없으므로 오늘
    상장한 종목도 첫날부터 포함된다(신규 상장 특수처리가 필요 없는 이유).
    아니면 최근 90일 평균 거래대금 상위 ``top_n``.

    ``dart_earnings`` 와 ``naver_consensus`` 가 이 문자열을 각자 한 벌씩 들고 있었다.
    유동성 창(90일)과 ADV 정의는 연구 결정이라 한 군데에만 있어야 한다.
    """
    if all_codes:
        return "SELECT DISTINCT code FROM daily_bars ORDER BY code", {}
    return (
        "SELECT code FROM daily_bars "
        "WHERE date >= (SELECT MAX(date) FROM daily_bars) - INTERVAL '90 days' "
        "GROUP BY code ORDER BY AVG(trade_value) DESC LIMIT %(n)s",
        {"n": top_n},
    )


#: 한 INSERT 문에 담을 최대 행 수. execute_values 는 페이지 하나를 통째로 하나의
#: SQL 문자열로 mogrify 하므로, 이 값이 곧 클라이언트 메모리의 상한이다.
_UPSERT_PAGE_SIZE = 1000


def _upsert(
    con: Any,
    table: str,
    cols: list[str],
    records: list[tuple],
    *,
    pk_cols: tuple[str, ...] = ("code", "date"),
    on_conflict: str = "update",
) -> int:
    """Insert/replace ``records`` (tuples ordered by ``cols``) into ``table``.

    sqlite: ``INSERT OR REPLACE``. Postgres: ``INSERT ... ON CONFLICT DO
    UPDATE`` on ``pk_cols`` — same natural-key upsert semantics either way.

    ``on_conflict="nothing"`` 은 기존 행을 보존한다. 폐지 종목 시세 백필처럼
    "이미 있으면 그게 더 신뢰할 수 있는 값"인 경우에 쓴다 — 키움 실측 거래대금을
    네이버 근사치로 덮어쓰지 않기 위함(``naver_delisted_bars``).
    """
    if on_conflict not in ("update", "nothing"):
        raise ValueError(f"on_conflict must be 'update' or 'nothing', got {on_conflict!r}")
    if not records:
        return 0
    if _is_pg(con):
        import psycopg2.extras  # noqa: PLC0415 — optional dep, only needed for this path

        update_cols = [c for c in cols if c not in pk_cols]
        if on_conflict == "nothing" or not update_cols:
            # update_cols가 비면(모든 컬럼이 pk_cols인 순수 연결 테이블, 예:
            # news_article_tickers) "DO UPDATE SET " 뒤에 대입식이 하나도 안
            # 남아 SQL 자체가 깨진다("syntax error at end of input", 2026-09-06
            # daily_news collect_toss_news 실측) — 갱신할 컬럼이 없으니
            # DO NOTHING이 의미상으로도 맞다.
            action = "DO NOTHING"
        else:
            action = "DO UPDATE SET " + ",".join(f"{c}=EXCLUDED.{c}" for c in update_cols)
        sql = (
            f"INSERT INTO {table}({','.join(cols)}) VALUES %s "
            f"ON CONFLICT ({','.join(pk_cols)}) {action}"
        )
        try:
            with con.cursor() as cur:
                # 페이지마다 rowcount 를 **누적**한다. execute_values 에 전체를 한
                # 문장으로 넘기면(page_size=len(records)) rowcount 는 정확하지만
                # 클라이언트가 records 전체를 하나의 SQL 문자열로 mogrify 하므로,
                # 큰 백필(예: sync_to_timescale --days 3650)에서 수 GB 짜리 문장이
                # 만들어진다. 페이지를 두되 각 페이지의 rowcount 를 더하면 둘 다
                # 지킨다 — 기본값(100)에 그냥 맡기면 마지막 배치만 반영돼
                # "13,316행 기록"이라 보고해놓고 실제로는 0행인 일이 생긴다(실측).
                affected = 0
                for start in range(0, len(records), _UPSERT_PAGE_SIZE):
                    page = records[start:start + _UPSERT_PAGE_SIZE]
                    psycopg2.extras.execute_values(cur, sql, page, page_size=len(page))
                    affected += cur.rowcount
        except Exception:
            # A failed statement leaves the whole Postgres transaction aborted
            # until rolled back — without this, every later upsert on this
            # connection fails with InFailedSqlTransaction even for unrelated,
            # valid records (cascading one bad row into the entire run).
            con.rollback()
            raise
    else:
        placeholders = ",".join(["?"] * len(cols))
        verb = "INSERT OR IGNORE" if on_conflict == "nothing" else "INSERT OR REPLACE"
        sql = f"{verb} INTO {table}({','.join(cols)}) VALUES({placeholders})"
        cur = con.executemany(sql, records)
        affected = cur.rowcount
    con.commit()
    # DO NOTHING/IGNORE 에서는 실제 삽입 수가 len(records) 보다 적다. len 을 돌려주면
    # "13,316행 기록"이라 보고해놓고 실제로는 0행인 일이 생긴다(실측).
    return affected if on_conflict == "nothing" else len(records)


def to_int(s: object) -> int:
    """Kiwoom numeric strings (``'+322500'``, ``'-1979879'``, ``''``) → int."""
    text = str(s or "").replace("+", "").strip()
    try:
        return int(text)
    except ValueError:
        return 0


def to_float(s: object) -> float:
    text = str(s or "").replace("+", "").strip()
    try:
        return float(text)
    except ValueError:
        return 0.0


def to_float_or_none(s: object) -> float | None:
    """콤마 섞인 숫자 문자열 → float, 파싱 불가면 ``None``.

    :func:`to_float` 와 달리 실패를 ``0.0`` 이 아니라 ``None`` 으로 돌려준다 —
    "값이 0" 과 "값이 없음" 이 다른 재무·컨센서스 필드용이다(0.0 으로 뭉개면
    커버리지 없는 종목이 목표주가 0원으로 보인다). dart_earnings 와
    naver_consensus 가 같은 6줄을 한 벌씩 들고 있었다.
    """
    text = str(s or "").replace(",", "").strip()
    try:
        return float(text)
    except ValueError:
        return None


def date_days_ago(days: int) -> str:
    """``days`` 일 전 날짜를 ``YYYYMMDD`` 로 — **항상 날짜를 돌려준다**.

    콜렉터 대여섯 곳이 ``time.strftime("%Y%m%d", time.localtime(time.time() -
    days * 86400))`` 을 각자 썼다. ``date`` 산술로 바꾸면 86400 고정 곱에 있던
    DST 인접 오차도 사라진다.

    ``days == 0`` 은 오늘이다. "창 없음" 을 빈 문자열로 신호하고 싶으면
    :func:`days_ago` 를 쓴다 — **두 규약을 섞으면 안 된다.** 원래 호출부 여섯 곳
    중 넷은 ``if days > 0 else ""`` 로 감싸고 있었고 둘(`short_credit.cutoff`,
    `combined.sd_cutoff`)은 감싸지 않았다. 그 둘에 빈 문자열 규약을 잘못 적용하면
    ``--days 0`` 에서 창 필터가 통째로 풀리고, Postgres 는 ``date >= ''`` 를
    ``invalid input syntax for type date`` 로 거절한다.
    """
    return (date.today() - timedelta(days=days)).strftime("%Y%m%d")


def days_ago(days: int) -> str:
    """``days`` 일 전 날짜, 단 ``days <= 0`` 이면 빈 문자열(=창 없음).

    "N일치만 보관" 처럼 0 을 "전량"으로 읽는 호출부용이다. 항상 날짜가 필요하면
    :func:`date_days_ago` 를 쓴다.
    """
    return date_days_ago(days) if days > 0 else ""


def progress_line(i: int, total: int, started: float, stats: dict[str, int], detail: str) -> str:
    """전종목 스윕의 진행 로그 한 줄 — ``[i/n] done= skip= fail= | detail | 속도 | ETA``.

    여섯 콜렉터가 같은 elapsed/rate/ETA 산술을 한 벌씩 들고 있었고 다른 건 가운데
    ``detail`` 조각뿐이었다. ``started`` 는 ``time.monotonic()`` 기준값이다.
    """
    elapsed = time.monotonic() - started
    rate = i / elapsed if elapsed else 0
    eta = (total - i) / rate / 60 if rate else 0
    return (f"  [{i}/{total}] done={stats['done']} skip={stats['skipped']} "
            f"fail={stats['failed']} | {detail} | {rate:.1f} stk/s | ETA {eta:.1f}m")


_STOCKS_COLS = ["code", "name", "market", "sector", "kind"]


def upsert_stocks(con: Any, stocks: list[dict]) -> int:
    """Insert/replace stock master rows. Returns the number written."""
    records = [tuple(s.get(c) for c in _STOCKS_COLS) for s in stocks]
    return _upsert(con, "stocks", _STOCKS_COLS, records, pk_cols=("code",))


def upsert_supply_demand(con: Any, records: list[tuple]) -> int:
    """Insert/replace supply_demand rows (tuples ordered by SUPPLY_DEMAND_COLUMNS)."""
    return _upsert(con, "supply_demand", SUPPLY_DEMAND_COLUMNS, records)


def upsert_daily_bars(con: Any, records: list[tuple]) -> int:
    """Insert/replace daily_bars rows (tuples ordered by DAILY_BAR_COLUMNS)."""
    return _upsert(con, "daily_bars", DAILY_BAR_COLUMNS, records)


_SHORT_SELLING_COLS = [
    "code", "date", "close", "volume",
    "short_qty", "short_balance", "short_ratio", "short_avg_price", "short_value",
]

_CREDIT_BALANCE_COLS = [
    "code", "date", "close",
    "new_qty", "repay_qty", "balance_qty", "balance_amt", "balance_rt", "credit_rt",
]


def upsert_short_selling(con: Any, records: list[tuple]) -> int:
    """Insert/replace short_selling rows."""
    return _upsert(con, "short_selling", _SHORT_SELLING_COLS, records)


def upsert_credit_balance(con: Any, records: list[tuple]) -> int:
    """Insert/replace credit_balance rows."""
    return _upsert(con, "credit_balance", _CREDIT_BALANCE_COLS, records)


_SECTOR_INDEX_COLS = [
    "code", "name", "date", "open", "high", "low", "close", "volume", "trade_value",
]


def upsert_sector_index(con: Any, records: list[tuple]) -> int:
    """Insert/replace sector_index rows."""
    return _upsert(con, "sector_index", _SECTOR_INDEX_COLS, records)


_SHARES_OUTSTANDING_COLS = ["code", "date", "shares_outstanding"]
_SHARES_OUTSTANDING_COLS_SOURCED = [*_SHARES_OUTSTANDING_COLS, "source"]


def upsert_shares_outstanding(
    con: Any, records: list[tuple], *, source: str | None = None
) -> int:
    """Insert/replace shares_outstanding_history rows.

    ``source`` 를 주면 4번째 컬럼으로 함께 쓴다. 안 주면 DDL 기본값
    ``'kiwoom'`` 이 박힌다 — **그래서 KRX 로 받은 행이 kiwoom 으로 기록됐다.**
    실측: `SELECT source, count(*)` 가 `kiwoom 28,892 / dart 2,013 / krx 0` 인데
    krx 수집기는 분명히 이 함수를 부르고 있었다. 어느 소스에서 왔는지 모르면
    소스별 신뢰도·백필 범위를 구분할 수 없다.
    """
    if source is not None:
        return _upsert(
            con, "shares_outstanding_history", _SHARES_OUTSTANDING_COLS_SOURCED,
            [(*r, source) for r in records],
        )
    return _upsert(con, "shares_outstanding_history", _SHARES_OUTSTANDING_COLS, records)


_EARNINGS_COLS = [
    "code", "period", "avail_date", "knowledge_date",
    "netinc", "netinc_prior", "revenue", "revenue_prior", "op_income", "op_income_prior",
]


_EARNINGS_KEY_COLS = ("code", "period", "knowledge_date")
_EARNINGS_VALUE_COLS = [c for c in _EARNINGS_COLS if c not in _EARNINGS_KEY_COLS]


def _latest_earnings(con: Any, periods: set[str]) -> dict[tuple[str, str], tuple]:
    """``{(code, period): value tuple}`` for the newest knowledge_date of each key."""
    if not periods:
        return {}
    ph = ",".join(["%s" if _is_pg(con) else "?"] * len(periods))
    sql = (
        f"SELECT code, period, {','.join(_EARNINGS_VALUE_COLS)} FROM earnings e "
        f"WHERE period IN ({ph}) AND knowledge_date = "
        "(SELECT MAX(knowledge_date) FROM earnings x WHERE x.code = e.code AND x.period = e.period)"
    )
    params = tuple(sorted(periods))
    if _is_pg(con):
        with con.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
    else:
        rows = con.execute(sql, params).fetchall()
    return {(r[0], r[1]): tuple(r[2:]) for r in rows}


def upsert_earnings(con: Any, records: list[tuple]) -> int:
    """Insert earnings rows (tuples ordered by _EARNINGS_COLS), versioned by knowledge_date.

    The key includes ``knowledge_date``, so a DART restatement collected later lands
    as a new row instead of overwriting what was known at the time — a backtest can
    then ask what was knowable on a given date rather than what is known now.

    Only *changed* figures are versioned: daily_earnings re-fetches the two most
    recent quarters for every code each weekday, and versioning those unchanged
    re-reads would add ~2,600 identical rows per quarter per day. Returns the number
    of rows actually written.
    """
    if not records:
        return 0
    idx = {c: i for i, c in enumerate(_EARNINGS_COLS)}
    latest = _latest_earnings(con, {r[idx["period"]] for r in records})
    fresh = [
        r for r in records
        if latest.get((r[idx["code"]], r[idx["period"]]))
        != tuple(r[idx[c]] for c in _EARNINGS_VALUE_COLS)
    ]
    return _upsert(con, "earnings", _EARNINGS_COLS, fresh, pk_cols=_EARNINGS_KEY_COLS)


_CONSENSUS_COLS = [
    "code", "date", "target_mean", "recomm_mean", "base_date", "fwd_eps", "prev_eps", "est_year",
]


def upsert_consensus(con: Any, records: list[tuple]) -> int:
    """Insert/replace consensus rows (tuples ordered by _CONSENSUS_COLS)."""
    return _upsert(con, "consensus", _CONSENSUS_COLS, records)


def upsert_daily_bars_adjusted(con: Any, records: list[tuple]) -> int:
    """Insert/replace daily_bars_adjusted rows (tuples ordered by DAILY_BAR_COLUMNS)."""
    return _upsert(con, "daily_bars_adjusted", DAILY_BAR_COLUMNS, records)


_DELISTED_STOCKS_COLS = ["code", "name", "market", "last_trade_date"]


def upsert_delisted_stocks(con: Any, records: list[tuple]) -> int:
    """Insert/replace delisted_stocks rows (tuples ordered by _DELISTED_STOCKS_COLS)."""
    return _upsert(con, "delisted_stocks", _DELISTED_STOCKS_COLS, records, pk_cols=("code",))


_NEWS_ARTICLE_COLS = [
    "id", "source", "category", "title", "url",
    "content", "summary", "author", "published_at", "collected_at",
]


def upsert_news_articles(con: Any, records: list[tuple]) -> int:
    """Insert/replace news_articles rows (tuples ordered by _NEWS_ARTICLE_COLS).

    ``id`` (source + url hash) is globally unique already, but Postgres side
    (init_timescale.sql) needs ``published_at`` in the PK too — it's the
    hypertable partition column and Timescale rejects a unique constraint that
    excludes it. Passing both here keeps sqlite/Postgres behavior identical:
    the same article re-scraped (same id) always updates the same row instead
    of piling up duplicates, which is the bug this table exists to avoid
    (krx-news-rest-api's Redis cache used the full article JSON as the dedup
    key, so ``collected_at`` alone made every re-crawl look like a new row).
    """
    return _upsert(con, "news_articles", _NEWS_ARTICLE_COLS, records, pk_cols=("id", "published_at"))


_NEWS_ARTICLE_TICKERS_COLS = ["article_id", "ticker"]


def upsert_news_article_tickers(con: Any, records: list[tuple]) -> int:
    """Insert/replace news_article_tickers rows (tuples ordered by (article_id, ticker))."""
    return _upsert(
        con, "news_article_tickers", _NEWS_ARTICLE_TICKERS_COLS, records,
        pk_cols=("article_id", "ticker"),
    )


_DISCLOSURES_COLS = [
    "id", "source", "title", "url", "company", "ticker",
    "disclosure_type", "published_at", "collected_at",
]


def upsert_disclosures(con: Any, records: list[tuple]) -> int:
    """Insert/replace disclosures rows (tuples ordered by _DISCLOSURES_COLS).

    ``pk_cols`` includes ``published_at`` for the Postgres path — Timescale's
    hypertable partition column (migrations/011) can't be excluded from the
    upsert's conflict target. sqlite ignores ``pk_cols`` here and upserts on
    the table's own ``id``-only PRIMARY KEY (see SCHEMA above), matching
    :func:`upsert_news_articles`.
    """
    return _upsert(con, "disclosures", _DISCLOSURES_COLS, records, pk_cols=("id", "published_at"))


_NEWS_JUDGMENTS_COLS = [
    "source_type", "source_id", "ticker", "event_type", "sentiment_direction",
    "related_codes", "is_stale_repeat", "first_seen_date", "price_impact_likely",
    "rationale", "model_id", "prompt_version", "knowledge_date",
    "confidence", "judged_at",
]


def upsert_news_judgments(con: Any, records: list[tuple]) -> int:
    """Insert news_judgments rows (tuples ordered by _NEWS_JUDGMENTS_COLS).

    ``on_conflict="nothing"`` — 판단은 한 번 쓰면 불변이다. 재실행 시 같은
    (source_type, source_id, ticker, prompt_version)는 기존 값을 그대로
    보존한다(LLM 출력이 확률적이라 재실행마다 값이 달라질 수 있는데, 그걸
    덮어쓰면 "그 시점에 실제로 어떤 판단이 있었는지"라는 point-in-time
    기록이 흔들린다).
    """
    return _upsert(con, "news_judgments", _NEWS_JUDGMENTS_COLS, records,
                   pk_cols=("source_type", "source_id", "ticker", "prompt_version"),
                   on_conflict="nothing")

