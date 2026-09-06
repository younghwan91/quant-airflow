"""폐지 종목 상장주식수 백필 — 시계열 조립·대상선정·기록 (네트워크 불요).

시가총액의 분모라 틀리면 유니버스 편입이 통째로 어긋난다. fetch/parse(어느
필드를 쓰는지, 보고서 폴백 등)는 krx-fundamentals-client의
``DartScraper.fetch_shares_outstanding``으로 옮겼다(2026-09-06, 그쪽 테스트
스위트가 검증) — 여기서는 이 파일이 여전히 맡는 시계열 조립·키 로테이션·대상
선정·DB 적재만 다룬다.
"""

from __future__ import annotations

import asyncio

from krx_fundamentals_client import DartQuotaExceededError, DartScraper, SharesOutstanding

import collectors.dart_shares as ds
from collectors.storage import CHECKED_DART_SHARES_LISTED, checked_codes, connect, mark_checked


def _run(coro):
    return asyncio.run(coro)


def _shares(ticker, year, shares=5_919_637_922, stlm="20251231", knowledge="20260310"):
    return SharesOutstanding(
        ticker=ticker, year=year, shares_outstanding=shares,
        stlm_dt=stlm, knowledge_date=knowledge,
    )


def test_dashed_converts_yyyymmdd():
    assert ds._dashed("20251231") == "2025-12-31"


def test_dashed_leaves_already_dashed_alone():
    assert ds._dashed("2025-12-31") == "2025-12-31"


def test_series_covers_every_year_of_the_trading_life(monkeypatch):
    """**시계열이어야 하는 이유** — market_cap_asof 는 date <= 조회일 로 찾는다.

    종목당 1점만 있으면(그것도 폐지일보다 뒤인 경우가 실측 35%) 그 종목의 모든
    거래일에서 시총이 NULL 이 된다. 거래 기간을 가로지르는 점들이 있어야 한다.
    """
    async def fake(self, ticker, year, on_status=None):
        return _shares(ticker, year, stlm=f"{year}1231")

    monkeypatch.setattr(DartScraper, "fetch_shares_outstanding", fake)
    scrapers = {"k": DartScraper(api_key="k")}
    got = _run(ds.shares_series(scrapers, ["k"], "005930", 2018, 2021, sleep=0))
    assert [stlm for _, stlm, _ in got] == [
        "2018-12-31", "2019-12-31", "2020-12-31", "2021-12-31"]


def test_series_skips_years_with_no_filing(monkeypatch):
    """일부 연도만 자료가 있어도 있는 연도만 시계열에 담는다."""
    async def fake(self, ticker, year, on_status=None):
        return _shares(ticker, year, stlm=f"{year}0930") if year != 2020 else None

    monkeypatch.setattr(DartScraper, "fetch_shares_outstanding", fake)
    scrapers = {"k": DartScraper(api_key="k")}
    got = _run(ds.shares_series(scrapers, ["k"], "005930", 2019, 2021, sleep=0))
    assert [stlm for _, stlm, _ in got] == ["2019-09-30", "2021-09-30"]


def test_series_empty_when_nothing_is_filed(monkeypatch):
    async def fake(self, ticker, year, on_status=None):
        return None

    monkeypatch.setattr(DartScraper, "fetch_shares_outstanding", fake)
    scrapers = {"k": DartScraper(api_key="k")}
    assert _run(ds.shares_series(scrapers, ["k"], "x", 2019, 2021, sleep=0)) == []


def test_series_dates_are_dashed_for_string_comparison_with_existing_rows(monkeypatch):
    """shares_outstanding_history.date 는 기존 행과 문자열 비교로 as-of 되므로
    라이브러리가 주는 대시 없는 YYYYMMDD를 그대로 적재하면 안 된다."""
    async def fake(self, ticker, year, on_status=None):
        return _shares(ticker, year, stlm="20251231", knowledge="20260310")

    monkeypatch.setattr(DartScraper, "fetch_shares_outstanding", fake)
    scrapers = {"k": DartScraper(api_key="k")}
    got = _run(ds.shares_series(scrapers, ["k"], "005930", 2025, 2025, sleep=0))
    assert got == [(5_919_637_922, "2025-12-31", "2026-03-10")]


def test_shares_series_rotates_to_next_key_on_quota(monkeypatch):
    """일한도(020)를 만나면 다음 키로 넘어간다 — 예전엔 keys[0] 하나뿐이었다.

    그 제약이 상장분 백필(2,595종목 × ~9.5콜)을 하루 한도 밖으로 밀어냈고,
    이틀로 나눈 분할이 정렬과 겹쳐 시대별로 기울어진 중간 상태를 만들었다.
    """
    seen = []

    async def fake(self, ticker, year, on_status=None):
        seen.append(self.api_key)
        if self.api_key == "k1":
            raise DartQuotaExceededError("한도초과")
        return _shares(ticker, year)

    monkeypatch.setattr(DartScraper, "fetch_shares_outstanding", fake)
    scrapers = {"k1": DartScraper(api_key="k1"), "k2": DartScraper(api_key="k2")}
    ki = [0]
    out = _run(ds.shares_series(scrapers, ["k1", "k2"], "005930", 2020, 2020, ki=ki, sleep=0))

    assert seen[:2] == ["k1", "k2"]     # 소진 즉시 다음 키로
    assert ki == [1]                    # 인덱스가 유지된다(다음 종목은 k2 로 시작)
    assert out and out[0][0] == 5_919_637_922


def test_shares_series_key_index_is_shared_across_calls(monkeypatch):
    """ki 를 넘기면 종목 간에 유지된다 — 안 그러면 종목마다 소진된 키를 또 친다."""
    seen = []

    async def fake(self, ticker, year, on_status=None):
        seen.append(self.api_key)
        if self.api_key == "k1":
            raise DartQuotaExceededError("한도초과")
        return _shares(ticker, year)

    monkeypatch.setattr(DartScraper, "fetch_shares_outstanding", fake)
    scrapers = {"k1": DartScraper(api_key="k1"), "k2": DartScraper(api_key="k2")}
    ki = [0]
    _run(ds.shares_series(scrapers, ["k1", "k2"], "a", 2020, 2020, ki=ki, sleep=0))
    seen.clear()
    _run(ds.shares_series(scrapers, ["k1", "k2"], "b", 2020, 2020, ki=ki, sleep=0))
    assert "k1" not in seen        # 두 번째 종목은 소진된 키를 다시 치지 않는다


def test_fetch_no_rotation_when_single_key_limited(monkeypatch):
    """키 하나뿐인데 020이면 로테이션 불가 → 빈 결과(스킵), 무한루프 없음."""
    async def always_exhausted(self, ticker, year, on_status=None):
        raise DartQuotaExceededError("한도초과")

    monkeypatch.setattr(DartScraper, "fetch_shares_outstanding", always_exhausted)
    scrapers = {"only": DartScraper(api_key="only")}
    ki = [0]
    assert _run(ds.shares_series(scrapers, ["only"], "005930", 2020, 2020, ki=ki, sleep=0)) == []
    assert ki[0] == 0


def test_targets_skip_codes_that_already_have_shares(tmp_path):
    """재실행 안전 — 이미 주식수가 있는 코드는 DART 를 다시 부르지 않는다."""
    con = connect(tmp_path / "t.db")
    bars = [("A", "2020-01-02", 1, 1, 1, 1, 1, 1, "naver"),
            ("B", "2020-01-02", 1, 1, 1, 1, 1, 1, "naver"),
            ("C", "2020-01-02", 1, 1, 1, 1, 1, 1, "kiwoom")]
    con.executemany(
        "INSERT INTO daily_bars(code,date,open,high,low,close,volume,trade_value,source)"
        " VALUES(?,?,?,?,?,?,?,?,?)", bars)
    con.execute("INSERT INTO shares_outstanding_history(code,date,shares_outstanding)"
                " VALUES('A','2019-12-31',100)")
    con.commit()

    got = ds._targets(con)
    assert [r[0] for r in got] == ["B"], "A=이미 있음, C=상장 종목이라 대상 아님"
    assert got[0][1] and got[0][2], "거래 구간(first, last)이 함께 와야 한다"
    con.close()


def test_write_preserves_existing_rows(tmp_path):
    """기존 키움/KRX 행을 DART 값으로 덮어쓰면 안 된다."""
    con = connect(tmp_path / "t.db")
    con.execute("INSERT INTO shares_outstanding_history"
                "(code,date,shares_outstanding,source) VALUES('A','2020-01-02',100,'kiwoom')")
    con.commit()
    ds._write(con, [("A", "2020-01-02", 999, "2020-03-10", "dart"),
                    ("A", "2021-01-02", 200, "2021-03-10", "dart")])
    rows = dict(con.execute(
        "SELECT date, shares_outstanding FROM shares_outstanding_history").fetchall())
    assert rows["2020-01-02"] == 100, "기존 행이 덮였다"
    assert rows["2021-01-02"] == 200
    con.close()


def test_targets_skip_codes_already_marked_as_having_no_dart_data(tmp_path):
    """42종목이 매주 2.2분을 성과 0행으로 태우던 경로 — 한 번 없으면 영원히 없다."""
    con = connect(tmp_path / "t.db")
    con.executemany(
        "INSERT INTO daily_bars(code,date,open,high,low,close,volume,trade_value,source)"
        " VALUES(?,?,?,?,?,?,?,?,?)",
        [("A", "2020-01-02", 1, 1, 1, 1, 1, 1, "naver"),
         ("B", "2020-01-02", 1, 1, 1, 1, 1, 1, "naver")])
    mark_checked(con, ds.CHECKED_DART_SHARES, ["A"], "2026-08-25")

    assert [r[0] for r in ds._targets(con)] == ["B"]
    # --refetch 면 마커를 무시하고 다시 훑는다
    assert [r[0] for r in ds._targets(con, refetch=True)] == ["A", "B"]
    con.close()


def test_codes_with_no_dart_data_get_marked(tmp_path):
    con = connect(tmp_path / "t.db")
    mark_checked(con, ds.CHECKED_DART_SHARES, ["A"], "2026-08-25")

    got = con.execute("SELECT checked_date FROM backfill_markers"
                      " WHERE code='A' AND source=?", (ds.CHECKED_DART_SHARES,)).fetchone()
    assert got[0] == "2026-08-25"
    con.close()


def test_markers_are_per_source_not_per_column(tmp_path):
    """소스가 늘어도 스키마가 안 바뀐다 — 004→007 로 세 번 재발한 그 자리.

    무엇보다 마커가 delisted_stocks 컬럼이던 시절엔 **상장 종목에 쓸 자리가
    없었다**(그 테이블에 상장 종목이 없다). 그래서 --listed 가 자료 없는 60종목을
    매 회차 다시 조회했다.
    """
    con = connect(tmp_path / "t.db")
    mark_checked(con, ds.CHECKED_DART_SHARES, ["A"], "2026-08-25")
    mark_checked(con, CHECKED_DART_SHARES_LISTED, ["B"], "2026-08-28")

    assert checked_codes(con, ds.CHECKED_DART_SHARES) == {"A"}
    assert checked_codes(con, CHECKED_DART_SHARES_LISTED) == {"B"}
    # 같은 코드가 소스별로 독립적으로 기록된다
    mark_checked(con, CHECKED_DART_SHARES_LISTED, ["A"], "2026-08-28")
    assert checked_codes(con, CHECKED_DART_SHARES_LISTED) == {"A", "B"}
    assert checked_codes(con, ds.CHECKED_DART_SHARES) == {"A"}
    con.close()
