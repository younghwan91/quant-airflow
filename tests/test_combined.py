"""collectors/combined.py + daily_bars.py — 증분 경계가 실제로 걸리는가.

`daily_collection_catchup` 은 "전종목이 이미 최신이면 몇 초 안에 끝난다" 는
전제로 매일 10:05 에 돈다. 2026-08-24 실측은 정반대였다 — 2,924초, 즉 16:00
본 수집(2,918초)의 완전한 중복. 원인이 둘이고 여기서 둘 다 막는다:

1. `_market_latest_date` 가 장중 **진행 중인 오늘 캔들**을 최신 거래일로 잡아,
   전날 수집분이 전 종목에서 "낡음" 판정을 받았다(skip=0).
2. 수급(ka10059)에 증분 가드가 아예 없어, 새 데이터가 존재할 수 없는 일요일에도
   175,266행을 다시 썼다.
"""

from __future__ import annotations

import sqlite3

import pytest


# --------------------------------------------------- 진행 중 캔들을 세지 않는다


class _FakeChart:
    """ka10081 흉내 — 첫 행이 오늘(진행 중), 그 다음이 완료된 거래일."""

    def __init__(self, rows):
        self._rows = rows
        self.calls = 0

    def stock_daily_chart(self, **kwargs):
        self.calls += 1
        return {"stk_dt_pole_chart_qry": self._rows}


class _FakeApi:
    def __init__(self, chart):
        self.chart = chart


def _rows(*dates):
    return [{"dt": d, "open_pric": "1", "high_pric": "1", "low_pric": "1",
             "cur_prc": "1", "trde_qty": "1", "trde_prica": "1"} for d in dates]


def test_intraday_probe_ignores_todays_unfinished_candle(monkeypatch):
    """10:05 에 도는 catchup 이 오늘 봉을 최신 거래일로 삼으면 스킵이 안 걸린다."""
    import collectors.daily_bars as db

    monkeypatch.setattr(db.time, "strftime", lambda fmt: {
        "%Y%m%d": "20260824", "%H%M": "1005"}[fmt])
    api = _FakeApi(_FakeChart(_rows("20260824", "20260821")))

    assert db._market_latest_date(api, "20260824") == "20260821"


def test_after_the_close_todays_bar_counts(monkeypatch):
    """16:00 본 수집은 오늘 봉이 확정이므로 그대로 최신 거래일이다."""
    import collectors.daily_bars as db

    monkeypatch.setattr(db.time, "strftime", lambda fmt: {
        "%Y%m%d": "20260824", "%H%M": "1600"}[fmt])
    api = _FakeApi(_FakeChart(_rows("20260824", "20260821")))

    assert db._market_latest_date(api, "20260824") == "20260824"


def test_probe_falls_back_to_today_when_the_call_fails(monkeypatch):
    """모르면 전부 받는다 — 조용히 스킵하는 것보다 낫다."""
    import collectors.daily_bars as db

    class _Boom:
        def stock_daily_chart(self, **kwargs):
            raise RuntimeError("vendor down")

    monkeypatch.setattr(db.time, "strftime", lambda fmt: {
        "%Y%m%d": "20260824", "%H%M": "1005"}[fmt])

    assert db._market_latest_date(_FakeApi(_Boom()), "20260824") == "20260824"


# ------------------------------------------------------------ 수급 증분 경계


@pytest.fixture
def con():
    c = sqlite3.connect(":memory:")
    c.execute("CREATE TABLE daily_bars (code TEXT, date TEXT, open INT, high INT,"
              " low INT, close INT, volume INT, trade_value INT,"
              " PRIMARY KEY (code, date))")
    c.execute("CREATE TABLE supply_demand (code TEXT, date TEXT,"
              " PRIMARY KEY (code, date))")
    return c


def test_sd_latest_date_reads_the_supply_demand_table(con):
    from collectors.supply_demand import _latest_sd_date as _sd_latest_date

    con.execute("INSERT INTO supply_demand VALUES ('005930', '20260821')")

    assert _sd_latest_date(con, "005930") == "20260821"
    assert _sd_latest_date(con, "000660") is None


def test_a_stock_current_in_both_tables_costs_zero_api_calls(con, monkeypatch):
    """일요일 catchup 이 48.6분을 태우던 경로 — 이제 요청이 0이어야 한다."""
    import collectors.combined as cb

    con.execute("INSERT INTO daily_bars VALUES"
                " ('005930','20260821',1,1,1,1,1,1)")
    con.execute("INSERT INTO supply_demand VALUES ('005930','20260821')")

    chart = _FakeChart(_rows("20260821"))
    sd_calls = []

    class _StockInfo:
        def investor_institution_by_stock(self, **kwargs):
            sd_calls.append(kwargs)
            return {"stk_invsr_orgn": []}

    api = _FakeApi(chart)
    api.stock_info = _StockInfo()
    monkeypatch.setattr(cb, "_market_latest_date", lambda *a, **k: "20260821")

    stats = cb.collect(api, con, [{"code": "005930", "name": "삼성전자"}], update=True)

    assert stats["skipped"] == 1
    assert chart.calls == 0, "일봉을 다시 받았다"
    assert sd_calls == [], "수급을 다시 받았다 — 가드가 안 걸렸다"


def test_a_stock_stale_only_in_supply_demand_fetches_only_that_tr(con, monkeypatch):
    """한쪽만 낡았으면 그쪽 TR 만 부른다 — 둘을 묶으면 절반이 낭비다."""
    import collectors.combined as cb

    con.execute("INSERT INTO daily_bars VALUES"
                " ('005930','20260821',1,1,1,1,1,1)")  # 일봉은 최신, 수급은 없음

    chart = _FakeChart(_rows("20260821"))
    sd_calls = []

    class _StockInfo:
        def investor_institution_by_stock(self, **kwargs):
            sd_calls.append(kwargs)
            return {"stk_invsr_orgn": []}

    api = _FakeApi(chart)
    api.stock_info = _StockInfo()
    monkeypatch.setattr(cb, "_market_latest_date", lambda *a, **k: "20260821")

    cb.collect(api, con, [{"code": "005930", "name": "삼성전자"}], update=True)

    assert chart.calls == 0
    assert len(sd_calls) == 1


def test_an_unfinished_candle_never_reaches_daily_bars(con, monkeypatch):
    """부분 봉이 확정 일봉으로 적재되면 16:00 전까지 읽는 쪽이 그걸 믿는다."""
    import collectors.combined as cb

    chart = _FakeChart(_rows("20260824", "20260821"))  # 첫 행이 진행 중

    class _StockInfo:
        def investor_institution_by_stock(self, **kwargs):
            return {"stk_invsr_orgn": []}

    api = _FakeApi(chart)
    api.stock_info = _StockInfo()
    monkeypatch.setattr(cb, "_market_latest_date", lambda *a, **k: "20260821")

    cb.collect(api, con, [{"code": "005930", "name": "삼성전자"}], update=True)

    stored = [r[0] for r in con.execute("SELECT date FROM daily_bars").fetchall()]
    assert "20260824" not in stored, "진행 중 캔들이 적재됐다"
    assert "20260821" in stored


def test_currency_check_costs_two_queries_not_two_per_stock(con, monkeypatch):
    """종목별 MAX(date) 는 건당 845ms 다 — 청크 515개를 가로지른다.

    실측 2026-08-26 catchup: `done=0 skip=2628` 로 API 호출이 0이었는데도
    16분이 걸렸다. 그 16분이 전부 이 조회였다. 집합 두 개로 받으면 47ms 다.
    """
    import collectors.combined as cb

    stocks = [{"code": f"{i:06d}", "name": f"종목{i}"} for i in range(300)]
    for s in stocks:
        con.execute("INSERT INTO daily_bars VALUES (?,'20260821',1,1,1,1,1,1)", (s["code"],))
        con.execute("INSERT INTO supply_demand VALUES (?,'20260821')", (s["code"],))
    con.commit()

    # 프록시로 감싸면 `_is_pg` 가 sqlite 를 Postgres 로 오인한다(isinstance 판정).
    # sqlite3 의 trace 콜백으로 실제 실행된 SQL 만 센다.
    seen: list[str] = []
    con.set_trace_callback(seen.append)
    monkeypatch.setattr(cb, "_market_latest_date", lambda *a, **k: "20260821")

    class _StockInfo:
        def investor_institution_by_stock(self, **kw):
            raise AssertionError("최신인데 API 를 불렀다")

    api = _FakeApi(_FakeChart(_rows("20260821")))
    api.stock_info = _StockInfo()

    stats = cb.collect(api, con, stocks, update=True)
    con.set_trace_callback(None)

    assert stats["skipped"] == 300
    lookups = [q for q in seen if "SELECT DISTINCT code" in q]
    assert len(lookups) == 2, f"종목 수와 무관하게 2회여야 한다 (실제 {len(lookups)}회)"
    assert not [q for q in seen if "MAX(date)" in q], "종목별 MAX(date) 가 남아 있다"
