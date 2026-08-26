"""collectors/short_credit.py — 깊이 기반 스킵과 Postgres 호환.

`weekly_history_backfill` 이 6주 연속 49~50분 붙박이였던 이유는 스킵 기준이
"최근 행이 있나"였기 때문이다. 매일 수집이 최근 구간을 채워두므로 그 기준은
전 종목에서 항상 참이 되고, 정작 백필의 목적인 **깊이**는 아무도 안 봤다.
"""

from __future__ import annotations

import sqlite3

import pytest

from collectors.short_credit import (
    codes_with_history_back_to,
    codes_with_rows_since,
    collect,
)


@pytest.fixture
def con():
    c = sqlite3.connect(":memory:")
    c.execute("CREATE TABLE short_selling (code TEXT, date TEXT,"
              " PRIMARY KEY (code, date))")
    c.execute("CREATE TABLE credit_balance (code TEXT, date TEXT,"
              " PRIMARY KEY (code, date))")
    return c


def test_depth_check_sees_how_far_back_the_history_reaches(con):
    con.execute("INSERT INTO short_selling VALUES ('005930', '20250101')")

    assert "005930" in codes_with_history_back_to(con, "20250601")
    assert "005930" not in codes_with_history_back_to(con, "20240101")


def test_recent_check_cannot_tell_depth(con):
    """왜 깊이 기준이 따로 필요한가 — 최근 기준은 얕은 종목도 통과시킨다."""
    con.execute("INSERT INTO short_selling VALUES ('123456', '20260821')")

    assert "123456" in codes_with_rows_since(con, "20260801")          # 최근엔 있다
    assert "123456" not in codes_with_history_back_to(con, "20251001")  # 깊이는 없다


def _api(calls):
    class _Short:
        def short_selling_trend(self, **kw):
            calls.append(("ss", kw["stk_cd"]))
            return {"shrts_trnsn": []}

    class _Info:
        def credit_trading_trend(self, **kw):
            calls.append(("cb", kw["stk_cd"]))
            return {"crd_trde_trend": []}

    class _Api:
        short_selling = _Short()
        stock_info = _Info()

    return _Api()


def test_deep_stocks_are_skipped_and_shallow_ones_are_not(con):
    """96.8% 가 이미 깊은데 매주 5,090 요청을 다 보내던 경로."""
    con.execute("INSERT INTO short_selling VALUES ('005930', '20240101')")  # 깊다

    calls = []
    stats = collect(
        _api(calls), con,
        [{"code": "005930", "name": "깊음"}, {"code": "123456", "name": "얕음"}],
        days=800, resume_depth=330,
    )

    assert stats["skipped"] == 1
    assert [c for c in calls if c[0] == "ss"] == [("ss", "123456")]


def test_depth_skip_is_off_by_default(con):
    """일일 수집은 깊이를 안 본다 — 기본값이 켜지면 조용히 수집이 준다."""
    con.execute("INSERT INTO short_selling VALUES ('005930', '20240101')")

    calls = []
    stats = collect(_api(calls), con, [{"code": "005930", "name": "깊음"}], days=10)

    assert stats["skipped"] == 0
    assert ("ss", "005930") in calls
