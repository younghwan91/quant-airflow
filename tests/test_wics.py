"""WICS 구성 이력 — 주 첫 거래일·섹터/산업군 결합·빈 응답 거절·재개 (네트워크 불요)."""

from __future__ import annotations

import datetime as dt

import pytest

from collectors.storage import connect, upsert_daily_bars
from collectors.wics import Fetcher, collect, snapshot, week_starts

D = dt.date


def test_week_starts_picks_first_trading_day_of_each_iso_week():
    days = [D(2019, 1, 2), D(2019, 1, 3), D(2019, 1, 7), D(2019, 1, 8), D(2019, 1, 15)]
    assert week_starts(days) == [D(2019, 1, 2), D(2019, 1, 7), D(2019, 1, 15)]


class _Resp:
    def __init__(self, body):
        self._body = body

    def raise_for_status(self):
        pass

    def json(self):
        return self._body


def _m(code, sec, idx_nm, sec_nm="IT"):
    return {"CMP_CD": code, "SEC_CD": sec, "SEC_NM_KOR": sec_nm, "IDX_NM_KOR": idx_nm}


class _FakeWise:
    def __init__(self, by_code: dict[str, list[dict]]):
        self.by_code, self.calls = by_code, []

    def __call__(self, url, params):
        self.calls.append((params["dt"], params["sec_cd"]))
        return _Resp({"list": self.by_code.get(params["sec_cd"], [])})


def _fetcher(fake):
    return Fetcher(get=fake, sleep=lambda s: None, clock=lambda: 0.0)


def test_snapshot_joins_sector_and_industry_and_counts_unassigned():
    fake = _FakeWise({
        "G45": [_m("005930", "G45", "WICS IT"), _m("036570", "G45", "WICS IT"), _m("999999", "G45", "WICS IT")],
        "G4530": [_m("005930", "G45", "WICS 반도체와반도체장비")],
        "G4510": [_m("036570", "G45", "WICS 소프트웨어와서비스")],
        "WI600": [_m("036570", "G50", "WI26 소프트웨어")],   # WI26 의 SEC_CD 는 지수 쪽 — 안 쓴다
        "WI330": [_m("999999", "G25", "WI26 미디어,교육")],
        "WI620": [_m("005930", "G45", "WI26 반도체")],
    })
    rows, unassigned = snapshot(_fetcher(fake), D(2019, 1, 2))
    assert rows == [
        ("2019-01-02", "005930", "G45", "IT", "G4530", "반도체와반도체장비", "WI620", "반도체", "wiseindex"),
        ("2019-01-02", "036570", "G45", "IT", "G4510", "소프트웨어와서비스", "WI600", "소프트웨어", "wiseindex"),
        ("2019-01-02", "999999", "G45", "IT", None, None, "WI330", "미디어,교육", "wiseindex"),
    ]
    assert unassigned == 0


def test_snapshot_counts_members_with_neither_grouping():
    fake = _FakeWise({"G45": [_m("005930", "G45", "WICS IT")]})
    _rows, unassigned = snapshot(_fetcher(fake), D(2019, 1, 2))
    assert unassigned == 1


def test_snapshot_rejects_empty_trading_day():
    with pytest.raises(RuntimeError, match="비었다"):
        snapshot(_fetcher(_FakeWise({})), D(2019, 1, 2))


def test_collect_skips_dates_already_written(tmp_path):
    con = connect(tmp_path / "t.db")
    upsert_daily_bars(con, [("005930", d, 1, 1, 1, 1, 1, 1) for d in ("2019-01-02", "2019-01-07", "2019-01-08")])
    fake = _FakeWise({"G45": [_m("005930", "G45", "WICS IT")], "G4530": [_m("005930", "G45", "WICS 반도체")]})
    first = collect(con, _fetcher(fake), since=D(2019, 1, 1), until=D(2019, 1, 31))
    assert first == {"weeks": 2, "already": 0, "written": 2, "rows": 2, "unassigned": 0}
    n = len(fake.calls)
    again = collect(con, _fetcher(fake), since=D(2019, 1, 1), until=D(2019, 1, 31))
    assert again["already"] == 2 and again["written"] == 0 and len(fake.calls) == n
