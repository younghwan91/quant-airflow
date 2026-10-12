"""키움 공매도·신용·프로그램 과거 백필 — 033 (네트워크 불요)."""

from __future__ import annotations

from collectors import flow_history as fh


def test_short_windows_cover_range_newest_first_without_gaps():
    w = fh.short_windows("20160901", "20180101", days=300)
    assert w[0][1] == "20180101" and w[-1][0] == "20160901"
    for (lo, _hi), (_lo2, hi2) in zip(w, w[1:]):
        assert fh._prev_day(lo) == hi2


def test_walk_back_moves_anchor_to_day_before_oldest_and_stops_at_since():
    pages = {"20170110": [{"dt": "20170109"}, {"dt": "20170105"}],
             "20170104": [{"dt": "20170103"}, {"dt": "20161228"}, {"dt": "20161201"}]}
    calls = []

    def call(dt):
        calls.append(dt)
        return pages.get(dt, [])

    rows = fh.walk_back(call, "20161215", "20170110")
    assert calls == ["20170110", "20170104"]
    assert [r["dt"] for r in rows] == ["20170109", "20170105", "20170103", "20161228"]


def test_walk_back_stops_when_listing_start_reached():
    assert fh.walk_back(lambda dt: [{"dt": "20200102"}] if dt == "20200110" else [], "20160901", "20200110") \
        == [{"dt": "20200102"}]


def test_build_program_records_parses_signed_and_skips_today():
    rows = [{"dt": "20161229", "cur_prc": "+1802000", "trde_qty": "150329", "prm_sell_amt": "70453",
             "prm_buy_amt": "86484", "prm_netprps_amt": "+16031", "prm_sell_qty": "39309", "prm_buy_qty": "48155",
             "prm_netprps_qty": "-8846", "stex_tp": "KRX"},
            {"dt": "20261013", "cur_prc": "1"}]
    assert fh.build_program_records("005930", rows, "20261013") == [
        ("005930", "20161229", 1802000, 150329, 70453, 86484, 16031, 39309, 48155, -8846, "KRX", "kiwoom_ka90013")]
