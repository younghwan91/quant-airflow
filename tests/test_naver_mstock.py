"""네이버 m.stock 외국인 보유율·순매수·리포트 — 032 (네트워크 불요)."""

from __future__ import annotations

from collectors import naver_mstock as nm
from collectors.storage import connect


def _item(bd, close="1,802,000", ratio="50.76%", indiv="+20,517"):
    return {"itemCode": "005930", "bizdate": bd, "foreignerPureBuyQuant": "+27,137", "foreignerHoldRatio": ratio,
            "organPureBuyQuant": "-48,134", "individualPureBuyQuant": indiv, "closePrice": close,
            "accumulatedTradingVolume": "150,329"}


def test_parse_trend_keeps_unknown_as_null_and_drops_priceless_rows():
    rows = nm.parse_trend([_item("20161229"), _item("20071012", indiv="-"),
                           _item("20161031", close="-", ratio="-")], "005930")
    assert rows == [("005930", "2016-12-29", 1802000, 150329, 50.76, 27137, -48134, 20517, "naver_mstock"),
                    ("005930", "2007-10-12", 1802000, 150329, 50.76, 27137, -48134, None, "naver_mstock")]


def test_fetch_trend_walks_back_with_oldest_bizdate_until_since():
    calls = []

    def get(path):
        calls.append(path)
        if "bizdate" not in path:
            return [_item(f"2008{m:02d}15") for m in (3, 2)] + [_item("20080115")] * 57 + [_item("20080110")]
        return [_item("20080105"), _item("20071228")] + [_item("20071201")] * 58

    rows = nm.fetch_trend("005930", get=get, sleep=0)
    assert calls[1].endswith("bizdate=20080110") and len(calls) == 2
    assert min(r[1] for r in rows) == "2008-01-05"          # 2008 이전 행은 버린다


def test_fetch_trend_stops_on_short_or_empty_page():
    assert nm.fetch_trend("000000", get=lambda p: [], sleep=0) == []
    assert len(nm.fetch_trend("005930", get=lambda p: [_item("20200102")], sleep=0)) == 1


def test_fetch_research_skips_known_and_reads_goal_price():
    lst = [{"researchId": 2, "itemCode": "005930", "itemName": "삼성전자", "brokerName": "DS", "writeDate": "2026-10-12",
            "title": "t", "category": "종목분석"},
           {"researchId": 1, "itemCode": "005930", "writeDate": "2026-10-01"}]
    detail = {"researchContent": {"opinion": "매수", "goalPrice": "530000", "prevGoalPrice": "", "priceAtWriteDate": "268500",
                                  "attachUrl": "u"}}

    def get(path):
        return lst if "page=1" in path else (detail if path.endswith("/2") else None)

    out = nm.fetch_research("005930", {1}, get=get, sleep=0)
    assert out == [(2, "005930", "삼성전자", "DS", "2026-10-12", "t", "종목분석", "매수", 530000, None, 268500, "u",
                    "naver_research")]


def test_run_marks_codes_and_resumes(tmp_path, monkeypatch):
    con = connect(tmp_path / "t.db")
    monkeypatch.setattr(nm, "fetch_trend", lambda code, sleep: nm.parse_trend([_item("20200102")], code))
    assert nm.run(con, "trend", ["005930", "000660"], sleep=0)["rows"] == 2
    assert nm.run(con, "trend", ["005930", "000660"], sleep=0)["skipped"] == 2
    assert con.execute("SELECT foreign_hold_ratio FROM naver_investor_trend WHERE code='000660'").fetchone()[0] == 50.76
