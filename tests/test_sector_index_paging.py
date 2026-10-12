"""ka20006 은 한 페이지만 준다 — ``until`` 이면 가장 오래된 날 전날을 다음 기준일로 거슬러 간다."""

from __future__ import annotations

import datetime as dt

from collectors.sector_index import collect
from collectors.storage import connect


class _Chart:
    def __init__(self):
        self.anchors = []

    def industry_daily_chart(self, inds_cd, base_dt):
        self.anchors.append(base_dt)
        end = dt.datetime.strptime(base_dt, "%Y%m%d")
        rows = [{"dt": (end - dt.timedelta(days=i)).strftime("%Y%m%d"), "cur_prc": "100"} for i in range(3)]
        return {"inds_dt_pole_qry": rows}


class _Api:
    def __init__(self):
        self.chart = _Chart()


def test_walks_back_until_the_until_date():
    api, con = _Api(), connect(":memory:")
    stats = collect(api, con, [{"code": "001", "name": "종합(KOSPI)"}], base_dt="20240110", until="20240103")
    assert api.chart.anchors == ["20240110", "20240107", "20240104"]
    dates = [r[0] for r in con.execute("SELECT date FROM sector_index ORDER BY date")]
    assert dates[0] == "20240103" and dates[-1] == "20240110" and stats["rows"] == 8


def test_default_is_one_page_from_today():
    api, con = _Api(), connect(":memory:")
    collect(api, con, [{"code": "001", "name": "종합(KOSPI)"}])
    assert len(api.chart.anchors) == 1
