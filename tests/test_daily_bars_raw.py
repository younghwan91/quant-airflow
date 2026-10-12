"""일봉 수집기 — 무수정(daily_bars_raw)·수정(daily_bars) 경로 분리 (네트워크 불요)."""

from __future__ import annotations

from collectors.daily_bars import collect
from collectors.storage import connect


class _Chart:
    def __init__(self):
        self.calls = []

    def stock_daily_chart(self, **kw):
        self.calls.append(kw)
        # 같은 날짜라도 수정/무수정 가격이 다르다(이후 50:1 분할 가정)
        price = "2060000" if kw["upd_stkpc_tp"] == "0" else "41200"
        return {"stk_dt_pole_chart_qry": [
            {"dt": "20170331", "open_pric": price, "high_pric": price, "low_pric": price,
             "cur_prc": price, "trde_qty": "100", "trde_prica": "206"}], "cont_yn": "N"}


class _Api:
    def __init__(self):
        self.chart = _Chart()


def test_raw_writes_unadjusted_prices_to_raw_table_only(tmp_path):
    con = connect(tmp_path / "t.db")
    api = _Api()
    collect(api, con, [{"code": "005930", "name": "삼성전자"}], raw=True, progress_every=1)
    assert api.chart.calls[0]["upd_stkpc_tp"] == "0"
    assert con.execute("SELECT close, source FROM daily_bars_raw").fetchall()[0][:] == (2060000, "kiwoom_raw")
    assert con.execute("SELECT count(*) FROM daily_bars").fetchone()[0] == 0


def test_default_keeps_adjusted_path(tmp_path):
    con = connect(tmp_path / "t.db")
    api = _Api()
    collect(api, con, [{"code": "005930", "name": "삼성전자"}], progress_every=1)
    assert api.chart.calls[0]["upd_stkpc_tp"] == "1"
    assert con.execute("SELECT close FROM daily_bars").fetchone()[0] == 41200
    assert con.execute("SELECT count(*) FROM daily_bars_raw").fetchone()[0] == 0


def test_resume_checks_the_table_being_written(tmp_path):
    con = connect(tmp_path / "t.db")
    api = _Api()
    collect(api, con, [{"code": "005930", "name": "삼성전자"}], progress_every=1)          # daily_bars 에만 있음
    stats = collect(api, con, [{"code": "005930", "name": "삼성전자"}], raw=True, resume=True, progress_every=1)
    assert stats["done"] == 1 and stats["skipped"] == 0          # raw 쪽은 비었으니 건너뛰지 않는다


class _EventChart:
    """005930 50:1 분할 창 — 키움은 이벤트 행에만 upd_stkpc_tp·upd_rt 를 채운다(2026-10-12 프로브)."""

    def stock_daily_chart(self, **kw):
        rows = [
            {"dt": "20180508", "cur_prc": "52600", "upd_stkpc_tp": "", "upd_rt": ""},
            {"dt": "20180504", "cur_prc": "51900", "upd_stkpc_tp": "8", "upd_rt": "-98.00"},
            {"dt": "20180503", "cur_prc": "2650000", "upd_stkpc_tp": "", "upd_rt": ""},
        ]
        return {"stk_dt_pole_chart_qry": rows, "cont_yn": "N"}


def test_events_are_kept_from_the_same_response(tmp_path):
    con = connect(tmp_path / "t.db")
    api = _Api()
    api.chart = _EventChart()
    stats = collect(api, con, [{"code": "005930", "name": "삼성전자"}], raw=True, progress_every=1)
    assert stats["events"] == 1
    rows = con.execute("SELECT code, date, upd_stkpc_tp, upd_rt, kind, source FROM corporate_actions").fetchall()
    assert [tuple(r) for r in rows] == [("005930", "20180504", "8", -98.0, "split", "kiwoom_ka10081")]
    assert con.execute("SELECT count(*) FROM daily_bars_raw").fetchone()[0] == 3


def test_event_kind_decodes_bits_and_keeps_unknowns():
    from collectors.daily_bars import event_kind

    assert event_kind("8") == "split"
    assert event_kind("3") == "rights+bonus"
    assert event_kind("512") == "bit512"
    assert event_kind("A") == "raw:A"
