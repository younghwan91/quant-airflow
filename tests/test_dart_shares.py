"""폐지 종목 상장주식수 백필 — 파싱·대상선정·기록 (네트워크 불요).

시가총액의 분모라 틀리면 유니버스 편입이 통째로 어긋난다. 특히 **어느 필드를 쓰느냐**가
조용한 오차의 원천이다 — 유통주식수(자기주식 제외)를 쓰면 시총이 과소 계상된다.
"""

from __future__ import annotations

import collectors.dart_shares as ds
from collectors.storage import connect

# 삼성전자 2025 사업보고서 형태(발췌). 합계 행에는 우선주가 섞여 있다.
PAYLOAD = {
    "status": "000",
    "list": [
        {"se": "보통주", "rcept_no": "20260310002820", "stlm_dt": "2025-12-31",
         "isu_stock_totqy": "20,000,000,000", "istc_totqy": "5,919,637,922",
         "tesstk_co": "91,828,987", "distb_stock_co": "5,827,808,935"},
        {"se": "우선주", "rcept_no": "20260310002820", "stlm_dt": "2025-12-31",
         "isu_stock_totqy": "5,000,000,000", "istc_totqy": "822,886,700",
         "tesstk_co": "20,515,497", "distb_stock_co": "802,371,203"},
        {"se": "합계", "rcept_no": "20260310002820", "stlm_dt": "2025-12-31",
         "istc_totqy": "6,742,524,622", "distb_stock_co": "6,630,180,138"},
    ],
}


def test_uses_issued_shares_not_distributed():
    """유통주식수(자기주식 제외)를 쓰면 시가총액이 과소 계상된다."""
    shares, stlm = ds.parse_shares(PAYLOAD)
    assert shares == 5_919_637_922      # istc_totqy (발행주식총수)
    assert shares != 5_827_808_935      # distb_stock_co (유통주식수)
    assert stlm == "2025-12-31"


def test_ignores_preferred_and_total_rows():
    """합계 행은 우선주를 포함한다 — 보통주 기준 유니버스의 시총을 부풀린다."""
    shares, _ = ds.parse_shares(PAYLOAD)
    assert shares != 6_742_524_622


def test_receipt_date_is_the_disclosure_day_not_the_reference_day():
    """기준일과 공시일이 다르다 — PIT 를 물으면 공시일을 봐야 한다."""
    assert ds.receipt_date(PAYLOAD) == "2026-03-10"
    assert ds.parse_shares(PAYLOAD)[1] == "2025-12-31"


def test_error_and_empty_payloads_yield_nothing():
    for p in ({}, {"status": "013", "message": "조회된 데이타가 없습니다."},
              {"status": "000", "list": []},
              {"status": "000", "list": [{"se": "보통주", "istc_totqy": "-"}]}):
        assert ds.parse_shares(p) == (None, None)


def _payload_for(stlm: str) -> dict:
    row = {**PAYLOAD["list"][0], "stlm_dt": stlm}
    return {"status": "000", "list": [row]}


def test_series_covers_every_year_of_the_trading_life(monkeypatch):
    """**시계열이어야 하는 이유** — market_cap_asof 는 date <= 조회일 로 찾는다.

    종목당 1점만 있으면(그것도 폐지일보다 뒤인 경우가 실측 35%) 그 종목의 모든
    거래일에서 시총이 NULL 이 된다. 거래 기간을 가로지르는 점들이 있어야 한다.
    """
    monkeypatch.setattr(ds, "fetch",
                        lambda key, cc, year, rc, **kw: _payload_for(f"{year}-12-31"))
    got = ds.shares_series("k", "c", 2018, 2021, sleep=0)
    assert [stlm for _, stlm, _ in got] == [
        "2018-12-31", "2019-12-31", "2020-12-31", "2021-12-31"]


def test_series_falls_back_to_quarterly_when_annual_is_missing(monkeypatch):
    """폐지 직전 해엔 사업보고서를 못 낸 경우가 많다."""
    calls = []

    def fake(key, cc, year, rc, **kw):
        calls.append((year, rc))
        return _payload_for(f"{year}-09-30") if rc == "11014" else {"status": "013"}

    monkeypatch.setattr(ds, "fetch", fake)
    got = ds.shares_series("k", "c", 2020, 2020, sleep=0)
    assert len(got) == 1 and got[0][1] == "2020-09-30"
    assert calls[0] == (2020, "11011"), "사업보고서를 먼저 시도해야 한다"


def test_series_takes_one_point_per_year(monkeypatch):
    """연 1점이면 충분하다 — 분기 전부는 4배 비싸고 주식수는 분기 내 잘 안 변한다."""
    calls = []

    def fake(key, cc, year, rc, **kw):
        calls.append((year, rc))
        return _payload_for(f"{year}-12-31")

    monkeypatch.setattr(ds, "fetch", fake)
    ds.shares_series("k", "c", 2019, 2021, sleep=0)
    assert len(calls) == 3, "연도마다 첫 성공에서 멈춰야 한다"


def test_series_empty_when_nothing_is_filed(monkeypatch):
    monkeypatch.setattr(ds, "fetch", lambda *a, **k: {"status": "013"})
    assert ds.shares_series("k", "x", 2019, 2021, sleep=0) == []


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
    con.execute("INSERT INTO delisted_stocks(code, dart_checked)"
                " VALUES('A','2026-08-25')")
    con.commit()

    assert [r[0] for r in ds._targets(con)] == ["B"]
    # --refetch 면 마커를 무시하고 다시 훑는다
    assert [r[0] for r in ds._targets(con, refetch=True)] == ["A", "B"]
    con.close()


def test_codes_with_no_dart_data_get_marked(tmp_path):
    con = connect(tmp_path / "t.db")
    con.execute("INSERT INTO delisted_stocks(code) VALUES('A')")
    con.commit()

    ds._mark_checked(con, ["A"], "2026-08-25")

    got = con.execute("SELECT dart_checked FROM delisted_stocks WHERE code='A'").fetchone()
    assert got[0] == "2026-08-25"
    con.close()
