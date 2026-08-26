"""collectors/listed_shares.py — 파서가 만들어낸 0 을 적재하지 않는다.

`storage.to_int` 는 파싱 실패를 0 으로 돌려준다. 그 0 이 그대로
`shares_outstanding_history` 에 들어가면 진짜 주식수와 구분되지 않고, 행이
**오늘 날짜**라 kr-quant `market_cap_asof` 의 backward as-of 가 집어가는 최신
점이 된다 — 그때부터 그 종목 시총은 `close * 0 = 0` 이다. 그쪽 가드는 `None`
만 걸러 0 은 통과시키므로, 시총을 분모로 쓰는 계산이 inf 가 된다.

2026-08-27 기준 DB 실측으로는 0 행이 없다(kiwoom 23,636 / dart 2,013 전부).
ka10001 이 `flo_stk` 를 늘 채워줘서 아직 안 터졌을 뿐이라, 그 전제가 깨지는
날을 여기서 고정한다.
"""

from __future__ import annotations

import sqlite3

import pytest

from collectors.listed_shares import _SHARES_FIELD, _SHARES_UNIT_MULTIPLIER, collect


@pytest.fixture
def con():
    c = sqlite3.connect(":memory:")
    c.execute("CREATE TABLE shares_outstanding_history (code TEXT, date TEXT,"
              " shares_outstanding INTEGER, source TEXT DEFAULT 'kiwoom',"
              " PRIMARY KEY (code, date))")
    return c


def _api(responses: dict[str, dict]):
    """`basic_stock_info` 가 코드별로 지정한 응답을 주는 최소 더미."""
    class _Info:
        def basic_stock_info(self, **kw):
            return responses[kw["stk_cd"]]

    class _Api:
        stock_info = _Info()

    return _Api()


def _stocks(*codes: str) -> list[dict]:
    return [{"code": c, "name": f"종목{c}"} for c in codes]


@pytest.mark.parametrize(
    "raw",
    [
        pytest.param(None, id="필드 자체가 없음"),
        pytest.param("", id="빈 문자열"),
        pytest.param("N/A", id="파싱 불가"),
        pytest.param("0", id="벤더가 진짜 0 을 줌"),
    ],
)
def test_missing_share_count_is_not_stored_as_zero(con, raw):
    api = _api({"005930": {} if raw is None else {_SHARES_FIELD: raw}})

    stats = collect(api, con, _stocks("005930"))

    assert con.execute("SELECT count(*) FROM shares_outstanding_history").fetchone()[0] == 0
    assert stats["failed"] == 1
    assert stats["done"] == 0


def test_good_rows_still_land_and_a_bad_one_does_not_take_them_down(con):
    api = _api({
        "005930": {_SHARES_FIELD: "5969783"},   # 정상 — 천주 단위
        "000660": {_SHARES_FIELD: ""},          # 결측 — 이 행만 버린다
        "035720": {_SHARES_FIELD: "445000"},    # 정상
    })

    stats = collect(api, con, _stocks("005930", "000660", "035720"))

    rows = dict(con.execute(
        "SELECT code, shares_outstanding FROM shares_outstanding_history").fetchall())
    assert rows == {
        "005930": 5969783 * _SHARES_UNIT_MULTIPLIER,
        "035720": 445000 * _SHARES_UNIT_MULTIPLIER,
    }
    assert stats["done"] == 2
    assert stats["failed"] == 1
