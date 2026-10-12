"""DART 실적 공시일 수집 — 분류·가장 이른 원본·재개·키 순환 (네트워크 불요)."""

from __future__ import annotations

import datetime as dt

import pytest

from collectors.dart_filing_dates import Lister, QuotaExhausted, classify, collect, earliest, quarter_before
from collectors.storage import (
    CHECKED_DART_FILING_DATES,
    checked_codes,
    connect,
    insert_earnings_disclosures,
    upsert_earnings,
)

D = dt.date


@pytest.mark.parametrize(("day", "period"), [
    (D(2020, 4, 7), "2020Q1"),
    (D(2020, 1, 30), "2019Q4"),
    (D(2020, 3, 31), "2019Q4"),     # 분기말 당일은 그 전 분기
    (D(2020, 7, 1), "2020Q2"),
    (D(2020, 12, 31), "2020Q3"),
])
def test_quarter_before(day, period):
    assert quarter_before(day) == period


@pytest.mark.parametrize(("name", "day", "want"), [
    ("연결재무제표기준영업(잠정)실적(공정공시)", D(2020, 4, 7), ("prelim", "2020Q1")),
    ("영업(잠정)실적(공정공시)", D(2021, 2, 10), ("prelim", "2020Q4")),
    ("매출액또는손익구조30%(대규모법인은15%)이상변경", D(2020, 1, 30), ("pl_change", "2019Q4")),
    ("매출액또는손익구조30%(대규모법인은15%)이상변동", D(2021, 4, 2), ("pl_change", "2020Q4")),
    ("매출액또는손익구조30%(대규모법인은15%)이상변경", D(2020, 9, 1), None),   # 비12월 결산
    ("분기보고서 (2020.03)", D(2020, 5, 15), ("periodic", "2020Q1")),
    ("반기보고서 (2020.06)", D(2020, 8, 14), ("periodic", "2020Q2")),
    ("사업보고서 (2019.12)", D(2020, 3, 30), ("periodic", "2019Q4")),
    ("[기재정정]연결재무제표기준영업(잠정)실적(공정공시)", D(2020, 4, 29), None),
    ("[첨부추가]분기보고서 (2020.03)", D(2020, 5, 20), None),
    ("영업(잠정)실적(공정공시)(자회사의 주요경영사항)", D(2020, 4, 20), None),
    ("사업보고서 (2020.06)", D(2020, 9, 28), None),      # 6월 결산 — earnings period 규약과 안 맞는다
    ("분기보고서 (2020.06)", D(2020, 8, 14), None),
    ("감사보고서제출", D(2020, 2, 24), None),
])
def test_classify(name, day, want):
    assert classify(name, day) == want


def _f(day: str, name: str, no: str) -> dict:
    return {"rcept_dt": day, "report_nm": name, "rcept_no": no}


def test_earliest_keeps_first_original_per_period_and_kind():
    filings = [
        _f("20200429", "연결재무제표기준영업(잠정)실적(공정공시)", "20200429000002"),   # 말일 확정치
        _f("20200407", "연결재무제표기준영업(잠정)실적(공정공시)", "20200407000001"),   # 초순 가이던스
        _f("20200429", "[기재정정]연결재무제표기준영업(잠정)실적(공정공시)", "20200429000003"),
        _f("20200515", "분기보고서 (2020.03)", "20200515000004"),
        _f("20200224", "감사보고서제출", "20200224000005"),
    ]
    assert earliest("005930", filings) == [
        ("005930", "2020Q1", "periodic", "2020-05-15", "20200515000004", "분기보고서 (2020.03)"),
        ("005930", "2020Q1", "prelim", "2020-04-07", "20200407000001", "연결재무제표기준영업(잠정)실적(공정공시)"),
    ]


def test_insert_never_moves_known_date_later(tmp_path):
    con = connect(tmp_path / "t.db")
    first = ("005930", "2020Q1", "prelim", "2020-04-07", "a", "x")
    later = ("005930", "2020Q1", "prelim", "2020-04-29", "b", "x")
    insert_earnings_disclosures(con, [first])
    insert_earnings_disclosures(con, [later])
    assert con.execute("SELECT rcept_dt FROM earnings_disclosures").fetchall()[0][0] == "2020-04-07"


class _Resp:
    def __init__(self, body: dict) -> None:
        self._body = body

    def raise_for_status(self) -> None:
        pass

    def json(self) -> dict:
        return self._body


class _FakeDart:
    """corp_code·pblntf_ty·page 별 응답. 키 'dead' 는 늘 020."""

    def __init__(self, pages: dict[tuple[str, str], list[list[dict]]]) -> None:
        self.pages, self.calls = pages, []

    def __call__(self, url, params):
        self.calls.append(params)
        if params["crtfc_key"] == "dead":
            return _Resp({"status": "020", "message": "한도"})
        got = self.pages.get((params["corp_code"], params["pblntf_ty"]))
        if not got:
            return _Resp({"status": "013", "message": "없음"})
        return _Resp({"status": "000", "total_page": len(got), "list": got[params["page_no"] - 1]})


def _lister(fake, keys=("k1",), max_calls=1000) -> Lister:
    return Lister(list(keys), max_calls=max_calls, get=fake, sleep=lambda s: None, clock=lambda: 0.0)


def _earnings(con, *codes):
    upsert_earnings(con, [(c, "2020Q1", "2020-05-15", "2020-05-15", 1.0, 1.0, 1.0, 1.0, 1.0, 1.0) for c in codes])


def test_collect_pages_rotates_keys_and_marks_done(tmp_path):
    con = connect(tmp_path / "t.db")
    _earnings(con, "005930", "000660")
    fake = _FakeDart({
        ("C1", "I"): [[_f("20200407", "영업(잠정)실적(공정공시)", "1")], [_f("20200130", "영업(잠정)실적(공정공시)", "2")]],
        ("C1", "A"): [[_f("20200515", "분기보고서 (2020.03)", "3")]],
    })
    stats = collect(con, _lister(fake, keys=("dead", "k1")), {"005930": "C1", "000660": "C2"},
                    since=D(2019, 1, 1), until=D(2026, 10, 12), today="2026-10-12")
    rows = con.execute("SELECT code, period, kind, rcept_dt FROM earnings_disclosures ORDER BY 1,2,3").fetchall()
    assert [tuple(r) for r in rows] == [
        ("005930", "2019Q4", "prelim", "2020-01-30"),
        ("005930", "2020Q1", "periodic", "2020-05-15"),
        ("005930", "2020Q1", "prelim", "2020-04-07"),
    ]
    assert stats["scanned"] == 2 and stats["stopped"] == 0
    assert checked_codes(con, CHECKED_DART_FILING_DATES) == {"005930", "000660"}
    # 다 훑은 종목은 다음 실행에서 다시 안 부른다
    n = len(fake.calls)
    again = collect(con, _lister(fake), {"005930": "C1", "000660": "C2"},
                    since=D(2019, 1, 1), until=D(2026, 10, 12), today="2026-10-13")
    assert len(fake.calls) == n and again["already"] == 2


def test_collect_stops_at_max_calls_without_marking_partial_code(tmp_path):
    con = connect(tmp_path / "t.db")
    _earnings(con, "005930", "000660")
    fake = _FakeDart({})
    # 종목당 2호출(A·I, 둘 다 013) — 3호출이면 두 번째 종목 중간에서 멈춘다
    stats = collect(con, _lister(fake, max_calls=3), {"005930": "C1", "000660": "C2"},
                    since=D(2019, 1, 1), until=D(2026, 10, 12), today="2026-10-12")
    assert stats["stopped"] == 1 and stats["scanned"] == 1
    assert checked_codes(con, CHECKED_DART_FILING_DATES) == {"000660"}   # 정렬순 첫 종목만


def test_all_keys_exhausted_raises_quota():
    lister = _lister(_FakeDart({}), keys=("dead",))
    with pytest.raises(QuotaExhausted):
        lister.filings("C1", D(2019, 1, 1), D(2026, 10, 12))
