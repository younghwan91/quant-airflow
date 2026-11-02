"""폐지 종목 상장주식수 백필 — 시계열 조립·대상선정·기록 (네트워크 불요).

시가총액의 분모라 틀리면 유니버스 편입이 통째로 어긋난다. fetch/parse(어느
필드를 쓰는지, 보고서 폴백 등)는 krx-fundamentals-client의
``DartScraper.fetch_shares_outstanding``으로 옮겼다(2026-09-06, 그쪽 테스트
스위트가 검증) — 여기서는 이 파일이 여전히 맡는 시계열 조립·키 로테이션·대상
선정·DB 적재만 다룬다.
"""

from __future__ import annotations

import asyncio
import json
from datetime import date

import pytest
from krx_fundamentals_client import DartQuotaExceededError, DartScraper, SharesOutstanding

import collectors.dart_shares as ds
from collectors.storage import CHECKED_DART_SHARES_LISTED, checked_codes, connect, mark_checked


def _run(coro):
    return asyncio.run(coro)


def _shares(ticker, year, shares=5_919_637_922, stlm="20251231", knowledge="20260310"):
    return SharesOutstanding(
        ticker=ticker, year=year, shares_outstanding=shares,
        stlm_dt=stlm, knowledge_date=knowledge,
    )


def test_dashed_converts_yyyymmdd():
    assert ds._dashed("20251231") == "2025-12-31"


def test_dashed_leaves_already_dashed_alone():
    assert ds._dashed("2025-12-31") == "2025-12-31"


def test_series_covers_every_year_of_the_trading_life(monkeypatch):
    """**시계열이어야 하는 이유** — market_cap_asof 는 date <= 조회일 로 찾는다.

    종목당 1점만 있으면(그것도 폐지일보다 뒤인 경우가 실측 35%) 그 종목의 모든
    거래일에서 시총이 NULL 이 된다. 거래 기간을 가로지르는 점들이 있어야 한다.
    """
    async def fake(self, ticker, year, on_status=None):
        return _shares(ticker, year, stlm=f"{year}1231")

    monkeypatch.setattr(DartScraper, "fetch_shares_outstanding", fake)
    scrapers = {"k": DartScraper(api_key="k")}
    got = _run(ds.shares_series(scrapers, ["k"], "005930", range(2018, 2022), sleep=0))
    assert [r.point[1] for r in got] == [
        "2018-12-31", "2019-12-31", "2020-12-31", "2021-12-31"]


def _status_fake(by_year):
    """연도 → (보고서마다 부를 status 목록, 돌려줄 값)."""
    async def fake(self, ticker, year, on_status=None):
        statuses, result = by_year[year]
        for st in statuses:
            on_status(st, "ctx")
        return result
    return fake


def test_year_is_no_filing_only_when_all_four_reports_say_013(monkeypatch):
    """**마커의 근거는 DART 013 뿐이다.** 파싱 실패·통신 오류·매핑 없음은 no_value — 마커가 안 찍혀 다시 본다."""
    monkeypatch.setattr(DartScraper, "fetch_shares_outstanding", _status_fake({
        2019: (["013"] * 4, None),         # 보고서 넷 다 없음
        2020: ([], None),                  # 000 인데 라벨을 못 읽었거나 corp_code 매핑 없음
        2021: (["013", "013", "800"], None),  # 일시 장애 섞임
        2022: (["013", "013", "013"], None),  # 통신 오류 하나를 라이브러리가 삼켰다
        2023: (["013"], _shares("x", 2023, stlm="20230930")),  # 사업보고서 없고 3분기 값
    }))
    got = _run(ds.shares_series({"k": DartScraper(api_key="k")}, ["k"], "x", range(2019, 2024), sleep=0))
    assert [r.status for r in got] == ["no_filing", "no_value", "no_value", "no_value", "row"]
    assert got[-1].point[1] == "2023-09-30"


def test_series_dates_are_dashed_for_string_comparison_with_existing_rows(monkeypatch):
    """shares_outstanding_history.date 는 기존 행과 문자열 비교로 as-of 되므로
    라이브러리가 주는 대시 없는 YYYYMMDD를 그대로 적재하면 안 된다."""
    async def fake(self, ticker, year, on_status=None):
        return _shares(ticker, year, stlm="20251231", knowledge="20260310")

    monkeypatch.setattr(DartScraper, "fetch_shares_outstanding", fake)
    scrapers = {"k": DartScraper(api_key="k")}
    got = _run(ds.shares_series(scrapers, ["k"], "005930", [2025], sleep=0))
    assert got[0].point == (5_919_637_922, "2025-12-31", "2026-03-10")


def test_shares_series_rotates_to_next_key_on_quota(monkeypatch):
    """일한도(020)를 만나면 다음 키로 넘어간다 — 예전엔 keys[0] 하나뿐이었다.

    그 제약이 상장분 백필(2,595종목 × ~9.5콜)을 하루 한도 밖으로 밀어냈고,
    이틀로 나눈 분할이 정렬과 겹쳐 시대별로 기울어진 중간 상태를 만들었다.
    """
    seen = []

    async def fake(self, ticker, year, on_status=None):
        seen.append(self.api_key)
        if self.api_key == "k1":
            raise DartQuotaExceededError("한도초과")
        return _shares(ticker, year)

    monkeypatch.setattr(DartScraper, "fetch_shares_outstanding", fake)
    scrapers = {"k1": DartScraper(api_key="k1"), "k2": DartScraper(api_key="k2")}
    ki = [0]
    out = _run(ds.shares_series(scrapers, ["k1", "k2"], "005930", [2020], ki=ki, sleep=0))

    assert seen[:2] == ["k1", "k2"]     # 소진 즉시 다음 키로
    assert ki == [1]                    # 인덱스가 유지된다(다음 종목은 k2 로 시작)
    assert out[0].point[0] == 5_919_637_922


def test_shares_series_key_index_is_shared_across_calls(monkeypatch):
    """ki 를 넘기면 종목 간에 유지된다 — 안 그러면 종목마다 소진된 키를 또 친다."""
    seen = []

    async def fake(self, ticker, year, on_status=None):
        seen.append(self.api_key)
        if self.api_key == "k1":
            raise DartQuotaExceededError("한도초과")
        return _shares(ticker, year)

    monkeypatch.setattr(DartScraper, "fetch_shares_outstanding", fake)
    scrapers = {"k1": DartScraper(api_key="k1"), "k2": DartScraper(api_key="k2")}
    ki = [0]
    _run(ds.shares_series(scrapers, ["k1", "k2"], "a", [2020], ki=ki, sleep=0))
    seen.clear()
    _run(ds.shares_series(scrapers, ["k1", "k2"], "b", [2020], ki=ki, sleep=0))
    assert "k1" not in seen        # 두 번째 종목은 소진된 키를 다시 치지 않는다


def test_quota_exhausted_raises_not_empty(monkeypatch):
    """키를 다 쓰면 예외다. 빈 결과로 돌려주던 때 한도 소진이 "자료 없음" 마커로 굳었다."""
    async def always_exhausted(self, ticker, year, on_status=None):
        raise DartQuotaExceededError("한도초과")

    monkeypatch.setattr(DartScraper, "fetch_shares_outstanding", always_exhausted)
    ki = [0]
    with pytest.raises(DartQuotaExceededError):
        _run(ds.shares_series({"only": DartScraper(api_key="only")}, ["only"], "005930", [2020], ki=ki, sleep=0))
    assert ki[0] == 0


def _bars(con, *rows):
    con.executemany(
        "INSERT INTO daily_bars(code,date,open,high,low,close,volume,trade_value,source)"
        " VALUES(?,?,1,1,1,1,1,1,?)", rows)
    con.commit()


def _shares_row(con, code, day, shares=100, source="dart"):
    con.execute("INSERT INTO shares_outstanding_history(code,date,shares_outstanding,source) VALUES(?,?,?,?)",
                (code, day, shares, source))
    con.commit()


def test_targets_resume_unit_is_code_year(tmp_path):
    """행이 하나 있다고 코드째 건너뛰지 않는다 — 비어 있는 해만 본다."""
    con = connect(tmp_path / "t.db")
    _bars(con, ("A", "2019-06-03", "naver"), ("A", "2020-06-03", "naver"), ("A", "2021-06-03", "naver"),
          ("B", "2020-01-02", "naver"), ("C", "2020-01-02", "kiwoom"))
    _shares_row(con, "A", "2020-12-31")
    got = dict(ds._targets(con))
    assert got == {"A": [2019, 2021], "B": [2020]}, "C 는 상장 쪽이라 대상 아님"
    assert dict(ds._targets(con, listed=True)) == {"C": [2020]}
    assert dict(ds._targets(con, listed=True, from_year=2021, to_year=2025)) == {}
    con.close()


def test_targets_year_marker_skips_only_that_year_even_with_refetch(tmp_path):
    con = connect(tmp_path / "t.db")
    _bars(con, ("A", "2019-06-03", "naver"), ("A", "2020-06-03", "naver"))
    mark_checked(con, ds.year_marker(2019), ["A"], "2026-11-02")
    assert dict(ds._targets(con)) == {"A": [2020]}
    assert dict(ds._targets(con, refetch=True)) == {"A": [2020]}, "한도에 걸린 재실행이 이어받아야 한다"
    con.close()


def test_targets_legacy_code_marker_is_honored_then_ignored_without_deleting(tmp_path):
    """옛 코드 단위 마커는 파싱 실패·한도 소진까지 찍었다 — --refetch 가 무시하고, 지우지는 않는다."""
    con = connect(tmp_path / "t.db")
    _bars(con, ("A", "2020-01-02", "kiwoom"), ("B", "2020-01-02", "kiwoom"))
    mark_checked(con, CHECKED_DART_SHARES_LISTED, ["A"], "2026-08-25")
    assert dict(ds._targets(con, listed=True)) == {"B": [2020]}
    assert dict(ds._targets(con, listed=True, refetch=True)) == {"A": [2020], "B": [2020]}
    assert checked_codes(con, CHECKED_DART_SHARES_LISTED) == {"A"}
    con.close()


def test_targets_refetch_reopens_years_with_only_a_quarter_fallback_row(tmp_path):
    """옛 파서가 사업보고서 라벨을 못 읽어 3분기 행만 남은 해(066570 2021) — 재조회해야 연말 행이 생긴다."""
    con = connect(tmp_path / "t.db")
    _bars(con, ("A", "2021-06-03", "kiwoom"), ("B", "2021-06-03", "kiwoom"))
    _shares_row(con, "A", "2021-09-30")
    _shares_row(con, "B", "2021-12-31")
    assert dict(ds._targets(con, listed=True)) == {}
    assert dict(ds._targets(con, listed=True, refetch=True)) == {"A": [2021]}
    con.close()


def test_targets_order_is_not_code_order(tmp_path):
    """코드순은 시대 표본추출이다(2,595종목 앞 2,000개: 2016년 상장 97%·2023~24년 13%) — md5 셔플, 재현 가능."""
    con = connect(tmp_path / "t.db")
    codes = [f"{i:06d}" for i in range(50)]
    _bars(con, *[(c, "2020-01-02", "naver") for c in codes])
    first = [c for c, _ in ds._targets(con)]
    assert sorted(first) == codes and first != codes
    assert first == [c for c, _ in ds._targets(con)]
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


def test_codes_with_no_dart_data_get_marked(tmp_path):
    con = connect(tmp_path / "t.db")
    mark_checked(con, ds.CHECKED_DART_SHARES, ["A"], "2026-08-25")

    got = con.execute("SELECT checked_date FROM backfill_markers"
                      " WHERE code='A' AND source=?", (ds.CHECKED_DART_SHARES,)).fetchone()
    assert got[0] == "2026-08-25"
    con.close()


def test_markers_are_per_source_not_per_column(tmp_path):
    """소스가 늘어도 스키마가 안 바뀐다 — 004→007 로 세 번 재발한 그 자리.

    무엇보다 마커가 delisted_stocks 컬럼이던 시절엔 **상장 종목에 쓸 자리가
    없었다**(그 테이블에 상장 종목이 없다). 그래서 --listed 가 자료 없는 60종목을
    매 회차 다시 조회했다.
    """
    con = connect(tmp_path / "t.db")
    mark_checked(con, ds.CHECKED_DART_SHARES, ["A"], "2026-08-25")
    mark_checked(con, CHECKED_DART_SHARES_LISTED, ["B"], "2026-08-28")

    assert checked_codes(con, ds.CHECKED_DART_SHARES) == {"A"}
    assert checked_codes(con, CHECKED_DART_SHARES_LISTED) == {"B"}
    # 같은 코드가 소스별로 독립적으로 기록된다
    mark_checked(con, CHECKED_DART_SHARES_LISTED, ["A"], "2026-08-28")
    assert checked_codes(con, CHECKED_DART_SHARES_LISTED) == {"A", "B"}
    assert checked_codes(con, ds.CHECKED_DART_SHARES) == {"A"}
    con.close()


def test_drop_unit_errors_catches_thousand_and_million_multiples():
    from collectors.dart_shares import drop_unit_errors
    series = [(17774267, "2022-12-31", "2023-03-21"), (17774267000, "2023-12-31", "2024-03-14"),
              (17774267, "2024-12-31", "2025-03-21")]
    keep, drop = drop_unit_errors(series)
    assert [p[1] for p in keep] == ["2022-12-31", "2024-12-31"]
    assert drop == [(17774267000, "2023-12-31", "2024-03-14")]


def test_drop_unit_errors_single_point_uses_db_reference_and_hard_cap():
    from collectors.dart_shares import drop_unit_errors
    # 점 하나뿐이어도 DB 의 키움 값과 비교한다 — ×1,000 된 소형주(44.6억 주)는 1e10 상한으로는 못 잡는다
    keep, drop = drop_unit_errors([(4463032000, "2026-06-30", "2026-08-14")], reference=[4463000, 4463000])
    assert keep == [] and len(drop) == 1
    keep, drop = drop_unit_errors([(161358585000000, "2019-12-31", "2021-05-20")])
    assert keep == [] and len(drop) == 1
    # 정상 증자(×3)는 남긴다
    keep, _ = drop_unit_errors([(10000000, "2020-12-31", "x"), (30000000, "2021-12-31", "y")])
    assert len(keep) == 2


# ── F1 전진 보호 ─────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("stlm", "known", "held"),
    [
        ("2021-12-31", "2026-11-05", True),   # 옛 기준일의 전진 구간 정정
        ("2025-12-31", "2027-01-10", True),
        ("2025-12-31", "2026-08-06", True),   # 예열 시작 — 실행마다 여기부터 다시 시뮬레이션한다
        ("2025-12-31", "2026-08-05", False),
        ("2026-06-30", "2026-11-20", False),  # 2026 기준일은 정상 경로
        ("2026-01-01", "2026-11-05", False),
    ],
)
def test_f1_held_out(stlm, known, held):
    assert ds.f1_held_out(stlm, known) is held


def test_f1_protect_from_matches_prereg_sim_from():
    assert ds.F1_PROTECT_FROM == "2026-08-06"


# ── 원장 적재 ────────────────────────────────────────────────────────────────


def _line(code, year, status, shares=None, day=None, known=None):
    return {"code": code, "year": year, "status": status, "shares": shares, "date": day, "knowledge_date": known}


def test_plan_ledger_only_new_keys_and_no_guess():
    rows = [
        _line("A", 2021, "row", 100, "2021-12-31", "2022-03-08"),
        _line("A", 2020, "row", 100, "2020-12-31", "2021-03-08"),   # 이미 있다 — 값이 달라도 안 건드린다
        _line("B", 2021, "row", 100, "2021-12-31", "2026-11-10"),   # F1 보호
        _line("C", 2019, "no_filing"),
        _line("D", 2019, "no_value"),
        _line("E", 2021, "no_value"),
        _line("E", 2021, "row", 7, "2021-12-31", "2022-03-08"),     # 재실행 뒤 줄이 이긴다
    ]
    plan = ds.plan_ledger(rows, [("A", "2020-12-31", 99)])
    assert plan.records == [("A", "2021-12-31", 100, "2022-03-08", "dart"), ("E", "2021-12-31", 7, "2022-03-08", "dart")]
    assert plan.conflicts == [("A", "2020-12-31", 100, "2021-03-08")]
    assert plan.held_out == [("B", "2021-12-31", 100, "2026-11-10")]
    assert plan.markers == [("C", 2019)]
    assert plan.no_value == [("D", 2019)]
    report = ds.plan_report(plan)
    assert "new_rows" in report and "F1 보호로 안 쓴 행 1" in report


def test_plan_ledger_applies_unit_error_guard_with_db_reference():
    rows = [_line("A", 2021, "row", 4_463_032_000, "2021-12-31", "2022-03-08")]
    plan = ds.plan_ledger(rows, [("A", "2020-12-31", 4_463_000), ("A", "2019-12-31", 4_463_000)])
    assert plan.records == [] and plan.unit_errors == [("A", "2021-12-31", 4_463_032_000, "2022-03-08")]
    assert "단위 오류로 버린 행 1" in ds.plan_report(plan)


def _insert(con, records, expect):
    return ds.insert_new_rows(
        con, "shares_outstanding_history", ["code", "date", "shares_outstanding", "knowledge_date", "source"],
        records, value="shares_outstanding", expect=expect)


def test_insert_new_rows_asserts_and_rolls_back_on_conflict(tmp_path):
    """기존 키와 겹치면 행 수 단언이 터지고 **새 행도 남지 않는다**."""
    con = connect(tmp_path / "t.db")
    _shares_row(con, "A", "2020-12-31", 99)
    new = ("B", "2021-12-31", 5, "2022-03-08", "dart")
    with pytest.raises(AssertionError):
        _insert(con, [new, ("A", "2020-12-31", 100, "2021-03-08", "dart")], 2)
    assert [tuple(r) for r in con.execute("SELECT code, shares_outstanding FROM shares_outstanding_history")] == [("A", 99)]
    with pytest.raises(AssertionError):
        _insert(con, [new], 2)  # 예상과 다르면 쓰기 전에
    assert _insert(con, [new], 1) == 1
    assert {r[0]: r[1] for r in con.execute("SELECT code, shares_outstanding FROM shares_outstanding_history")} == {"A": 99, "B": 5}
    con.close()


# ── 첫 공시 도장 ─────────────────────────────────────────────────────────────


def test_period_of_only_quarter_ends():
    assert ds.period_of("2021-12-31") == "2021Q4"
    assert ds.period_of("2021-09-30") == "2021Q3"
    assert ds.period_of("2021-03-31") == "2021Q1"
    assert ds.period_of("2021-11-30") is None


def test_late_rows_only_stamped_after_original():
    main = [("017670", "2021-12-31", 218833144, "2022-08-12"),  # 늦음
            ("066570", "2021-12-31", 163647814, "2022-03-16"),  # 같음
            ("000001", "2021-12-31", 100, "2022-03-10"),        # 원본이 하루 뒤(접수일 표기 차) — 늦은 게 아니다
            ("000002", "2021-12-31", 100, "2022-08-12"),        # 옆 표에 이미 있음
            ("000003", "2021-12-31", 100, "2022-08-12")]        # 원본 정보 없음
    disc = [("017670", "2021Q4", "R1", "2022-03-16"), ("066570", "2021Q4", "R2", "2022-03-16"),
            ("000001", "2021Q4", "R3", "2022-03-11"), ("000002", "2021Q4", "R4", "2022-03-11")]
    got = ds.late_rows(main, disc, [("000002", "2021-12-31")])
    assert [(r["code"], r["rcept_no"], r["first_public_date"]) for r in got] == [("017670", "R1", "2022-03-16")]


def _parsed(common, asof=date(2021, 12, 31)):
    from collectors.dart_doc import Shares
    return Shares(common, None, common, asof, "acode", asof_inferred=asof is None)


@pytest.mark.parametrize(
    ("parsed", "value", "why"),
    [
        (_parsed(100), 100, "ok"),
        (_parsed(110), 110, "ok"),                         # 정정으로 값이 달라졌다 — 원본 값이 그때 알던 값이다
        (None, None, "no_table"),
        (_parsed(100, date(2020, 12, 31)), 100, "ok"),     # 표 앞 날짜가 틀려도 정정본과 값이 같으면(058420 2021)
        (_parsed(110, date(2020, 12, 31)), None, "asof_mismatch"),
        (_parsed(100_000), None, "ratio_outlier"),         # 다른 표를 읽었거나 단위 오류
        (_parsed(100, None), 100, "ok"),                   # 기준일을 못 찾아도 정정본과 같으면
        (_parsed(110, None), None, "asof_unknown"),
    ],
)
def test_original_value_skips_rather_than_guesses(parsed, value, why):
    assert ds.original_value(parsed, {"date": "2021-12-31", "shares": 100}) == (value, why)


def test_apply_originals_additive_with_expect(tmp_path, monkeypatch):
    db = tmp_path / "t.db"
    ledger = tmp_path / "o.jsonl"
    base = {"knowledge_date": "2022-08-12", "shares": 100, "first_public_date": "2022-03-16"}
    lines = [{**base, "code": "A", "date": "2021-12-31", "rcept_no": "R1", "shares_original": 100, "why": "ok"},
             {**base, "code": "B", "date": "2021-12-31", "rcept_no": "R2", "shares_original": None, "why": "no_table"}]
    ledger.write_text("".join(json.dumps(x) + "\n" for x in lines))
    args = type("A", (), {"db": str(db), "apply_original": str(ledger), "dry_run": False, "expect_rows": 2})
    with pytest.raises(AssertionError):
        ds.apply_originals(args)
    args.expect_rows = 1
    assert ds.apply_originals(args) == 0
    con = connect(db)
    assert [tuple(r) for r in con.execute(
        "SELECT code, date, first_public_date, rcept_no, shares_original FROM shares_original_filing")] == [
        ("A", "2021-12-31", "2022-03-16", "R1", 100)]
    con.close()
    args.expect_rows = 0   # 재실행: 이미 있는 행은 건너뛴다
    assert ds.apply_originals(args) == 0
