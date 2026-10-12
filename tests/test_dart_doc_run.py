"""dart_doc_run — 캐시·재시도 규칙(네트워크 없이 _get 을 바꿔 끼운다)."""

from __future__ import annotations

import pytest

import collectors.dart_doc_run as dr


def _dart(tmp_path, bodies):
    d = dr.Dart("k", tmp_path, retries=3, pause=0)
    it = iter(bodies)
    d._get = lambda path, **params: next(it)  # noqa: ARG005
    return d


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr(dr.time, "sleep", lambda s: None)


def test_transient_errors_are_retried_then_cached_as_zip(tmp_path):
    d = _dart(tmp_path, [b'{"status":"800","message":"x"}', b"<html>502</html>", b"PK\x03\x04zip"])
    assert d.document("1") == b"PK\x03\x04zip"
    assert (tmp_path / "doc_1.zip").exists()


def test_no_document_status_is_cached_permanently(tmp_path):
    d = _dart(tmp_path, [b"<result><status>014</status><message>no file</message></result>"])
    assert d.document("2") is None
    assert (tmp_path / "doc_2.missing").exists()
    assert d.document("2") is None  # 다시 묻지 않는다(_get 이 더 불리면 StopIteration)


def test_persistent_transient_error_raises_and_is_not_cached(tmp_path):
    d = _dart(tmp_path, [b'{"status":"900"}'] * 3)
    with pytest.raises(dr.DartTransient):
        d.document("3")
    assert not (tmp_path / "doc_3.missing").exists()


def test_quota_stops_immediately(tmp_path):
    d = _dart(tmp_path, [b"<result><status>020</status></result>"])
    with pytest.raises(dr.DartQuota):
        d.document("4")


def test_list_json_error_body_is_retried(tmp_path):
    ok = b'{"status":"000","total_page":1,"list":[{"rcept_no":"20160516003860","report_nm":"x"}]}'
    d = _dart(tmp_path, [b"not json", ok])
    assert d.filings("c", "20070101", "20170630")[0]["rcept_no"] == "20160516003860"


def test_list_window_is_shared_across_commands():
    assert dr.list_window([2015, 2016]) == ("20070101", "20170630")
    assert dr.list_window([2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015]) == ("20070101", "20170630")


def test_corp_map_drops_unconfirmed_delisted_mapping():
    """corpCode 의 그 코드 주인이 거래하던 해에 보고서가 없으면(코드 재사용) 쓰지 않는다."""
    rows = [{"corp_code": "A", "stock_code": "015670"}, {"corp_code": "B", "stock_code": "005930"}]
    mapping = {"015670": {"corp_code": "A", "status": "stock_code_unconfirmed"},
               "012655": {"corp_code": "C", "status": "class_share_of_012650"},
               "000280": {"corp_code": None, "status": "ambiguous"}}
    m = dr.corp_map(rows, mapping)
    assert "015670" not in m and "000280" not in m
    assert m["012655"] == ["C"] and m["005930"] == ["B"]


def test_read_jsonl_drops_truncated_last_line(tmp_path):
    p = tmp_path / "out.jsonl"
    p.write_text('{"code": "000001", "records": []}\n{"code": "0000')
    assert [r["code"] for r in dr.read_jsonl(p)] == ["000001"]
    assert p.read_text() == '{"code": "000001", "records": []}\n'


def test_yield_counts_unmapped_codes_and_fails_empty_years():
    rows = [
        {"code": "a", "filings": {"2010Q1": {}}, "records": [{"period": "2010Q1", "netinc": 1.0, "is_correction": False}]},
        {"code": "b", "filings": {}, "records": [], "error": "no_map"},
        {"code": "c", "filings": {}, "records": [], "error": "no_dec_fye_filing"},
    ]
    st = dr.yield_report(rows)
    assert (st["code_ok"], st["code_no_map"], st["code_non_dec"]) == (1, 1, 1)
    bad = dr.yield_failures(st, [2010, 2011], 0.9)
    assert "2011: 보고서 0건" in bad
    assert any(b.startswith("종목: 1/2") for b in bad)  # 12월 결산 아님은 분모에서 뺀다, 매핑 없음은 남긴다
    assert not any(b.startswith("2010") for b in bad)


def test_code_status_categories():
    assert dr.code_status({"error": "no_dec_fye_filing"}) == "non_dec"
    assert dr.code_status({"error": "no_filing"}) == "no_filing"
    assert dr.code_status({"error": None, "records": [{"netinc": None}]}) == "parse_fail"
    assert dr.code_status({"error": None, "records": [{"netinc": 1.0}]}) == "ok"


# ---------------------------------------------------------------- load(sqlite 경로)·상한·원자적 쓰기


def _rec(period: str, kd: str, rno: str, netinc: float | None, corr: bool = False) -> dict:
    return {"period": period, "knowledge_date": kd, "rcept_no": rno, "is_correction": corr, "basis": "ofs",
            "netinc": netinc, "derived": False, "basis_ctrl": "ofs", "netinc_ctrl": netinc, "derived_ctrl": False,
            "ctrl_q": None, "ctrl_ytd": None, "ctrl_q_derived": False, "cfs_q": None, "cfs_ytd": None,
            "cfs_q_derived": False, "ofs_q": netinc, "ofs_ytd": netinc, "ofs_q_derived": False, "ytd_start": None,
            "src": {"ofs": "is:x"}, "flags": ["ofs_x"],
            "shares": {"common": 100, "preferred": 0, "total": 100, "asof": "2009-03-31", "asof_inferred": False,
                       "method": "acode"}}


def test_load_rows_upserts_into_sqlite_and_keeps_later_receipt(tmp_path):
    from collectors.storage import connect, fetchall

    con = connect(tmp_path / "t.db")
    rows = [{"code": "005930", "records": [
        _rec("2009Q1", "2009-05-15", "20090515000002", 2.0),
        _rec("2009Q1", "2009-05-15", "20090515000001", 1.0),  # 같은 날 앞 접수번호 — 뒤 판이 남는다
        _rec("2009Q2", "2009-08-14", "20090814000001", 3.0),
    ]}]
    assert dr.load_rows(con, rows) >= 2
    con.commit()
    got = fetchall(con, "SELECT period, rcept_no, netinc, shares_common, flags, netinc_prior FROM earnings_dart_doc "
                        "ORDER BY period", ())
    assert [tuple(r) for r in got] == [("2009Q1", "20090515000002", 2.0, 100, '["ofs_x"]', None),
                                       ("2009Q2", "20090814000001", 3.0, 100, '["ofs_x"]', None)]
    # 다시 넣어도 같은 행(멱등)
    dr.load_rows(con, rows)
    con.commit()
    assert fetchall(con, "SELECT count(*) FROM earnings_dart_doc", ())[0][0] == 2


def test_load_rows_with_zero_records_raises(tmp_path):
    from collectors.storage import connect

    con = connect(tmp_path / "t.db")
    with pytest.raises(RuntimeError, match="0건"):
        dr.load_rows(con, [{"code": "x", "records": [], "error": "no_map"}])


def test_record_row_fills_prior_when_present():
    from collectors.storage import EARNINGS_DART_DOC_COLS

    rec = {**_rec("2009Q1", "2009-05-15", "1", 2.0), "netinc_prior": 1.5, "prior_basis_note": "x"}
    row = dict(zip(EARNINGS_DART_DOC_COLS, dr.record_row("005930", rec, pg=False), strict=True))
    assert (row["netinc_prior"], row["prior_basis_note"], row["ofs_prior_q"]) == (1.5, "x", None)
    assert row["is_correction"] == 0 and row["shares_method"] == "acode"


def test_call_budget_stops_cleanly_and_persists_per_key_per_day(tmp_path):
    d = dr.Dart("k", tmp_path, key_name="DART_API_KEY_X", max_calls=2)
    d._tick()
    d._tick()
    with pytest.raises(dr.DartBudget):
        d._tick()
    again = dr.Dart("k", tmp_path, key_name="DART_API_KEY_X", max_calls=2)  # 같은 날 다시 시작해도 이어진다
    assert again.today == 2


def test_atomic_write_leaves_no_temp(tmp_path):
    p = tmp_path / "doc_1.zip"
    dr.atomic_write(p, b"PK")
    assert p.read_bytes() == b"PK"
    assert [x.name for x in tmp_path.iterdir()] == ["doc_1.zip"]
