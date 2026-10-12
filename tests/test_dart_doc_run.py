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
