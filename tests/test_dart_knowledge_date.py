"""``collect_all_financials_batched`` 의 knowledge_date 모드.

과거 백필에 ``today`` 를 박으면 "2026년에 알게 된 2018년 실적"이 되어
``knowledge_date <= asof`` 로 읽는 과거 시점 백테스트에서 통째로 사라진다 —
백필의 목적 자체가 무효가 되는데, 터지지 않고 조용히 비어 보인다.
"""

from __future__ import annotations

import collectors.dart_earnings as de

CORP = {"000020": "00100001", "000030": "00100002"}
PERIODS = [(2018, 1)]
TODAY = "20260815"


def _stub_fetch(monkeypatch):
    """DART 호출을 고정 응답으로 대체 — 네트워크·키 불요."""
    def fake(keys, ki, corp_codes, year, q):
        return {cc: (100.0, 90.0, 1000.0, 900.0, 50.0, 45.0) for cc in corp_codes}, None
    monkeypatch.setattr(de, "_fetch_multi_with_rotation", fake)


def _rows(monkeypatch, **kw):
    _stub_fetch(monkeypatch)
    return de.collect_all_financials_batched(
        ["k"], CORP, PERIODS, sleep=0, today=TODAY, **kw)


def test_default_mode_stamps_today(monkeypatch):
    """일간 수집: 오늘 알게 된 값이므로 today 가 맞다."""
    rows = _rows(monkeypatch)
    assert rows, "고정 응답이면 행이 나와야 한다"
    for r in rows:
        assert r[3] == TODAY


def test_avail_mode_stamps_avail_date(monkeypatch):
    """과거 백필: 공시된 날 알 수 있었던 값이므로 avail_date."""
    rows = _rows(monkeypatch, knowledge_date="avail")
    assert rows
    for r in rows:
        avail, kd = r[2], r[3]
        assert kd == avail
        assert kd != TODAY


def test_avail_mode_survives_a_point_in_time_read(monkeypatch):
    """avail 모드로 넣은 2018 행은 2019 시점 as-of 조회에 잡혀야 한다."""
    rows = _rows(monkeypatch, knowledge_date="avail")
    asof = "20190101"
    visible = [r for r in rows if r[3] <= asof]
    assert visible, "avail 모드인데 과거 시점에서 안 보이면 백필이 무의미하다"

    stamped_today = _rows(monkeypatch)
    assert not [r for r in stamped_today if r[3] <= asof], (
        "today 모드는 과거 시점에서 안 보이는 게 정상 — 이 대비가 플래그의 존재 이유"
    )


def test_done_periods_still_skips(monkeypatch):
    """이미 수집한 (code, period)는 모드와 무관하게 건너뛴다."""
    rows = _rows(monkeypatch, knowledge_date="avail",
                 done_periods={("000020", "2018Q1")})
    assert [r[0] for r in rows] == ["000030"]
