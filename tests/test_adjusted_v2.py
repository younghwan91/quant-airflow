"""daily_bars_adj_v2 계산 — 029 (DB 불요, 순수 함수만)."""

from __future__ import annotations

from datetime import date

import pytest

from collectors import adjusted_v2 as av

D = date


def test_adjust_multiplies_only_days_before_event():
    # 005930 2018-05-04 50:1 분할(−98%) — 그 전날까지 × 0.02, 그날부터 그대로
    rows = [(D(2018, 5, 3), 2650000, 2660000, 2600000, 2650000, 1000, 10),
            (D(2018, 5, 4), 53000, 53900, 51800, 51900, 39565391, 20)]
    out = av.adjust(rows, {D(2018, 5, 4): -98.0})
    assert out[0][4] == pytest.approx(53000) and out[0][5] == 50000 and out[0][7] == 2650000
    assert out[0][8] == pytest.approx(0.02)
    assert out[1][4] == 51900 and out[1][8] == 1.0 and out[1][6] == 20


def test_adjust_compounds_later_events_and_skips_broken_rate():
    rows = [(D(2020, 1, d), 100, 100, 100, 100, 10, 1) for d in (1, 2, 3)]
    out = av.adjust(rows, {D(2020, 1, 2): 400.0, D(2020, 1, 3): -50.0, D(2020, 1, 1): -100.0})
    assert [r[8] for r in out] == [pytest.approx(2.5), pytest.approx(0.5), 1.0]


def test_krx_patch_uses_minute_when_basis_matches():
    row = (D(2026, 10, 8), 269500 * 0.5, 0, 0, 263000 * 0.5, 0, 0, 263000, 0.5)
    r, src = av.krx_patch(row, (269500.0, 270000.0, 263000.0, 262000.0, 19_000_000, 5.0e12))
    assert src == "minute_krx"
    assert r[4] == pytest.approx(131000) and r[7] == 262000 and r[5] == 38_000_000 and r[6] == 5_000_000


def test_krx_patch_nulls_instead_of_guessing():
    row = (D(2026, 10, 8), 1000.0, 0, 0, 1000.0, 5, 1, 1000, 1.0)
    r, src = av.krx_patch(row, None)
    assert src == "no_krx_source" and r[1:8] == (None,) * 7
    r, src = av.krx_patch(row, (5000.0, 5000.0, 5000.0, 5000.0, 1, 1.0))   # 분봉이 병합 뒤 기준
    assert src == "minute_basis_mismatch" and r[4] is None


def test_build_code_patches_only_from_krx_only_date():
    rows = [(D(2026, 9, 11), 100, 110, 90, 105, 10, 1), (D(2026, 9, 14), 100, 120, 90, 111, 10, 1)]
    out = av.build_code("000001", rows, {}, {("000001", D(2026, 9, 14)): (100.0, 110.0, 95.0, 104.0, 9, 1e6)})
    assert [(r[1], r[5], r[10]) for r in out] == [(D(2026, 9, 11), 105, "kiwoom_raw"),
                                                   (D(2026, 9, 14), 104.0, "minute_krx")]
    assert len(out[0]) == len(av.COLUMNS)


def test_delisted_rows_keep_naver_adjusted_prices_without_factor():
    out = av.delisted_rows("003450", [(D(2016, 9, 9), 10, 11, 9, 10, 5, 1)], {})
    assert out == [("003450", D(2016, 9, 9), 10, 11, 9, 10, 5, 1, None, None, "naver_adj", "naver_adj")]


def test_infer_events_from_kiwoom_ratio_step():
    # 001530 2022-04-19: 원주가 284,500 → 27,550(10:1 분할), 키움 수정주가는 27,483 → 26,761 — 이벤트 행 없음
    rows = [(D(2022, 4, 18), 284500, 0, 0, 284500, 0, 0), (D(2022, 4, 19), 27550, 0, 0, 27700, 5, 1)]
    adj = {D(2022, 4, 18): 27483.0, D(2022, 4, 19): 26761.0}
    assert av.infer_events(rows, adj, {}) == {D(2022, 4, 19): pytest.approx(-90.0, abs=0.01)}
    assert av.infer_events(rows, adj, {D(2022, 4, 19): -90.0}) == {}        # 키움이 준 값이 있으면 그대로


def test_infer_events_ignores_seam_and_real_moves():
    rows = [(D(2020, 1, 1), 100, 0, 0, 100, 1, 1), (D(2020, 1, 2), 100, 0, 0, 200, 1, 1)]
    assert av.infer_events(rows, {D(2020, 1, 1): 100.0, D(2020, 1, 2): 200.0}, {}) == {}   # 키움도 같이 뛴다
    assert av.infer_events(rows, {D(2020, 1, 1): 10.0, D(2020, 1, 2): 200.0}, {}) == {}    # 수집 이음매(키움 +1900%)


def test_krx_patch_keeps_halted_day_without_minutes():
    row = (D(2026, 10, 8), 1000.0, 1000.0, 1000.0, 1000.0, 0, 0, 1000, 1.0)
    assert av.krx_patch(row, None) == (row, "halted")
