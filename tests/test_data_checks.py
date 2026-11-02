"""collectors.data_checks — 검사들이 현재 모양의 데이터는 통과시키고, 위반을 주입하면 실패하는지.

통과만 보는 테스트는 규칙이 죽어도 초록이다 — 검사마다 위반 주입 테스트를 짝으로 둔다.
"""

from __future__ import annotations

import datetime as dt
import pathlib
import re

import pytest

from collectors import data_checks as dc
from collectors import storage


@pytest.fixture(autouse=True)
def _shares_checks_out_of_the_way(request, monkeypatch):
    """세션·항등식 쪽 테스트의 시드 DB 에는 주식수 표가 없다 — 주식수 검사는 아래 test_shares_* 가 따로 본다."""
    if request.node.name.startswith("test_shares_"):
        return
    monkeypatch.setattr(dc, "fetch_shares_canaries", lambda con: {
        (c, d): (s, k) for c, d, s, k in dc.SHARES_CANARIES})
    monkeypatch.setattr(dc, "fetch_shares_missing", lambda con, a, b: {})


T = dt.time
AFTER_DAY = dt.date(2026, 10, 8)
BEFORE_DAY = dt.date(2026, 9, 11)


def _bars(*times: str, n: int = 10) -> dict[dt.time, int]:
    return {T.fromisoformat(t): n for t in times}


NORMAL_AFTER = ("09:00", "12:00", "15:20", "15:30", "15:32", "15:35", "16:00", "18:00", "19:59", "20:02")
#: 새 세션 한 분 — NXT 프리마켓이 섞이면 매분 ~1,000종목이다.
NEW_SESSION_ROWS = 1_000
NORMAL_BEFORE = ("09:00", "12:00", "15:20", "15:30", "15:35")


# ── 세션 범위 ───────────────────────────────────────────────────────────────


def test_session_passes_regular_and_aftermarket():
    assert dc.check_session(AFTER_DAY, _bars(*NORMAL_AFTER)).ok
    assert dc.check_session(BEFORE_DAY, _bars(*NORMAL_BEFORE)).ok


@pytest.mark.parametrize("bad", ["08:00", "08:30", "08:59", "15:36", "15:50", "15:59", "20:11", "20:30"])
def test_session_fails_on_injected_out_of_session_bar(bad):
    r = dc.check_session(AFTER_DAY, {**_bars(*NORMAL_AFTER), **_bars(bad, n=NEW_SESSION_ROWS)})
    assert not r.ok
    assert r.kind == "session"
    assert bad in r.detail


def test_session_fails_on_new_premarket_session_all_codes():
    # 진짜 새 세션: 08:00~08:59 매분 전 종목 — 종목 수를 실제로 넘겨도 실패
    premarket = {T(8, m): NEW_SESSION_ROWS for m in range(60)}
    r = dc.check_session(AFTER_DAY, {**_bars(*NORMAL_AFTER), **premarket}, outside_codes=NEW_SESSION_ROWS)
    assert not r.ok
    assert "60000/" in r.detail


def test_session_passes_stray_late_bars():
    # 실측: 20:00~20:02 지연 체결 1~4행이 21거래일 중 12일 — 창 안(20:10 까지)
    stray = {T(20, 0): 1, T(20, 1): 3, T(20, 2): 2, T(20, 7): 1}
    r = dc.check_session(AFTER_DAY, {**_bars(*NORMAL_AFTER), **stray})
    assert r.ok and "모두 세션 안" in r.detail
    # 창 밖 1~2행 떠돌이 봉도 통과하되 상세에 남긴다
    r = dc.check_session(AFTER_DAY, {**_bars(*NORMAL_AFTER), T(20, 31): 1, T(8, 40): 1})
    assert r.ok
    assert "떠돌이" in r.detail and "20:31" in r.detail and "08:40" in r.detail


@pytest.mark.parametrize(("rows_per_min", "minutes", "codes", "ok"), [
    (1, 49, 19, True),    # 49행·19종목 — 문턱 밑
    (10, 5, 5, False),    # 50행 — 행 문턱
    (1, 20, 20, False),   # 20종목 — 종목 문턱
])
def test_session_outside_thresholds(rows_per_min, minutes, codes, ok):
    outside = {T(8, m): rows_per_min for m in range(minutes)}
    r = dc.check_session(AFTER_DAY, {**_bars(*NORMAL_AFTER), **outside}, outside_codes=codes)
    assert r.ok is ok


def test_session_unknown_codes_falls_back_to_rows():
    # outside_codes 없이 부르면 행 수를 종목 수로 본다(보수적) — 20행이면 종목 문턱에 걸린다
    assert not dc.check_session(AFTER_DAY, {**_bars(*NORMAL_AFTER), **{T(8, m): 1 for m in range(20)}}).ok
    assert dc.check_session(AFTER_DAY, {**_bars(*NORMAL_AFTER), **{T(8, m): 1 for m in range(19)}}).ok


def test_session_fails_on_aftermarket_bar_before_aftermarket_start():
    r = dc.check_session(BEFORE_DAY, {**_bars(*NORMAL_BEFORE), **_bars("16:30", n=NEW_SESSION_ROWS)})
    assert not r.ok


def test_session_delayed_open_days_shift_one_hour():
    # 2025-11-13 수능일 10:00~16:35, 2026-01-02 신년 개장일 10:00~15:35 — 실측 그대로 통과
    csat_late = _bars("15:21", "15:29", n=700)  # 마감 지연일엔 15:21~15:29 에도 연속매매 봉이 있다
    assert dc.check_session(dt.date(2025, 11, 13), {**csat_late, **_bars("10:00", "16:20", "16:30", "16:32", "16:35")}).ok
    assert dc.check_session(dt.date(2026, 1, 2), _bars("10:00", "15:30", "15:35")).ok
    # 지연일에도 창 밖은 실패
    big = NEW_SESSION_ROWS
    assert not dc.check_session(dt.date(2025, 11, 13), {**csat_late, **_bars("10:00", "16:35"), **_bars("16:50", n=big)}).ok
    # 개장만 늦은 날(신년)은 마감이 그대로라 15:36~ 봉은 실패
    assert not dc.check_session(dt.date(2026, 1, 2), {**_bars("10:00", "15:35"), **_bars("16:10", n=big)}).ok


def test_session_new_year_open_delay_keeps_normal_close_and_aftermarket():
    # 신년 개장일은 개장만 늦다 — 애프터마켓 시대(2027-01-04 가정)에도 16:00~ 봉이 통과해야 하고
    # 15:36~15:59 는 여전히 창 밖이다.
    day = dt.date(2027, 1, 4)
    assert dc.check_session(day, _bars("10:00", "15:30", "15:35", "16:00", "16:45", "19:59")).ok
    assert not dc.check_session(day, {**_bars("10:00", "15:30", "16:00"), **_bars("15:45", n=NEW_SESSION_ROWS)}).ok


def test_close_bar_window_by_day_profile():
    normal = _bars(*NORMAL_AFTER)
    new_year = _bars("10:00", "15:30", "15:35", "15:21", n=1)  # 15:21 떠돌이 1행은 마감 지연이 아니다
    csat = {**_bars("10:00", "15:30", "16:30", "16:35"), **_bars("15:21", "15:25", n=700)}
    assert dc.close_bar_window(normal) == (T(15, 30), T(15, 32))
    assert dc.close_bar_window(new_year) == (T(15, 30), T(15, 32))
    assert dc.close_bar_window(csat) == (T(16, 30), T(16, 32))


def test_session_fails_on_empty_day():
    r = dc.check_session(AFTER_DAY, {})
    assert not r.ok and r.kind == "sample"


# ── 기준가 항등식 ────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("price", "tick"),
    [(1_999, 1), (2_000, 5), (4_995, 5), (5_000, 10), (19_990, 10), (20_000, 50),
     (49_950, 50), (50_000, 100), (199_900, 100), (200_000, 500), (499_500, 500), (500_000, 1_000)],
)
def test_krx_tick(price, tick):
    assert dc.krx_tick(price) == tick


PRE = dt.date(2023, 1, 24)   # 호가 개편 전 마지막 날
POST = dt.date(2023, 1, 25)


@pytest.mark.parametrize(
    ("price", "tick"),
    [(999, 1), (1_000, 5), (4_995, 5), (5_000, 10), (9_990, 10), (10_000, 50), (49_950, 50),
     (50_000, 100), (99_900, 100), (100_000, 500), (499_500, 500), (500_000, 1_000)],
)
def test_krx_tick_before_reform_is_kospi_table(price, tick):
    assert dc.krx_tick(price, PRE) == tick


def test_krx_tick_switches_on_reform_day():
    assert dc.krx_tick(1_500, PRE) == 5 and dc.krx_tick(1_500, POST) == 1
    assert dc.krx_tick(150_000, PRE) == 500 and dc.krx_tick(150_000, POST) == 100
    assert dc.krx_tick(150_000) == 100  # 날짜 없으면 개편 뒤 표


def test_tick_round_half_up_like_sql_numeric():
    # migration 031 검증 쿼리와 같은 값: krx_tick_round(150250, PRE/POST) = 150500 | 150300
    assert dc.tick_round(150_250, PRE) == 150_500
    assert dc.tick_round(150_250, POST) == 150_300
    assert dc.tick_round(2_002.5, POST) == 2_005  # .5 는 올림(SQL round(numeric))


def test_base_close_recovers_regular_close():
    # 005930 10-07 supply_demand: close 269,000 · flu_rt −110 → 272,000 = 10-06 15:30 분봉(통합 종가는 273,000)
    assert dc.base_close(269_000, -110, dt.date(2026, 10, 6)) == 272_000  # 실측 행 그대로
    assert dc.base_close(1_234, 0) == 1_234


def test_base_close_falls_back_to_finer_grid():
    # 스팩(2025-03~ 1원 단위): 기준가 2,027 → 다음 날 2,030, flu_rt = round(14.8) = 15. 표 단위 5원으로는 2,025(24.7bp)라 못 맞춘다.
    assert dc.base_close(2_030, 15, dt.date(2026, 3, 2)) == 2_027
    # 개편 전 코스닥 5만원 이상 100원: 150,300 → 155,000, flu_rt 313. 코스피 표(500원)의 150,500 은 299bp 라 못 맞춘다.
    assert dc.base_close(155_000, 313, dt.date(2022, 6, 1)) == 150_300
    # 진짜 값이 굵은 격자 위면 굵은 격자 값 그대로(코스피 2017 005930: 2,099,000 → 2,060,000, flu_rt −186)
    assert dc.base_close(2_060_000, -186, dt.date(2017, 3, 30)) == 2_099_000


def test_base_close_returns_none_when_not_unique_or_unreproducible():
    # 046070 2026-09-10 실측: close 410 · flu_rt −9,950 — 1bp 가 기준가의 2% 라 이웃 격자값도 맞는다
    assert dc.base_close(410, -9_950, dt.date(2026, 9, 9)) is None
    # 1원 격자로도 flu_rt 를 못 맞춘다(기준가 재설정)
    assert dc.base_close(3, 4_000, POST) is None
    assert dc.base_close(None, 10) is None and dc.base_close(1_000, None) is None
    assert dc.base_close(1_000, -10_000) is None and dc.base_close(0, 0) is None


def test_base_close_on_split_day_gives_adjusted_base():
    # 005930 2018-05-04(50:1 분할 첫날) close 51,900 · flu_rt −208 → 53,000 = 2,650,000/50 — 그래서 뷰는 base_reset 으로 뺀다
    assert dc.base_close(51_900, -208, dt.date(2018, 5, 3)) == 53_000


def _sd(code, day, close, flu):
    return (code, dt.date.fromisoformat(day), close, flu)


def test_derive_regular_close_src_cases():
    d1, d2, d3 = "2026-10-06", "2026-10-07", "2026-10-08"
    rows = [
        _sd("005930", d1, 273_000, 0), _sd("005930", d2, 269_000, -110), _sd("005930", d3, 263_000, -331),
        _sd("A_DEL", d1, 1_000, 0), _sd("A_DEL", d2, 1_010, 100),                     # d3 없음 = 폐지
        _sd("B_GAP", d1, 5_000, 0), _sd("B_GAP", d3, 5_100, 200),                      # d2 행 없음
        _sd("C_EVT", d1, 900, 0), _sd("C_EVT", d2, 4_500, 0), _sd("C_EVT", d3, 4_500, 0),  # d2 에 5:1 병합
        _sd("D_BAD", d1, 410, 0), _sd("D_BAD", d2, 410, -9_950), _sd("D_BAD", d3, 410, 0),
    ]
    out = dc.derive_regular_close(rows, [("C_EVT", dt.date(2026, 10, 7))])
    D = dt.date.fromisoformat
    assert out["005930", D(d1)] == (272_000, "identity")
    assert out["005930", D(d3)] == (None, "pending")
    assert out["A_DEL", D(d1)] == (1_000, "identity")
    assert out["A_DEL", D(d2)] == (None, "no_next")
    assert out["B_GAP", D(d1)] == (None, "gap")
    assert out["C_EVT", D(d1)] == (None, "base_reset")
    assert out["C_EVT", D(d2)] == (4_500, "identity")
    assert out["D_BAD", D(d1)] == (None, "unmatched")


# ── migration 031 과 파이썬 판이 같은 표를 쓰는지(한쪽만 고치면 깨진다) ──────────

ROOT = pathlib.Path(__file__).resolve().parents[1]
MIGRATION_031 = ROOT / "sql" / "migrations" / "031_regular_close.sql"


def _sql_constants(text: str) -> dict:
    """031 의 krx_tick_size 두 갈래 표·격자·날짜·허용 폭을 뽑는다."""
    body = text[text.index("CREATE OR REPLACE FUNCTION krx_tick_size"):text.index("CREATE OR REPLACE FUNCTION krx_tick_round")]
    # 안쪽 CASE 두 개 — 첫째가 개편 뒤(WHEN p_day >= ...), 둘째가 개편 전(ELSE) 표
    post_part, pre_part = re.findall(r"CASE WHEN p_price.*?ELSE \d+ END", body, flags=re.S)

    def bands(part: str):
        pairs = re.findall(r"p_price < (\d+) THEN (\d+)", part)
        top = re.search(r"ELSE (\d+) END", part)
        return tuple((int(a), int(b)) for a, b in pairs), int(top.group(1))

    base = text[text.index("FUNCTION krx_base_close"):text.index("CREATE OR REPLACE VIEW")]
    return {
        "reform": re.search(r"p_day >= DATE '([\d-]+)'", body).group(1),
        "post": bands(post_part),
        "pre": bands(pre_part),
        "grid": tuple(int(x) for x in re.search(r"ARRAY\[([\d, ]+)\]", base).group(1).split(",")),
        "tol": set(re.findall(r"- p_flu_rt\) <= ([\d.]+)", base)),
        "aftermarket": set(re.findall(r"s\.date (?:<|>=) DATE '([\d-]+)'", text)),
    }


def _py_constants() -> dict:
    return {
        "reform": dc.TICK_REFORM.isoformat(),
        "post": (dc.TICK_BANDS, dc.TOP_TICK),
        "pre": (dc.TICK_BANDS_PRE_REFORM, dc.TOP_TICK),
        "grid": dc.BASE_TICK_GRID,
        "tol": {str(float(dc.FLU_RT_TOL_BP))},
        "aftermarket": {dc.AFTERMARKET_START.isoformat()},
    }


def test_migration_031_matches_python_constants():
    assert _sql_constants(MIGRATION_031.read_text()) == _py_constants()


@pytest.mark.parametrize(
    ("old", "new"),
    [("p_price < 20000 THEN 10", "p_price < 20000 THEN 5"),        # 개편 뒤 표
     ("p_price < 100000 THEN 100", "p_price < 100000 THEN 50"),     # 개편 전 표
     ("DATE '2023-01-25'", "DATE '2023-01-26'"),
     ("ARRAY[1000, 500, 100, 50, 10, 5, 1]", "ARRAY[1000, 100, 10, 1]"),
     ("WHERE s.date >= DATE '2026-09-14'", "WHERE s.date >= DATE '2026-09-15'")],
)
def test_migration_031_drift_is_detected(old, new):
    text = MIGRATION_031.read_text()
    assert old in text
    assert _sql_constants(text.replace(old, new, 1)) != _py_constants()


def test_init_timescale_has_migration_031_ddl_verbatim():
    mig = MIGRATION_031.read_text()
    ddl = mig[mig.index("CREATE OR REPLACE FUNCTION krx_tick_size"):mig.index("-- 검증(적용 뒤):")].rstrip()
    assert ddl in (ROOT / "sql" / "init_timescale.sql").read_text()


def _identity_fixture(n: int = 400):
    regular = {f"{i:06d}": 10_000 + 10 * i for i in range(n)}
    rows = []
    for code, prev in regular.items():
        close = prev + 50  # 애프터마켓이 있어도 close 는 아무 값이나 될 수 있다
        flu = (close / prev - 1) * 1e4
        rows.append((code, float(close), round(flu, 2)))
    return regular, rows


def test_identity_passes_when_base_is_prev_regular_close():
    regular, rows = _identity_fixture()
    r = dc.check_base_price_identity(BEFORE_DAY, AFTER_DAY, regular, rows)
    assert r.ok, r.detail
    assert "0/400" in r.detail


def test_identity_fails_when_flu_rt_base_changes():
    # 위반 주입: 기준가가 애프터마켓 종가(정규장 +1%)로 바뀐 경우 — 5% 종목
    regular, rows = _identity_fixture()
    bad = []
    for i, (code, close, _) in enumerate(rows):
        if i % 20 == 0:
            base = regular[code] * 1.01
            bad.append((code, close, round((close / base - 1) * 1e4, 2)))
        else:
            bad.append(rows[i])
    r = dc.check_base_price_identity(BEFORE_DAY, AFTER_DAY, regular, bad)
    assert not r.ok
    assert "20/400" in r.detail


def test_identity_tolerates_few_mismatches_under_threshold():
    regular, rows = _identity_fixture()
    code, close, _ = rows[0]
    rows[0] = (code, close, 0.0)  # 권리락 등 1 종목
    assert dc.check_base_price_identity(BEFORE_DAY, AFTER_DAY, regular, rows).ok


def test_identity_fails_on_too_few_pairs():
    regular, rows = _identity_fixture(n=50)
    r = dc.check_base_price_identity(BEFORE_DAY, AFTER_DAY, regular, rows)
    assert not r.ok
    assert "표본" in r.detail


def test_identity_skips_null_rows():
    regular, rows = _identity_fixture()
    rows.append(("999999", None, None))
    regular["999999"] = 1_000
    assert dc.check_base_price_identity(BEFORE_DAY, AFTER_DAY, regular, rows).ok


# ── 원주가 카나리아 ─────────────────────────────────────────────────────────


def test_canary():
    assert dc.check_raw_price_canary(2_060_000).ok
    assert dc.check_raw_price_canary(None).ok
    assert not dc.check_raw_price_canary(41_200).ok  # 위반 주입: 수정주가가 섞임


# ── 조정주가 대 원주가 ─────────────────────────────────────────────────────

D = dt.date
P, T1 = D(2026, 9, 10), D(2026, 9, 11)   # 애프터마켓 전 — 전일 원 종가 = 기준가


def _cell(code="000001", *, adj=(5_000.0, 5_100.0), raw=(10_000.0, 10_200.0), flu=None, prev=P, day=T1):
    """flu 를 주면 기준가 = raw(t)/(1+flu/1e4)(수급 close = 원 종가)."""
    base = None if flu is None else raw[1] / (1 + flu / 1e4)
    return dc.AdjCell(code, prev, day, *adj, *raw, flu, base)


def _clean(n=dc.ADJ_MIN_PAIRS):
    return [_cell(f"{i:06d}", flu=200) for i in range(n)]


def _adj_check(cells, resets=(), **kw):
    return dc.check_adjusted_vs_raw(P, T1, cells, resets, **kw)


def test_pair_adjusted_cells_uses_previous_row_and_supply_base():
    adj = [("A", D(2026, 9, 8), 100.0), ("A", D(2026, 9, 9), 110.0), ("A", D(2026, 9, 11), 121.0),  # 09-10 정지
           ("B", D(2026, 9, 10), None), ("B", D(2026, 9, 11), 50.0)]
    raw = {("A", D(2026, 9, 9)): 1_100, ("A", D(2026, 9, 11)): 1_210}
    cells = dc.pair_adjusted_cells(adj, raw, D(2026, 9, 9), {("A", D(2026, 9, 11)): (1_210, 1_000.0)})
    assert [(c.code, c.prev, c.day) for c in cells] == [("A", D(2026, 9, 8), D(2026, 9, 9)), ("A", D(2026, 9, 9), D(2026, 9, 11))]
    first, second = cells
    assert first.raw_prev is None and first.raw == 1_100 and first.base is None
    assert second.raw_prev == 1_100 and second.flu_rt == 1_000 and second.base == pytest.approx(1_100)


def test_adjusted_vs_raw_passes_when_returns_match():
    r = _adj_check(_clean(), strict=True)
    assert r.ok and not r.warn, r.detail
    assert f"이음매 0/{dc.ADJ_MIN_PAIRS}" in r.detail


def test_adjusted_vs_raw_collection_seam_fails_when_strict_warns_otherwise():
    # 117670 2026-10-06 모양: 조정주가 10,420 → 5,155, 원주가 10,420 → 10,310, 이벤트는 10-21(창 밖)
    seam = _cell("117670", adj=(10_420.0, 5_155.0), raw=(10_420.0, 10_310.0), flu=-106)
    cells = [*_clean(), seam]
    r = _adj_check(cells, [("117670", D(2026, 10, 21))], strict=True)
    assert not r.ok and r.kind == "seam"
    assert "이음매 1/" in r.detail and "117670" in r.detail
    r = _adj_check(cells, strict=False)
    assert r.ok and r.warn and "117670" in r.detail


def test_adjusted_vs_raw_zeroed_limit_day_fails():
    # 001380 2025-03-26 모양: 원주가 3,500 → 2,450 하한가인데 조정주가 4,909 → 4,909
    zeroed = _cell("001380", adj=(4_909.0, 4_909.0), raw=(3_500.0, 2_450.0), flu=-3_000)
    assert not _adj_check([*_clean(), zeroed], strict=True).ok
    # 같은 하한가를 조정주가도 따라가면 통과
    kept = _cell("001380", adj=(4_909.0, 3_436.3), raw=(3_500.0, 2_450.0), flu=-3_000)
    assert _adj_check([*_clean(), kept], strict=True).ok


def test_adjusted_vs_raw_event_in_prev_t_is_skipped():
    split = _cell("005930", adj=(53_000.0, 51_900.0), raw=(2_650_000.0, 51_900.0), flu=-208)
    r = _adj_check([*_clean(), split], [("005930", T1)], strict=True)
    assert r.ok and "이벤트 1" in r.detail
    # 이벤트가 (prev, t] 밖이면(t 다음 날) 설명이 안 된다 — 원주가가 이어져 있으니 이음매
    seam = _cell("005930", adj=(10_000.0, 5_000.0), raw=(10_000.0, 10_000.0), flu=0)
    assert not _adj_check([*_clean(), seam], [("005930", D(2026, 9, 14))], strict=True).ok


def test_adjusted_vs_raw_missing_raw_is_skipped_not_failed():
    delisted = dc.AdjCell("999999", P, T1, 1_000.0, 100.0, None, None)
    r = _adj_check([*_clean(), delisted], strict=True)
    assert r.ok and "원주가 없음(폐지) 1" in r.detail


def test_adjusted_vs_raw_base_reset_without_event_is_skipped():
    # 298000 2026-06-15: 원주가 38,900 → 55,000(+41%), 기준가 77,800(×2 재설정, 이벤트 없음), 조정주가 −29.3% 가 맞다
    reset = _cell("298000", adj=(77_800.0, 55_000.0), raw=(38_900.0, 55_000.0), flu=-2_931)
    r = _adj_check([*_clean(), reset], strict=True)
    assert r.ok and "기준가 재설정 1" in r.detail


def test_adjusted_vs_raw_out_of_limit_is_skipped():
    # 정리매매 등 ±30% 밖(flu −91%) — 원주가 자체가 끊겨 판단하지 않는다
    crash = _cell("005110", adj=(1_254.0, 1_254.0), raw=(1_254.0, 112.0), flu=-9_107)
    assert _adj_check([*_clean(), crash], strict=True).ok
    # 수급 행이 없으면 원주가 비로 잰다: +43% 는 띠 밖, +28% 는 안(조정주가 0% 면 |dev| 21.9%)
    assert _adj_check([*_clean(), _cell("X", adj=(100.0, 100.0), raw=(100.0, 143.0))], strict=True).ok
    assert not _adj_check([*_clean(), _cell("X", adj=(100.0, 100.0), raw=(100.0, 128.0))], strict=True).ok


def test_adjusted_vs_raw_aftermarket_limit_uses_flu_rt_not_raw_ratio():
    # 0035S0 2026-09-30: 전일 통합 종가 27,050(정규장 29,900), 상한가 38,850 = 기준가 대비 +29.9%, 원주가 비 +43.6%
    after_prev, after_day = D(2026, 9, 29), D(2026, 9, 30)
    zeroed = _cell("0035S0", adj=(38_850.0, 38_850.0), raw=(27_050.0, 38_850.0), flu=2_993, prev=after_prev, day=after_day)
    assert not _adj_check([*_clean(), zeroed], strict=True).ok
    kept = _cell("0035S0", adj=(27_050.0, 38_850.0), raw=(27_050.0, 38_850.0), flu=2_993, prev=after_prev, day=after_day)
    assert _adj_check([*_clean(), kept], strict=True).ok
    # 애프터마켓 등락으로 기준가가 전일 통합 종가와 35% 다른 날(087010 10-08)도 원주가는 이어진 것으로 본다
    gap = _cell("087010", adj=(96_900.0, 48_000.0), raw=(96_900.0, 91_500.0), flu=-2_994, prev=after_prev, day=after_day)
    assert not _adj_check([*_clean(), gap], strict=True).ok
    # 애프터마켓 전이면 같은 35% 차이는 기준가 재설정이다
    assert _adj_check([*_clean(), _cell("087010", adj=(96_900.0, 48_000.0), raw=(96_900.0, 91_500.0), flu=-2_994)],
                      strict=True).ok


def test_adjusted_vs_raw_too_few_pairs_fails_even_when_not_strict():
    r = _adj_check(_clean(n=10), strict=False)
    assert not r.ok and r.kind == "sample"


def test_adjusted_canaries_expected_state():
    broken = {("025620", D(2021, 7, 29)): 15.39, ("258830", D(2021, 7, 29)): -0.8, ("001380", D(2025, 3, 26)): 0.0,
              ("052670", D(2026, 2, 9)): 300.48}
    fixed = {("025620", D(2021, 7, 29)): 0.017, ("258830", D(2021, 7, 29)): 0.0, ("001380", D(2025, 3, 26)): -0.30,
             ("052670", D(2026, 2, 9)): -0.80}
    loose = dc.check_adjusted_canaries(broken, strict=False)
    assert len(loose) == 4 and all(r.ok and r.warn for r in loose)
    assert not any(r.ok for r in dc.check_adjusted_canaries(broken, strict=True))
    for strict in (False, True):
        rs = dc.check_adjusted_canaries(fixed, strict=strict)
        assert all(r.ok and not r.warn for r in rs)
    assert "ADJUSTED_STRICT" in dc.check_adjusted_canaries(fixed, strict=False)[0].detail
    assert all(r.ok and not r.warn for r in dc.check_adjusted_canaries({}, strict=True))  # 행 없음


def test_adjusted_strict_default_is_on_after_swing_it_fix():
    # 2026-11-02 swing-it PR #12 배포·재계산 뒤 [WARN] 0건을 확인하고 뒤집었다(모듈 docstring 5)
    assert dc.ADJUSTED_STRICT is True


# ── 조회 경로(sqlite) ─────────────────────────────────────────────────────


def _seed(
    tmp_path, *, extra_bar: str | None = None, extra_codes: int = 1, extra_day: dt.date | None = None,
    premarket: bool = False, canary: int | None = 2_060_000, shift_base: bool = False,
    adj_seam_day: dt.date | None = None,
):
    """``extra_bar``: 그 분에 ``extra_codes`` 종목 봉을 더한다(``extra_day`` 기본 T). ``premarket``: T 의
    08:00~08:59 전 종목 봉(진짜 새 세션). 조정주가는 원주가 × 0.5 — ``adj_seam_day`` 면 000000 의 그날부터 × 0.25
    (이벤트 없는 이음매)."""
    path = tmp_path / "q.db"
    con = storage.connect(path)
    con.execute("CREATE TABLE minute_bars (code TEXT, ts TEXT, open REAL, high REAL, low REAL, close REAL,"
                " volume INTEGER, trade_value REAL, PRIMARY KEY (code, ts))")
    dm1, d0, d1 = dt.date(2026, 10, 6), dt.date(2026, 10, 7), AFTER_DAY
    for i in range(dc.IDENTITY_MIN_PAIRS):
        code = f"{i:06d}"
        prev = 10_000 + 10 * i
        con.execute("INSERT INTO supply_demand (code, date, close, flu_rt, source) VALUES (?,?,?,0, 'kiwoom')",
                    (code, dm1.isoformat(), prev))
        for day in (dm1, d0, d1):
            factor = 0.25 if adj_seam_day and code == "000000" and day >= adj_seam_day else 0.5
            con.execute("INSERT INTO daily_bars_raw (code, date, close) VALUES (?,?,?)", (code, day.isoformat(), prev))
            con.execute("INSERT INTO daily_bars_adjusted (code, date, close) VALUES (?,?,?)",
                        (code, day.isoformat(), prev * factor))
        for day in (d0, d1):
            for hm, px in (("09:00", prev - 100), ("15:30", prev), ("19:59", prev + 300)):
                con.execute("INSERT INTO minute_bars VALUES (?,?,?,?,?,?,?,?)",
                            (code, f"{day} {hm}:00", px, px, px, px, 1, px))
        close = prev + 300  # 애프터마켓 섞인 종가
        base = prev * 1.01 if shift_base else prev
        for day, cl, flu in ((d0, prev, 0.0), (d1, close, round((close / base - 1) * 1e4, 2))):
            con.execute("INSERT INTO supply_demand (code, date, close, flu_rt, source) VALUES (?,?,?,?, 'kiwoom')",
                        (code, day.isoformat(), cl, flu))
    if extra_bar:
        for i in range(extra_codes):
            con.execute("INSERT INTO minute_bars VALUES (?,?,?,?,?,?,?,?)",
                        (f"{i:06d}", f"{extra_day or d1} {extra_bar}:00", 1, 1, 1, 1, 1, 1))
    if premarket:
        for i in range(dc.IDENTITY_MIN_PAIRS):
            for m in range(60):
                con.execute("INSERT INTO minute_bars VALUES (?,?,?,?,?,?,?,?)",
                            (f"{i:06d}", f"{d1} 08:{m:02d}:00", 1, 1, 1, 1, 1, 1))
    if canary is not None:
        con.execute("INSERT INTO daily_bars_raw (code, date, open, high, low, close, volume, trade_value)"
                    " VALUES ('005930', '2017-03-31', 0, 0, 0, ?, 0, 0)", (canary,))
    con.commit()
    con.close()
    return str(path)


ASOF = ["--asof", "2026-10-09"]


def test_main_passes_on_current_shape(tmp_path, capsys):
    assert dc.main(["--db", _seed(tmp_path), *ASOF]) == 0
    out = capsys.readouterr().out
    assert "identity 2026-10-07→2026-10-08" in out
    assert "불일치 0/300" in out


def test_main_fails_on_injected_premarket_session(tmp_path, capsys):
    assert dc.main(["--db", _seed(tmp_path, premarket=True), *ASOF]) == dc.EXIT_DATA_FAIL
    out = capsys.readouterr().out
    assert "[FAIL:세션 이탈] session 2026-10-08" in out
    assert "18000/" in out and "300종목" in out
    assert "— 세션 이탈 1" in out


def test_main_passes_stray_bars(tmp_path, capsys):
    # 떠돌이 1~2행(20:31·08:30)은 통과 — 1행 때문에 매일 경보가 울리던 결함(리뷰 M1)
    assert dc.main(["--db", _seed(tmp_path, extra_bar="20:31", extra_codes=2), *ASOF]) == 0
    assert "떠돌이" in capsys.readouterr().out
    assert dc.main(["--db", _seed(tmp_path / "b", extra_bar="08:30"), *ASOF]) == 0


def test_main_fails_on_wide_outside_minute_by_codes(tmp_path):
    # 한 분이라도 20종목 이상이면 새 세션 — 행 문턱(50) 밑이어도
    assert dc.main(["--db", _seed(tmp_path, extra_bar="08:30", extra_codes=dc.OUTSIDE_FAIL_CODES), *ASOF]) == 1


def test_main_checks_prev_day_session_too(tmp_path, capsys):
    # T 의 애프터마켓은 06:30 에 일부만 있다 — 다 적재된 T-1 에서 새 세션을 잡는다
    path = _seed(tmp_path, extra_bar="21:00", extra_codes=100, extra_day=dt.date(2026, 10, 7))
    assert dc.main(["--db", path, *ASOF]) == 1
    assert "[FAIL:세션 이탈] session 2026-10-07" in capsys.readouterr().out


def test_main_fails_on_injected_base_change(tmp_path, capsys):
    assert dc.main(["--db", _seed(tmp_path, shift_base=True), *ASOF]) == 1
    out = capsys.readouterr().out
    assert "[FAIL:항등식 불일치]" in out and "— 항등식 불일치 1" in out


def test_main_fails_on_injected_adjusted_canary(tmp_path):
    assert dc.main(["--db", _seed(tmp_path, canary=41_200), *ASOF]) == 1


def test_main_passes_without_canary_row(tmp_path):
    assert dc.main(["--db", _seed(tmp_path, canary=None), *ASOF]) == 0


def test_main_adjusted_seam_warns_then_fails_when_strict(tmp_path, capsys, monkeypatch):
    path = _seed(tmp_path, adj_seam_day=dt.date(2026, 10, 7))
    monkeypatch.setattr(dc, "ADJUSTED_STRICT", False)
    assert dc.main(["--db", path, *ASOF]) == 0
    out = capsys.readouterr().out
    assert "[WARN] adjusted_vs_raw 2026-10-07~2026-10-07" in out and "000000 2026-10-07" in out
    assert "(경고 1)" in out
    monkeypatch.setattr(dc, "ADJUSTED_STRICT", True)
    assert dc.main(["--db", path, *ASOF]) == dc.EXIT_DATA_FAIL
    out = capsys.readouterr().out
    assert "[FAIL:조정주가 이음매] adjusted_vs_raw" in out and "— 조정주가 이음매 1" in out


def test_main_adjusted_check_skips_t(tmp_path, monkeypatch):
    # T 의 원주가는 19:00 스냅숏 — T 칸의 어긋남은 보지 않는다(다음 날 창에서 본다)
    monkeypatch.setattr(dc, "ADJUSTED_STRICT", True)
    assert dc.main(["--db", _seed(tmp_path, adj_seam_day=AFTER_DAY), *ASOF]) == 0


def test_main_empty_adjusted_is_sample_shortage(tmp_path, capsys):
    path = _seed(tmp_path)
    con = storage.connect(path)
    con.execute("DELETE FROM daily_bars_adjusted")
    con.commit()
    con.close()
    assert dc.main(["--db", path, *ASOF]) == 1
    assert "[FAIL:표본 부족] adjusted_vs_raw" in capsys.readouterr().out


def test_main_too_few_pairs_is_sample_shortage(tmp_path, capsys):
    path = _seed(tmp_path)
    con = storage.connect(path)
    con.execute("DELETE FROM supply_demand WHERE date = '2026-10-08' AND code >= '000100'")
    con.commit()
    con.close()
    assert dc.main(["--db", path, *ASOF]) == 1
    assert "[FAIL:표본 부족] identity" in capsys.readouterr().out


# ── DSN 은 환경변수로만 ─────────────────────────────────────────────────────


def test_main_rejects_dsn_on_command_line():
    with pytest.raises(SystemExit) as e:
        dc.main(["--db", "postgresql://u:pw@localhost/x", *ASOF])  # allowlist-secret
    assert e.value.code == 2


def test_main_reads_db_from_env(tmp_path, monkeypatch):
    monkeypatch.setenv(dc.DSN_ENV, _seed(tmp_path))
    assert dc.main(ASOF) == 0


def test_main_without_db_is_connect_failure(monkeypatch, capsys):
    monkeypatch.delenv(dc.DSN_ENV, raising=False)
    assert dc.main(ASOF) == dc.EXIT_CONNECT_FAIL
    assert "[FAIL:접속 실패]" in capsys.readouterr().out


def test_main_connect_failure_is_distinct_and_masks_password(monkeypatch, capsys):
    pytest.importorskip("psycopg2")
    monkeypatch.setenv(dc.DSN_ENV, "postgresql://u:hunter2pw@127.0.0.1:1/x?connect_timeout=2")  # allowlist-secret
    assert dc.main(ASOF) == dc.EXIT_CONNECT_FAIL
    out = capsys.readouterr().out
    assert "[FAIL:접속 실패]" in out
    assert "hunter2pw" not in out


def test_daily_finalize_check_task_shape():
    """DAG: 상류가 실패해도 검사(all_done), 시간 제한, DSN 은 환경변수로(명령줄 --db 없음)."""
    import ast

    src = (pathlib.Path(__file__).parents[1] / "dags" / "daily_finalize.py").read_text()
    fn = next(n for n in ast.walk(ast.parse(src)) if isinstance(n, ast.FunctionDef) and n.name == "check_data_quality")
    kw = {k.arg: k.value for d in fn.decorator_list if isinstance(d, ast.Call) for k in d.keywords}
    assert ast.literal_eval(kw["trigger_rule"]) == "all_done"
    assert ast.literal_eval(kw["retries"]) == 0
    assert "execution_timeout" in kw
    body = ast.unparse(fn)
    assert "'--db'" not in body
    assert dc.DSN_ENV in body


# ── 상장주식수 ───────────────────────────────────────────────────────────────


def _good_shares():
    return {(c, d): (s, k) for c, d, s, k in dc.SHARES_CANARIES}


def test_shares_canaries_pass_when_values_and_first_known_match():
    got = dc.check_shares_canaries(_good_shares(), strict=True)
    assert len(got) == len(dc.SHARES_CANARIES) and all(r.ok and not r.warn for r in got)


def test_shares_canary_known_values_are_the_measured_ones():
    # 카나리아 표 자체가 조용히 바뀌지 않게(2026-11-02 실측) — 숫자는 공시에서 왔다
    table = {(c, str(d)): (s, str(k) if k else None) for c, d, s, k in dc.SHARES_CANARIES}
    assert table["009150", "2021-12-31"][0] == 74_693_696
    assert table["034730", "2021-12-31"][0] == 74_149_329
    assert table["066570", "2021-12-31"] == (163_647_814, "2022-03-16")
    assert table["068270", "2021-12-31"][1] == "2022-03-08"
    assert table["017670", "2021-12-31"][1] == "2022-03-16"


@pytest.mark.parametrize(
    ("code", "day", "value", "needle"),
    [
        ("009150", dt.date(2021, 12, 31), (None, None), "주식수 None"),            # 행이 없다
        ("034730", dt.date(2021, 12, 31), (70_360_297, None), "주식수 70360297"),  # 1분기 값
        ("068270", dt.date(2021, 12, 31), (None, dt.date(2022, 5, 12)), "처음 안 날 2022-05-12"),  # 늦은 도장
    ],
)
def test_shares_canary_warns_before_backfill_and_fails_after(code, day, value, needle):
    observed = _good_shares()
    observed[code, day] = value
    soft = [r for r in dc.check_shares_canaries(observed, strict=False) if code in r.name]
    assert soft[0].ok and soft[0].warn and needle in soft[0].detail and "SHARES_STRICT" in soft[0].detail
    hard = [r for r in dc.check_shares_canaries(observed, strict=True) if code in r.name]
    assert not hard[0].ok and hard[0].kind == "shares_canary"


def test_shares_missing_gate():
    by_year = {2021: (1000, 50), 2022: (1000, 51), 2023: (0, 0)}
    strict = {r.name: r for r in dc.check_shares_missing(by_year, strict=True)}
    assert strict["shares_missing 2021"].ok
    assert not strict["shares_missing 2022"].ok and strict["shares_missing 2022"].kind == "shares_missing"
    assert not strict["shares_missing 2023"].ok, "상장 종목 0 은 통과가 아니다(CLAUDE.md §5)"
    soft = dc.check_shares_missing(by_year, strict=False)
    assert all(r.ok for r in soft) and [r.warn for r in soft] == [False, True, True]


def test_shares_strict_stays_soft_until_backfill_is_done():
    # 뒤집는 조건 — 모듈 docstring 6. 백필·마이그레이션 전에 머지되면 매일 빨갛다
    assert dc.SHARES_STRICT is False


def test_shares_fetch_from_tables(tmp_path):
    con = storage.connect(tmp_path / "s.db")
    day = "2021-12-31"
    con.execute("INSERT INTO shares_outstanding_history(code,date,shares_outstanding,knowledge_date,source)"
                " VALUES('068270',?,137947128,'2022-05-12','dart')", (day,))
    con.execute("INSERT INTO shares_original_filing(code,date,first_public_date,rcept_no,shares_original)"
                " VALUES('068270',?,'2022-03-08','R',137947128)", (day,))
    con.execute("INSERT INTO shares_outstanding_history(code,date,shares_outstanding,knowledge_date,source)"
                " VALUES('066570',?,163647814,'2022-03-16','dart')", (day,))
    got = dc.fetch_shares_canaries(con)
    assert got["068270", dt.date(2021, 12, 31)] == (137947128, dt.date(2022, 3, 8))
    assert got["066570", dt.date(2021, 12, 31)] == (163647814, dt.date(2022, 3, 16))
    assert got["009150", dt.date(2021, 12, 31)] == (None, None)
    # 결측률: A 행 있음 · B 없음 · C 013 마커 · D 는 상장 마스터에 없음(폐지) — 분모 3, 결측 1
    for code in "ABC":
        con.execute("INSERT INTO stocks(code,name) VALUES(?,?)", (code, code))
    for code in "ABCD":
        con.execute("INSERT INTO daily_bars(code,date,open,high,low,close,volume,trade_value,source)"
                    " VALUES(?,'2021-12-30',1,1,1,1,1,1,'kiwoom')", (code,))
    con.execute("INSERT INTO shares_outstanding_history(code,date,shares_outstanding,source) VALUES('A','2021-09-30',5,'dart')")
    storage.mark_checked(con, f"{storage.CHECKED_DART_SHARES_YEAR}:2021", ["C"], "2026-11-02")
    assert dc.fetch_shares_missing(con, 2021, 2022) == {2021: (3, 1), 2022: (0, 0)}
    con.close()
