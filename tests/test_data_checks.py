"""collectors.data_checks — 세 검사가 현재 모양의 데이터는 통과시키고, 위반을 주입하면 실패하는지.

통과만 보는 테스트는 규칙이 죽어도 초록이다 — 검사마다 위반 주입 테스트를 짝으로 둔다.
"""

from __future__ import annotations

import datetime as dt
import pathlib
import re

import pytest

from collectors import data_checks as dc
from collectors import storage

T = dt.time
AFTER_DAY = dt.date(2026, 10, 8)
BEFORE_DAY = dt.date(2026, 9, 11)


def _bars(*times: str, n: int = 10) -> dict[dt.time, int]:
    return {T.fromisoformat(t): n for t in times}


NORMAL_AFTER = ("09:00", "12:00", "15:20", "15:30", "15:32", "15:35", "16:00", "18:00", "19:59", "20:02")
NORMAL_BEFORE = ("09:00", "12:00", "15:20", "15:30", "15:35")


# ── 세션 범위 ───────────────────────────────────────────────────────────────


def test_session_passes_regular_and_aftermarket():
    assert dc.check_session(AFTER_DAY, _bars(*NORMAL_AFTER)).ok
    assert dc.check_session(BEFORE_DAY, _bars(*NORMAL_BEFORE)).ok


@pytest.mark.parametrize("bad", ["08:00", "08:30", "08:59", "15:36", "15:50", "15:59", "20:03", "20:30"])
def test_session_fails_on_injected_out_of_session_bar(bad):
    r = dc.check_session(AFTER_DAY, _bars(*NORMAL_AFTER, bad))
    assert not r.ok
    assert bad in r.detail


def test_session_fails_on_aftermarket_bar_before_aftermarket_start():
    r = dc.check_session(BEFORE_DAY, _bars(*NORMAL_BEFORE, "16:30"))
    assert not r.ok


def test_session_delayed_open_days_shift_one_hour():
    # 2025-11-13 수능일 10:00~16:35, 2026-01-02 신년 개장일 10:00~15:35 — 실측 그대로 통과
    csat_late = _bars("15:21", "15:29", n=700)  # 마감 지연일엔 15:21~15:29 에도 연속매매 봉이 있다
    assert dc.check_session(dt.date(2025, 11, 13), {**csat_late, **_bars("10:00", "16:20", "16:30", "16:32", "16:35")}).ok
    assert dc.check_session(dt.date(2026, 1, 2), _bars("10:00", "15:30", "15:35")).ok
    # 지연일에도 창 밖은 실패
    assert not dc.check_session(dt.date(2025, 11, 13), {**csat_late, **_bars("10:00", "16:35", "16:50")}).ok
    # 개장만 늦은 날(신년)은 마감이 그대로라 15:36~ 봉은 실패
    assert not dc.check_session(dt.date(2026, 1, 2), _bars("10:00", "15:35", "16:10")).ok


def test_session_new_year_open_delay_keeps_normal_close_and_aftermarket():
    # 신년 개장일은 개장만 늦다 — 애프터마켓 시대(2027-01-04 가정)에도 16:00~ 봉이 통과해야 하고
    # 15:36~15:59 는 여전히 창 밖이다.
    day = dt.date(2027, 1, 4)
    assert dc.check_session(day, _bars("10:00", "15:30", "15:35", "16:00", "16:45", "19:59")).ok
    assert not dc.check_session(day, _bars("10:00", "15:30", "15:45", "16:00")).ok


def test_close_bar_window_by_day_profile():
    normal = _bars(*NORMAL_AFTER)
    new_year = _bars("10:00", "15:30", "15:35", "15:21", n=1)  # 15:21 떠돌이 1행은 마감 지연이 아니다
    csat = {**_bars("10:00", "15:30", "16:30", "16:35"), **_bars("15:21", "15:25", n=700)}
    assert dc.close_bar_window(normal) == (T(15, 30), T(15, 32))
    assert dc.close_bar_window(new_year) == (T(15, 30), T(15, 32))
    assert dc.close_bar_window(csat) == (T(16, 30), T(16, 32))


def test_session_fails_on_empty_day():
    assert not dc.check_session(AFTER_DAY, {}).ok


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


# ── 조회 경로(sqlite) ─────────────────────────────────────────────────────


def _seed(tmp_path, *, extra_bar: str | None = None, canary: int | None = 2_060_000, shift_base: bool = False):
    path = tmp_path / "q.db"
    con = storage.connect(path)
    con.execute("CREATE TABLE minute_bars (code TEXT, ts TEXT, open REAL, high REAL, low REAL, close REAL,"
                " volume INTEGER, trade_value REAL, PRIMARY KEY (code, ts))")
    d0, d1 = dt.date(2026, 10, 7), AFTER_DAY
    for i in range(dc.IDENTITY_MIN_PAIRS):
        code = f"{i:06d}"
        prev = 10_000 + 10 * i
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
        con.execute("INSERT INTO minute_bars VALUES (?,?,?,?,?,?,?,?)",
                    ("000000", f"{d1} {extra_bar}:00", 1, 1, 1, 1, 1, 1))
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


def test_main_fails_on_injected_premarket_bar(tmp_path):
    assert dc.main(["--db", _seed(tmp_path, extra_bar="08:30"), *ASOF]) == 1


def test_main_fails_on_injected_base_change(tmp_path):
    assert dc.main(["--db", _seed(tmp_path, shift_base=True), *ASOF]) == 1


def test_main_fails_on_injected_adjusted_canary(tmp_path):
    assert dc.main(["--db", _seed(tmp_path, canary=41_200), *ASOF]) == 1


def test_main_passes_without_canary_row(tmp_path):
    assert dc.main(["--db", _seed(tmp_path, canary=None), *ASOF]) == 0
