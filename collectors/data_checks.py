"""시세 데이터 품질 검사 — 확정치(``daily_finalize``) 뒤에 돌고, 하나라도 깨지면 비0 종료.

2026-09-14 KRX 애프터마켓(16:00~20:00 연속매매, ETF·ETN 제외) 개장으로 키움 ka10081/ka10080 의
접미사 없는 KRX 코드 응답에 애프터마켓 체결이 섞였다. 그날부터 ``daily_bars``·``daily_bars_adjusted``·
``daily_bars_raw``·``supply_demand.close``·``acc_trde_qty`` 의 고가·저가·종가·거래량은 **정규장 값이
아니다**(시가만 정규장). 이걸 한 달 가까이 아무도 몰랐고, 알아챈 뒤에도 "오진" 이라는 틀린 판정이
docstring 에 남았다(``daily_finalize``). 원천 정의가 조용히 바뀌는 것을 다음엔 그날 아침에 잡으려고
세 검사를 코드로 둔다.

1. **세션 범위**(``check_session``) — 그날 ``minute_bars`` 의 모든 봉 시각이 알려진 세션 안이어야 한다.
   실측(2025-08-01~2026-10-12, 289거래일):
     정규장·동시호가·장후 시간외  09:00~15:35 (15:30 종가 단일가, 15:32·15:35 시간외 종가 봉)
     애프터마켓(2026-09-14~)       16:00~19:59, 20:00~20:02 지연 체결 5행
     개장 1시간 지연일              첫 봉 10:00 — 2026-01-02 신년 개장일(10:00~15:35, 마감은 그대로)
     개장·마감 1시간 지연일          2025-11-13 수능일(10:00~16:35, 15:21~15:29 에도 연속매매 봉 ~6,700행)
   08:00~08:59·15:36~15:59 봉은 0 이다. 그 밖에 봉이 하나라도 있으면 새 세션이 생긴 것이다(NXT 프리마켓이
   섞이는 등). 개장 지연(첫 봉 ≥10:00)과 마감 지연(15:21~15:29 봉 ≥100행 — 평일엔 0~1행)은 따로 판정한다
   — 신년 개장일은 개장만 늦다.
2. **기준가 항등식**(``check_base_price_identity``) — t 의 정규장 종가(분봉 15:30~15:32 마지막 봉, 마감 지연일 16:30~16:32)는
   t+1 의 ``supply_demand`` 에서 ``틱반올림(close / (1 + flu_rt/1e4))`` 로 복원된다. ``flu_rt`` 의 기준가는
   애프터마켓 이후에도 전일 **정규장** 종가이고, 바뀐 건 ``close`` 쪽이다(09-14 이후 99.98% 정확).
   불일치가 문턱을 넘으면 ``flu_rt`` 정의나 수집이 또 바뀐 것이다. 권리락 등 기준가 조정 종목이 남는 몫이다.
   복원은 :func:`base_close`(호가 표 단위부터 1원까지 격자 폴백 — 스팩·개편 전 코스닥) — DB 의 ``regular_close`` 뷰
   (migration 031 ``krx_base_close``)와 같은 계산이다. 이 검사가 터지면 그 뷰도 틀린 값을 내고 있다.
   **전 거래일만 본다** — ``minute_bars`` 과거분은 수정주가라(뒤에 일어난 분할·병합이 소급) 오래된 날짜로 돌리면
   그만큼 어긋난다(2025-08 12% → 2026-07 2.5% → 2026-09~ 0.07%, 001520 2025-08-01 분봉 1,310 = 수정 일봉, 원 종가 655).
3. **원주가 카나리아**(``check_raw_price_canary``) — ``daily_bars_raw`` 005930 2017-03-31 은 2,060,000
   (액면분할 전 원 종가)이어야 한다. 행이 없으면 통과(백필 범위 밖일 수 있다). 수정주가가 섞여 들어오면
   41,200 이 된다(``daily_bars`` 와 같은 값).

``minute_bars`` 는 scalp-it 이 쓰는 테이블이다(DDL 도 거기). 그날 분봉이 비면 두 검사가 표본 0 으로 실패한다
— 빈 표본을 통과로 넘기지 않는다(CLAUDE.md §5).

판정은 순수 함수(DB 없이 테스트), 조회는 :func:`storage.fetchall`(sqlite·Postgres 양쪽, ``?`` 파라미터)에 둔다.
"""

from __future__ import annotations

import argparse
import datetime as dt
import math
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from decimal import Decimal
from fractions import Fraction
from typing import Any
from zoneinfo import ZoneInfo

from . import storage

#: 애프터마켓 개장일. 이 날 전에는 16:00 이후 봉이 있으면 안 된다.
AFTERMARKET_START = dt.date(2026, 9, 14)

#: (시작, 끝) 분 단위, 양끝 포함. 근거는 모듈 docstring 의 실측.
REGULAR_WINDOW = (dt.time(9, 0), dt.time(15, 35))
AFTERMARKET_WINDOW = (dt.time(16, 0), dt.time(20, 2))
#: 첫 봉이 이 시각 이후면 개장 지연일(수능·신년) — 정규장 창 시작을 10:00 으로.
DELAYED_OPEN = dt.time(10, 0)
#: 평소 종가 단일가(15:20~15:30)라 체결이 없는 구간. 여기 봉이 많으면 마감 지연일(수능)이다.
LATE_CONTINUOUS = (dt.time(15, 21), dt.time(15, 29))
CLOSE_DELAY_MIN_ROWS = 100
DELAY_SHIFT = dt.timedelta(hours=1)

#: 정규장 종가 봉(15:30 종가 단일가, 드물게 15:32). 마감 지연일엔 1시간 늦춘다.
CLOSE_BAR_WINDOW = (dt.time(15, 30), dt.time(15, 32))

#: 호가가격단위 개편일(2023-01-25). 기준가를 정한 날(t) 기준으로 표를 고른다.
TICK_REFORM = dt.date(2023, 1, 25)
#: (이 가격 미만, 단위). 개편 뒤 — 코스피·코스닥 공통.
TICK_BANDS = ((2_000, 1), (5_000, 5), (20_000, 10), (50_000, 50), (200_000, 100), (500_000, 500))
#: 개편 전 코스피 표(가장 굵은 쪽 — 코스닥 5만원 이상 100원은 :func:`base_close` 격자 폴백이 잡는다).
TICK_BANDS_PRE_REFORM = ((1_000, 1), (5_000, 5), (10_000, 10), (50_000, 50), (100_000, 100), (500_000, 500))
TOP_TICK = 1_000
#: 기준가 복원 때 표 단위부터 시도하는 격자(굵은 것부터).
BASE_TICK_GRID = (1_000, 500, 100, 50, 10, 5, 1)
#: flu_rt(bp) 재현 허용 폭 — 키움 flu_rt 는 (close/기준가 − 1)×1e4 의 반올림이다.
FLU_RT_TOL_BP = Fraction(1, 2)

IDENTITY_MAX_MISMATCH = 0.01
#: 항등식 표본이 이보다 적으면 실패 — 분봉이 반쪽만 들어왔거나 조인이 깨진 것이다(평소 1,400 안팎).
IDENTITY_MIN_PAIRS = 300

CANARY = ("005930", dt.date(2017, 3, 31), 2_060_000)


@dataclass(frozen=True)
class CheckResult:
    name: str
    ok: bool
    detail: str


# ── 순수 함수 ──────────────────────────────────────────────────────────────


def _shift(t: dt.time, delta: dt.timedelta) -> dt.time:
    return (dt.datetime.combine(dt.date.min, t) + delta).time()


def day_profile(bar_counts: Mapping[dt.time, int]) -> tuple[bool, bool]:
    """(개장 지연, 마감 지연). 신년 개장일은 개장만, 수능일은 둘 다 1시간 늦다."""
    open_delayed = min(bar_counts) >= DELAYED_OPEN
    lo, hi = LATE_CONTINUOUS
    close_delayed = sum(n for t, n in bar_counts.items() if lo <= t <= hi) >= CLOSE_DELAY_MIN_ROWS
    return open_delayed, close_delayed


def session_windows(day: dt.date, bar_counts: Mapping[dt.time, int]) -> list[tuple[dt.time, dt.time]]:
    """그날 허용 세션 창 목록. 지연일이면 해당 끝만 1시간 늦춘다(마감 지연이면 애프터마켓 끝도)."""
    open_delayed, close_delayed = day_profile(bar_counts)
    zero = dt.timedelta(0)
    open_d = DELAY_SHIFT if open_delayed else zero
    close_d = DELAY_SHIFT if close_delayed else zero
    windows = [(_shift(REGULAR_WINDOW[0], open_d), _shift(REGULAR_WINDOW[1], close_d))]
    if day >= AFTERMARKET_START:
        windows.append((AFTERMARKET_WINDOW[0], _shift(AFTERMARKET_WINDOW[1], close_d)))
    return windows


def close_bar_window(bar_counts: Mapping[dt.time, int]) -> tuple[dt.time, dt.time]:
    """정규장 종가 봉을 찾을 창 — 마감 지연일(수능)만 16:30~16:32."""
    d = DELAY_SHIFT if bar_counts and day_profile(bar_counts)[1] else dt.timedelta(0)
    return _shift(CLOSE_BAR_WINDOW[0], d), _shift(CLOSE_BAR_WINDOW[1], d)


def check_session(day: dt.date, bar_counts: Mapping[dt.time, int]) -> CheckResult:
    """``bar_counts``: 그날 분봉 시각(분) → 봉 수. 창 밖 봉이 하나라도 있으면 실패."""
    name = f"session {day}"
    if not bar_counts:
        return CheckResult(name, False, "minute_bars 0행 — 분봉 수집이 빠졌다(scalp-it)")
    windows = session_windows(day, bar_counts)
    outside = {t: n for t, n in bar_counts.items() if not any(a <= t <= b for a, b in windows)}
    total = sum(bar_counts.values())
    span = ", ".join(f"{a:%H:%M}~{b:%H:%M}" for a, b in windows)
    if outside:
        sample = ", ".join(f"{t:%H:%M}×{n}" for t, n in sorted(outside.items())[:10])
        return CheckResult(
            name, False,
            f"알려진 세션({span}) 밖 봉 {sum(outside.values())}/{total}행: {sample}"
            " — 새 세션이 생겼다(원천 정의 변경). docs/schema.md 의 분봉 설명과 이 검사를 같이 고친다",
        )
    return CheckResult(name, True, f"{total}행 모두 세션 안({span})")


def krx_tick(price: float | Fraction, day: dt.date | None = None) -> int:
    """``day`` 시점 그 가격대의 **가장 굵은** KRX 호가단위(``day`` 없으면 2023-01-25 개편 뒤 표).

    개편 뒤는 코스피·코스닥 공통, 그 전은 코스피 표다 — 개편 전 코스닥 5만원 이상(100원)과 2025-03~ 스팩(1원)은
    이 표보다 가늘고, :func:`base_close` 의 격자 폴백이 그 몫을 잡는다. SQL 판은 migration 031 ``krx_tick_size``.
    """
    bands = TICK_BANDS if day is None or day >= TICK_REFORM else TICK_BANDS_PRE_REFORM
    for bound, tick in bands:
        if price < bound:
            return tick
    return TOP_TICK


def _exact(x: float | int | Decimal | Fraction) -> Fraction:
    """DB 값(정수·numeric·real)을 SQL numeric 처럼 십진 그대로 — float 의 이진 꼬리를 끌고 오지 않는다."""
    if isinstance(x, (int, Decimal, Fraction)):
        return Fraction(x)
    return Fraction(repr(float(x)))


def _round_half_up(x: Fraction) -> int:
    """SQL ``round(numeric)`` 과 같은 반올림(양수에서 0.5 는 올림)."""
    return math.floor(x + Fraction(1, 2)) if x >= 0 else -math.floor(-x + Fraction(1, 2))


def tick_round(price: float | Fraction, day: dt.date | None = None) -> int:
    tick = krx_tick(price, day)
    return _round_half_up(_exact(price) / tick) * tick


def base_close(close: float | int | Decimal, flu_rt_bp: float | int | Decimal, day: dt.date | None = None) -> int | None:
    """t+1 의 ``supply_demand`` 한 행(close, flu_rt)에서 t(=``day``) 의 기준가 = 정규장 종가를 복원한다.

    원값 ``close / (1 + flu_rt/1e4)`` 를 그 가격대 표 단위부터 1원까지 더 가는 격자로 반올림해 보며 flu_rt 를
    ±0.5bp 안에서 재현하는 첫 값을 고른다(키움 flu_rt 는 반올림 — 2025-01~2026-09 99.92% 일치). 진짜 기준가가 굵은
    격자 위에 있으면 그 반올림이 곧 진짜 값이고, 아니면 굵은 후보는 2bp 넘게 어긋나 flu_rt 를 못 맞춘다 — 그래서
    시장 구분 없이 스팩·개편 전 코스닥도 맞는다. None: 1원 격자로도 못 맞추거나(기준가 재설정) 이웃 격자값도 맞아
    유일하지 않다(flu_rt ≈ −100% 처럼 bp 정밀도가 호가단위보다 굵을 때).
    SQL 판은 migration 031 ``krx_base_close`` — 같은 결과여야 한다.
    """
    if close is None or flu_rt_bp is None:
        return None
    c, f = _exact(close), _exact(flu_rt_bp)
    if c <= 0 or f <= -10_000:
        return None
    raw = c * 10_000 / (10_000 + f)
    std = krx_tick(raw, day)
    for g in BASE_TICK_GRID:
        if g > std:
            continue
        cand = _round_half_up(raw / g) * g
        if cand > 0 and _reproduces(c, f, cand):
            # 이웃 격자값도 맞추면 flu_rt 정밀도가 모자란 것(flu_rt ≈ −100% 등) — 고르지 않는다
            if _reproduces(c, f, cand + g) or (cand > g and _reproduces(c, f, cand - g)):
                return None
            return cand
    return None


def _reproduces(close: Fraction, flu_rt_bp: Fraction, base: int) -> bool:
    return abs((close / base - 1) * 10_000 - flu_rt_bp) <= FLU_RT_TOL_BP


def derive_regular_close(
    rows: Iterable[tuple[str, dt.date, float | None, float | None]],
    base_resets: Iterable[tuple[str, dt.date]] = (),
) -> dict[tuple[str, dt.date], tuple[int | None, str]]:
    """migration 031 ``regular_close`` 뷰의 2026-09-14~ 갈래를 파이썬으로 — (code, date) → (정규장 종가, src).

    ``rows``: ``supply_demand`` 키움 행 (code, date, close, flu_rt) — 거래일 달력도 이 날짜들로 만든다.
    ``base_resets``: ``corporate_actions`` 에서 upd_rt ≠ 0 인 (code, 새 기준 첫 거래일).
    src 는 identity·pending·no_next·gap·base_reset·unmatched(뜻은 031 머리말).
    """
    by_code: dict[str, list[tuple[dt.date, float | None, float | None]]] = {}
    for code, day, close, flu in rows:
        by_code.setdefault(code, []).append((day, close, flu))
    days = sorted({d for series in by_code.values() for d, _, _ in series})
    next_day = dict(zip(days, [*days[1:], None], strict=True))
    resets = set(base_resets)
    out: dict[tuple[str, dt.date], tuple[int | None, str]] = {}
    for code, series in by_code.items():
        series.sort(key=lambda r: r[0])
        for i, (day, _, _) in enumerate(series):
            nxt = series[i + 1] if i + 1 < len(series) else None
            if next_day[day] is None:
                out[code, day] = (None, "pending")
            elif nxt is None:
                out[code, day] = (None, "no_next")
            elif nxt[0] != next_day[day]:
                out[code, day] = (None, "gap")
            elif (code, nxt[0]) in resets:
                out[code, day] = (None, "base_reset")
            else:
                base = base_close(nxt[1], nxt[2], day)
                out[code, day] = (base, "identity" if base is not None else "unmatched")
    return out


def check_base_price_identity(
    prev_day: dt.date,
    day: dt.date,
    regular_close: Mapping[str, float],
    next_rows: Iterable[tuple[str, float | None, float | None]],
    *,
    max_mismatch: float = IDENTITY_MAX_MISMATCH,
    min_pairs: int = IDENTITY_MIN_PAIRS,
) -> CheckResult:
    """``regular_close``: prev_day 정규장 종가(분봉). ``next_rows``: day 의 (code, close, flu_rt)."""
    name = f"identity {prev_day}→{day}"
    pairs = 0
    bad: list[tuple[str, float, int | None]] = []
    for code, close, flu in next_rows:
        if code not in regular_close or close is None or flu is None or close <= 0:
            continue
        pairs += 1
        implied = base_close(close, flu, prev_day)
        if implied != regular_close[code]:
            bad.append((code, regular_close[code], implied))
    if pairs < min_pairs:
        return CheckResult(
            name, False,
            f"대조 표본 {pairs} < {min_pairs} — {prev_day} 분봉 종가 봉이나 {day} 수급이 반쪽이다",
        )
    rate = len(bad) / pairs
    sample = ", ".join(f"{c} 분봉 {int(r):,} vs 복원 {'없음' if i is None else f'{i:,}'}" for c, r, i in bad[:5])
    detail = f"불일치 {len(bad)}/{pairs} = {rate:.2%} (문턱 {max_mismatch:.0%})" + (f"; 예 {sample}" if bad else "")
    if rate > max_mismatch:
        return CheckResult(
            name, False,
            detail + " — supply_demand.flu_rt 의 기준가 정의나 close 수집이 바뀌었다",
        )
    return CheckResult(name, True, detail)


def check_raw_price_canary(close: float | None) -> CheckResult:
    code, day, expected = CANARY
    name = f"canary daily_bars_raw {code} {day}"
    if close is None:
        return CheckResult(name, True, "행 없음 — 통과(백필 범위 밖)")
    if int(close) != expected:
        return CheckResult(
            name, False,
            f"close {int(close):,} ≠ {expected:,} — 무수정 주가 테이블에 수정주가가 섞였다(upd_stkpc_tp)",
        )
    return CheckResult(name, True, f"close {expected:,}")


# ── 조회(sqlite·Postgres 공용) ──────────────────────────────────────────────


def _as_date(v: Any) -> dt.date:
    if isinstance(v, dt.datetime):
        return v.date()
    if isinstance(v, dt.date):
        return v
    return dt.date.fromisoformat(str(v)[:10])


def _as_datetime(v: Any) -> dt.datetime:
    return v if isinstance(v, dt.datetime) else dt.datetime.fromisoformat(str(v))


def _ts(day: dt.date, t: dt.time) -> str:
    return f"{day.isoformat()} {t:%H:%M:%S}"


def trading_days(con: Any, before: dt.date, n: int) -> list[dt.date]:
    """``before`` 전의 최근 거래일 n 개(오름차순). 거래일 달력은 ``supply_demand`` 의 날짜다."""
    out: list[dt.date] = []
    cursor = before
    for _ in range(n):
        row = storage.fetchone(con, "SELECT MAX(date) FROM supply_demand WHERE date < ?", (cursor.isoformat(),))
        if not row or row[0] is None:
            break
        cursor = _as_date(row[0])
        out.append(cursor)
    return sorted(out)


def fetch_bar_counts(con: Any, day: dt.date) -> dict[dt.time, int]:
    rows = storage.fetchall(
        con,
        "SELECT ts, COUNT(*) FROM minute_bars WHERE ts >= ? AND ts < ? GROUP BY ts",
        (day.isoformat(), (day + dt.timedelta(days=1)).isoformat()),
    )
    counts: dict[dt.time, int] = {}
    for ts, n in rows:
        t = _as_datetime(ts).time().replace(second=0, microsecond=0)
        counts[t] = counts.get(t, 0) + int(n)
    return counts


def fetch_regular_close(con: Any, day: dt.date, window: tuple[dt.time, dt.time]) -> dict[str, float]:
    """day 의 종목별 정규장 종가 = ``window``(:func:`close_bar_window`) 안 마지막 분봉 종가."""
    lo, hi = window
    rows = storage.fetchall(
        con,
        "SELECT m.code, m.close FROM minute_bars m"
        " JOIN (SELECT code, MAX(ts) AS mts FROM minute_bars WHERE ts >= ? AND ts <= ? GROUP BY code) x"
        "   ON m.code = x.code AND m.ts = x.mts",
        (_ts(day, lo), _ts(day, hi)),
    )
    return {code: float(close) for code, close in rows if close is not None}


def fetch_supply_rows(con: Any, day: dt.date) -> list[tuple[str, float | None, float | None]]:
    return storage.fetchall(
        con,
        "SELECT code, close, flu_rt FROM supply_demand WHERE date = ? AND source = 'kiwoom'",
        (day.isoformat(),),
    )


def fetch_canary_close(con: Any) -> float | None:
    code, day, _ = CANARY
    row = storage.fetchone(con, "SELECT close FROM daily_bars_raw WHERE code = ? AND date = ?", (code, day.isoformat()))
    return None if not row else row[0]


def run_checks(con: Any, asof: dt.date) -> list[CheckResult]:
    """``asof`` 전 마지막 거래일 T 를 본다: T 의 세션, (T 직전 거래일 → T) 항등식, 카나리아."""
    days = trading_days(con, asof, 2)
    if len(days) < 2:
        return [CheckResult("trading days", False, f"{asof} 전 supply_demand 거래일이 2개 미만")]
    prev_day, day = days
    return [
        check_session(day, fetch_bar_counts(con, day)),
        check_base_price_identity(
            prev_day, day,
            fetch_regular_close(con, prev_day, close_bar_window(fetch_bar_counts(con, prev_day))),
            fetch_supply_rows(con, day),
        ),
        check_raw_price_canary(fetch_canary_close(con)),
    ]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--db", help="sqlite 경로 또는 postgresql:// DSN (읽기만 한다)")
    ap.add_argument("--asof", type=dt.date.fromisoformat, default=None,
                    help="이 날짜 전 마지막 거래일을 본다(기본: 오늘 KST)")
    ap.add_argument("--days", type=int, default=1,
                    help="--asof 부터 거꾸로 거래일 N 개를 차례로 본다(이력 점검용, 기본 1)")
    args = ap.parse_args(argv)

    con = storage.connect(args.db)
    if storage._is_pg(con):
        con.set_session(readonly=True)
    # 컨테이너 시계는 UTC 일 수 있다 — 06:30 KST 는 UTC 로 전날이라 하루 묵은 날을 보게 된다.
    asof = args.asof or dt.datetime.now(ZoneInfo("Asia/Seoul")).date()
    targets = [asof]
    if args.days > 1:
        targets = [d + dt.timedelta(days=1) for d in trading_days(con, asof, args.days)]
    results: list[CheckResult] = []
    for i, t in enumerate(targets):
        batch = run_checks(con, t)
        if i < len(targets) - 1:  # 카나리아는 날짜와 무관 — 한 번만
            batch = [r for r in batch if not r.name.startswith("canary")]
        results.extend(batch)
    for r in results:
        print(f"[{'OK' if r.ok else 'FAIL'}] {r.name}: {r.detail}", flush=True)
    failed = [r for r in results if not r.ok]
    if failed:
        print(f"데이터 품질 검사 실패 {len(failed)}/{len(results)}", flush=True)
        return 1
    print(f"데이터 품질 검사 통과 {len(results)}건", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
