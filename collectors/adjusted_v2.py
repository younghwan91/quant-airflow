"""daily_bars_adj_v2 — 원주가 × 수정 이벤트 누적계수, 2026-09-14~ 는 KRX 분봉 OHLC (migration 029).

**왜.** ``daily_bars_adjusted`` 는 수정주가로 받은 ``daily_bars`` 를 ±30% 점프 탐지로 다시 이어 붙인다 — 수집 시점
이음매가 남고(가짜 이음매 423개, swing-it 실측), 2026-09-14 부터는 키움 일봉 종가가 KRX 정규장 종가가 아니라
KRX 애프터마켓(16:00~20:00, 2026-09-14~) 20:00 마지막가다(종목-일 86%, 평균 0.7% — NXT 가 아니다, docs/schema.md 애프터마켓 절). 여기서는 두 원천을 바로 쓴다:

- **가격 = daily_bars_raw(무수정) × 계수.** 계수 = 그날 **뒤에** 찍힌 corporate_actions 의 ``1 + upd_rt/100`` 곱.
  ``date`` 는 새 기준 첫 거래일이므로 그날 자신은 곱하지 않는다. 거래량은 계수로 나눈다(주식수 기준을 맞춘다).
  종류(kind)로 거르지 않는다 — 배당락(4)으로 찍힌 행도 ``upd_rt`` ≠ 0 이면 실제 시가 갭이 그만큼 있다(주식배당,
  2026-10-13 실측 202건 중 200건 설명). 현금배당은 ``upd_rt`` 가 0 이라 곱해도 그대로다. 키움이 이벤트를 빠뜨린
  날은 키움 수정주가(daily_bars) ÷ 원주가 비율의 계단으로 채운다(``infer_events`` — daily_bars 가 있는 2016-09~ 만).
- **2026-09-14~ OHLCV = 분봉 09:00~15:32.** 종가는 15:32 까지 마지막 봉(동시호가 체결이 15:30~15:32 봉에 찍힌다).
  09-14 이전 한 달로 검증: 마지막 봉 종가 = 일봉 종가 99.7%, 시가 99.0%, 고저 99.2%, 거래량 중앙 비율 0.9994.
  분봉 시가가 원주가 시가와 0.5% 넘게 다르면(분봉이 수집 뒤 소급 조정된 경우) 쓰지 않는다 → ``close_src``
  'minute_basis_mismatch', 분봉이 아예 없으면 'no_krx_source' — 둘 다 OHLCV 를 NULL 로 둔다(틀린 값보다 없는 값).
- **폐지 종목 = daily_bars_delisted_naver(수정가격).** 키움이 폐지 종목 원주가·이벤트를 안 주므로 계수를 만들
  수 없다 — 네이버 수정가격을 그대로 넣고 ``factor``·``raw_close`` 는 NULL, ``close_src``='naver_adj'.
  상장 종목 원주가가 시작하는 날부터만 넣는다 — 그 앞을 넣으면 그 구간은 폐지 종목만 있는 유니버스가 된다.

swing-it 사전등록 87955ba 의 합격 검사 4종은 ``daily_bars_adj_v2_build`` 에서 통과한 뒤 이름을 바꿨다(030 머리말에
결과). 다시 검사할 땐 ``--table`` 로 따로 만들어 본다.

CLI:
    python -m collectors.adjusted_v2 --db <DSN>                  # 전 종목 재계산
    python -m collectors.adjusted_v2 --db <DSN> --codes 005930   # 일부
"""

from __future__ import annotations

import argparse
import math
import time
from datetime import date
from typing import Any

from .storage import _is_pg, _upsert, connect, default_db_path, fetchall, fetchone

TABLE = "daily_bars_adj_v2"
KRX_ONLY_FROM = date(2026, 9, 14)   # 이날부터 키움 일봉 종가 = KRX 애프터마켓 20:00 가
OPEN_TOL = 0.005                    # 분봉 시가 ↔ 원주가 시가 허용 오차(같은 기준인지)

COLUMNS = [
    "code", "date", "open", "high", "low", "close", "volume", "trade_value",
    "raw_close", "factor", "close_src", "source",
]

# 분봉 → KRX 정규장 일봉. 시가 = 첫 봉, 종가 = 15:32 까지 마지막 봉, 거래대금은 원 단위 합.
MINUTE_DAILY_SQL = """
SELECT code, ts::date,
       (array_agg(open ORDER BY ts))[1], max(high), min(low), (array_agg(close ORDER BY ts DESC))[1],
       sum(volume)::bigint, sum(trade_value)::float8
  FROM minute_bars
 WHERE ts >= %s AND ts::time BETWEEN '09:00' AND '15:32'
 GROUP BY 1, 2
"""


def adjust(rows: list[tuple], events: dict[date, float]) -> list[tuple]:
    """원주가 일봉 → 수정 일봉.

    ``rows``: (date, open, high, low, close, volume, trade_value) 날짜 오름차순.
    ``events``: {이벤트일: upd_rt(%)}. 반환: (date, o, h, l, c, volume, trade_value, raw_close, factor) 오름차순.
    """
    ev = sorted(events.items(), reverse=True)
    out = []
    f, i = 1.0, 0
    for d, o, h, lo, c, v, tv in reversed(rows):
        while i < len(ev) and ev[i][0] > d:
            step = 1 + ev[i][1] / 100
            if step > 0:   # −100% 같은 깨진 값은 건너뛴다(0 을 곱하면 이력 전체가 0)
                f *= step
            i += 1
        out.append((
            d, _mul(o, f), _mul(h, f), _mul(lo, f), _mul(c, f),
            None if v is None else round(v / f), tv, c, f,
        ))
    out.reverse()
    return out


def _mul(x: float | None, f: float) -> float | None:
    return None if x is None else round(x * f, 4)   # 0.02 × 2,060,000 = 41200.00000000004 같은 꼬리 제거


def krx_patch(row: tuple, minute: tuple | None) -> tuple[tuple, str]:
    """KRX_ONLY_FROM 이후 행의 OHLCV 를 분봉 값으로 바꾼다.

    ``row``: adjust() 한 행. ``minute``: (open, high, low, close, volume, trade_value_won) 또는 None.
    분봉은 원 가격(수집 당시 기준)이라 원주가 시가와 같은지로 기준을 확인하고, 같으면 계수를 곱한다.
    """
    d, o, _h, _l, _c, _v, _tv, raw_c, f = row
    raw_open = None if o is None else o / f
    if minute is None:
        return (d, None, None, None, None, None, None, None, f), "no_krx_source"
    mo, mh, ml, mc, mv, mtv = minute
    if not raw_open or not mo or abs(mo / raw_open - 1) > OPEN_TOL:
        return (d, None, None, None, None, None, None, None, f), "minute_basis_mismatch"
    tv = None if mtv is None else round(mtv / 1e6)   # 원 → 백만원(daily_bars 단위)
    return (d, _mul(mo, f), _mul(mh, f), _mul(ml, f), _mul(mc, f), round(mv / f), tv, round(mc), f), "minute_krx"


def infer_events(rows: list[tuple], kiwoom_adj: dict[date, float], events: dict[date, float]) -> dict[date, float]:
    """corporate_actions 에 없는 이벤트를 키움 수정주가(daily_bars) ÷ 원주가 비율의 계단으로 찾는다.

    키움이 이벤트 행을 안 주거나 upd_rt 를 0 으로 준 날이 있다(2016-09~2026-10 원주가 |log| > 0.4 점프 97건 중
    83건 — 001530 2022-04-19 10:1 분할, 002420 2021-12-22 2:1 병합). 그런 날도 키움 수정주가에는 계수가 들어가 있어
    전날·그날 비율(daily_bars ÷ 원주가)이 정확히 그만큼 바뀐다(0.1·0.5·2.0·1.5 …). 조건: 원주가 |log| > 0.4,
    비율 계단 |log| > 0.1, 키움 수정주가 자체의 일간 변화 ≤ 31%(가격제한폭 안 — 수집 시점 이음매 날은 빠진다).
    반환: {날짜: upd_rt(%)} — ``events`` 에 0 아닌 값이 있는 날은 건드리지 않는다.
    """
    out: dict[date, float] = {}
    prev = None
    for d, _o, _h, _l, c, _v, _tv in rows:
        b = kiwoom_adj.get(d)
        if prev and c and b and d not in events:
            pd_, pc, pb = prev
            if abs(math.log(c / pc)) > 0.4 and abs(b / pb - 1) <= 0.31:
                step = (pb / pc) / (b / c)
                if abs(math.log(step)) > 0.1:
                    out[d] = round((step - 1) * 100, 4)
        prev = (d, c, b) if c and b else None
    return out


def build_code(code: str, rows: list[tuple], events: dict[date, float],
               minutes: dict[tuple[str, date], tuple]) -> list[tuple]:
    out = []
    for r in adjust(rows, events):
        src = "kiwoom_raw"
        if r[0] >= KRX_ONLY_FROM:
            r, src = krx_patch(r, minutes.get((code, r[0])))
        out.append((code, *r, src, "kiwoom_raw"))
    return out


def delisted_rows(code: str, rows: list[tuple], minutes: dict[tuple[str, date], tuple]) -> list[tuple]:
    """네이버 수정가격 — 계수 없이 그대로. KRX_ONLY_FROM 이후는 상장 종목과 같은 분봉 규칙."""
    out = []
    for d, o, h, lo, c, v, tv in rows:
        r, src = (d, o, h, lo, c, v, tv, None, 1.0), "naver_adj"
        if d >= KRX_ONLY_FROM:
            r, src = krx_patch(r, minutes.get((code, d)))
        out.append((code, *r[:7], None, None, src, "naver_adj"))
    return out


def collect(con: Any, codes: list[str] | None = None, table: str = TABLE) -> dict[str, int]:
    if not _is_pg(con):
        raise SystemExit("Postgres 전용 — minute_bars·corporate_actions 는 운영 DB 에만 있다")
    start = fetchone(con, "SELECT min(date) FROM daily_bars_raw", ())[0]
    if start is None:
        raise SystemExit("daily_bars_raw 가 비었다")
    minutes = {(r[0], r[1]): tuple(r[2:]) for r in fetchall(con, MINUTE_DAILY_SQL, (KRX_ONLY_FROM,))}
    if not minutes:
        raise SystemExit(f"{KRX_ONLY_FROM} 이후 분봉이 없다 — 종가를 만들 수 없다")
    listed = [r[0] for r in fetchall(con, "SELECT DISTINCT code FROM daily_bars_raw ORDER BY 1")]
    listed_set = set(listed)
    delisted = [r[0] for r in fetchall(
        con, "SELECT DISTINCT code FROM daily_bars_delisted_naver WHERE date >= %s ORDER BY 1", (start,))
        if r[0] not in listed_set]
    if codes:
        listed = [c for c in listed if c in codes]
        delisted = [c for c in delisted if c in codes]
    stats = {"listed": 0, "delisted": 0, "rows": 0, "inferred": 0}
    t0 = time.time()
    for n, code in enumerate(listed + delisted, 1):
        if n <= len(listed):
            rows = fetchall(con, "SELECT date, open, high, low, close, volume, trade_value FROM daily_bars_raw"
                                 " WHERE code = %s ORDER BY date", (code,))
            events = {d: float(rt) for d, rt in fetchall(
                con, "SELECT date, upd_rt FROM corporate_actions WHERE code = %s AND upd_rt <> 0", (code,))}
            kiwoom_adj = {d: float(c) for d, c in fetchall(
                con, "SELECT date, close FROM daily_bars WHERE code = %s AND close > 0", (code,))}
            inferred = infer_events(rows, kiwoom_adj, events)
            for d, rt in sorted(inferred.items()):
                print(f"  추정 이벤트 {code} {d} upd_rt={rt:+.2f}%", flush=True)
            stats["inferred"] += len(inferred)
            recs = build_code(code, rows, {**events, **inferred}, minutes)
            stats["listed"] += 1
        else:
            rows = fetchall(con, "SELECT date, open, high, low, close, volume, trade_value"
                                 " FROM daily_bars_delisted_naver WHERE code = %s AND date >= %s ORDER BY date",
                            (code, start))
            recs = delisted_rows(code, rows, minutes)
            stats["delisted"] += 1
        stats["rows"] += _upsert(con, table, COLUMNS, recs)
        if n % 200 == 0:
            print(f"  [{n}/{len(listed) + len(delisted)}] {stats} {time.time() - t0:.0f}s", flush=True)
    return stats


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--db", default=str(default_db_path()))
    ap.add_argument("--codes", default="", help="쉼표 구분 — 비우면 전 종목")
    ap.add_argument("--table", default=TABLE)
    a = ap.parse_args()
    con = connect(a.db)
    stats = collect(con, [c for c in a.codes.split(",") if c] or None, a.table)
    print(f"✅ 완료: {stats}")
    if stats["rows"] == 0:
        raise SystemExit("0행 — 빈 결과를 성공으로 넘기지 않는다")


if __name__ == "__main__":
    main()
