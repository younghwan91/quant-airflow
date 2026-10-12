"""키움 공매도·신용잔고·프로그램 매매 **과거** 백필 — 2016-09~ (migration 033, swing-it 요청 2026-10-13).

매일 수집(``short_credit``)은 최근 창만 받는다 — 공매도 2024-05~, 신용 2026-02~(ka10013 을 ``dt="0"`` 으로
불러 최근 100행만 온다). 2026-10-13 실측으로 세 TR 모두 과거 기준일을 받는다:

- ``ka10014`` 공매도: ``strt_dt``~``end_dt`` 구간(한 번에 ~1년 — 300일 창으로 나눈다).
- ``ka10013`` 신용: ``dt``=기준일 → 그날까지 100거래일. 가장 오래된 날 전날을 다음 기준일로 거슬러 간다.
- ``ka90013`` 프로그램: ``date``=기준일 → 20거래일. 응답은 매수·매도·순매수 합계뿐(차익·비차익 분리 없음),
  금액은 백만원(005930 2016-12-29 매수 48,155주 × 1,796,000 ≈ 86,484).

**상장 생존 종목만이다.** 키움은 폐지 코드에 빈 응답을 준다(ka10081·ka10059 와 같다) — 이 구간을 쓰는 쪽은
생존편향을 따로 다뤄야 한다. 기존 행과 겹치지 않게 각 표의 최소 날짜 전날까지만 받는다(압축 청크를 다시 풀지
않으려고). 세 TR 은 버킷이 따로라 한 루프에서 섞어 부르면 벽시계가 겹친다. 끝낸 종목은 backfill_markers 에
남긴다(종류별).

CLI:
    python -m collectors.flow_history --prod --what short,credit --db <DSN>
    python -m collectors.flow_history --prod --what program --db <DSN>
"""

from __future__ import annotations

import argparse
import time
from datetime import date, datetime, timedelta
from typing import Any, Callable

from kiwoom_client.base import KiwoomAPIError

from .kiwoom_cli import add_common_args, build_universe, open_session, print_banner
from .short_credit import _build_cb_records, _build_ss_records
from .storage import (
    _upsert,
    checked_codes,
    fetchall,
    mark_checked,
    to_int,
    upsert_credit_balance,
    upsert_short_selling,
)

SINCE = "20160901"
SHORT_WINDOW_DAYS = 300
MARKERS = {"short": "kiwoom_short_2016", "credit": "kiwoom_credit_2016", "program": "kiwoom_program_2016"}
PROGRAM_COLUMNS = ["code", "date", "close", "volume", "prm_sell_amt", "prm_buy_amt", "prm_net_amt",
                   "prm_sell_qty", "prm_buy_qty", "prm_net_qty", "stex_tp", "source"]


def _ymd(d: date) -> str:
    return d.strftime("%Y%m%d")


def _prev_day(ymd: str) -> str:
    return _ymd(datetime.strptime(ymd, "%Y%m%d").date() - timedelta(days=1))


def short_windows(since: str, until: str, days: int = SHORT_WINDOW_DAYS) -> list[tuple[str, str]]:
    """[since, until] 을 ``days`` 일 창으로 — 최근 창부터."""
    out = []
    end = datetime.strptime(until, "%Y%m%d").date()
    start = datetime.strptime(since, "%Y%m%d").date()
    while end >= start:
        lo = max(start, end - timedelta(days=days - 1))
        out.append((_ymd(lo), _ymd(end)))
        end = lo - timedelta(days=1)
    return out


def walk_back(call: Callable[[str], list[dict]], since: str, until: str, max_calls: int = 400) -> list[dict]:
    """기준일 TR 을 ``until`` 부터 ``since`` 까지 거슬러 부른다. 빈 응답·진전 없음에서 멈춘다."""
    rows: list[dict] = []
    anchor = until
    for _ in range(max_calls):
        page = [r for r in call(anchor) if r.get("dt")]
        if not page:
            break
        rows.extend(r for r in page if since <= r["dt"] <= until)
        oldest = min(r["dt"] for r in page)
        nxt = _prev_day(oldest)
        if oldest <= since or nxt >= anchor:    # 구간 끝 / 진전 없음(상장일에 닿음)
            break
        anchor = nxt
    return rows


def build_program_records(code: str, rows: list[dict], today: str) -> list[tuple]:
    out = []
    for r in rows:
        dt = r.get("dt", "")
        if not dt or dt >= today:
            continue
        out.append((code, dt, abs(to_int(r.get("cur_prc"))), to_int(r.get("trde_qty")),
                    to_int(r.get("prm_sell_amt")), to_int(r.get("prm_buy_amt")), to_int(r.get("prm_netprps_amt")),
                    to_int(r.get("prm_sell_qty")), to_int(r.get("prm_buy_qty")), to_int(r.get("prm_netprps_qty")),
                    r.get("stex_tp") or None, "kiwoom_ka90013"))
    return out


def _first_dates(con: Any, table: str) -> dict[str, str]:
    """종목별 기존 최소 날짜(YYYYMMDD) — 그 전날까지만 받는다(종목마다 깊이가 다르다: 공매도 82종목은 330일뿐)."""
    return {c: str(d).replace("-", "") for c, d in fetchall(con, f"SELECT code, min(date) FROM {table} GROUP BY code")}


def collect(api: Any, con: Any, codes: list[str], what: list[str], *, since: str = SINCE,
            progress_every: int = 50) -> dict[str, int]:
    today = time.strftime("%Y%m%d")
    first = {w: _first_dates(con, t) for w, t in
             (("short", "short_selling"), ("credit", "credit_balance"), ("program", "program_trading")) if w in what}
    done = {w: checked_codes(con, MARKERS[w]) for w in what}
    stats = {"codes": 0, "failed": 0, "short": 0, "credit": 0, "program": 0}
    t0 = time.monotonic()
    for i, code in enumerate(codes, 1):
        ss_until, cb_until, pg_until = (_prev_day(first[w].get(code, today)) if w in first else today
                                        for w in ("short", "credit", "program"))
        try:
            if "short" in what and code not in done["short"]:
                recs = []
                for lo, hi in short_windows(since, ss_until):
                    resp = api.short_selling.short_selling_trend(stk_cd=code, tm_tp="1", strt_dt=lo, end_dt=hi)
                    page = _build_ss_records(code, resp, lo, today)
                    if not page:
                        break        # 그 창에 거래가 없다 = 상장 전
                    recs.extend(r for r in page if r[1] <= hi)
                stats["short"] += upsert_short_selling(con, recs)
                mark_checked(con, MARKERS["short"], [code], today)
            if "credit" in what and code not in done["credit"]:
                rows = walk_back(lambda dt: api.stock_info.credit_trading_trend(
                    stk_cd=code, dt=dt, qry_tp="1").get("crd_trde_trend") or [], since, cb_until)
                recs = _build_cb_records(code, {"crd_trde_trend": rows}, since, today)
                stats["credit"] += upsert_credit_balance(con, recs)
                mark_checked(con, MARKERS["credit"], [code], today)
            if "program" in what and code not in done["program"]:
                rows = walk_back(lambda dt: api.market.program_trading_by_stock_day(
                    amt_qty_tp="1", stk_cd=code, date=dt).get("stk_daly_prm_trde_trnsn") or [], since, pg_until)
                stats["program"] += _upsert(con, "program_trading", PROGRAM_COLUMNS,
                                            build_program_records(code, rows, today))
                mark_checked(con, MARKERS["program"], [code], today)
            stats["codes"] += 1
        except KiwoomAPIError as e:
            stats["failed"] += 1
            print(f"  ⚠️ {code}: rc={e.code} {e.message[:60]}", flush=True)
        if i % progress_every == 0 or i == len(codes):
            el = time.monotonic() - t0
            print(f"  [{i}/{len(codes)}] {stats} {el / 60:.0f}m ETA {el / i * (len(codes) - i) / 60:.0f}m"
                  f" {datetime.now():%H:%M}", flush=True)
    return stats


def main() -> int:
    ap = add_common_args(argparse.ArgumentParser(description=__doc__.split("\n")[0]))
    ap.add_argument("--what", default="short,credit", help="short,credit,program 중 쉼표 구분")
    ap.add_argument("--since", default=SINCE)
    ap.add_argument("--codes", default="", help="쉼표 구분 — 비우면 상장 보통주 전체")
    args = ap.parse_args()
    what = [w for w in args.what.split(",") if w]
    if not what or set(what) - set(MARKERS):
        raise SystemExit(f"--what 은 {sorted(MARKERS)} 중에서")
    con, api = open_session(args)
    codes = [c for c in args.codes.split(",") if c] or [s["code"] for s in build_universe(api, con, args)]
    print_banner(args, [{"code": c} for c in codes], f"{args.since}~ {','.join(what)}")
    stats = collect(api, con, codes, what, since=args.since)
    api.close()
    print(f"\n✅ 완료: {stats}")
    if stats["codes"] and not any(stats[w] for w in what):
        raise SystemExit("0행 — 빈 응답을 성공으로 넘기지 않는다")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
