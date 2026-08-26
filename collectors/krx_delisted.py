"""Delisted-stock master list from KRX — closes the survivorship-bias gap.

``features/universe.py`` flags delisting survivorship as an unresolved gap: a
point-in-time universe built only from codes that still trade *today* silently
excludes every stock that later went bankrupt/got merged/delisted, biasing any
backtest toward survivors.

KRX's ``getJsonData.cmd`` endpoint requires a real member login for its
statistical (MDCSTAT-prefixed) reports as of this project's KRX API audit
(2026-07) — see ``krx_shares.py`` for the login wall that blocks historical
상장주식수 backfill. But the *finder* endpoints (autocomplete/search widgets
behind KRX's stock-picker UI, e.g. ``finder_listdelisu``) are NOT behind that
wall — they return real data with no login. This module uses one of those to
get the full list of delisted stock codes, no auth required.

The finder endpoint only gives ``(code, name, market)`` — no delisting date.
This module cross-references that list against this DB's own ``daily_bars``:
for a delisted code we already have price history for, its last recorded
trading day is a good real-world approximation of its delisting date (in
practice the two are the same day or within a few trading days). Codes in the
KRX delisted list that never appear in our ``daily_bars`` (delisted before our
history starts, or never covered) get ``last_trade_date = None`` — still
useful as "this code is delisted, exclude it from any universe as of today",
just without a precise cutoff date.

CLI: ``python -m collectors.krx_delisted --db <DSN>``
"""

from __future__ import annotations

import argparse
import json
import urllib.parse
import urllib.request

from .storage import fetchall

FINDER_URL = "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
REFERER = "https://data.krx.co.kr/contents/MDC/MDI/outerLoader/index.cmd"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")


def parse_delisted_finder(payload: dict) -> list[tuple[str, str, str]]:
    """Extract ``[(code, name, market), ...]`` from a ``finder_listdelisu`` response.

    Args:
        payload: Parsed JSON with a ``block1`` list of row dicts (KRX's finder
            response shape — each row has ``short_code``, ``codeName``,
            ``marketName``).

    Returns:
        ``(code, name, market)`` tuples, skipping rows with no code. Empty
        list when ``block1`` is missing or empty.
    """
    rows = payload.get("block1") or []
    out: list[tuple[str, str, str]] = []
    for row in rows:
        code = (row.get("short_code") or "").strip()
        if not code:
            continue
        out.append((code, row.get("codeName") or "", row.get("marketName") or ""))
    return out


def fetch_delisted_list() -> list[tuple[str, str, str]]:
    """Fetch the full KRX delisted-stock list via the no-auth finder endpoint."""
    params = {"bld": "dbms/comm/finder/finder_listdelisu", "mktsel": "ALL",
              "searchText": "", "typeNo": "0"}
    body = urllib.parse.urlencode(params).encode()
    req = urllib.request.Request(FINDER_URL, data=body, headers={
        "User-Agent": UA, "Referer": REFERER, "X-Requested-With": "XMLHttpRequest"})
    with urllib.request.urlopen(req, timeout=30) as r:
        payload = json.loads(r.read().decode())
    return parse_delisted_finder(payload)


def main() -> int:
    ap = argparse.ArgumentParser(
        description="KRX 상장폐지종목 마스터 리스트 수집 (생존편향 보정용, 무인증)")
    ap.add_argument("--db", default=None)
    args = ap.parse_args()

    from .storage import connect, default_db_path, upsert_delisted_stocks
    con = connect(args.db or str(default_db_path()))

    delisted = fetch_delisted_list()
    print(f"KRX 상장폐지 리스트 {len(delisted)}건", flush=True)

    # 상폐 리스트(수천 건)를 통째로 IN(...) 파라미터로 넘기면 대형 파라미터 목록이
    # 플래너에 부담을 준다(실측: 공유메모리 부족 에러) — daily_bars는 종목 수(~2,600)만큼만
    # 있으니 전체를 한 번에 집계해서 파이썬 dict로 조회하는 편이 훨씬 가볍다.
    last_dates: dict[str, str] = {
        code: str(last)
        for code, last in fetchall(
            con, "SELECT code, MAX(date) FROM daily_bars GROUP BY code")
    }

    records = [
        (code, name, market, last_dates.get(code))
        for code, name, market in delisted
    ]
    n = upsert_delisted_stocks(con, records)
    con.close()
    matched = sum(1 for r in records if r[3] is not None)
    print(f"DONE rows={n} (daily_bars 매칭 {matched}건 — last_trade_date 확보)", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
