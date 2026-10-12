"""DART 공시 목록에서 실적이 **실제로 알려진 날** → earnings_disclosures (migrations/020).

``earnings.avail_date`` 는 법정 마감일 근사(분기말 +45일·연말 +90일)라 실제 공시보다 늦다
(005930 2020Q1: avail 05-15, 잠정실적 04-07). 종목마다 ``list.json`` 을 2019년부터 한 번에
훑어(corp_code 를 주면 기간 제한이 없다) 세 종류의 첫 원본 공시일을 남긴다:

- ``prelim``    영업(잠정)실적(공정공시)              — period = 접수일 직전 분기말
- ``pl_change`` 매출액또는손익구조30%(15%)이상변경    — period = 전년 Q4(1~4월 접수만, 연간 수치)
- ``periodic``  사업·반기·분기보고서 (YYYY.MM)        — period = 보고서명의 YYYY.MM

``[기재정정]`` 같은 괄호 접두 공시와 자회사 공시는 뺀다. 비12월 결산(보고서 월이 종류와
안 맞는다)은 earnings 의 period 규약과 어긋나 건너뛴다.

DART 일한도(키당 20,000)를 매일 도는 daily_earnings·daily_news 와 나눠 쓰므로
``--max-calls`` 에서 멈추고, 다 훑은 종목은 ``backfill_markers``
(``dart_filing_dates``) 에 남겨 다음 실행이 이어 받는다.

CLI:
    python -m collectors.dart_filing_dates --db <DSN> [--since 2019-01-01] [--max-calls 8000]
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import re
import sys
import time
from collections.abc import Callable
from typing import Any

from .dart_earnings import collect_keys
from .storage import (
    CHECKED_DART_FILING_DATES,
    checked_codes,
    connect,
    fetchall,
    insert_earnings_disclosures,
    mark_checked,
)

LIST_URL = "https://opendart.fss.or.kr/api/list.json"
PAGE_COUNT = 100
MIN_INTERVAL = 0.15
# 정기공시(A)와 거래소공시(I) — 잠정실적은 공정공시, 손익구조변경은 수시공시로 둘 다 I 다.
PBLNTF_TYPES = ("A", "I")

_PERIODIC = re.compile(r"^(사업|반기|분기)보고서 \((\d{4})\.(\d{2})\)$")
#: 보고서 종류 → 12월 결산이면 나올 수 있는 월
_PERIODIC_MONTHS = {"사업": {"12"}, "반기": {"06"}, "분기": {"03", "09"}}
_MONTH_QUARTER = {"03": 1, "06": 2, "09": 3, "12": 4}


class QuotaExhausted(Exception):
    """모든 키가 일한도(020)거나 --max-calls 에 닿았다 — 다음 실행이 이어 받는다."""


def quarter_before(day: dt.date) -> str:
    """``day`` 보다 **앞서** 끝난 마지막 분기의 'YYYYQn' — 분기말 당일 접수는 그 전 분기다
    (그날 장부가 닫히지도 않았다)."""
    q = (day.month - 1) // 3          # 올해 day 의 달 이전에 끝난 분기 수
    if q == 0:
        return f"{day.year - 1}Q4"
    return f"{day.year}Q{q}"


def classify(report_nm: str, rcept_dt: dt.date) -> tuple[str, str] | None:
    """보고서명 → (kind, period). 실적 공시가 아니면 None."""
    name = report_nm.strip()
    if name.startswith("[") or "자회사" in name:
        return None
    m = _PERIODIC.match(name)
    if m:
        kind_nm, year, month = m.groups()
        if month not in _PERIODIC_MONTHS[kind_nm]:
            return None                # 비12월 결산
        return "periodic", f"{year}Q{_MONTH_QUARTER[month]}"
    if "영업(잠정)실적" in name:
        return "prelim", quarter_before(rcept_dt)
    if "매출액또는손익구조" in name:
        # 직전 사업연도 수치다 — 12월 결산이면 1~4월에 나온다(2026-10-12 중간 실측 4,153건 중 4,074).
        # 5~12월 접수(79건)는 비12월 결산이라 직전 분기말로 매기면 엉뚱한 period 가 된다.
        if rcept_dt.month > 4:
            return None
        return "pl_change", f"{rcept_dt.year - 1}Q4"
    return None


def earliest(code: str, filings: list[dict]) -> list[tuple]:
    """한 종목의 공시 목록 → (code, period, kind) 마다 가장 이른 원본 한 행."""
    best: dict[tuple[str, str], tuple] = {}
    for f in filings:
        day = dt.datetime.strptime(f["rcept_dt"], "%Y%m%d").date()
        got = classify(f["report_nm"], day)
        if got is None:
            continue
        kind, period = got
        row = (code, period, kind, day.isoformat(), f["rcept_no"], f["report_nm"].strip())
        key = (period, kind)
        if key not in best or (row[3], row[4]) < (best[key][3], best[key][4]):
            best[key] = row
    return sorted(best.values())


class Lister:
    """list.json 페이지 수집 — 요청 간격, 키 순환(020), 호출 상한."""

    def __init__(self, keys: list[str], *, max_calls: int, get: Callable[..., Any] | None = None,
                 sleep: Callable[[float], None] = time.sleep, clock: Callable[[], float] = time.monotonic) -> None:
        if not keys:
            raise ValueError("DART API 키가 하나도 없다")
        if get is None:
            import httpx  # noqa: PLC0415 — 테스트는 get 을 주입한다

            get = httpx.Client(timeout=30).get
        self._get, self._sleep, self._clock = get, sleep, clock
        self._keys, self._ki, self._last = keys, 0, -1e9
        self.calls, self.max_calls = 0, max_calls

    def _page(self, params: dict) -> dict:
        while True:
            if self.calls >= self.max_calls:
                raise QuotaExhausted(f"--max-calls {self.max_calls} 도달")
            wait = self._last + MIN_INTERVAL - self._clock()
            if wait > 0:
                self._sleep(wait)
            self._last = self._clock()
            self.calls += 1
            r = self._get(LIST_URL, params={**params, "crtfc_key": self._keys[self._ki]})
            r.raise_for_status()
            body = r.json()
            status = body.get("status")
            if status == "020":
                if self._ki + 1 >= len(self._keys):
                    raise QuotaExhausted("모든 DART 키 일한도(020)")
                self._ki += 1
                print(f"DART 키 일한도(020) → 키{self._ki + 1}로 로테이션", flush=True)
                continue
            if status == "013":
                return {"list": [], "total_page": 0}
            if status != "000":
                raise RuntimeError(f"DART list.json status {status}: {body.get('message')}")
            return body

    def filings(self, corp_code: str, since: dt.date, until: dt.date) -> list[dict]:
        out: list[dict] = []
        for ty in PBLNTF_TYPES:
            page = 1
            while True:
                body = self._page({"corp_code": corp_code, "bgn_de": since.strftime("%Y%m%d"),
                                   "end_de": until.strftime("%Y%m%d"), "pblntf_ty": ty,
                                   "page_no": page, "page_count": PAGE_COUNT})
                out.extend(body.get("list") or [])
                if page >= int(body.get("total_page") or 0):
                    break
                page += 1
        return out


def collect(con: Any, lister: Lister, corp_map: dict[str, str], *, since: dt.date, until: dt.date,
            today: str) -> dict[str, int]:
    """earnings 에 있는 종목 중 아직 안 훑은 것을 훑는다. 종목 단위로 커밋·마킹 — 중간에 멈춰도 이어 받는다."""
    codes = sorted(r[0] for r in fetchall(con, "SELECT DISTINCT code FROM earnings", ()))
    done = checked_codes(con, CHECKED_DART_FILING_DATES)
    todo = [c for c in codes if c not in done]
    stats = {"universe": len(codes), "already": len(codes) - len(todo), "scanned": 0,
             "no_corp_code": 0, "rows": 0, "calls": 0, "stopped": 0}
    started = time.monotonic()
    for i, code in enumerate(todo, 1):
        corp = corp_map.get(code)
        if corp is None:
            stats["no_corp_code"] += 1
            continue
        try:
            filings = lister.filings(corp, since, until)
        except QuotaExhausted as e:
            print(f"⏸ {e} — {i - 1}/{len(todo)} 종목에서 멈춤, 다음 실행이 이어 받는다", flush=True)
            stats["stopped"] = 1
            break
        stats["rows"] += insert_earnings_disclosures(con, earliest(code, filings))
        mark_checked(con, CHECKED_DART_FILING_DATES, [code], today)
        stats["scanned"] += 1
        if i % 100 == 0:
            print(f"{i}/{len(todo)} 종목 · 새 행 {stats['rows']} · 호출 {lister.calls} · "
                  f"{time.monotonic() - started:.0f}s", flush=True)
    stats["calls"] = lister.calls
    return stats


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--db", required=True)
    ap.add_argument("--since", default="2019-01-01")
    ap.add_argument("--max-calls", type=int, default=8000,
                    help="이번 실행의 list.json 호출 상한 — 일한도를 daily_earnings·daily_news 와 나눠 쓴다")
    args = ap.parse_args()
    keys = collect_keys()
    if not keys:
        print("DART_API_KEY 가 없다", file=sys.stderr)
        return 2

    from krx_fundamentals_client import DartScraper  # noqa: PLC0415

    corp_map = asyncio.run(DartScraper(keys[0]).load_corp_codes())
    today = dt.date.today()
    con = connect(args.db)
    stats = collect(con, Lister(keys, max_calls=args.max_calls), corp_map,
                    since=dt.date.fromisoformat(args.since), until=today, today=today.isoformat())
    print(" | ".join(f"{k} {v}" for k, v in stats.items()), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
