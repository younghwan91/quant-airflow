"""Collect quarterly net income (당기순이익) YoY from DART — the input to the
validated PEAD⊕value alpha (see :mod:`kr_quant.strategies.pead`).

DART's ``fnlttSinglAcnt``/``fnlttMultiAcnt`` return ``thstrm_amount`` (current
period) and ``frmtrm_amount`` (prior-year same period); ``krx-fundamentals-client``
(pip, https://github.com/younghwan91/krx-fundamentals-client)'s ``DartScraper``
parses both into ``FinancialStatement.{net_income,revenue,operating_income}``
and their ``*_prior`` twins, so YoY earnings growth — the PEAD surprise proxy —
comes straight from one call. Each figure is stamped with a lookahead-safe
``avail_date`` = period-end + filing lag (see :func:`_available_date`), so
downstream use never peeks at a report before it was public.

DART API keys are pooled (see :func:`collect_keys`) and rotated on
``DartQuotaExceededError`` (status 020, daily quota) — ``dart.py``'s
``rotate_on_quota*`` helpers are payload/sync-based and don't fit
``DartScraper``'s async+exception interface, so the rotation is reimplemented
here (same pattern as ``news_dart.py``'s ``_scrape_with_rotation``).

``main`` (``kq-collect-earnings``) wires fetching + the liquid universe and
writes the CSV/DB rows that ``kq-pead`` consumes.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import os
import sys
from datetime import datetime
from typing import Any

import pandas as pd
from krx_fundamentals_client import DartQuotaExceededError, DartScraper, FinancialStatement, ReportType

from .config import DART_KEY_ENV_VARS
from .storage import universe_query

# reprt_code: Q1, half-year(=Q2 cumulative), Q3, annual.
QUARTER_REPORT_TYPE: dict[int, ReportType] = {
    1: ReportType.Q1, 2: ReportType.HALF, 3: ReportType.Q3, 4: ReportType.ANNUAL,
}
QUARTER_END = {1: "0331", 2: "0630", 3: "0930", 4: "1231"}

#: fnlttMultiAcnt 1회 호출당 종목 상한과 같은 값 — 우리 쪽 배치도 이 크기로
#: 나눠 부르므로, DartScraper.fetch_financials_batch 내부 청크(라이브러리
#: 자체도 100개씩 쪼갠다)는 매번 정확히 1회만 돈다. 그래야 020(일한도)이 배치
#: 중간에 나도 "이 배치까지는 성공"이라는 재개 단위가 유지된다 — 우리가 한
#: 호출에 몇백 종목을 몰아넣고 라이브러리가 내부에서 여러 청크로 쪼개면, 첫
#: 청크만 성공한 상태에서 020 예외가 나도 그 부분 성공분을 돌려받을 방법이
#: 없다(라이브러리의 for 루프 도중 예외가 그대로 전파돼 함수 전체가 빈손으로
#: 끝난다).
MULTI_BATCH_SIZE = 100

# kr_quant.features.fundamentals.available_date의 인라인 복제 — 원래 kr-quant의
# features 모듈에 있었으나, dart_earnings.py가 quant-airflow로 이전되면서
# features 전체를 끌어올 이유 없이 이 5줄짜리 순수함수만 복제(중복이 공유패키지보다 단순).
QUARTER_LAG_DAYS = 45
ANNUAL_LAG_DAYS = 90


def _available_date(period_end: pd.Timestamp | str, *, is_annual: bool) -> pd.Timestamp:
    """period_end + 공시 지연(분기 45일/연간 90일) — lookahead-safe 가용일."""
    lag = ANNUAL_LAG_DAYS if is_annual else QUARTER_LAG_DAYS
    return pd.Timestamp(period_end) + pd.Timedelta(days=lag)


def yoy_growth(netinc: float | None, prior: float | None) -> float | None:
    """YoY net-income growth = (curr - prior) / |prior|, or None if not computable."""
    if netinc is None or prior in (None, 0):
        return None
    return (netinc - prior) / abs(prior)


def _statement_to_tuple(
    stmt: FinancialStatement | None,
) -> tuple[float | None, float | None, float | None, float | None, float | None, float | None]:
    """``FinancialStatement`` → ``(ni, nip, rev, revp, oi, oip)``. ``None`` → all-``None``."""
    if stmt is None:
        return (None, None, None, None, None, None)
    return (
        stmt.net_income, stmt.net_income_prior,
        stmt.revenue, stmt.revenue_prior,
        stmt.operating_income, stmt.operating_income_prior,
    )


def collect_keys() -> list[str]:
    """DART keys from env in priority order: ``DART_API_KEY``, ``DART_API_KEY_2/3/...``.

    Each key has its own 20,000-call/day quota (per-key, not per-IP), so listing
    several lets collection roll over to the next when one hits the daily cap.
    """
    return [v for v in (os.environ.get(n) for n in DART_KEY_ENV_VARS) if v]


async def _load_corp_map_with_rotation(
    scrapers: dict[str, DartScraper], keys: list[str],
) -> dict[str, str]:
    """``DartScraper.load_corp_codes()`` with key rotation on daily-limit (020).

    Same rotation contract as ``dart.rotate_on_quota_raising``: try each key in
    order, skip past ``DartQuotaExceededError``, re-raise the last one if every
    key is exhausted. Without this, a single exhausted ``keys[0]`` kills the
    whole run even when ``keys[1..]`` still have quota (real incident: a 14.5h
    overnight backfill legitimately stopped for the day via 020 mid-run, and the
    very next retry died instantly on ``keys[0]`` alone despite a second key
    being available).
    """
    last: DartQuotaExceededError | None = None
    for key in keys:
        try:
            return await scrapers[key].load_corp_codes()
        except DartQuotaExceededError as e:
            last = e
    raise last if last else DartQuotaExceededError("DART API 키가 하나도 없음")


async def _fetch_with_rotation(
    scrapers: dict[str, DartScraper], keys: list[str], ki: list[int],
    ticker: str, year: int, quarter: int,
) -> tuple[float | None, float | None, float | None, float | None, float | None, float | None]:
    """Fetch one company's financials, rotating key on DART daily-limit (020).

    ``ki`` is a one-element list holding the current key index, mutated in place
    so the rotation persists across calls (once a key is exhausted it stays
    skipped) — mirrors ``dart.rotate_on_quota``'s contract.
    """
    report_type = QUARTER_REPORT_TYPE[quarter]
    while True:
        scraper = scrapers[keys[ki[0]]]
        try:
            stmt = await scraper.fetch_financials(ticker, year, report_type)
            return _statement_to_tuple(stmt)
        except DartQuotaExceededError:
            if ki[0] + 1 >= len(keys):
                return (None, None, None, None, None, None)
            ki[0] += 1
            print(f"DART 키 일한도(020) 도달 → 키{ki[0] + 1}로 로테이션 (fnlttSinglAcnt)", flush=True)


async def _fetch_multi_with_rotation(
    scrapers: dict[str, DartScraper], keys: list[str], ki: list[int],
    tickers: list[str], year: int, quarter: int,
) -> tuple[dict[str, tuple[float | None, float | None, float | None, float | None, float | None, float | None]], str | None]:
    """``_fetch_with_rotation``'s batch counterpart — same key-rotation-on-020 logic.

    Returns ``(결과, 실패 사유 또는 None)``. The reason distinguishes "no filing
    this quarter" (DART status 013, normal) from a real failure (010/100/800/900)
    via ``DartScraper``'s ``on_status`` callback — without it, both collapse into
    an indistinguishable all-``None`` result and a real vendor outage would be
    reported as a quiet success (see :func:`collect_all_financials_batched`'s
    docstring for the incident this guards against).
    """
    report_type = QUARTER_REPORT_TYPE[quarter]
    statuses: list[str] = []

    def on_status(status: str, _context: str) -> None:
        if status != "013":
            statuses.append(status)

    while True:
        scraper = scrapers[keys[ki[0]]]
        try:
            result = await scraper.fetch_financials_batch(tickers, year, report_type, on_status=on_status)
            break
        except DartQuotaExceededError:
            if ki[0] + 1 >= len(keys):
                result = dict.fromkeys(tickers, None)
                statuses = ["020"]
                break
            ki[0] += 1
            print(f"DART 키 일한도(020) 도달 → 키{ki[0] + 1}로 로테이션 (fnlttMultiAcnt)", flush=True)
            statuses = []

    failure = statuses[0] if statuses else None
    six_tuples = {ticker: _statement_to_tuple(stmt) for ticker, stmt in result.items()}
    return six_tuples, failure


async def collect_all_financials_batched(
    scrapers: dict[str, DartScraper],
    keys: list[str],
    corp_map: dict[str, str],
    periods: list[tuple[int, int]],
    *,
    sleep: float = 0.25,
    batch_size: int = MULTI_BATCH_SIZE,
    done_periods: set[tuple[str, str]] | None = None,
    today: str | None = None,
    knowledge_date: str = "today",
    failures: list[tuple[str, str]] | None = None,
) -> "list[tuple[str, str, str, str, float | None, float | None, float | None, float | None, float | None, float | None]]":
    """Collect every (code, period) via ``fnlttMultiAcnt`` batches of ``batch_size``.

    ~27 batches × len(periods) calls total for the full universe, vs. one call per
    (code, period) in the original per-company path.

    Args:
        scrapers: ``{key: DartScraper(key)}`` — one persistent scraper per API key
            (reused across periods/batches so corp_code caching and HTTP
            connections aren't rebuilt every call).
        keys: DART API keys (rotated on 020, see :func:`collect_keys`).
        corp_map: ``{stock_code: corp_code}`` (from :func:`_load_corp_map_with_rotation`)
            — used here only to define the universe; the corp_code lookup for
            each fetch happens inside ``DartScraper`` itself.
        periods: ``(year, quarter)`` pairs to collect, oldest-safe order doesn't matter.
        sleep: Delay between batch calls (politeness, not needed for the quota itself).
        batch_size: Companies per call (frozen at the DART-documented cap of 100).
        done_periods: ``{(code, period)}`` already collected — skipped (resume support).
        today: ``YYYYMMDD`` for the avail_date look-ahead guard (defaults to now).
        knowledge_date: ``"today"`` (기본) 또는 ``"avail"``.

            일간 수집에서는 ``today`` 가 맞다 — 그 값을 실제로 오늘 알게 됐고,
            정정공시라면 새 버전으로 쌓여야 한다.

            **과거 백필에는 ``avail`` 을 써야 한다.** 오래전에 공시돼 계속 공개돼
            있던 값을 이제서야 수집하는 경우, ``today`` 를 박으면 "2026년에 알게 된
            2018년 실적"이 되어 ``knowledge_date <= asof`` 로 읽는 과거 시점
            백테스트에서 **통째로 안 보인다**. migration 001 이 기존 행을
            ``knowledge_date = avail_date`` 로 채운 것과 같은 규약이다.

    Returns:
        Rows ready for :func:`.storage.upsert_earnings` — one per
        (code, period) with a non-``None`` net income, ``avail_date`` ≤ ``today``.
    """
    today = today or datetime.now().strftime("%Y%m%d")
    done_periods = done_periods or set()
    stock_codes = list(corp_map.keys())
    ki = [0]
    rows: list[tuple] = []
    failures = [] if failures is None else failures
    for year, q in periods:
        avail = _available_date(f"{year}-{QUARTER_END[q][:2]}-{QUARTER_END[q][2:]}",
                               is_annual=(q == 4)).strftime("%Y%m%d")
        if avail > today:
            continue
        period = f"{year}Q{q}"
        pending = [sc for sc in stock_codes if (sc, period) not in done_periods]
        for b0 in range(0, len(pending), batch_size):
            batch_codes = pending[b0:b0 + batch_size]
            result, failure = await _fetch_multi_with_rotation(scrapers, keys, ki, batch_codes, year, q)
            if failure:
                failures.append((period, failure))
            for sc in batch_codes:
                ni, nip, rev, revp, oi, oip = result[sc]
                if ni is None:
                    continue
                kd = avail if knowledge_date == "avail" else today
                rows.append((sc, period, avail, kd, ni, nip, rev, revp, oi, oip))
            await asyncio.sleep(sleep)
        print(f"[{period}] 누적 rows={len(rows)}", flush=True)
    return rows


def _write_row_csv(w: "csv.writer", code: str, period: str, avail: str,
                    ni: float | None, nip: float | None, rev: float | None,
                    revp: float | None, oi: float | None, oip: float | None) -> None:
    """첫 6컬럼(code,period,avail,netinc,prior,yoy)은 기존 스키마 불변 —
    매출·영업이익 4컬럼을 뒤에 append (하위호환: 기존 리더는 앞 6개만 읽음)."""
    def _c(x):
        return x if x is not None else ""
    w.writerow([code, period, avail, ni, _c(nip),
                _c(yoy_growth(ni, nip)), _c(rev), _c(revp), _c(oi), _c(oip)])


def _write_row_db(con: Any, code: str, period: str, avail: str, known: str,
                   ni: float | None, nip: float | None, rev: float | None,
                   revp: float | None, oi: float | None, oip: float | None) -> None:
    from .storage import upsert_earnings
    upsert_earnings(con, [(code, period, avail, known, ni, nip, rev, revp, oi, oip)])


def _recent_quarters(n: int, today: datetime | None = None) -> list[tuple[int, int]]:
    """The N most recent (year, quarter) pairs counting back from the current quarter."""
    today = today or datetime.now()
    year = today.year
    q = (today.month - 1) // 3 + 1
    out: list[tuple[int, int]] = []
    for _ in range(n):
        out.append((year, q))
        q -= 1
        if q == 0:
            q = 4
            year -= 1
    return out


def _universe_query(args: argparse.Namespace) -> tuple[str, dict]:
    """SQL (+ params) selecting the code universe: all ``daily_bars`` codes or top-N liquid."""
    return universe_query(all_codes=args.all_codes, top_n=args.top_n)


def _period_placeholders(periods: list[tuple[int, int]]) -> tuple[str, dict[str, str]]:
    """``period IN (...)`` 의 자리표시자 문자열과 파라미터 dict.

    pyformat(``%(name)s``) 이다 — 이 경로는 psycopg2 전용이다(``universe_query`` 도
    같은 스타일을 쓴다).

    **함수로 뽑아둔 이유가 있다.** 원래 이 두 줄은 ``main()`` 안에 인라인이었고,
    자리표시자를 ``",".join(["%(p%d)s" % i for i in ...])`` 로 만들었다. 그런데
    ``"%(p%d)s"`` 를 ``%`` 연산자에 넘기면 파이썬은 ``%(...)s`` 를 **매핑 키**로
    읽어 ``TypeError: format requires a mapping`` 을 던진다. 문자열 안에 리터럴
    ``%`` 를 남기려면 ``%%`` 여야 한다.

    그 코드는 `daily_earnings` 를 2026-08-27 에 두 번(재시도 포함) 실패시켰다.
    ``main()`` 안에 있어서 단위 테스트가 닿지 않았고, 검증할 때 표현식을 **손으로
    옮겨 적어** 돌리는 바람에 옮기면서 ``%%`` 로 고쳐 써서 통과했다 — 실행되는
    코드가 아니라 사본을 시험한 것이다. 이제 그 사본이 존재할 수 없다.

    ``%`` 연산자를 아예 안 쓰고 f-string 으로 만든다.
    """
    want = sorted({f"{year}Q{q}" for year, q in periods})
    ph = ",".join(f"%(p{i})s" for i in range(len(want)))
    return ph, {f"p{i}": v for i, v in enumerate(want)}


async def _run(args: argparse.Namespace, keys: list[str], con: Any,
                codes: list[str], periods: list[tuple[int, int]], today: str) -> int:
    scrapers = {key: DartScraper(api_key=key) for key in keys}
    try:
        # done_periods는 multi-batch/per-company 두 경로가 공유한다 — DISTINCT +
        # 이번 실행이 실제로 볼 분기로 제한한다. 예전엔 `SELECT code, period FROM
        # earnings`로 테이블 전체를 pandas로 끌어와 파이썬에서 중복을 버렸는데,
        # 키가 (code, period, knowledge_date)라 정정공시 버전까지 전부 딸려온다.
        done_periods: set[tuple[str, str]] = set()
        if args.db_table and periods:
            ph, ph_params = _period_placeholders(periods)
            existing = pd.read_sql_query(
                f"SELECT DISTINCT code, period FROM earnings WHERE period IN ({ph})",  # noqa: S608 — 자리표시자만 조립
                con, params=ph_params)
            done_periods = set(zip(existing["code"], existing["period"]))
        else:
            con.close()

        done: set[str] = set()
        if args.out and os.path.exists(args.out):
            for r in csv.reader(open(args.out)):
                if r:
                    done.add(r[0])

        corp = await _load_corp_map_with_rotation(scrapers, keys)
        print(f"corp_map {len(corp)} | universe {len(codes)} | keys {len(keys)} | already done {len(done)}", flush=True)

        if args.multi_batch:
            corp_universe = {sc: cc for sc, cc in corp.items() if sc in set(codes)}
            failures: list[tuple[str, str]] = []
            rows = await collect_all_financials_batched(
                scrapers, keys, corp_universe, periods, sleep=args.sleep,
                done_periods=done_periods, today=today,
                knowledge_date=args.knowledge_date, failures=failures)
            from .storage import upsert_earnings
            upsert_earnings(con, rows)
            con.close()
            print(f"DONE rows={len(rows)} (multi-batch)", flush=True)
            if failures:
                # **실패를 성공으로 보고하지 않는다.** 예전엔 네트워크/한도 실패가
                # 조용히 all-None 을 만들고 → `if ni is None: continue` 가 조용히
                # 버려서, 일한도 소진이든 벤더 장애든 `DONE rows=...` 로 exit 0 이었다.
                # Airflow 는 성공으로 기록했고 retries 도 발동하지 않았다.
                kinds = ", ".join(sorted({f"{p}:{s}" for p, s in failures})[:8])
                print(f"❌ DART 배치 {len(failures)}건 실패 (status={kinds})",
                      file=sys.stderr, flush=True)
                return 1
            return 0

        ki = [0]
        f = open(args.out, "a", newline="") if args.out else None
        w = csv.writer(f) if f else None
        n = 0
        for i, code in enumerate(codes, 1):
            if not args.db_table and (code in done or code not in corp):
                continue
            if args.db_table and code not in corp:
                continue
            for year, q in periods:
                avail = _available_date(f"{year}-{QUARTER_END[q][:2]}-{QUARTER_END[q][2:]}",
                                       is_annual=(q == 4)).strftime("%Y%m%d")
                if avail > today:
                    continue
                period = f"{year}Q{q}"
                if args.db_table and (code, period) in done_periods:
                    continue
                ni, nip, rev, revp, oi, oip = await _fetch_with_rotation(scrapers, keys, ki, code, year, q)
                await asyncio.sleep(args.sleep)
                if ni is None:
                    continue
                if args.db_table:
                    _write_row_db(con, code, period, avail, today, ni, nip, rev, revp, oi, oip)
                else:
                    _write_row_csv(w, code, period, avail, ni, nip, rev, revp, oi, oip)
                n += 1
            if f:
                f.flush()
            if i % 25 == 0:
                print(f"[{i}/{len(codes)}] rows={n}", flush=True)
        if f:
            f.close()
        if args.db_table:
            con.close()
        print(f"DONE rows={n}", flush=True)
        return 0
    finally:
        for scraper in scrapers.values():
            await scraper.close()


def main() -> int:
    ap = argparse.ArgumentParser(description="DART 분기 순이익 YoY 수집 (PEAD 입력)")
    ap.add_argument("--out", required=False, default=None, help="출력 CSV 경로")
    ap.add_argument("--top-n", type=int, default=800, help="유동성 상위 N종목")
    ap.add_argument("--from-year", type=int, default=2018)
    ap.add_argument("--to-year", type=int, default=datetime.now().year)
    ap.add_argument("--db", default=None)
    ap.add_argument("--sleep", type=float, default=0.25)
    ap.add_argument("--db-table", action="store_true", help="CSV 대신 earnings 테이블에 직접 upsert")
    ap.add_argument("--all-codes", action="store_true", help="유동성 상위 N 대신 daily_bars 전종목 사용")
    ap.add_argument("--knowledge-date", choices=("today", "avail"), default="today",
                    help="수집분의 knowledge_date. 일간 수집=today(기본), "
                         "과거 백필=avail (안 그러면 과거 as-of 조회에서 안 보임)")
    ap.add_argument("--recent-quarters", type=int, default=None,
                    help="전체 이력 대신 최근 N개 분기만 수집 (현재+직전, 일일 증분용)")
    ap.add_argument("--multi-batch", action="store_true",
                    help="fnlttMultiAcnt로 최대 100개씩 묶어 수집 (전종목 백필용, "
                         "회사당 1콜 대신 ~1/100로 콜 수 절감). --db-table과 함께 사용.")
    args = ap.parse_args()
    if not args.db_table and not args.out:
        ap.error("--out is required unless --db-table is set")
    if args.multi_batch and not args.db_table:
        ap.error("--multi-batch requires --db-table (batched rows are upserted directly)")

    keys = collect_keys()
    if not keys:
        raise SystemExit("환경변수 DART_API_KEY 필요")

    from .storage import connect, default_db_path
    con = connect(args.db or str(default_db_path()))
    q_sql, q_params = _universe_query(args)
    top = pd.read_sql_query(q_sql, con, params=q_params)
    codes = top["code"].tolist()

    today = datetime.now().strftime("%Y%m%d")
    if args.recent_quarters is not None:
        periods = _recent_quarters(args.recent_quarters)
    else:
        periods = [(year, q) for year in range(args.from_year, args.to_year + 1) for q in (1, 2, 3, 4)]

    return asyncio.run(_run(args, keys, con, codes, periods, today))


if __name__ == "__main__":
    raise SystemExit(main())
