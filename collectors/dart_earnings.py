"""Collect quarterly net income (당기순이익) YoY from DART — the input to the
validated PEAD⊕value alpha (see :mod:`kr_quant.strategies.pead`).

DART's ``fnlttSinglAcnt`` returns ``thstrm_amount`` (current period) and
``frmtrm_amount`` (prior-year same period), so YoY earnings growth — the PEAD
surprise proxy — comes straight from one call. Each figure is stamped with a
lookahead-safe ``avail_date`` = period-end + filing lag (see
:func:`_available_date`), so downstream use never peeks at a report before
it was public.

``parse_net_income`` is pure (JSON in → numbers out) and unit-tested without the
network. ``main`` (``kq-collect-earnings``) wires fetching + the liquid universe
and writes the CSV that ``kq-pead`` consumes.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import time
import urllib.parse
import urllib.request
import sys
import zipfile
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Any

import pandas as pd

from .storage import to_float_or_none, universe_query

BASE = "https://opendart.fss.or.kr/api"
# reprt_code: Q1, half-year(=Q2 cumulative), Q3, annual.
QUARTER_REPORT = {1: "11013", 2: "11012", 3: "11014", 4: "11011"}
QUARTER_END = {1: "0331", 2: "0630", 3: "0930", 4: "1231"}
NET_INCOME_ACCOUNTS = ("당기순이익", "당기순이익(손실)", "분기순이익", "반기순이익")
# 매출·영업이익 계정명 (krx-fundamentals-api ACCOUNT_MAP 준용) — Code33(EPS·매출·마진
# 3분기 연속 가속) 재현의 매출·마진 소스. 동일 ``fnlttSinglAcnt`` 응답에서 함께 온다.
REVENUE_ACCOUNTS = ("매출액", "수익(매출액)", "영업수익")
OP_INCOME_ACCOUNTS = ("영업이익", "영업이익(손실)")

# kr_quant.features.fundamentals.available_date의 인라인 복제 — 원래 kr-quant의
# features 모듈에 있었으나, dart_earnings.py가 quant-airflow로 이전되면서
# features 전체를 끌어올 이유 없이 이 5줄짜리 순수함수만 복제(중복이 공유패키지보다 단순).
QUARTER_LAG_DAYS = 45
ANNUAL_LAG_DAYS = 90


def _available_date(period_end: pd.Timestamp | str, *, is_annual: bool) -> pd.Timestamp:
    """period_end + 공시 지연(분기 45일/연간 90일) — lookahead-safe 가용일."""
    lag = ANNUAL_LAG_DAYS if is_annual else QUARTER_LAG_DAYS
    return pd.Timestamp(period_end) + pd.Timedelta(days=lag)


#: 콤마 섞인 숫자 문자열 → float|None. 정본은 storage 하나다 — 이 6줄이
#: dart_earnings 와 naver_consensus 에 한 벌씩 있었다.
_to_float = to_float_or_none


def _pick(rows: list[dict], names: tuple[str, ...]) -> tuple[float | None, float | None]:
    """First (current, prior-year) amounts whose ``account_nm`` is in ``names``.

    Shared by :func:`parse_net_income` and :func:`parse_financials`. Mirrors the
    original net-income scan: fill prior even before current is found, and stop
    at the first row that yields a current amount.
    """
    cur = prior = None
    for row in rows:
        if row.get("account_nm", "").strip() in names:
            v = _to_float(row.get("thstrm_amount"))
            vp = _to_float(row.get("frmtrm_amount"))
            if v is not None:
                cur = v
            if vp is not None:
                prior = vp
            if cur is not None:
                break
    return cur, prior


def parse_net_income(payload: dict) -> tuple[float | None, float | None]:
    """Extract (current, prior-year) net income from a ``fnlttSinglAcnt`` response.

    Args:
        payload: Parsed DART JSON. ``status`` must be "000"; ``list`` holds the
            statement rows.

    Returns:
        ``(netinc, netinc_prior)`` — current-period and prior-year-same-period net
        income, or ``(None, None)`` when the report is missing or has no
        net-income line. Picks the first row whose ``account_nm`` is a known
        net-income label (see :data:`NET_INCOME_ACCOUNTS`).
    """
    if payload.get("status") != "000":
        return None, None
    return _pick(payload.get("list", []), NET_INCOME_ACCOUNTS)


def parse_financials(
    payload: dict,
) -> tuple[float | None, float | None, float | None, float | None, float | None, float | None]:
    """Extract net income + revenue + operating income (current & prior) at once.

    The Code33 SEPA filter needs EPS **and** revenue **and** margin acceleration;
    revenue and operating income ride along in the same ``fnlttSinglAcnt`` payload
    as net income, so one parse yields all three. Margin = op_income / revenue is
    derived downstream (Phase 1), keeping this a pure extractor.

    Args:
        payload: Parsed DART JSON (same shape as :func:`parse_net_income`).

    Returns:
        ``(netinc, netinc_prior, revenue, revenue_prior, op_income, op_income_prior)``.
        Any leg absent from the report is ``None`` — a net-income-only response
        yields the net-income pair with the revenue/op-income legs ``None`` (no
        crash), and ``status != "000"`` yields all six ``None``.
    """
    if payload.get("status") != "000":
        return None, None, None, None, None, None
    rows = payload.get("list", [])
    ni, nip = _pick(rows, NET_INCOME_ACCOUNTS)
    rev, revp = _pick(rows, REVENUE_ACCOUNTS)
    oi, oip = _pick(rows, OP_INCOME_ACCOUNTS)
    return ni, nip, rev, revp, oi, oip


def yoy_growth(netinc: float | None, prior: float | None) -> float | None:
    """YoY net-income growth = (curr - prior) / |prior|, or None if not computable."""
    if netinc is None or prior in (None, 0):
        return None
    return (netinc - prior) / abs(prior)


def _get_json(url: str, params: dict, *, retries: int = 3) -> dict:
    q = urllib.parse.urlencode(params)
    for _ in range(retries):
        try:
            with urllib.request.urlopen(f"{url}?{q}", timeout=20) as r:
                return json.loads(r.read().decode())
        except Exception:
            time.sleep(1.0)
    return {}


def load_corp_map(api_key: str) -> dict[str, str]:
    """Return ``{stock_code: corp_code}`` from DART's corpCode.xml zip.

    Raises ``RuntimeError`` with the DART status when the response is an error
    XML (e.g. status 020 = daily call-limit exceeded, 010 = bad key) rather than
    the expected zip — otherwise the caller would see an opaque ``BadZipFile``.
    """
    q = urllib.parse.urlencode({"crtfc_key": api_key})
    with urllib.request.urlopen(f"{BASE}/corpCode.xml?{q}", timeout=60) as r:
        raw = r.read()
    if not raw[:2] == b"PK":  # zip magic; DART errors come back as small XML
        status = ""
        try:
            status = ET.fromstring(raw.decode()).findtext("status") or ""
        except Exception:
            pass
        raise RuntimeError(f"DART corpCode 오류 (status={status!r}) — 한도초과(020)/키오류(010) 등 확인")
    z = zipfile.ZipFile(io.BytesIO(raw))
    root = ET.fromstring(z.read(z.namelist()[0]).decode())
    out: dict[str, str] = {}
    for it in root.iter("list"):
        sc = (it.findtext("stock_code") or "").strip()
        cc = (it.findtext("corp_code") or "").strip()
        if sc and cc:
            out[sc] = cc
    return out


def load_corp_map_with_rotation(keys: list[str]) -> dict[str, str]:
    """``load_corp_map`` with key rotation on daily-limit (020) — same rotation
    ``_fetch_with_rotation`` already does for the per-quarter fetches, but for
    the one-time corp_code map load at startup. Without this, a single
    exhausted key[0] kills the whole run even when key[1..] still have quota
    (real incident: a 14.5h overnight backfill run legitimately stopped for
    the day via 020 mid-run, and the very next retry died instantly on
    ``load_corp_map(keys[0])`` alone despite a second key being available).
    """
    last_err: Exception | None = None
    for key in keys:
        try:
            return load_corp_map(key)
        except RuntimeError as e:
            # 에러 메시지 템플릿 자체에 "한도초과(020)/키오류(010)" 안내문구가 항상
            # 박혀있어 "020" in str(e)는 010 에러에도 항상 참이 된다 — status='020'
            # 형태로 실제 값만 정확히 매칭해야 함(실측 버그).
            if "status='020'" not in str(e):
                raise
            last_err = e
    raise last_err if last_err else RuntimeError("DART 키 없음")


def fetch_net_income(api_key: str, corp_code: str, year: int, quarter: int) -> tuple[float | None, float | None]:
    """Fetch (current, prior) net income for one corp/year/quarter from DART."""
    return parse_net_income(_fetch_payload(api_key, corp_code, year, quarter))


def _fetch_payload(api_key: str, corp_code: str, year: int, quarter: int) -> dict:
    return _get_json(f"{BASE}/fnlttSinglAcnt.json", {
        "crtfc_key": api_key, "corp_code": corp_code,
        "bsns_year": str(year), "reprt_code": QUARTER_REPORT[quarter],
    })


def fetch_financials(
    api_key: str, corp_code: str, year: int, quarter: int,
) -> tuple[float | None, float | None, float | None, float | None, float | None, float | None]:
    """Fetch net income + revenue + operating income (current & prior) in one call."""
    return parse_financials(_fetch_payload(api_key, corp_code, year, quarter))


MULTI_BATCH_SIZE = 100  # fnlttMultiAcnt cap: "조회 가능한 회사 개수가 초과하였습니다 (최대 100건)" (021)


def _fetch_multi_payload(api_key: str, corp_codes: list[str], year: int, quarter: int) -> dict:
    """One ``fnlttMultiAcnt`` call for up to :data:`MULTI_BATCH_SIZE` companies at once.

    Collapses the per-company ``fnlttSinglAcnt`` loop (~95,000 calls for the full
    ~2,634-code universe × 36 quarters) to ~970 calls total (found 2026-07-12 via
    the DART official dev-guide: ``fnlttMultiAcnt`` batches comma-joined corp_codes,
    capped at 100 per call — no true "all companies in one call" endpoint exists).
    """
    return _get_json(f"{BASE}/fnlttMultiAcnt.json", {
        "crtfc_key": api_key, "corp_code": ",".join(corp_codes),
        "bsns_year": str(year), "reprt_code": QUARTER_REPORT[quarter],
    })


def parse_financials_multi(
    payload: dict, corp_codes: list[str],
) -> dict[str, tuple[float | None, float | None, float | None, float | None, float | None, float | None]]:
    """Same six-tuple as :func:`parse_financials`, once per company in a multi-batch response.

    Args:
        payload: ``fnlttMultiAcnt`` JSON — ``list`` rows carry a ``corp_code`` field
            (unlike the single-company endpoint) to distinguish companies.
        corp_codes: The batch requested, so every company gets an entry (all-``None``
            six-tuple) even when DART returned no rows for it (e.g. no filing that
            quarter) — callers can then skip writing without a KeyError.

    Returns:
        ``{corp_code: (netinc, netinc_prior, revenue, revenue_prior, op_income,
        op_income_prior)}`` for every code in ``corp_codes``.
    """
    out = dict.fromkeys(corp_codes, (None, None, None, None, None, None))
    if payload.get("status") != "000":
        return out
    by_corp: dict[str, list[dict]] = {}
    for row in payload.get("list", []):
        by_corp.setdefault(row.get("corp_code", ""), []).append(row)
    for cc, rows in by_corp.items():
        if cc not in out:
            continue
        ni, nip = _pick(rows, NET_INCOME_ACCOUNTS)
        rev, revp = _pick(rows, REVENUE_ACCOUNTS)
        oi, oip = _pick(rows, OP_INCOME_ACCOUNTS)
        out[cc] = (ni, nip, rev, revp, oi, oip)
    return out


def _fetch_multi_with_rotation(
    keys: list[str], ki: list[int], corp_codes: list[str], year: int, quarter: int,
) -> dict[str, tuple[float | None, float | None, float | None, float | None, float | None, float | None]]:
    """``_fetch_with_rotation``'s batch counterpart — same key-rotation-on-020 logic.

    반환값은 ``(결과, 실패 사유 또는 None)`` 이다. 사유를 돌려주는 이유는 아래
    :func:`collect_all_financials_batched` 의 주석에 있다 — 요약하면 **"공시가
    없다"와 "우리가 못 받았다"가 구분되지 않아 실패가 성공으로 보고**됐다.
    """
    payload = _fetch_multi_payload(keys[ki[0]], corp_codes, year, quarter)
    while payload.get("status") == "020" and ki[0] + 1 < len(keys):
        ki[0] += 1
        print(f"DART 키 일한도(020) 도달 → 키{ki[0] + 1}로 로테이션", flush=True)
        payload = _fetch_multi_payload(keys[ki[0]], corp_codes, year, quarter)
    status = payload.get("status")
    # 013 = "조회된 데이터가 없습니다" — 그 분기에 공시가 없다는 정상 응답이다.
    # 그 외의 비-000 은 우리 쪽 문제(020 한도소진, 010 키오류, {} 네트워크 실패).
    failure = None if status in ("000", "013") else (status or "empty")
    return parse_financials_multi(payload, corp_codes), failure


def collect_all_financials_batched(
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
) -> "list[tuple[str, str, str, float | None, float | None, float | None, float | None, float | None, float | None]]":
    """Collect every (code, period) via ``fnlttMultiAcnt`` batches of ``batch_size``.

    ~27 batches × len(periods) calls total for the full universe, vs. one call per
    (code, period) in the original per-company path — see :func:`_fetch_multi_payload`.

    Args:
        keys: DART API keys (rotated on 020, see :func:`collect_keys`).
        corp_map: ``{stock_code: corp_code}`` (from :func:`load_corp_map_with_rotation`).
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
            corp_codes = [corp_map[sc] for sc in batch_codes]
            result, failure = _fetch_multi_with_rotation(keys, ki, corp_codes, year, q)
            if failure:
                failures.append((period, failure))
            for sc, cc in zip(batch_codes, corp_codes):
                ni, nip, rev, revp, oi, oip = result[cc]
                if ni is None:
                    continue
                kd = avail if knowledge_date == "avail" else today
                rows.append((sc, period, avail, kd, ni, nip, rev, revp, oi, oip))
            time.sleep(sleep)
        print(f"[{period}] 누적 rows={len(rows)}", flush=True)
    return rows


def collect_keys() -> list[str]:
    """DART keys from env in priority order: ``DART_API_KEY``, ``DART_API_KEY_2/3/...``.

    Each key has its own 20,000-call/day quota (per-key, not per-IP), so listing
    several lets collection roll over to the next when one hits the daily cap.
    """
    keys: list[str] = []
    for name in ("DART_API_KEY", "DART_API_KEY_2", "DART_API_KEY_3", "DART_API_KEY_4"):
        v = os.environ.get(name)
        if v:
            keys.append(v)
    return keys


def _fetch_with_rotation(
    keys: list[str], ki: list[int], corp_code: str, year: int, quarter: int,
) -> tuple[float | None, float | None, float | None, float | None, float | None, float | None]:
    """Fetch financials, rotating to the next key on DART daily-limit (status 020).

    ``ki`` is a one-element list holding the current key index, mutated in place so
    the rotation persists across calls (once a key is exhausted it stays skipped).
    """
    payload = _fetch_payload(keys[ki[0]], corp_code, year, quarter)
    while payload.get("status") == "020" and ki[0] + 1 < len(keys):
        ki[0] += 1
        print(f"DART 키 일한도(020) 도달 → 키{ki[0] + 1}로 로테이션", flush=True)
        payload = _fetch_payload(keys[ki[0]], corp_code, year, quarter)
    return parse_financials(payload)


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
    ki = [0]  # 현재 키 인덱스 (020 한도 시 로테이션)

    import pandas as pd
    from .storage import connect, default_db_path
    con = connect(args.db or str(default_db_path()))
    q_sql, q_params = _universe_query(args)
    top = pd.read_sql_query(q_sql, con, params=q_params)
    done_periods: set[tuple[str, str]] = set()
    if args.db_table:
        existing = pd.read_sql_query("SELECT code, period FROM earnings", con)
        done_periods = set(zip(existing["code"], existing["period"]))
    else:
        con.close()
    codes = top["code"].tolist()

    done: set[str] = set()
    if args.out and os.path.exists(args.out):
        for r in csv.reader(open(args.out)):
            if r:
                done.add(r[0])
    corp = load_corp_map_with_rotation(keys)
    print(f"corp_map {len(corp)} | universe {len(codes)} | keys {len(keys)} | already done {len(done)}", flush=True)

    today = datetime.now().strftime("%Y%m%d")
    if args.recent_quarters is not None:
        periods = _recent_quarters(args.recent_quarters)
    else:
        periods = [(year, q) for year in range(args.from_year, args.to_year + 1) for q in (1, 2, 3, 4)]

    if args.multi_batch:
        corp_universe = {sc: cc for sc, cc in corp.items() if sc in set(codes)}
        failures: list[tuple[str, str]] = []
        rows = collect_all_financials_batched(
            keys, corp_universe, periods, sleep=args.sleep,
            done_periods=done_periods, today=today,
            knowledge_date=args.knowledge_date, failures=failures)
        from .storage import upsert_earnings
        upsert_earnings(con, rows)
        con.close()
        print(f"DONE rows={len(rows)} (multi-batch)", flush=True)
        if failures:
            # **실패를 성공으로 보고하지 않는다.** 예전엔 `_get_json` 이 3회
            # 재시도 뒤 `{}` 를 돌려주고 → `parse_financials_multi` 가 전원
            # all-None 을 만들고 → `if ni is None: continue` 가 조용히 버려서,
            # 일한도 소진이든 네트워크 장애든 `DONE rows=...` 로 exit 0 이었다.
            # Airflow 는 성공으로 기록했고 retries 도 발동하지 않았다.
            #
            # (code, period) resume 덕에 빠진 조합은 다음 실행이 메워왔지만,
            # 그건 "실패해도 티가 안 난다"는 뜻이지 안전하다는 뜻이 아니다.
            # 수집분은 위에서 이미 커밋했으므로 여기서 죽어도 잃는 게 없다.
            kinds = ", ".join(sorted({f"{p}:{s}" for p, s in failures})[:8])
            print(
                f"❌ DART 배치 {len(failures)}건 실패 (status={kinds})",
                file=sys.stderr, flush=True,
            )
            return 1
        return 0

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
            ni, nip, rev, revp, oi, oip = _fetch_with_rotation(keys, ki, corp[code], year, q)
            time.sleep(args.sleep)
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


if __name__ == "__main__":
    raise SystemExit(main())
