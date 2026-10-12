"""DART 원문 파서(:mod:`collectors.dart_doc`)의 네트워크·DB 쪽 — 전체 실행·검증·폐지 종목 매핑·커버리지.

파싱은 전부 ``dart_doc`` 의 순수 함수다. 여기는 받아오고(디스크 캐시), 돌리고, 대조만 한다. DB 는 읽기만 한다.

하위 명령::

    python -m collectors.dart_doc_run run      --codes @codes.txt --years 2007-2015 --out out.jsonl
    python -m collectors.dart_doc_run compare  --jsonl out.jsonl            # earnings·shares 와 대조(2015~2016)
    python -m collectors.dart_doc_run map-delisted --out map.json            # 폐지 종목 → 고유번호
    python -m collectors.dart_doc_run coverage --map map.json --out cov.json # 연도별 추정 커버리지

**캐시**는 레포 밖 디스크(기본 ``~/data/dartdoc_cache``) — /tmp 는 tmpfs 라 재부팅에 날아가고, 원문 11만 건 ≈ 16GB 다.
**재시도 vs 영구 캐시**: '문서 없음'이 확실한 응답(013 조회 결과 없음·014 파일 없음)만 ``.missing`` 으로 남기고,
800/900·JSON 이 아닌 본문·네트워크 오류는 재시도 뒤에도 실패하면 예외로 올린다(캐시하지 않는다 — 다음 실행이 다시 받는다).
020(일한도)은 즉시 멈춘다.
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import os
import re
import sys
import time
import traceback
import zipfile
from collections import Counter
from pathlib import Path

from . import dart_doc as dd

log = logging.getLogger("dart_doc_run")

DEFAULT_CACHE = Path.home() / "data" / "dartdoc_cache"
BASE = "https://opendart.fss.or.kr/api"
#: 문서가 없다는 게 확실한 상태 — 영구 캐시해도 되는 것
NO_DATA = {"013", "014"}
#: 잠깐의 장애 — 재시도
TRANSIENT = {"800", "900", "901"}


class DartQuota(RuntimeError):
    """020 일한도 — 그날은 더 받지 않는다."""


class DartTransient(RuntimeError):
    """재시도 뒤에도 남은 일시 오류 — 캐시하지 않는다."""


class Dart:  # pragma: no cover — 네트워크
    """list.json·document.xml 디스크 캐시 + 호출 수 계수. 키 값은 어디에도 찍지 않는다."""

    def __init__(self, key: str, cache: Path, *, retries: int = 4, pause: float = 0.12) -> None:
        self.key, self.cache, self.calls, self.retries, self.pause = key, cache, 0, retries, pause
        cache.mkdir(parents=True, exist_ok=True)

    def _get(self, path: str, **params: str) -> bytes:
        import requests

        last = ""
        for attempt in range(self.retries):
            self.calls += 1
            try:
                r = requests.get(f"{BASE}/{path}", params={"crtfc_key": self.key, **params}, timeout=60)
            except requests.RequestException as e:
                last = type(e).__name__
            else:
                if r.status_code < 500:
                    time.sleep(self.pause)
                    return r.content
                last = f"http {r.status_code}"
            time.sleep(2 ** attempt)
        raise DartTransient(f"{path} {params.get('rcept_no') or params.get('corp_code')}: {last}")

    @staticmethod
    def _status(body: bytes) -> str | None:
        m = re.search(rb'"status"\s*:\s*"(\d+)"|<status>(\d+)</status>', body[:600])
        return (m.group(1) or m.group(2)).decode() if m else None

    def _json(self, path: str, **params: str) -> dict:
        for attempt in range(self.retries):
            body = self._get(path, **params)
            try:
                d = json.loads(body)
            except ValueError:
                d = {"status": self._status(body) or "json_error"}
            st = d.get("status")
            if st == "020":
                raise DartQuota(path)
            if st in TRANSIENT or st == "json_error":
                time.sleep(2 ** attempt)
                continue
            return d
        raise DartTransient(f"{path}: status {st}")

    def corp_rows(self) -> list[dict]:
        p = self.cache / "corpCode.json"
        if not p.exists():
            import xml.etree.ElementTree as ET

            z = zipfile.ZipFile(io.BytesIO(self._get("corpCode.xml")))
            root = ET.fromstring(z.read(z.namelist()[0]))
            rows = [{k: (el.findtext(k) or "").strip() for k in ("corp_code", "corp_name", "stock_code", "modify_date")}
                    for el in root.iter("list")]
            if not rows:
                raise RuntimeError("corpCode 가 비었다 — 0건 성공으로 넘기지 않는다(CLAUDE.md §5)")
            p.write_text(json.dumps(rows, ensure_ascii=False))
        return json.loads(p.read_text())

    def filings(self, corp: str, bgn: str, end: str) -> list[dict]:
        p = self.cache / f"list_{corp}_{bgn}_{end}.json"
        if p.exists():
            return json.loads(p.read_text())
        items, page = [], 1
        while True:
            d = self._json("list.json", corp_code=corp, bgn_de=bgn, end_de=end, pblntf_ty="A",
                           page_no=str(page), page_count="100")
            st = d.get("status")
            if st == "013":
                break
            if st != "000":
                raise RuntimeError(f"list.json status={st} {d.get('message', '')}")
            items += d.get("list", [])
            if page >= int(d.get("total_page", 1)):
                break
            page += 1
        p.write_text(json.dumps(items, ensure_ascii=False))
        return items

    def document(self, rcept_no: str) -> bytes | None:
        p = self.cache / f"doc_{rcept_no}.zip"
        miss = self.cache / f"doc_{rcept_no}.missing"
        if p.exists():
            return p.read_bytes()
        if miss.exists():
            return None
        for attempt in range(self.retries):
            body = self._get("document.xml", rcept_no=rcept_no)
            if body[:2] == b"PK":
                p.write_bytes(body)
                return body
            st = self._status(body)
            if st == "020":
                raise DartQuota("document.xml")
            if st in NO_DATA:
                miss.write_bytes(body[:600])
                return None
            time.sleep(2 ** attempt)  # 800/900·HTML 오류 본문 등 — 캐시하지 않고 다시
        raise DartTransient(f"document {rcept_no}: status {st}")


# --------------------------------------------------------------------------- 공통


def list_window(years: list[int]) -> tuple[str, str]:
    """list.json 조회 창 — 명령마다 같은 캐시 파일을 쓰도록 2007-01-01 ~ 2017-06-30 을 기본으로 잡고 필요하면 넓힌다."""
    return f"{min(2007, min(years))}0101", f"{max(2016, max(years)) + 1}0630"


def _codes(arg: str) -> list[str]:
    return Path(arg[1:]).read_text().split() if arg.startswith("@") else arg.split(",")


def _years(arg: str) -> list[int]:
    if "-" in arg:
        a, b = arg.split("-")
        return list(range(int(a), int(b) + 1))
    return [int(y) for y in arg.split(",")]


def _dart(a: argparse.Namespace) -> Dart:  # pragma: no cover
    return Dart(os.environ[a.key_env], Path(a.cache).expanduser())


#: 매핑으로 쓰지 않는 상태 — corpCode 의 그 stock_code 주인이 거래하던 해에 정기보고서가 없다(코드 재사용·펀드)
UNUSABLE_MAP = {"stock_code_unconfirmed", "unconfirmed", "ambiguous", "no_name_match", "not_verified", "class_share_unmapped"}


def corp_map(rows: list[dict], mapping: dict[str, dict] | None) -> dict[str, list[str]]:
    """stock_code → [corp_code]. 폐지 종목 매핑(map-delisted 결과)이 있으면 그 판정이 corpCode 보다 먼저다.

    확인되지 않은 매핑(``stock_code_unconfirmed`` 등)은 **빼낸다** — corpCode 의 현재 주인이 코드를 재사용한
    다른 회사면 그 회사 실적이 붙는다(2026-10-13 재리뷰).
    """
    out: dict[str, list[str]] = {}
    for r in rows:
        if r["stock_code"]:
            out.setdefault(r["stock_code"], []).append(r["corp_code"])
    for code, m in (mapping or {}).items():
        if m.get("status") in UNUSABLE_MAP or not m.get("corp_code"):
            out.pop(code, None)
        else:
            out[code] = [m["corp_code"]]
    return out


def _corp_map(dart: Dart, map_path: str | None) -> dict[str, list[str]]:  # pragma: no cover
    return corp_map(dart.corp_rows(), json.loads(Path(map_path).read_text()) if map_path else None)


def read_jsonl(path: Path) -> list[dict]:
    """종목 단위 jsonl — 마지막 줄이 쓰다 잘렸으면(중단) 그 줄을 버리고 파일도 거기까지로 줄인다."""
    if not path.exists():
        return []
    lines = path.read_text().splitlines()
    out = []
    for i, line in enumerate(lines):
        if not line.strip():
            continue
        try:
            out.append(json.loads(line))
        except ValueError:
            if i != len(lines) - 1:
                raise
            log.warning("잘린 마지막 줄을 버린다: %s…", line[:60])
            path.write_text("".join(x + "\n" for x in lines[:i]))
    return out


#: 실행 실패 하한(기본 90%) — 이걸 밑돌면 장애로 끝낸다(CLAUDE.md §5). 완료 조건 C 의 합격 바(95%, 사용자 승인)
#: 와 다르다: 합격 바는 earnings 와의 **일치율**이고, 이 하한은 받은 보고서 중 값을 낸 **산출률**이다.
def yield_report(rows: list[dict]) -> dict:
    """누적 jsonl 전체로 산출률을 잰다(이어 하기여도 지금까지 전부).

    - 연도별: 그해 원본 보고서 중 netinc 를 낸 비율. 보고서가 0건인 연도도 실패다.
    - 종목별: 매핑이 없거나 보고서가 없는 종목(no_filing·no_dec_fye_filing)까지 분모에 넣은, 값이 하나라도 있는 종목 비율.
    """
    st: Counter = Counter()
    for r in rows:
        st["codes"] += 1
        st["codes_with_value"] += any(x["netinc"] is not None for x in r.get("records", []))
        if r.get("error"):
            st[f"code_{r['error']}"] += 1
        for period in r.get("filings", {}):
            st[f"y{period[:4]}_filings"] += 1
        for x in r.get("records", []):
            if not x["is_correction"]:
                st[f"y{x['period'][:4]}_with_value"] += x["netinc"] is not None
    return dict(st)


def yield_failures(st: dict, years: list[int], floor: float) -> list[str]:
    bad = []
    for y in years:
        n, v = st.get(f"y{y}_filings", 0), st.get(f"y{y}_with_value", 0)
        if n == 0:
            bad.append(f"{y}: 보고서 0건")
        elif v / n < floor:
            bad.append(f"{y}: {v}/{n} = {v / n:.1%}")
    n, v = st.get("codes", 0), st.get("codes_with_value", 0)
    if n and v / n < floor:
        bad.append(f"종목: {v}/{n} = {v / n:.1%}(매핑 없음·보고서 없음 포함)")
    return bad


# --------------------------------------------------------------------------- run: 전체 실행 → 종목별 jsonl


def run(a: argparse.Namespace) -> int:  # pragma: no cover — 네트워크
    years = _years(a.years)
    dart = _dart(a)
    corp = _corp_map(dart, a.map)
    out = Path(a.out)
    done = {r["code"] for r in read_jsonl(out)}  # 이어 하기 — 이미 쓴 종목은 건너뛴다
    errlog = out.with_suffix(".errors.log")
    bgn, end = list_window(years)
    stats: Counter = Counter()
    with out.open("a") as fo, errlog.open("a") as fe:
        for code in _codes(a.codes):
            if code in done:
                stats["resumed_skip"] += 1
                continue
            items = [it for cp in corp.get(code, []) for it in dart.filings(cp, bgn, end)]
            filings = {p: f for p, f in dd.select_filings(items).items() if int(p[:4]) in years}
            reports: dict[str, list[dd.ReportResult]] = {}
            meta: dict[str, dict] = {}
            for period, f in sorted(filings.items()):
                rnos = [f["rcept_no"]] + (f["corrections"] if a.corrections else [])
                for k, rno in enumerate(rnos):
                    raw = dart.document(rno)
                    stats["docs"] += 1
                    if raw is None:
                        stats["doc_missing"] += 1
                        continue
                    try:
                        rr = dd.parse_report(dd.doc_files(raw), rno, int(period[:4]), int(period[-1]))
                    except Exception:  # noqa: BLE001 — 문서 하나의 예외는 격리하고 센다
                        stats["parse_error"] += 1
                        fe.write(f"{code} {period} {rno}\n{traceback.format_exc()}\n")
                        continue
                    # 원본이 목록에 없으면 첫 판도 정정본이다
                    rr.is_correction = k > 0 or f["original_missing"]
                    reports.setdefault(period, []).append(rr)
                meta[period] = {"rcept_no": f["rcept_no"], "corrections": f["corrections"],
                                "original_missing": f["original_missing"]}
            recs = dd.build_records(code, reports)
            stats["records"] += len(recs)
            fo.write(json.dumps({"code": code, "filings": meta, "records": recs,
                                 "error": None if filings else ("no_filing" if not items else "no_dec_fye_filing")},
                                ensure_ascii=False, default=str) + "\n")
            fo.flush()
    stats["dart_calls"] = dart.calls
    st = yield_report(read_jsonl(out))
    print(json.dumps({**dict(stats), **st}, ensure_ascii=False))
    bad = yield_failures(st, years, a.min_yield)
    if bad:
        print(f"⚠️ 산출률 하한 {a.min_yield:.0%} 미달(누적): {bad}", file=sys.stderr)
        return 1
    return 0


# --------------------------------------------------------------------------- compare: earnings·shares 대조


def compare(a: argparse.Namespace) -> int:  # pragma: no cover — DB
    import psycopg2

    rows = [json.loads(x) for x in Path(a.jsonl).read_text().splitlines() if x.strip()]
    by = {r["code"]: r for r in rows}
    codes = list(by)
    con = psycopg2.connect(os.environ["KR_QUANT_DB"])
    try:
        cur = con.cursor()
        cur.execute("SELECT code, period, netinc, netinc_prior FROM earnings WHERE period LIKE '2016Q_' AND code = ANY(%s)", (codes,))
        ref: dict[tuple[str, str], float | None] = {}
        for code, period, ni, nip in cur.fetchall():
            ref[(code, period)] = ni
            ref[(code, "2015" + period[4:])] = nip
        cur.execute("SELECT code, date, shares_outstanding FROM shares_outstanding_history WHERE source='dart' "
                    "AND date BETWEEN '2015-01-01' AND '2016-12-31' AND code = ANY(%s)", (codes,))
        sref = [(c, str(d), v) for c, d, v in cur.fetchall()]
    finally:
        con.close()
    n = ok = nc = 0
    cats: Counter = Counter()
    for (code, period), r in ref.items():
        if r is None:
            continue
        n += 1
        rec = by.get(code, {})
        if rec.get("error"):
            cats[rec["error"]] += 1
            continue
        # earnings 는 지금의 API 값(마지막 정정 반영) — 그 분기 레코드 중 가장 늦은 knowledge_date 와 비교
        vers = [x for x in rec.get("records", []) if x["period"] == period]
        if not vers:
            cats["no_filing_for_period"] += 1
            continue
        nc += 1
        v = max(vers, key=lambda x: x["knowledge_date"])["netinc"]
        if dd.agrees(v, r):
            ok += 1
        else:
            cats["parser_none" if v is None else "value_diff"] += 1
    print(f"netinc 일치 {ok}/{n} = {ok / max(n, 1):.2%}(전체 분모) · {ok}/{nc} = {ok / max(nc, 1):.2%}(대조 가능한 보고서)  {dict(cats)}")
    sn = sok = 0
    for code, d, v in sref:  # 기준일이 같은 행끼리만
        shares = [x["shares"] for x in by.get(code, {}).get("records", []) if x.get("shares") and x["shares"].get("asof") == d]
        if not shares:
            continue
        sn += 1
        sok += any(s["common"] == v for s in shares)
    print(f"shares(보통주 발행주식총수, 기준일 같은 행) 일치 {sok}/{sn} = {sok / max(sn, 1):.2%}")
    return 0


# --------------------------------------------------------------------------- map-delisted: 폐지 종목 → 고유번호


def _delisted_targets(cur) -> dict[str, dict]:  # pragma: no cover — DB
    """폐지 종목(6자리 숫자 코드) → 이름들·거래 연도(daily_bars_delisted_naver)·폐지 연도(kind_delistings)."""
    t: dict[str, dict] = {}

    def get(code: str) -> dict:
        return t.setdefault(code, {"names": [], "years": set(), "delist_year": None})

    cur.execute("SELECT code, name FROM delisted_stocks WHERE code ~ '^[0-9]{6}$'")
    for code, name in cur.fetchall():
        get(code)["names"].append(name)
    cur.execute("SELECT code, name, extract(year FROM delist_date)::int FROM kind_delistings WHERE code ~ '^[0-9]{6}$'")
    for code, name, y in cur.fetchall():
        d = get(code)
        if name and name not in d["names"]:
            d["names"].append(name)
        d["delist_year"] = y
    cur.execute("SELECT code, array_agg(DISTINCT extract(year FROM date)::int) FROM daily_bars_delisted_naver GROUP BY code")
    for code, yrs in cur.fetchall():
        get(code)["years"] = set(yrs)
    return t


def map_delisted(a: argparse.Namespace) -> int:  # pragma: no cover — 네트워크·DB
    import psycopg2

    dart = _dart(a)
    rows = dart.corp_rows()
    idx = dd.corp_name_index(rows)
    by_stock = {r["stock_code"]: r["corp_code"] for r in rows if r["stock_code"]}
    con = psycopg2.connect(os.environ["KR_QUANT_DB"])
    try:
        targets = _delisted_targets(con.cursor())
    finally:
        con.close()
    lo, hi = _years(a.years)[0], _years(a.years)[-1]
    window = set(range(lo, hi + 1))
    bgn, end = list_window(_years(a.years))
    out: dict[str, dict] = {}
    stats: Counter = Counter()
    # 보통주 먼저 — 우선주·신주는 보통주 매핑을 따른다
    order = sorted(targets, key=lambda c: (dd.parent_common_code(c, (targets[c]["names"] or [""])[0]) is not None, c))
    for code in order:
        t = targets[code]
        if t["years"] & window or (t["delist_year"] is not None and lo <= t["delist_year"] <= hi + 1):
            group = "in_window"
        elif not t["years"] and t["delist_year"] is None:
            group = "unknown_years"  # 거래·폐지 연도를 모른다 — list.json 창(lo~hi) 안 정기보고서로만 확인
        else:
            group = "out_of_window"
        relevant = group != "out_of_window"
        parent = dd.parent_common_code(code, (t["names"] or [""])[0])
        if code in by_stock:
            rec = {"corp_code": by_stock[code], "status": "stock_code"}
            if group == "in_window" and t["years"] and not a.no_verify:
                # 코드 재사용 — corpCode 의 그 stock_code 주인이 다른(나중) 회사일 수 있다. 거래하던 해 정기보고서로 확인
                items = dart.filings(by_stock[code], bgn, end)
                if not dd._confirms(code, items, t["years"]):
                    rec["status"] = "stock_code_unconfirmed"
        elif parent:
            pc = by_stock.get(parent) or (out.get(parent) or {}).get("corp_code")
            rec = {"corp_code": pc, "status": "class_share_of_" + parent if pc else "class_share_unmapped", "parent": parent}
        else:
            cands = dd.name_candidates(code, t["names"], idx)
            if not relevant or a.no_verify:
                rec = {"corp_code": None, "status": "not_verified", "candidates": cands}
            elif len(cands) > a.max_candidates:
                rec = {"corp_code": None, "status": "ambiguous", "candidates": cands}
            else:
                items = {c: dart.filings(c, bgn, end) for c in cands}
                corp, status = dd.resolve_delisted(code, cands, items, t["years"])
                rec = {"corp_code": corp, "status": status, "candidates": cands}
        rec["names"] = t["names"]
        rec["traded_years"] = sorted(t["years"])
        rec["group"] = group
        rec["delist_year"] = t["delist_year"]
        out[code] = rec
        kind = "class_share" if parent else "common"
        st = re.sub(r"_\d+$", "", rec["status"]).replace("class_share_of", "class_share_mapped")
        stats[(group, kind, st)] += 1
    Path(a.out).write_text(json.dumps(out, ensure_ascii=False, indent=0))
    for k, v in sorted(stats.items()):
        print(k, v)
    print("dart_calls", dart.calls)
    return 0


# --------------------------------------------------------------------------- coverage: 연도별 추정 커버리지


def coverage(a: argparse.Namespace) -> int:  # pragma: no cover — 네트워크·DB
    """그해 거래대금 10억 이상인 날이 하루라도 있는 종목 중, 고유번호가 이어지고 그해 정기보고서가 있는 비율.

    거래대금: 폐지 종목은 ``daily_bars_delisted_naver.trade_value``(이미 close×volume 근사, 백만원).
    상장 종목은 2016-09 전 ``daily_bars_raw`` 가 없어 ``naver_investor_trend`` 의 close×volume 근사를 쓴다 —
    그 표가 아직 일부 종목만 채워져 있으면 분모도 그만큼만이다(결과에 분모 크기를 같이 낸다).
    """
    import psycopg2

    years = _years(a.years)
    dart = _dart(a)
    corp = _corp_map(dart, a.map)
    con = psycopg2.connect(os.environ["KR_QUANT_DB"])
    try:
        cur = con.cursor()
        cur.execute("SET statement_timeout = '900s'")
        cur.execute("SELECT code FROM stocks")
        listed_now = {r[0] for r in cur.fetchall()}
        denom: dict[tuple[int, str], set[str]] = {}
        cur.execute("""SELECT extract(year FROM date)::int, code FROM naver_investor_trend
                       WHERE date BETWEEN %s AND %s AND close::numeric * volume >= 1e9 GROUP BY 1, 2""",
                    (f"{years[0]}-01-01", f"{years[-1]}-12-31"))
        for y, code in cur.fetchall():
            denom.setdefault((y, "listed" if code in listed_now else "delisted"), set()).add(code)
        cur.execute("""SELECT extract(year FROM date)::int, code FROM daily_bars_delisted_naver
                       WHERE date BETWEEN %s AND %s AND trade_value >= 1000 GROUP BY 1, 2""",
                    (f"{years[0]}-01-01", f"{years[-1]}-12-31"))
        for y, code in cur.fetchall():
            denom.setdefault((y, "delisted"), set()).add(code)
    finally:
        con.close()
    bgn, end = list_window(years)
    table = []
    cache: dict[str, tuple[set[int], set[int]]] = {}
    for (y, grp), codes in sorted(denom.items()):
        c = Counter()
        for code in codes:
            c["n"] += 1
            cps = corp.get(code, [])
            if not cps:
                continue
            c["mapped"] += 1
            if code not in cache:
                items = [it for cp in cps for it in dart.filings(cp, bgn, end)]
                cache[code] = (dd.periodic_years(items, dec_only=True), dd.periodic_years(items, dec_only=False))
            dec, anyfye = cache[code]
            c["report_dec"] += y in dec
            c["report_any"] += y in anyfye
        table.append({"year": y, "group": grp, **c,
                      "mapped_pct": round(c["mapped"] / c["n"], 4), "report_dec_pct": round(c["report_dec"] / c["n"], 4),
                      "report_any_pct": round(c["report_any"] / c["n"], 4)})
    Path(a.out).write_text(json.dumps(table, ensure_ascii=False, indent=1))
    print(f"{'연도':>4} {'구분':<8} {'분모':>5} {'매핑':>7} {'12월결산보고서':>14} {'모든결산보고서':>14}")
    for r in table:
        print(f"{r['year']:>4} {r['group']:<8} {r['n']:>5} {r['mapped_pct']:>7.1%} {r['report_dec_pct']:>14.1%} {r['report_any_pct']:>14.1%}")
    print("dart_calls", dart.calls)
    return 0


def main(argv: list[str] | None = None) -> int:  # pragma: no cover
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)

    def common(p: argparse.ArgumentParser) -> None:
        p.add_argument("--cache", default=str(DEFAULT_CACHE), help="원문·list.json 캐시(레포 밖 디스크)")
        p.add_argument("--key-env", default="DART_API_KEY_3", help="키를 담은 환경변수 이름(값은 출력하지 않는다)")
        p.add_argument("--map", help="map-delisted 결과 — 폐지 종목 고유번호")

    p = sub.add_parser("run")
    common(p)
    p.add_argument("--codes", required=True)
    p.add_argument("--years", default="2007-2015")
    p.add_argument("--out", required=True, help="종목 단위 jsonl — 있으면 이어 쓴다")
    p.add_argument("--corrections", action="store_true", help="정정본도 받아 별도 레코드로")
    p.add_argument("--min-yield", type=float, default=0.9,
                   help="실행 실패 하한(연도별·종목별 산출률) — 완료 조건 C 의 일치율 바(95%%)와 다른 값이다")
    p.set_defaults(fn=run)
    p = sub.add_parser("compare")
    p.add_argument("--jsonl", required=True)
    p.set_defaults(fn=compare)
    p = sub.add_parser("map-delisted")
    common(p)
    p.add_argument("--years", default="2007-2016", help="거래 연도가 이 범위인 종목만 list.json 으로 확인")
    p.add_argument("--no-verify", action="store_true")
    p.add_argument("--max-candidates", type=int, default=4, help="이름이 같은 법인이 이보다 많으면 확인하지 않고 버린다")
    p.add_argument("--out", required=True)
    p.set_defaults(fn=map_delisted)
    p = sub.add_parser("coverage")
    common(p)
    p.add_argument("--years", default="2008-2016")
    p.add_argument("--out", required=True)
    p.set_defaults(fn=coverage)
    a = ap.parse_args(argv)
    return a.fn(a)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
