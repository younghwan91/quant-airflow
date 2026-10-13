"""DART 원문 파서(:mod:`collectors.dart_doc`)의 네트워크·DB 쪽 — 전체 실행·검증·폐지 종목 매핑·커버리지.

파싱은 전부 ``dart_doc`` 의 순수 함수다. 여기는 받아오고(디스크 캐시), 돌리고, 대조만 한다. DB 는 읽기만 한다.

하위 명령::

    python -m collectors.dart_doc_run run      --codes @codes.txt --years 2007-2015 --out out.jsonl
    python -m collectors.dart_doc_run compare  --jsonl out.jsonl            # earnings·shares 와 대조(2015~2016)
    python -m collectors.dart_doc_run map-delisted --out map.json            # 폐지 종목 → 고유번호
    python -m collectors.dart_doc_run targets --map map.json                 # 2007~2015 대상 → codes_1..3.txt
    python -m collectors.dart_doc_run load --jsonl out*.jsonl [--dry-run]    # → earnings_dart_doc(034)
    python -m collectors.dart_doc_run coverage --map map.json                # 적재 뒤 연도별 커버리지(호출 없음)
    python -m collectors.dart_doc_run reparse-prior --jsonl run_*.jsonl --update  # 전년 동기 열(캐시만, 호출 0)

키 3개 병렬 실행: 키마다 프로세스 하나, ``--key-env`` 와 ``--codes`` 만 다르게. 같은 캐시를 같이 써도 된다 —
캐시 파일은 임시 파일 → rename 으로만 쓴다. ``--max-calls``(기본 18,000)는 키별 하루 상한이라 운영 DAG 몫(키 일한도
20,000 중)을 남기고, 닿으면 그 종목을 쓰지 않고 종료 3 으로 멈춘다 — 다음 날 같은 명령이 이어받는다.

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


class DartBudget(RuntimeError):
    """``--max-calls`` 하루 상한 — 운영 DAG 몫을 남기려고 우리가 먼저 멈춘다. 다음 날 같은 명령으로 이어받는다."""


def atomic_write(path: Path, data: bytes) -> None:
    """임시 파일에 쓰고 rename — 같은 캐시를 여러 프로세스(키 3개 병렬)가 써도 반쯤 쓴 파일을 읽지 않는다."""
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    tmp.write_bytes(data)
    os.replace(tmp, path)


class Dart:  # pragma: no cover — 네트워크
    """list.json·document.xml 디스크 캐시 + 호출 수 계수. 키 값은 어디에도 찍지 않는다."""

    def __init__(self, key: str, cache: Path, *, retries: int = 4, pause: float = 0.12,
                 key_name: str = "", max_calls: int | None = None) -> None:
        self.key, self.cache, self.calls, self.retries, self.pause = key, cache, 0, retries, pause
        self.max_calls = max_calls
        cache.mkdir(parents=True, exist_ok=True)
        # 키별 오늘 호출 수 — 같은 날 다시 시작해도 상한이 이어진다(키 하나 = 프로세스 하나 전제)
        self._counter = cache / f"calls_{key_name or 'key'}_{time.strftime('%Y%m%d')}.txt"
        self.today = int(self._counter.read_text() or 0) if self._counter.exists() else 0

    def _tick(self) -> None:
        if self.max_calls is not None and self.today >= self.max_calls:
            self.flush()
            raise DartBudget(f"오늘 {self.today}회 — 상한 {self.max_calls}")
        self.calls += 1
        self.today += 1
        if self.today % 50 == 0:
            self.flush()

    def flush(self) -> None:
        atomic_write(self._counter, str(self.today).encode())

    def _get(self, path: str, **params: str) -> bytes:
        import requests

        last = ""
        for attempt in range(self.retries):
            self._tick()
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
            atomic_write(p, json.dumps(rows, ensure_ascii=False).encode())
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
        atomic_write(p, json.dumps(items, ensure_ascii=False).encode())
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
                atomic_write(p, body)
                return body
            st = self._status(body)
            if st == "020":
                raise DartQuota("document.xml")
            if st in NO_DATA:
                atomic_write(miss, body[:600])
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
    return Dart(os.environ[a.key_env], Path(a.cache).expanduser(), key_name=a.key_env,
                max_calls=getattr(a, "max_calls", None))


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


def read_jsonl(path: Path, *, repair: bool = True) -> list[dict]:
    """종목 단위 jsonl — 마지막 줄이 쓰다 잘렸으면(중단) 그 줄을 버리고, ``repair`` 면 파일도 거기까지로 줄인다.

    다른 프로세스가 쓰고 있는 파일(운영 run_*.jsonl)을 읽을 때는 ``repair=False`` — 읽기만 한다.
    """
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
            if repair:
                path.write_text("".join(x + "\n" for x in lines[:i]))
    return out


#: 실행 실패 하한(기본 90%) — 이걸 밑돌면 장애로 끝낸다(CLAUDE.md §5). 완료 조건 C 의 합격 바(95%, 사용자 승인)
#: 와 다르다: 합격 바는 earnings 와의 **일치율**이고, 이 하한은 받은 보고서 중 값을 낸 **산출률**이다.
def code_status(r: dict) -> str:
    """종목 단위 결과: ``non_dec``(12월 결산 아님 — 알려진 비커버리지)·``no_map``(고유번호 없음)·
    ``no_filing``(고유번호는 있는데 창 안 정기보고서 없음)·``ok``(값 하나 이상)·``parse_fail``(보고서는 있는데 값 없음)."""
    err = r.get("error")
    if err == "no_dec_fye_filing":
        return "non_dec"
    if err in ("no_map", "no_filing"):
        return err
    return "ok" if any(x["netinc"] is not None for x in r.get("records", [])) else "parse_fail"


def yield_report(rows: list[dict]) -> dict:
    """누적 jsonl 전체로 산출률을 잰다(이어 하기여도 지금까지 전부).

    - 연도별: 그해 보고서가 있는 분기 중 netinc 를 낸(어느 판에서든) 비율. 보고서가 0건인 연도도 실패다.
    - 종목별(:func:`code_status`): 하한 분모 = ok + parse_fail + no_map + no_filing. 12월 결산이 아닌 종목(non_dec)은
      파서가 일부러 다루지 않는 **알려진 비커버리지**라 분모에서 빼고 따로 센다. no_map·no_filing 은 진짜 구멍이라 남긴다.
    """
    st: Counter = Counter()
    for r in rows:
        st["codes"] += 1
        st[f"code_{code_status(r)}"] += 1
        st["parse_errors"] += r.get("parse_errors", 0)
        valued = {x["period"] for x in r.get("records", []) if x["netinc"] is not None}
        for period, f in r.get("filings", {}).items():
            st[f"y{period[:4]}_filings"] += 1
            st[f"y{period[:4]}_with_value"] += period in valued  # 원본이 목록에 없어 첫 판이 정정본인 분기도 센다
            st[f"y{period[:4]}_original_missing"] += bool(f.get("original_missing"))
    return dict(st)


def yield_failures(st: dict, years: list[int], floor: float) -> list[str]:
    bad = []
    for y in years:
        n, v = st.get(f"y{y}_filings", 0), st.get(f"y{y}_with_value", 0)
        if n == 0:
            bad.append(f"{y}: 보고서 0건")
        elif v / n < floor:
            bad.append(f"{y}: {v}/{n} = {v / n:.1%}")
    ok = st.get("code_ok", 0)
    n = ok + sum(st.get(f"code_{k}", 0) for k in ("parse_fail", "no_map", "no_filing"))
    if n and ok / n < floor:
        bad.append(f"종목: {ok}/{n} = {ok / n:.1%}(매핑 없음·보고서 없음 포함, 12월 결산 아님 {st.get('code_non_dec', 0)} 제외)")
    return bad


# --------------------------------------------------------------------------- run: 전체 실행 → 종목별 jsonl


def _process_code(dart: Dart, corp: dict[str, list[str]], code: str, years: list[int], bgn: str, end: str,
                  corrections: bool, stats: Counter, fe) -> dict:  # pragma: no cover — 네트워크
    items = [it for cp in corp.get(code, []) for it in dart.filings(cp, bgn, end)]
    filings = {p: f for p, f in dd.select_filings(items).items() if int(p[:4]) in years}
    reports: dict[str, list[dd.ReportResult]] = {}
    meta: dict[str, dict] = {}
    n_parse_err = 0
    for period, f in sorted(filings.items()):
        rnos = [f["rcept_no"]] + (f["corrections"] if corrections else [])
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
                n_parse_err += 1
                fe.write(f"{code} {period} {rno}\n{traceback.format_exc()}\n")
                continue
            # 원본이 목록에 없으면 첫 판도 정정본이다
            rr.is_correction = k > 0 or f["original_missing"]
            reports.setdefault(period, []).append(rr)
        meta[period] = {"rcept_no": f["rcept_no"], "corrections": f["corrections"],
                        "original_missing": f["original_missing"]}
    recs = dd.build_records(code, reports)
    stats["records"] += len(recs)
    return {"code": code, "filings": meta, "records": recs, "parse_errors": n_parse_err,
            "error": None if filings else ("no_map" if not corp.get(code) else
                                           "no_filing" if not items else "no_dec_fye_filing")}


def run(a: argparse.Namespace) -> int:  # pragma: no cover — 네트워크
    """종목 단위 jsonl 에 이어 쓴다. 상한(``--max-calls``)·일한도(020)에 닿으면 그 종목은 쓰지 않고 멈춘다(종료 3)
    — 다음 날 같은 명령이 이어받는다(받은 원문은 캐시에 남아 있어 그 종목도 다시 부르는 수가 적다)."""
    years = _years(a.years)
    dart = _dart(a)
    corp = _corp_map(dart, a.map)
    out = Path(a.out)
    done = {r["code"] for r in read_jsonl(out)}  # 이어 하기 — 이미 쓴 종목은 건너뛴다
    errlog = out.with_suffix(".errors.log")
    bgn, end = list_window(years)
    stats: Counter = Counter()
    stopped = None
    with out.open("a") as fo, errlog.open("a") as fe:
        for code in _codes(a.codes):
            if code in done:
                stats["resumed_skip"] += 1
                continue
            try:
                line = _process_code(dart, corp, code, years, bgn, end, a.corrections, stats, fe)
            except (DartBudget, DartQuota) as e:
                stopped = f"{type(e).__name__}: {e}"
                break
            except DartTransient as e:  # 재시도로도 안 된 일시 오류 — 이 종목은 쓰지 않고 다음 실행에 맡긴다
                stats["transient_skip"] += 1
                fe.write(f"{code} transient {e}\n")
                continue
            fo.write(json.dumps(line, ensure_ascii=False, default=str) + "\n")
            fo.flush()
    dart.flush()
    stats["dart_calls"] = dart.calls
    stats["dart_calls_today_this_key"] = dart.today
    st = yield_report(read_jsonl(out))
    print(json.dumps({**dict(stats), **st}, ensure_ascii=False))
    if stopped:
        print(f"⏸ 멈춤({stopped}) — 다음 날 같은 명령으로 이어받는다", file=sys.stderr)
        return 3
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


# --------------------------------------------------------------------------- load: jsonl → earnings_dart_doc


def record_row(code: str, rec: dict, *, pg: bool) -> tuple:
    """build_records 레코드 → ``storage.EARNINGS_DART_DOC_COLS`` 순서 튜플. prior 열은 jsonl 에 있으면 넣고 없으면 NULL."""
    from .storage import EARNINGS_DART_DOC_COLS

    sh = rec.get("shares") or {}
    src, flags = rec.get("src") or {}, list(rec.get("flags") or [])
    if pg:  # pragma: no cover — Postgres 경로
        import psycopg2.extras

        src_v, flags_v = psycopg2.extras.Json(src), flags
    else:
        src_v, flags_v = json.dumps(src, ensure_ascii=False), json.dumps(flags, ensure_ascii=False)
    val = {**rec, "code": code, "shares_common": sh.get("common"), "shares_preferred": sh.get("preferred"),
           "shares_total": sh.get("total"), "shares_asof": sh.get("asof"),
           "shares_asof_inferred": bool(sh.get("asof_inferred")), "shares_method": sh.get("method"),
           "src": src_v, "flags": flags_v, "source": "dart_doc"}
    bools = {"is_correction", "derived", "derived_ctrl", "ctrl_q_derived", "cfs_q_derived", "ofs_q_derived",
             "shares_asof_inferred"}
    return tuple((bool(val.get(c)) if pg else int(bool(val.get(c)))) if c in bools else val.get(c)
                 for c in EARNINGS_DART_DOC_COLS)


def load_rows(con, rows: list[dict], *, page: int = 5000) -> int:
    """jsonl 행들 → ``earnings_dart_doc`` upsert. 0건이면 예외(§5). 같은 PK 는 접수번호가 늦은 행이 남는다."""
    from .storage import _is_pg, upsert_earnings_dart_doc

    pg = _is_pg(con)
    recs = [record_row(r["code"], x, pg=pg) for r in rows for x in r.get("records", [])]
    if not recs:
        raise RuntimeError("적재할 레코드가 0건이다 — 빈 결과를 성공으로 넘기지 않는다(CLAUDE.md §5)")
    n = 0
    for i in range(0, len(recs), page):
        n += upsert_earnings_dart_doc(con, recs[i:i + page])
    return n


def load(a: argparse.Namespace) -> int:  # pragma: no cover — DB (테스트는 load_rows 로)
    rows = [r for p in a.jsonl for r in read_jsonl(Path(p))]
    by: Counter = Counter()
    for r in rows:
        for x in r.get("records", []):
            by[(x["period"][:4], "correction" if x["is_correction"] else "original")] += 1
    if not by:
        raise RuntimeError("적재할 레코드가 0건이다 — 빈 결과를 성공으로 넘기지 않는다(CLAUDE.md §5)")
    print(f"종목 {len(rows)} · 레코드 {sum(by.values())}")
    for (y, k), n in sorted(by.items()):
        print(f"  {y} {k:<10} {n:>7}")
    if a.dry_run:
        return 0
    from .storage import connect

    con = connect(a.db)
    try:
        n = load_rows(con, rows)
        con.commit()
    finally:
        con.close()
    print(f"earnings_dart_doc upsert {n}행")
    return 0


# --------------------------------------------------------------------------- reparse-prior: 전년 동기 열 채우기

PRIOR_FIELDS = ("netinc_prior", "netinc_ctrl_prior", "ctrl_prior_q", "cfs_prior_q", "ofs_prior_q", "prior_basis_note")


def reparse_prior_rows(rows: list[dict], cache: Path) -> tuple[list[dict], Counter]:
    """jsonl 레코드의 접수번호 원문을 **캐시에서만** 다시 읽어 전년 동기 열을 만든다(DART 호출 0).

    종목마다 레코드에 있는 판(원본·정정)을 모두 다시 파싱해 ``build_records`` 를 돌린다 — 전년 동기 값은 그 분기
    값과 같은 기준·같은 표의 비교 열이라, 당기 값의 기준·차분 여부를 다시 계산해야 맞출 수 있다. 다시 만든 당기
    netinc 가 jsonl 과 다르면 ``netinc_drift`` 로 센다(파서가 바뀐 경우 — 그 레코드도 그대로 낸다).
    """
    st: Counter = Counter()
    out: list[dict] = []
    for r in rows:
        recs = r.get("records") or []
        if not recs:
            continue
        reports: dict[str, list[dd.ReportResult]] = {}
        for x in recs:
            z = cache / f"doc_{x['rcept_no']}.zip"
            if not z.exists():
                st["doc_not_cached"] += 1
                continue
            try:
                rr = dd.parse_report(dd.doc_files(z.read_bytes()), x["rcept_no"], int(x["period"][:4]), int(x["period"][-1]))
            except Exception:  # noqa: BLE001 — 문서 하나의 예외는 격리
                st["parse_error"] += 1
                continue
            rr.is_correction = bool(x["is_correction"])
            reports.setdefault(x["period"], []).append(rr)
        new = {(y["period"], y["knowledge_date"]): y for y in dd.build_records(r["code"], reports)}
        lines = []
        for x in recs:
            y = new.get((x["period"], x["knowledge_date"]))
            if y is None:
                st["record_missing"] += 1
                continue
            st["records"] += 1
            st["drift"] += not dd.agrees(y["netinc"], x["netinc"]) and not (y["netinc"] is None and x["netinc"] is None)
            st["with_prior"] += y["netinc_prior"] is not None
            lines.append({"period": x["period"], "knowledge_date": x["knowledge_date"], "rcept_no": x["rcept_no"],
                          **{f: y[f] for f in PRIOR_FIELDS}})
        out.append({"code": r["code"], "records": lines})
    return out, st


def reparse_prior(a: argparse.Namespace) -> int:  # pragma: no cover — 캐시·DB
    cache = Path(a.cache).expanduser()
    rows = [r for p in a.jsonl for r in read_jsonl(Path(p), repair=False)]  # 운영 run_*.jsonl 은 읽기만
    out, st = reparse_prior_rows(rows, cache)
    if not st["records"]:
        raise RuntimeError("전년 동기 열을 만든 레코드 0건 — 빈 결과를 성공으로 넘기지 않는다(CLAUDE.md §5)")
    if a.out:
        with Path(a.out).open("w") as fo:
            for line in out:
                fo.write(json.dumps(line, ensure_ascii=False) + "\n")
    print(json.dumps(dict(st), ensure_ascii=False))
    if a.update:
        from .storage import connect

        con = connect(a.db)
        try:
            n = update_prior(con, out)
            con.commit()
        finally:
            con.close()
        print(f"earnings_dart_doc 전년 동기 열 UPDATE {n}행")
    return 0


def update_prior(con, out: list[dict]) -> int:
    """``earnings_dart_doc`` 의 전년 동기 열만 PK 로 UPDATE(당기 열은 건드리지 않는다). 0행이면 예외."""
    from .storage import _is_pg

    ph = "%s" if _is_pg(con) else "?"
    sql = (f"UPDATE earnings_dart_doc SET {', '.join(f'{f} = {ph}' for f in PRIOR_FIELDS)} "  # noqa: S608 — 고정 열 이름
           f"WHERE code = {ph} AND period = {ph} AND knowledge_date = {ph}")
    params = [(*(x[f] for f in PRIOR_FIELDS), r["code"], x["period"], x["knowledge_date"])
              for r in out for x in r["records"]]
    cur = con.cursor()
    n = 0
    for p in params:
        cur.execute(sql, p)
        n += cur.rowcount
    if n == 0:
        raise RuntimeError("UPDATE 0행 — 적재 전이거나 PK 가 안 맞는다(CLAUDE.md §5)")
    return n


# --------------------------------------------------------------------------- targets: 전체 실행 대상


def targets(a: argparse.Namespace) -> int:  # pragma: no cover — 네트워크·DB
    """2007~2015 에 한 번이라도 상장돼 있던 보통주 → ``codes_1.txt`` … ``codes_N.txt``(키 N개 병렬용, 번갈아 나눈다).

    - 상장 중(``stocks``, 보통주만): 2016-09 전 거래 이력이 DB 에 아직 없어 list.json 으로 창(2007~2015) 안
      정기보고서가 있는 종목만 넣는다 — 그 list 캐시는 run 이 그대로 다시 쓴다(호출 낭비 없음).
    - 폐지(``daily_bars_delisted_naver`` 2007~2015 거래 ∪ map-delisted 의 in_window): 보통주(끝자리 0)만.
      펀드처럼 거래하던 해에 정기보고서가 없다고 확인된 것(``stock_code_unconfirmed``)은 실적 대상이 아니라 뺀다.
      매핑이 안 된 종목은 **넣는다** — run 이 no_map(진짜 구멍)으로 센다.
    """
    import psycopg2

    years = _years(a.years)
    dart = _dart(a)
    mapping = json.loads(Path(a.map).read_text()) if a.map else {}
    corp = corp_map(dart.corp_rows(), mapping)
    con = psycopg2.connect(os.environ["KR_QUANT_DB"])
    try:
        cur = con.cursor()
        cur.execute("SET statement_timeout = '900s'")
        cur.execute("SELECT code FROM stocks WHERE code ~ '^[0-9]{5}0$'")
        listed = sorted(r[0] for r in cur.fetchall())
        cur.execute("SELECT DISTINCT code FROM daily_bars_delisted_naver "
                    "WHERE date BETWEEN %s AND %s AND code ~ '^[0-9]{5}0$'",
                    (f"{years[0]}-01-01", f"{years[-1]}-12-31"))
        delisted = {r[0] for r in cur.fetchall()}
    finally:
        con.close()
    delisted |= {c for c, m in mapping.items() if c.endswith("0") and m.get("group") == "in_window"
                 and dd.parent_common_code(c, (m.get("names") or [""])[0]) is None}
    delisted -= set(listed)
    bgn, end = list_window(years)
    stats: Counter = Counter()
    out: list[str] = []
    try:
        for code in listed:
            yrs: set[int] = set()
            for cp in corp.get(code, []):
                yrs |= dd.periodic_years(dart.filings(cp, bgn, end), dec_only=False)
            if yrs & set(years):
                out.append(code)
                stats["listed_in"] += 1
            else:
                stats["listed_out"] += 1
    except (DartBudget, DartQuota) as e:
        dart.flush()
        print(f"⏸ 멈춤({e}) — 상장 종목 확인 중. 다시 실행하면 캐시로 이어간다", file=sys.stderr)
        return 3
    for code in sorted(delisted):
        if (mapping.get(code) or {}).get("status") == "stock_code_unconfirmed":
            stats["delisted_excluded_no_reports"] += 1
            continue
        out.append(code)
        stats["delisted_in" if code in corp else "delisted_unmapped"] += 1
    dart.flush()
    out = sorted(set(out))
    if not out:
        raise RuntimeError("대상 0종목 — 빈 결과를 성공으로 넘기지 않는다(CLAUDE.md §5)")
    d = Path(a.cache).expanduser()
    for k in range(a.parts):
        atomic_write(d / f"codes_{k + 1}.txt", (" ".join(out[k::a.parts]) + "\n").encode())
    print(json.dumps({**dict(stats), "total": len(out), "parts": a.parts, "dart_calls": dart.calls}, ensure_ascii=False))
    return 0


# --------------------------------------------------------------------------- coverage: 적재 뒤 연도별 커버리지


def coverage(a: argparse.Namespace) -> int:  # pragma: no cover — DB·캐시
    """적재된 ``earnings_dart_doc`` 기준 연도별 커버리지(상장/폐지 따로). DART 호출 없음(list 캐시만 읽는다).

    분모: 그해 거래대금 10억 이상인 날이 하루라도 있는 종목.
    - 상장 종목: ``daily_bars_raw``(trade_value 백만원)가 그해에 있으면 그것, 없으면 ``naver_investor_trend``
      종가×거래량 근사(``가격`` 열에 ``근사`` 표시).
    - 폐지 종목: ``daily_bars_delisted_naver``(trade_value 자체가 종가×거래량 근사, 백만원 — 늘 ``근사``).
    열: 분모·실적 있는 종목(비율, 우선주는 보통주 실적이 있으면 있음)·알려진 비커버리지(12월 결산 아님·외국 기업 9xxxxx·
    매핑 없음·보고서 없음)를 뺀 비율·12월 결산 아님 종목·그 종목-일 비중·미실행(list 캐시 없음).
    12월 결산 여부·보고서 유무는 run 이 쓴 list.json 캐시(사업보고서 기준월)로 본다.
    """
    import psycopg2

    years = _years(a.years)
    cache = Path(a.cache).expanduser()
    mapping = json.loads(Path(a.map).read_text()) if a.map else {}
    corp = corp_map(json.loads((cache / "corpCode.json").read_text()), mapping)
    bgn, end = list_window(years)
    con = psycopg2.connect(os.environ["KR_QUANT_DB"])
    try:
        cur = con.cursor()
        cur.execute("SET statement_timeout = '1800s'")
        cur.execute("SELECT code FROM stocks")
        listed_now = {r[0] for r in cur.fetchall()}
        days: dict[tuple[int, str], dict[str, int]] = {}
        src_used: dict[tuple[int, str], str] = {}
        for y in years:
            span = (f"{y}-01-01", f"{y}-12-31")
            cur.execute("SELECT code, count(*) FROM daily_bars_raw WHERE date BETWEEN %s AND %s GROUP BY code "
                        "HAVING max(trade_value) >= 1000", span)
            raw = dict(cur.fetchall())
            if raw:
                src_used[(y, "listed")] = "daily_bars_raw"
            else:
                cur.execute("SELECT code, count(*) FROM naver_investor_trend WHERE date BETWEEN %s AND %s "
                            "GROUP BY code HAVING max(close::numeric * volume) >= 1e9", span)
                raw = dict(cur.fetchall())
                src_used[(y, "listed")] = "naver_investor_trend(근사)"
            for code, n in raw.items():
                days.setdefault((y, "listed" if code in listed_now else "delisted"), {})[code] = n
            cur.execute("SELECT code, count(*) FROM daily_bars_delisted_naver WHERE date BETWEEN %s AND %s "
                        "GROUP BY code HAVING max(trade_value) >= 1000", span)
            for code, n in cur.fetchall():
                if code not in listed_now:
                    days.setdefault((y, "delisted"), {}).setdefault(code, n)
            src_used.setdefault((y, "delisted"), "daily_bars_delisted_naver(근사)")
        cur.execute("SELECT DISTINCT code, left(period, 4)::int FROM earnings_dart_doc WHERE netinc IS NOT NULL")
        have = {(c, y) for c, y in cur.fetchall()}
    finally:
        con.close()

    def fye(code: str) -> tuple[set[int], set[int]] | None:
        items: list[dict] = []
        for cp in corp.get(code, []):
            p = cache / f"list_{cp}_{bgn}_{end}.json"
            if not p.exists():
                return None
            items += json.loads(p.read_text())
        return dd.periodic_years(items, dec_only=True), dd.periodic_years(items, dec_only=False)

    table = []
    for (y, grp), dmap in sorted(days.items()):
        c: Counter = Counter({k: 0 for k in ("n", "with_rows", "pref_via_common", "foreign", "no_map", "not_run",
                                              "no_report", "non_dec", "non_dec_days")})
        tot_days = sum(dmap.values())
        for code, n in dmap.items():
            c["n"] += 1
            # 우선주(끝자리 0 아님)는 같은 회사 보통주 공시로 실적이 있다 — 2026-10-14 폐지 잔여 177건 전부 이것
            via_common = code[-1] != "0" and (code[:5] + "0", y) in have
            c["with_rows"] += (code, y) in have or via_common
            c["pref_via_common"] += via_common and (code, y) not in have
            if code.startswith("9") and (code, y) not in have:
                c["foreign"] += 1   # 900xxx 외국 기업(외화 재무제표) — 파서 대상 아님
                continue
            if not corp.get(code):
                c["no_map"] += 1
                continue
            f = fye(code)
            if f is None:
                c["not_run"] += 1
                continue
            dec, anyfye = f
            if y not in anyfye:
                c["no_report"] += 1
            elif y not in dec:
                c["non_dec"] += 1
                c["non_dec_days"] += n
        known = c["non_dec"] + c["no_map"] + c["no_report"] + c["foreign"]
        table.append({"year": y, "group": grp, "price_src": src_used.get((y, grp)), **c,
                      "with_rows_pct": round(c["with_rows"] / c["n"], 4) if c["n"] else None,
                      "with_rows_pct_ex_known": round(c["with_rows"] / (c["n"] - known), 4) if c["n"] > known else None,
                      "non_dec_day_share": round(c["non_dec_days"] / tot_days, 4) if tot_days else None})
    if a.out:
        Path(a.out).write_text(json.dumps(table, ensure_ascii=False, indent=1))
    print(f"{'연도':>4} {'구분':<8} {'분모':>5} {'실적있음':>8} {'비율':>6} {'제외후':>6} {'비12월':>6} {'비12월 종목-일':>12} "
          f"{'외국':>4} {'매핑없음':>8} {'보고서없음':>9} {'미실행':>6}  가격")
    for r in table:
        print(f"{r['year']:>4} {r['group']:<8} {r['n']:>5} {r['with_rows']:>8} {r['with_rows_pct'] or 0:>6.1%} "
              f"{r['with_rows_pct_ex_known'] or 0:>6.1%} {r['non_dec']:>6} {r['non_dec_day_share'] or 0:>12.1%} "
              f"{r['foreign']:>4} {r['no_map']:>8} {r['no_report']:>9} {r['not_run']:>6}  {r['price_src']}")
    return 0


def main(argv: list[str] | None = None) -> int:  # pragma: no cover
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)

    def common(p: argparse.ArgumentParser) -> None:
        p.add_argument("--cache", default=str(DEFAULT_CACHE), help="원문·list.json 캐시(레포 밖 디스크)")
        p.add_argument("--key-env", default="DART_API_KEY_3", help="키를 담은 환경변수 이름(값은 출력하지 않는다)")
        p.add_argument("--map", help="map-delisted 결과 — 폐지 종목 고유번호")
        p.add_argument("--max-calls", type=int, default=18_000,
                       help="이 키의 하루 호출 상한(운영 DAG 몫을 남긴다, 키 일한도 20,000) — 닿으면 깨끗이 멈춘다(종료 3)")

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
    p = sub.add_parser("coverage", help="적재 뒤 연도별 커버리지(상장/폐지) — DART 호출 없음")
    p.add_argument("--cache", default=str(DEFAULT_CACHE))
    p.add_argument("--map")
    p.add_argument("--years", default="2008-2016")
    p.add_argument("--out")
    p.set_defaults(fn=coverage)
    p = sub.add_parser("load", help="jsonl → earnings_dart_doc upsert")
    p.add_argument("--jsonl", nargs="+", required=True)
    p.add_argument("--db", default=os.environ.get("KR_QUANT_DB"), help="DSN 또는 sqlite 경로(기본 KR_QUANT_DB)")
    p.add_argument("--dry-run", action="store_true", help="연도별 레코드 수만 찍는다")
    p.set_defaults(fn=load)
    p = sub.add_parser("reparse-prior", help="캐시 원문만 다시 읽어 전년 동기 열 채우기(DART 호출 0)")
    p.add_argument("--jsonl", nargs="+", required=True, help="run 결과(읽기만 한다)")
    p.add_argument("--cache", default=str(DEFAULT_CACHE))
    p.add_argument("--out", help="PK + 전년 동기 열 jsonl")
    p.add_argument("--update", action="store_true", help="earnings_dart_doc 의 전년 동기 열을 UPDATE")
    p.add_argument("--db", default=os.environ.get("KR_QUANT_DB"))
    p.set_defaults(fn=reparse_prior)
    p = sub.add_parser("targets", help="2007~2015 상장 이력 보통주 → codes_N.txt")
    common(p)
    p.add_argument("--years", default="2007-2015")
    p.add_argument("--parts", type=int, default=3)
    p.set_defaults(fn=targets)
    a = ap.parse_args(argv)
    return a.fn(a)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
