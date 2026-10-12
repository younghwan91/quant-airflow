"""네이버 m.stock JSON — 2008~ 일별 외국인 보유율·투자자 순매수, 증권사 리포트 목표가 (migration 032).

**왜.** swing-it 이 2008~2016 독립 표본에서 수급 신호를 시험하려는데(사용자 "데이터가 없으면 만들어서라도"),
키움은 외국인 보유율을 최근 50일(ka10008)만, 투자자 순매수(ka10059)는 폐지 코드에 빈 응답을 준다.
옛 ``finance.naver.com/item/frgn.naver`` 는 2026 개편(Next.js)으로 표가 사라졌다 — 2026-10-13 실측 행 0.
같은 자료를 모바일 JSON 이 준다(로그인 불요, 2008·폐지 종목 확인):

- ``/api/stock/{code}/trend?pageSize=60&bizdate=YYYYMMDD`` — bizdate **이전** 60거래일(61 이상은 400).
  종가·거래량은 원 가격(005930 2007-10-12 557,000). 개인 순매수는 옛 구간이 '-'(모름 → NULL).
  외국인 순매수는 개인+외국인+기관+기타법인 합 0 으로 맞춘 네이버 정의(키움 foreign_ 과 다르다, naver_supply_demand).
- ``/api/research/stock/{code}?pageSize=100&page=N`` 리포트 목록(삼성전자 2013-04 까지), 상세
  ``/api/research/stock/{code}/{researchId}`` 에 opinion·goalPrice. 게시 **시각은 없다** — write_date 는 날짜뿐이라
  as-of 는 다음 거래일로 쓰는 게 안전하다. EPS 컨센서스 이력은 무료 원천이 없다.

대상 = stocks(6자리) ∪ daily_bars_delisted_naver 코드(028 의 폐지 유니버스). 조회한 코드는 backfill_markers 로
남겨 재실행 때 건너뛴다(자료 유무 무관).

CLI:
    python -m collectors.naver_mstock trend --db <DSN>               # 2008-01~ 전 종목
    python -m collectors.naver_mstock research --db <DSN>
    python -m collectors.naver_mstock trend --db <DSN> --codes 005930 --refetch
"""

from __future__ import annotations

import argparse
import json
import time
import urllib.error
import urllib.request
from datetime import date, datetime
from typing import Any

from .storage import _upsert, checked_codes, connect, default_db_path, fetchall, mark_checked

BASE = "https://m.stock.naver.com/api"
UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128 Safari/537.36"
SINCE = "2008-01-01"
PAGE = 60
CHECKED_TREND = "naver_trend_2008"
CHECKED_RESEARCH = "naver_research"

TREND_COLUMNS = ["code", "date", "close", "volume", "foreign_hold_ratio",
                 "foreign_net", "institution_net", "individual_net", "source"]
RESEARCH_COLUMNS = ["research_id", "code", "item_name", "broker", "write_date", "title", "category",
                    "opinion", "goal_price", "prev_goal_price", "price_at_write", "attach_url", "source"]


def get_json(path: str, *, retries: int = 3, timeout: int = 20) -> Any:
    """GET → JSON. 4xx 는 None(요청이 틀렸다 — 재시도해도 같다), 네트워크 오류는 재시도 뒤 예외."""
    req = urllib.request.Request(BASE + path, headers={"User-Agent": UA})
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:  # noqa: S310 — 고정 호스트
                return json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if 400 <= e.code < 500:
                return None
            if attempt == retries - 1:
                raise
        except (urllib.error.URLError, TimeoutError, OSError, json.JSONDecodeError):
            if attempt == retries - 1:
                raise
        time.sleep(2 * (attempt + 1))
    return None


def _num(s: Any) -> int | None:
    """'+1,234' → 1234, '-' / '' / None → None (모름 — 0 으로 바꾸지 않는다)."""
    if s is None:
        return None
    t = str(s).replace(",", "").replace("+", "").strip()
    if t in ("", "-"):
        return None
    try:
        return int(t)
    except ValueError:
        return None


def _pct(s: Any) -> float | None:
    t = str(s or "").replace("%", "").replace(",", "").strip()
    try:
        return float(t)
    except ValueError:
        return None


def parse_trend(items: list[dict], code: str) -> list[tuple]:
    """trend 응답 → TREND_COLUMNS. 종가가 없는 행(폐지일 '-' 행)은 버린다."""
    out = []
    for x in items or []:
        bd = x.get("bizdate") or ""
        close = _num(x.get("closePrice"))
        if len(bd) != 8 or close is None or close <= 0:
            continue
        out.append((code, f"{bd[:4]}-{bd[4:6]}-{bd[6:]}", close, _num(x.get("accumulatedTradingVolume")),
                    _pct(x.get("foreignerHoldRatio")), _num(x.get("foreignerPureBuyQuant")),
                    _num(x.get("organPureBuyQuant")), _num(x.get("individualPureBuyQuant")), "naver_mstock"))
    return out


def fetch_trend(code: str, *, since: str = SINCE, get=get_json, sleep: float = 0.2,
                max_calls: int = 120) -> list[tuple]:
    """최근부터 ``since`` 까지 60거래일씩 거슬러 간다. bizdate 는 그 날짜 **이전** 행을 준다."""
    rows: list[tuple] = []
    bizdate = ""
    for _ in range(max_calls):
        q = f"/stock/{code}/trend?pageSize={PAGE}" + (f"&bizdate={bizdate}" if bizdate else "")
        items = get(q)
        time.sleep(sleep)
        if not items:
            break
        oldest = min(x.get("bizdate") or "99999999" for x in items)
        rows.extend(r for r in parse_trend(items, code) if r[1] >= since)
        if oldest.replace("-", "") <= since.replace("-", "") or len(items) < PAGE or oldest == bizdate:
            break
        bizdate = oldest
    return rows


def parse_research(item: dict, detail: dict | None) -> tuple:
    rc = (detail or {}).get("researchContent") or {}
    return (int(item["researchId"]), item.get("itemCode"), item.get("itemName"), item.get("brokerName"),
            item.get("writeDate"), item.get("title"), item.get("category"),
            rc.get("opinion") or None, _num(rc.get("goalPrice")), _num(rc.get("prevGoalPrice")),
            _num(rc.get("priceAtWriteDate")), rc.get("attachUrl"), "naver_research")


def fetch_research(code: str, known: set[int], *, since: str = SINCE, get=get_json,
                   sleep: float = 0.2, max_pages: int = 50) -> list[tuple]:
    """리포트 목록을 넘기며 ``known`` 에 없는 것만 상세를 받는다."""
    out = []
    for page in range(1, max_pages + 1):
        items = get(f"/research/stock/{code}?pageSize=100&page={page}")
        time.sleep(sleep)
        if not items:
            break
        for it in items:
            if (it.get("writeDate") or "") < since or int(it["researchId"]) in known:
                continue
            detail = get(f"/research/stock/{code}/{it['researchId']}")
            time.sleep(sleep)
            out.append(parse_research(it, detail))
        if len(items) < 100 or min(it.get("writeDate") or "" for it in items) < since:
            break
    return out


def universe(con: Any) -> list[str]:
    listed = [r[0] for r in fetchall(con, "SELECT code FROM stocks")]
    delisted = [r[0] for r in fetchall(con, "SELECT DISTINCT code FROM daily_bars_delisted_naver")]
    return sorted({c for c in listed + delisted if len(c) == 6 and c.isdigit()})


def run(con: Any, kind: str, codes: list[str], *, refetch: bool = False, sleep: float = 0.2) -> dict[str, int]:
    marker = CHECKED_TREND if kind == "trend" else CHECKED_RESEARCH
    done = set() if refetch else checked_codes(con, marker)
    todo = [c for c in codes if c not in done]
    known = {r[0] for r in fetchall(con, "SELECT research_id FROM naver_research_reports")} if kind == "research" else set()
    stats = {"codes": 0, "empty": 0, "rows": 0, "skipped": len(codes) - len(todo)}
    t0 = time.time()
    today = date.today().isoformat()
    for i, code in enumerate(todo, 1):
        if kind == "trend":
            recs = fetch_trend(code, sleep=sleep)
            stats["rows"] += _upsert(con, "naver_investor_trend", TREND_COLUMNS, recs)
        else:
            recs = fetch_research(code, known, sleep=sleep)
            stats["rows"] += _upsert(con, "naver_research_reports", RESEARCH_COLUMNS, recs,
                                     pk_cols=("research_id",))
            known.update(r[0] for r in recs)
        stats["codes"] += 1
        stats["empty"] += not recs
        mark_checked(con, marker, [code], today)
        if i % 50 == 0 or i == len(todo):
            el = time.time() - t0
            eta = el / i * (len(todo) - i) / 60
            print(f"  [{i}/{len(todo)}] {stats} {el / 60:.0f}m ETA {eta:.0f}m {datetime.now():%H:%M}", flush=True)
    return stats


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("kind", choices=["trend", "research"])
    ap.add_argument("--db", default=str(default_db_path()))
    ap.add_argument("--codes", default="")
    ap.add_argument("--refetch", action="store_true", help="마커를 무시하고 다시 받는다")
    ap.add_argument("--sleep", type=float, default=0.2)
    a = ap.parse_args()
    con = connect(a.db)
    codes = [c for c in a.codes.split(",") if c] or universe(con)
    stats = run(con, a.kind, codes, refetch=a.refetch, sleep=a.sleep)
    print(f"✅ 완료: {stats}")
    if stats["codes"] and stats["rows"] == 0:
        raise SystemExit("0행 — 빈 응답을 성공으로 넘기지 않는다(엔드포인트가 바뀌었는지 확인)")


if __name__ == "__main__":
    main()
