"""2006~ 폐지 종목 일봉(네이버) + KIND 상장폐지 목록 — 2006~2016 이력 확장의 생존편향 쪽 (migration 028).

**왜 별도 표인가.** ``naver_delisted_bars`` 는 2016-09-09 부터를 ``daily_bars``(source='naver')에 넣는다. 그
이전을 같은 표에 넣으면 ``daily_bars`` 의 2016 이전 구간은 **폐지 종목만** 있는 유니버스가 된다(상장 종목의
2006~2016 원주가는 ``daily_bars_raw`` 로 따로 간다). 그 구간을 읽는 쪽이 생존편향을 거꾸로 뒤집어쓴다. 그래서
``daily_bars_delisted_naver`` 에 2006 부터 폐지일까지 통째로 둔다(2016-09 이후는 ``daily_bars`` 네이버 행과 겹친다).

**수정가격이다.** 네이버 siseJson 은 키움 수정주가와 같은 값을 준다(``naver_delisted_bars`` 머리말 — 2018-05
50:1 분할 구간까지 일치). 폐지 종목은 키움이 일봉·수정 이벤트를 안 주므로(빈 행 하나, 2026-08-13 실측) 원 가격으로
되돌릴 방법이 없다 — ``source='naver_adj'``. 수익률에는 맞고, "그날 원 가격 × 그날 주식수" 시총에는 틀릴 수 있다.

**대상 = KIND 목록 ∪ KRX 폐지 목록(delisted_stocks) − 지금 상장 코드.** KIND 상장폐지 현황(무인증)은 폐지일·사유를
주지만 2006~2008 이 해마다 16~35건뿐이라(1999~2005 는 60~127건) 빠진 게 있어 보인다. KRX finder 목록
(delisted_stocks)은 날짜가 없다. 둘을 합쳐 받고, 실제 마지막 거래일은 네이버 응답으로 본다. 지금 상장 코드는
뺀다 — KIND 2006~2016 588건 중 35건이 지금 상장이다(시장 이전·재상장·코드 재사용). 그 종목들의 이력은 키움이 준다.

CLI:
    python -m collectors.delisted_history --db <DSN>              # KIND 목록 + 미조회 코드 전부
    python -m collectors.delisted_history --db <DSN> --limit 20   # 표본
"""

from __future__ import annotations

import argparse
import html
import re
import time
import urllib.parse
import urllib.request
from typing import Any

from .naver_delisted_bars import fetch_sise, parse_sise
from .storage import (
    CHECKED_DELISTED_HIST,
    DAILY_BAR_COLUMNS,
    _upsert,
    connect,
    default_db_path,
    fetchall,
    mark_checked,
)

START = "20060101"
SOURCE = "naver_adj"

KIND_URL = "https://kind.krx.co.kr/investwarn/delcompany.do"
KIND_REFERER = "https://kind.krx.co.kr/investwarn/delcompany.do?method=searchDelCompanyMain"
UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128 Safari/537.36"

_TR = re.compile(r"<tr>(.*?)</tr>", re.S)
_TD = re.compile(r"<td[^>]*>(.*?)</td>", re.S)
_TAG = re.compile(r"<[^>]+>")
_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def fetch_kind_html(from_date: str = "1990-01-01", to_date: str | None = None) -> str:
    """KIND 상장폐지 현황 — 한 요청에 전 기간(약 2,000건). 응답은 cp949 HTML 표."""
    to_date = to_date or time.strftime("%Y-%m-%d")
    data = urllib.parse.urlencode({
        "method": "searchDelCompanySub", "currentPageSize": 5000, "pageIndex": 1,
        "orderMode": 1, "orderStat": "D", "searchMode": "", "searchCodeType": "",
        "searchCorpName": "", "marketType": "", "repIsuSrtCd": "", "forward": "delcompany_down",
        "fromDate": from_date, "toDate": to_date,
    }).encode()
    req = urllib.request.Request(KIND_URL, data=data, headers={"User-Agent": UA, "Referer": KIND_REFERER})
    with urllib.request.urlopen(req, timeout=60) as r:  # noqa: S310 — 고정 호스트
        return r.read().decode("cp949", "ignore")


def parse_kind(body: str) -> list[tuple[str, str, str, str]]:
    """KIND 표 → ``[(code, delist_date, name, reason)]``. 칸: 번호·회사명·종목코드·폐지일자·폐지사유·비고."""
    out = []
    for row in _TR.findall(body):
        cells = [html.unescape(_TAG.sub("", c)).strip() for c in _TD.findall(row)]
        if len(cells) < 5 or not re.fullmatch(r"\d{6}", cells[2]) or not _DATE.match(cells[3]):
            continue
        out.append((cells[2], cells[3], cells[1], cells[4]))
    return out


def target_codes(con: Any, kind_codes: set[str], *, refetch: bool = False) -> list[str]:
    """KIND ∪ delisted_stocks(6자리, 코넥스 제외) − stocks(지금 상장) − 이미 받은 코드."""
    dl = {r[0] for r in fetchall(con, "SELECT code, market FROM delisted_stocks") if
          re.fullmatch(r"\d{6}", r[0] or "") and r[1] != "코넥스"}
    listed = {r[0] for r in fetchall(con, "SELECT code FROM stocks")}
    done = set() if refetch else {
        r[0] for r in fetchall(con, "SELECT code FROM backfill_markers WHERE source = ?", (CHECKED_DELISTED_HIST,))}
    return sorted((dl | kind_codes) - listed - done)


def to_records(body: str, code: str) -> list[tuple]:
    """네이버 행(source='naver') → 이 표의 source 로 바꾼다. 정지일 정규화 등은 ``parse_sise`` 그대로."""
    return [(*r[:-1], SOURCE) for r in parse_sise(body, code)]


def collect(con: Any, codes: list[str], *, fetch=fetch_sise, sleep: float = 0.25,
            progress_every: int = 100) -> dict[str, int]:
    stats = {"codes": len(codes), "with_data": 0, "empty": 0, "failed": 0, "rows": 0}
    done: list[str] = []
    today = time.strftime("%Y-%m-%d")
    t0 = time.time()
    for i, code in enumerate(codes, 1):
        body = fetch(code, start=START)
        if not body:
            stats["failed"] += 1                     # 네트워크 실패 — 표시하지 않아 다음에 다시 받는다
        else:
            rows = to_records(body, code)
            if rows:
                stats["with_data"] += 1
                stats["rows"] += _upsert(con, "daily_bars_delisted_naver", [*DAILY_BAR_COLUMNS, "source"], rows)
            else:
                stats["empty"] += 1                  # 2006 이전 폐지 등 — 구간 안 자료가 없다
            done.append(code)
        if i % progress_every == 0 or i == len(codes):
            el = time.time() - t0
            print(f"  [{i}/{len(codes)}] 자료={stats['with_data']} 없음={stats['empty']} 실패={stats['failed']} "
                  f"행={stats['rows']:,} | {i / el if el else 0:.1f}종목/s", flush=True)
            mark_checked(con, CHECKED_DELISTED_HIST, done, today)
            done = []
        time.sleep(sleep)
    return stats


def main() -> int:
    ap = argparse.ArgumentParser(description="2006~ 폐지 종목 일봉(네이버) + KIND 상장폐지 목록 (028)")
    ap.add_argument("--db", default=None)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--sleep", type=float, default=0.25)
    ap.add_argument("--refetch", action="store_true", help="이미 받은 코드도 다시")
    args = ap.parse_args()

    from .config import mask_dsn

    con = connect(args.db or default_db_path())
    kind = parse_kind(fetch_kind_html())
    if len(kind) < 1000:   # 2026-10 기준 약 2,000건 — 빈/잘린 응답을 성공으로 넘기지 않는다(CLAUDE.md §5)
        raise RuntimeError(f"KIND 상장폐지 목록이 {len(kind)}건뿐 — 응답 형식이 바뀌었나")
    n_kind = _upsert(con, "kind_delistings", ["code", "delist_date", "name", "reason"], kind,
                     pk_cols=("code", "delist_date"))
    codes = target_codes(con, {k[0] for k in kind}, refetch=args.refetch)
    if args.limit:
        codes = codes[: args.limit]
    print(f"🔌 {mask_dsn(args.db)} | KIND {n_kind}건 | 대상 {len(codes)}종목 | start={START}", flush=True)
    stats = collect(con, codes, sleep=args.sleep)
    con.close()
    print(f"DONE {stats}", flush=True)
    if codes and stats["with_data"] == 0:
        raise RuntimeError("대상은 있는데 자료가 한 종목도 없다 — 네이버 응답을 확인")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
