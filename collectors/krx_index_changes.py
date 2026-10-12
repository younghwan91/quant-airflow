"""KRX 지수운영공지 → index_notices·index_changes·index_constituents (migrations/021).

index.krx.co.kr 의 "지수운영공지" 게시판(MKD01040000)은 data.krx.co.kr 와 달리 로그인 없이
목록(``/board/<id>/list``)·본문(``/view``)·첨부목록(``/list?cmd=filelist``)을 POST 로 준다. 첨부는
``/contents/COM/GenerateOTP.jspx`` 로 1회용 코드를 받아 ``file.krx.co.kr/download.jspx`` 에 낸다.

1. 공지 전부를 ``index_notices`` 에 원문으로 둔다(이미 있는 글은 건너뛴다).
2. 대표지수 **정기변경 본 공지**(:func:`is_periodic_main`)의 엑셀을 읽어 지수별 편입·편출과 변경 후
   전체 명단을 넣는다. 엑셀은 표준 라이브러리로 읽는다(zip + XML) — 런타임에 openpyxl 이 없다.
   시행일은 본문의 "'20.6.12(금)에 정기변경" / "반영일 : '17.12.15(금)" 에서 뽑는다 — 못 뽑으면 예외.

수시변경(특례편입·인적분할·합병·신규상장)은 공지마다 형식이 달라 구조화하지 않는다 —
``index_notices`` 의 제목·본문으로 찾는다.

CLI:
    python -m collectors.krx_index_changes --db <DSN>
"""

from __future__ import annotations

import argparse
import datetime as dt
import html
import io
import re
import time
import zipfile
from collections.abc import Callable
from typing import Any
from xml.etree import ElementTree as ET

from .storage import connect, execute, fetchall, insert_index_changes, insert_index_constituents

BBS = "MKD01040000"
BASE = "https://index.krx.co.kr"
REFERER = f"{BASE}/board/{BBS}/bbs"
FILE_SERVER = "https://file.krx.co.kr/download.jspx"
UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128 Safari/537.36"
MIN_INTERVAL = 0.4

#: 시트·제목의 지수 표기 → 저장 이름
INDEX_NAMES = (
    (re.compile(r"(?:KOSPI|코스피)\s*200"), "KOSPI200"),
    (re.compile(r"(?:KOSDAQ|코스닥)\s*150"), "KOSDAQ150"),
    (re.compile(r"KRX\s*300"), "KRX300"),
    (re.compile(r"(?:KOSPI|코스피)\s*100"), "KOSPI100"),
    (re.compile(r"(?:KOSPI|코스피)\s*50"), "KOSPI50"),
)
#: 대표지수 정기변경 공지에 같이 걸리는 파생·섹터·전략지수 공지 — 본 공지가 아니다
_NOT_MAIN = re.compile(r"섹터|CAP|전략|팩터|예측배당|고배당|가치저변동성|중소형주|초대형|Mid|MID|밸류업|ESG|거버넌스"
                       r"|레버리지|인버스|동일가중|테마|선물|KTOP|배당|저변동|모멘텀|퀄리티|밸류가중|GIVI|스마트베타"
                       r"|IHS|내재가치|한중|아시아|리츠")
_ISIN = re.compile(r"^[A-Z]{2}[0-9A-Z]{9}[0-9]$")
_SHORT = re.compile(r"^[0-9][0-9A-Z]{5}$")      # 2021-06 엑셀만 ISIN 대신 단축코드를 썼다
#: 정기변경 후 명단 크기 — 다르면 행을 조용히 흘린 것이다(숫자 셀로 앞 0 이 날아간 코드 등)
INDEX_SIZE = {"KOSPI200": 200, "KOSDAQ150": 150, "KRX300": 300, "KOSPI100": 100, "KOSPI50": 50}
_EFFECTIVE = re.compile(r"(\d{2})\s*\.\s*(\d{1,2})\s*\.\s*(\d{1,2})\s*\.?\s*\((?:월|화|수|목|금)\)")


def is_periodic_main(title: str) -> bool:
    """KOSPI 200·KOSDAQ 150 정기변경 **본 공지**인가."""
    return ("정기변경" in title and bool(re.search(r"(?:KOSPI|코스피|KOSDAQ|코스닥)\s*(?:200|150)", title))
            and not _NOT_MAIN.search(title))


def index_name(text: str) -> str | None:
    for pat, name in INDEX_NAMES:
        if pat.search(text):
            return name
    return None


def effective_date(body: str) -> dt.date:
    """본문의 첫 "'YY.M.D(요일)" — 정기변경일. 없으면 예외(틀린 날짜보다 없는 게 낫다)."""
    m = _EFFECTIVE.search(html.unescape(html.unescape(body)))
    if not m:
        raise ValueError("본문에서 정기변경일을 못 찾았다")
    yy, mm, dd = (int(x) for x in m.groups())
    return dt.date(2000 + yy, mm, dd)


# --- 엑셀(표준 라이브러리) --------------------------------------------------------

_NS = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
       "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships"}


def _col(ref: str) -> int:
    n = 0
    for ch in re.match(r"[A-Z]+", ref).group():
        n = n * 26 + ord(ch) - 64
    return n - 1


def read_xlsx(data: bytes) -> list[tuple[str, list[list[str]]]]:
    """xlsx 바이트 → [(시트명, 행렬)]. 값은 전부 문자열(빈 칸은 '')."""
    z = zipfile.ZipFile(io.BytesIO(data))
    shared: list[str] = []
    if "xl/sharedStrings.xml" in z.namelist():
        for si in ET.fromstring(z.read("xl/sharedStrings.xml")).findall("m:si", _NS):
            shared.append("".join(t.text or "" for t in si.iter(f"{{{_NS['m']}}}t")))
    rels = {r.get("Id"): r.get("Target") for r in ET.fromstring(z.read("xl/_rels/workbook.xml.rels"))}
    sheets = []
    for sh in ET.fromstring(z.read("xl/workbook.xml")).find("m:sheets", _NS):
        target = rels[sh.get(f"{{{_NS['r']}}}id")].lstrip("/")
        target = target if target.startswith("xl/") else "xl/" + target
        grid = []
        for row in ET.fromstring(z.read(target)).iter(f"{{{_NS['m']}}}row"):
            cells: dict[int, str] = {}
            for c in row.findall("m:c", _NS):
                v, t = c.find("m:v", _NS), c.get("t")
                if t == "inlineStr":
                    val = "".join(x.text or "" for x in c.iter(f"{{{_NS['m']}}}t"))
                elif v is None:
                    continue
                elif t == "s":
                    val = shared[int(v.text)]
                else:
                    val = v.text or ""
                cells[_col(c.get("r"))] = val.strip()
            if cells:
                grid.append([cells.get(i, "") for i in range(max(cells) + 1)])
        sheets.append((sh.get("name") or "", grid))
    return sheets


def _member(row: list[str], i: int) -> tuple[str, str | None, str] | None:
    """(code, isin|None, name) — i 칸이 ISIN 이나 단축코드면."""
    if i >= len(row):
        return None
    cell, name = row[i], row[i + 1] if i + 1 < len(row) else ""
    if _ISIN.match(cell):
        return cell[3:9], cell, name
    if _SHORT.match(cell):
        return cell, None, name
    return None


def parse_changes(grid: list[list[str]]) -> list[tuple[str, str, str | None, str]]:
    """교체 시트 → [(action, code, isin, name)]. 머리행의 편입/신규/선정 · 제외/편출 열이 블록 시작이다."""
    def is_add(c: str) -> bool:
        return bool(re.search(r"편입|신규|선정", c)) and not re.search(r"제외|편출", c)

    def is_drop(c: str) -> bool:
        return bool(re.search(r"제외|편출", c)) and not re.search(r"편입|신규|선정", c)

    # 머리행 = 편입 칸과 제외 칸이 **따로** 있는 첫 행 — 제목 "편입/제외 종목" 은 한 칸에 둘 다라 아니다.
    # 칸 이름은 해마다 다르다: 편입·신규·선정·신규편입종목 / 제외·편출·제외종목(2015-12 는 앞에 섹터 열까지).
    head = next((i for i, r in enumerate(grid) if any(map(is_add, r)) and any(map(is_drop, r))), None)
    if head is None:
        if not parse_members(grid):
            return []          # 교체 0종목(2018-06 KOSPI 50) — 시트는 있고 제목뿐이다
        raise ValueError("편입·제외 머리행이 없는데 종목이 있다")
    add_col = next(j for j, c in enumerate(grid[head]) if is_add(c))
    drop_col = next(j for j, c in enumerate(grid[head]) if is_drop(c))
    out = []
    for r in grid[head + 1:]:
        for action, j in (("add", add_col), ("drop", drop_col)):
            m = _member(r, j)
            if m:
                out.append((action, *m))
    return out


def parse_members(grid: list[list[str]]) -> list[tuple[str, str | None, str]]:
    """구성종목 시트 → [(code, isin, name)] — 어느 열이든 코드 칸과 그 오른쪽 이름."""
    out = []
    for r in grid:
        for j in range(len(r)):
            m = _member(r, j)
            if m:
                out.append(m)
    return out


def parse_workbook(sheets: list[tuple[str, list[list[str]]]]) -> tuple[dict[str, list], dict[str, list]]:
    """→ ({지수: 변경}, {지수: 명단}). 시트명이 지수와 '구성종목' 여부를 말한다."""
    changes: dict[str, list] = {}
    members: dict[str, list] = {}
    for name, grid in sheets:
        idx = index_name(name)
        if idx is None:
            continue
        if "구성종목" in name and not re.search(r"교체|변경|편출입", name):
            members[idx] = parse_members(grid)
            if len(members[idx]) != INDEX_SIZE[idx]:
                raise ValueError(f"{name}: 명단 {len(members[idx])}종목 — {INDEX_SIZE[idx]} 이어야 한다")
        else:
            changes[idx] = parse_changes(grid)
    return changes, members


# --- 게시판 ---------------------------------------------------------------------

class Board:
    """지수운영공지 게시판 — 요청 간격을 지키는 단일 스레드 클라이언트."""

    def __init__(self, request: Callable[..., Any] | None = None, sleep: Callable[[float], None] = time.sleep,
                 clock: Callable[[], float] = time.monotonic) -> None:
        if request is None:
            import httpx  # noqa: PLC0415 — 테스트는 request 를 주입한다

            client = httpx.Client(headers={"User-Agent": UA, "Referer": REFERER}, timeout=30)
            request = client.request
        self._req, self._sleep, self._clock, self._last = request, sleep, clock, -1e9

    def _call(self, method: str, url: str, **kw: Any) -> Any:
        wait = self._last + MIN_INTERVAL - self._clock()
        if wait > 0:
            self._sleep(wait)
        self._last = self._clock()
        r = self._req(method, url, **kw)
        r.raise_for_status()
        return r

    def posts(self, start: str = "20150101", end: str | None = None) -> list[tuple[int, str, dt.date]]:
        """(글번호, 제목, 게시일) 전부 — 페이지를 새 글이 안 나올 때까지 넘긴다."""
        end = end or dt.date.today().strftime("%Y%m%d")
        found: dict[int, tuple[int, str, dt.date]] = {}
        for page in range(1, 1000):
            text = self._call("POST", f"{BASE}/board/{BBS}/list", data={
                "bbsId": BBS, "bbsUrl": BBS, "curPage": page, "boardStyle": "blog", "listOrder": "new",
                "srchTitle": "", "srchWord": "", "startDate": start, "endDate": end}).text
            items = re.findall(r"data-view='(\d+)'[^>]*>(?:<span[^>]*></span>)?([^<]*)</a></div>"
                               r"<div class='blog-write-date'>작성일 : ([\d-]+)", text)
            new = [(int(s), html.unescape(t).strip(), dt.date.fromisoformat(d)) for s, t, d in items if int(s) not in found]
            if not new:
                break
            for p in new:
                found[p[0]] = p
        return sorted(found.values())

    def body(self, seq: int) -> str:
        text = self._call("POST", f"{BASE}/board/{BBS}/view", data={"bbsId": BBS, "bbsSeq": seq, "bbsUrl": BBS}).text
        text = html.unescape(re.sub(r"<br\s*/?>", "\n", text))
        text = re.sub(r"<[^>]+>", " ", text)
        start = text.find("작성자")
        end = text.find("인기게시글")
        text = text[start if start >= 0 else 0: end if end > 0 else None]
        return re.sub(r"[ \t]+", " ", re.sub(r"\n\s*\n+", "\n", text)).strip()

    def files(self, seq: int) -> list[tuple[str, str]]:
        text = self._call("POST", f"{BASE}/board/{BBS}/list?cmd=filelist", data={"bbsId": BBS, "bbsSeq": seq}).text
        return [(f, html.unescape(n)) for f, n in re.findall(r"data-file-seq='(\d+)'>([^<]+)</a>", text)]

    def download(self, seq: int, file_seq: str) -> bytes:
        code = self._call("GET", f"{BASE}/contents/COM/GenerateOTP.jspx", params={
            "name": "fileDown", "url": "COM/board_attach_down", "filetype": "att",
            "bbsId": BBS, "bbsSeq": seq, "attachFileSeq": file_seq}).text
        return self._call("POST", FILE_SERVER, data={"code": code}).content


def collect(con: Any, board: Board, *, now: Callable[[], dt.datetime] | None = None) -> dict[str, int]:
    now = now or (lambda: dt.datetime.now(dt.UTC))
    have = {int(r[0]) for r in fetchall(con, "SELECT bbs_seq FROM index_notices", ())}
    loaded = {int(r[0]) for r in fetchall(con, "SELECT DISTINCT bbs_seq FROM index_constituents", ())}
    stats = {"posts": 0, "new_notices": 0, "periodic": 0, "changes": 0, "members": 0}
    for seq, title, posted in board.posts():
        stats["posts"] += 1
        periodic = is_periodic_main(title)
        if seq in have and not (periodic and seq not in loaded):
            continue
        body = board.body(seq)
        files = board.files(seq)
        if seq not in have:
            execute(con, "INSERT INTO index_notices(bbs_seq, posted_date, title, body, attachments, fetched_at) "
                         "VALUES (?, ?, ?, ?, ?, ?)",
                    (seq, posted.isoformat(), title, body, "\n".join(n for _, n in files), now().isoformat()))
            stats["new_notices"] += 1
        if periodic and seq not in loaded:
            xlsx = [f for f in files if f[1].lower().endswith(".xlsx")]
            if not xlsx:
                raise RuntimeError(f"정기변경 공지 {seq} 에 xlsx 첨부가 없다: {files}")
            eff = effective_date(body)
            changes, members = parse_workbook(read_xlsx(board.download(seq, xlsx[0][0])))
            if not members:
                raise RuntimeError(f"정기변경 공지 {seq}: 구성종목 시트를 못 읽었다")
            # 명단을 맨 나중에 — 재실행은 "명단이 있나" 로 이 공지를 끝냈는지 본다(변경은 DO NOTHING 이라 다시 넣어도 된다).
            change_rows = [(idx, eff.isoformat(), action, code, isin, name, posted.isoformat(), seq)
                           for idx, rows in changes.items() for action, code, isin, name in rows]
            member_rows = [(idx, eff.isoformat(), code, isin, name, posted.isoformat(), seq)
                           for idx, rows in members.items() for code, isin, name in rows]
            insert_index_changes(con, change_rows)
            insert_index_constituents(con, member_rows)
            stats["changes"] += len(change_rows)
            stats["members"] += len(member_rows)
            stats["periodic"] += 1
            print(f"{posted} #{seq} → 시행 {eff}: " + ", ".join(
                f"{k} +{sum(a == 'add' for a, *_ in v)}/-{sum(a == 'drop' for a, *_ in v)}" for k, v in changes.items()),
                flush=True)
    return stats


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--db", required=True)
    args = ap.parse_args()
    con = connect(args.db)
    stats = collect(con, Board())
    print(" | ".join(f"{k} {v}" for k, v in stats.items()), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
