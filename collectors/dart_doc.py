"""DART 정기보고서 **원문**(``document.xml``)에서 분기 순이익·발행주식총수를 뽑는 순수 파서.

**왜 원문인가.** ``earnings``(``dart_earnings.py``)의 원천인 ``fnlttMultiAcnt``/``fnlttSinglAcnt``
는 2015 사업연도부터만 답한다. 2007~2015 를 메우려면 사업·반기·분기보고서 원문 표를 읽어야
한다(2006~2016 이력 확장, 사용자 승인 2026-10-12 — 완료 조건 C: 2016 겹침 구간 95% 일치).

**무엇을 같은 의미로 맞추나** — ``earnings`` 표와 같은 기간 의미:

- ``period`` = ``'2009Q2'`` 꼴 달력 분기(12월 결산사만. 3·6월 결산사는 ``period_from_report_nm``
  이 ``None`` 으로 거른다 — ``dart_earnings._REPORT_KIND_QUARTERS`` 와 같은 규칙).
- Q1~Q3 = **3개월** 값, Q4 = **사업연도 전체**(사업보고서 당기). ``earnings`` 2016Q4 삼성전자
  22.7조 = 연간 순이익으로 실측. 표에 3개월 열이 없으면(요약재무정보만 있는 2010~2013 보고서)
  누적 − 직전 분기 누적으로 만들고 ``derived`` 플래그를 단다(:func:`assemble_quarters`).
- 단위 = **원**. 표마다 ``(단위 : 백만원)`` 이 다르다 — 단위를 못 찾으면 값을 버린다(틀린 값보다 없는 값).
- ``knowledge_date`` = 최초(정정 아닌) 접수번호 앞 8자리 = 접수일. 정정은 별도 행으로 쌓을 수 있게
  호출부가 ``rcept_no`` 를 그대로 넘긴다.

**순이익 세 가지 기준**(``NetIncome``): 연결 지배기업 소유주 귀속(``ctrl``), 연결 당기순이익
전체(``cfs``, 비지배 포함), 별도/개별(``ofs``). ``earnings.netinc`` 는 ``cfs`` 가 있으면 그것,
없으면 ``ofs`` 다(``krx_fundamentals_client._parse_financial_rows`` — 연결 우선, 계정명
'당기순이익' = 비지배 포함 합계). 그래서 :func:`pick_netinc` 는 두 정의를 따로 낸다:
``earnings`` 와 같은 ``cfs→ofs`` 와, 지배지분 우선 ``ctrl→cfs→ofs``. 어느 쪽을 썼는지 ``basis``.

**K-GAAP 시절(~2010) 함정**: 분기보고서의 ``5. 연결재무제표`` 는 **직전 사업연도** 연결표다
(분기 연결 의무가 없었다). 표 머리의 기수·기간이 보고서 기간과 맞는지(:func:`_current_columns`)
보지 않으면 2007 연간 연결 순이익이 2008Q1 값으로 들어간다.

**발행주식총수**(``Shares``): ``4. 주식의 총수 등`` 표의 ``Ⅳ. 발행주식의 총수`` 행(ACODE
``ISU_STK3``) — 보통주·우선주·합계. ``shares_outstanding_history`` 의 ``istc_totqy``(발행주식총수,
유통주식수 아님 — ``dart_shares.py`` 문서)와 같은 의미다.

원문 인코딩: XML 선언은 utf-8 이지만 옛 보고서는 cp949 다(2026-10-13 실측) — utf-8 먼저, 실패하면 cp949.

네트워크·DB 를 쓰는 부분은 맨 아래 ``main``(검증 CLI)뿐이다. 그 위는 전부 순수 함수.
"""

from __future__ import annotations

import io
import re
import zipfile
from dataclasses import dataclass, field
from datetime import date
from html.parser import HTMLParser

# --------------------------------------------------------------------------- 문서 → 블록


@dataclass
class Cell:
    text: str
    tag: str = "td"
    colspan: int = 1
    rowspan: int = 1
    acode: str = ""


@dataclass
class Table:
    rows: list[list[Cell]]


@dataclass
class Block:
    """문서 순서대로의 조각 — ``kind`` 는 ``title``·``text``·``table``."""

    kind: str
    text: str = ""
    table: Table | None = None


def decode_document(raw: bytes) -> str:
    """utf-8 선언이지만 옛 보고서는 cp949 — utf-8 먼저, 실패하면 cp949."""
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.decode("cp949", errors="replace")


def _ws(s: str) -> str:
    return re.sub(r"\s+", " ", s.replace("\xa0", " ")).strip()


class _Parser(HTMLParser):
    _CELL = {"td", "th", "te", "tu"}

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.blocks: list[Block] = []
        self._text: list[str] = []
        self._title: list[str] | None = None
        self._tables: list[Table] = []
        self._cells: list[tuple[Cell, list[str]] | None] = []  # 테이블 스택마다 열린 셀

    def _flush_text(self) -> None:
        t = _ws("".join(self._text))
        if t:
            self.blocks.append(Block("text", t))
        self._text = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        a = {k.lower(): (v or "") for k, v in attrs}
        if tag == "table":
            if not self._tables:
                self._flush_text()
            self._tables.append(Table(rows=[]))
            self._cells.append(None)
        elif tag == "tr" and self._tables:
            self._close_cell()
            self._tables[-1].rows.append([])
        elif tag in self._CELL and self._tables:
            self._close_cell()
            if not self._tables[-1].rows:
                self._tables[-1].rows.append([])

            def _int(v: str) -> int:
                return int(v) if v.isdigit() and int(v) > 0 else 1

            cell = Cell("", tag, _int(a.get("colspan", "1")), _int(a.get("rowspan", "1")), a.get("acode", ""))
            self._cells[-1] = (cell, [])
        elif tag == "title" and not self._tables:
            self._flush_text()
            self._title = []
        elif tag in ("p", "br"):
            if self._tables and self._cells[-1]:
                self._cells[-1][1].append(" ")
            elif not self._tables and self._title is None:
                self._flush_text()

    def _close_cell(self) -> None:
        if self._tables and self._cells[-1]:
            cell, buf = self._cells[-1]
            cell.text = _ws("".join(buf))
            self._tables[-1].rows[-1].append(cell)
            self._cells[-1] = None

    def handle_endtag(self, tag: str) -> None:
        if tag in self._CELL:
            self._close_cell()
        elif tag == "table" and self._tables:
            self._close_cell()
            t = self._tables.pop()
            self._cells.pop()
            self.blocks.append(Block("table", table=Table([r for r in t.rows if r])))
        elif tag == "title" and self._title is not None:
            t = _ws("".join(self._title))
            if t:
                self.blocks.append(Block("title", t))
            self._title = None

    def handle_data(self, data: str) -> None:
        if self._tables:
            if self._cells[-1]:
                self._cells[-1][1].append(data)
        elif self._title is not None:
            self._title.append(data)
        else:
            self._text.append(data)

    def close(self) -> None:
        super().close()
        while self._tables:
            self.handle_endtag("table")
        self._flush_text()


def parse_blocks(xml: str) -> list[Block]:
    """DART dsd XML(태그 SECTION·TITLE·TABLE·TR·TD/TE/TH/TU·P) → 문서 순서 블록."""
    p = _Parser()
    p.feed(xml.replace("&cr;", " "))
    p.close()
    return p.blocks


def grid(table: Table) -> list[list[Cell | None]]:
    """colspan/rowspan 을 펼친 격자. 펼쳐진 자리는 같은 ``Cell`` 을 가리킨다."""
    out: list[list[Cell | None]] = []
    pending: dict[tuple[int, int], Cell] = {}
    for r, row in enumerate(table.rows):
        line: list[Cell | None] = []
        c = 0
        it = iter(row)
        nxt = next(it, None)
        while nxt is not None or (r, c) in pending:
            if (r, c) in pending:
                line.append(pending.pop((r, c)))
                c += 1
                continue
            cell = nxt
            nxt = next(it, None)
            for k in range(cell.colspan):
                line.append(cell)
                for dr in range(1, cell.rowspan):
                    pending[(r + dr, c + k)] = cell
            c += cell.colspan
        out.append(line)
    return out


# --------------------------------------------------------------------------- 숫자·단위·날짜

_NUM_RE = re.compile(r"^(?P<neg>[\(△▲Δ∆\-−–]|\(\s*-)?\s*(?P<n>\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?\s*\)?\s*(?:원|주)?$")


def parse_number(text: str) -> float | None:
    """``'1,234'``·``'(1,234)'``·``'△1,234'``·``'-1,234'`` → 수. ``'-'``·빈칸·날짜·문장 → ``None``."""
    s = _ws(text).replace(" ", "")
    if not s or s in ("-", "–", "−", "0"):
        return 0.0 if s == "0" else None
    m = _NUM_RE.match(s)
    if not m:
        return None
    v = float(m.group("n").replace(",", ""))
    neg = bool(m.group("neg")) or (s.startswith("(") and s.endswith(")"))
    return -v if neg else v


_UNIT_RE = re.compile(r"단\s*위\s*[:：]?\s*[\(\[]?\s*(백\s*만\s*원|천\s*원|억\s*원|십\s*억\s*원|원|천\s*달\s*러|달\s*러|USD|US\$|천\s*US|미\s*화|백\s*만\s*US|백\s*만\s*달\s*러|엔|위\s*안)")
_UNIT_MULT = {"원": 1, "천원": 1_000, "백만원": 1_000_000, "억원": 100_000_000, "십억원": 1_000_000_000}


def unit_multiplier(text: str) -> int | None:
    """``'(단위 : 백만원)'`` → 1e6. 원화가 아닌 단위(달러 등)·단위 없음 → ``None``(값을 버린다)."""
    m = _UNIT_RE.search(text)
    if not m:
        return None
    return _UNIT_MULT.get(m.group(1).replace(" ", ""))


_DATE = r"(\d{4})\s*[.년\-/]\s*(\d{1,2})\s*[.월\-/]\s*(\d{1,2})\s*일?"
_RANGE_RE = re.compile(_DATE + r"\s*(?:부터|~|-|∼|～)\s*" + _DATE)
_ASOF_RE = re.compile(_DATE + r"\s*\.?\s*현재")
_KISU_RE = re.compile(r"제\s*(\d{1,3})\s*(?:\(\s*당\s*\)\s*)?기")
_YM_RE = re.compile(r"(\d{4})\s*[.년]\s*(\d{1,2})\s*월?\s*(?:말|\.\s*\d{1,2}|$|\s)")


def _mkdate(y: str, m: str, d: str) -> date | None:
    try:
        return date(int(y), int(m), int(d))
    except ValueError:
        return None


def date_ranges(text: str) -> list[tuple[date, date]]:
    out = []
    for m in _RANGE_RE.finditer(text):
        a, b = _mkdate(*m.group(1, 2, 3)), _mkdate(*m.group(4, 5, 6))
        if a and b:
            out.append((a, b))
    return out


# --------------------------------------------------------------------------- 보고서 메타

_REPORT_NM_RE = re.compile(r"^(?:\[[^\]]*\]\s*)?(분기보고서|반기보고서|사업보고서)\s*\((\d{4})\.(\d{2})\)")
_KIND_Q = {"분기보고서": {"03": 1, "09": 3}, "반기보고서": {"06": 2}, "사업보고서": {"12": 4}}
QUARTER_END = {1: (3, 31), 2: (6, 30), 3: (9, 30), 4: (12, 31)}


def period_from_report_nm(report_nm: str) -> tuple[int, int] | None:
    """``'반기보고서 (2009.06)'`` → ``(2009, 2)``. 12월 결산이 아니면(기준월이 안 맞으면) ``None``."""
    m = _REPORT_NM_RE.match(report_nm.strip())
    if not m:
        return None
    q = _KIND_Q[m.group(1)].get(m.group(3))
    return (int(m.group(2)), q) if q else None


def is_correction(report_nm: str) -> bool:
    return report_nm.strip().startswith("[")


def select_filings(items: list[dict]) -> dict[str, dict]:
    """``list.json`` 항목 → ``{period: {'rcept_no','rcept_dt','report_nm','corrections':[...]}}``.

    최초 제출(``[기재정정]``·``[첨부추가]`` 등 대괄호 접두가 없는 것 중 가장 이른 접수번호)이
    ``knowledge_date`` 의 기준이다. 정정은 목록으로만 남긴다 — 값이 바뀌었으면 호출부가 정정
    접수일로 새 버전을 쌓는다(``earnings`` PK 의 ``knowledge_date`` 규약, migration 001).
    원본이 목록에 없고 정정만 있으면 가장 이른 정정을 쓰고 ``original_missing`` 을 단다.
    """
    by: dict[str, list[dict]] = {}
    for it in items:
        per = period_from_report_nm(it.get("report_nm", ""))
        if per:
            by.setdefault(f"{per[0]}Q{per[1]}", []).append(it)
    out = {}
    for period, lst in by.items():
        lst = sorted(lst, key=lambda x: x["rcept_no"])
        orig = [x for x in lst if not is_correction(x["report_nm"])]
        first = orig[0] if orig else lst[0]
        out[period] = {
            "rcept_no": first["rcept_no"], "rcept_dt": first["rcept_no"][:8],
            "report_nm": first["report_nm"], "original_missing": not orig,
            "corrections": [x["rcept_no"] for x in lst if x is not first],
        }
    return out


def period_end(year: int, q: int) -> date:
    return date(year, *QUARTER_END[q])


def cover_kisu(blocks: list[Block]) -> int | None:
    """표지의 ``(제 40 기 1 분기)`` → 40. 표지는 문서 앞 몇 블록이다."""
    for b in blocks[:12]:
        txt = b.text if b.kind != "table" else " ".join(c.text for r in b.table.rows for c in r)
        m = _KISU_RE.search(txt)
        if m:
            return int(m.group(1))
    return None


# --------------------------------------------------------------------------- 순이익 표


_ENUM_RE = re.compile(r"^(?:[\(\[]?(?:[ⅠⅡⅢⅣⅤⅥⅦⅧⅨⅩⅪⅫ]+|[IVXL]+|\d{1,2}|[가-하])[\.\)\]]|[ㆍ·\-\*※])+")


def norm_label(text: str) -> str:
    """계정명 정규화 — 번호(Ⅹ. / 1. / (1) / 가.)·괄호주(주석·(손실))·공백 제거."""
    s = text.replace(" ", "").replace("\xa0", "")
    s = re.sub(r"\[([^\]]*)\]", r"\1", s)
    for _ in range(3):
        s2 = _ENUM_RE.sub("", s)
        if s2 == s:
            break
        s = s2
    return s


_TOTAL_NI_RE = re.compile(r"^(?:연결)?(?:총)?(?:당기|당분기|당반기|분기|반기|기간|당)?순(?:이익|손실|손익)$")
_EXCLUDE = ("주당", "포괄", "계속", "중단", "차감전", "법인세", "귀속", "지배", "소수", "누계")


def _core(label: str) -> str:
    return re.sub(r"\([^)]*\)", "", label)


def is_total_ni(label: str) -> bool:
    n = norm_label(label)
    if any(x in n for x in _EXCLUDE):
        return False
    return bool(_TOTAL_NI_RE.match(_core(n)))


def _loss_only(label: str) -> bool:
    n = norm_label(label)
    return "손실" in n and "이익" not in n and "손익" not in n


def _ctrl_row(labels: list[str], i_ni: int) -> int | None:
    """총 순이익 행 다음에서 지배기업 소유주 귀속 순이익 행. 총포괄손익 귀속은 건너뛴다."""
    in_ni_attr = False
    in_ci_attr = False
    seen_ci = False
    for j in range(i_ni + 1, min(len(labels), i_ni + 45)):
        lab = norm_label(labels[j])
        if not lab:
            continue
        if "주당" in lab:
            break
        is_ctrl = "지배" in lab and "비지배" not in lab and "소수" not in lab
        if "귀속" in lab and not is_ctrl and "비지배" not in lab:
            in_ci_attr = "포괄" in lab
            in_ni_attr = not in_ci_attr
            continue
        if "포괄" in lab and not is_ctrl:
            seen_ci = True
            continue
        if is_ctrl:
            if "포괄" in lab or in_ci_attr:
                continue
            if in_ni_attr or re.search(r"순(이익|손익|손실)", lab) or (not seen_ci and j - i_ni <= 3):
                return j
    return None


@dataclass
class NICandidate:
    """한 표에서 읽은 순이익(원 단위). 현재 기간 열만."""

    fs: str                       # 'cfs' | 'ofs'
    kind: str                     # 'statement' | 'summary'
    total_3m: float | None = None
    total_cum: float | None = None
    ctrl_3m: float | None = None
    ctrl_cum: float | None = None
    unit: int = 1
    file: str = ""
    flags: list[str] = field(default_factory=list)


def _header_rows(g: list[list[Cell | None]]) -> int:
    """첫 숫자 행 앞까지가 머리 행."""
    for r, line in enumerate(g):
        if any(c is not None and parse_number(c.text) is not None and c.text.strip() not in ("-",) for c in line[1:]):
            return r
    return len(g)


def _context_text(blocks: list[Block], idx: int, max_blocks: int = 8) -> list[str]:
    """표 앞 블록들의 텍스트(가까운 것이 뒤). 다른 큰 표를 만나면 멈춘다."""
    out: list[str] = []
    for b in reversed(blocks[max(0, idx - max_blocks):idx]):
        if b.kind == "table":
            if len(b.table.rows) > 8:
                break
            out.append(" ".join(c.text for r in b.table.rows for c in r))
        else:
            out.append(b.text)
    return list(reversed(out))


#: 표 앞 문맥에서 가장 가까운 '표 이름' — 무슨 표인지(손익·현금흐름·요약)와 연결/별도를 정한다.
#: 자본변동표·재무상태표·주석 류가 가장 가까우면 그 표는 버린다(주석의 부문별 순이익 따위).
_HEADING_RE = re.compile(
    r"(?P<is>손\s*익\s*계\s*산\s*서)"
    r"|(?P<cf>현\s*금\s*흐\s*름\s*표)"
    r"|(?P<sm>요\s*약\s*[^\s]{0,4}\s*재\s*무\s*정\s*보)"
    r"|(?P<other>자\s*본\s*변\s*동\s*표|재\s*무\s*상\s*태\s*표|대\s*차\s*대\s*조\s*표|이익잉여금처분|결손금처리|주\s*석|부문|주당|배당|영업실적)"
)
_QUAL_RE = re.compile(r"연\s*결|별\s*도|개\s*별")


def _heading(context: str) -> tuple[str | None, str | None]:
    """``(kind, fs)`` — kind ∈ ``is``·``cf``·``summary``·``None``, fs ∈ ``cfs``·``ofs``·``None``(모름).

    '연결 및 개별재무제표는 ...' 같은 본문 언급이 아니라 표 바로 앞 제목을 보려고 가장 가까운
    (마지막) 일치를 쓴다. 연결/별도 수식어는 그 제목 앞 8자·뒤 8자('요약재무정보(연결)')에서 찾는다.
    """
    ms = list(_HEADING_RE.finditer(context))
    if not ms:
        return None, None
    m = ms[-1]
    kind = {"is": "is", "cf": "cf", "sm": "summary", "other": None}[m.lastgroup]
    if kind is None:
        return None, None
    around = context[max(0, m.start() - 8):m.start()] + m.group(0) + context[m.end():m.end() + 8]
    quals = [q.replace(" ", "") for q in _QUAL_RE.findall(around)]
    if "연결" in quals and not any(q in ("별도", "개별") for q in quals):
        return kind, "cfs"
    if any(q in ("별도", "개별") for q in quals) and "연결" not in quals:
        return kind, "ofs"
    return kind, None


def _current_columns(
    g: list[list[Cell | None]], hdr: int, context: str, pend: date, q: int, kisu: int | None,
) -> tuple[int | None, int | None, list[str]]:
    """현재 기간의 ``(3개월 열, 누적 열)``. 기수·기간이 보고서와 안 맞으면 ``(None, None)``.

    머리 행의 첫 값 열 묶음(같은 윗머리)이 당기다(DART 표는 당기가 맨 앞). 묶음 머리의
    '제 N 기' 가 표지 기수와 다르거나, 표 앞 첫 '부터~까지' 기간의 끝이 보고서 기간말과
    다르면 직전 사업연도 표(K-GAAP 분기보고서의 연결재무제표)로 보고 버린다.
    """
    flags: list[str] = []
    ncol = max(len(line) for line in g) if g else 0
    heads: list[list[str]] = []
    for c in range(1, ncol):
        parts: list[str] = []
        for r in range(hdr):
            cell = g[r][c] if c < len(g[r]) else None
            if cell is not None and cell.text and cell.text not in parts:
                parts.append(cell.text)
        heads.append(parts)
    # 값 열: 주석·비고 열 제외
    cols = [c for c in range(1, ncol) if not any(re.search(r"주\s*석|비\s*고|^주$", p) for p in heads[c - 1])]
    if not cols:
        return None, None, ["no_value_cols"]
    top = {c: (heads[c - 1][0] if heads[c - 1] else "") for c in cols}
    # 당기 묶음 고르기 — 머리에 '전기'·'전분기' 류로 시작하면 그 묶음은 건너뛴다
    first = cols[0]
    for c in cols:
        t = top[c].replace(" ", "")
        if not re.match(r"^(전|前)", t):
            first = c
            break
    group = [c for c in cols if top[c] == top[first]] if top[first] else [first]
    # 묶음이 연속이 아니면 첫 연속 구간만
    run = [group[0]]
    for c in group[1:]:
        if c == run[-1] + 1:
            run.append(c)
        else:
            break
    group = run
    label = top[first].replace(" ", "")
    # 기간 검증 — 날짜(머리 → 표 앞 '부터~까지') > 머리의 '2016년 3월말' > 기수 > '당기' 류
    m = _KISU_RE.search(top[first])
    hdr_text = " ".join(" ".join(heads[c - 1]) for c in group)
    rng = date_ranges(hdr_text) or date_ranges(context)
    ym = _YM_RE.search(hdr_text)
    if rng:
        if rng[0][1] != pend:
            return None, None, ["stale_range"]
        flags.append("verified_range")
    elif ym:
        if (int(ym.group(1)), int(ym.group(2))) != (pend.year, pend.month):
            return None, None, ["stale_ym"]
        flags.append("verified_ym")
    elif m and kisu is not None:
        if int(m.group(1)) != kisu:
            return None, None, ["stale_kisu"]
        flags.append("verified_kisu")
    elif not re.search(r"당(기|분기|반기)", label):
        asof = _ASOF_RE.findall(context)
        if asof:
            d = _mkdate(*asof[0])
            if d and d != pend:
                return None, None, ["stale_asof"]
        flags.append("unverified_period")
    if len(group) == 1:
        c = group[0]
        sub = " ".join(heads[c - 1][1:])
        if q in (1, 4):
            return c, c, flags
        if "3개월" in sub.replace(" ", ""):
            return c, None, flags
        r0 = date_ranges(" ".join(heads[c - 1])) or rng
        if r0 and r0[0][0].month == pend.month - 2:
            return c, None, flags
        return None, c, flags + (["single_col_assumed_cum"] if not r0 and "누적" not in sub else [])
    c3 = next((c for c in group if "3개월" in " ".join(heads[c - 1][1:]).replace(" ", "")), None)
    cc = next((c for c in group if "누적" in " ".join(heads[c - 1][1:]).replace(" ", "")), None)
    if c3 is None and cc is None:
        c3, cc = group[0], group[1]
        flags.append("subcols_assumed")
    if q in (1, 4):
        v = cc if cc is not None else c3
        return v, v, flags
    return c3, cc, flags


def _cell_num(g: list[list[Cell | None]], r: int, c: int | None) -> float | None:
    if c is None or r >= len(g) or c >= len(g[r]) or g[r][c] is None:
        return None
    if c > 0 and g[r][c] is g[r][c - 1]:  # colspan 으로 펼쳐진 자리(라벨이 번진 것)
        return None
    return parse_number(g[r][c].text)


def _iter_ni_tables(blocks: list[Block]):
    """순이익 행이 있는 표마다 ``(i, g, labels, hdr, ctx_list, own, kind, fs)``."""
    section = ""
    for i, b in enumerate(blocks):
        if b.kind == "title":
            section = b.text
            continue
        if b.kind == "text" and len(b.text) < 40 and re.search(r"^\s*\d\s*\.\s*(요약)?\s*(연결)?\s*재무", b.text):
            section = b.text  # 2010~2013 보고서는 절 제목이 TITLE 이 아니라 본문 문단이다
            continue
        if b.kind != "table":
            continue
        g = grid(b.table)
        if len(g) < 3:
            continue
        labels = [(line[0].text if line and line[0] is not None else "") for line in g]
        ni_rows = [r for r, lab in enumerate(labels) if is_total_ni(lab)]
        if not ni_rows:
            continue
        hdr = _header_rows(g)
        if hdr >= len(g):
            continue
        ctx_list = _context_text(blocks, i)
        own = " ".join(c.text for line in g[:hdr] for c in line if c is not None)
        kind, fs = _heading(" ".join(ctx_list) + " " + own)
        if kind is None:
            continue
        r_ni = ni_rows[0]
        if norm_label(labels[r_ni]).startswith("연결"):
            fs = "cfs"
        r_ctrl = _ctrl_row(labels, r_ni)
        if fs is None:
            fs = "cfs" if (r_ctrl is not None or "연결" in section.replace(" ", "")) else "ofs"
        if fs == "ofs":
            r_ctrl = None
        yield i, g, labels, hdr, ctx_list, own, kind, fs, r_ni, r_ctrl


def _doc_kisu(blocks: list[Block], pend: date) -> int | None:
    """기간이 날짜로 확인되는 표의 당기 머리 '제 N 기' — 표지 기수는 회사가 잘못 쓰기도 한다(034810 2016Q1 표지 53기, 표 63기)."""
    for i, g, _labels, hdr, ctx_list, _own, _kind, _fs, _r, _c in _iter_ni_tables(blocks):
        heads = " ".join(g[r][1].text for r in range(hdr) if len(g[r]) > 1 and g[r][1] is not None)
        rng = date_ranges(heads) or date_ranges(" ".join(ctx_list))
        m = _KISU_RE.search(heads)
        if rng and rng[0][1] == pend and m:
            return int(m.group(1))
    return None


def extract_ni_candidates(
    blocks: list[Block], pend: date, q: int, *, kisu: int | None = None, file: str = "",
) -> list[NICandidate]:
    """문서의 손익계산서·현금흐름표·요약재무정보 표에서 현재 기간 순이익 후보. 순서 = 문서 순서.

    ``kisu`` 는 날짜가 없는 표(요약재무정보)의 기간 확인용 — 날짜가 있는 표에서 읽은 기수를
    먼저 쓰고(:func:`_doc_kisu`), 없을 때만 넘겨받은 표지 기수를 쓴다.
    """
    out: list[NICandidate] = []
    kisu = _doc_kisu(blocks, pend) or kisu
    for _i, g, labels, hdr, ctx_list, own, kind, fs, r_ni, r_ctrl in _iter_ni_tables(blocks):
        ctx = " ".join(ctx_list)
        unit = unit_multiplier(own) or unit_multiplier(" ".join(ctx_list[-3:])) or unit_multiplier(ctx)
        if unit is None:
            continue
        c3, cc, flags = _current_columns(g, hdr, ctx, pend, q, kisu)
        if kind == "cf":  # 현금흐름표는 늘 누적
            cc = cc if cc is not None else c3
            c3 = cc if q in (1, 4) else None
        if c3 is None and cc is None:
            continue
        sign = -1 if _loss_only(labels[r_ni]) else 1
        csign = -1 if r_ctrl is not None and _loss_only(labels[r_ctrl]) else 1

        def val(r: int | None, c: int | None, s: int) -> float | None:
            if r is None:
                return None
            v = _cell_num(g, r, c)
            if v is None:
                return None
            return (-abs(v) if s < 0 else v) * unit

        cand = NICandidate(
            fs=fs, kind=kind, unit=unit, file=file, flags=flags,
            total_3m=val(r_ni, c3, sign), total_cum=val(r_ni, cc, sign),
            ctrl_3m=val(r_ctrl, c3, csign), ctrl_cum=val(r_ctrl, cc, csign),
        )
        if cand.total_3m is None and cand.total_cum is None:
            continue
        out.append(cand)
    return out


#: 원 단위 순이익 상한 — 2007~2015 최대(삼성전자 2013 연결 30.5조)의 3배. 넘으면 단위 오류로 본다.
MAX_ABS_NI = 1e14


@dataclass
class NetIncome:
    """한 보고서의 현재 기간 순이익(원). ``*_3m`` = 3개월, ``*_cum`` = 누적(Q1·Q4 는 둘이 같다)."""

    cfs_3m: float | None = None
    cfs_cum: float | None = None
    ctrl_3m: float | None = None
    ctrl_cum: float | None = None
    ofs_3m: float | None = None
    ofs_cum: float | None = None
    src: dict[str, str] = field(default_factory=dict)
    flags: list[str] = field(default_factory=list)


_KIND_RANK = {"is": 0, "cf": 1, "summary": 2}


def choose_net_income(cands: list[NICandidate]) -> NetIncome:
    """후보 → 연결/별도 각각. 3개월은 손익계산서에서만, 누적은 손익 > 현금흐름 > 요약(백만원 반올림) 순.

    원 단위로 1e14 를 넘는 후보는 단위 오류로 버린다(``*_scale_reject``).
    """
    ni = NetIncome()
    for fs in ("cfs", "ofs"):
        pool = [c for c in cands if c.fs == fs]
        ok = [c for c in pool if all(v is None or abs(v) <= MAX_ABS_NI for v in (c.total_3m, c.total_cum, c.ctrl_3m, c.ctrl_cum))]
        if len(ok) < len(pool):
            ni.flags.append(f"{fs}_scale_reject")
        ok.sort(key=lambda c: (_KIND_RANK[c.kind], "unverified_period" in c.flags))
        if not ok:
            continue

        def first(attr: str, kinds: tuple[str, ...] = ("is", "cf", "summary"), pool: list[NICandidate] = ok) -> float | None:
            return next((getattr(c, attr) for c in pool if c.kind in kinds and getattr(c, attr) is not None), None)

        t3, tc = first("total_3m", ("is",)), first("total_cum")
        if fs == "cfs":
            ni.cfs_3m, ni.cfs_cum = t3, tc
            ni.ctrl_3m, ni.ctrl_cum = first("ctrl_3m", ("is",)), first("ctrl_cum")
        else:
            ni.ofs_3m, ni.ofs_cum = t3, tc
        best = ok[0]
        ni.src[fs] = f"{best.kind}:{best.file}"
        ni.flags += [f"{fs}_{f}" for f in best.flags if not f.startswith("verified")]
    return ni


# --------------------------------------------------------------------------- 주식의 총수


@dataclass
class Shares:
    common: int | None
    preferred: int | None
    total: int | None
    asof: date | None
    method: str


def _share_cols(g: list[list[Cell | None]], upto: int) -> dict[int, str]:
    """머리 행에서 열 → '보통주'|'우선주'|'종류'|'합계'."""
    out: dict[int, str] = {}
    for r in range(upto):
        for c, cell in enumerate(g[r]):
            if cell is None:
                continue
            t = cell.text.replace(" ", "")
            if t.startswith("보통주") or t == "보통":
                out[c] = "보통주"
            elif t.startswith("우선주") or t == "우선":
                out[c] = "우선주"
            elif t.startswith("종류주"):
                out[c] = "종류"
            elif t in ("합계", "계", "총계") or t.startswith("합계"):
                out[c] = "합계"
    return out


def extract_shares(blocks: list[Block], pend: date | None = None) -> Shares | None:
    """``Ⅳ. 발행주식의 총수`` 행 → 보통주·우선주·합계. 첫 번째로 찾은 표를 쓴다."""
    for i, b in enumerate(blocks):
        if b.kind != "table":
            continue
        g = grid(b.table)
        row_idx = None
        method = ""
        for r, line in enumerate(g):
            if any(c is not None and c.acode == "ISU_STK3" for c in line):
                row_idx, method = r, "acode"
                break
        if row_idx is None:
            for r, line in enumerate(g):
                lab = "".join(norm_label(c.text) for c in line[:2] if c is not None)
                if "발행주식의총수" in lab and "발행할" not in lab and "현재까지" not in lab:
                    row_idx, method = r, "label"
                    break
        if row_idx is None:
            continue
        line = g[row_idx]
        cols = _share_cols(g, row_idx)
        vals: dict[str, int] = {}
        if method == "acode":
            seq = [(c, cell) for c, cell in enumerate(line) if cell is not None and cell.acode in ("ISU_STK3", "SUM_ISU_STK3")]
            seen: set[int] = set()
            for k, (c, cell) in enumerate(seq):
                if id(cell) in seen:
                    continue
                seen.add(id(cell))
                v = parse_number(cell.text)
                if v is None:
                    continue
                name = cols.get(c) or ("합계" if cell.acode.startswith("SUM") else ("보통주" if k == 0 else "우선주"))
                vals[name] = vals.get(name, 0) + int(v) if name != "합계" else int(v)
        else:
            seen = set()
            for c, cell in enumerate(line):
                if cell is None or id(cell) in seen or c not in cols:
                    continue
                seen.add(id(cell))
                v = parse_number(cell.text)
                if v is not None:
                    vals[cols[c]] = vals.get(cols[c], 0) + int(v) if cols[c] != "합계" else int(v)
        common = vals.get("보통주")
        pref = None
        if "우선주" in vals or "종류" in vals:
            pref = vals.get("우선주", 0) + vals.get("종류", 0)
        total = vals.get("합계")
        if total is None and common is not None:
            total = common + (pref or 0)
        if common is None and total is None:
            continue
        ctx = " ".join(_context_text(blocks, i, 4)) + " " + " ".join(c.text for r in b.table.rows[:2] for c in r)
        asof = None
        m = _ASOF_RE.search(ctx)
        if m:
            asof = _mkdate(*m.group(1, 2, 3))
        return Shares(common, pref, total, asof or pend, method)
    return None


# --------------------------------------------------------------------------- 보고서 단위 조립


@dataclass
class ReportResult:
    rcept_no: str
    year: int
    q: int
    ni: NetIncome
    shares: Shares | None
    kisu: int | None


def doc_files(zip_bytes: bytes) -> list[tuple[str, str]]:
    """``document.xml`` zip → ``[(name, xml)]``, 본문(``<rcept_no>.xml``) 먼저, 첨부(``_NNNNN``) 뒤."""
    z = zipfile.ZipFile(io.BytesIO(zip_bytes))
    names = sorted(z.namelist(), key=lambda n: ("_" in n.rsplit("/", 1)[-1], n))
    return [(n, decode_document(z.read(n))) for n in names]


def parse_report(files: list[tuple[str, str]], rcept_no: str, year: int, q: int) -> ReportResult:
    """본문 먼저, 본문에 없으면 첨부(감사·검토보고서)까지 후보를 모아 고른다."""
    pend = period_end(year, q)
    cands: list[NICandidate] = []
    shares = None
    kisu = None
    for k, (name, xml) in enumerate(files):
        blocks = parse_blocks(xml)
        if k == 0:
            kisu = cover_kisu(blocks)
            shares = extract_shares(blocks, pend)
        main_has = {c.fs for c in cands if c.kind == "is"}
        if k > 0 and {"cfs", "ofs"} <= main_has:
            break
        got = extract_ni_candidates(blocks, pend, q, kisu=kisu if k == 0 else None, file=name.rsplit("/", 1)[-1])
        if k > 0:  # 첨부(감사·검토보고서)는 본문에 손익계산서가 없는 기준만 채운다
            got = [c for c in got if c.kind in ("is", "cf") and c.fs not in main_has]
        cands += got
    return ReportResult(rcept_no, year, q, choose_net_income(cands), shares, kisu)


def pick_netinc(ni: NetIncome, which: str, *, controlling_first: bool) -> tuple[float | None, str | None]:
    """``which`` = ``'3m'``|``'cum'``. ``controlling_first=False`` 가 ``earnings`` 정의(연결 합계→별도)."""
    order = ("ctrl", "cfs", "ofs") if controlling_first else ("cfs", "ofs")
    for basis in order:
        v = getattr(ni, f"{basis}_{which}")
        if v is not None:
            return v, basis
    return None, None


def assemble_quarters(reports: dict[str, ReportResult], *, controlling_first: bool) -> dict[str, tuple[float | None, str | None, list[str]]]:
    """``{'2009Q1': ReportResult, ...}`` → ``{period: (netinc, basis, flags)}`` — ``earnings`` 기간 의미.

    Q1·Q4 = 누적(Q4 = 연간). Q2·Q3 = 3개월 열, 없으면 같은 기준의 누적 − 직전 분기 누적(``derived``).
    직전 분기와 기준이 다르면 만들지 않는다(연결 반기 누적 − 별도 1분기 같은 혼합 금지).
    """
    out: dict[str, tuple[float | None, str | None, list[str]]] = {}
    for period, rr in reports.items():
        y, q = rr.year, rr.q
        if q in (1, 4):
            v, basis = pick_netinc(rr.ni, "cum", controlling_first=controlling_first)
            if v is None and q == 1:
                v, basis = pick_netinc(rr.ni, "3m", controlling_first=controlling_first)
            out[period] = (v, basis, [])
            continue
        v, basis = pick_netinc(rr.ni, "3m", controlling_first=controlling_first)
        if v is not None:
            out[period] = (v, basis, [])
            continue
        cum, basis = pick_netinc(rr.ni, "cum", controlling_first=controlling_first)
        prev = reports.get(f"{y}Q{q - 1}")
        if cum is not None and prev is not None:
            pv = getattr(prev.ni, f"{basis}_cum")
            if pv is not None:
                out[period] = (cum - pv, basis, ["derived"])
                continue
        out[period] = (None, None, ["no_3m"])
    return out


# --------------------------------------------------------------------------- 검증 CLI (네트워크·DB)


def agrees(value: float | None, ref: float | None, tol: float = 0.005) -> bool:
    """검증 합격 정의(사용자 승인): 둘 다 있고, 같거나 상대오차 0.5% 이내."""
    if value is None or ref is None:
        return False
    return value == ref or abs(value - ref) <= tol * abs(ref)


class _Dart:  # pragma: no cover — 네트워크
    """document.xml·list.json 디스크 캐시 + 호출 수 계수. 키 값은 어디에도 찍지 않는다."""

    BASE = "https://opendart.fss.or.kr/api"

    def __init__(self, key: str, cache: "Path") -> None:  # noqa: F821
        self.key, self.cache, self.calls = key, cache, 0
        cache.mkdir(parents=True, exist_ok=True)

    def _get(self, path: str, **params: str):
        import requests

        self.calls += 1
        r = requests.get(f"{self.BASE}/{path}", params={"crtfc_key": self.key, **params}, timeout=60)
        r.raise_for_status()
        return r

    def corp_codes(self) -> dict[str, list[str]]:
        import json
        import xml.etree.ElementTree as ET

        p = self.cache / "corpCode.json"
        if not p.exists():
            z = zipfile.ZipFile(io.BytesIO(self._get("corpCode.xml").content))
            root = ET.fromstring(z.read(z.namelist()[0]))
            lst = [{k: (el.findtext(k) or "").strip() for k in ("corp_code", "corp_name", "stock_code")} for el in root.iter("list")]
            if not lst:
                raise RuntimeError("corpCode 가 비었다 — 0건 성공으로 넘기지 않는다")
            p.write_text(json.dumps(lst, ensure_ascii=False))
        out: dict[str, list[str]] = {}
        for r in json.loads(p.read_text()):
            if r["stock_code"]:
                out.setdefault(r["stock_code"], []).append(r["corp_code"])
        return out

    def filings(self, corp: str, bgn: str, end: str) -> list[dict]:
        import json

        p = self.cache / f"list_{corp}_{bgn}_{end}.json"
        if p.exists():
            return json.loads(p.read_text())
        items, page = [], 1
        while True:
            d = self._get("list.json", corp_code=corp, bgn_de=bgn, end_de=end, pblntf_ty="A",
                          page_no=str(page), page_count="100").json()
            if d.get("status") == "013":
                break
            if d.get("status") != "000":
                raise RuntimeError(f"list.json status={d.get('status')}")
            items += d.get("list", [])
            if page >= int(d.get("total_page", 1)):
                break
            page += 1
        p.write_text(json.dumps(items, ensure_ascii=False))
        return items

    def document(self, rcept_no: str) -> bytes | None:
        import time

        p = self.cache / f"doc_{rcept_no}.zip"
        if not p.exists():
            r = self._get("document.xml", rcept_no=rcept_no)
            if r.content[:2] != b"PK":
                if b"020" in r.content[:500]:
                    raise RuntimeError("DART 일한도(020)")
                return None
            p.write_bytes(r.content)
            time.sleep(0.15)
        return p.read_bytes()


def _cli() -> None:  # pragma: no cover — 네트워크·DB
    import argparse
    import json
    import os
    from collections import Counter
    from pathlib import Path

    ap = argparse.ArgumentParser(description="DART 원문 파서 검증 — earnings·shares_outstanding_history 와 대조(DB 읽기 전용)")
    ap.add_argument("--codes", required=True, help="쉼표 구분 종목코드 또는 @파일(공백 구분)")
    ap.add_argument("--years", default="2015,2016")
    ap.add_argument("--cache", required=True, help="원문 zip·list.json 캐시 디렉터리(커밋 금지)")
    ap.add_argument("--key-env", default="DART_API_KEY_3", help="키를 담은 환경변수 이름(값은 출력하지 않는다)")
    ap.add_argument("--out", required=True, help="종목·분기별 결과 JSON")
    ap.add_argument("--latest", action="store_true", help="정정이 있으면 마지막 정정본도 받아 따로 파싱")
    ap.add_argument("--compare", action="store_true", help="KR_QUANT_DB 의 earnings/shares 와 대조(2015=2016 보고서의 netinc_prior)")
    a = ap.parse_args()
    years = [int(y) for y in a.years.split(",")]
    codes = Path(a.codes[1:]).read_text().split() if a.codes.startswith("@") else a.codes.split(",")
    dart = _Dart(os.environ[a.key_env], Path(a.cache))
    corp = dart.corp_codes()
    bgn, end = f"{min(years)}0101", f"{max(years) + 1}0630"

    def parse(rno: str, period: str) -> ReportResult | None:
        raw = dart.document(rno)
        return None if raw is None else parse_report(doc_files(raw), rno, int(period[:4]), int(period[-1]))

    result: dict[str, dict] = {}
    for code in codes:
        items = [it for cp in corp.get(code, []) for it in dart.filings(cp, bgn, end)]
        filings = {p: f for p, f in select_filings(items).items() if int(p[:4]) in years}
        if not filings:
            kinds = Counter(re.sub(r"^\[[^\]]*\]", "", it["report_nm"]).strip()[-8:-1] for it in items)
            result[code] = {"error": "no_dec_fye_filing" if items else "no_filing", "report_months": dict(kinds)}
            continue
        orig = {p: parse(f["rcept_no"], p) for p, f in sorted(filings.items())}
        orig = {p: r for p, r in orig.items() if r is not None}
        latest = dict(orig)
        if a.latest:
            for p, f in filings.items():
                if f["corrections"]:
                    r = parse(f["corrections"][-1], p)
                    if r is not None:
                        latest[p] = r
        out: dict[str, dict] = {}
        for tag, reps in (("orig", orig), ("latest", latest)):
            e = assemble_quarters(reps, controlling_first=False)
            c = assemble_quarters(reps, controlling_first=True)
            for p in filings:
                d = out.setdefault(p, {"rcept_no": filings[p]["rcept_no"], "rcept_dt": filings[p]["rcept_dt"],
                                       "corrections": filings[p]["corrections"],
                                       "original_missing": filings[p]["original_missing"]})
                d[f"{tag}_netinc"], d[f"{tag}_basis"], d[f"{tag}_flags"] = e.get(p, (None, None, ["unparsed"]))
                d[f"{tag}_netinc_ctrl_first"], d[f"{tag}_basis_ctrl_first"], _ = c.get(p, (None, None, []))
                rr = reps.get(p)
                d[f"{tag}_ni"] = None if rr is None else {k: v for k, v in rr.ni.__dict__.items() if v not in (None, [], {})}
                if tag == "orig" and rr is not None and rr.shares is not None:
                    d["shares"] = {**rr.shares.__dict__, "asof": str(rr.shares.asof)}
        result[code] = {"periods": out}
    Path(a.out).write_text(json.dumps(result, ensure_ascii=False, default=str, indent=1))
    print(f"parsed codes={len(codes)} dart_calls={dart.calls}")
    if a.compare:
        _compare(result)


def _compare(result: dict[str, dict]) -> None:  # pragma: no cover — DB
    import os
    from collections import Counter

    import psycopg2

    codes = list(result)
    con = psycopg2.connect(os.environ["KR_QUANT_DB"])
    try:
        cur = con.cursor()
        cur.execute("SELECT code, period, netinc, netinc_prior FROM earnings WHERE period LIKE '2016Q_' AND code = ANY(%s)", (codes,))
        ref: dict[tuple[str, str], float | None] = {}
        for code, period, ni, nip in cur.fetchall():
            ref[(code, period)] = ni
            ref[(code, "2015" + period[4:])] = nip
        cur.execute("SELECT code, date, shares_outstanding FROM shares_outstanding_history WHERE source='dart' "
                    "AND date BETWEEN '2016-01-01' AND '2016-12-31' AND code = ANY(%s)", (codes,))
        sref = {c: (str(d), v) for c, d, v in cur.fetchall()}
    finally:
        con.close()
    for tag in ("orig", "latest"):
        n = ok = 0
        cats: Counter = Counter()
        for (code, period), r in ref.items():
            if r is None:
                continue
            res = result.get(code, {})
            if "error" in res:
                n += 1
                cats[res["error"]] += 1
                continue
            d = res["periods"].get(period)
            n += 1
            if d is None:
                cats["no_filing_for_period"] += 1
                continue
            v = d[f"{tag}_netinc"]
            if agrees(v, r):
                ok += 1
            else:
                cats["parser_none" if v is None else "value_diff"] += 1
        print(f"[{tag}] netinc 일치 {ok}/{n} = {ok / max(n, 1):.2%}  불일치 {dict(cats)}")
    sn = sok = 0
    for code, (d, v) in sref.items():
        p = result.get(code, {}).get("periods", {}).get("2016Q4")
        sn += 1
        sok += bool(p and p.get("shares") and p["shares"]["common"] == v)
    print(f"shares(보통주 발행주식총수, 2016 사업보고서) 일치 {sok}/{sn} = {sok / max(sn, 1):.2%}")


if __name__ == "__main__":  # pragma: no cover
    _cli()
