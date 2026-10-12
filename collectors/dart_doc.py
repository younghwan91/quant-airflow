"""DART 정기보고서 **원문**(``document.xml``)에서 분기 순이익·발행주식총수를 뽑는 순수 파서.

**왜 원문인가.** ``earnings``(``dart_earnings.py``)의 원천인 ``fnlttMultiAcnt``/``fnlttSinglAcnt``
는 2015 사업연도부터만 답한다. 2007~2015 를 메우려면 사업·반기·분기보고서 원문 표를 읽어야
한다(2006~2016 이력 확장, 사용자 승인 2026-10-12 — 완료 조건 C: 2016 겹침 구간 95% 일치).

**무엇을 같은 의미로 맞추나** — ``earnings`` 표와 같은 기간 의미:

- ``period`` = ``'2009Q2'`` 꼴 달력 분기, **12월 결산사만**. 보고서명 기준월(``period_from_report_nm``)에
  더해 그 해 사업보고서 기준월이 12가 아니면 뺀다(:func:`select_filings` — 6월 결산사의 '분기보고서
  (2015.09)' 는 회계 1분기다). 참고로 ``earnings`` 는 비12월 결산사를 회계분기 그대로 달력분기 라벨로
  담고 있다(000970·006370 실측) — 이 파서는 그 행들과 같은 뜻을 만들지 않는다.
- Q1~Q3 = **3개월** 값, Q4 = **사업연도 전체**(사업보고서 당기). ``earnings`` 2016Q4 삼성전자
  22.7조 = 연간 순이익으로 실측. 표에 3개월 열이 없으면(요약재무정보만 있는 2010~2013 보고서)
  누적 − 직전 분기 누적으로 만들고 ``derived`` 플래그를 단다(:func:`assemble_quarters`).
- 단위 = **원**. 표마다 ``(단위 : 백만원)`` 이 다르다 — 단위를 못 찾으면 값을 버린다(틀린 값보다 없는 값).
- ``knowledge_date`` = 최초(정정 아닌) 접수번호 앞 8자리 = 접수일. 정정은 별도 행으로 쌓을 수 있게
  호출부가 ``rcept_no`` 를 그대로 넘긴다.

**순이익 세 가지 기준**(``NetIncome``): 연결 지배기업 소유주 귀속(``ctrl``), 연결 당기순이익
전체(``cfs``, 비지배 포함), 별도/개별(``ofs``). ``earnings.netinc`` 는 ``cfs`` 가 있으면 그것,
없으면 ``ofs`` 다(``krx_fundamentals_client._parse_financial_rows`` — 연결 우선, 계정명
'당기순이익' = 비지배 포함 합계). 그래서 :func:`basis_order` 는 두 정의를 따로 낸다:
``earnings`` 와 같은 ``cfs→ofs`` 와, 지배지분 우선 ``ctrl→cfs→ofs``. 어느 쪽을 썼는지 ``basis``.

**K-GAAP 시절(~2010) 함정**: 분기보고서의 ``5. 연결재무제표`` 는 **직전 사업연도** 연결표다
(분기 연결 의무가 없었다). 표 머리의 기수·기간이 보고서 기간과 맞는지(:func:`_current_columns`)
보지 않으면 2007 연간 연결 순이익이 2008Q1 값으로 들어간다.

**발행주식총수**(``Shares``): ``4. 주식의 총수 등`` 표의 ``Ⅳ. 발행주식의 총수`` 행(ACODE
``ISU_STK3``) — 보통주·우선주·합계. ``shares_outstanding_history`` 의 ``istc_totqy``(발행주식총수,
유통주식수 아님 — ``dart_shares.py`` 문서)와 같은 의미다.

원문 인코딩: XML 선언은 utf-8 이지만 옛 보고서는 cp949 다(2026-10-13 실측) — utf-8 먼저, 실패하면 cp949.

**표 고르기**: 표 바로 앞 제목(손익계산서·현금흐름표·요약재무정보·배당)으로 종류를, 앞뒤 수식어·귀속
행·절 제목으로 연결/별도를 정하고, 기간은 날짜('부터~까지')·'2016년 3월말'·기수로 확인한 표만 쓴다.
3개월은 손익계산서에서만, 누적은 손익 > 현금흐름 > 요약표(반올림) 순. 본문에 본표가 없으면 첨부
(감사·검토보고서)까지 본다. 배당표는 단위 대조에만 쓴다(:func:`_reconcile_units`).

이 파일은 전부 순수 함수다. 받아오기·전체 실행·검증·폐지 종목 매핑은 :mod:`collectors.dart_doc_run`.
"""

from __future__ import annotations

import io
import re
import unicodedata
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

_NUM_RE = re.compile(r"^(?P<neg>\(\s*[-−]\s*\)|[\(△▲Δ∆\-−–]|\(\s*-)?\s*(?P<n>\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?\s*\)?\s*(?:원|주)?$")


def parse_number(text: str) -> float | None:
    """``'1,234'``·``'(1,234)'``·``'△1,234'``·``'-1,234'`` → 수. ``'-'``·빈칸·날짜·문장 → ``None``."""
    # K-GAAP 감사보고서 표기: '￦ 33,295,643,940', 합계 밑줄 '33,295,643,940 ==========' / '-----'
    t = _ws(text)
    # 은행 양식 '85,702,748,535 (72,106,748,535)' — 앞 숫자가 그 칸의 값, 괄호는 대손준비금 반영후 조정이익(139130 2011 반기)
    m2 = re.fullmatch(r"([\(△\-]?\s*[\d,]+\s*\)?)\s+\(\s*[\d,]+\s*\)", t)
    if m2 and not t.startswith("("):
        t = m2.group(1)
    s = re.sub(r"[=＝]{2,}|-{3,}|[￦₩]", "", t).replace(" ", "")
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
#: '(기준일 : 2015년 12월 31일 )' — K-IFRS 시절 '주식의 총수 현황' 표 머리
_BASEDATE_RE = re.compile(r"기\s*준\s*일\s*[:：]?\s*" + _DATE)
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
    # 사업보고서 기준월이 12가 아닌 해(그 해·전해 분기)는 뺀다 — 6월 결산사의 '분기보고서 (2015.09)' 는
    # 회계 1분기라 보고서명만 보면 달력 3분기로 오인된다(001080, 2026-10-13 실측).
    non_dec_years: set[int] = set()
    for it in items:
        m = _REPORT_NM_RE.match(it.get("report_nm", "").strip())
        if m and m.group(1) == "사업보고서" and m.group(3) != "12":
            non_dec_years |= {int(m.group(2)), int(m.group(2)) - 1}
    by: dict[str, list[dict]] = {}
    for it in items:
        per = period_from_report_nm(it.get("report_nm", ""))
        if per and per[0] not in non_dec_years:
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


def _cover_text(blocks: list[Block]) -> str:
    return " ".join(b.text if b.kind != "table" else " ".join(c.text for r in b.table.rows for c in r) for b in blocks[:12])


def cover_period_start(blocks: list[Block], pend: date) -> date | None:
    """표지 '사업연도 2011년 05월 17일 부터 2011년 09월 30일 까지' 의 시작일(끝이 기간말과 같을 때만)."""
    rng = [r for r in date_ranges(_cover_text(blocks)) if r[1] == pend]
    return rng[0][0] if rng else None


def cover_kisu(blocks: list[Block]) -> int | None:
    """표지의 ``(제 40 기 1 분기)`` → 40. 표지는 문서 앞 몇 블록이다."""
    for b in blocks[:12]:
        txt = b.text if b.kind != "table" else " ".join(c.text for r in b.table.rows for c in r)
        m = _KISU_RE.search(txt)
        if m:
            return int(m.group(1))
    return None


# --------------------------------------------------------------------------- 순이익 표


#: 번호 — 로마숫자는 ASCII·유니코드 혼용('XⅢ')·소문자 l 오타('Xl.반기순손익', 139130 2013 반기)까지
_ENUM_RE = re.compile(r"^(?:[\(\[]?(?:[ⅠⅡⅢⅣⅤⅥⅦⅧⅨⅩⅪⅫIVXLivxl]+|\d{1,2}|[가-하])[\.\)\]]|[ㆍ·\-\*※])+")


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


_TOTAL_NI_RE = re.compile(r"^(?:연결)?(?:총)?(?:당기|당분기|당반기|분기|반기|기간|당)?(?:연결)?(?:총)?순(?:이익|손실|손익)$")
_EXCLUDE = ("주당", "포괄", "계속", "중단", "차감전", "법인세", "귀속", "지배", "소수", "누계")


def _core(label: str) -> str:
    """괄호주 제거 — 중첩('(대손준비금반영후조정이익당반기(누적): …)')·닫히지 않은 꼬리('…(대손준비금 반')까지."""
    prev = None
    while prev != label:
        prev, label = label, re.sub(r"\([^()]*\)", "", label)
    return re.sub(r"\(.*$", "", label)


def is_total_ni(label: str) -> bool:
    n = norm_label(label)
    if any(x in n for x in _EXCLUDE):
        return False
    return bool(_TOTAL_NI_RE.match(_core(n)))


def _loss_only(label: str) -> bool:
    n = norm_label(label)
    return "손실" in n and "이익" not in n and "손익" not in n


def _attribution_rows(labels: list[str], i_ni: int) -> tuple[int | None, int | None]:
    """총 순이익 행 다음의 ``(지배기업 소유주 귀속 행, 비지배지분 귀속 행)``. 총포괄손익 귀속은 건너뛴다.

    K-GAAP 표기 '지배회사지분순이익'·'소수주주지분순이익', IFRS '지배기업소유주지분순이익(손실)'·
    '비지배지분순이익', '당기순이익의 귀속' 아래 '지배기업의 소유주' 를 모두 잡는다. 옛 K-GAAP(~2007)
    '총당기순이익 → 외부주주지분순이익 → 연결당기순이익' 에서는 비지배 뒤의 합계 행이 지배지분이다(005380 2007).
    """
    in_ni_attr = in_ci_attr = seen_ci = False
    r_ctrl = r_nci = None
    for j in range(i_ni + 1, min(len(labels), i_ni + 45)):
        lab = norm_label(labels[j])
        if not lab:
            continue
        if "주당" in lab:
            break
        is_nci = "비지배" in lab or "소수주주" in lab or "소수지분" in lab or "외부주주" in lab
        is_ctrl = "지배" in lab and not is_nci and "소수" not in lab
        if "귀속" in lab and not is_ctrl and not is_nci:
            in_ci_attr = "포괄" in lab
            in_ni_attr = not in_ci_attr
            continue
        if "포괄" in lab and not is_ctrl and not is_nci:
            seen_ci = True
            continue
        if "포괄" in lab or in_ci_attr:
            continue
        near = in_ni_attr or bool(re.search(r"순(이익|손익|손실)", lab)) or (not seen_ci and j - i_ni <= 3)
        if is_ctrl and r_ctrl is None and near:
            r_ctrl = j
        elif is_nci and r_nci is None and near:
            r_nci = j
        if r_ctrl is not None and r_nci is not None:
            break
        # 옛 K-GAAP: '총당기순이익 → 외부주주지분순이익 → 연결당기순이익' — 비지배 다음의 합계 행이 지배지분이다
        if r_nci is not None and r_ctrl is None and j > r_nci and is_total_ni(labels[j]):
            r_ctrl = j
            break
    return r_ctrl, r_nci


def _ctrl_row(labels: list[str], i_ni: int) -> int | None:
    return _attribution_rows(labels, i_ni)[0]


@dataclass
class NICandidate:
    """한 표에서 읽은 순이익(원 단위). 현재 기간 열만."""

    fs: str                       # 'cfs' | 'ofs'
    kind: str                     # 'statement' | 'summary'
    total_3m: float | None = None
    total_cum: float | None = None
    ctrl_3m: float | None = None
    ctrl_cum: float | None = None
    unit: float = 1
    file: str = ""
    flags: list[str] = field(default_factory=list)
    #: 같은 표의 전년 동기 비교 열(Q1~Q3 3개월·Q4 연간) — 손익계산서 본표에서만
    total_prior: float | None = None
    ctrl_prior: float | None = None
    prior_note: str | None = None


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
    r"|(?P<dv>배\s*당)"
    r"|(?P<fsw>재\s*무\s*제\s*표)"
    r"|(?P<other>자\s*본\s*변\s*동\s*표|재\s*무\s*상\s*태\s*표|대\s*차\s*대\s*조\s*표|이익잉여금처분|결손금처리|주\s*석|부문|주당|영업실적)"
)
_QUAL_RE = re.compile(r"연\s*결|별\s*도|개\s*별")


def _heading(context: str) -> tuple[str | None, str | None]:
    """``(kind, fs)`` — kind ∈ ``is``·``cf``·``summary``·``div``·``None``, fs ∈ ``cfs``·``ofs``·``None``(모름).

    ``div`` = '배당에 관한 사항' 표의 '(연결)당기순이익(백만원)' 행 — 값으로는 쓰지 않고 단위 대조에만 쓴다.

    '연결 및 개별재무제표는 ...' 같은 본문 언급이 아니라 표 바로 앞 제목을 보려고 가장 가까운
    (마지막) 일치를 쓴다. 연결/별도 수식어는 그 제목 앞 8자·뒤 8자('요약재무정보(연결)')에서 찾는다.
    """
    # 조사가 바로 붙은 일치('…손익계산서를 재작성하였습니다', '…요약포괄손익계산서는 다음과')는 제목이 아니라
    # 문장 속 언급이다(042420·083310 2016, 2026-10-13).
    ms = [m for m in _HEADING_RE.finditer(context) if m.lastgroup == "dv" or not _PARTICLE_RE.match(context, m.end())]
    if not ms:
        return None, None
    m = ms[-1]
    kind = {"is": "is", "cf": "cf", "sm": "summary", "dv": "div", "fsw": "fsword", "other": None}[m.lastgroup]
    if kind is None:
        return None, None
    if kind in ("is", "cf") and re.search(r"요\s*약[^가-힣]*(?:연\s*결|별\s*도|개\s*별)?\s*(?:포\s*괄\s*)?$", context[max(0, m.start() - 12):m.start()]):
        kind = "summary"  # '[요약 연결손익계산서]' — 요약재무정보의 손익 부분(백만원 반올림, 누적)
    around = context[max(0, m.start() - 8):m.start()] + m.group(0) + context[m.end():m.end() + 8]
    quals = [q.replace(" ", "") for q in _QUAL_RE.findall(around)]
    if kind == "fsword":
        # 요약재무정보 아래 '가. 연결재무제표 / 나. 재무제표' 소제목 — 수식어 없는 '재무제표' 는 별도다
        if "연결" in quals and not any(q in ("별도", "개별") for q in quals):
            return kind, "cfs"
        return kind, "ofs"
    if "연결" in quals and not any(q in ("별도", "개별") for q in quals):
        return kind, "cfs"
    if any(q in ("별도", "개별") for q in quals) and "연결" not in quals:
        return kind, "ofs"
    return kind, None


_PARTICLE_RE = re.compile(r"\s*(?:는|은|를|을|에|의|과|와|로|으로|이|가|도|상)(?![가-힣])|\s*(?:에\s*관한|에\s*대한)")


def _heading_text(blocks: list[str]) -> str:
    """제목 찾기용 문맥 — 긴 문단은 앞 80자·끝 160자만(소제목은 문단 머리, '(단위 : 천원)' 앞 표 이름은 꼬리에 붙는다)."""
    return " ".join(t if len(t) < 300 else t[:80] + " … " + t[-160:] for t in blocks)


_PERIODISH_RE = re.compile(r"제\s*\d+|당\s*기|전\s*기|분\s*기|반\s*기|\d{4}|3\s*개\s*월|누\s*적")


def _current_columns(
    g: list[list[Cell | None]], hdr: int, context: list[str], pend: date, q: int, kisu: int | None,
) -> tuple[list[int], list[int], list[str]]:
    """현재 기간의 ``(3개월 열들, 누적 열들)`` — 값은 그중 첫 숫자 칸. 기간이 안 맞으면 둘 다 빈 목록.

    열이 목록인 이유: '과목 | 3개월(빈 칸) | 3개월(값) | 누적(빈 칸) | 누적(값)' 처럼 머리 하나가
    두 칸에 걸치고 값은 둘째 칸에 있는 표가 있다(203690 2015 반기).

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
    cols = [c for c in range(1, ncol)
            if not any(re.search(r"주\s*석|비\s*고|^주$|주식의\s*종류|^구\s*분$|^과\s*목$", p) for p in heads[c - 1])]
    if not cols:
        return [], [], ["no_value_cols"]
    # 모든 값 열에 같은 글이 걸친 머리 줄('[요약 연결손익계산서]' 표제)은 묶음 기준이 아니다 — 열마다 갈리는 첫 줄을 쓴다
    lvl = 0
    while (len(cols) > 1 and len({tuple(heads[c - 1][lvl:lvl + 1]) for c in cols}) == 1
           and any(len(heads[c - 1]) > lvl + 1 for c in cols)
           and not _PERIODISH_RE.search(heads[cols[0] - 1][lvl])):
        # 기간 표기('제 1(당) 기 3분기')는 당기 열만 있는 표에서도 모든 열에 걸친다 — 그건 표제가 아니다(139130 2011Q3)
        lvl += 1
    for c in cols:
        heads[c - 1] = heads[c - 1][lvl:]
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
    hdr_text = " ".join(" ".join(heads[c - 1]) for c in group)
    m = _KISU_RE.search(hdr_text)  # '당기' 아래 줄에 '제39기' 가 오는 표도 있다(012160 배당표 — 당기=직전 사업연도)
    # 표 앞 문맥에서는 **가장 가까운** 기간 표기 블록만 본다 — 앞 표의 기간을 빌려 오지 않게
    near = next((r for r in (date_ranges(t) for t in reversed(context)) if r), [])
    rng = date_ranges(hdr_text) or near
    ym = _YM_RE.search(hdr_text)
    if rng:
        if (rng[0][1].year, rng[0][1].month) != (pend.year, pend.month):
            # 일(day)은 보지 않는다 — '2009년 03월 03일 까지' 같은 회사 오타(085310 2009Q1)로 본표를 버리지 않게.
            # 직전 연도·직전 분기 표는 연·월이 달라 그대로 걸러진다
            return [], [], ["stale_range"]
        flags.append("verified_range")
    elif ym:
        if (int(ym.group(1)), int(ym.group(2))) != (pend.year, pend.month):
            return [], [], ["stale_ym"]
        flags.append("verified_ym")
    elif m and kisu is not None:
        if int(m.group(1)) != kisu:
            return [], [], ["stale_kisu"]
        flags.append("verified_kisu")
    elif not re.search(r"당(기|분기|반기)", label):
        asof = _ASOF_RE.findall(" ".join(context))
        if asof:
            d = _mkdate(*asof[0])
            if d and d != pend:
                return [], [], ["stale_asof"]
        years = {int(y) for y in re.findall(r"(?<!\d)((?:19|20)\d{2})(?!\d)", hdr_text)}
        if years and pend.year not in years:
            return [], [], ["stale_year"]
        flags.append("unverified_period")
    if q in (1, 4):  # 1분기·사업연도: 3개월 = 누적
        return group, group, flags

    def sub(c: int) -> str:
        return " ".join(heads[c - 1][1:]).replace(" ", "")

    c3 = [c for c in group if "3개월" in sub(c)]
    cc = [c for c in group if "누적" in sub(c)]
    if c3 or cc:
        return c3, cc, flags
    if len(group) >= 2 and len({tuple(heads[c - 1]) for c in group}) > 1:
        return [group[0]], [group[1]], flags + ["subcols_assumed"]
    # 머리가 똑같은 칸 여럿 = 한 기간의 '세부 금액 | 소계' 두 칸(은행·K-GAAP 양식) — 한 열로 보고 첫 숫자 칸을 쓴다
    #(024110 2012 반기: 순이익은 둘째 칸 780,323, 지배기업주주지분은 첫 칸 777,785)
    # 열 하나 — 머리의 기간이 3개월이면 3개월, 아니면 누적(요약재무정보·현금흐름표는 누적)
    r0 = date_ranges(" ".join(heads[group[0] - 1])) or rng
    if r0 and r0[0][0].month == pend.month - 2:
        return group, [], flags
    return [], group, flags + ([] if r0 else ["single_col_assumed_cum"])


def _prior_columns(
    g: list[list[Cell | None]], hdr: int, context: list[str], pend: date, q: int, cur_last: int,
) -> tuple[list[int], str | None]:
    """당기 묶음 바로 뒤의 **전년 동기** 비교 열 — ``(열들, 사정)``. ``earnings.netinc_prior``(fnlttMultiAcnt
    ``frmtrm_amount``)와 같은 뜻: Q1~Q3 = 전년 같은 분기 3개월, Q4 = 전년 연간. 같은 표라 기준(연결/별도)이 같고
    재작성(K-IFRS 전환 등)도 그 보고서 기준으로 반영돼 있다.

    확인: 그 묶음 머리 또는 표 앞 두 번째 '부터~까지' 의 끝이 (전년, 같은 달)이거나, 기수가 당기 − 1 이거나,
    머리가 '전…' 으로 시작. Q2·Q3 에 3개월 비교 열이 없으면(누적만) 빈 목록 + ``prior_no_3m``.
    """
    ncol = max(len(line) for line in g) if g else 0
    heads: list[list[str]] = []
    for c in range(1, ncol):
        parts: list[str] = []
        for r in range(hdr):
            cell = g[r][c] if c < len(g[r]) else None
            if cell is not None and cell.text and cell.text not in parts:
                parts.append(cell.text)
        heads.append(parts)
    cols = [c for c in range(cur_last + 1, ncol)
            if not any(re.search(r"주\s*석|비\s*고|^주$", x) for x in heads[c - 1])]
    if not cols:
        return [], "prior_no_column"
    lvl = 0
    while (len(cols) > 1 and len({tuple(heads[c - 1][lvl:lvl + 1]) for c in cols}) == 1
           and any(len(heads[c - 1]) > lvl + 1 for c in cols) and not _PERIODISH_RE.search(heads[cols[0] - 1][lvl])):
        lvl += 1
    top = {c: " ".join(heads[c - 1][lvl:lvl + 1]) for c in cols}
    grp = [cols[0]]
    for c in cols[1:]:
        if top[c] == top[cols[0]] and c == grp[-1] + 1:
            grp.append(c)
        else:
            break
    label = top[cols[0]]
    near = next((r for r in (date_ranges(t) for t in reversed(context)) if r), [])
    rng = date_ranges(" ".join(" ".join(heads[c - 1]) for c in grp)) or near[1:2]
    cur_heads = " ".join(" ".join(heads[c - 1]) for c in range(1, cur_last + 1) if c - 1 < len(heads))
    km, kc = _KISU_RE.search(label), _KISU_RE.search(cur_heads)
    ym = _YM_RE.search(" ".join(" ".join(heads[c - 1]) for c in grp))
    near_asof = next((a for a in (_ASOF_RE.findall(t) for t in reversed(context)) if a), [])
    if not rng and len(near_asof) >= 2:  # '제43기 1분기말 2015.03.31 현재'(033540 2016Q1 연결 손익 머리)
        d = _mkdate(*near_asof[1])
        rng = [(d, d)] if d else []
    if rng:
        if (rng[0][1].year, rng[0][1].month) != (pend.year - 1, pend.month):
            return [], "prior_period_mismatch"
    elif ym:
        if (int(ym.group(1)), int(ym.group(2))) != (pend.year - 1, pend.month):
            return [], "prior_period_mismatch"
    elif km and kc:
        if int(km.group(1)) != int(kc.group(1)) - 1:
            return [], "prior_period_mismatch"
    elif not re.match(r"^\s*(전|前)", label):
        return [], "prior_unverified"
    if q in (1, 4):
        return grp, None

    def sub(c: int) -> str:
        return " ".join(heads[c - 1][lvl + 1:]).replace(" ", "")

    p3 = [c for c in grp if "3개월" in sub(c)]
    if p3:
        return p3, None
    if any("누적" in sub(c) for c in grp):
        return [], "prior_no_3m"
    # 머리에 3개월/누적이 없으면 그 묶음 기간으로 — 3개월이면 쓰고 누적이면 없음
    if rng and rng[0][0].month == pend.month - 2:
        return grp, None
    return [], "prior_no_3m"


def _cell_num(g: list[list[Cell | None]], r: int, cols: list[int]) -> float | None:
    """``cols`` 중 첫 숫자 칸. colspan 으로 라벨이 번진 자리는 건너뛴다."""
    for c in cols:
        if r >= len(g) or c >= len(g[r]) or g[r][c] is None:
            continue
        if c > 0 and g[r][c] is g[r][c - 1] and g[r][c] is g[r][0]:
            continue
        v = parse_number(g[r][c].text)
        if v is not None:
            return v
    return None


_FIN_START_RE = re.compile(r"재\s*무\s*에\s*관\s*한\s*사\s*항")
_FIN_END_RE = re.compile(r"^\s*(?:IV|Ⅳ)\s*\.|감\s*사\s*인\s*의\s*감\s*사\s*의\s*견|이\s*사\s*의\s*경\s*영\s*진\s*단")


def financial_section(blocks: list[Block]) -> tuple[int, int]:
    """본문의 'III. 재무에 관한 사항' 블록 구간 ``[start, end)``. 없으면 문서 전체.

    목차에도 같은 제목이 나오므로 표가 가장 많은 구간을 고른다. 이 구간 밖의 표(최대주주의
    요약연결재무정보 — 001550 2015Q1 은 다른 회사 순이익 176억을 자기 것으로 읽을 뻔했다)는 보지 않는다.
    """
    marks = [(i, "s" if _FIN_START_RE.search(b.text) else "e") for i, b in enumerate(blocks)
             if b.kind != "table" and len(b.text) < 60 and (_FIN_START_RE.search(b.text) or _FIN_END_RE.search(b.text))]
    best, span = 0, (0, len(blocks))
    for k, (i, kind) in enumerate(marks):
        if kind != "s":
            continue
        end = next((j for j, kd in marks[k + 1:] if kd == "e"), len(blocks))
        n = sum(1 for b in blocks[i:end] if b.kind == "table")
        if n > best:
            best, span = n, (i, end)
    return span


_SUBSECTION_RE = re.compile(r"^\s*[가-하]\s*[.,)]\s*(요\s*약)?\s*(연\s*결|별\s*도|개\s*별)?\s*재\s*무")
_SUBSIDIARY_RE = re.compile(r"종\s*속\s*(회\s*사|기\s*업)|연\s*결\s*실\s*체")
_COMPANY_RE = re.compile(r"주\s*식\s*회\s*사|㈜|\(\s*주\s*\)")


_CONT_NI_RE = re.compile(r"^계속(?:사업|영업)(?:당기|분기|반기)?(?:순)?(?:이익|손익|손실)$")


def _iter_ni_tables(blocks: list[Block], span: tuple[int, int] | None = None):
    """순이익 행이 있는 표마다 ``(i, g, labels, hdr, ctx_list, own, kind, fs, r_ni, (r_ctrl, r_nci))``."""
    section = ""
    seen_cfs_is = False  # 이 문서에서 '연결 …손익계산서' 로 명시된 표를 이미 봤다
    lo, hi = span or (0, len(blocks))
    for i, b in enumerate(blocks):
        if b.kind == "title":
            section = b.text
            continue
        if b.kind == "text" and (re.search(r"^\s*\d\s*\.\s*(요약)?\s*(연결|별도|개별)?\s*재무", b.text)
                                 or (len(b.text) < 40 and _SUBSECTION_RE.search(b.text))):
            # 2010~2013 보고서는 절 제목이 TITLE 이 아니라 본문 문단(머리)이다. '가.요약재무정보' 같은 소제목도
            # 절로 친다 — '1. 연결 재무제표에 관한 사항' 아래 별도 요약이 이어지는 보고서가 있다(011320 2012)
            section = b.text[:40]
            continue
        if b.kind != "table":
            continue
        g = grid(b.table)
        if len(g) < 3:
            continue
        labels = [(line[0].text if line and line[0] is not None else "") for line in g]
        ni_rows = [r for r, lab in enumerate(labels) if is_total_ni(lab)]
        if not ni_rows:
            # '당기순이익' 행 없이 '계속사업이익(손실)' 로 끝나는 K-GAAP 손익계산서(088390 2009Q3) — 중단사업 행이
            # 없으면 그게 순이익이다
            normed_all = [_core(norm_label(x)) for x in labels]
            if not any("중단" in x for x in normed_all):
                ni_rows = [r for r, x in enumerate(normed_all) if _CONT_NI_RE.match(x)][-1:]
        if not ni_rows:
            continue
        hdr = _header_rows(g)
        if hdr >= len(g):
            continue
        ctx_list = _context_text(blocks, i)
        # 머리의 '주석'·'비고' 열 이름은 표 이름이 아니다(012320 2012 별도 감사보고서 — '주석' 열 때문에 표를 버렸다)
        own = " ".join(c.text for line in g[:hdr] for c in line
                       if c is not None and not re.fullmatch(r"\s*(주\s*석|비\s*고)\s*", c.text))
        # 머리 칸의 괄호 주('(검토를 받지않은 반기재무제표)')는 표 이름이 아니다(001310 2013 반기)
        kind, fs = _heading(_heading_text(ctx_list) + " " + re.sub(r"\([^()]*\)", " ", own))
        if kind == "fsword":  # 'N. 요약재무정보' 절 안의 소제목일 때만 요약표로 본다
            kind = "summary" if "요약" in section.replace(" ", "") else None
        if kind is None or (kind == "summary" and not lo <= i < hi):
            continue
        r_ni = ni_rows[0]
        label_cfs = norm_label(labels[r_ni]).startswith("연결")
        r_ctrl, r_nci = _attribution_rows(labels, r_ni)
        if fs is None:
            # 수식어 없는 '손 익 계 산 서' — 표 머리의 회사 표기가 먼저다: '주식회사 유니크 및 종속회사' 면 연결,
            # '주식회사 유니크' 만이면 별도(011320 2012: 별도 손익을 연결로 읽어 +16% 틀린 값).
            # 같은 문서에서 '연결 …손익계산서' 를 이미 봤으면 수식어 없는 손익계산서는 별도다(012320 2012).
            # 본표의 계정명 '연결반(당)기순이익' 은 회사가 연결표를 베껴 쓴 오기일 수 있어(006220 2015 반기 별도)
            # 회사 표기보다 뒤에 본다(요약표는 계정명이 먼저). 그다음 귀속 행, 마지막으로 절 제목.
            head = " ".join(ctx_list[-2:])
            if _SUBSIDIARY_RE.search(head) or (label_cfs and kind == "summary"):
                fs = "cfs"
            elif _COMPANY_RE.search(head) or (kind == "is" and seen_cfs_is):
                fs = "ofs"
            elif label_cfs or r_ctrl is not None or r_nci is not None:
                fs = "cfs"
            else:
                fs = "cfs" if "연결" in section.replace(" ", "") else "ofs"
        elif kind == "is" and fs == "cfs":
            seen_cfs_is = True
        if fs == "ofs":
            r_ctrl = r_nci = None
        yield i, g, labels, hdr, ctx_list, own, kind, fs, r_ni, (r_ctrl, r_nci)


def _doc_kisu(blocks: list[Block], pend: date) -> int | None:
    """기간이 날짜로 확인되는 표의 당기 머리 '제 N 기' — 표지 기수는 회사가 잘못 쓰기도 한다(034810 2016Q1 표지 53기, 표 63기)."""
    for i, g, _labels, hdr, ctx_list, _own, _kind, _fs, _r, _c in _iter_ni_tables(blocks):
        heads = " ".join(g[r][1].text for r in range(hdr) if len(g[r]) > 1 and g[r][1] is not None)
        rng = date_ranges(heads) or next((r for r in (date_ranges(t) for t in reversed(ctx_list)) if r), [])
        m = _KISU_RE.search(heads)
        if rng and (rng[0][1].year, rng[0][1].month) == (pend.year, pend.month) and m:
            return int(m.group(1))
    return None


def extract_ni_candidates(
    blocks: list[Block], pend: date, q: int, *, kisu: int | None = None, file: str = "", main: bool = True,
) -> list[NICandidate]:
    """문서의 손익계산서·현금흐름표·요약재무정보 표에서 현재 기간 순이익 후보. 순서 = 문서 순서.

    ``kisu`` 는 날짜가 없는 표(요약재무정보)의 기간 확인용 — 날짜가 있는 표에서 읽은 기수를
    먼저 쓰고(:func:`_doc_kisu`), 없을 때만 넘겨받은 표지 기수를 쓴다.
    """
    out: list[NICandidate] = []
    kisu = _doc_kisu(blocks, pend) or kisu
    span = financial_section(blocks) if main else None
    for _i, g, labels, hdr, ctx_list, own, kind, fs, r_ni, (r_ctrl, r_nci) in _iter_ni_tables(blocks, span):
        ctx = " ".join(ctx_list)
        if kind == "div":
            mu = re.search(r"\(\s*(백\s*만\s*원|천\s*원|억\s*원|원)\s*\)", labels[r_ni])
            unit = _UNIT_MULT.get(mu.group(1).replace(" ", "")) if mu else None
        else:
            unit = unit_multiplier(own) or unit_multiplier(" ".join(ctx_list[-3:])) or unit_multiplier(ctx)
        if unit is None:
            continue
        c3, cc, flags = _current_columns(g, hdr, ctx_list, pend, q, kisu)
        if "unverified_period" in flags:  # 기간을 확인 못 한 표는 쓰지 않는다(틀린 값보다 없는 값)
            continue
        if kind in ("is", "cf") and not any(f.startswith("verified") for f in flags):
            # 본표는 날짜·기수로 확인돼야 한다. '당분기|전기' 머리만 있는 표는 주석(이익잉여금 변동 등)이다(193250 2016Q3).
            continue
        if kind in ("cf", "div"):  # 현금흐름표·배당표는 누적이고 귀속 행이 없다
            cc = cc or c3
            c3 = cc if q in (1, 4) else []
        if not c3 and not cc:
            continue
        sign = -1 if _loss_only(labels[r_ni]) else 1
        nci_before = r_nci is None and any(
            any(w in norm_label(labels[j]) for w in ("소수주주", "소수지분", "비지배", "외부주주")) and "포괄" not in labels[j]
            for j in range(max(0, r_ni - 4), r_ni))

        def val(r: int | None, c: list[int], s: int = 1) -> float | None:
            if r is None:
                return None
            v = _cell_num(g, r, c)
            if v is None:
                return None
            return (-abs(v) if s < 0 or (r != r_ni and _loss_only(labels[r])) else v) * unit

        def ctrl(cols: list[int], fl: list[str] = flags) -> float | None:
            """지배지분은 총 순이익과 **같은 표**의 귀속 행에서만, ctrl + 비지배 ≈ 총 순이익일 때만."""
            tot = val(r_ni, cols, sign)
            if kind != "is" or fs != "cfs" or tot is None:
                return None
            nci = val(r_nci, cols)  # '-'·빈칸 = 0
            if r_ctrl is None:
                if not nci:  # 귀속·비지배 행이 둘 다 없거나 비지배가 '-' — 전부 지배지분
                    # 옛 K-GAAP 연결 양식은 '소수주주지분순이익' 을 먼저 빼고 당기순이익을 낸다 — 합계가 곧 지배지분
                    fl.append("ctrl_eq_total_nci_deducted" if nci_before else "ctrl_eq_total")
                    return tot
                fl.append("ctrl_missing")
                return None
            c = val(r_ctrl, cols)
            if c is None:
                fl.append("ctrl_missing")
                return None
            if abs(c + (nci or 0) - tot) > max(0.005 * abs(tot), 2 * unit):
                fl.append("ctrl_inconsistent")  # 011700 2015Q2·Q3 — 지배+비지배가 합계와 안 맞는다
                return None
            return c

        cand = NICandidate(
            fs=fs, kind=kind, unit=unit, file=file, flags=flags,
            total_3m=val(r_ni, c3, sign), total_cum=val(r_ni, cc, sign),
            ctrl_3m=ctrl(c3) if c3 else None, ctrl_cum=ctrl(cc) if cc else None,
        )
        if kind == "is" and (c3 or cc):
            pcols, note = _prior_columns(g, hdr, ctx_list, pend, q, max(c3 + cc))
            if pcols:
                cand.total_prior = val(r_ni, pcols, sign)
                cand.ctrl_prior = ctrl(pcols, [])  # 비교 열 사정은 당기 ctrl 표시에 섞지 않는다
            cand.prior_note = note
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
    #: 전년 동기(Q1~Q3 3개월, Q4 전년 연간) — 그 분기 값을 낸 **같은 표**의 비교 열. 없으면 None, 사정은 prior_note
    cfs_prior: float | None = None
    ctrl_prior: float | None = None
    ofs_prior: float | None = None
    prior_note: dict[str, str] = field(default_factory=dict)


_KIND_RANK = {"is": 0, "cf": 1, "summary": 2}
_SCALES = (1e3, 1e6, 1e-3, 1e-6)


def _reconcile_units(cands: list[NICandidate]) -> None:
    """본표(손익·현금흐름)의 단위 표기가 틀린 경우를 같은 보고서의 요약재무정보·배당표와 대조해 바로잡는다.

    048410 2012Q1: 손익계산서 머리는 '(단위 : 백만원)' 인데 숫자는 원(-1,323,705,715) — 그대로 곱하면 -1,323조.
    같은 보고서 배당표는 '-1,324 백만원'. 본표 값이 상한을 넘고, 누적값 ÷ 대조값이 정확히 10^±3·10^±6(±2%)이며
    ×1 로 맞는 대조값이 하나도 없을 때만 고친다(``unit_rescaled``). 상한 안에서 10^k 로 어긋나면
    ``unit_conflict`` — :func:`choose_net_income` 이 그 기준을 버린다(틀린 값보다 없는 값).
    """
    divs = [c.total_cum for c in cands if c.kind == "div" and c.total_cum]
    # 요약재무정보의 열 하나가 3개월인지 누적인지 모를 때(Q2·Q3): 배당표 누적과 맞으면 누적, 어긋나면 버린다.
    # 069080 2010Q3 요약표 '제11기 3분기 317' 은 3개월 값(누적 4,287 — 배당표·손익계산서).
    for c in cands:
        if c.kind == "summary" and "single_col_assumed_cum" in c.flags and c.total_cum and divs:
            if not any(abs(c.total_cum / d - 1) < 0.01 for d in divs):
                c.total_cum = c.ctrl_cum = None
                c.flags.append("summary_ambiguous_dropped")
    sums = [c.total_cum for c in cands if c.kind == "summary" and c.total_cum and abs(c.total_cum) <= MAX_ABS_NI]
    divs = [d for d in divs if abs(d) <= MAX_ABS_NI]
    if not sums and not divs:
        return
    for c in cands:
        if c.kind not in ("is", "cf") or not c.total_cum:
            continue
        if any(abs(c.total_cum / r - 1) < 0.02 for r in sums + divs):
            continue

        def scale(refs: list[float], v: float = c.total_cum) -> float | None:
            return next((k for k in _SCALES for r in refs if abs(v / r / k - 1) < 0.02), None)

        # 본표 값이 상한(1e14 원)을 넘어 그 자체로 말이 안 될 때만 대조값 쪽 단위로 고친다. 상한 안이면
        # 어느 쪽이 틀렸는지 모른다 — 배당표도 틀린다(123750 2015Q1: '(백만원)' 이라 쓰고 천원으로 적어
        # 본표를 1,000배로 키울 뻔했다), 요약표도 틀린다(093370 2012Q1). 그러면 그 기준을 통째로 버린다.
        if abs(c.total_cum) > MAX_ABS_NI:
            k = scale(divs) or scale(sums)
        else:
            k = None
            if scale(sums):
                c.flags.append("unit_conflict")
            elif scale(divs):  # 손으로 치는 배당표보다 검토받은 본표를 믿는다 — 표시만
                c.flags.append("div_unit_mismatch")
        if k is None:
            continue
        for attr in ("total_3m", "total_cum", "ctrl_3m", "ctrl_cum", "total_prior", "ctrl_prior"):
            v = getattr(c, attr)
            if v is not None:
                setattr(c, attr, v / k)
        c.unit = c.unit / k
        c.flags.append("unit_rescaled")


def choose_net_income(cands: list[NICandidate], *, q_is_cum: bool = False) -> NetIncome:
    """후보 → 연결/별도 각각. 3개월은 손익계산서에서만, 누적은 손익 > 현금흐름 > 요약(백만원 반올림) 순.

    원 단위로 1e14 를 넘는 후보는 단위 오류로 버린다(``*_scale_reject``).
    """
    ni = NetIncome()
    _reconcile_units(cands)
    if any("unit_rescaled" in c.flags for c in cands):
        ni.flags.append("unit_rescaled")
    for fs in ("cfs", "ofs"):
        pool = [c for c in cands if c.fs == fs and c.kind != "div"]
        if any("unit_conflict" in c.flags for c in pool):
            ni.flags.append(f"{fs}_unit_conflict")
            continue
        ok = [c for c in pool if all(v is None or abs(v) <= MAX_ABS_NI for v in (c.total_3m, c.total_cum, c.ctrl_3m, c.ctrl_cum))]
        if len(ok) < len(pool):
            ni.flags.append(f"{fs}_scale_reject")
        # 같은 종류면 날짜로 기간이 확인된 표, 그다음 작은 단위(원 > 천원 — 요약 절의 천원 단위 손익표보다 본표)
        ok.sort(key=lambda c: (_KIND_RANK[c.kind], "verified_range" not in c.flags, c.unit))
        if not ok:
            continue

        def first(attr: str, kinds: tuple[str, ...] = ("is", "cf", "summary"), pool: list[NICandidate] = ok) -> float | None:
            return next((getattr(c, attr) for c in pool if c.kind in kinds and getattr(c, attr) is not None), None)

        # 지배지분은 합계를 고른 **그 표**의 값만 쓴다 — 다른 표(요약재무정보)에서 빌려오지 않는다(099830·115960)
        c3 = next((c for c in ok if c.kind == "is" and c.total_3m is not None), None)
        cc = next((c for c in ok if c.total_cum is not None), None)
        t3, tc = (c3.total_3m if c3 else None), (cc.total_cum if cc else None)
        if fs == "cfs":
            ni.cfs_3m, ni.cfs_cum = t3, tc
            ni.ctrl_3m = c3.ctrl_3m if c3 else None
            ni.ctrl_cum = cc.ctrl_cum if cc and cc.kind == "is" else None
            for c in (c3, cc):
                ni.flags += [f"cfs_{f}" for f in dict.fromkeys(c.flags if c else []) if f.startswith("ctrl_") and f"cfs_{f}" not in ni.flags]
        else:
            ni.ofs_3m, ni.ofs_cum = t3, tc
        # 전년 동기: 그 분기 값(Q1~Q3 3개월 열, Q4 연간)을 낸 손익계산서의 비교 열
        pc = c3 if c3 is not None else (cc if cc is not None and cc.kind == "is" and q_is_cum else None)
        if pc is None:
            ni.prior_note[fs] = "no_printed_quarter"  # 3개월 값을 차분으로 만든 분기 — 비교 열도 쓰지 않는다
        else:
            def sane(v: float | None) -> float | None:
                return None if v is not None and abs(v) > MAX_ABS_NI else v

            setattr(ni, f"{fs}_prior", sane(pc.total_prior))
            if fs == "cfs":
                ni.ctrl_prior = sane(pc.ctrl_prior)
            if pc.prior_note:
                ni.prior_note[fs] = pc.prior_note
            elif pc.total_prior is not None and sane(pc.total_prior) is None:
                ni.prior_note[fs] = "prior_scale_reject"
        best = ok[0]
        ni.src[fs] = f"{best.kind}:{best.file}"
        if cc:
            ni.src[f"{fs}_cum"] = cc.kind
        ni.flags += [f"{fs}_{f}" for f in dict.fromkeys(best.flags) if not f.startswith(("verified", "ctrl_"))]
    return ni


# --------------------------------------------------------------------------- 주식의 총수


@dataclass
class Shares:
    common: int | None
    preferred: int | None
    total: int | None
    asof: date | None
    method: str
    #: 표·표 앞에서 '…현재' 기준일을 못 찾았다 — 분기말로 채우지 않는다(읽는 쪽이 정한다)
    asof_inferred: bool = False


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
        m = _ASOF_RE.search(ctx) or _BASEDATE_RE.search(ctx)
        if m:
            asof = _mkdate(*m.group(1, 2, 3))
        return Shares(common, pref, total, asof, method, asof_inferred=asof is None)
    return _shares_from_voting_table(blocks, pend)


def _shares_from_voting_table(blocks: list[Block], pend: date | None) -> Shares | None:
    """대체 경로: '5. 의결권 현황' 표의 '발행주식총수(A) | 보통주 | N' 행.

    2010 무렵 분기보고서는 '가. 주식의 총수' 를 문장으로만 쓰고 표를 두지 않았다(002230 2010Q1 등
    개발 표본 2010 분기의 1/4). 의결권 현황 표는 같은 기준일의 발행주식총수를 종류별로 준다.
    """
    for i, b in enumerate(blocks):
        if b.kind != "table":
            continue
        g = grid(b.table)
        vals: dict[str, int] = {}
        for line in g:
            if not line or line[0] is None or "발행주식총수" not in norm_label(line[0].text):
                continue
            kind = next((c.text.replace(" ", "") for c in line[1:] if c is not None and c.text.replace(" ", "") in ("보통주", "우선주", "종류주식")), None)
            num = next((parse_number(c.text) for c in line[1:] if c is not None and parse_number(c.text) is not None), None)
            if kind and kind not in vals:
                vals[kind] = int(num or 0)
        if "보통주" not in vals:
            continue
        common = vals["보통주"]
        pref = sum(v for k, v in vals.items() if k != "보통주")
        ctx = " ".join(_context_text(blocks, i, 3)) + " " + " ".join(c.text for r in b.table.rows[:1] for c in r)
        m = _ASOF_RE.search(ctx) or _BASEDATE_RE.search(ctx) or re.search(_DATE, ctx)
        asof = _mkdate(*m.group(1, 2, 3)) if m else None
        return Shares(common, pref, common + pref, asof, "voting", asof_inferred=asof is None)
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
    is_correction: bool = False
    #: 누적 시작일 — 표지 '사업연도 2011년 05월 17일 부터 …'. 보통 1월 1일, 설립 첫해는 설립일(139130 DGB 2011)
    ytd_start: date | None = None

    @property
    def knowledge_date(self) -> str:
        """접수일 = 접수번호 앞 8자리 → 'YYYY-MM-DD'(``earnings.knowledge_date`` 와 같은 뜻)."""
        r = self.rcept_no
        return f"{r[:4]}-{r[4:6]}-{r[6:8]}"


_ERRATA_RE = re.compile(r"정\s*정\s*신\s*고")
_COVER_RE = re.compile(r"^\s*(?:사\s*업|반\s*기|분\s*기)\s*보\s*고\s*서\s*$")


def report_body_start(blocks: list[Block]) -> int:
    """정정 공시 앞머리의 정오표('정정신고(보고)' — 정정 전·후 표가 나란히)를 건너뛴 본문 시작 블록.

    122690 2015 사업보고서 정정본: 정오표의 '정정 전' 연결 손익계산서(순이익 6,022,683,114)가 본문보다 앞에
    있어, 첫 표를 쓰면 정정본 레코드에 **정정 전 값**이 들어갔다. 정오표 뒤 표지('사 업 보 고 서')부터 읽는다.
    """
    if not any(b.kind == "title" and _ERRATA_RE.search(b.text) for b in blocks[:6]):
        return 0
    return next((i for i, b in enumerate(blocks) if i > 1 and b.kind == "text" and _COVER_RE.match(b.text)), 0)


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
    ytd_start = None
    for k, (name, xml) in enumerate(files):
        blocks = parse_blocks(xml)
        if k == 0:
            blocks = blocks[report_body_start(blocks):]
            kisu = cover_kisu(blocks)
            ytd_start = cover_period_start(blocks, pend)
            if ytd_start is None or ytd_start.year != year or (ytd_start == date(year, 3 * q - 2, 1) and kisu != 1):
                # 표지가 그 분기 3개월만 적은 보고서(K-GAAP 3분기 일부)는 1월 1일 누적과 같다.
                # 제1기(설립 첫해)는 분기 첫날 설립도 있다(175330 JB금융지주 2013-07-01) — 그대로 둔다
                ytd_start = date(year, 1, 1)
            shares = extract_shares(blocks, pend)
        main_has = {c.fs for c in cands if c.kind == "is"}
        if k > 0 and {"cfs", "ofs"} <= main_has:
            break
        got = extract_ni_candidates(blocks, pend, q, kisu=kisu if k == 0 else None,
                                    file=name.rsplit("/", 1)[-1], main=k == 0)
        if k > 0:  # 첨부(감사·검토보고서)는 본문에 손익계산서가 없는 기준만 채운다
            got = [c for c in got if c.kind in ("is", "cf") and c.fs not in main_has]
        cands += got
    return ReportResult(rcept_no, year, q, choose_net_income(cands, q_is_cum=q in (1, 4)), shares, kisu,
                        ytd_start=ytd_start)


def basis_order(ni: NetIncome, *, controlling_first: bool) -> tuple[str, ...]:
    """순이익 기준 우선순위.

    ``controlling_first=False`` 는 ``earnings`` 와 같은 정의다: 연결 합계 → 별도. 다만 연결이 **요약표에만**
    있고 별도는 손익계산서 본표가 있으면 별도를 먼저 쓴다 — ``earnings`` 의 원천(XBRL 주요계정)은 그
    보고서에 실린 본표를 따르므로, 본표 없는 연결은 API 에도 없다(001550·008730 2015, 2026-10-13 대조).
    """
    if controlling_first:
        return ("ctrl", "cfs", "ofs")

    def stmt(fs: str) -> bool:
        return ni.src.get(fs, "").split(":")[0] in ("is", "cf")

    return tuple([b for b in ("cfs", "ofs") if stmt(b)] + [b for b in ("cfs", "ofs") if not stmt(b)])


def assemble_quarters(reports: dict[str, ReportResult], *, controlling_first: bool) -> dict[str, tuple[float | None, str | None, list[str]]]:
    """``{'2009Q1': ReportResult, ...}`` → ``{period: (netinc, basis, flags)}`` — ``earnings`` 기간 의미.

    Q1·Q4 = 누적(Q4 = 연간). Q2·Q3 = 기준 우선순위(:func:`basis_order`)마다 3개월 열, 없으면 **같은 기준의**
    누적 − 직전 분기 누적(``derived``) — 상위 기준을 차분으로라도 만들 수 있으면 하위 기준으로 내려가지
    않는다. 연결 반기 누적 − 별도 1분기 같은 혼합은 하지 않는다.
    """
    out: dict[str, tuple[float | None, str | None, list[str]]] = {}
    for period, rr in reports.items():
        y, q = rr.year, rr.q
        res: tuple[float | None, str | None, list[str]] = (None, None, ["no_value"] + rr.ni.flags[:1])
        prev = reports.get(f"{y}Q{q - 1}")
        for b in basis_order(rr.ni, controlling_first=controlling_first):
            v3, vc = getattr(rr.ni, f"{b}_3m"), getattr(rr.ni, f"{b}_cum")
            if q in (1, 4):
                v = vc if vc is not None else (v3 if q == 1 else None)
                if v is not None:
                    res = (v, b, [])
                    break
                continue
            if v3 is not None:
                res = (v3, b, [])
                break
            pv = getattr(prev.ni, f"{b}_cum") if prev is not None else None
            if vc is not None and pv is not None:
                res = (vc - pv, b, ["derived"])
                break
        out[period] = res
    return out


# --------------------------------------------------------------------------- 검증 CLI (네트워크·DB)


# --------------------------------------------------------------------------- 상장폐지 종목 → DART 고유번호
# corpCode.xml 의 stock_code 는 지금 상장된 회사에만 채워져 있다 — 폐지 종목(delisted_stocks)은 그 열로 21%만
# 이어진다(2026-10-13 리뷰 실측 781/3,717). 나머지는 stock_code 가 빈 법인 중 이름이 같은 곳을 찾고, 그 법인의
# list.json 에 거래하던 해의 정기보고서가 있는지로 확인한다. 이름이 같은 법인이 둘 이상이면 버린다(동명이인).

_CORP_NOISE_RE = re.compile(r"\(주\)|\(유\)|\(株\)|㈜|주식회사|유한회사|[\s·.,&\-]")


def normalize_corp_name(name: str) -> str:
    """'(주)삼성테크윈'·'삼성테크윈 주식회사'·'삼성테크윈㈜' → '삼성테크윈'. 전각·대소문자도 맞춘다."""
    return _CORP_NOISE_RE.sub("", unicodedata.normalize("NFKC", name or "")).upper()


def corp_name_index(corp_rows: list[dict]) -> dict[str, list[dict]]:
    idx: dict[str, list[dict]] = {}
    for r in corp_rows:
        idx.setdefault(normalize_corp_name(r.get("corp_name", "")), []).append(r)
    return idx


def name_candidates(code: str, names: list[str], index: dict[str, list[dict]]) -> list[str]:
    """이름이 같고 stock_code 가 비었거나 같은 코드인 법인의 고유번호들(다른 상장 코드가 붙은 법인은 남이다)."""
    out: list[str] = []
    for n in names:
        for r in index.get(normalize_corp_name(n), []):
            if r.get("stock_code", "") in ("", code) and r["corp_code"] not in out:
                out.append(r["corp_code"])
    return out


_ANY_REPORT_RE = re.compile(r"^(?:\[[^\]]*\]\s*)?(?:분기보고서|반기보고서|사업보고서)\s*\((\d{4})\.(\d{2})\)")


def periodic_years(items: list[dict], *, dec_only: bool) -> set[int]:
    """list.json 항목 → 정기보고서가 있는 사업연도. ``dec_only`` 면 이 파서가 다루는 12월 결산 분기만."""
    if dec_only:
        return {int(p[:4]) for p in select_filings(items)}
    return {int(m.group(1)) for it in items if (m := _ANY_REPORT_RE.match(it.get("report_nm", "").strip()))}


def _confirms(code: str, items: list[dict], traded_years: set[int]) -> bool:
    if any((it.get("stock_code") or "").strip() == code for it in items):
        return True
    yrs = periodic_years(items, dec_only=False)
    return bool(yrs) and (not traded_years or bool(yrs & (traded_years | {y - 1 for y in traded_years})))


def resolve_delisted(
    code: str, candidates: list[str], items_by_corp: dict[str, list[dict]], traded_years: set[int],
) -> tuple[str | None, str]:
    """후보 고유번호 → ``(corp_code 또는 None, 상태)``.

    후보마다 list.json 에 거래하던 해(또는 그 전해 사업연도)의 정기보고서가 있는지 본다. 확인되는 후보가
    정확히 하나여야 매핑한다 — 후보가 하나면 ``confirmed``, 여럿 중 하나만 확인되면 ``confirmed_among_n``.
    확인 후보가 둘 이상이면 ``ambiguous``(동명이인, 버린다), 하나도 없으면 ``unconfirmed``.
    거래 연도를 모르면(``traded_years`` 비어 있음) 정기보고서가 하나라도 있으면 확인으로 본다.
    """
    if not candidates:
        return None, "no_name_match"
    ok = [c for c in candidates if _confirms(code, items_by_corp.get(c, []), traded_years)]
    if len(ok) > 1:
        return None, "ambiguous"
    if not ok:
        return None, "unconfirmed"
    return ok[0], ("confirmed" if len(candidates) == 1 else f"confirmed_among_{len(candidates)}")


_CLASS_SHARE_RE = re.compile(r"(\d*우[A-Z]?|\(\d*우[A-Z]?\)|\d*신|\(\d*신\)|전환)$")


def parent_common_code(code: str, name: str) -> str | None:
    """우선주·신주 코드('012655 쌍용건설우', '023601 삼보판지1신') → 보통주 코드('012650'). 아니면 ``None``.

    우선주·신주는 따로 실적을 내는 회사가 아니다 — 보통주 회사의 고유번호를 쓴다.
    """
    if len(code) == 6 and code.isdigit() and code[-1] != "0" and _CLASS_SHARE_RE.search((name or "").replace(" ", "")):
        return code[:5] + "0"
    return None


def agrees(value: float | None, ref: float | None, tol: float = 0.005) -> bool:
    """검증 합격 정의(사용자 승인): 둘 다 있고, 같거나 상대오차 0.5% 이내."""
    if value is None or ref is None:
        return False
    return value == ref or abs(value - ref) <= tol * abs(ref)


# --------------------------------------------------------------------------- 레코드(적재 단위)

BASES = ("ctrl", "cfs", "ofs")


def _basis_values(rr: ReportResult, prev: ReportResult | None, b: str) -> dict:
    """한 기준의 ``{q, q_derived, ytd}``. 3개월이 없으면 **같은 기준**의 누적 − 직전 분기 누적.

    기준이 다른 값끼리는 빼지 않는다 — 직전 분기에 그 기준 누적이 없으면 ``q`` 는 None 이다.
    Q4 의 ``q`` 는 사업보고서에 3개월 열이 없으므로 연간 − 3분기 누적(같은 기준)이다.
    """
    v3, vc = getattr(rr.ni, f"{b}_3m"), getattr(rr.ni, f"{b}_cum")
    qstart = date(rr.year, 3 * rr.q - 2, 1)
    if rr.q == 1 or (rr.ytd_start is not None and rr.ytd_start >= qstart):
        # 1분기, 또는 이 분기 안에 설립돼 누적 = 이 분기 전부(139130 DGB 2011 반기: 05-17~06-30)
        v = vc if vc is not None else v3
        return {"q": v3 if v3 is not None else v, "q_derived": False, "ytd": v}
    if rr.q in (2, 3) and v3 is not None:
        return {"q": v3, "q_derived": False, "ytd": vc}
    pv = getattr(prev.ni, f"{b}_cum") if prev is not None else None
    # 차분은 직전 분기 누적과 누적 시작일이 같을 때만(설립 첫해·결산기 변경으로 시작일이 다르면 만들지 않는다)
    if vc is not None and pv is not None and prev.ytd_start == rr.ytd_start:
        return {"q": vc - pv, "q_derived": True, "ytd": vc}
    return {"q": None, "q_derived": False, "ytd": vc}


def build_records(code: str, reports: dict[str, list[ReportResult]]) -> list[dict]:
    """``{period: [원본, 정정, ...]}`` → 적재 레코드(``earnings_dart_doc`` 한 행 = 한 레코드).

    - 레코드 = 공시 한 건. ``knowledge_date`` = 그 공시 접수일 — 정정은 정정본 접수일로 **따로** 낸다
      (한 레코드에 원본 날짜와 정정 값이 같이 있지 않다).
    - Q2·Q3(·Q4 의 ``*_q``)를 차분으로 만들 때 직전 분기는 **그 공시 접수일까지 알려진** 마지막 판을 쓴다(PIT).
    - 기준마다(``ctrl``·``cfs``·``ofs``) 3개월(``*_q``)·누적(``*_ytd``)·``*_q_derived`` 를 다 낸다. Q4 의
      ``*_ytd`` 가 사업연도 전체(별도 연간 ``ofs_ytd`` 포함).
    - ``netinc``/``basis`` 는 ``earnings`` 정의(연결 합계 → 별도, :func:`basis_order`), ``netinc_ctrl``/
      ``basis_ctrl`` 는 지배지분 우선. 둘 다 그 분기 값(Q1~Q3 3개월, Q4 연간)이다.
    - PK (code, period, knowledge_date) — 같은 날 두 판이면 늦은 접수번호만 남긴다. 순이익 값이 하나도 없는
      판(정정본, 또는 주식수도 없는 원본)은 내지 않는다 — PK 경쟁에도 끼지 않는다.
    """
    out: dict[tuple[str, str], dict] = {}
    for period, versions in reports.items():
        y, q = int(period[:4]), int(period[-1])
        prev_versions = sorted(reports.get(f"{y}Q{q - 1}", []), key=lambda r: r.rcept_no) if q > 1 else []
        for rr in sorted(versions, key=lambda r: r.rcept_no):
            prev = next((p for p in reversed(prev_versions) if p.rcept_no <= rr.rcept_no), None)
            vals = {b: _basis_values(rr, prev, b) for b in BASES}
            rec: dict = {"code": code, "period": period, "knowledge_date": rr.knowledge_date,
                         "rcept_no": rr.rcept_no, "is_correction": rr.is_correction,
                         # 누적 시작일이 1월 1일이 아니면 기록(설립 첫해 등) — *_ytd 는 그날부터의 누적이다
                         "ytd_start": (rr.ytd_start.isoformat() if rr.ytd_start and rr.ytd_start != date(y, 1, 1) else None)}
            for b in BASES:
                rec[f"{b}_q"], rec[f"{b}_ytd"], rec[f"{b}_q_derived"] = vals[b]["q"], vals[b]["ytd"], vals[b]["q_derived"]
            for tag, ctrl_first in (("", False), ("_ctrl", True)):
                pick = None
                for b in basis_order(rr.ni, controlling_first=ctrl_first):
                    v = vals[b]["ytd"] if q == 4 else vals[b]["q"]
                    if v is not None:
                        pick = (v, b, q != 4 and vals[b]["q_derived"])
                        break
                rec[f"netinc{tag}"], rec[f"basis{tag}"], rec[f"derived{tag}"] = pick or (None, None, False)
            # 전년 동기 — netinc·netinc_ctrl 과 같은 기준의 비교 열. 그 분기 값이 차분이면 비교 열도 없다(NULL)
            for b in BASES:
                rec[f"{b}_prior_q"] = getattr(rr.ni, f"{b}_prior")
            notes = []
            for tag, bkey, dkey in (("", "basis", "derived"), ("_ctrl", "basis_ctrl", "derived_ctrl")):
                b = rec[bkey]
                rec[f"netinc{tag}_prior"] = None if b is None or rec[dkey] else getattr(rr.ni, f"{b}_prior")
                if b is not None and rec[f"netinc{tag}_prior"] is None:
                    why = "derived" if rec[dkey] else rr.ni.prior_note.get("cfs" if b == "ctrl" else b, "no_prior")
                    notes.append(f"{bkey}={b}:{why}")
            rec["prior_basis_note"] = ";".join(dict.fromkeys(notes)) or None
            rec["src"] = rr.ni.src
            rec["flags"] = rr.ni.flags
            sh = rr.shares
            rec["shares"] = None if sh is None else {
                "common": sh.common, "preferred": sh.preferred, "total": sh.total,
                "asof": sh.asof.isoformat() if sh.asof else None, "asof_inferred": sh.asof_inferred, "method": sh.method}
            # 값이 하나도 없는 판은 내지 않는다 — 정정본이면 원본 값을 NULL 로 가리고, 같은 날 원본과 PK 를
            # 다투어 원본을 떨어뜨린다(2026-10-13 재리뷰). 원본은 주식수만 있어도 낸다.
            has_ni = any(rec[k] is not None for k in ("netinc", "netinc_ctrl", *(f"{b}_{w}" for b in BASES for w in ("q", "ytd"))))
            if not has_ni and (rr.is_correction or rec["shares"] is None):
                continue
            key = (period, rec["knowledge_date"])
            if key not in out or out[key]["rcept_no"] < rec["rcept_no"]:
                out[key] = rec
    return sorted(out.values(), key=lambda r: (r["period"], r["knowledge_date"]))
