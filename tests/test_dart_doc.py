"""DART 원문 파서(dart_doc) — 합성 XML 조각으로만(네트워크·DB 불요).

실제 원문에서 겪은 모양을 줄여 옮겼다: K-GAAP 분기보고서의 직전 연도 연결표, 머리가 두 칸에 걸친
3개월 열, 단위를 '백만원' 이라 쓰고 원으로 적은 표, 최대주주의 요약연결재무정보, 6월 결산사의
'분기보고서 (2015.09)', 2010 분기보고서의 의결권 현황 표.
"""

from __future__ import annotations

import io
import zipfile
from datetime import date

import pytest

import collectors.dart_doc as dd


def _row(*cells: str, tag: str = "TD") -> str:
    return "<TR>" + "".join(c if c.startswith("<T") else f"<{tag}>{c}</{tag}>" for c in cells) + "</TR>"


def _table(*rows: str) -> str:
    return "<TABLE>" + "".join(rows) + "</TABLE>"


def _doc(*parts: str, cover: str = "(제 10 기 1 분기)") -> str:
    return f"<DOCUMENT><BODY><P>분 기 보 고 서</P><P>{cover}</P>{''.join(parts)}</BODY></DOCUMENT>"


def _is_table(title: str, rng: str, unit: str, ni: tuple[str, ...], *, ctrl: tuple[str, ...] | None = None,
              head: str = "제 10 기 반기", prev: str = "제 9 기 반기") -> str:
    rows = [
        _row("", f'<TH COLSPAN="2">{head}</TH>', f'<TH COLSPAN="2">{prev}</TH>', tag="TH"),
        _row("", "3개월", "누적", "3개월", "누적", tag="TH"),
        _row("매출액", "1,000", "2,000", "900", "1,800"),
        _row("영업이익", "100", "200", "90", "180"),
        _row("법인세비용", "10", "20", "9", "18"),
        _row("당기순이익(손실)", *ni),
    ]
    if ctrl:
        rows += [_row("당기순이익의 귀속", "", "", "", ""), _row(" 지배기업의 소유주에게 귀속되는 당기순이익", *ctrl),
                 _row(" 비지배지분에 귀속되는 당기순이익", "1", "2", "1", "2")]
    return f"<P>{title}</P>" + _table(_row(rng), _row(unit)) + _table(*rows)


# ---------------------------------------------------------------- 기본 조각


def test_decode_falls_back_to_cp949():
    """XML 선언은 utf-8 이지만 옛 보고서는 cp949(2026-10-13 실측)."""
    raw = "당기순이익".encode("cp949")
    assert dd.decode_document(raw) == "당기순이익"
    assert dd.decode_document("당기순이익".encode()) == "당기순이익"


@pytest.mark.parametrize(("text", "want"), [
    ("1,234", 1234.0), ("(1,234)", -1234.0), ("△1,234", -1234.0), ("-1,234", -1234.0),
    ("(-)4,618", -4618.0), ("12,345원", 12345.0), ("-", None), ("", None), ("2016.01.01", None), ("제 10 기", None),
])
def test_parse_number(text, want):
    assert dd.parse_number(text) == want


@pytest.mark.parametrize(("text", "want"), [
    ("(단위 : 백만원)", 1_000_000), ("(단위: 천원)", 1_000), ("단위 : 원", 1), ("(단위 : 억원)", 100_000_000),
    ("(단위 : USD)", None), ("(단위 : 천달러)", None), ("단위 없음", None),
])
def test_unit_multiplier(text, want):
    assert dd.unit_multiplier(text) == want


def test_period_from_report_nm_only_december_quarters():
    assert dd.period_from_report_nm("반기보고서 (2009.06)") == (2009, 2)
    assert dd.period_from_report_nm("[기재정정]사업보고서 (2014.12)") == (2014, 4)
    assert dd.period_from_report_nm("분기보고서 (2009.06)") is None  # 3월 결산 1분기
    assert dd.period_from_report_nm("주요사항보고서(자기주식취득결정)") is None


def test_select_filings_prefers_original_and_lists_corrections():
    items = [
        {"rcept_no": "20160517000128", "report_nm": "[기재정정]분기보고서 (2016.03)"},
        {"rcept_no": "20160516003860", "report_nm": "분기보고서 (2016.03)"},
        {"rcept_no": "20160330000265", "report_nm": "[첨부추가]사업보고서 (2015.12)"},
    ]
    got = dd.select_filings(items)
    assert got["2016Q1"]["rcept_no"] == "20160516003860"
    assert got["2016Q1"]["rcept_dt"] == "20160516"
    assert got["2016Q1"]["corrections"] == ["20160517000128"]
    # 원본이 목록에 없고 정정만 있으면 가장 이른 정정을 쓰되 표시한다
    assert got["2015Q4"]["original_missing"] is True


def test_select_filings_drops_non_december_fye_years():
    """6월 결산사의 '분기보고서 (2015.09)' 는 회계 1분기 — 달력 3분기로 넣으면 안 된다(001080)."""
    items = [
        {"rcept_no": "20151116000359", "report_nm": "분기보고서 (2015.09)"},
        {"rcept_no": "20160516001628", "report_nm": "분기보고서 (2016.03)"},
        {"rcept_no": "20160928000126", "report_nm": "사업보고서 (2016.06)"},
        {"rcept_no": "20150930000168", "report_nm": "사업보고서 (2015.06)"},
    ]
    assert dd.select_filings(items) == {}


def test_parse_blocks_keeps_cells_titles_and_acode():
    xml = '<TITLE>4. 주식의 총수 등</TITLE><TABLE><TR><TD COLSPAN="2">A</TD><TE ACODE="ISU_STK3"> 1,000</TE></TR></TABLE>'
    blocks = dd.parse_blocks(xml)
    assert blocks[0].kind == "title" and blocks[0].text == "4. 주식의 총수 등"
    cells = blocks[1].table.rows[0]
    assert cells[0].colspan == 2 and cells[1].acode == "ISU_STK3" and cells[1].text == "1,000"


def test_grid_expands_rowspan_and_colspan():
    t = dd.Table([[dd.Cell("a", rowspan=2), dd.Cell("b", colspan=2)], [dd.Cell("c"), dd.Cell("d")]])
    g = dd.grid(t)
    assert [c.text for c in g[0]] == ["a", "b", "b"]
    assert [c.text for c in g[1]] == ["a", "c", "d"]


# ---------------------------------------------------------------- 순이익


def _report(xml: str, year: int, q: int) -> dd.ReportResult:
    return dd.parse_report([("x.xml", xml)], "r", year, q)


def test_quarterly_consolidated_and_separate_with_controlling():
    """IFRS 반기보고서 — 3개월/누적, 연결(지배 귀속 포함)·별도, 단위 백만원 → 원."""
    xml = _doc(
        "<TITLE>III. 재무에 관한 사항</TITLE>",
        _is_table("연결 포괄손익계산서", "제 10 기 반기 2016.01.01 부터 2016.06.30 까지", "(단위 : 백만원)",
                  ("5,847", "11,100", "5,752", "10,378"), ctrl=("5,826", "11,089", "5,626", "10,146")),
        _is_table("포괄손익계산서", "제 10 기 반기 2016.01.01 부터 2016.06.30 까지", "(단위 : 원)",
                  ("(1,500)", "2,000", "300", "600")),
        "<TITLE>IV. 감사인의 감사의견 등</TITLE>",
    )
    ni = _report(xml, 2016, 2).ni
    assert (ni.cfs_3m, ni.cfs_cum) == (5_847e6, 11_100e6)
    assert (ni.ctrl_3m, ni.ctrl_cum) == (5_826e6, 11_089e6)
    assert (ni.ofs_3m, ni.ofs_cum) == (-1_500.0, 2_000.0)


def test_kgaap_prior_year_consolidated_table_is_rejected():
    """K-GAAP 분기보고서의 '연결재무제표' 는 직전 사업연도 표 — 2007 연간이 2008Q1 로 들어가면 안 된다."""
    xml = _doc(
        _is_table("손익계산서", "제40기 1분기 2008년 01월 01일 부터 2008년 03월 31일 까지", "(단위 : 백만원)",
                  ("2,187", "2,187", "1,599", "1,599"), head="제40기 1분기", prev="제39기 1분기"),
        _is_table("연결손익계산서", "제 39 기 2007. 01. 01 부터 2007. 12. 31 까지", "(단위 : 백만원)",
                  ("7,922", "7,922", "8,193", "8,193"), head="제 39 기", prev="제 38 기"),
        cover="(제 40 기 1 분기)",
    )
    ni = _report(xml, 2008, 1).ni
    assert ni.ofs_cum == 2_187e6
    assert ni.cfs_cum is None and ni.cfs_3m is None


def test_header_spanning_two_cells_uses_the_numeric_one():
    """'3개월' 머리가 빈 칸·값 칸 두 칸에 걸친 표(203690 2015 반기)."""
    rows = [
        _row("과 목", '<TH COLSPAN="4">제2(당)반기</TH>', '<TH COLSPAN="4">제1(전)반기</TH>', tag="TH"),
        _row("과 목", '<TH COLSPAN="2">3 개 월</TH>', '<TH COLSPAN="2">누 적</TH>',
             '<TH COLSPAN="2">3 개 월</TH>', '<TH COLSPAN="2">누 적</TH>', tag="TH"),
        _row("Ⅰ. 영업수익", "", "-", "", "-", "", "-", "", "-"),
        _row("Ⅴ. 영업이익", "", "10", "", "20", "", "1", "", "2"),
        _row("Ⅷ. 법인세비용", "", "1", "", "2", "", "0", "", "0"),
        _row("X. 당기순이익", "", "52,001,652", "", "61,177,768", "", "(4,398)", "", "(4,398)"),
    ]
    xml = _doc("<P>반 기 포 괄 손 익 계 산 서</P>",
               _table(_row("제2(당)반기 2015년 01월 01일부터 2015년 06월 30일까지 (단위 : 원)")), _table(*rows))
    ni = _report(xml, 2015, 2).ni
    assert (ni.ofs_3m, ni.ofs_cum) == (52_001_652.0, 61_177_768.0)


def test_unit_mislabel_fixed_by_dividend_table():
    """손익계산서는 '(단위 : 백만원)' 인데 숫자는 원(048410 2012Q1) — 배당표 '-1,324 백만원' 과 10^6 차이."""
    div = _table(
        _row("구 분", "주식의 종류", "제13기 1분기", "제12기", "제11기", tag="TH"),
        _row("주당액면가액 (원)", "", "500", "500", "500"),
        _row('<TD COLSPAN="2">당기순이익 (백만원)</TD>', "-1,324", "-18,301", "-7,056"),
    )
    xml = _doc(
        "<P>나. 최근 3사업연도 배당에 관한 사항</P>", div,
        _is_table("포괄손익계산서", "제 13 기 1분기 2012.01.01 부터 2012.03.31 까지", "(단위 : 백만원)",
                  ("(1,323,705,715)", "(1,323,705,715)", "(1,276,794,115)", "(1,276,794,115)"),
                  head="제 13 기 1분기", prev="제 12 기 1분기"),
        cover="(제 13 기 1 분기)",
    )
    ni = _report(xml, 2012, 1).ni
    assert ni.ofs_cum == -1_323_705_715.0
    assert "unit_rescaled" in ni.flags


def test_unit_conflict_with_summary_only_drops_basis():
    """요약표와만 10^6 어긋나면 어느 쪽이 틀렸는지 모른다 — 값을 내지 않는다(틀린 값보다 없는 값)."""
    summary = _table(
        _row("과 목", "제 7 기 1분기", "제 6 기", tag="TH"),
        _row("자산총계", "100", "90"),
        _row("당기순이익", "286", "19,785"),
    )
    xml = _doc(
        "<TITLE>III. 재무에 관한 사항</TITLE><P>1. 요약 재무 정보</P><P>(단위 : 백만원)</P>", summary,
        _is_table("포괄손익계산서", "제 7 기 1분기 2012.01.01 부터 2012.03.31 까지", "(단위 : 천원)",
                  ("286,987,155", "286,987,155", "1", "1"), head="제 7 기 1분기", prev="제 6 기 1분기"),
        "<TITLE>IV. 감사인의 감사의견 등</TITLE>", cover="(제 7 기 1 분기)",
    )
    ni = _report(xml, 2012, 1).ni
    assert ni.ofs_cum is None and "ofs_unit_conflict" in ni.flags


def test_major_shareholder_summary_outside_financial_section_is_ignored():
    """'VI. 주주에 관한 사항' 의 최대주주 요약연결재무정보(001550 2015Q1 — 다른 회사 순이익 176억)."""
    other = _table(
        _row("구 분", "제59기 당분기", "제58기", tag="TH"),
        _row("", "2015년 3월말", "2014년 12월말", tag="TH"),
        _row("자산총계", "1", "1"),
        _row("당기순이익(손실)", "17,663,535", "17,836,951"),
    )
    xml = _doc(
        "<TITLE>III. 재무에 관한 사항</TITLE>",
        _is_table("포괄손익계산서", "제 61 기 1분기 2015.01.01 부터 2015.03.31 까지", "(단위 : 원)",
                  ("129,077,173", "129,077,173", "1", "1"), head="제 61 기 1분기", prev="제 60 기 1분기"),
        "<TITLE>IV. 감사인의 감사의견 등</TITLE><TITLE>VI. 주주에 관한 사항</TITLE>",
        "<P>7) 최대주주의 요약연결재무정보 (단위: 천원)</P>", other, cover="(제 61 기 1 분기)",
    )
    ni = _report(xml, 2015, 1).ni
    assert ni.ofs_cum == 129_077_173.0
    assert ni.cfs_cum is None


# ---------------------------------------------------------------- 분기 조립


def _rr(year: int, q: int, **kw: float | None) -> dd.ReportResult:
    src = kw.pop("src", None) or {}
    return dd.ReportResult("r", year, q, dd.NetIncome(src=src, **kw), None, None)


def test_assemble_q4_is_annual_and_q2_derived_from_same_basis():
    reports = {
        "2011Q1": _rr(2011, 1, cfs_3m=100.0, cfs_cum=100.0, ofs_3m=80.0, ofs_cum=80.0, src={"cfs": "is:x", "ofs": "is:x"}),
        "2011Q2": _rr(2011, 2, cfs_cum=250.0, ofs_3m=70.0, ofs_cum=150.0, src={"cfs": "summary:x", "ofs": "is:x"}),
        "2011Q4": _rr(2011, 4, cfs_3m=900.0, cfs_cum=900.0, src={"cfs": "is:x"}),
    }
    ctrl_first = dd.assemble_quarters(reports, controlling_first=True)
    # 지배지분이 없으면 연결 합계 — Q2 는 연결 누적 차분(250-100)이 별도 3개월(70)보다 먼저
    assert ctrl_first["2011Q2"] == (150.0, "cfs", ["derived"])
    assert ctrl_first["2011Q4"] == (900.0, "cfs", [])
    # earnings 정의: 연결이 요약표뿐인 분기는 본표가 있는 별도 먼저
    earn = dd.assemble_quarters(reports, controlling_first=False)
    assert earn["2011Q2"] == (70.0, "ofs", [])
    assert earn["2011Q1"] == (100.0, "cfs", [])


def test_assemble_never_mixes_bases_in_a_difference():
    reports = {
        "2009Q1": _rr(2009, 1, ofs_3m=10.0, ofs_cum=10.0, src={"ofs": "is:x"}),
        "2009Q2": _rr(2009, 2, cfs_cum=50.0, src={"cfs": "summary:x"}),
    }
    assert dd.assemble_quarters(reports, controlling_first=True)["2009Q2"][0] is None


# ---------------------------------------------------------------- 발행주식총수


def test_shares_from_acode_table():
    t = _table(
        _row("(2008.3.31 현재 )", "(단위 : 주)"),
    ) + _table(
        _row('<TH COLSPAN="2" ROWSPAN="2">구 분</TH>', '<TH COLSPAN="3">주식의 종류</TH>', tag="TH"),
        _row("보통주", "우선주", "합계", tag="TH"),
        _row('<TD COLSPAN="2">Ⅱ. 현재까지 발행한 주식의 총수</TD>', '<TE ACODE="ISU_STK2">155,609,337</TE>',
             '<TE ACODE="ISU_STK2">23,893,427</TE>', '<TE ACODE="SUM_ISU_STK2">179,502,764</TE>'),
        _row('<TD COLSPAN="2">Ⅳ. 발행주식의 총수 (Ⅱ-Ⅲ)</TD>', '<TE ACODE="ISU_STK3"> 147,299,337</TE>',
             '<TE ACODE="ISU_STK3"> 22,833,427</TE>', '<TE ACODE="SUM_ISU_STK3"> 170,132,764</TE>'),
        _row('<TD COLSPAN="2">Ⅵ. 유통주식수 (Ⅳ-Ⅴ)</TD>', '<TE ACODE="FLT_STK">126,469,789</TE>',
             '<TE ACODE="FLT_STK">19,853,734</TE>', '<TE ACODE="SUM_FLT_STK">146,323,523</TE>'),
    )
    sh = dd.extract_shares(dd.parse_blocks(_doc(t)), date(2008, 3, 31))
    assert (sh.common, sh.preferred, sh.total) == (147_299_337, 22_833_427, 170_132_764)
    assert sh.asof == date(2008, 3, 31) and sh.method == "acode"


def test_shares_from_label_without_acode():
    t = _table(
        _row("구 분", "보통주", "우선주", "합계", tag="TH"),
        _row("Ⅰ. 발행할 주식의 총수", "400", "100", "500"),
        _row("Ⅳ. 발행주식의 총수", "1,000", "-", "1,000"),
    )
    sh = dd.extract_shares(dd.parse_blocks(_doc(t)), date(2015, 12, 31))
    assert (sh.common, sh.total, sh.method) == (1_000, 1_000, "label")


def test_shares_fallback_to_voting_rights_table():
    """2010 분기보고서는 '주식의 총수' 를 문장으로만 — 의결권 현황 표에서(002230 2010Q1)."""
    t = _table(_row("[2010. 03. 31 현재]", "(단위 : 주)")) + _table(
        _row('<TH COLSPAN="2">구 분</TH>', "주식수", "비고", tag="TH"),
        _row('<TD ROWSPAN="2">발행주식총수(A)</TD>', "보통주", "14,000,000", "-"),
        _row("우선주", "-", "-"),
        _row('<TD ROWSPAN="2">의결권없는 주식수(B)</TD>', "보통주", "-", "-"),
        _row("우선주", "-", "-"),
    )
    sh = dd.extract_shares(dd.parse_blocks(_doc("<P>4. 주식의 총수 등 발행주식의 총수는 보통주 14,000,000</P>", t)), date(2010, 3, 31))
    assert (sh.common, sh.preferred, sh.method) == (14_000_000, 0, "voting")
    assert sh.asof == date(2010, 3, 31)


def test_doc_files_puts_main_document_first():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("20140331002427_00760.xml", "<A/>")
        z.writestr("20140331002427.xml", "본문".encode("cp949"))
    names = [n for n, _ in dd.doc_files(buf.getvalue())]
    assert names[0] == "20140331002427.xml"
    assert dd.doc_files(buf.getvalue())[0][1] == "본문"


def test_agrees_definition():
    assert dd.agrees(100.0, 100.4) and dd.agrees(-100.0, -100.5)
    assert not dd.agrees(100.0, 101.0) and not dd.agrees(None, 1.0) and not dd.agrees(1.0, None)


@pytest.mark.parametrize("label", ["XⅢ.당기순이익", "Ⅹ. 당기순이익(손실)", "(1) 분기순손실", "연결당기순이익", "당기순이익(주석24)"])
def test_total_net_income_labels(label):
    """'XⅢ' 처럼 ASCII X 와 로마숫자 Ⅲ 을 섞은 번호도 있다(012160 2016 반기)."""
    assert dd.is_total_ni(label)


@pytest.mark.parametrize("label", ["법인세비용차감전순이익", "지배기업의 소유주에게 귀속되는 당기순이익", "기본주당순이익",
                                   "당기순이익의 귀속", "총포괄이익", "계속영업당기순이익"])
def test_not_total_net_income_labels(label):
    assert not dd.is_total_ni(label)


def test_kisu_on_second_header_row_marks_prior_year_column_stale():
    """'당기|전기' 아래 줄에 '제39기|제38기' — 제40기 반기보고서의 배당표 '당기' 는 직전 사업연도(012160)."""
    rows = [
        _row("구 분", "당기", "전기", tag="TH"),
        _row("구 분", "제39기", "제38기", tag="TH"),
        _row("매출액", "10", "9"),
        _row("영업이익", "2", "1"),
        _row("당기순이익", "3,677", "2,075"),
    ]
    g = dd.grid(dd.parse_blocks(_table(*rows))[0].table)
    c3, cc, flags = dd._current_columns(g, 2, [], date(2016, 6, 30), 2, 40)
    assert (c3, cc, flags) == ([], [], ["stale_kisu"])


def test_summary_income_table_with_spanning_title_row():
    """'[요약 연결손익계산서]' 표제가 모든 열에 걸친 요약표(069080 2016 반기) — 첫 열만 당기, 누적, 요약 취급."""
    rows = [
        _row('<TH COLSPAN="4">[요약 연결손익계산서]</TH>', tag="TH"),
        _row("구 분", "제17기 반기 ('16.01.01~06.30)", "제16기 반기 ('15.01.01~06.30)", "제16기 ('15.01.01~12.31)", tag="TH"),
        _row("영업수익", "113,918", "91,927", "242,222"),
        _row("영업이익", "31,695", "31,440", "70,000"),
        _row("당기순이익", "23,630", "29,145", "60,235"),
    ]
    xml = _doc("<TITLE>III. 재무에 관한 사항</TITLE><TITLE>1. 요약재무정보</TITLE>",
               _table(_row("(단위:백만원)")), _table(*rows), "<TITLE>IV. 감사인의 감사의견 등</TITLE>",
               cover="(제 17 기)")
    cands = dd.extract_ni_candidates(dd.parse_blocks(xml), date(2016, 6, 30), 2, kisu=17)
    assert [(c.kind, c.fs, c.total_3m, c.total_cum) for c in cands] == [("summary", "cfs", None, 23_630e6)]


def test_statement_name_inside_a_sentence_is_not_a_heading():
    """'…요약포괄손익계산서는 다음과 같습니다' 뒤의 종속기업 표를 손익계산서로 읽지 않는다(083310 2016)."""
    assert dd._heading("연결대상 종속기업의 요약포괄손익계산서는 다음과 같습니다. (단위: 천원)") == (None, None)
    assert dd._heading("연결 포괄손익계산서 제 15 기 2016.01.01 부터 2016.12.31 까지") == ("is", "cfs")
    assert dd._heading("나. 최근 3사업연도 배당에 관한 사항") == ("div", None)


# ---------------------------------------------------------------- 지배지분(리뷰 2026-10-13 회귀)


def _cfs_is(ni_rows: list[tuple[str, tuple[str, ...]]], *, unit: str = "(단위 : 원)", q: str = "3분기",
            end: str = "2015.09.30") -> str:
    rows = [
        _row("", f'<TH COLSPAN="2">제 10 기 {q}</TH>', f'<TH COLSPAN="2">제 9 기 {q}</TH>', tag="TH"),
        _row("", "3개월", "누적", "3개월", "누적", tag="TH"),
        _row("매출액", "1,000", "2,000", "900", "1,800"),
        _row("영업이익", "100", "200", "90", "180"),
        _row("법인세비용", "10", "20", "9", "18"),
    ] + [_row(lab, *vals) for lab, vals in ni_rows]
    return ("<TITLE>III. 재무에 관한 사항</TITLE><TITLE>2. 연결재무제표</TITLE><P>연결 포괄손익계산서</P>"
            + _table(_row(f"제 10 기 {q} 2015.01.01 부터 {end} 까지"), _row(unit)) + _table(*rows))


def _ni(xml: str, q: int = 3) -> dd.NetIncome:
    return _report(_doc(xml, cover=f"(제 10 기 {q} 분기)"), 2015, q).ni


def test_ctrl_is_never_taken_from_summary_table():
    """099830 2015Q3: 요약표의 '지배기업의소유주지분' 칸이 실제로는 기타포괄손익 — 본표에 귀속·비지배 행이 없으면 ctrl = 합계."""
    summary = _table(
        _row("구 분", "제 10 기 3분기", "제 9 기", tag="TH"),
        _row("자산총계", "1", "1"),
        _row("당기순이익", "(8,032)", "100"),
        _row("지배기업의소유주지분", "3,964", "100"),
    )
    xml = ("<TITLE>III. 재무에 관한 사항</TITLE><TITLE>1. 요약재무정보</TITLE><P>가. 요약연결재무정보 (단위 : 백만원)</P>"
           + summary + _cfs_is([("당기순이익(손실)", ("(3,945,792,869)", "(8,032,482,293)", "1", "1"))]))
    ni = _ni(xml)
    assert (ni.ctrl_3m, ni.ctrl_cum) == (-3_945_792_869.0, -8_032_482_293.0)
    assert "cfs_ctrl_eq_total" in ni.flags


def test_ctrl_requires_ctrl_plus_nci_equal_total():
    """011700 2015Q2·Q3: 지배+비지배가 합계와 어긋나면 ctrl 을 비운다."""
    xml = _cfs_is([
        ("당기순이익", ("1,721,391,194", "3,481,555,040", "1", "1")),
        ("지배기업 소유주지분", ("1,810,785,642", "3,591,115,154", "1", "1")),
        ("비지배지분", ("(10,000)", "(20,000)", "0", "0")),
    ])
    ni = _ni(xml)
    assert ni.cfs_3m == 1_721_391_194.0
    assert ni.ctrl_3m is None and ni.ctrl_cum is None
    assert "cfs_ctrl_inconsistent" in ni.flags


def test_ctrl_label_with_jibun_and_quarter_consolidated_total():
    """034730 2015Q3: 합계 '분기연결순이익', 지배 '지배기업소유주지분순이익(손실)'."""
    xml = _cfs_is([
        ("분기연결순이익", ("5,020,527", "5,374,785", "113,276", "380,842")),
        ("지배기업소유주지분순이익(손실)", ("4,909,676", "5,261,846", "111,692", "378,276")),
        ("비지배지분순이익", ("110,851", "112,939", "1,584", "2,566")),
        ("총포괄이익의 귀속", ("", "", "", "")),
        ("지배기업소유주지분", ("4,981,293", "5,357,381", "1", "1")),
    ], unit="(단위 : 백만원)")
    ni = _ni(xml)
    assert (ni.cfs_3m, ni.ctrl_3m, ni.ctrl_cum) == (5_020_527e6, 4_909_676e6, 5_261_846e6)


def test_negative_ctrl_with_positive_total_is_kept_when_consistent():
    """000300 2015Q1 처럼 비지배가 크면 합계 +, 지배 − 가 맞다 — 합이 맞으면 그대로."""
    xml = _cfs_is([
        ("당기순이익(손실)", ("805,544,490", "805,544,490", "1", "1")),
        ("지배기업소유주지분순이익(손실)", ("(803,750,957)", "(803,750,957)", "1", "1")),
        ("비지배지분순이익", ("1,609,295,447", "1,609,295,447", "1", "1")),
    ], q="1분기", end="2015.03.31")
    assert _ni(xml, 1).ctrl_cum == -803_750_957.0


def test_ctrl_equals_total_when_nci_is_dash():
    xml = _cfs_is([
        ("당기순이익", ("100", "300", "1", "1")),
        ("당기순이익의 귀속", ("", "", "", "")),
        ("지배기업의 소유주에게 귀속되는 당기순이익", ("-", "-", "-", "-")),
        ("비지배지분에 귀속되는 당기순이익", ("-", "-", "-", "-")),
    ])
    ni = _ni(xml)
    assert ni.ctrl_3m is None  # 지배 행이 있는데 값이 '-' — 모른다
    xml2 = _cfs_is([("당기순이익", ("100", "300", "1", "1")), ("비지배지분순이익", ("-", "-", "-", "-"))])
    assert (_ni(xml2).ctrl_3m, _ni(xml2).ctrl_cum) == (100.0, 300.0)


def test_q3_ctrl_not_derived_from_another_table():
    """115960 2016Q3: 요약표 ctrl 누적(−30,022천원)과 이전 분기 누적을 빼 원문에 없는 값(−13.19B)을 만들던 경로."""
    q2 = _rr(2016, 2, cfs_3m=8_168_388_010.0, cfs_cum=13_162_101_090.0, ctrl_3m=8_168_388_010.0,
             ctrl_cum=13_162_101_090.0, src={"cfs": "is:x"})
    q3 = _rr(2016, 3, cfs_3m=2_971_570_672.0, cfs_cum=16_133_671_762.0, src={"cfs": "is:x"})  # 본표 ctrl 없음
    recs = dd.build_records("115960", {"2016Q2": [q2], "2016Q3": [q3]})
    r3 = next(r for r in recs if r["period"] == "2016Q3")
    assert r3["ctrl_q"] is None and r3["ctrl_ytd"] is None
    assert (r3["netinc_ctrl"], r3["basis_ctrl"]) == (2_971_570_672.0, "cfs")


# ---------------------------------------------------------------- 레코드(정정·기준 혼합)


def _rv(rcept: str, year: int, q: int, corr: bool = False, **kw) -> dd.ReportResult:
    src = kw.pop("src", None) or {}
    return dd.ReportResult(rcept, year, q, dd.NetIncome(src=src, **kw), None, None, is_correction=corr)


def test_correction_is_a_separate_record_with_its_own_knowledge_date():
    reports = {
        "2016Q1": [_rv("20160516003860", 2016, 1, ofs_3m=100.0, ofs_cum=100.0, src={"ofs": "is:x"}),
                   _rv("20160517000128", 2016, 1, True, cfs_3m=120.0, cfs_cum=120.0, ofs_3m=100.0, ofs_cum=100.0,
                       src={"cfs": "is:x", "ofs": "is:x"})],
    }
    recs = dd.build_records("033540", reports)
    assert [(r["knowledge_date"], r["is_correction"], r["netinc"], r["basis"]) for r in recs] == [
        ("2016-05-16", False, 100.0, "ofs"), ("2016-05-17", True, 120.0, "cfs")]


def test_derived_quarter_uses_prior_version_known_at_that_date_and_same_basis():
    reports = {
        "2009Q1": [_rv("20090515000001", 2009, 1, ofs_3m=10.0, ofs_cum=10.0, src={"ofs": "is:x"}),
                   _rv("20091120000001", 2009, 1, True, ofs_3m=12.0, ofs_cum=12.0, src={"ofs": "is:x"})],
        "2009Q2": [_rv("20090814000001", 2009, 2, ofs_cum=30.0, cfs_cum=50.0, src={"ofs": "summary:x", "cfs": "summary:x"})],
    }
    r2 = next(r for r in dd.build_records("x", reports) if r["period"] == "2009Q2")
    # 08-14 에 알려진 1분기는 원본(10) — 11-20 정정(12)을 쓰면 미래 정보
    assert (r2["ofs_q"], r2["ofs_q_derived"]) == (20.0, True)
    # 연결은 1분기 연결 누적이 없으니 만들지 않는다(별도 1분기를 빼지 않는다)
    assert r2["cfs_q"] is None and r2["cfs_ytd"] == 50.0
    assert (r2["netinc"], r2["basis"], r2["derived"]) == (20.0, "ofs", True)


def test_q4_record_has_annual_for_every_basis_including_separate():
    reports = {"2008Q4": [_rv("20090331000001", 2008, 4, cfs_3m=900.0, cfs_cum=900.0, ofs_3m=700.0, ofs_cum=700.0,
                              src={"cfs": "is:x", "ofs": "is:x"})],
               "2008Q3": [_rv("20081114000001", 2008, 3, ofs_3m=150.0, ofs_cum=500.0, src={"ofs": "is:x"})]}
    r4 = next(r for r in dd.build_records("x", reports) if r["period"] == "2008Q4")
    assert (r4["netinc"], r4["basis"], r4["cfs_ytd"], r4["ofs_ytd"]) == (900.0, "cfs", 900.0, 700.0)
    assert (r4["ofs_q"], r4["ofs_q_derived"]) == (200.0, True)  # 연간 − 3분기 누적, 같은 기준
    assert r4["cfs_q"] is None  # 3분기 연결 누적이 없다 — 별도 3분기를 빼지 않는다


def test_shares_without_reference_date_are_flagged_not_filled():
    t = _table(
        _row("구 분", "보통주", "우선주", "합계", tag="TH"),
        _row("Ⅳ. 발행주식의 총수", "1,000", "-", "1,000"),
    )
    sh = dd.extract_shares(dd.parse_blocks(_doc(t)), date(2015, 12, 31))
    assert sh.asof is None and sh.asof_inferred is True


# ---------------------------------------------------------------- 폐지 종목 매핑


def test_normalize_corp_name():
    assert dd.normalize_corp_name("(주)삼성테크윈") == dd.normalize_corp_name("삼성테크윈 주식회사") == "삼성테크윈"
    assert dd.normalize_corp_name("ＡＰ우주통신㈜") == "AP우주통신"


def test_name_candidates_skip_corps_listed_under_another_code():
    rows = [{"corp_code": "1", "corp_name": "동아건설", "stock_code": ""},
            {"corp_code": "2", "corp_name": "동아건설(주)", "stock_code": "999990"},
            {"corp_code": "3", "corp_name": "삼보판지", "stock_code": "023600"}]
    idx = dd.corp_name_index(rows)
    assert dd.name_candidates("000280", ["동아건설"], idx) == ["1"]


def test_resolve_delisted_requires_exactly_one_confirmed_candidate():
    items = {"a": [{"report_nm": "사업보고서 (2009.12)"}], "b": [{"report_nm": "사업보고서 (2010.12)"}], "c": []}
    assert dd.resolve_delisted("x", ["a"], items, {2010}) == ("a", "confirmed")
    assert dd.resolve_delisted("x", ["a", "c"], items, {2010}) == ("a", "confirmed_among_2")
    assert dd.resolve_delisted("x", ["a", "b"], items, {2010}) == (None, "ambiguous")  # 동명이인
    assert dd.resolve_delisted("x", ["c"], items, {2010}) == (None, "unconfirmed")
    assert dd.resolve_delisted("x", [], items, {2010}) == (None, "no_name_match")


def test_class_shares_follow_common_code():
    assert dd.parent_common_code("012655", "쌍용건설우") == "012650"
    assert dd.parent_common_code("023601", "삼보판지1신") == "023600"
    assert dd.parent_common_code("005897", "동원증권(2우B)") == "005890"
    assert dd.parent_common_code("012650", "쌍용건설") is None


def test_correction_errata_before_body_is_skipped():
    """122690 2015 정정본: 앞머리 정오표의 '정정 전' 손익계산서가 아니라 본문 값을 읽는다."""
    before = _is_table("연결 포괄손익계산서", "제 10 기 반기 2016.01.01 부터 2016.06.30 까지", "(단위 : 원)",
                       ("1", "6,022", "1", "1"))
    after = _is_table("연결 포괄손익계산서", "제 10 기 반기 2016.01.01 부터 2016.06.30 까지", "(단위 : 원)",
                      ("1", "5,971", "1", "1"))
    xml = ("<DOCUMENT><BODY><TITLE>정 정 신 고 (보고)</TITLE><P>4. 정정사항</P>" + before + after
           + "<P>반 기 보 고 서</P><P>(제 10 기)</P><TITLE>III. 재무에 관한 사항</TITLE>" + after + "</BODY></DOCUMENT>")
    blocks = dd.parse_blocks(xml)
    assert dd.report_body_start(blocks) > 0
    assert _report(xml, 2016, 2).ni.cfs_cum == 5_971.0


# ---------------------------------------------------------------- 연대별 손 검증(2026-10-13) 회귀


def _annual_is(title: str, company: str, rows: list[tuple[str, ...]], *, unit: str = "(단위 : 원)",
               head: tuple[str, ...] = ("과 목", "제 37 기", "제 36 기")) -> str:
    body = [_row(*head, tag="TH"), _row("매 출 액", "1,000", "900"), _row("영 업 이 익", "100", "90"),
            _row("법인세비용", "10", "9")] + [_row(*r) for r in rows]
    return _table(_row(f"{title} 제37기 : 2012년 1월 1일부터 2012년 12월 31일까지 제36기 : 2011년 1월 1일부터 "
                       f"2011년 12월 31일까지 {company} {unit}")) + _table(*body)


def test_unqualified_statement_with_company_only_header_is_separate():
    """011320 FY2012: '연결 재무제표에 관한 사항' 절 아래 '손 익 계 산 서 … 주식회사 유니크' 는 별도 — 연결로 읽어 +16%."""
    xml = _doc("<TITLE>III. 재무에 관한 사항</TITLE><P>1. 연결 재무제표에 관한 사항</P>",
               _annual_is("연 결 손 익 계 산 서", "주식회사 유니크 및 종속회사", [("당기순이익", "8,439,607,574", "1")]),
               "<P>가.요약재무정보</P>",
               _annual_is("손 익 계 산 서", "주식회사 유니크", [("당기순이익", "9,813,677,800", "1")]),
               cover="(제 37 기)")
    ni = _report(xml, 2012, 4).ni
    assert (ni.cfs_cum, ni.ofs_cum) == (8_439_607_574.0, 9_813_677_800.0)


def test_unqualified_statement_after_consolidated_is_separate():
    """012320 FY2012: 본문 '연결 포괄손익계산서' 다음의 수식어 없는 '포괄손익계산서' 는 별도."""
    xml = _doc("<TITLE>III. 재무에 관한 사항</TITLE><TITLE>2. 연결재무제표</TITLE>",
               _annual_is("연결 포괄손익계산서", "", [("당기순이익", "42,879,202,491", "1")]),
               _annual_is("포괄손익계산서", "", [("당기순이익", "56,917,579,696", "1")]), cover="(제 37 기)")
    ni = _report(xml, 2012, 4).ni
    assert (ni.cfs_cum, ni.ofs_cum) == (42_879_202_491.0, 56_917_579_696.0)


def test_note_column_header_does_not_hide_statement():
    """012320 FY2012 별도 감사보고서: 머리의 '주석' 열이 표 이름 판정을 가로채 손익계산서를 버리던 것."""
    xml = _doc(_annual_is("포 괄 손 익 계 산 서", "주식회사 경동도시가스", [("당기순이익", "", "56,917,579,696", "", "1")],
                          head=("과 목", "주석", "제36(당)기", "제36(당)기", "제35(전)기")), cover="(제 37 기)")
    assert _report(xml, 2012, 4).ni.ofs_cum == 56_917_579_696.0


@pytest.mark.parametrize(("text", "want"), [("33,295,643,940 ==========", 33_295_643_940.0), ("￦ 637,119,009", 637_119_009.0),
                                            ("(141,703,827,180)", -141_703_827_180.0)])
def test_kgaap_audit_report_number_marks(text, want):
    """012320·089480 2010 연결감사보고서: 합계 밑줄 '=====' 와 '￦' 가 붙은 숫자."""
    assert dd.parse_number(text) == want


def test_kgaap_controlling_and_minority_labels():
    """012320 2010: '지배회사지분순이익 (주석24)'·'소수주주지분순손실', 089480 2010: '당기순손실'·'지배회사지분순손실'."""
    xml = _doc(_annual_is("연 결 손 익 계 산 서", "주식회사 경동도시가스와 종속회사", [
        ("Ⅹ.연결당기순이익", "", "33,295,643,940 =====", "", "1"),
        ("1. 지배회사지분순이익 (주석24)", "", "33,646,051,672", "", "1"),
        ("2. 소수주주지분순손실 (주석24)", "", "(350,407,732)", "", "1"),
    ], head=("과 목", "제 34(당) 기", "제 34(당) 기", "제 33(전) 기", "제 33(전) 기")), cover="(제 37 기)")
    ni = _report(xml, 2012, 4).ni
    assert (ni.cfs_cum, ni.ctrl_cum) == (33_295_643_940.0, 33_646_051_672.0)
    xml2 = _doc(_annual_is("연 결 손 익 계 산 서", "주식회사 평산 및 종속회사", [
        ("ⅩⅡ.당기순손실", "", "141,703,827,180 ====", "", "1"),
        ("1. 지배회사지분순손실", "", "(141,703,827,180)", "", "1"),
    ], head=("과 목", "제 17(당) 기", "제 17(당) 기", "제 16(전) 기", "제 16(전) 기")), cover="(제 37 기)")
    ni2 = _report(xml2, 2012, 4).ni
    assert (ni2.cfs_cum, ni2.ctrl_cum) == (-141_703_827_180.0, -141_703_827_180.0)


def test_old_kgaap_total_minus_outside_shareholders():
    """005380 FY2007: '총당기순이익 → 외부주주지분순이익 → 연결당기순이익'(지배), FY2008 '소수지분순이익'."""
    xml = _doc(_annual_is("연 결 손 익 계 산 서", "", [
        ("XIII.총당기순이익", "1,956,087", "1"), ("XIV.외부주주지분순이익", "355,607", "1"), ("XV.연결당기순이익", "1,600,480", "1"),
    ], unit="(단위 : 백만원)"), cover="(제 37 기)")
    ni = _report(xml, 2012, 4).ni
    assert (ni.cfs_cum, ni.ctrl_cum) == (1_956_087e6, 1_600_480e6)
    xml2 = _doc(_annual_is("연 결 손 익 계 산 서", "", [
        ("XI.연결총당기순이익", "1,092,552", "1"), ("1.지배회사지분순이익", "857,751", "1"), ("2.소수지분순이익", "234,801", "1"),
    ], unit="(단위 : 백만원)"), cover="(제 37 기)")
    assert _report(xml2, 2012, 4).ni.ctrl_cum == 857_751e6


def test_bank_half_year_statement_with_detail_and_subtotal_cells():
    """024110 기업은행 2012 반기: '반기연결순이익 (대손준비금…(누적)…' 라벨, 한 기간이 '세부 | 소계' 두 칸, 백만원."""
    rows = [
        _row("과 목", '<TH COLSPAN="2">제52(당)기 반기</TH>', '<TH COLSPAN="2">제51(전)기 반기</TH>', tag="TH"),
        _row("I. 순이자손익", "", "2,279,194", "", "2,332,563"),
        _row("1. 이자수익", "4,782,970", "", "4,540,090", ""),
        _row("XX. 영업이익", "", "1,000,000", "", "1"),
        _row("XVIII. 법인세비용차감전순이익", "", "1,043,760", "", "1,318,956"),
        _row("XX. 반기연결순이익 (대손준비금 반영후 조정이익 당반기(누적): 1,000 원", "", "780,323", "", "993,091"),
        _row("반기연결순이익의 귀속", "", "", "", ""),
        _row("지배기업주주지분반기순이익", "777,785", "", "995,628", ""),
        _row("비지배지분반기순이익(손실)", "2,538", "", "(2,537)", ""),
    ]
    xml = _doc("<TITLE>III. 재무에 관한 사항</TITLE><P>나. 연결포괄손익계산서</P>",
               _table(_row("제 52 기 반기 2012년 01월 01일부터 2012년 06월 30일까지 중소기업은행과 그 종속회사 (단위 : 백만원)")),
               _table(*rows), cover="(제 52 기 반기)")
    ni = _report(xml, 2012, 2).ni
    assert (ni.cfs_3m, ni.cfs_cum, ni.ctrl_cum) == (None, 780_323e6, 777_785e6)


def test_same_day_original_and_empty_correction_keeps_original():
    """같은 날 원본(값 있음)과 정정본(값 없음)이 들어오면 원본이 남는다 — 빈 판은 PK 경쟁에 끼지 않는다."""
    reports = {"2016Q1": [_rv("20160516000001", 2016, 1, ofs_3m=100.0, ofs_cum=100.0, src={"ofs": "is:x"}),
                          _rv("20160516000099", 2016, 1, True)]}
    recs = dd.build_records("x", reports)
    assert [(r["rcept_no"], r["netinc"]) for r in recs] == [("20160516000001", 100.0)]


def test_same_day_two_valued_versions_keep_later_receipt():
    reports = {"2016Q1": [_rv("20160516000001", 2016, 1, ofs_3m=100.0, ofs_cum=100.0, src={"ofs": "is:x"}),
                          _rv("20160516000099", 2016, 1, True, ofs_3m=110.0, ofs_cum=110.0, src={"ofs": "is:x"})]}
    recs = dd.build_records("x", reports)
    assert [(r["rcept_no"], r["netinc"], r["is_correction"]) for r in recs] == [("20160516000099", 110.0, True)]


def test_empty_correction_on_later_day_is_not_emitted():
    reports = {"2016Q1": [_rv("20160516000001", 2016, 1, ofs_3m=100.0, ofs_cum=100.0, src={"ofs": "is:x"}),
                          _rv("20160601000001", 2016, 1, True)]}
    assert [r["knowledge_date"] for r in dd.build_records("x", reports)] == ["2016-05-16"]
