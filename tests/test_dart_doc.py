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
