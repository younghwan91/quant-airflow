"""KRX 지수운영공지 — 본 공지 판정·시행일·엑셀 파싱·재개 (네트워크 불요)."""

from __future__ import annotations

import datetime as dt
import io
import zipfile

import pytest

from collectors.krx_index_changes import (
    collect,
    effective_date,
    is_periodic_main,
    parse_changes,
    parse_workbook,
    read_xlsx,
)
from collectors.storage import connect


@pytest.mark.parametrize(("title", "want"), [
    ("KOSPI 200, KOSDAQ 150, KRX300 구성종목 정기변경 안내", True),
    ("2017년 12월 KOSDAQ 150 구성종목 정기변경 안내", True),
    ("'22.12월 KOSPI 200, KOSDAQ 150, KRX 300 지수 정기변경 안내", True),      # "구성종목" 이 없다
    ("KOSPI 200 섹터지수 등 구성종목 정기변경", False),
    ("KOSPI 100, KOSPI 50, KOSPI 200 중소형주, KOSPI 200 초대형제외, KRX Mid 200 구성종목 정기변경", False),
    ("2018년 6월 KOSPI 200 고배당 지수 등 3개 전략형지수 구성종목 정기변경 안내", False),
    ("카카오뱅크 신규상장에 따른 KOSPI 200 등 수시변경 안내", False),
])
def test_is_periodic_main(title, want):
    assert is_periodic_main(title) is want


@pytest.mark.parametrize(("body", "want"), [
    ("한국거래소(KRX)는 KOSPI 200 ... 구성종목을 &amp;#39;16.6.10(금)에 정기변경할 예정입니다.", dt.date(2016, 6, 10)),
    ("교체종목수 : 13종목 - 반영일 : '17.12.15(금)", dt.date(2017, 12, 15)),
    ("구성종목을 ‘23. 6. 9.(금)에 정기변경할 예정", dt.date(2023, 6, 9)),
    ("‘21.6.11(금)에 정기변경 ... 재상장 다음 매매거래일(5.28)에 편입", dt.date(2021, 6, 11)),
])
def test_effective_date(body, want):
    assert effective_date(body) == want


def test_effective_date_missing_raises():
    with pytest.raises(ValueError):
        effective_date("세부내용은 붙임 참조")


def test_parse_changes_isin_short_code_and_empty():
    isin = [["< KOSPI 200 교체종목 >"], ["편입", "", "제외"], ["종목코드", "종목명", "종목코드", "종목명"],
            ["KR7241560002", "두산밥캣", "KR7078520004", "에이블씨엔씨"],
            ["", "", "KR7029530003", "신도리코"]]
    assert parse_changes(isin) == [
        ("add", "241560", "KR7241560002", "두산밥캣"),
        ("drop", "078520", "KR7078520004", "에이블씨엔씨"),
        ("drop", "029530", "KR7029530003", "신도리코"),
    ]
    short = [["KOSPI 200 지수 편입/제외종목"], ["편입", "", "제외"], ["종목코드", "종목명", "종목코드", "종목명"],
             ["001440", "대한전선", "005180", "빙그레"]]
    assert parse_changes(short) == [("add", "001440", None, "대한전선"), ("drop", "005180", None, "빙그레")]
    assert parse_changes([["< KOSPI 50 교체종목 >"]]) == []      # 2018-06 KOSPI 50: 교체 0종목
    sector = [["코스닥 150 편입/제외 종목"], ["섹 터", "신규편입종목", "", "", "제외종목"],      # 2015-12
              ["", "종목코드", "종목명", "유동비율(%)", "종목코드", "종목명"],
              ["소 재", "178920", "SKC코오롱PI", "45", "005160", "동국산업"]]
    assert parse_changes(sector) == [("add", "178920", None, "SKC코오롱PI"), ("drop", "005160", None, "동국산업")]


def test_parse_workbook_rejects_short_member_list():
    members = [["KOSDAQ 150 구성종목"], ["종목코드", "종목명"]] + [[f"KR7{i:06d}000", f"n{i}"] for i in range(149)]
    with pytest.raises(ValueError, match="149"):
        parse_workbook([("KOSDAQ 150 구성종목", members)])


def _xlsx(sheets: dict[str, list[list[str]]]) -> bytes:
    """inline string 만 쓰는 최소 xlsx."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        ns = 'xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" ' \
             'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"'
        z.writestr("xl/workbook.xml", f"<workbook {ns}><sheets>" + "".join(
            f'<sheet name="{n}" sheetId="{i}" r:id="rId{i}"/>' for i, n in enumerate(sheets, 1)) + "</sheets></workbook>")
        z.writestr("xl/_rels/workbook.xml.rels",
                   '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">' + "".join(
                       f'<Relationship Id="rId{i}" Target="worksheets/sheet{i}.xml"/>' for i in range(1, len(sheets) + 1))
                   + "</Relationships>")
        for i, grid in enumerate(sheets.values(), 1):
            rows = "".join(f'<row r="{r}">' + "".join(
                f'<c r="{chr(65 + c)}{r}" t="inlineStr"><is><t>{v}</t></is></c>' for c, v in enumerate(row) if v)
                + "</row>" for r, row in enumerate(grid, 1))
            z.writestr(f"xl/worksheets/sheet{i}.xml", f"<worksheet {ns}><sheetData>{rows}</sheetData></worksheet>")
    return buf.getvalue()


KQ_CHANGES = [["KOSDAQ 150 교체종목"], ["편입", "", "제외"], ["종목코드", "종목명", "종목코드", "종목명"],
              ["KR7036930006", "주성엔지니어링", "KR7011040003", "경동제약"]]
KQ_MEMBERS = [["KOSDAQ 150 구성종목"], ["종목코드", "종목명"]] + [[f"KR7{i:06d}000", f"n{i}"] for i in range(150)]


def test_read_xlsx_roundtrip():
    sheets = read_xlsx(_xlsx({"KOSDAQ 150 교체종목": KQ_CHANGES}))
    assert sheets == [("KOSDAQ 150 교체종목", [["KOSDAQ 150 교체종목"], ["편입", "", "제외"],
                                              ["종목코드", "종목명", "종목코드", "종목명"],
                                              ["KR7036930006", "주성엔지니어링", "KR7011040003", "경동제약"]])]


class _FakeBoard:
    def __init__(self):
        self.downloads = 0

    def posts(self):
        return [(486, "KOSDAQ 150 구성종목 정기변경 안내", dt.date(2016, 11, 24)),
                (487, "2016년 12월 코스피200 고배당/저변동성 및 코스닥150저변동성 정기변경", dt.date(2016, 12, 5))]

    def body(self, seq):
        return "KOSDAQ 150 지수의 구성종목을 '16.12.9(금)에 정기변경할 예정입니다." if seq == 486 else "전략지수"

    def files(self, seq):
        return [("1", "16.12월 코스닥150 정기변경(대외공지).xlsx")]

    def download(self, seq, file_seq):
        self.downloads += 1
        return _xlsx({"KOSDAQ 150 교체종목": KQ_CHANGES, "KOSDAQ 150 구성종목": KQ_MEMBERS})


def test_collect_writes_notices_changes_members_and_resumes(tmp_path):
    con = connect(tmp_path / "t.db")
    board = _FakeBoard()
    now = lambda: dt.datetime(2026, 10, 12, tzinfo=dt.UTC)  # noqa: E731
    stats = collect(con, board, now=now)
    assert stats == {"posts": 2, "new_notices": 2, "periodic": 1, "changes": 2, "members": 150}
    rows = con.execute("SELECT index_name, effective_date, action, code, announce_date FROM index_changes ORDER BY action").fetchall()
    assert [tuple(r) for r in rows] == [("KOSDAQ150", "2016-12-09", "add", "036930", "2016-11-24"),
                                        ("KOSDAQ150", "2016-12-09", "drop", "011040", "2016-11-24")]
    assert con.execute("SELECT count(*) FROM index_constituents").fetchone()[0] == 150
    again = collect(con, board, now=now)
    assert again["new_notices"] == 0 and again["periodic"] == 0 and board.downloads == 1
