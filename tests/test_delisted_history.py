"""2006~ 폐지 종목 일봉(네이버)·KIND 목록 — 028 (네트워크 불요)."""

from __future__ import annotations

from collectors import delisted_history as dh
from collectors.storage import CHECKED_DELISTED_HIST, connect

KIND = """<table><tr><th>번호</th></tr>
<tr><td>2</td><td>히스토스템</td><td>036840</td><td>2011-09-15</td><td>기업의 계속성</td><td></td></tr>
<tr><td>1</td><td>현대엘리베이터</td><td>017800</td><td>1996-07-03</td><td>시장 이전</td><td></td></tr>
<tr><td>x</td><td>머리</td><td>종목코드</td><td>폐지일자</td><td></td><td></td></tr></table>"""

SISE = '[["날짜"],\n["20110913", 60, 70, 50, 65, 100, 0.0],\n["20110914", 0, 0, 0, 55, 0, 0.0]]'


def test_parse_kind_keeps_only_well_formed_rows():
    assert dh.parse_kind(KIND) == [
        ("036840", "2011-09-15", "히스토스템", "기업의 계속성"),
        ("017800", "1996-07-03", "현대엘리베이터", "시장 이전")]


def _db(tmp_path):
    con = connect(tmp_path / "t.db")
    con.executemany("INSERT INTO delisted_stocks(code, name, market) VALUES (?,?,?)",
                    [("003450", "현대증권", "유가증권"), ("123456", "코넥스사", "코넥스"), ("A1234", "채권", "유가증권")])
    con.execute("INSERT INTO stocks(code, name) VALUES ('017800', '현대엘리베이터')")
    con.commit()
    return con


def test_targets_union_minus_listed_konex_and_done(tmp_path):
    con = _db(tmp_path)
    assert dh.target_codes(con, {"036840", "017800"}) == ["003450", "036840"]
    con.execute("INSERT INTO backfill_markers VALUES ('003450', ?, '2026-10-12')", (CHECKED_DELISTED_HIST,))
    con.commit()
    assert dh.target_codes(con, {"036840"}) == ["036840"]
    assert dh.target_codes(con, {"036840"}, refetch=True) == ["003450", "036840"]


def test_collect_writes_adjusted_rows_and_marks_only_answered_codes(tmp_path):
    con = _db(tmp_path)
    bodies = {"036840": SISE, "003450": '[["날짜"]]', "000001": ""}
    stats = dh.collect(con, ["000001", "003450", "036840"], fetch=lambda c, start: bodies[c], sleep=0)
    assert stats == {"codes": 3, "with_data": 1, "empty": 1, "failed": 1, "rows": 2}
    rows = [tuple(r) for r in con.execute(
        "SELECT date, open, high, low, close, source FROM daily_bars_delisted_naver ORDER BY date")]
    assert rows == [("2011-09-13", 60, 70, 50, 65, "naver_adj"), ("2011-09-14", 55, 55, 55, 55, "naver_adj")]
    assert con.execute("SELECT count(*) FROM daily_bars").fetchone()[0] == 0      # daily_bars 는 건드리지 않는다
    marked = {r[0] for r in con.execute("SELECT code FROM backfill_markers WHERE source = ?", (CHECKED_DELISTED_HIST,))}
    assert marked == {"003450", "036840"}          # 네트워크 실패(빈 본문)는 표시하지 않아 다음에 다시 받는다
