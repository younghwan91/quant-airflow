"""상장 종목 네이버 수정주가 기준(037) — v2 이벤트 추정용 (네트워크 불요)."""

from __future__ import annotations

from datetime import date

from collectors import adjusted_v2 as av
from collectors import listed_naver_ref as ln
from collectors.storage import CHECKED_LISTED_NAVER, checked_codes, connect

BODY = '[["날짜","시가","고가","저가","종가","거래량"],["20110503", 0, 0, 0, 3820, 0],["20110504", 4100, 4500, 4000, 4390, 5143940]]'


def test_collect_writes_naver_adj_rows_and_marks_only_answered_codes(tmp_path):
    con = connect(tmp_path / "t.db")
    stats = ln.collect(con, ["003720", "999999"], fetch=lambda c, start, end: BODY if c == "003720" else "", sleep=0)
    assert stats["rows"] == 2 and stats["failed"] == 1
    assert tuple(con.execute("SELECT close, source FROM daily_bars_listed_naver WHERE date='2011-05-03'").fetchone()) == (3820, "naver_adj")
    assert checked_codes(con, CHECKED_LISTED_NAVER) == {"003720"}       # 네트워크 실패는 표시하지 않는다


def test_naver_ref_recovers_missing_event_at_halt_resume():
    # 003720: 거래정지(거래량 0) 뒤 재개일 원주가 38,200 → 4,390, 키움 이벤트 없음. 네이버 수정가 3,820 → 4,390.
    rows = [(date(2011, 5, 3), 38200, 38200, 38200, 38200, 0, 0), (date(2011, 5, 4), 4100, 4500, 4000, 4390, 5143940, 1)]
    ref = {date(2011, 5, 3): 3820.0, date(2011, 5, 4): 4390.0}
    assert av.infer_events(rows, ref, {}) == {date(2011, 5, 4): -90.0}
