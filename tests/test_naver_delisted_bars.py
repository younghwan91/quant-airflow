"""``parse_sise`` — siseJson 본문 파싱 (순수함수, 네트워크 불요)."""

from __future__ import annotations

from collectors.naver_delisted_bars import DAILY_BAR_SOURCE_COLUMNS, parse_sise

# 실제 응답 형태: JSON 이 아니라 작은따옴표 헤더 + 개행/탭이 섞인 JS 배열 리터럴.
SAMPLE = """ [['날짜', '시가', '고가', '저가', '종가', '거래량', '외국인소진율'],

\t
\t\t
["20160104", 141980, 148434, 141980, 145853, 6493, 11.21],
\t\t
["20160105", 145853, 156179, 140690, 151662, 13352, 11.2],
\t\t
["20160106", 151661, 156179, 146498, 147144, 0, 11.24]
]
"""


def test_parses_every_row():
    rows = parse_sise(SAMPLE, "060240")
    assert len(rows) == 3
    assert [r[1] for r in rows] == ["2016-01-04", "2016-01-05", "2016-01-06"]


def test_column_order_matches_contract():
    (row,) = parse_sise('["20160104", 100, 110, 90, 105, 1000, 1.0]', "000020")
    assert len(row) == len(DAILY_BAR_SOURCE_COLUMNS)
    named = dict(zip(DAILY_BAR_SOURCE_COLUMNS, row))
    assert named["code"] == "000020"
    assert named["date"] == "2016-01-04"
    assert (named["open"], named["high"], named["low"], named["close"]) == (100, 110, 90, 105)
    assert named["volume"] == 1000
    assert named["source"] == "naver"


def test_trade_value_is_close_times_volume_in_millions():
    """테이블 규약이 백만원 단위 — 원 단위로 넣으면 ADV 필터가 1e6 배 틀린다."""
    (row,) = parse_sise('["20160104", 100, 110, 90, 50000, 2000000, 1.0]', "000020")
    named = dict(zip(DAILY_BAR_SOURCE_COLUMNS, row))
    assert named["trade_value"] == round(50000 * 2000000 / 1e6)  # 100,000 백만원 = 1000억


def test_zero_volume_rows_are_kept():
    """거래정지일도 '상장돼 있었다'는 사실이라 남긴다 — 지우면 생존편향 스멜을 교란한다."""
    rows = parse_sise(SAMPLE, "060240")
    assert dict(zip(DAILY_BAR_SOURCE_COLUMNS, rows[-1]))["volume"] == 0


def test_nonpositive_close_rows_are_dropped():
    body = '["20160104", 0, 0, 0, 0, 0, 0.0],\n["20160105", 100, 110, 90, 105, 10, 1.0]'
    rows = parse_sise(body, "000020")
    assert len(rows) == 1
    assert rows[0][1] == "2016-01-05"


def test_halt_day_ohlc_normalized_to_close():
    """네이버는 정지일을 OHLC=0 으로 주지만 키움은 OHLC=종가로 저장한다.

    0을 그대로 넣으면 고가/저가를 읽는 로직이 같은 테이블 안에서 소스에 따라 다른
    값을 보게 된다 — 터지지 않고 수치만 틀리는 종류.
    """
    (row,) = parse_sise('["20210113", 0, 0, 0, 2410, 0, 6.27]', "036180")
    n = dict(zip(DAILY_BAR_SOURCE_COLUMNS, row))
    assert (n["open"], n["high"], n["low"], n["close"]) == (2410, 2410, 2410, 2410)
    assert n["volume"] == 0


def test_partially_broken_ohlc_falls_back_per_field():
    """한 필드만 비정상인 행 — 예전 ``and`` 분기가 못 잡던 구멍.

    정규화가 "셋 다 0" 일 때만 걸리던 시절엔 이 행이 분기를 안 타고, 뒤따르는
    ``min(low, close)`` 가 음수를 그대로 남겨 **daily_bars 에 음수 저가**가 실렸다.
    조건을 ``or`` 로 푸는 건 답이 아니다 — 그러면 멀쩡한 시가·고가까지 close 로
    뭉갠다. 그래서 필드별로 본다: 나쁜 필드만 close 가 되고 나머지는 보존된다.
    """
    (row,) = parse_sise('["20210113", 1200, 1300, -5, 1250, 100, 1.0]', "036180")
    n = dict(zip(DAILY_BAR_SOURCE_COLUMNS, row))
    assert n["low"] > 0                        # 음수가 그대로 실리지 않는다
    assert (n["open"], n["high"]) == (1200, 1300)  # 멀쩡한 값은 보존
    assert n["low"] <= n["close"] <= n["high"]


def test_zero_low_alone_does_not_flatten_the_bar():
    """저가만 0 인 행. ``or`` 로 넓혔다면 시가·고가가 종가로 뭉개졌을 자리."""
    (row,) = parse_sise('["20210114", 1200, 1300, 0, 1250, 100, 1.0]', "036180")
    n = dict(zip(DAILY_BAR_SOURCE_COLUMNS, row))
    assert (n["open"], n["high"]) == (1200, 1300)
    assert n["low"] == 1250                    # 값이 없으므로 종가로

def test_close_outside_range_widens_the_bar():
    """소스가 종가를 고가 밖으로 주는 행이 있다(정리매매 동전주). 봉 정의상 불가능."""
    (row,) = parse_sise('["20210727", 20, 20, 20, 21, 1709794, 0.04]', "152550")
    n = dict(zip(DAILY_BAR_SOURCE_COLUMNS, row))
    assert n["high"] == 21 and n["low"] == 20 and n["close"] == 21
    assert n["low"] <= n["close"] <= n["high"]


def test_every_parsed_bar_is_internally_consistent():
    rows = parse_sise(SAMPLE, "060240")
    for r in rows:
        n = dict(zip(DAILY_BAR_SOURCE_COLUMNS, r))
        assert n["low"] <= n["open"] <= n["high"]
        assert n["low"] <= n["close"] <= n["high"]


def test_empty_or_garbage_body_yields_nothing():
    for body in ("", "  ", "<html>error</html>", "[['날짜','시가']]"):
        assert parse_sise(body, "000020") == []


def test_schema_has_source_and_upsert_preserves_existing(tmp_path):
    """스키마에 source 가 있고, 백필이 기존 행을 덮지 않는지.

    이 둘이 한 테스트인 이유: 컬럼이 빠지면 백필이 sqlite 신규 DB 에서 죽고
    (실제로 마이그레이션만 넣고 CREATE TABLE 을 빠뜨려 그랬다), on_conflict 가
    update 로 새면 키움 실측 거래대금이 네이버 근사치로 덮인다.
    """
    from collectors.naver_delisted_bars import DAILY_BAR_SOURCE_COLUMNS, _insert_bars
    from collectors.storage import connect

    con = connect(tmp_path / "t.db")
    cols = DAILY_BAR_SOURCE_COLUMNS
    assert cols[-1] == "source"

    kiwoom = ("000020", "2020-01-02", 100, 110, 90, 105, 1000, 105, "kiwoom")
    con.executemany(
        f"INSERT INTO daily_bars({','.join(cols)}) VALUES({','.join(['?'] * len(cols))})",
        [kiwoom])
    con.commit()

    naver = ("000020", "2020-01-02", 9, 9, 9, 9, 9, 9, "naver")
    new_day = ("000020", "2020-01-03", 1, 2, 1, 2, 5, 1, "naver")
    _insert_bars(con, [naver, new_day])

    rows = dict(con.execute("SELECT date, source FROM daily_bars").fetchall())
    assert rows["2020-01-02"] == "kiwoom", "기존 키움 행이 덮였다"
    assert rows["2020-01-03"] == "naver", "새 날짜는 들어와야 한다"
    con.close()


def test_empty_probe_is_recorded_so_next_run_skips_it(tmp_path):
    """구간 내 데이터가 없던 코드를 기록해 매주 다시 조회하지 않는지.

    상장폐지는 과거 사실이라 한 번 없으면 영원히 없다. 기록하지 않으면 주간 DAG 가
    매번 ~1,750회의 빈 요청을 보낸다(실측).
    """
    from collectors.naver_delisted_bars import _delisted_codes, _mark_checked
    from collectors.storage import connect

    con = connect(tmp_path / "t.db")
    con.executemany(
        "INSERT INTO delisted_stocks(code, name, market) VALUES(?,?,?)",
        [("000020", "A", "코스닥"), ("000030", "B", "코스닥")])
    con.commit()

    assert set(_delisted_codes(con)) == {"000020", "000030"}

    _mark_checked(con, ["000020"], "2026-08-15")
    assert _delisted_codes(con) == ["000030"], "확인된 코드는 다음 회차에서 빠져야 한다"
    assert set(_delisted_codes(con, refetch=True)) == {"000020", "000030"}, \
        "--refetch 는 전량 복원해야 한다"
    con.close()


def test_insert_reports_actual_rows_not_attempted(tmp_path):
    """DO NOTHING 에서 '기록' 수가 실제 삽입 수여야 한다.

    len(records) 를 돌려주면 "13,316행 기록"이라 보고해놓고 실제로는 0행인 일이
    생긴다(실측). 그 수를 보고 "더 받을 게 없다"를 판단하므로 틀리면 재조회가 영영
    끝나지 않는다.
    """
    from collectors.naver_delisted_bars import DAILY_BAR_SOURCE_COLUMNS, _insert_bars
    from collectors.storage import connect

    con = connect(tmp_path / "t.db")
    rows = [("000020", "2020-01-02", 1, 2, 1, 2, 5, 1, "naver"),
            ("000020", "2020-01-03", 1, 2, 1, 2, 5, 1, "naver")]
    assert len(DAILY_BAR_SOURCE_COLUMNS) == len(rows[0])

    assert _insert_bars(con, rows) == 2, "신규 2행"
    assert _insert_bars(con, rows) == 0, "전부 중복이면 0이어야 한다"
    assert _insert_bars(con, [*rows, ("000020", "2020-01-06", 1, 2, 1, 2, 5, 1, "naver")]) == 1
    con.close()
