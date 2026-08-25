"""Naver consensus parsing. Pure JSON in -> numbers out (no network)."""

from __future__ import annotations

import argparse

from collectors import storage
from collectors.naver_consensus import _universe_query, parse_consensus, parse_estimate


def test_parse_consensus_extracts_target_and_recommendation():
    payload = {"consensusInfo": {
        "itemCode": "005930", "createDate": "2026-07-09",
        "recommMean": "4.04", "priceTargetMean": "513,958",
    }}
    tm, rm, bd = parse_consensus(payload)
    assert tm == 513958.0
    assert rm == 4.04
    assert bd == "2026-07-09"


def test_parse_consensus_no_coverage_returns_none():
    assert parse_consensus({}) == (None, None, None)
    assert parse_consensus({"consensusInfo": {}}) == (None, None, None)


def test_parse_consensus_handles_partial_fields():
    tm, rm, bd = parse_consensus({"consensusInfo": {"priceTargetMean": "1,000"}})
    assert tm == 1000.0
    assert rm is None
    assert bd is None


def _finance(titles, eps_cols):
    return {"financeInfo": {
        "trTitleList": titles,
        "rowList": [{"title": {"name": "EPS"}, "columns": eps_cols}],
    }}


def test_parse_estimate_extracts_forward_and_prior_eps():
    payload = _finance(
        [{"key": "202412", "isConsensus": "N"}, {"key": "202512", "isConsensus": "N"},
         {"key": "202612", "isConsensus": "Y"}],
        {"202412": {"value": "4,950"}, "202512": {"value": "6,564"},
         "202612": {"value": "46,664"}},
    )
    fwd, prev, ey = parse_estimate(payload)
    assert fwd == 46664.0        # 2026 consensus estimate
    assert prev == 6564.0        # latest actual (2025)
    assert ey == "202612"


def test_parse_estimate_no_consensus_year_returns_none():
    payload = _finance([{"key": "202512", "isConsensus": "N"}], {"202512": {"value": "1"}})
    assert parse_estimate(payload) == (None, None, None)
    assert parse_estimate({}) == (None, None, None)


def test_universe_query_all_codes_has_no_limit():
    args = argparse.Namespace(all_codes=True, top_n=800)
    sql, params = _universe_query(args)
    assert "LIMIT" not in sql
    assert "DISTINCT" in sql
    assert params == {}


def test_universe_query_default_uses_top_n_limit():
    args = argparse.Namespace(all_codes=False, top_n=800)
    sql, params = _universe_query(args)
    assert "LIMIT %(n)s" in sql
    assert params == {"n": 800}


def test_all_codes_universe_includes_newly_listed_stock():
    # 신규상장 종목은 daily_bars에 오늘치 한 줄만 있고 90일 유동성 윈도우
    # 밖이라 top-N에는 안 잡힐 수 있다 — --all-codes는 그런 종목도 상장
    # 첫날부터 바로 포함해야 한다.
    con = storage.connect(":memory:")
    storage.upsert_daily_bars(con, [
        ("005930", "2026-07-10", 70000, 71000, 69500, 70500, 1000000, 70000000000),
        ("999999", "2026-07-10", 10000, 10500, 9800, 10200, 50000, 500000000),  # 오늘 상장한 신규종목
    ])
    args = argparse.Namespace(all_codes=True, top_n=800)
    sql, params = _universe_query(args)

    import pandas as pd
    codes = pd.read_sql_query(sql, con, params=params)["code"].tolist()
    assert "999999" in codes
    assert "005930" in codes


def test_db_table_upserts_correct_tuple_shape():
    con = storage.connect(":memory:")
    storage.upsert_consensus(con, [
        ("005930", "2026-07-11", 513958.0, 4.04, "2026-07-09", 46664.0, 6564.0, "202612"),
    ])

    import pandas as pd
    row = pd.read_sql_query("SELECT * FROM consensus", con).iloc[0]
    assert row["code"] == "005930"
    assert row["date"] == "2026-07-11"
    assert row["target_mean"] == 513958.0
    assert row["recomm_mean"] == 4.04
    assert row["fwd_eps"] == 46664.0


# ------------------------------------------------- 유니버스 축소 · sleep 낭비


def test_covered_only_universe_reads_the_consensus_table():
    """전종목 2,627개 중 실제 적재는 하루 660행 — 73%가 매일 헛돈다."""
    import argparse

    from collectors.naver_consensus import _universe_query

    sql, params = _universe_query(argparse.Namespace(
        covered_days=90, all_codes=True, top_n=800))

    assert "FROM consensus" in sql
    assert params == {"d": 90}


def test_covered_days_zero_keeps_the_old_behaviour():
    """기본값이 켜지면 조용히 유니버스가 줄어든다 — 명시할 때만 걸려야 한다."""
    import argparse

    from collectors.naver_consensus import _universe_query

    sql, _ = _universe_query(argparse.Namespace(
        covered_days=0, all_codes=True, top_n=800))

    assert "FROM daily_bars" in sql


def test_a_work_unit_sleeps_once_not_twice(monkeypatch):
    """작업 단위 마지막 sleep 은 다음 요청을 늦추지 않고 워커만 놀린다."""
    import collectors.naver_consensus as nc

    naps = []
    monkeypatch.setattr(nc.time, "sleep", lambda s: naps.append(s))
    monkeypatch.setattr(nc, "fetch_consensus", lambda c: (1.0, 2.0, "20260821"))
    monkeypatch.setattr(nc, "fetch_estimate", lambda c: (3.0, 4.0, "202612"))

    nc._fetch_both("005930", 0.2)

    assert naps == [0.2], "요청 사이 한 번만 자야 한다"
