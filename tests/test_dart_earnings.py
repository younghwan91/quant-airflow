"""DART earnings collection via krx-fundamentals-client's ``DartScraper``.

Network is faked by monkeypatching ``DartScraper`` methods directly (no real
HTTP) — the rotation/failure-detection logic is what's under test here, not
DART's wire format (that lives in krx-fundamentals-client's own test suite).
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import datetime

import pytest
from krx_fundamentals_client import DartQuotaExceededError, DartScraper, FinancialStatement, ReportType

from collectors import dart_earnings, storage
from collectors.dart_earnings import (
    _fetch_multi_with_rotation,
    _fetch_with_rotation,
    _load_corp_map_with_rotation,
    _period_placeholders,
    _recent_quarters,
    _statement_to_tuple,
    _universe_query,
    collect_all_financials_batched,
    collect_keys,
    yoy_growth,
)


def _stmt(ticker="005930", year=2023, report_type=ReportType.Q1, **kw) -> FinancialStatement:
    return FinancialStatement(ticker=ticker, year=year, report_type=report_type, **kw)


def _run(coro):
    return asyncio.run(coro)


def test_yoy_growth_math_and_guards():
    assert yoy_growth(200.0, 100.0) == 1.0            # +100%
    assert yoy_growth(50.0, 100.0) == -0.5            # -50%
    assert yoy_growth(10.0, -20.0) == 1.5             # divides by |prior|
    assert yoy_growth(10.0, 0) is None                # no divide-by-zero
    assert yoy_growth(None, 100.0) is None


def test_statement_to_tuple_extracts_current_and_prior():
    stmt = _stmt(net_income=200.0, net_income_prior=100.0,
                 revenue=1000.0, revenue_prior=900.0,
                 operating_income=300.0, operating_income_prior=250.0)
    assert _statement_to_tuple(stmt) == (200.0, 100.0, 1000.0, 900.0, 300.0, 250.0)


def test_statement_to_tuple_none_is_all_none():
    assert _statement_to_tuple(None) == (None,) * 6


def test_collect_keys_priority_order(monkeypatch):
    monkeypatch.setenv("DART_API_KEY", "k1")
    monkeypatch.setenv("DART_API_KEY_2", "k2")
    monkeypatch.delenv("DART_API_KEY_3", raising=False)
    assert collect_keys() == ["k1", "k2"]
    monkeypatch.delenv("DART_API_KEY", raising=False)
    monkeypatch.delenv("DART_API_KEY_2", raising=False)
    assert collect_keys() == []          # 키 없으면 빈 리스트


def test_fetch_rotates_to_next_key_on_daily_limit(monkeypatch):
    # 키1은 일한도(020), 키2는 정상 → 로테이션 후 키2 데이터 반환.
    calls = []
    good = _stmt(net_income=200.0, net_income_prior=100.0, revenue=1000.0, revenue_prior=900.0)

    async def fake_fetch_financials(self, ticker, year, report_type):
        calls.append(self.api_key)
        if self.api_key == "k1":
            raise DartQuotaExceededError("한도초과")
        return good

    monkeypatch.setattr(DartScraper, "fetch_financials", fake_fetch_financials)
    scrapers = {"k1": DartScraper(api_key="k1"), "k2": DartScraper(api_key="k2")}
    ki = [0]
    ni, nip, rev, revp, oi, oip = _run(
        _fetch_with_rotation(scrapers, ["k1", "k2"], ki, "005930", 2023, 1))
    assert ki[0] == 1                    # 키2로 로테이션됨
    assert calls == ["k1", "k2"]         # k1(020) 후 k2 재시도
    assert (ni, rev) == (200.0, 1000.0)  # 키2 데이터 파싱됨


def test_fetch_no_rotation_when_single_key_limited(monkeypatch):
    # 키 하나뿐인데 020이면 로테이션 불가 → all-None 반환(스킵), 무한루프 없음.
    async def always_exhausted(self, ticker, year, report_type):
        raise DartQuotaExceededError("한도초과")

    monkeypatch.setattr(DartScraper, "fetch_financials", always_exhausted)
    scrapers = {"only": DartScraper(api_key="only")}
    ki = [0]
    assert _run(_fetch_with_rotation(scrapers, ["only"], ki, "005930", 2023, 1)) == (None,) * 6
    assert ki[0] == 0


def test_write_row_db_upserts_correct_tuple_shape(monkeypatch):
    # --db-table 모드는 CSV 대신 storage.upsert_earnings를 호출해야 하고,
    # 튜플 순서는 _EARNINGS_COLS(code,period,avail_date,knowledge_date,netinc,
    # netinc_prior,revenue,revenue_prior,op_income,op_income_prior)와 정확히 일치해야 한다.
    calls = []
    monkeypatch.setattr(
        "collectors.storage.upsert_earnings",
        lambda con, records: calls.append((con, records)),
    )
    dart_earnings._write_row_db(
        "fake_con", "005930", "2023Q1", "20230515", "20230515",
        200.0, 100.0, 1000.0, 900.0, 300.0, 250.0,
    )
    assert len(calls) == 1
    con, records = calls[0]
    assert con == "fake_con"
    assert records == [("005930", "2023Q1", "20230515", "20230515",
                        200.0, 100.0, 1000.0, 900.0, 300.0, 250.0)]


def test_write_row_db_passes_none_through_without_coercion(monkeypatch):
    # DB 경로는 CSV 경로(_c)와 달리 빈 값을 ""로 바꾸지 않고 None 그대로 넘겨야 한다
    # (psycopg2/sqlite가 NULL을 네이티브로 처리하므로).
    calls = []
    monkeypatch.setattr(
        "collectors.storage.upsert_earnings",
        lambda con, records: calls.append(records),
    )
    dart_earnings._write_row_db(
        "fake_con", "005930", "2023Q1", "20230515", "20230515",
        -50.0, None, None, None, None, None,
    )
    assert calls == [[("005930", "2023Q1", "20230515", "20230515",
                       -50.0, None, None, None, None, None)]]


def test_recent_quarters_within_year():
    assert _recent_quarters(2, today=datetime(2026, 7, 11)) == [(2026, 3), (2026, 2)]


def test_recent_quarters_crosses_year_boundary():
    assert _recent_quarters(5, today=datetime(2026, 7, 11)) == [
        (2026, 3), (2026, 2), (2026, 1), (2025, 4), (2025, 3),
    ]


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
    # 신규상장(IPO) 종목은 daily_bars에 최근 며칠치만 있고 과거 이력이 없다 —
    # --all-codes 유니버스 쿼리가 이런 종목도 상장 첫날부터 바로 포함하는지 검증.
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


def test_new_stock_with_no_prior_earnings_is_never_skipped():
    # 신규상장 종목은 earnings 테이블에 (code, period) 이력이 전혀 없다 —
    # done_periods 조회에서 빈 집합이 나와야 하고, 그 종목의 모든 분기가
    # (기존 종목의 새 분기와 마찬가지로) 정상적으로 fetch 대상이어야 한다.
    con = storage.connect(":memory:")
    storage.upsert_earnings(con, [
        ("005930", "2023Q1", "20230515", "20230515", 200.0, 100.0, 1000.0, 900.0, 300.0, 250.0),
    ])

    import pandas as pd
    existing = pd.read_sql_query("SELECT code, period FROM earnings", con)
    done_periods = set(zip(existing["code"], existing["period"]))

    new_code = "999999"
    assert not any(code == new_code for code, _ in done_periods)
    assert (new_code, "2023Q1") not in done_periods  # 스킵 대상 아님 → fetch 진행


def test_load_corp_map_rotates_past_key_with_daily_limit(monkeypatch):
    # 키1이 020(한도초과)이면 키2로 넘어가서 정상 로드되어야 한다 — 실제 장애 재현:
    # 14.5시간 백필 중 키1이 한도에 걸려 정상 종료됐는데, 바로 재트리거하니
    # 키1만 써서 로테이션 없이 즉시 죽었던 버그.
    calls = []

    async def fake_load_corp_codes(self):
        calls.append(self.api_key)
        if self.api_key == "k1":
            raise DartQuotaExceededError("한도초과")
        return {"005930": "00126380"}

    monkeypatch.setattr(DartScraper, "load_corp_codes", fake_load_corp_codes)
    scrapers = {"k1": DartScraper(api_key="k1"), "k2": DartScraper(api_key="k2")}
    result = _run(_load_corp_map_with_rotation(scrapers, ["k1", "k2"]))
    assert calls == ["k1", "k2"]
    assert result == {"005930": "00126380"}


def test_load_corp_map_rotation_raises_after_all_keys_exhausted(monkeypatch):
    async def always_exhausted(self):
        raise DartQuotaExceededError("한도초과")

    monkeypatch.setattr(DartScraper, "load_corp_codes", always_exhausted)
    scrapers = {"k1": DartScraper(api_key="k1"), "k2": DartScraper(api_key="k2")}
    with pytest.raises(DartQuotaExceededError):
        _run(_load_corp_map_with_rotation(scrapers, ["k1", "k2"]))


def test_fetch_multi_rotates_to_next_key_on_daily_limit(monkeypatch):
    good = {"005930": _stmt(net_income=200.0, net_income_prior=100.0)}
    calls = []

    async def fake_fetch_batch(self, tickers, year, report_type, on_status=None):
        calls.append(self.api_key)
        if self.api_key == "k1":
            raise DartQuotaExceededError("한도초과")
        return good

    monkeypatch.setattr(DartScraper, "fetch_financials_batch", fake_fetch_batch)
    scrapers = {"k1": DartScraper(api_key="k1"), "k2": DartScraper(api_key="k2")}
    ki = [0]
    out, failure = _run(_fetch_multi_with_rotation(scrapers, ["k1", "k2"], ki, ["005930"], 2023, 1))
    assert ki[0] == 1
    assert calls == ["k1", "k2"]
    assert out["005930"][:2] == (200.0, 100.0)
    assert failure is None, "로테이션이 성공했으면 실패로 세면 안 된다"


def test_fetch_multi_no_rotation_when_all_keys_exhausted(monkeypatch):
    async def always_exhausted(self, tickers, year, report_type, on_status=None):
        raise DartQuotaExceededError("한도초과")

    monkeypatch.setattr(DartScraper, "fetch_financials_batch", always_exhausted)
    scrapers = {"only": DartScraper(api_key="only")}
    ki = [0]
    out, failure = _run(_fetch_multi_with_rotation(scrapers, ["only"], ki, ["005930"], 2023, 1))
    assert out == {"005930": (None,) * 6}
    assert failure == "020"


def test_collect_all_financials_batched_chunks_by_batch_size_and_skips_done_periods():
    # 4개 종목 중 1개는 done_periods라 배치대상에서 먼저 빠지고, 남은 3개가
    # batch_size=2로 2개 배치(2+1)로 분할돼야 한다. 레주메(완료분 재수집 안 함)도 확인.
    corp_map = {
        "005930": "00126380", "000660": "00164779",
        "035420": "00266961", "051910": "00356361",
    }
    calls = []

    async def fake_fetch_multi_with_rotation(scrapers, keys, ki, tickers, year, quarter):
        calls.append(list(tickers))
        return {t: (100.0, 50.0, None, None, None, None) for t in tickers}, None

    orig = dart_earnings._fetch_multi_with_rotation
    dart_earnings._fetch_multi_with_rotation = fake_fetch_multi_with_rotation
    try:
        rows = _run(collect_all_financials_batched(
            {"k1": None}, ["k1"], corp_map, [(2023, 1)], batch_size=2, sleep=0.0,
            done_periods={("000660", "2023Q1")}, today="20991231"))
    finally:
        dart_earnings._fetch_multi_with_rotation = orig

    assert len(calls) == 2                       # 3종목(1개 제외) → 배치 2개(2+1)
    assert sum(len(c) for c in calls) == 3        # 000660은 done_periods라 배치 대상에서 빠짐
    codes_seen = {r[0] for r in rows}
    assert codes_seen == {"005930", "035420", "051910"}
    assert "000660" not in codes_seen             # 레주메: 이미 완료분 재수집 안 함
    row = next(r for r in rows if r[0] == "005930")
    assert row[1] == "2023Q1"
    assert row[4:6] == (100.0, 50.0)              # netinc, netinc_prior


def test_a_quota_exhausted_batch_is_reported_not_swallowed():
    """일한도 소진이 `DONE rows=0` 으로 조용히 성공 보고되던 경로.

    예전엔 네트워크/한도 실패가 조용히 all-None을 만들고 → 호출부의
    `if ni is None: continue`가 버려서, Airflow는 성공으로 기록하고 retries도
    발동하지 않았다.
    """
    async def exhausted(scrapers, keys, ki, tickers, year, quarter):
        return {t: (None,) * 6 for t in tickers}, "020"

    orig = dart_earnings._fetch_multi_with_rotation
    dart_earnings._fetch_multi_with_rotation = exhausted
    try:
        failures: list[tuple[str, str]] = []
        rows = _run(dart_earnings.collect_all_financials_batched(
            {"k1": None}, ["k1"], {"005930": "00126380"}, [(2023, 1)], sleep=0.0,
            today="20991231", failures=failures))
    finally:
        dart_earnings._fetch_multi_with_rotation = orig

    assert rows == []
    assert failures == [("2023Q1", "020")]


def test_a_quarter_with_no_filing_is_not_a_failure():
    """013 = '조회된 데이터가 없습니다' 는 정상이다 — 이걸 실패로 세면 매일 죽는다."""
    async def no_data(scrapers, keys, ki, tickers, year, quarter):
        return {t: (None,) * 6 for t in tickers}, None

    orig = dart_earnings._fetch_multi_with_rotation
    dart_earnings._fetch_multi_with_rotation = no_data
    try:
        failures: list[tuple[str, str]] = []
        _run(dart_earnings.collect_all_financials_batched(
            {"k1": None}, ["k1"], {"005930": "00126380"}, [(2023, 1)], sleep=0.0,
            today="20991231", failures=failures))
    finally:
        dart_earnings._fetch_multi_with_rotation = orig

    assert failures == []


def test_period_placeholders_are_valid_pyformat():
    """2026-08-27 daily_earnings 를 두 번 죽인 회귀.

    자리표시자를 ``"%(p%d)s" % i`` 로 만들면 파이썬이 ``%(...)s`` 를 매핑 키로 읽어
    ``TypeError: format requires a mapping`` 이 난다. 이 테스트는 ``main()`` 이
    실제로 부르는 함수를 그대로 부른다 — 표현식을 손으로 옮겨 적으면 옮기면서
    고쳐 써서 통과해버린다(그게 원래 사고의 원인이었다).
    """
    ph, params = _period_placeholders([(2026, 2), (2026, 1)])
    assert ph == "%(p0)s,%(p1)s"
    assert params == {"p0": "2026Q1", "p1": "2026Q2"}
    # psycopg2 가 실제로 바인딩하는 모양인지 — 문자열 보간이 성립해야 한다.
    assert f"period IN ({ph})" % params == "period IN ('2026Q1','2026Q2')".replace("'", "")


def test_period_placeholders_dedupe_and_sort():
    ph, params = _period_placeholders([(2025, 4), (2026, 1), (2025, 4)])
    assert ph == "%(p0)s,%(p1)s"
    assert list(params.values()) == ["2025Q4", "2026Q1"]


def test_period_placeholders_empty_is_caller_guarded():
    """빈 periods 는 `IN ()` 를 만든다 — main() 이 `and periods` 로 막는다."""
    ph, params = _period_placeholders([])
    assert ph == "" and params == {}

