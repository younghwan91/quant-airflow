"""DART earnings parsing. Pure JSON in -> numbers out (no network)."""

from __future__ import annotations

import argparse
from datetime import datetime

from collectors import storage
from collectors import dart_earnings
from collectors.dart_earnings import (
    _recent_quarters,
    _universe_query,
    collect_all_financials_batched,
    collect_keys,
    load_corp_map_with_rotation,
    parse_financials,
    parse_financials_multi,
    parse_net_income,
    yoy_growth,
)


def _payload(rows, status="000"):
    return {"status": status, "list": rows}


def test_parse_net_income_picks_current_and_prior():
    payload = _payload([
        {"account_nm": "매출액", "thstrm_amount": "1,000", "frmtrm_amount": "900"},
        {"account_nm": "당기순이익", "thstrm_amount": "2,206,125", "frmtrm_amount": "974,571"},
    ])
    ni, nip = parse_net_income(payload)
    assert ni == 2206125.0
    assert nip == 974571.0


def test_parse_net_income_handles_loss_label_and_blanks():
    payload = _payload([
        {"account_nm": "당기순이익(손실)", "thstrm_amount": "-500", "frmtrm_amount": ""},
    ])
    ni, nip = parse_net_income(payload)
    assert ni == -500.0
    assert nip is None


def test_parse_net_income_missing_or_error_returns_none():
    assert parse_net_income({"status": "013"}) == (None, None)  # no data
    assert parse_net_income(_payload([{"account_nm": "자산총계", "thstrm_amount": "5"}])) == (None, None)


def test_yoy_growth_math_and_guards():
    assert yoy_growth(200.0, 100.0) == 1.0            # +100%
    assert yoy_growth(50.0, 100.0) == -0.5            # -50%
    assert yoy_growth(10.0, -20.0) == 1.5             # divides by |prior|
    assert yoy_growth(10.0, 0) is None                # no divide-by-zero
    assert yoy_growth(None, 100.0) is None


def test_parse_financials_extracts_all_three_lines():
    payload = _payload([
        {"account_nm": "매출액", "thstrm_amount": "1,000", "frmtrm_amount": "900"},
        {"account_nm": "영업이익", "thstrm_amount": "300", "frmtrm_amount": "250"},
        {"account_nm": "당기순이익", "thstrm_amount": "200", "frmtrm_amount": "100"},
    ])
    ni, nip, rev, revp, oi, oip = parse_financials(payload)
    assert (ni, nip) == (200.0, 100.0)
    assert (rev, revp) == (1000.0, 900.0)
    assert (oi, oip) == (300.0, 250.0)


def test_parse_financials_revenue_variant_and_missing_legs():
    # 수익(매출액) 변형은 revenue로 잡히고, 영업이익 없으면 op_income None, 순이익은 정상.
    payload = _payload([
        {"account_nm": "수익(매출액)", "thstrm_amount": "5,000", "frmtrm_amount": "4,000"},
        {"account_nm": "당기순이익(손실)", "thstrm_amount": "-50", "frmtrm_amount": ""},
    ])
    ni, nip, rev, revp, oi, oip = parse_financials(payload)
    assert (ni, nip) == (-50.0, None)
    assert (rev, revp) == (5000.0, 4000.0)
    assert (oi, oip) == (None, None)          # 영업이익 라인 없음 → 크래시 없이 None


def test_parse_financials_error_status_all_none():
    assert parse_financials({"status": "013"}) == (None,) * 6


def test_parse_net_income_still_backward_compatible():
    # 기존 시그니처·동작 불변 (하위호환).
    payload = _payload([
        {"account_nm": "당기순이익", "thstrm_amount": "2,206,125", "frmtrm_amount": "974,571"},
    ])
    assert parse_net_income(payload) == (2206125.0, 974571.0)


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
    good = _payload([
        {"account_nm": "매출액", "thstrm_amount": "1,000", "frmtrm_amount": "900"},
        {"account_nm": "당기순이익", "thstrm_amount": "200", "frmtrm_amount": "100"},
    ])
    calls = []

    def fake_payload(api_key, corp_code, year, quarter):
        calls.append(api_key)
        return {"status": "020", "message": "한도초과"} if api_key == "k1" else good

    monkeypatch.setattr(dart_earnings, "_fetch_payload", fake_payload)
    ki = [0]
    ni, nip, rev, revp, oi, oip = dart_earnings._fetch_with_rotation(["k1", "k2"], ki, "c", 2023, 1)
    assert ki[0] == 1                    # 키2로 로테이션됨
    assert calls == ["k1", "k2"]         # k1(020) 후 k2 재시도
    assert (ni, rev) == (200.0, 1000.0)  # 키2 데이터 파싱됨


def test_fetch_no_rotation_when_single_key_limited(monkeypatch):
    # 키 하나뿐인데 020이면 로테이션 불가 → None 반환(스킵), 무한루프 없음.
    monkeypatch.setattr(dart_earnings, "_fetch_payload",
                        lambda *a: {"status": "020"})
    ki = [0]
    assert dart_earnings._fetch_with_rotation(["only"], ki, "c", 2023, 1) == (None,) * 6
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


def test_db_table_resume_skips_existing_code_period_but_fetches_new_period(monkeypatch):
    con = storage.connect(":memory:")
    storage.upsert_earnings(con, [
        ("005930", "2023Q1", "20230515", "20230515", 200.0, 100.0, 1000.0, 900.0, 300.0, 250.0),
    ])

    import pandas as pd
    existing = pd.read_sql_query("SELECT code, period FROM earnings", con)
    done_periods = set(zip(existing["code"], existing["period"]))
    assert ("005930", "2023Q1") in done_periods

    calls = []

    def fake_fetch_with_rotation(keys, ki, corp_code, year, q):
        calls.append((corp_code, year, q))
        return (10.0, 5.0, None, None, None, None)

    monkeypatch.setattr(dart_earnings, "_fetch_with_rotation", fake_fetch_with_rotation)

    code, corp_code = "005930", "00126380"
    periods = [(2023, 1), (2023, 2)]
    for year, q in periods:
        period = f"{year}Q{q}"
        if (code, period) in done_periods:
            continue
        dart_earnings._fetch_with_rotation([], [0], corp_code, year, q)

    assert calls == [(corp_code, 2023, 2)]


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
    # load_corp_map(keys[0])이 로테이션 없이 키1만 써서 즉시 죽었던 버그.
    calls = []

    def fake_load_corp_map(api_key):
        calls.append(api_key)
        if api_key == "k1":
            raise RuntimeError("DART corpCode 오류 (status='020') — 한도초과(020)/키오류(010) 등 확인")
        return {"005930": "00126380"}

    monkeypatch.setattr(dart_earnings, "load_corp_map", fake_load_corp_map)
    result = load_corp_map_with_rotation(["k1", "k2"])
    assert calls == ["k1", "k2"]
    assert result == {"005930": "00126380"}


def test_load_corp_map_rotation_reraises_non_020_errors_without_trying_next_key(monkeypatch):
    calls = []

    def fake_load_corp_map(api_key):
        calls.append(api_key)
        raise RuntimeError("DART corpCode 오류 (status='010') — 한도초과(020)/키오류(010) 등 확인")

    monkeypatch.setattr(dart_earnings, "load_corp_map", fake_load_corp_map)
    import pytest
    with pytest.raises(RuntimeError, match="010"):
        load_corp_map_with_rotation(["bad_key", "k2"])
    assert calls == ["bad_key"]  # 010(키오류)은 로테이션 대상 아님 — 즉시 재발생, k2 시도 안 함


def test_load_corp_map_rotation_raises_after_all_keys_exhausted(monkeypatch):
    def fake_load_corp_map(api_key):
        raise RuntimeError("DART corpCode 오류 (status='020') — 한도초과(020)/키오류(010) 등 확인")

    monkeypatch.setattr(dart_earnings, "load_corp_map", fake_load_corp_map)
    import pytest
    with pytest.raises(RuntimeError, match="020"):
        load_corp_map_with_rotation(["k1", "k2"])


def test_parse_financials_multi_groups_rows_by_corp_code():
    # fnlttMultiAcnt는 단일회사 응답과 달리 row마다 corp_code가 붙어 여러 회사가
    # 한 list에 섞여 온다 — corp_code별로 분리해 각자 (ni,nip,rev,revp,oi,oip)를 뽑아야 한다.
    payload = _payload([
        {"corp_code": "00126380", "account_nm": "매출액", "thstrm_amount": "1,000", "frmtrm_amount": "900"},
        {"corp_code": "00126380", "account_nm": "당기순이익", "thstrm_amount": "200", "frmtrm_amount": "100"},
        {"corp_code": "00164779", "account_nm": "매출액", "thstrm_amount": "5,000", "frmtrm_amount": "4,000"},
        {"corp_code": "00164779", "account_nm": "당기순이익", "thstrm_amount": "800", "frmtrm_amount": "600"},
    ])
    out = parse_financials_multi(payload, ["00126380", "00164779"])
    assert out["00126380"] == (200.0, 100.0, 1000.0, 900.0, None, None)
    assert out["00164779"] == (800.0, 600.0, 5000.0, 4000.0, None, None)


def test_parse_financials_multi_fills_none_for_companies_with_no_rows():
    # 배치에 넣은 회사인데 그 분기에 공시가 없으면(신설/휴장 등) 전부 None — KeyError 없이.
    payload = _payload([
        {"corp_code": "00126380", "account_nm": "당기순이익", "thstrm_amount": "200", "frmtrm_amount": "100"},
    ])
    out = parse_financials_multi(payload, ["00126380", "00999999"])
    assert out["00126380"][:2] == (200.0, 100.0)
    assert out["00999999"] == (None, None, None, None, None, None)


def test_parse_financials_multi_error_status_all_none():
    out = parse_financials_multi({"status": "013"}, ["00126380", "00164779"])
    assert out == {"00126380": (None,) * 6, "00164779": (None,) * 6}


def test_fetch_multi_rotates_to_next_key_on_daily_limit(monkeypatch):
    good = _payload([
        {"corp_code": "00126380", "account_nm": "당기순이익", "thstrm_amount": "200", "frmtrm_amount": "100"},
    ])
    calls = []

    def fake_multi_payload(api_key, corp_codes, year, quarter):
        calls.append(api_key)
        return {"status": "020"} if api_key == "k1" else good

    monkeypatch.setattr(dart_earnings, "_fetch_multi_payload", fake_multi_payload)
    ki = [0]
    out, failure = dart_earnings._fetch_multi_with_rotation(
        ["k1", "k2"], ki, ["00126380"], 2023, 1)
    assert ki[0] == 1
    assert calls == ["k1", "k2"]
    assert out["00126380"][:2] == (200.0, 100.0)
    assert failure is None, "로테이션이 성공했으면 실패로 세면 안 된다"


def test_collect_all_financials_batched_chunks_by_batch_size_and_skips_done_periods():
    # 4개 종목 중 1개는 done_periods라 배치대상에서 먼저 빠지고, 남은 3개가
    # batch_size=2로 2개 배치(2+1)로 분할돼야 한다. 레주메(완료분 재수집 안 함)도 확인.
    corp_map = {
        "005930": "00126380", "000660": "00164779",
        "035420": "00266961", "051910": "00356361",
    }
    calls = []

    def fake_fetch_multi_with_rotation(keys, ki, corp_codes, year, quarter):
        calls.append(list(corp_codes))
        return {cc: (100.0, 50.0, None, None, None, None) for cc in corp_codes}, None

    import collectors.dart_earnings as mod
    orig = mod._fetch_multi_with_rotation
    mod._fetch_multi_with_rotation = fake_fetch_multi_with_rotation
    try:
        rows = collect_all_financials_batched(
            ["k1"], corp_map, [(2023, 1)], batch_size=2, sleep=0.0,
            done_periods={("000660", "2023Q1")}, today="20991231",
        )
    finally:
        mod._fetch_multi_with_rotation = orig

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

    `_get_json` 이 3회 재시도 뒤 `{}` 를 주고 → 파서가 전원 all-None 을 만들고
    → 호출부의 `if ni is None: continue` 가 버려서, Airflow 는 성공으로 기록하고
    retries 도 발동하지 않았다.
    """
    import collectors.dart_earnings as mod

    def exhausted(api_key, corp_codes, year, quarter):
        return {"status": "020", "message": "일일 조회 한도 초과"}

    orig = mod._fetch_multi_payload
    mod._fetch_multi_payload = exhausted
    try:
        failures: list[tuple[str, str]] = []
        rows = mod.collect_all_financials_batched(
            ["k1"], {"005930": "00126380"}, [(2023, 1)], sleep=0.0,
            today="20991231", failures=failures)
    finally:
        mod._fetch_multi_payload = orig

    assert rows == []
    assert failures == [("2023Q1", "020")]


def test_a_quarter_with_no_filing_is_not_a_failure():
    """013 = '조회된 데이터가 없습니다' 는 정상이다 — 이걸 실패로 세면 매일 죽는다."""
    import collectors.dart_earnings as mod

    def no_data(api_key, corp_codes, year, quarter):
        return {"status": "013", "message": "조회된 데이터가 없습니다"}

    orig = mod._fetch_multi_payload
    mod._fetch_multi_payload = no_data
    try:
        failures: list[tuple[str, str]] = []
        mod.collect_all_financials_batched(
            ["k1"], {"005930": "00126380"}, [(2023, 1)], sleep=0.0,
            today="20991231", failures=failures)
    finally:
        mod._fetch_multi_payload = orig

    assert failures == []
