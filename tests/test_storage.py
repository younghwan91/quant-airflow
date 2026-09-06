"""Storage layer (write side): schema, numeric coercion, idempotent upserts. No network.

Read-side tests (market_cap_asof, connect() dispatch) live in
kr-quant/tests/test_storage.py alongside kr_quant/storage.py's read half.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from collectors.storage import (
    DAILY_BAR_COLUMNS,
    SUPPLY_DEMAND_COLUMNS,
    _EARNINGS_COLS,
    _upsert,
    connect,
    to_float,
    to_int,
    upsert_earnings,
    upsert_stocks,
    upsert_supply_demand,
)


def _earnings(code, period, avail_date, knowledge_date, netinc):
    """One earnings row ordered by _EARNINGS_COLS; only netinc varies per restatement."""
    values = {"code": code, "period": period, "avail_date": avail_date,
              "knowledge_date": knowledge_date, "netinc": netinc, "netinc_prior": 1.0,
              "revenue": 10.0, "revenue_prior": 9.0, "op_income": 2.0, "op_income_prior": 1.5}
    return tuple(values[c] for c in _EARNINGS_COLS)


def test_to_int_handles_kiwoom_strings():
    assert to_int("+322500") == 322500
    assert to_int("-1979879") == -1979879
    assert to_int("") == 0
    assert to_int(None) == 0
    assert to_int("abc") == 0


def test_to_float_handles_signs():
    assert to_float("+7.86") == 7.86
    assert to_float("") == 0.0


def test_upsert_is_idempotent(tmp_path):
    con = connect(tmp_path / "t.db")
    upsert_stocks(con, [{"code": "005930", "name": "삼성전자",
                         "market": "거래소", "sector": "전기/전자", "kind": "A"}])

    record = tuple(
        [{"code": "005930", "date": "20260612", "close": 322500, "flu_rt": 7.86,
          "acc_trde_qty": 31006148, "individual": -1979879, "foreign_": 971587,
          "institution": 1097529, "fnnc_invt": 0, "insrnc": 0, "invtrt": 0,
          "bank": 0, "penfnd_etc": 0, "samo_fund": 0, "natn": 0, "etc_corp": 0}[c]
         for c in SUPPLY_DEMAND_COLUMNS]
    )
    upsert_supply_demand(con, [record])
    upsert_supply_demand(con, [record])  # same PK again

    n = con.execute("SELECT COUNT(*) FROM supply_demand").fetchone()[0]
    assert n == 1  # INSERT OR REPLACE → no duplicate
    row = con.execute("SELECT foreign_ FROM supply_demand").fetchone()
    assert row["foreign_"] == 971587
    con.close()


def test_upsert_uses_on_conflict_for_postgres_connection():
    """Non-sqlite connections get ON CONFLICT DO UPDATE via execute_values, not INSERT OR REPLACE.

    execute_values itself is psycopg2's own (already well-tested) code, so it's
    patched out here — this test only needs to prove _upsert builds the right
    ON CONFLICT SQL and passes the records through.
    """
    fake_con = MagicMock()
    fake_cursor = MagicMock()
    fake_con.cursor.return_value.__enter__.return_value = fake_cursor
    records = [("005930", "20260706", 100)]

    with patch("psycopg2.extras.execute_values") as execute_values:
        n = _upsert(fake_con, "daily_bars", ["code", "date", "close"], records)

    assert n == 1
    execute_values.assert_called_once()
    call_args = execute_values.call_args[0]
    assert call_args[0] is fake_cursor
    sql = call_args[1]
    assert "ON CONFLICT (code,date) DO UPDATE SET close=EXCLUDED.close" in sql
    assert call_args[2] == records
    fake_con.commit.assert_called_once()


def _bar(code, date, close):
    values = {"code": code, "date": date, "open": close, "high": close,
              "low": close, "close": close, "volume": 0, "trade_value": 0}
    return tuple(values[c] for c in DAILY_BAR_COLUMNS)


def test_upsert_earnings_keeps_the_value_that_was_known_before_a_restatement(tmp_path):
    """A DART restatement must add a version, not overwrite what we knew earlier.

    The daily DAG re-collects the two most recent quarters every weekday, so a
    revised figure lands on a (code, period) that already has a row. Overwriting
    it makes any backtest of that window read today's number as if it had been
    known at the time.
    """
    con = connect(tmp_path / "t.db")
    upsert_earnings(con, [_earnings("005930", "2024Q1", "20240515", "20240515", 6.6e12)])
    upsert_earnings(con, [_earnings("005930", "2024Q1", "20240515", "20241114", 6.4e12)])

    rows = con.execute(
        "SELECT knowledge_date, netinc FROM earnings ORDER BY knowledge_date"
    ).fetchall()
    assert [r["knowledge_date"] for r in rows] == ["20240515", "20241114"]
    assert [r["netinc"] for r in rows] == [6.6e12, 6.4e12]
    con.close()


def test_upsert_earnings_does_not_add_a_version_when_nothing_changed(tmp_path):
    """Re-collecting the same figures on a later day must not grow the table.

    daily_earnings re-fetches the two most recent quarters for every code each
    weekday. Versioning on collection date alone would append ~2,600 identical
    rows per quarter per day; only a changed figure is a new version.
    """
    con = connect(tmp_path / "t.db")
    upsert_earnings(con, [_earnings("005930", "2024Q1", "20240515", "20240515", 6.6e12)])
    upsert_earnings(con, [_earnings("005930", "2024Q1", "20240515", "20240516", 6.6e12)])

    rows = con.execute("SELECT knowledge_date FROM earnings").fetchall()
    assert [r["knowledge_date"] for r in rows] == ["20240515"]
    con.close()


def test_news_judgments_table_exists():
    from collectors.storage import connect
    con = connect(":memory:")
    cols = {r[1] for r in con.execute("PRAGMA table_info(news_judgments)").fetchall()}
    assert cols == {
        "source_type", "source_id", "ticker", "event_type",
        "sentiment_direction", "related_codes", "is_stale_repeat",
        "first_seen_date", "price_impact_likely", "rationale",
        "model_id", "prompt_version", "knowledge_date",
        "confidence", "judged_at",
    }


def test_upsert_news_judgments_is_idempotent_and_keeps_first_write(tmp_path):
    from collectors.storage import connect, upsert_news_judgments

    con = connect(tmp_path / "t.db")
    # 날짜 컬럼은 이 레포 관례대로 압축형 YYYYMMDD(예: dart_earnings.py의
    # today/avail_date)로 통일 — earnings.knowledge_date와 같은 포맷.
    row = ("news", "toss:abc", "005930", "실적", 1, "[]", 0, None, True,
           "실적 서프라이즈", "gemini-test", "v1", "20260906", 75,
           "2026-09-06T00:00:00+00:00")
    upsert_news_judgments(con, [row])

    # 같은 PK로 재실행 — rationale이 달라져도 기존 행이 안 바뀐다(immutable).
    changed = ("news", "toss:abc", "005930", "실적", -1, "[]", 0, None, True,
               "바뀐 서술", "gemini-test", "v1", "20260906", 10,
               "2026-09-06T01:00:00+00:00")
    upsert_news_judgments(con, [changed])

    rows = con.execute("SELECT rationale FROM news_judgments").fetchall()
    assert len(rows) == 1
    assert rows[0]["rationale"] == "실적 서프라이즈"  # 처음 쓴 값 그대로
    con.close()

