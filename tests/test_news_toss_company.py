"""토스 종목별 뉴스 콜렉터 — 유니버스·페이지 규칙·기존 id 보존·원장·아카이브 이전 (네트워크 불요)."""

from __future__ import annotations

import datetime as dt
import json

import pytest
from krx_news_client.scrapers.base import make_article_id
from krx_news_client.scrapers.toss import build_article_url

from collectors import news_toss_company as m
from collectors.storage import connect, upsert_news_articles

KST = m.KST


def _t(s: str) -> dt.datetime:
    return dt.datetime.fromisoformat(s).replace(tzinfo=KST)


def _raw(nid: str, created: str, title: str = "제목") -> dict:
    return {"id": nid, "title": title, "summary": "요약", "createdAt": created, "source": "news1"}


# --- 순수 함수 ------------------------------------------------------------------

def test_article_id_matches_daily_news_rule():
    """daily_news(토스 하이라이트)와 같은 기사면 같은 id — 한 행으로 합쳐지는 근거."""
    nid = "financial_202610081506192977"
    assert m.article_id(nid) == make_article_id("toss", build_article_url(nid))
    assert m.article_id(nid).startswith("toss:")


def test_select_universe_orders_by_value_then_code_and_drops_market():
    rows = [("000002", 5.0), ("000001", 5.0), ("069500", 9.0), ("000003", 0.0), ("000004", 1.0)]
    assert m.select_universe(rows, size=3) == ["000001", "000002"]  # 069500 은 상위 3 안이지만 뺀다


def test_universes_use_previous_trading_day():
    rows = [("2026-10-05", "AAA", 2.0), ("2026-10-05", "BBB", 1.0),
            ("2026-10-06", "BBB", 3.0), ("2026-10-06", "AAA", 1.0)]
    uni = m.universes(rows, dt.date(2026, 10, 6), dt.date(2026, 10, 7), size=1)
    assert uni == {dt.date(2026, 10, 6): ["AAA"], dt.date(2026, 10, 7): ["BBB"]}


def test_universes_skip_weekends():
    rows = [("2026-10-09", "AAA", 1.0)]
    uni = m.universes(rows, dt.date(2026, 10, 10), dt.date(2026, 10, 11))
    assert uni == {}


def test_page_rows_keeps_new_rows_and_asks_for_more_while_any_is_new():
    since = _t("2026-10-07T00:00:00")
    body = [_raw("a", "2026-10-07T09:00:00"), _raw("b", "2026-10-06T23:59:59"), {"id": "c"}]
    keep, more = m.page_rows(body, since)
    assert [r["id"] for r in keep] == ["a"] and more is True
    keep, more = m.page_rows([_raw("b", "2026-10-06T10:00:00")], since)
    assert keep == [] and more is False


def test_first_seen_is_backfill_end_or_first_covering_run():
    done = _t("2026-09-17T03:58:00")
    runs = [(_t("2026-09-23T16:30:00"), _t("2026-09-23T16:45:00")),
            (_t("2026-09-24T16:30:00"), _t("2026-09-24T16:44:00"))]
    assert m.first_seen_for(_t("2026-09-10T10:00:00"), done, runs) == done
    assert m.first_seen_for(_t("2026-09-24T09:00:00"), done, runs) == _t("2026-09-24T16:44:00")


# --- DB 쓰기 -----------------------------------------------------------------------

def _item(nid: str, code: str, created: str, seen: str = "2026-10-08T17:20:00") -> dict:
    return m.item_from_raw(_raw(nid, created), code, _t(seen))


def test_write_items_skips_existing_id_even_with_other_published_at(tmp_path):
    """토스가 고친 기사(createdAt 이 바뀐 판)를 두 번째 행으로 만들지 않는다."""
    con = connect(tmp_path / "t.db")
    aid = m.article_id("x1")
    upsert_news_articles(con, [(aid, "toss", "breaking", "옛 제목", build_article_url("x1"), "", "",
                                "파이낸셜", "2026-10-07T09:00:00+09:00", "2026-10-07T10:05:00+09:00")])
    stats = m.write_items(con, [_item("x1", "005930", "2026-10-07T09:30:00")], m.SOURCE_FEED)
    assert stats["articles_new"] == 0 and stats["pairs_new"] == 1
    row = con.execute("SELECT title, category FROM news_articles WHERE id=?", (aid,)).fetchone()
    assert tuple(row) == ("옛 제목", "breaking")
    flag = con.execute("SELECT inserted_article FROM news_company_feed").fetchone()[0]
    assert flag == 0


def test_write_items_one_article_many_codes_earliest_version_and_rerun_is_idempotent(tmp_path):
    con = connect(tmp_path / "t.db")
    items = [_item("y1", "005930", "2026-10-07T09:30:00"), _item("y1", "000660", "2026-10-07T09:10:00")]
    first = m.write_items(con, items, m.SOURCE_FEED)
    assert first == {"articles_seen": 1, "articles_new": 1, "pairs_seen": 2, "pairs_new": 2}
    pub = con.execute("SELECT published_at, content FROM news_articles").fetchone()
    assert pub[0] == "2026-10-07T09:10:00+09:00" and pub[1] is None   # 본문은 모름(NULL)
    assert {r[0] for r in con.execute("SELECT inserted_article FROM news_company_feed")} == {1}
    again = m.write_items(con, items, m.SOURCE_FEED)
    assert again["articles_new"] == 0 and again["pairs_new"] == 0
    assert con.execute("SELECT count(*) FROM news_company_feed").fetchone()[0] == 2


# --- 페이지 수집기 --------------------------------------------------------------------

class _Resp:
    def __init__(self, status: int, body: list | None = None) -> None:
        self.status_code, self._body = status, body

    def json(self) -> dict:
        return {"result": {"body": self._body}}


def _fetcher(pages: dict[tuple[str, int], _Resp]) -> tuple[m.Fetcher, list]:
    calls: list = []
    slept: list = []
    clock = iter(range(0, 10_000))

    def get(url, params):
        code = url.rsplit("/", 1)[1]
        calls.append((code, params["number"]))
        return pages.get((code, params["number"]), _Resp(200, []))

    f = m.Fetcher(get=get, sleep=slept.append, clock=lambda: float(next(clock)) * 0.1)
    return f, calls


def test_fetcher_stops_at_old_page_and_reports_page_cap_on_400():
    since = _t("2026-10-07T00:00:00")
    f, calls = _fetcher({
        ("AAA", 1): _Resp(200, [_raw("a", "2026-10-08T09:00:00")]),
        ("AAA", 2): _Resp(200, [_raw("b", "2026-10-06T09:00:00")]),
        ("BBB", 1): _Resp(200, [_raw("c", "2026-10-08T09:00:00")]),
        ("BBB", 2): _Resp(400),
    })
    assert f.code("AAA", since)[0] == "done" and [c for c in calls if c[0] == "AAA"] == [("AAA", 1), ("AAA", 2)]
    status, rows = f.code("BBB", since)
    assert status == "page_cap" and [r["id"] for r in rows] == ["c"]


def test_fetcher_404_first_page_is_an_empty_feed_and_500_raises():
    since = _t("2026-10-07T00:00:00")
    f, _ = _fetcher({("AAA", 1): _Resp(404), ("BBB", 1): _Resp(500)})
    assert f.code("AAA", since) == ("done", [])
    with pytest.raises(RuntimeError):
        f.code("BBB", since)


# --- 일일 증분 ---------------------------------------------------------------------

def _seed_bars(con, day: str, codes: dict[str, int]) -> None:
    con.executemany("INSERT INTO daily_bars_adjusted(code, date, trade_value) VALUES(?,?,?)",
                    [(c, day, v) for c, v in codes.items()])
    con.commit()


class _FakeFetcher:
    def __init__(self, by_code: dict[str, list[dict] | Exception]) -> None:
        self.by_code, self.calls = by_code, []

    def code(self, code, since_at):
        self.calls.append((code, since_at))
        got = self.by_code.get(code, [])
        if isinstance(got, Exception):
            raise got
        return "done", got


def test_collect_writes_pairs_and_ledger_and_next_run_starts_from_last_clean_day(tmp_path):
    con = connect(tmp_path / "t.db")
    _seed_bars(con, "2026-10-07", {"AAA": 3, "BBB": 2, "069500": 9})
    fetcher = _FakeFetcher({"AAA": [_raw("n1", "2026-10-08T09:00:00")], "BBB": []})
    stats = m.collect(con, since=dt.date(2026, 10, 8), until=dt.date(2026, 10, 8), fetcher=fetcher,
                      now=lambda: _t("2026-10-08T17:10:00"))
    assert stats["codes"] == 2 and stats["articles_new"] == 1 and stats["failed"] == 0
    assert [c for c, _ in fetcher.calls] == ["AAA", "BBB"]           # 069500 은 받지 않는다
    ledger = con.execute("SELECT code, status, n_rows FROM news_company_feed_fetches ORDER BY code").fetchall()
    assert [tuple(r) for r in ledger] == [("AAA", "done", 1), ("BBB", "done", 0)]  # 0 건도 "받았다" 로 남는다
    assert m.last_clean_run_day(con) == dt.date(2026, 10, 8)


def test_collect_records_failure_raises_and_failed_run_does_not_advance_since(tmp_path):
    con = connect(tmp_path / "t.db")
    _seed_bars(con, "2026-10-07", {"AAA": 3, "BBB": 2})
    m.upsert_news_company_feed_fetches(con, [("AAA", "2026-10-07T17:10:00+09:00", "2026-10-07", "done",
                                              None, 0, "2026-10-07T17:20:00+09:00", m.SOURCE_FEED)])
    fetcher = _FakeFetcher({"AAA": [_raw("n1", "2026-10-08T09:00:00")], "BBB": RuntimeError("HTTP 503")})
    with pytest.raises(RuntimeError, match="1개 종목 실패"):
        m.collect(con, since=dt.date(2026, 10, 8), until=dt.date(2026, 10, 8), fetcher=fetcher,
                  now=lambda: _t("2026-10-08T17:10:00"))
    # 성공한 종목의 기사는 남는다(재실행 멱등) — 그러나 다음 런의 시작일은 실패 없던 런 그대로.
    assert con.execute("SELECT count(*) FROM news_company_feed").fetchone()[0] == 1
    assert m.last_clean_run_day(con) == dt.date(2026, 10, 7)


def test_collect_all_empty_is_an_error_not_a_green_run(tmp_path):
    con = connect(tmp_path / "t.db")
    _seed_bars(con, "2026-10-07", {"AAA": 3})
    with pytest.raises(RuntimeError, match="0건"):
        m.collect(con, since=dt.date(2026, 10, 8), until=dt.date(2026, 10, 8),
                  fetcher=_FakeFetcher({}), now=lambda: _t("2026-10-08T17:10:00"))


def test_collect_stops_after_consecutive_failures(tmp_path):
    con = connect(tmp_path / "t.db")
    _seed_bars(con, "2026-10-07", {f"{i:06d}": 100 - i for i in range(1, 9)})
    fetcher = _FakeFetcher({f"{i:06d}": RuntimeError("blocked") for i in range(1, 9)})
    with pytest.raises(RuntimeError, match="시도하지 못했다"):
        m.collect(con, since=dt.date(2026, 10, 8), until=dt.date(2026, 10, 8), fetcher=fetcher,
                  now=lambda: _t("2026-10-08T17:10:00"))
    assert len(fetcher.calls) == m.MAX_CONSECUTIVE_FAILURES


# --- 아카이브 이전 -------------------------------------------------------------------

def test_import_archive_uses_global_earliest_version_and_records_ledger(tmp_path):
    con = connect(tmp_path / "t.db")
    _seed_bars(con, "2026-09-22", {"AAA": 2, "BBB": 1})
    arc = tmp_path / "arc"
    arc.mkdir()
    (arc / "AAA.jsonl").write_text("\n".join(json.dumps(r) for r in [
        _raw("s1", "2026-09-10T10:00:00"), _raw("s2", "2026-09-23T09:00:00")]) + "\n")
    (arc / "BBB.jsonl").write_text(json.dumps(_raw("s1", "2026-09-10T09:00:00")) + "\n")
    for c, status in (("AAA", "done"), ("BBB", "page_cap")):
        (arc / f"{c}.done").write_text(json.dumps(
            {"status": status, "rows": 1, "since": "2023-03-16", "finished_at": "2026-09-17T03:58:00+09:00"}))
    ledger = tmp_path / "ledger.jsonl"
    ledger.write_text(json.dumps({"run_started": "2026-09-23T16:30:00+09:00",
                                  "run_finished": "2026-09-23T16:45:00+09:00", "since": "2026-09-23",
                                  "until": "2026-09-23", "codes": 2, "status": "ok"}) + "\n")

    total = m.import_archive(con, arc, ledger)
    assert total["articles_new"] == 2 and total["pairs_new"] == 3
    pub = con.execute("SELECT published_at FROM news_articles WHERE id=?", (m.article_id("s1"),)).fetchone()[0]
    assert pub == "2026-09-10T09:00:00+09:00"   # 묶음 순서가 아니라 전역에서 가장 이른 판
    seen = con.execute("SELECT first_seen_at FROM news_company_feed WHERE article_id=?",
                       (m.article_id("s2"),)).fetchone()[0]
    assert seen == "2026-09-23T16:45:00+09:00"   # 백필 뒤 기사는 그걸 담은 증분 런
    fetches = con.execute("SELECT code, status, run_started, n_rows FROM news_company_feed_fetches "
                          "ORDER BY run_started, code").fetchall()
    assert [tuple(r) for r in fetches] == [
        ("AAA", "done", "2026-09-17T03:58:00+09:00", 1),
        ("BBB", "page_cap", "2026-09-17T03:58:00+09:00", 1),
        ("AAA", "done", "2026-09-23T16:30:00+09:00", 1),
        ("BBB", "done", "2026-09-23T16:30:00+09:00", 0),
    ]
    again = m.import_archive(con, arc, ledger)
    assert again["articles_new"] == 0 and again["pairs_new"] == 0
