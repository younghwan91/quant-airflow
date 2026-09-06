"""Naver consensus 유니버스 선정·동시성·DB 적재.

fetch/parse 자체(네이버 JSON 응답 파싱)는 krx-fundamentals-client의
``NaverConsensusScraper``로 옮겨서 그쪽 테스트 스위트가 검증한다(2026-09-06,
DartScraper와 같은 분리). 여기서는 이 파일이 여전히 맡는 것 —
유니버스 SQL, 동시성 제한(``_fetch_both``), CSV/DB 적재 형태 — 만 다룬다.
"""

from __future__ import annotations

import argparse
import asyncio

from collectors import storage
from collectors.naver_consensus import _fetch_both, _universe_query


def _run(coro):
    return asyncio.run(coro)


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
        # est_year는 krx-fundamentals-client 전환 후 "202612"(연월) 대신
        # "2026"(연도만) — 다운스트림에서 파싱해 쓰는 곳이 없어 정보 손실 없음.
        ("005930", "2026-07-11", 513958.0, 4.04, "2026-07-09", 46664.0, 6564.0, "2026"),
    ])

    import pandas as pd
    row = pd.read_sql_query("SELECT * FROM consensus", con).iloc[0]
    assert row["code"] == "005930"
    assert row["date"] == "2026-07-11"
    assert row["target_mean"] == 513958.0
    assert row["recomm_mean"] == 4.04
    assert row["fwd_eps"] == 46664.0


# ------------------------------------------------- 유니버스 축소


def test_covered_only_universe_reads_the_consensus_table():
    """전종목 2,627개 중 실제 적재는 하루 660행 — 73%가 매일 헛돈다."""
    sql, params = _universe_query(argparse.Namespace(
        covered_days=90, all_codes=True, top_n=800))

    assert "FROM consensus" in sql
    assert params == {"d": 90}


def test_covered_days_zero_keeps_the_old_behaviour():
    """기본값이 켜지면 조용히 유니버스가 줄어든다 — 명시할 때만 걸려야 한다."""
    sql, _ = _universe_query(argparse.Namespace(
        covered_days=0, all_codes=True, top_n=800))

    assert "FROM daily_bars" in sql


# ------------------------------------------------- 동시성 제한(_fetch_both)


class _FakeScraper:
    """NaverConsensusScraper를 흉내내는 페이크 — 실제 HTTP 없이 반환값만 조립한다."""

    def __init__(self):
        self.calls: list[str] = []

    async def fetch_consensus(self, code: str):
        self.calls.append(f"consensus:{code}")
        return (1.0, 2.0, "2026-08-21")

    async def fetch_estimate(self, code: str):
        self.calls.append(f"estimate:{code}")
        return (3.0, 4.0, 2026)


def test_fetch_both_returns_combined_tuple():
    scraper = _FakeScraper()
    sem = asyncio.Semaphore(4)
    result = _run(_fetch_both(scraper, sem, "005930"))
    assert result == ("005930", 1.0, 2.0, "2026-08-21", 3.0, 4.0, 2026)
    assert scraper.calls == ["consensus:005930", "estimate:005930"]


def test_fetch_both_respects_semaphore_concurrency_limit():
    """세마포어 크기(workers)를 넘는 동시 실행이 없어야 한다."""
    max_seen = 0
    current = 0
    lock = asyncio.Lock()

    class _SlowScraper:
        async def fetch_consensus(self, code: str):
            nonlocal max_seen, current
            async with lock:
                current += 1
                max_seen = max(max_seen, current)
            await asyncio.sleep(0.01)
            async with lock:
                current -= 1
            return (1.0, None, None)

        async def fetch_estimate(self, code: str):
            return (None, None, None)

    async def _go():
        scraper = _SlowScraper()
        sem = asyncio.Semaphore(2)
        await asyncio.gather(*[_fetch_both(scraper, sem, str(i)) for i in range(10)])

    _run(_go())
    assert max_seen <= 2
