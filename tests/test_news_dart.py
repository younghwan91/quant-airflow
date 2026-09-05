"""DART 공시 콜렉터 — 레코드 변환·키 순환·upsert 멱등성 (네트워크 불요)."""

from __future__ import annotations

import asyncio
from datetime import datetime

import pytest
from krx_news_client import DartQuotaExceededError
from krx_news_client.models.schemas import Disclosure, NewsSource

from collectors import news_dart
from collectors.news_dart import _disclosure_record, _scrape_with_rotation
from collectors.storage import connect, upsert_disclosures


def _disclosure(disclosure_id: str, *, ticker: str = "005930") -> Disclosure:
    return Disclosure(
        id=disclosure_id,
        source=NewsSource.DART,
        title="주요사항보고서",
        url=f"https://dart.fss.or.kr/dsaf001/main.do?rcept_no={disclosure_id}",
        company="삼성전자",
        ticker=ticker,
        disclosure_type="주요사항보고서",
        published_at=datetime(2026, 9, 5, 9, 0, 0),
        collected_at=datetime(2026, 9, 5, 10, 5, 0),
    )


def test_disclosure_record_matches_column_order():
    disclosure = _disclosure("dart:abc123")
    record = _disclosure_record(disclosure)
    assert record == (
        "dart:abc123", "dart", "주요사항보고서",
        "https://dart.fss.or.kr/dsaf001/main.do?rcept_no=dart:abc123",
        "삼성전자", "005930", "주요사항보고서",
        "2026-09-05T09:00:00", "2026-09-05T10:05:00",
    )


def test_upsert_disclosures_is_idempotent_on_rerun(tmp_path):
    """같은 id로 하루 두 번(10:05/16:05) 재수집해도 행이 하나만 남는다."""
    con = connect(tmp_path / "t.db")
    disclosure = _disclosure("dart:rerun")

    upsert_disclosures(con, [_disclosure_record(disclosure)])

    recollected = disclosure.model_copy(update={"collected_at": datetime(2026, 9, 5, 16, 5, 0)})
    upsert_disclosures(con, [_disclosure_record(recollected)])

    rows = con.execute("SELECT id, collected_at FROM disclosures").fetchall()
    assert len(rows) == 1
    assert rows[0]["collected_at"] == "2026-09-05T16:05:00"


class _FakeScraper:
    """DartScraper 대역 — 키별 결과/예외를 미리 정해둔다."""

    _by_key: dict[str, object] = {}
    calls: list[str] = []

    def __init__(self, api_key: str) -> None:
        self.api_key = api_key
        _FakeScraper.calls.append(api_key)

    async def scrape_disclosures(self):
        outcome = _FakeScraper._by_key[self.api_key]
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    async def close(self) -> None:
        pass


def test_scrape_with_rotation_returns_first_success(monkeypatch):
    disclosures = [_disclosure("dart:x")]
    _FakeScraper.calls = []
    _FakeScraper._by_key = {"key1": disclosures}
    monkeypatch.setattr(news_dart, "DartScraper", _FakeScraper)

    result = asyncio.run(_scrape_with_rotation(["key1"]))

    assert result == disclosures
    assert _FakeScraper.calls == ["key1"]


def test_scrape_with_rotation_moves_to_next_key_on_quota_exhaustion(monkeypatch):
    disclosures = [_disclosure("dart:y")]
    _FakeScraper.calls = []
    _FakeScraper._by_key = {
        "key1": DartQuotaExceededError("일한도 소진"),
        "key2": disclosures,
    }
    monkeypatch.setattr(news_dart, "DartScraper", _FakeScraper)

    result = asyncio.run(_scrape_with_rotation(["key1", "key2"]))

    assert result == disclosures
    assert _FakeScraper.calls == ["key1", "key2"]


def test_scrape_with_rotation_raises_when_all_keys_exhausted(monkeypatch):
    _FakeScraper.calls = []
    _FakeScraper._by_key = {
        "key1": DartQuotaExceededError("일한도 소진 1"),
        "key2": DartQuotaExceededError("일한도 소진 2"),
    }
    monkeypatch.setattr(news_dart, "DartScraper", _FakeScraper)

    with pytest.raises(DartQuotaExceededError):
        asyncio.run(_scrape_with_rotation(["key1", "key2"]))

    assert _FakeScraper.calls == ["key1", "key2"]
