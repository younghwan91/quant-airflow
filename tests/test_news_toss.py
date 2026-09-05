"""토스 뉴스 콜렉터 — 레코드 변환·피드 간 dedup·upsert 멱등성 (네트워크 불요)."""

from __future__ import annotations

from datetime import datetime

from krx_news_client.models.schemas import NewsArticle, NewsCategory, NewsSource

from collectors.news_toss import _article_record, _dedupe, _ticker_records
from collectors.storage import connect, upsert_news_article_tickers, upsert_news_articles


def _article(article_id: str, *, tickers: list[str] | None = None) -> NewsArticle:
    return NewsArticle(
        id=article_id,
        source=NewsSource.TOSS,
        category=NewsCategory.MARKET,
        title=f"제목 {article_id}",
        url=f"https://tossinvest.com/news?id={article_id}",
        content="본문",
        summary="요약",
        tickers=tickers or [],
        author="토큰포스트",
        published_at=datetime(2026, 9, 5, 12, 0, 0),
        collected_at=datetime(2026, 9, 5, 12, 5, 0),
    )


def test_article_record_matches_column_order():
    article = _article("toss:abc123", tickers=["005930"])
    record = _article_record(article)
    assert record == (
        "toss:abc123", "toss", "market", "제목 toss:abc123",
        "https://tossinvest.com/news?id=toss:abc123", "본문", "요약", "토큰포스트",
        "2026-09-05T12:00:00", "2026-09-05T12:05:00",
    )


def test_ticker_records_flattens_and_skips_empty():
    articles = [_article("a", tickers=["005930", "000660"]), _article("b", tickers=[])]
    assert _ticker_records(articles) == [("a", "005930"), ("a", "000660")]


def test_dedupe_keeps_one_row_per_id_across_feeds():
    """ALL_HIGHLIGHT/HOT 처럼 같은 기사가 여러 피드에서 중복으로 나와도 하나만 남는다."""
    same_id_twice = [_article("toss:x"), _article("toss:x")]
    assert len(_dedupe(same_id_twice)) == 1

    two_distinct = [_article("toss:x"), _article("toss:y")]
    assert {a.id for a in _dedupe(two_distinct)} == {"toss:x", "toss:y"}


def test_upsert_news_articles_is_idempotent_on_rerun(tmp_path):
    """같은 id로 두 번 크롤링해도(예: 5분마다 재수집) 행이 하나만 남는다.

    krx-news-rest-api 옛 캐시(Redis ZSET, member=article JSON)는 collected_at이
    매번 달라 재수집마다 새 항목이 쌓였다 — 여기서는 id 자연키 upsert라 그 버그가
    재현되지 않는다.
    """
    con = connect(tmp_path / "t.db")
    article = _article("toss:rerun", tickers=["005930"])

    upsert_news_articles(con, [_article_record(article)])
    upsert_news_article_tickers(con, _ticker_records([article]))

    # 재크롤링: collected_at만 바뀐 같은 기사
    recrawled = article.model_copy(update={"collected_at": datetime(2026, 9, 5, 12, 10, 0)})
    upsert_news_articles(con, [_article_record(recrawled)])
    upsert_news_article_tickers(con, _ticker_records([recrawled]))

    rows = con.execute("SELECT id, collected_at FROM news_articles").fetchall()
    assert len(rows) == 1
    assert rows[0]["collected_at"] == "2026-09-05T12:10:00"

    ticker_rows = con.execute("SELECT article_id, ticker FROM news_article_tickers").fetchall()
    assert len(ticker_rows) == 1
