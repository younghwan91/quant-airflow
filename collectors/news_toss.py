"""Collect Toss Invest news into TimescaleDB via krx-news-client.

krx-news-client(pip, https://github.com/younghwan91/krx-news-rest-api)는 서버가
아니라 kiwoom-client와 같은 클라이언트 라이브러리다 — 호출할 때마다 토스 API에
직접 요청해 정규화된 ``NewsArticle``을 돌려준다. 이 콜렉터는 그 결과를
``news_articles``/``news_article_tickers``에 ``id`` 자연키로 upsert한다
(migrations/010 참고). krx-news-rest-api의 옛 Redis 캐시는 전체 article JSON을
dedup 키로 써서 collected_at 때문에 같은 기사가 계속 새 항목으로 쌓였는데,
여기서는 ``id``(source+url 해시)만으로 upsert하므로 그 문제가 없다 — 토스의
ALL_HIGHLIGHT/HOT/SOARING_STOCK 피드가 같은 기사를 중복으로 줘도 id 하나당
한 행만 남는다.

CLI:
    python -m collectors.news_toss --db <DSN>
"""

from __future__ import annotations

import argparse
import asyncio
from typing import Any

from krx_news_client import TossScraper
from krx_news_client.models.schemas import NewsArticle

from .storage import connect, upsert_news_article_tickers, upsert_news_articles


def _article_record(article: NewsArticle) -> tuple:
    return (
        article.id,
        article.source.value,
        article.category.value,
        article.title,
        article.url,
        article.content,
        article.summary,
        article.author,
        article.published_at.isoformat(),
        article.collected_at.isoformat(),
    )


def _ticker_records(articles: list[NewsArticle]) -> list[tuple]:
    return [(article.id, ticker) for article in articles for ticker in article.tickers]


def _dedupe(articles: list[NewsArticle]) -> list[NewsArticle]:
    """같은 뉴스가 여러 피드(ALL_HIGHLIGHT/HOT/SOARING_STOCK)에 겹쳐 나올 때 id당 하나만 남긴다."""
    return list({article.id: article for article in articles}.values())


async def collect(con: Any) -> dict[str, int]:
    scraper = TossScraper()
    try:
        articles = await scraper.scrape_news()
    finally:
        await scraper.close()

    unique = _dedupe(articles)
    article_rows = upsert_news_articles(con, [_article_record(a) for a in unique])
    ticker_rows = upsert_news_article_tickers(con, _ticker_records(unique))
    return {
        "fetched": len(articles),
        "unique": len(unique),
        "articles": article_rows,
        "tickers": ticker_rows,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="토스증권 뉴스 TimescaleDB 수집기")
    parser.add_argument("--db", default=None, help="DSN (postgresql://... 또는 sqlite 경로)")
    args = parser.parse_args()

    con = connect(args.db)
    stats = asyncio.run(collect(con))
    con.close()
    print(
        f"✅ 토스 뉴스: fetched={stats['fetched']} unique={stats['unique']} "
        f"articles={stats['articles']} tickers={stats['tickers']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
