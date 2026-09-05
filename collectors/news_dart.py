"""Collect DART disclosures into TimescaleDB via krx-news-client.

krx-news-client(pip, https://github.com/younghwan91/krx-news-client)의
``DartScraper.scrape_disclosures()``가 DART Open API(``list.json``)를 직접 불러
전일~당일 공시를 정규화된 ``Disclosure``로 돌려준다. 이 콜렉터는 그 결과를
``disclosures``에 ``id`` 자연키로 upsert한다(migrations/011 참고). ``ticker``가
DART API의 ``stock_code``를 그대로 쓰므로 news_toss.py와 달리 별도 정규화
테이블이 없다 — 공시 1건은 발행사 1곳이다.

공시 목록 자체는 하루 두 창(daily_news DAG) 합쳐도 콜 수가 적지만(페이지 몇
개), DART API 키는 dart_earnings/dart_shares의 재무제표 수집과 같은 풀
(``DART_API_KEY``/``_2``/``_3``/``_4``, 키마다 20,000콜/일)을 공유한다. 그
무거운 수집이 먼저 키를 소진해뒀을 수 있으므로, 이 콜렉터도 같은
``collect_keys()`` 순서로 키를 순환한다 — 한 키가 일한도(020)를 맞으면
``krx_news_client.DartQuotaExceededError``를 받고 다음 키로 넘어간다(그 예외가
없으면 "그 날 공시가 없음"과 "한도로 못 가져옴"을 구분 못 해 히스토리에 조용한
결측이 생긴다 — DartScraper가 013은 조용히 빈 리스트로 넘기고 020만 예외로
올리는 이유).

CLI:
    python -m collectors.news_dart --db <DSN>
"""

from __future__ import annotations

import argparse
import asyncio
from typing import Any

from krx_news_client import DartQuotaExceededError, DartScraper
from krx_news_client.models.schemas import Disclosure

from .dart_earnings import collect_keys
from .storage import connect, upsert_disclosures


def _disclosure_record(d: Disclosure) -> tuple:
    return (
        d.id,
        d.source.value,
        d.title,
        d.url,
        d.company,
        d.ticker,
        d.disclosure_type,
        d.published_at.isoformat(),
        d.collected_at.isoformat(),
    )


async def _scrape_with_rotation(keys: list[str]) -> list[Disclosure]:
    """``DartScraper(key).scrape_disclosures()``를 부르고, 일한도(020)면 다음 키로.

    ``dart_earnings.rotate_on_quota_raising``과 같은 순환 규칙(키 소진 시 다음
    키, 전부 소진이면 마지막 예외를 올림)이지만 스크레이퍼가 async라 그 sync
    유틸을 그대로 못 쓴다 — 여기서 async로 다시 구현한다.
    """
    last_exc: DartQuotaExceededError | None = None
    for key in keys:
        scraper = DartScraper(api_key=key)
        try:
            return await scraper.scrape_disclosures()
        except DartQuotaExceededError as e:
            last_exc = e
            continue
        finally:
            await scraper.close()
    raise last_exc if last_exc else DartQuotaExceededError("DART API 키가 설정되지 않음")


async def collect(con: Any) -> dict[str, int]:
    keys = collect_keys()
    if not keys:
        return {"fetched": 0, "disclosures": 0}

    disclosures = await _scrape_with_rotation(keys)
    rows = upsert_disclosures(con, [_disclosure_record(d) for d in disclosures])
    return {"fetched": len(disclosures), "disclosures": rows}


def main() -> int:
    parser = argparse.ArgumentParser(description="DART 공시 TimescaleDB 수집기")
    parser.add_argument("--db", default=None, help="DSN (postgresql://... 또는 sqlite 경로)")
    args = parser.parse_args()

    con = connect(args.db)
    stats = asyncio.run(collect(con))
    con.close()
    print(f"✅ DART 공시: fetched={stats['fetched']} disclosures={stats['disclosures']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
