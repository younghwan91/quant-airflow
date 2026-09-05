"""뉴스/공시를 LLM이 읽고 구조화된 판단(news_judgments)으로 남긴다.

daily_news가 쌓은 news_articles/disclosures 원문만으로는 단기 트레이딩
신호로 못 쓴다 — event_type/sentiment/related_codes(동일 테마 종목)/
is_stale_repeat(재탕 뉴스 판별)를 이 모듈이 채운다. 설계 근거는
docs/superpowers/specs/2026-09-06-news-llm-judgments-design.md — 특히
related_codes/is_stale_repeat는 scalp-it 세션의 실제 트레이더 인터뷰
피드백(만쥬 46번 답변)으로 추가됐다.

순수 함수(build_prompt/parse_judgment)와 네트워크 I/O(judge_item)를
분리하는 이 레포의 관례(dart_earnings.py)를 따른다 — 전자는 이 파일
단독으로, 후자는 실제 Gemini 호출을 mock한 테스트로 검증한다.

CLI:
    python -m collectors.news_judge --db <DSN>
"""

from __future__ import annotations

import json
from dataclasses import dataclass

EVENT_TYPES: tuple[str, ...] = (
    "실적", "유상증자", "자사주", "최대주주변경", "소송", "가이던스", "규제", "기타",
)

PROMPT_VERSION = "v1"

#: 최근 며칠 내 같은 (ticker, event_type) 판단이 있으면 재탕 후보로 본다.
#: 이벤트 유형별로 다를 수 있다는 걸 알지만(유상증자는 실적보다 오래
#: 유효) v1은 단일값으로 시작 — 실측 후 유형별로 나눌지 결정한다.
STALE_LOOKBACK_DAYS = 7


@dataclass(frozen=True)
class Judgment:
    event_type: str
    sentiment_direction: int
    related_codes: list[str]
    is_stale_repeat: bool
    first_seen_date: str | None
    price_impact_likely: bool
    rationale: str


def build_prompt(item: dict, prior_context: list[dict]) -> str:
    """LLM에 넘길 프롬프트. item={ticker,title,content,published_at}."""
    lines = [
        "다음 한국 주식시장 뉴스/공시를 읽고 JSON으로만 답하라.",
        f"종목코드: {item['ticker']}",
        f"발행일시: {item['published_at']}",
        f"제목: {item['title']}",
        f"본문: {item['content']}",
        "",
        f"event_type은 다음 중 하나여야 한다: {', '.join(EVENT_TYPES)}",
        "sentiment_direction은 -1(부정)/0(중립)/1(긍정) 중 하나.",
        "related_codes: 이 재료로 같이 움직일 만한 동일 테마 종목코드 목록(없으면 빈 배열).",
        "is_stale_repeat: 아래 최근 유사 판단 목록에 비춰 이미 알려진 재탕 재료인가.",
        "first_seen_date: is_stale_repeat가 true면 최초로 본 날짜(YYYYMMDD), 아니면 null.",
        "price_impact_likely: 단기(수일 내) 가격에 영향 줄 만한가 (true/false).",
        "rationale: 판단 근거를 한두 문장으로.",
    ]
    if prior_context:
        lines.append("")
        lines.append("최근 유사 판단 이력:")
        for p in prior_context:
            lines.append(f"- {p['date']}: {p['rationale']}")
    lines.append("")
    lines.append(
        '응답 형식(JSON만, 다른 텍스트 없이): {"event_type": "...", '
        '"sentiment_direction": 0, "related_codes": [], "is_stale_repeat": false, '
        '"first_seen_date": null, "price_impact_likely": false, "rationale": "..."}'
    )
    return "\n".join(lines)


def parse_judgment(llm_response: str) -> Judgment | None:
    """LLM 응답 텍스트 → Judgment, 또는 파싱/검증 실패 시 None.

    실패를 예외로 올리지 않는다 — 확률적 모델의 이상 출력은 버그가 아니라
    예상된 잡음이고, 호출부(collect())가 이 항목만 스킵하고 넘어가야 한다.
    """
    try:
        data = json.loads(llm_response)
    except (json.JSONDecodeError, TypeError):
        return None

    required = {
        "event_type", "sentiment_direction", "related_codes", "is_stale_repeat",
        "first_seen_date", "price_impact_likely", "rationale",
    }
    if not isinstance(data, dict) or not required.issubset(data.keys()):
        return None

    event_type = data["event_type"]
    if event_type not in EVENT_TYPES:
        return None

    sentiment = data["sentiment_direction"]
    if not isinstance(sentiment, int) or isinstance(sentiment, bool) or sentiment not in (-1, 0, 1):
        return None

    related_codes = data["related_codes"]
    if not isinstance(related_codes, list) or not all(isinstance(c, str) for c in related_codes):
        return None

    is_stale = data["is_stale_repeat"]
    if not isinstance(is_stale, bool):
        return None

    first_seen = data["first_seen_date"]
    if first_seen is not None and not isinstance(first_seen, str):
        return None

    price_impact = data["price_impact_likely"]
    if not isinstance(price_impact, bool):
        return None

    rationale = data["rationale"]
    if not isinstance(rationale, str):
        return None

    return Judgment(
        event_type=event_type, sentiment_direction=sentiment,
        related_codes=related_codes, is_stale_repeat=is_stale,
        first_seen_date=first_seen, price_impact_likely=price_impact,
        rationale=rationale,
    )
