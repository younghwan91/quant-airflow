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
from datetime import datetime, timedelta
from typing import Any, Callable

from .storage import fetchall, upsert_news_judgments

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


def judge_item(
    generate: Callable[[str], str], item: dict, prior_context: list[dict],
) -> Judgment | None:
    """프롬프트 생성 → LLM 호출(generate) → 파싱. generate는 텍스트→텍스트 함수라
    Gemini든 다른 제공사든 이 하나로 국한된다(model_id는 호출부가 별도로 남긴다)."""
    prompt = build_prompt(item, prior_context)
    response = generate(prompt)
    return parse_judgment(response)


def _gemini_generate(model_id: str, api_key: str) -> Callable[[str], str]:
    """실제 Gemini API를 부르는 generate 함수를 만든다.

    SDK: ``google-genai`` (PyPI, 2026-09 기준 최신 2.22.0) — 구
    ``google-generativeai`` 는 2025-11-30부로 deprecated 됐다
    (https://ai.google.dev/gemini-api/docs/libraries). 공식 사용 예
    (https://ai.google.dev/gemini-api/docs/text-generation ,
    https://googleapis.github.io/python-genai/):

        from google import genai
        client = genai.Client(api_key=...)
        response = client.models.generate_content(model=..., contents=...)
        response.text
    """
    from google import genai  # noqa: PLC0415 — optional dep, only needed for this path

    client = genai.Client(api_key=api_key)

    def generate(prompt: str) -> str:
        response = client.models.generate_content(model=model_id, contents=prompt)
        return response.text

    return generate


def _pending_items(con: Any) -> list[tuple[str, str, dict]]:
    """(source_type, source_id, item-dict) — 아직 v1 판단이 없는 것만.

    ``fetchall()``(``.storage``)을 쓴다 — sqlite3.Row는 이름 접근이 되지만
    psycopg2 기본 커서는 튜플만 돌려주므로(이 파일의 다른 콜렉터, 예:
    dart_shares.py의 ``_targets``/``_listed_targets``와 동일하게) **위치 인덱스로만
    접근**한다. SQL도 두 백엔드 공통 문법만 쓴다 — sqlite 전용 ``GLOB``/``date()``
    함수는 Postgres에 없다. 6자리 티커 필터는 ``LENGTH(ticker) = 6``으로
    근사한다(완벽한 "숫자만" 검증은 아니지만, 지금까지 관측된 비-KRX 코드가
    전부 6자보다 길어서 이 정도로 충분하다 — 스펙의 정확한 목표는 서로 다른
    두 백엔드에서 똑같이 도는 정규식 지원 여부에 안 매달리는 것).

    news_articles는 news_article_tickers로 여러 종목에 걸릴 수 있어 조인,
    disclosures는 ticker 컬럼 하나로 충분(1:1, news_dart.py 기존 설계와 동일).
    """
    out: list[tuple[str, str, dict]] = []
    rows = fetchall(con,
        "SELECT d.id, d.ticker, d.title, d.company, d.published_at FROM disclosures d "
        "WHERE d.ticker IS NOT NULL AND NOT EXISTS ("
        "  SELECT 1 FROM news_judgments j WHERE j.source_type='disclosure' "
        "  AND j.source_id=d.id AND j.ticker=d.ticker AND j.prompt_version=?)",
        (PROMPT_VERSION,))
    for source_id, ticker, title, company, published_at in rows:
        # disclosures 테이블엔 본문이 없다(news_dart.py가 구조화 필드만 저장) —
        # title이 실제 내용이고, company/disclosure_type은 보조 맥락이다.
        out.append(("disclosure", source_id, {
            "ticker": ticker, "title": title,
            "content": f"발행사: {company or '(알수없음)'}",
            "published_at": published_at,
        }))

    rows = fetchall(con,
        "SELECT a.id, t.ticker, a.title, a.summary, a.published_at "
        "FROM news_articles a JOIN news_article_tickers t ON t.article_id = a.id "
        "WHERE LENGTH(t.ticker) = 6 AND NOT EXISTS ("
        "  SELECT 1 FROM news_judgments j WHERE j.source_type='news' "
        "  AND j.source_id=a.id AND j.ticker=t.ticker AND j.prompt_version=?)",
        (PROMPT_VERSION,))
    for source_id, ticker, title, summary, published_at in rows:
        out.append(("news", source_id, {
            "ticker": ticker, "title": title, "content": summary or "",
            "published_at": published_at,
        }))
    return out


def _prior_context(con: Any, ticker: str, cutoff: str) -> list[dict]:
    """``cutoff``(YYYYMMDD) 이후 같은 ticker의 판단 이력(재탕 판별용 컨텍스트).

    날짜 하한을 SQL 함수(sqlite ``date()``) 대신 **파이썬에서 미리 계산해
    문자열로 넘긴다** — ``storage.date_days_ago()``와 같은 이유(Postgres/sqlite
    둘 다 이해하는 건 리터럴 비교뿐, 방언별 날짜 함수가 아니다).
    """
    rows = fetchall(con,
        "SELECT knowledge_date, rationale FROM news_judgments "
        "WHERE ticker = ? AND knowledge_date >= ? "
        "ORDER BY knowledge_date DESC LIMIT 5",
        (ticker, cutoff))
    return [{"date": knowledge_date, "rationale": rationale}
            for knowledge_date, rationale in rows]


def collect(
    con: Any, generate: Callable[[str], str], *, model_id: str,
    today: str | None = None,
) -> dict[str, int]:
    """재개 가능한 판단 루프. 반환: {"target", "judged", "api_failures"}.

    ``today``는 이 레포 관례대로 압축형 ``YYYYMMDD``(예: ``dart_earnings.py``의
    ``today = datetime.now().strftime("%Y%m%d")``) — DATE 컬럼(Postgres)도
    이 포맷을 그대로 받는다(``earnings.knowledge_date``와 동일 선례).
    """
    today = today or datetime.now().strftime("%Y%m%d")
    cutoff = (datetime.strptime(today, "%Y%m%d") - timedelta(days=STALE_LOOKBACK_DAYS)).strftime("%Y%m%d")
    targets = _pending_items(con)
    judged = api_failures = 0
    rows: list[tuple] = []
    for source_type, source_id, item in targets:
        prior = _prior_context(con, item["ticker"], cutoff)
        try:
            j = judge_item(generate, item, prior)
        except Exception:
            api_failures += 1
            continue
        if j is None:
            continue
        rows.append((
            source_type, source_id, item["ticker"], j.event_type,
            j.sentiment_direction, json.dumps(j.related_codes, ensure_ascii=False),
            j.is_stale_repeat, j.first_seen_date, j.price_impact_likely,
            j.rationale, model_id, PROMPT_VERSION, today,
        ))
        judged += 1
    if rows:
        upsert_news_judgments(con, rows)
    return {"target": len(targets), "judged": judged, "api_failures": api_failures}
