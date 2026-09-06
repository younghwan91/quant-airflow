"""뉴스/공시를 LLM이 읽고 구조화된 판단(news_judgments)으로 남긴다.

daily_news가 쌓은 news_articles/disclosures 원문만으로는 단기 트레이딩
신호로 못 쓴다 — event_type/sentiment/related_codes(동일 테마 종목)/
is_stale_repeat(재탕 뉴스 판별)를 이 모듈이 채운다. 설계 근거는
docs/superpowers/specs/2026-09-06-news-llm-judgments-design.md — 특히
related_codes/is_stale_repeat는 scalp-it 세션의 실제 트레이더 인터뷰
피드백(만쥬 46번 답변)으로 추가됐다.

confidence/judged_at 두 필드는 scalp-it 세션이 2026-09-06 세션간 문의에서
추가로 요청한 것이다(마이그레이션 013) — "신뢰도 점수 없으면 오탐을 못
거른다", "판단이 늦으면 체결 기반 신호와 같은 후행 근사 함정에 빠진다"는
이유로, sentiment_direction만으로는 스캘핑 소비 쪽에서 필터/가중치로 못
쓴다는 피드백이다. confidence는 LLM 자체 확신도(0~100, LLM 응답에서
파싱), judged_at은 LLM 응답이 실제로 돌아온 시각(시스템이 찍음, LLM이
주장하는 값이 아니다 — knowledge_date가 날짜 단위라 초 단위 레이턴시
측정에 못 쓰인다).

순수 함수(build_prompt/parse_judgment)와 네트워크 I/O(judge_item)를
분리하는 이 레포의 관례(dart_earnings.py)를 따른다 — 전자는 이 파일
단독으로, 후자는 실제 Claude 호출을 mock한 테스트로 검증한다.

CLI:
    python -m collectors.news_judge --db <DSN>
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
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

#: v1은 새로 들어오는 항목만 판단한다(스펙 "백필 범위" — 과거분을 오늘
#: 날짜로 판단하면 knowledge_date에 lookahead가 섞인다, CLAUDE.md §3).
#: 그렇다고 정확히 "오늘"만 보면 직전 실행이 놓친 항목(수집 지연·재시작)을
#: 영영 못 줍는다 — 며칠치 여유만 둔다(전체 이력 대비 무시할 만한 폭).
PENDING_LOOKBACK_DAYS = 3


@dataclass(frozen=True)
class Judgment:
    event_type: str
    sentiment_direction: int
    related_codes: list[str]
    is_stale_repeat: bool
    first_seen_date: str | None
    price_impact_likely: bool
    rationale: str
    confidence: int


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
        "confidence: 이 판단 전체에 대한 스스로의 확신도, 0(전혀 확신 없음)~"
        "100(매우 확신) 정수. 정보가 불충분하거나 애매하면 낮게 매겨라 —"
        " 낮은 확신도는 다운스트림에서 이 판단을 걸러내는 데 쓰인다.",
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
        '"first_seen_date": null, "price_impact_likely": false, "rationale": "...", '
        '"confidence": 0}'
    )
    return "\n".join(lines)


def _strip_code_fence(text: str) -> str:
    """마크다운 코드펜스(```json ... ``` / ``` ... ```)를 벗겨낸다.

    Claude Haiku 4.5가 "JSON만, 다른 텍스트 없이"라고 명시해도 실측으로
    ```json 펜스를 씌워 응답하는 걸 확인했다(2026-09-06, Gemini→Claude
    전환 스모크테스트) — 이걸 안 벗기면 모든 판단이 parse_failures로
    잡힌다. 문자열이 아니면 그대로 반환해 아래 json.loads가 기존과
    동일하게 TypeError를 내게 둔다(이 함수가 새 실패 모드를 만들지
    않는다).
    """
    if not isinstance(text, str):
        return text
    stripped = text.strip()
    if stripped.startswith("```"):
        stripped = stripped.removeprefix("```json").removeprefix("```").strip()
        stripped = stripped.removesuffix("```").strip()
    return stripped


def parse_judgment(llm_response: str) -> Judgment | None:
    """LLM 응답 텍스트 → Judgment, 또는 파싱/검증 실패 시 None.

    실패를 예외로 올리지 않는다 — 확률적 모델의 이상 출력은 버그가 아니라
    예상된 잡음이고, 호출부(collect())가 이 항목만 스킵하고 넘어가야 한다.
    """
    try:
        data = json.loads(_strip_code_fence(llm_response))
    except (json.JSONDecodeError, TypeError):
        return None

    required = {
        "event_type", "sentiment_direction", "related_codes", "is_stale_repeat",
        "first_seen_date", "price_impact_likely", "rationale", "confidence",
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
    if first_seen is not None:
        # 정확히 YYYYMMDD 8자리 숫자만 받는다 — strptime만으로는 "%Y"가
        # 자릿수 가변이라 "2026090" 같은 값도 통과할 수 있다. 여기를
        # 통과 못 하면 Postgres DATE 컬럼(news_judgments.first_seen_date)에
        # 못 들어갈 값이니 판단 전체를 버린다(행 일부만 쓰지 않는다).
        if not (isinstance(first_seen, str) and len(first_seen) == 8 and first_seen.isdigit()):
            return None
        try:
            datetime.strptime(first_seen, "%Y%m%d")
        except ValueError:
            return None

    price_impact = data["price_impact_likely"]
    if not isinstance(price_impact, bool):
        return None

    rationale = data["rationale"]
    if not isinstance(rationale, str):
        return None

    confidence = data["confidence"]
    if not isinstance(confidence, int) or isinstance(confidence, bool) or not (0 <= confidence <= 100):
        return None

    return Judgment(
        event_type=event_type, sentiment_direction=sentiment,
        related_codes=related_codes, is_stale_repeat=is_stale,
        first_seen_date=first_seen, price_impact_likely=price_impact,
        rationale=rationale, confidence=confidence,
    )


def judge_item(
    generate: Callable[[str], str], item: dict, prior_context: list[dict],
) -> Judgment | None:
    """프롬프트 생성 → LLM 호출(generate) → 파싱. generate는 텍스트→텍스트 함수라
    제공사가 뭐든(Claude든 다른 곳이든) 이 하나로 국한된다(model_id는 호출부가
    별도로 남긴다)."""
    prompt = build_prompt(item, prior_context)
    response = generate(prompt)
    return parse_judgment(response)


def _claude_generate(model_id: str, api_key: str) -> Callable[[str], str]:
    """실제 Claude API를 부르는 generate 함수를 만든다.

    Gemini(2.5 Flash)에서 전환(2026-09-06) — scalp-it 세션 요청으로 가격·
    효과성을 조사한 결과, 이 워크로드(하루 수십~수백 건) 규모에선 제공사간
    토큰 단가 차이가 월 몇 달러 수준이라 무의미하고, scalp-it이 요구한
    "오탐 최소화 우선"에는 구조화 출력 신뢰도가 더 중요하다는 결론이었다.
    모델 교체는 model_id가 행마다 남으니 설정값 변경이지 재설계가 아니다
    (docs/superpowers/specs/2026-09-06-news-llm-judgments-design.md).

    SDK: 공식 ``anthropic`` 패키지(PyPI, 2026-09 기준 1.x). 공식 사용 예
    (https://github.com/anthropics/anthropic-sdk-python):

        import anthropic
        client = anthropic.Anthropic(api_key=...)
        response = client.messages.create(model=..., max_tokens=..., messages=[...])
        response.content[0].text

    Haiku 4.5는 기본적으로 thinking을 안 켜므로(claude-api 스킬 참고) 이
    분류 태스크에선 thinking 파라미터를 아예 생략한다 — 응답이 JSON 하나뿐이라
    추론 과정을 노출할 이유가 없다. max_tokens=512는 rationale(한두 문장) +
    related_codes 배열을 포함한 JSON 응답에 여유 있게 맞춘 값이다.
    """
    import anthropic  # noqa: PLC0415 — optional dep, only needed for this path

    client = anthropic.Anthropic(api_key=api_key)

    def generate(prompt: str) -> str:
        response = client.messages.create(
            model=model_id, max_tokens=512,
            messages=[{"role": "user", "content": prompt}],
        )
        return next(block.text for block in response.content if block.type == "text")

    return generate


def _pending_items(con: Any, since: str) -> list[tuple[str, str, dict]]:
    """(source_type, source_id, item-dict) — 아직 v1 판단이 없는 것만.

    ``since``는 ISO 날짜(``YYYY-MM-DD``, ``published_at``이 저장된
    ``datetime.isoformat()`` 형식과 같은 자릿수 규칙 — news_dart.py/news_toss.py의
    ``d.published_at.isoformat()``)로, ``published_at >= since`` 문자열 비교가
    올바르게 동작하려면 두 값이 같은 ISO 8601 자리수 순서를 따라야 한다(bare
    ``YYYYMMDD``는 대시가 없어 문자열 비교가 어긋난다). 이 하한이 없으면 첫
    배포 때 기존 히스토리 전체가 오늘 날짜의 knowledge_date로 영구 기록된다
    (스펙 "백필 범위", CLAUDE.md §3 lookahead 오염과 같은 부류).

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
        "WHERE d.ticker IS NOT NULL AND d.published_at >= ? AND NOT EXISTS ("
        "  SELECT 1 FROM news_judgments j WHERE j.source_type='disclosure' "
        "  AND j.source_id=d.id AND j.ticker=d.ticker AND j.prompt_version=?)",
        (since, PROMPT_VERSION))
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
        "WHERE LENGTH(t.ticker) = 6 AND a.published_at >= ? AND NOT EXISTS ("
        "  SELECT 1 FROM news_judgments j WHERE j.source_type='news' "
        "  AND j.source_id=a.id AND j.ticker=t.ticker AND j.prompt_version=?)",
        (since, PROMPT_VERSION))
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
    """재개 가능한 판단 루프. 반환: {"target", "judged", "parse_failures", "api_failures"}.

    ``today``는 이 레포 관례대로 압축형 ``YYYYMMDD``(예: ``dart_earnings.py``의
    ``today = datetime.now().strftime("%Y%m%d")``) — DATE 컬럼(Postgres)도
    이 포맷을 그대로 받는다(``earnings.knowledge_date``와 동일 선례).

    건마다 즉시 upsert한다(끝에 몰아쓰지 않는다) — 중간에 프로세스가 죽어도
    이미 판단한 건을 잃지 않고, 같은 런 안에서도 ``_prior_context``가 방금
    쓴 판단을 바로 볼 수 있다(``is_stale_repeat`` 판별의 전제).
    """
    today = today or datetime.now().strftime("%Y%m%d")
    today_dt = datetime.strptime(today, "%Y%m%d")
    cutoff = (today_dt - timedelta(days=STALE_LOOKBACK_DAYS)).strftime("%Y%m%d")
    # v1은 새로 들어오는 항목만 판단한다(스펙 "백필 범위") — since는
    # published_at(ISO datetime 문자열)과 문자열 비교되므로 대시 포함 ISO
    # 날짜(YYYY-MM-DD)여야 한다.
    since = (today_dt - timedelta(days=PENDING_LOOKBACK_DAYS)).strftime("%Y-%m-%d")
    targets = _pending_items(con, since)
    judged = parse_failures = api_failures = 0
    for source_type, source_id, item in targets:
        prior = _prior_context(con, item["ticker"], cutoff)
        # judge_item(generate, item, prior)를 통째로 감싸지 않는다 —
        # build_prompt의 버그(예: item에 키 누락)까지 "API실패"로 잘못
        # 집계되면 재시도만 반복되고 진짜 원인이 안 드러난다. generate
        # 호출(네트워크 I/O)만 예외를 잡는다; parse_judgment는 설계상
        # 예외를 던지지 않는다(위 docstring 참고).
        prompt = build_prompt(item, prior)
        try:
            response = generate(prompt)
        except Exception:
            api_failures += 1
            continue
        # generate()가 돌아온 직후를 judged_at으로 찍는다 — LLM이 스스로
        # 주장하는 시각이 아니라 시스템이 관측한 시각이어야 scalp-it이
        # 요청한 "published_at 대비 실제 레이턴시" 측정이 신뢰할 수 있다.
        judged_at = datetime.now(timezone.utc).isoformat()
        j = parse_judgment(response)
        if j is None:
            parse_failures += 1
            continue
        upsert_news_judgments(con, [(
            source_type, source_id, item["ticker"], j.event_type,
            j.sentiment_direction, json.dumps(j.related_codes, ensure_ascii=False),
            j.is_stale_repeat, j.first_seen_date, j.price_impact_likely,
            j.rationale, model_id, PROMPT_VERSION, today, j.confidence, judged_at,
        )])
        judged += 1
    return {
        "target": len(targets), "judged": judged,
        "parse_failures": parse_failures, "api_failures": api_failures,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="뉴스/공시 LLM 판단")
    ap.add_argument("--db", default=None, help="DSN (postgresql://... 또는 sqlite 경로)")
    ap.add_argument("--model-id", default="claude-haiku-4-5",
                    help="Claude 모델 ID (anthropic SDK, client.messages.create)")
    args = ap.parse_args()

    import os
    from .storage import connect

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        raise SystemExit("환경변수 ANTHROPIC_API_KEY 필요")

    con = connect(args.db)
    generate = _claude_generate(args.model_id, api_key)
    stats = collect(con, generate, model_id=args.model_id)
    con.close()
    print(f"대상 {stats['target']}건 | 판단 {stats['judged']}건 | "
          f"파싱실패 {stats['parse_failures']}건 | "
          f"API실패 {stats['api_failures']}건", flush=True)
    if stats["api_failures"] > 0:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
