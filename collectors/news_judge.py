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

순수 함수(build_prompt/build_request/parse_judgment)와 네트워크 I/O(ClaudeBatchAPI)를
분리하는 이 레포의 관례(dart_earnings.py)를 따른다 — 전자는 이 파일
단독으로, 후자는 BatchAPI 를 가짜로 바꾼 테스트로 검증한다.

v2(2026-10-12): Haiku 5.5 + 프롬프트 캐싱 + Message Batches API — 기사 LLM 분석의 의무
규칙이다(CLAUDE.md §3). 동기 messages.create 루프로 되돌리지 않는다. 배치는 50% 할인,
고정 지시문 캐시 읽기는 0.1배다. 배치 id 는 ``news_judge_batches`` 원장에 남아 태스크가
죽거나 못 기다려도 다음 실행이 수거한다.

CLI:
    python -m collectors.news_judge --db <DSN> [--poll-timeout 3600]
"""

from __future__ import annotations

import argparse
import json
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterable, Protocol

from .storage import execute, fetchall, upsert_news_judgments

EVENT_TYPES: tuple[str, ...] = (
    "실적", "유상증자", "자사주", "최대주주변경", "소송", "가이던스", "규제", "기타",
)

#: v2(2026-10-12): Haiku 4.5 → 5.5, 고정 지시문을 system(프롬프트 캐시)으로 분리.
#: v1 행(전부 Haiku 4.5)은 migration 018 로 legacy.news_judgments_haiku45 에 보관했다.
PROMPT_VERSION = "v2"

#: 판단 모델. 2026-10-12 부터 Haiku 5.5 만 쓴다(4.5 대비 단가 1/10). DAG 는 --model-id 를
#: 넘기지 않으므로 이 값이 운영 모델이다.
MODEL_ID = "claude-haiku-5-5"

MAX_TOKENS = 512

#: 배치 하나를 기다리는 최대 시간. Batch API 는 24시간 안을 약속할 뿐이지만 수백 건은 보통
#: 수 분이다(실측 전 값). 60분이면 08:45 → 09:45, 10:05 → 11:05 로 다음 실행과 안 겹친다.
#: 못 기다린 배치는 원장에 남고 다음 실행(08:45/10:05/16:05)이 먼저 수거한다.
POLL_TIMEOUT_S = 60 * 60
POLL_INTERVAL_S = 30

#: 수거 때 합산해 찍는 실제 과금 usage — 캐시가 걸렸는지(cache_read)를 여기서 본다.
USAGE_FIELDS = ("input_tokens", "cache_creation_input_tokens", "cache_read_input_tokens", "output_tokens")

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


#: 매 호출 똑같은 지시문 — system 으로 보내 프롬프트 캐시에 태운다(v2, 2026-10-12).
#: v1 은 이 지시문이 user 메시지 안 기사 본문 **뒤에** 있어 앞부분이 매번 달랐다 —
#: 캐시가 원리상 안 걸리는 순서였다(캐시는 접두 일치). 지시문 내용은 v1 그대로다.
SYSTEM_PROMPT = "\n".join([
    "다음 한국 주식시장 뉴스/공시를 읽고 JSON으로만 답하라.",
    "",
    f"event_type은 다음 중 하나여야 한다: {', '.join(EVENT_TYPES)}",
    "sentiment_direction은 -1(부정)/0(중립)/1(긍정) 중 하나.",
    "related_codes: 이 재료로 같이 움직일 만한 동일 테마 종목코드 목록(없으면 빈 배열).",
    "is_stale_repeat: 메시지의 최근 유사 판단 이력에 비춰 이미 알려진 재탕 재료인가"
    "(이력이 없으면 false).",
    "first_seen_date: is_stale_repeat가 true면 최초로 본 날짜(YYYYMMDD), 아니면 null.",
    "price_impact_likely: 단기(수일 내) 가격에 영향 줄 만한가 (true/false).",
    "rationale: 판단 근거를 한두 문장으로.",
    "confidence: 이 판단 전체에 대한 스스로의 확신도, 0(전혀 확신 없음)~"
    "100(매우 확신) 정수. 정보가 불충분하거나 애매하면 낮게 매겨라 —"
    " 낮은 확신도는 다운스트림에서 이 판단을 걸러내는 데 쓰인다.",
    "",
    '응답 형식(JSON만, 다른 텍스트 없이): {"event_type": "...", '
    '"sentiment_direction": 0, "related_codes": [], "is_stale_repeat": false, '
    '"first_seen_date": null, "price_impact_likely": false, "rationale": "...", '
    '"confidence": 0}',
])


def build_prompt(item: dict, prior_context: list[dict]) -> str:
    """LLM에 넘길 user 메시지(건마다 다른 부분만). item={ticker,title,content,published_at}.

    고정 지시문은 :data:`SYSTEM_PROMPT` 에 있다.
    """
    lines = [
        f"종목코드: {item['ticker']}",
        f"발행일시: {item['published_at']}",
        f"제목: {item['title']}",
        f"본문: {item['content']}",
    ]
    if prior_context:
        lines.append("")
        lines.append("최근 유사 판단 이력:")
        for p in prior_context:
            lines.append(f"- {p['date']}: {p['rationale']}")
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


def _pending_items(con: Any, since: str) -> list[tuple[str, str, dict]]:
    """(source_type, source_id, item-dict) — 아직 현재 PROMPT_VERSION 판단이 없는 것만.

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


def build_request(custom_id: str, prompt: str, model_id: str) -> dict:
    """Message Batches 요청 한 건.

    고정 지시문(:data:`SYSTEM_PROMPT`)은 cache_control 블록 — 같은 배치 안 요청들이 이 접두를
    공유해 캐시 읽기(0.1배)로 과금된다. 5.5 의 캐시 최소 접두는 512 토큰이고 지시문이 그
    언저리라, 실제로 걸리는지는 수거 때 찍는 ``cache_read`` 합계로 확인한다(안 걸려도 오류
    없이 정가일 뿐). Haiku 5.5 는 thinking 이 **기본으로 켜져** 있어 끈다 — 안 끄면
    max_tokens 가 생각에 먹혀 빈 응답이 난다(swing-it 파일럿 2026-10-08: 500건 중 320건).
    temperature 는 5.5 가 받지 않는다. max_tokens=512 는 rationale(한두 문장) + related_codes
    배열을 포함한 JSON 응답에 여유 있게 맞춘 값이다.
    """
    return {
        "custom_id": custom_id,
        "params": {
            "model": model_id,
            "max_tokens": MAX_TOKENS,
            "system": [{"type": "text", "text": SYSTEM_PROMPT,
                        "cache_control": {"type": "ephemeral"}}],
            "thinking": {"type": "disabled"},
            "messages": [{"role": "user", "content": prompt}],
        },
    }


@dataclass(frozen=True)
class BatchResult:
    custom_id: str
    text: str | None          # None = errored/expired/canceled — 다음 실행이 다시 판단한다
    usage: dict[str, int]


class BatchAPI(Protocol):
    def submit(self, requests: list[dict]) -> str: ...
    def ended_at(self, batch_id: str) -> str | None: ...   # None = 아직 처리 중
    def results(self, batch_id: str) -> Iterable[BatchResult]: ...


class ClaudeBatchAPI:
    """공식 ``anthropic`` SDK 의 Message Batches(``client.messages.batches``).

    **create 는 재시도하지 않는다**(``max_retries=0``) — 5xx·연결 오류 뒤에도 배치가 실제로
    생겼을 수 있어, 재시도는 같은 기사를 두 번 과금한다(swing-it judge.py 의 SubmitUncertain 과
    같은 원칙). 실패하면 태스크가 실패하고, 다음 실행이 원장에 없는 기사만 다시 낸다.
    조회(retrieve·results)는 SDK 기본 재시도를 그대로 쓴다 — 읽기는 과금이 없다.
    """

    def __init__(self, api_key: str) -> None:
        import anthropic  # noqa: PLC0415 — optional dep, only needed for this path

        self._client = anthropic.Anthropic(api_key=api_key)

    def submit(self, requests: list[dict]) -> str:
        return self._client.with_options(max_retries=0).messages.batches.create(
            requests=requests).id

    def ended_at(self, batch_id: str) -> str | None:
        batch = self._client.messages.batches.retrieve(batch_id)
        if batch.processing_status != "ended":
            return None
        return batch.ended_at.astimezone(timezone.utc).isoformat()

    def results(self, batch_id: str) -> Iterable[BatchResult]:
        for r in self._client.messages.batches.results(batch_id):
            if r.result.type != "succeeded":
                yield BatchResult(r.custom_id, None, {})
                continue
            message = r.result.message
            text = next((b.text for b in message.content if b.type == "text"), None)
            usage = {k: getattr(message.usage, k, 0) or 0 for k in USAGE_FIELDS}
            yield BatchResult(r.custom_id, text, usage)


def _open_batches(con: Any) -> list[tuple[str, str, str, list]]:
    """원장에서 아직 수거하지 않은 배치 — (batch_id, model_id, prompt_version, items)."""
    rows = fetchall(con,
        "SELECT batch_id, model_id, prompt_version, items FROM news_judge_batches "
        "WHERE collected_at IS NULL ORDER BY submitted_at")
    # Postgres 는 items 가 JSONB 라 psycopg2 가 이미 list 로 준다, sqlite 는 TEXT.
    return [(b, m, v, items if isinstance(items, list) else json.loads(items))
            for b, m, v, items in rows]


def _collect_ended(con: Any, api: BatchAPI, today: str, stats: dict[str, int]) -> None:
    """끝난 배치의 결과를 news_judgments 에 쓰고 원장에 수거 시각을 찍는다.

    쓰고 나서 찍는다 — 그 사이에 죽으면 다음 실행이 같은 배치를 다시 수거하는데,
    upsert 가 ``on_conflict="nothing"`` 이라 같은 판단이 두 번 들어가지 않는다.
    judged_at = 배치 종료 시각(API 가 준 값) — 결과가 실제로 존재하게 된 시각이다.
    """
    for batch_id, model_id, prompt_version, items in _open_batches(con):
        ended_at = api.ended_at(batch_id)
        if ended_at is None:
            continue
        rows = []
        for r in api.results(batch_id):
            source_type, source_id, ticker = items[int(r.custom_id.removeprefix("r"))]
            for k in USAGE_FIELDS:
                stats[k] += r.usage.get(k, 0)
            if r.text is None:
                stats["api_failures"] += 1
                continue
            j = parse_judgment(r.text)
            if j is None:
                stats["parse_failures"] += 1
                continue
            rows.append((
                source_type, source_id, ticker, j.event_type,
                j.sentiment_direction, json.dumps(j.related_codes, ensure_ascii=False),
                j.is_stale_repeat, j.first_seen_date, j.price_impact_likely,
                j.rationale, model_id, prompt_version, today, j.confidence, ended_at,
            ))
        if rows:
            upsert_news_judgments(con, rows)
        stats["judged"] += len(rows)
        execute(con, "UPDATE news_judge_batches SET collected_at = ? WHERE batch_id = ?",
                (datetime.now(timezone.utc).isoformat(), batch_id))


def collect(
    con: Any, api: BatchAPI, *, model_id: str, today: str | None = None,
    poll_timeout_s: float = POLL_TIMEOUT_S, poll_interval_s: float = POLL_INTERVAL_S,
    sleep: Callable[[float], None] = time.sleep, clock: Callable[[], float] = time.monotonic,
) -> dict[str, int]:
    """지난 배치 수거 → 새 기사 배치 제출 → 끝날 때까지(최대 ``poll_timeout_s``) 기다려 수거.

    반환 stats: target(이번에 낸 기사 수)·judged·parse_failures·api_failures·in_flight(못
    기다리고 남긴 기사 수 — 다음 실행이 먼저 수거한다)와 usage 토큰 합계.

    ``today``는 이 레포 관례대로 압축형 ``YYYYMMDD`` — knowledge_date 는 **수거한 날**이다
    (어제 낸 배치를 오늘 수거하면 오늘 안 것). 늦게 잡을 뿐 앞당기지 않는다(CLAUDE.md §3).

    배치 id 는 create 가 돌아오자마자 원장(``news_judge_batches``)에 쓴다 — 이후 어디서
    죽어도(재시도 포함) 다음 실행이 그 배치를 알고, 그 안의 기사를 다시 내지 않는다.

    v1(동기 루프)과 달라진 점: 같은 배치 안 기사끼리는 서로의 판단을 ``_prior_context`` 로
    못 본다 — 같은 날 같은 종목의 재탕은 ``is_stale_repeat`` 로 덜 잡힌다. 이전 실행들의
    판단은 그대로 보인다.
    """
    today = today or datetime.now().strftime("%Y%m%d")
    today_dt = datetime.strptime(today, "%Y%m%d")
    cutoff = (today_dt - timedelta(days=STALE_LOOKBACK_DAYS)).strftime("%Y%m%d")
    # 새로 들어오는 항목만 판단한다(스펙 "백필 범위") — since는 published_at(ISO datetime
    # 문자열)과 문자열 비교되므로 대시 포함 ISO 날짜(YYYY-MM-DD)여야 한다.
    since = (today_dt - timedelta(days=PENDING_LOOKBACK_DAYS)).strftime("%Y-%m-%d")
    stats = dict.fromkeys(
        ("target", "judged", "parse_failures", "api_failures", "in_flight", *USAGE_FIELDS), 0)

    _collect_ended(con, api, today, stats)   # 지난 실행이 못 기다린 배치부터

    in_flight = {tuple(it) for *_, items in _open_batches(con) for it in items}
    targets = [(st, sid, item) for st, sid, item in _pending_items(con, since)
               if (st, sid, item["ticker"]) not in in_flight]
    if targets:
        requests, items = [], []
        for i, (source_type, source_id, item) in enumerate(targets):
            prior = _prior_context(con, item["ticker"], cutoff)
            requests.append(build_request(f"r{i}", build_prompt(item, prior), model_id))
            items.append([source_type, source_id, item["ticker"]])
        batch_id = api.submit(requests)
        execute(con,
            "INSERT INTO news_judge_batches (batch_id, model_id, prompt_version, submitted_at, items) "
            "VALUES (?, ?, ?, ?, ?)",
            (batch_id, model_id, PROMPT_VERSION, datetime.now(timezone.utc).isoformat(),
             json.dumps(items, ensure_ascii=False)))
        stats["target"] = len(targets)

    deadline = clock() + poll_timeout_s
    while True:
        _collect_ended(con, api, today, stats)
        con.commit()   # 폴링 동안 psycopg2 트랜잭션을 열어두지 않는다(idle in transaction)
        if not _open_batches(con) or clock() >= deadline:
            break
        sleep(poll_interval_s)
    stats["in_flight"] = sum(len(items) for *_, items in _open_batches(con))
    con.commit()
    return stats


def main() -> int:
    ap = argparse.ArgumentParser(description="뉴스/공시 LLM 판단 (Message Batches)")
    ap.add_argument("--db", default=None, help="DSN (postgresql://... 또는 sqlite 경로)")
    ap.add_argument("--model-id", default=MODEL_ID, help="Claude 모델 ID — Haiku 5.5 만 쓴다")
    ap.add_argument("--poll-timeout", type=float, default=POLL_TIMEOUT_S,
                    help="배치를 기다리는 최대 초(못 기다린 배치는 다음 실행이 수거)")
    args = ap.parse_args()

    import os

    from .storage import connect

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        raise SystemExit("환경변수 ANTHROPIC_API_KEY 필요")

    con = connect(args.db)
    stats = collect(con, ClaudeBatchAPI(api_key), model_id=args.model_id,
                    poll_timeout_s=args.poll_timeout)
    con.close()
    print(f"제출 {stats['target']}건 | 판단 {stats['judged']}건 | "
          f"파싱실패 {stats['parse_failures']}건 | API실패 {stats['api_failures']}건 | "
          f"진행중 {stats['in_flight']}건", flush=True)
    print("usage: " + " ".join(f"{k}={stats[k]}" for k in USAGE_FIELDS), flush=True)
    if stats["in_flight"]:
        print(f"⚠️ 배치 {stats['in_flight']}건을 못 기다렸다 — 다음 실행이 먼저 수거한다", flush=True)
    if stats["api_failures"] > 0:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
