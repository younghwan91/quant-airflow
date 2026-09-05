# 뉴스/공시 LLM 판단(news_judgments) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `daily_news`가 쌓는 `news_articles`/`disclosures`를 LLM(Gemini)이 읽어
`news_judgments`에 구조화된 판단(이벤트 유형·감성·동일 테마 종목·재탕 여부)을
쌓고, 장전(08:45)·장중(10:05·16:05) DAG에서 그 판단을 만든다.

**Architecture:** 순수 파싱/프롬프트 함수(네트워크 없이 단위테스트)와 얇은
Gemini 호출 wrapper를 분리하는 이 레포의 기존 관례(`dart_earnings.py`)를
따른다. 스키마는 Postgres(운영)와 sqlite(로컬/테스트) 두 정본에 반영한다
(`CLAUDE.md` §2 체크리스트). `news_judgments`는 `earnings`와 같은 **일반
테이블**이다(hypertable 아님) — 물량이 하루 수십~수백 건이라 압축 정책이
필요 없다.

**Tech Stack:** Python, psycopg2/sqlite3(기존 `collectors/storage.py` 추상화),
Gemini API(패키지명·버전은 Task 4에서 확인 후 핀), Airflow(TaskFlow API,
기존 DAG 패턴).

**Spec:** `docs/superpowers/specs/2026-09-06-news-llm-judgments-design.md`

## Global Constraints

- upsert 키는 `(source_type, source_id, ticker, prompt_version)` — 재실행해도
  중복 판단 없음, 기존 행은 절대 덮어쓰지 않음(`on_conflict="nothing"`).
- `knowledge_date`는 판단이 실제로 이뤄진 시점(DAG 런타임)만 찍는다 — 백필
  없음(v1은 도입 시점 이후 신규 항목만).
- API 레벨 실패(rate limit/인증/네트워크)는 태스크를 exit 1로 죽인다(실패를
  성공으로 보고하지 않는다). LLM 출력 파싱 실패는 항목 스킵만 하고 태스크는
  안 죽인다.
- 자격증명(Gemini API 키)은 Airflow Fernet Variable에만 두고 수집
  subprocess에만 주입한다 — 태스크 로그에 안 찍는다(`_masked()` 패턴).

---

### Task 1: 스키마 — 마이그레이션 + sqlite SCHEMA + docs

**Files:**
- Create: `sql/migrations/012_news_judgments.sql`
- Modify: `sql/init_timescale.sql` (같은 테이블 반영)
- Modify: `collectors/storage.py` (sqlite `SCHEMA` 상수에 테이블 추가, 약 269번째 줄 `disclosures` 정의 뒤)
- Modify: `docs/schema.md` (표 + 마이그레이션 목록)
- Test: `tests/test_storage.py` (또는 없으면 `tests/test_news_judgments_schema.py` 신설)

**Interfaces:**
- Produces: `news_judgments` 테이블 — 컬럼 `source_type, source_id, ticker,
  event_type, sentiment_direction, related_codes, is_stale_repeat,
  first_seen_date, price_impact_likely, rationale, model_id, prompt_version,
  knowledge_date`. PK `(source_type, source_id, ticker, prompt_version)`.
  `related_codes`는 **TEXT, JSON 인코딩된 배열 문자열**(Postgres `TEXT[]`
  아님 — `collectors/storage.py`의 제네릭 `_upsert()`가 타입 어댑터 없이
  파라미터화된 값을 그대로 바인딩하므로, 배열 타입을 쓰면 Postgres/sqlite
  경로가 갈라진다. JSON 문자열로 두면 두 백엔드가 완전히 동일한 코드를 탄다).

- [ ] **Step 1: 마이그레이션 작성**

`sql/migrations/012_news_judgments.sql`:

```sql
-- news_judgments: LLM이 news_articles/disclosures를 읽고 낸 구조화된 판단.
--
-- 왜: daily_news가 쌓는 원문 텍스트만으로는 단기 트레이딩 신호로 못 쓴다.
-- event_type/sentiment/related_codes/is_stale_repeat를 LLM이 구조화해 이
-- 테이블에 남긴다. 설계 근거는
-- docs/superpowers/specs/2026-09-06-news-llm-judgments-design.md 참고 —
-- 특히 related_codes(동일 테마 페어트레이드용)와 is_stale_repeat(재탕 뉴스
-- 판별)는 scalp-it 세션의 실제 트레이더 인터뷰 피드백으로 추가됐다.
--
-- earnings와 같은 이유로 일반 테이블이다(hypertable 아님) — 물량이 하루
-- 수십~수백 건이라 압축 정책 대상이 아니다.
--
-- related_codes는 TEXT(JSON 인코딩 배열) — Postgres TEXT[]를 안 쓰는 이유는
-- collectors/storage.py의 _upsert()가 타입 어댑터 없이 파라미터를 그대로
-- 바인딩해서, 배열 타입을 쓰면 Postgres/sqlite 두 경로가 갈라지기 때문이다.
--
-- upsert 키에 prompt_version이 들어가는 이유: 프롬프트/모델이 바뀌면 새
-- 버전으로 새 행을 쌓고 기존 행은 절대 안 고친다 — earnings의
-- knowledge_date 정정 이력 규약과 같은 재현성 원칙(한 번 쓴 LLM 판단은
-- 그 시점의 사실로 고정, 나중 모델로 재해석해서 덮어쓰지 않는다).
--
-- 적용:
--   psql "$DB_URL" -v ON_ERROR_STOP=1 -f sql/migrations/012_news_judgments.sql

BEGIN;

CREATE TABLE IF NOT EXISTS news_judgments (
    source_type         TEXT NOT NULL,   -- 'news' | 'disclosure'
    source_id           TEXT NOT NULL,   -- news_articles.id 또는 disclosures.id
    ticker              TEXT NOT NULL,
    event_type          TEXT NOT NULL,   -- 실적/유상증자/자사주/최대주주변경/소송/가이던스/규제/기타
    sentiment_direction INTEGER NOT NULL,  -- -1/0/1
    related_codes       TEXT NOT NULL DEFAULT '[]',  -- JSON 배열 문자열
    is_stale_repeat     BOOLEAN NOT NULL DEFAULT FALSE,
    first_seen_date     DATE,            -- is_stale_repeat=true일 때만 채움
    price_impact_likely BOOLEAN NOT NULL DEFAULT FALSE,
    rationale           TEXT NOT NULL DEFAULT '',
    model_id            TEXT NOT NULL,
    prompt_version      TEXT NOT NULL,
    knowledge_date      DATE NOT NULL,   -- 판단이 실제로 이뤄진 날 (백필 없음)
    PRIMARY KEY (source_type, source_id, ticker, prompt_version)
);
CREATE INDEX IF NOT EXISTS idx_news_judgments_ticker ON news_judgments(ticker);
CREATE INDEX IF NOT EXISTS idx_news_judgments_knowledge_date ON news_judgments(knowledge_date);

COMMIT;

-- 검증:
--   SELECT source_type, source_id, ticker, event_type FROM news_judgments LIMIT 5;
--   -- upsert 멱등성: 재실행 후 (source_type,source_id,ticker,prompt_version) 중복 0행
--   SELECT source_type, source_id, ticker, prompt_version, count(*)
--   FROM news_judgments GROUP BY 1,2,3,4 HAVING count(*) > 1;
--
-- 롤백:
--   DROP TABLE news_judgments;
```

- [ ] **Step 2: `sql/init_timescale.sql`에 같은 테이블 반영**

`earnings` 테이블 정의(약 133번째 줄) 바로 뒤에 위 `CREATE TABLE`~`CREATE
INDEX` 블록(BEGIN/COMMIT 제외, init 스크립트는 전체가 한 트랜잭션)을 그대로
붙여넣는다.

- [ ] **Step 3: `collectors/storage.py`의 sqlite `SCHEMA`에 반영**

`disclosures` 테이블 정의(268번째 줄 `idx_disclosures_published_at` 인덱스)
바로 뒤에 추가:

```sql
CREATE TABLE IF NOT EXISTS news_judgments (
    source_type         TEXT NOT NULL,
    source_id           TEXT NOT NULL,
    ticker              TEXT NOT NULL,
    event_type          TEXT NOT NULL,
    sentiment_direction INTEGER NOT NULL,
    related_codes       TEXT NOT NULL DEFAULT '[]',
    is_stale_repeat     BOOLEAN NOT NULL DEFAULT 0,
    first_seen_date     TEXT,
    price_impact_likely BOOLEAN NOT NULL DEFAULT 0,
    rationale           TEXT NOT NULL DEFAULT '',
    model_id            TEXT NOT NULL,
    prompt_version      TEXT NOT NULL,
    knowledge_date      TEXT NOT NULL,
    PRIMARY KEY (source_type, source_id, ticker, prompt_version)
);
CREATE INDEX IF NOT EXISTS idx_news_judgments_ticker ON news_judgments(ticker);
CREATE INDEX IF NOT EXISTS idx_news_judgments_knowledge_date ON news_judgments(knowledge_date);
```

(sqlite는 날짜를 TEXT로 저장하는 이 파일의 기존 관례를 따른다 — `disclosures`의
`published_at TEXT`와 동일.)

- [ ] **Step 4: 테스트로 스키마 생성 확인**

```python
def test_news_judgments_table_exists():
    from collectors.storage import connect
    con = connect(":memory:")
    cols = {r[1] for r in con.execute("PRAGMA table_info(news_judgments)").fetchall()}
    assert cols == {
        "source_type", "source_id", "ticker", "event_type",
        "sentiment_direction", "related_codes", "is_stale_repeat",
        "first_seen_date", "price_impact_likely", "rationale",
        "model_id", "prompt_version", "knowledge_date",
    }
```

Run: `pytest tests/test_storage.py -k news_judgments -v` (파일이 없으면
`tests/test_news_judgments_schema.py`로 새로 만들고 그 안에 이 테스트만 둔다)
Expected: PASS

- [ ] **Step 5: `docs/schema.md` 갱신**

`news_articles` 행(20번째 줄) 근처에 새 행 추가:

```
| `news_judgments` | LLM이 news_articles/disclosures를 읽고 낸 판단. PK `(source_type, source_id, ticker, prompt_version)` — prompt_version이 바뀌면 새 행, 기존 행은 절대 안 고침(재현성) |
```

마이그레이션 목록(76번째 줄 `010_news_articles` 근처)에 추가:

```
| `012_news_judgments` | `news_judgments` 신설 — LLM 뉴스/공시 판단, 장전/장중 DAG가 채움 |
```

- [ ] **Step 6: Commit**

```bash
git add sql/migrations/012_news_judgments.sql sql/init_timescale.sql \
        collectors/storage.py docs/schema.md tests/test_news_judgments_schema.py
git commit -m "feat(news): news_judgments 테이블 스키마 추가"
```

---

### Task 2: `storage.py` — `upsert_news_judgments()`

**Files:**
- Modify: `collectors/storage.py` (Task 1에서 만든 `_upsert()` 근처, `upsert_disclosures` 뒤)
- Test: `tests/test_storage.py` (기존 파일에 추가, 없으면 Task 1에서 만든 스키마 테스트 파일에 추가)

**Interfaces:**
- Consumes: `_upsert(con, table, cols, records, *, pk_cols, on_conflict)` (Task 1 스키마 대상, 기존 함수 시그니처 그대로)
- Produces: `upsert_news_judgments(con, records: list[tuple]) -> int`,
  `_NEWS_JUDGMENTS_COLS: list[str]` (다음 태스크들이 튜플 순서를 맞출 때 참조)

- [ ] **Step 1: 실패하는 테스트 작성**

```python
def test_upsert_news_judgments_is_idempotent_and_keeps_first_write(tmp_path):
    from collectors.storage import connect, upsert_news_judgments

    con = connect(tmp_path / "t.db")
    # 날짜 컬럼은 이 레포 관례대로 압축형 YYYYMMDD(예: dart_earnings.py의
    # today/avail_date)로 통일 — earnings.knowledge_date와 같은 포맷.
    row = ("news", "toss:abc", "005930", "실적", 1, "[]", 0, None, True,
           "실적 서프라이즈", "gemini-test", "v1", "20260906")
    upsert_news_judgments(con, [row])

    # 같은 PK로 재실행 — rationale이 달라져도 기존 행이 안 바뀐다(immutable).
    changed = ("news", "toss:abc", "005930", "실적", -1, "[]", 0, None, True,
               "바뀐 서술", "gemini-test", "v1", "20260906")
    upsert_news_judgments(con, [changed])

    rows = con.execute("SELECT rationale FROM news_judgments").fetchall()
    assert len(rows) == 1
    assert rows[0]["rationale"] == "실적 서프라이즈"  # 처음 쓴 값 그대로
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `pytest tests/test_storage.py -k news_judgments_is_idempotent -v`
Expected: FAIL — `ImportError: cannot import name 'upsert_news_judgments'`

- [ ] **Step 3: 구현**

`collectors/storage.py`의 `upsert_disclosures` 정의 뒤에 추가:

```python
_NEWS_JUDGMENTS_COLS = [
    "source_type", "source_id", "ticker", "event_type", "sentiment_direction",
    "related_codes", "is_stale_repeat", "first_seen_date", "price_impact_likely",
    "rationale", "model_id", "prompt_version", "knowledge_date",
]


def upsert_news_judgments(con: Any, records: list[tuple]) -> int:
    """Insert news_judgments rows (tuples ordered by _NEWS_JUDGMENTS_COLS).

    ``on_conflict="nothing"`` — 판단은 한 번 쓰면 불변이다. 재실행 시 같은
    (source_type, source_id, ticker, prompt_version)는 기존 값을 그대로
    보존한다(LLM 출력이 확률적이라 재실행마다 값이 달라질 수 있는데, 그걸
    덮어쓰면 "그 시점에 실제로 어떤 판단이 있었는지"라는 point-in-time
    기록이 흔들린다).
    """
    return _upsert(con, "news_judgments", _NEWS_JUDGMENTS_COLS, records,
                   pk_cols=("source_type", "source_id", "ticker", "prompt_version"),
                   on_conflict="nothing")
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `pytest tests/test_storage.py -k news_judgments -v`
Expected: PASS (2 tests: 스키마 존재 확인 + 멱등성)

- [ ] **Step 5: Commit**

```bash
git add collectors/storage.py tests/test_storage.py
git commit -m "feat(news): upsert_news_judgments 추가"
```

---

### Task 3: `news_judge.py` — 순수 함수 (프롬프트 생성 · 응답 파싱)

**Files:**
- Create: `collectors/news_judge.py`
- Test: `tests/test_news_judge.py`

**Interfaces:**
- Produces:
  - `EVENT_TYPES: tuple[str, ...]` — `("실적", "유상증자", "자사주", "최대주주변경", "소송", "가이던스", "규제", "기타")`
  - `Judgment` (dataclass): `event_type: str, sentiment_direction: int,
    related_codes: list[str], is_stale_repeat: bool, first_seen_date: str | None,
    price_impact_likely: bool, rationale: str`
  - `build_prompt(item: dict, prior_context: list[dict]) -> str`
  - `parse_judgment(llm_response: str) -> Judgment | None`

`item`은 판단 대상 1건: `{"ticker": str, "title": str, "content": str,
"published_at": str}` (news_articles/disclosures 공통으로 뽑아낸 최소 필드 —
Task 5의 `collect()`가 각 테이블에서 이 shape으로 변환해 넘긴다).

`prior_context`는 같은 ticker로 최근 N일 내 이미 쓰인 판단들의
`[{"date": str, "rationale": str}]` 목록(`date`는 그 판단의 `knowledge_date`,
`YYYYMMDD` — `news_judgments`가 원본 기사의 `published_at`을 안 저장하므로
그건 못 넣는다) — `is_stale_repeat` 판단의 근거로 프롬프트에 포함한다
(Task 4에서 DB 조회로 채워 넘긴다, 이 태스크에선 그냥 파라미터로 받는다).

- [ ] **Step 1: 실패하는 테스트 작성**

```python
from collectors.news_judge import EVENT_TYPES, Judgment, build_prompt, parse_judgment


def test_build_prompt_includes_item_fields_and_event_type_choices():
    item = {"ticker": "005930", "title": "삼성전자 3분기 실적 발표",
            "content": "영업이익 10조원...", "published_at": "2026-09-06T08:30:00"}
    prompt = build_prompt(item, prior_context=[])
    assert "005930" in prompt
    assert "삼성전자 3분기 실적 발표" in prompt
    for et in EVENT_TYPES:
        assert et in prompt


def test_build_prompt_includes_prior_context_when_present():
    item = {"ticker": "005930", "title": "새 소식", "content": "...",
            "published_at": "2026-09-06T08:30:00"}
    # prior_context의 "date"는 이전 판단의 knowledge_date(YYYYMMDD) — 원본
    # 기사의 published_at이 아니다. news_judgments에 published_at을 안
    # 저장하므로(그 정보는 news_articles/disclosures 쪽에 있다) 재구성 안 한다.
    prior = [{"date": "20260901", "rationale": "이전 유상증자 발표"}]
    prompt = build_prompt(item, prior_context=prior)
    assert "20260901" in prompt
    assert "이전 유상증자 발표" in prompt


def test_parse_judgment_valid_response():
    response = '''{"event_type": "실적", "sentiment_direction": 1,
        "related_codes": ["000660", "005935"], "is_stale_repeat": false,
        "first_seen_date": null, "price_impact_likely": true,
        "rationale": "영업이익 서프라이즈"}'''
    j = parse_judgment(response)
    assert j == Judgment(
        event_type="실적", sentiment_direction=1,
        related_codes=["000660", "005935"], is_stale_repeat=False,
        first_seen_date=None, price_impact_likely=True,
        rationale="영업이익 서프라이즈",
    )


def test_parse_judgment_rejects_unknown_event_type():
    response = '{"event_type": "존재안함", "sentiment_direction": 0, ' \
               '"related_codes": [], "is_stale_repeat": false, ' \
               '"first_seen_date": null, "price_impact_likely": false, "rationale": ""}'
    assert parse_judgment(response) is None


def test_parse_judgment_rejects_out_of_range_sentiment():
    response = '{"event_type": "기타", "sentiment_direction": 5, ' \
               '"related_codes": [], "is_stale_repeat": false, ' \
               '"first_seen_date": null, "price_impact_likely": false, "rationale": ""}'
    assert parse_judgment(response) is None


def test_parse_judgment_rejects_malformed_json():
    assert parse_judgment("이건 JSON이 아님") is None


def test_parse_judgment_rejects_missing_field():
    response = '{"event_type": "기타", "sentiment_direction": 0}'
    assert parse_judgment(response) is None
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `pytest tests/test_news_judge.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'collectors.news_judge'`

- [ ] **Step 3: 구현**

`collectors/news_judge.py`:

```python
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

import argparse
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
    if sentiment not in (-1, 0, 1):
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
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `pytest tests/test_news_judge.py -v`
Expected: PASS (7 tests)

- [ ] **Step 5: Commit**

```bash
git add collectors/news_judge.py tests/test_news_judge.py
git commit -m "feat(news): news_judge 순수 함수(build_prompt/parse_judgment) 추가"
```

---

### Task 4: `news_judge.py` — Gemini 호출 + 재개 가능한 `collect()`

**Files:**
- Modify: `collectors/news_judge.py`
- Modify: `docker/requirements.txt`
- Test: `tests/test_news_judge.py` (Task 3 파일에 추가)

**Interfaces:**
- Consumes: `build_prompt`, `parse_judgment`, `Judgment`, `STALE_LOOKBACK_DAYS`,
  `PROMPT_VERSION` (Task 3), `collectors.storage.upsert_news_judgments`,
  `_NEWS_JUDGMENTS_COLS` (Task 2)
- Produces: `judge_item(generate: Callable[[str], str], item: dict,
  prior_context: list[dict]) -> Judgment | None`, `collect(con, generate,
  model_id: str, *, today: str | None = None) -> dict[str, int]` (반환:
  `{"target": N, "judged": M, "api_failures": K}`)

**Step 0 — Gemini SDK 확인 (구현 전 필수):** 이 시점에 실제로 pip에서
현재 권장되는 Gemini Python SDK 패키지명·버전을 확인한다(`pip index versions
google-genai` 또는 https://ai.google.dev/gemini-api/docs/libraries 참고 —
이 계획을 쓴 시점엔 확정 짓지 않았다, 추측으로 버전을 박지 않는다). 확인한
정확한 이름/버전으로 아래 Step 3의 `import`와 `docker/requirements.txt` 줄을
채운다.

- [ ] **Step 1: 실패하는 테스트 작성 (judge_item — generate는 mock)**

```python
from collectors.news_judge import judge_item


def test_judge_item_returns_parsed_judgment_on_valid_response():
    def fake_generate(prompt: str) -> str:
        assert "005930" in prompt
        return ('{"event_type": "실적", "sentiment_direction": 1, '
                '"related_codes": [], "is_stale_repeat": false, '
                '"first_seen_date": null, "price_impact_likely": true, '
                '"rationale": "테스트"}')

    item = {"ticker": "005930", "title": "제목", "content": "본문",
            "published_at": "2026-09-06T08:30:00"}
    j = judge_item(fake_generate, item, prior_context=[])
    assert j is not None
    assert j.event_type == "실적"


def test_judge_item_returns_none_on_unparseable_response():
    j = judge_item(lambda prompt: "JSON 아님",
                    {"ticker": "005930", "title": "t", "content": "c",
                     "published_at": "2026-09-06T08:30:00"}, prior_context=[])
    assert j is None
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `pytest tests/test_news_judge.py -k judge_item -v`
Expected: FAIL — `ImportError: cannot import name 'judge_item'`

- [ ] **Step 3: `judge_item` 구현 + Gemini 클라이언트 wrapper**

`collectors/news_judge.py`에 추가(Step 0에서 확인한 실제 패키지로 `_gemini_generate` 채움):

```python
from typing import Callable


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

    Step 0에서 확인한 SDK로 채운다 — 아래는 그 SDK의 공식 사용 예를 그대로
    따르는 자리이지, 지어낸 API가 아니다. 반드시 Step 0 확인 후 이 함수 본문을
    실제 SDK 호출로 교체한다.
    """
    raise NotImplementedError(
        "Step 0에서 확인한 Gemini SDK로 이 함수를 채운다 — "
        "model_id/api_key를 받아 prompt->response 텍스트 호출을 감싼다."
    )
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `pytest tests/test_news_judge.py -k judge_item -v`
Expected: PASS (2 tests — `_gemini_generate`는 아직 안 부르므로 안 깨짐)

- [ ] **Step 5: 실패하는 테스트 작성 (`collect` — resume·API실패 카운트)**

```python
from collectors.storage import connect, upsert_disclosures, upsert_news_judgments
from collectors.news_judge import collect


def _seed_disclosure(con, disclosure_id="dart:x", ticker="005930"):
    upsert_disclosures(con, [(
        disclosure_id, "dart", "제목", "https://dart.fss.or.kr/x",
        "삼성전자", ticker, "주요사항보고서",
        "2026-09-06T08:00:00", "2026-09-06T08:45:00",
    )])


def test_collect_judges_new_disclosure_and_upserts(tmp_path):
    con = connect(tmp_path / "t.db")
    _seed_disclosure(con)

    def fake_generate(prompt: str) -> str:
        return ('{"event_type": "기타", "sentiment_direction": 0, '
                '"related_codes": [], "is_stale_repeat": false, '
                '"first_seen_date": null, "price_impact_likely": false, '
                '"rationale": "테스트"}')

    stats = collect(con, fake_generate, model_id="gemini-test", today="20260906")
    assert stats == {"target": 1, "judged": 1, "api_failures": 0}

    rows = con.execute("SELECT ticker, event_type FROM news_judgments").fetchall()
    assert len(rows) == 1
    assert rows[0]["ticker"] == "005930"


def test_collect_skips_already_judged_at_same_prompt_version(tmp_path):
    con = connect(tmp_path / "t.db")
    _seed_disclosure(con)
    upsert_news_judgments(con, [(
        "disclosure", "dart:x", "005930", "기타", 0, "[]", 0, None, False,
        "기존 판단", "gemini-test", "v1", "20260906",
    )])

    calls = []
    def fake_generate(prompt: str) -> str:
        calls.append(prompt)
        return "안 불려야 함"

    stats = collect(con, fake_generate, model_id="gemini-test", today="20260906")
    assert calls == []
    assert stats == {"target": 0, "judged": 0, "api_failures": 0}


def test_collect_counts_api_failures_without_writing_a_row(tmp_path):
    con = connect(tmp_path / "t.db")
    _seed_disclosure(con)

    def failing_generate(prompt: str) -> str:
        raise RuntimeError("rate limited")

    stats = collect(con, failing_generate, model_id="gemini-test", today="20260906")
    assert stats == {"target": 1, "judged": 0, "api_failures": 1}
    assert con.execute("SELECT count(*) AS n FROM news_judgments").fetchone()["n"] == 0


def test_collect_skips_malformed_output_without_counting_as_api_failure(tmp_path):
    con = connect(tmp_path / "t.db")
    _seed_disclosure(con)

    stats = collect(con, lambda prompt: "JSON 아님", model_id="gemini-test", today="20260906")
    assert stats == {"target": 1, "judged": 0, "api_failures": 0}
```

- [ ] **Step 6: 테스트 실패 확인**

Run: `pytest tests/test_news_judge.py -k collect -v`
Expected: FAIL — `ImportError: cannot import name 'collect'`

- [ ] **Step 7: `collect()` 구현**

```python
from datetime import datetime, timedelta
from typing import Any

from .storage import fetchall, upsert_news_judgments


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
```

- [ ] **Step 8: 테스트 통과 확인**

Run: `pytest tests/test_news_judge.py -v`
Expected: PASS (전체 — Task 3 7개 + Task 4 6개, 13 tests)

- [ ] **Step 9: `docker/requirements.txt`에 Gemini SDK 추가**

Step 0에서 확인한 정확한 패키지명·버전으로 한 줄 추가(예시 형태 —
실제 값은 확인 후 채운다):

```
<확인한-패키지명>>=<확인한-버전>
```

- [ ] **Step 10: Commit**

```bash
git add collectors/news_judge.py tests/test_news_judge.py docker/requirements.txt
git commit -m "feat(news): news_judge collect() — 재개 가능한 판단 루프 + Gemini 연동"
```

---

### Task 5: CLI + DAG 연결

**Files:**
- Modify: `collectors/news_judge.py` (`main()` 추가)
- Modify: `dags/_common.py` (`gemini_env()` 추가)
- Create: `dags/premarket_news_judgment.py`
- Modify: `dags/daily_news.py` (판단 태스크 추가)
- Test: `tests/test_news_judge.py` (CLI 파싱만 — 네트워크 없이)

**Interfaces:**
- Consumes: `collect()` (Task 4), `dags/_common.py`의 `DEFAULT_TASK_KW,
  run_collector, timescale_dsn, dart_env` (기존, 패턴만 참고 — DART 키는 이
  DAG에 안 씀)
- Produces: `main() -> int` (CLI), `gemini_env() -> dict[str, str]`

- [ ] **Step 1: `news_judge.py`에 `main()` 추가**

```python
def main() -> int:
    ap = argparse.ArgumentParser(description="뉴스/공시 LLM 판단")
    ap.add_argument("--db", default=None, help="DSN (postgresql://... 또는 sqlite 경로)")
    ap.add_argument("--model-id", default="gemini-flash-placeholder",
                    help="Step 0에서 확인한 실제 모델 문자열로 기본값 교체")
    args = ap.parse_args()

    import os
    from .storage import connect

    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        raise SystemExit("환경변수 GEMINI_API_KEY 필요")

    con = connect(args.db)
    generate = _gemini_generate(args.model_id, api_key)
    stats = collect(con, generate, model_id=args.model_id)
    con.close()
    print(f"대상 {stats['target']}건 | 판단 {stats['judged']}건 | "
          f"API실패 {stats['api_failures']}건", flush=True)
    if stats["api_failures"] > 0:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 2: CLI 파싱 테스트**

```python
def test_main_requires_gemini_api_key(monkeypatch):
    import pytest
    from collectors import news_judge

    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.setattr("sys.argv", ["news_judge", "--db", ":memory:"])
    with pytest.raises(SystemExit, match="GEMINI_API_KEY"):
        news_judge.main()
```

Run: `pytest tests/test_news_judge.py -k main -v`
Expected: PASS

- [ ] **Step 3: `dags/_common.py`에 `gemini_env()` 추가**

`dart_env()` 함수 뒤에 추가:

```python
def gemini_env() -> dict[str, str]:
    # Gemini 키도 다른 자격증명과 같이 Fernet Variables에만 둔다.
    env = os.environ.copy()
    env["GEMINI_API_KEY"] = Variable.get("GEMINI_API_KEY")
    return env
```

- [ ] **Step 4: `dags/premarket_news_judgment.py` 신설**

```python
"""장전 뉴스/공시 판단 — 08:45 KST, 09:00 시가 전 시가 진입 판단용.

daily_news와 같은 수집 태스크(news_toss.py/news_dart.py, 자연키 upsert라
몇 번을 다시 돌아도 안전)를 재사용하고, 그 뒤에 news_judge.py로 판단한다.
daily_news(10:05·16:05)가 나중에 같은 항목을 다시 봐도 news_judgments의
(source_type, source_id, ticker, prompt_version) upsert 키 덕에 중복
판단되지 않는다.

만쥬 인터뷰(scalp-it 레포 docs/research/manju/46-manju-answers.md §3)의
"08:50까지 테마 매핑 완성" 워크플로우와 맞추려고 08:45로 잡았다 — 설계 근거는
docs/superpowers/specs/2026-09-06-news-llm-judgments-design.md 참고.
"""

from __future__ import annotations

import sys

import pendulum
from airflow.decorators import dag, task

from _common import DEFAULT_TASK_KW, dart_env, gemini_env, run_collector, timescale_dsn


@dag(
    dag_id="premarket_news_judgment",
    schedule="45 8 * * 1-5",  # 평일 08:45 KST
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["kr-quant", "collection", "news", "llm"],
)
def premarket_news_judgment():

    @task(**DEFAULT_TASK_KW)
    def collect_toss_news() -> None:
        run_collector([
            sys.executable, "-m", "collectors.news_toss",
            "--db", timescale_dsn(),
        ])

    @task(**DEFAULT_TASK_KW)
    def collect_dart_disclosures() -> None:
        run_collector([
            sys.executable, "-m", "collectors.news_dart",
            "--db", timescale_dsn(),
        ], env=dart_env())

    @task(**DEFAULT_TASK_KW)
    def judge_news() -> None:
        run_collector([
            sys.executable, "-m", "collectors.news_judge",
            "--db", timescale_dsn(),
        ], env=gemini_env())

    [collect_toss_news(), collect_dart_disclosures()] >> judge_news()


premarket_news_judgment()
```

- [ ] **Step 5: `dags/daily_news.py`에 판단 태스크 추가**

`dags/daily_news.py`의 `collect_dart_disclosures` 태스크 정의 뒤,
`collect_toss_news()`/`collect_dart_disclosures()` 호출 앞에 추가:

```python
    @task(**DEFAULT_TASK_KW)
    def judge_news() -> None:
        run_collector([
            sys.executable, "-m", "collectors.news_judge",
            "--db", timescale_dsn(),
        ], env=gemini_env())
```

그리고 마지막 줄을:

```python
    [collect_toss_news(), collect_dart_disclosures()] >> judge_news()
```

로 바꾼다(기존 `collect_toss_news()` / `collect_dart_disclosures()` 두 줄
대체). import 줄도 `gemini_env` 추가:

```python
from _common import DEFAULT_TASK_KW, dart_env, gemini_env, run_collector, timescale_dsn
```

- [ ] **Step 6: DAG AST 파싱 확인 (CI가 실제로 돌리는 것과 같은 검사)**

```bash
python - <<'PY'
import ast, pathlib
for name in ("premarket_news_judgment.py", "daily_news.py"):
    ast.parse(pathlib.Path("dags", name).read_text())
    print(f"{name}: OK")
PY
```

Expected: 둘 다 `OK` 출력, 예외 없음

- [ ] **Step 7: 전체 테스트 + lint**

```bash
ruff check collectors/ dags/ tests/
pytest -q
```

Expected: ruff `All checks passed!`, 기존 204여 개 + 이번에 추가한 테스트
전부 PASS

- [ ] **Step 8: Commit**

```bash
git add collectors/news_judge.py dags/_common.py dags/premarket_news_judgment.py dags/daily_news.py
git commit -m "feat(news): 장전/장중 DAG에 LLM 판단 태스크 연결"
```

---

## Self-Review 메모 (계획 작성자용, 실행자는 무시)

- **스펙 커버리지:** 데이터 모델(Task 1-2), 실행 흐름 3개 트리거(Task 5),
  모듈 구조(Task 3-4), 에러 처리 2종(Task 4 `collect()`), 백필 없음(Task 4
  `_pending_items`가 기존 행 존재만 체크, 과거 스캔 없음), 검증 쿼리(Task 1
  마이그레이션 주석), 컨슈머 조인(스펙에 예시 있음, 코드 변경 불필요 — 읽기
  전용이므로 이 계획에 태스크 없음, 의도적). 커버 안 된 것: `first_seen_date`를
  `is_stale_repeat=true`일 때 실제로 채우는 로직은 LLM 응답을 그대로 신뢰한다
  (DB가 계산해서 강제하지 않음) — 스펙의 "SQL로 후보를 좁히고 LLM이 최종
  판단"과 일치, 의도된 설계.
- **플레이스홀더 스캔:** Task 4 Step 0/Step 3의 Gemini SDK 확인은 "TBD"가
  아니라 "지금 확정하면 틀릴 걸 알아서 명시적 검증 스텝으로 미룬 것" —
  구체적 명령(`pip index versions`)과 그 이후 무엇을 채워야 하는지 정확히
  지정돼 있다.
- **타입 일관성:** `Judgment` 필드명이 Task 3(정의)·Task 4(`judge_item`/`collect`
  소비)·Task 1 마이그레이션 컬럼명과 전부 일치 확인함(`related_codes`,
  `is_stale_repeat`, `first_seen_date`, `price_impact_likely` 철자 통일).
