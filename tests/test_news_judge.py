from collectors.news_judge import (
    EVENT_TYPES, PROMPT_VERSION, SYSTEM_PROMPT, BatchResult, Judgment, build_prompt, build_request, collect,
    parse_judgment,
)
from collectors.storage import connect, upsert_disclosures, upsert_news_judgments


def _seed_disclosure(con, disclosure_id="dart:x", ticker="005930", published_at="2026-09-06T08:00:00"):
    upsert_disclosures(con, [(
        disclosure_id, "dart", "제목", "https://dart.fss.or.kr/x",
        "삼성전자", ticker, "주요사항보고서",
        published_at, "2026-09-06T08:45:00",
    )])


def test_build_prompt_includes_item_fields_and_event_type_choices():
    item = {"ticker": "005930", "title": "삼성전자 3분기 실적 발표",
            "content": "영업이익 10조원...", "published_at": "2026-09-06T08:30:00"}
    prompt = build_prompt(item, prior_context=[])
    assert "005930" in prompt
    assert "삼성전자 3분기 실적 발표" in prompt
    # 고정 지시문은 system(프롬프트 캐시) 쪽에만 — user 에 섞이면 캐시 접두가 매번 달라진다
    for et in EVENT_TYPES:
        assert et in SYSTEM_PROMPT
    assert "응답 형식" in SYSTEM_PROMPT
    assert "응답 형식" not in prompt


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
        "rationale": "영업이익 서프라이즈", "confidence": 80}'''
    j = parse_judgment(response)
    assert j == Judgment(
        event_type="실적", sentiment_direction=1,
        related_codes=["000660", "005935"], is_stale_repeat=False,
        first_seen_date=None, price_impact_likely=True,
        rationale="영업이익 서프라이즈", confidence=80,
    )


def test_parse_judgment_strips_markdown_code_fence():
    # Claude Haiku 4.5는 "JSON만, 다른 텍스트 없이"라고 명시해도 실측으로
    # ```json 펜스를 씌워 응답한다(2026-09-06 스모크테스트) — 이걸 못 벗기면
    # Gemini→Claude 전환 후 모든 판단이 parse_failures로 잡힌다.
    response = ('```json\n{"event_type": "실적", "sentiment_direction": 1, '
                '"related_codes": [], "is_stale_repeat": false, '
                '"first_seen_date": null, "price_impact_likely": true, '
                '"rationale": "테스트", "confidence": 80}\n```')
    j = parse_judgment(response)
    assert j is not None
    assert j.event_type == "실적"
    assert j.confidence == 80


def test_parse_judgment_rejects_unknown_event_type():
    response = '{"event_type": "존재안함", "sentiment_direction": 0, ' \
               '"related_codes": [], "is_stale_repeat": false, ' \
               '"first_seen_date": null, "price_impact_likely": false, "rationale": "", ' \
               '"confidence": 50}'
    assert parse_judgment(response) is None


def test_parse_judgment_rejects_out_of_range_sentiment():
    response = '{"event_type": "기타", "sentiment_direction": 5, ' \
               '"related_codes": [], "is_stale_repeat": false, ' \
               '"first_seen_date": null, "price_impact_likely": false, "rationale": "", ' \
               '"confidence": 50}'
    assert parse_judgment(response) is None


def test_parse_judgment_rejects_malformed_json():
    assert parse_judgment("이건 JSON이 아님") is None


def test_parse_judgment_rejects_missing_field():
    response = '{"event_type": "기타", "sentiment_direction": 0}'
    assert parse_judgment(response) is None


def test_parse_judgment_rejects_boolean_sentiment():
    response = '{"event_type": "기타", "sentiment_direction": true, ' \
               '"related_codes": [], "is_stale_repeat": false, ' \
               '"first_seen_date": null, "price_impact_likely": false, "rationale": "", ' \
               '"confidence": 50}'
    assert parse_judgment(response) is None


def test_parse_judgment_rejects_out_of_range_confidence():
    response = '{"event_type": "기타", "sentiment_direction": 0, ' \
               '"related_codes": [], "is_stale_repeat": false, ' \
               '"first_seen_date": null, "price_impact_likely": false, "rationale": "", ' \
               '"confidence": 101}'
    assert parse_judgment(response) is None


def test_parse_judgment_rejects_boolean_confidence():
    response = '{"event_type": "기타", "sentiment_direction": 0, ' \
               '"related_codes": [], "is_stale_repeat": false, ' \
               '"first_seen_date": null, "price_impact_likely": false, "rationale": "", ' \
               '"confidence": true}'
    assert parse_judgment(response) is None


def test_parse_judgment_rejects_non_yyyymmdd_first_seen_date():
    response = ('{"event_type": "기타", "sentiment_direction": 0, '
                '"related_codes": [], "is_stale_repeat": true, '
                '"first_seen_date": "2026년 9월", "price_impact_likely": false, '
                '"rationale": ""}')
    assert parse_judgment(response) is None


def test_parse_judgment_rejects_invalid_calendar_date_first_seen():
    # 8자리 숫자지만 달력에 없는 날짜(13월) — strptime이 ValueError를 던진다.
    response = ('{"event_type": "기타", "sentiment_direction": 0, '
                '"related_codes": [], "is_stale_repeat": true, '
                '"first_seen_date": "20261301", "price_impact_likely": false, '
                '"rationale": ""}')
    assert parse_judgment(response) is None


def test_parse_judgment_accepts_valid_yyyymmdd_first_seen_date():
    response = ('{"event_type": "기타", "sentiment_direction": 0, '
                '"related_codes": [], "is_stale_repeat": true, '
                '"first_seen_date": "20260901", "price_impact_likely": false, '
                '"rationale": "", "confidence": 50}')
    j = parse_judgment(response)
    assert j is not None
    assert j.first_seen_date == "20260901"


def test_pending_items_excludes_disclosures_published_before_since(tmp_path):
    from collectors.news_judge import _pending_items

    con = connect(tmp_path / "t.db")
    _seed_disclosure(con, disclosure_id="dart:old", ticker="005930",
                      published_at="2026-08-01T08:00:00")
    _seed_disclosure(con, disclosure_id="dart:new", ticker="000660",
                      published_at="2026-09-06T08:00:00")

    items = _pending_items(con, since="2026-09-01")
    ids = {source_id for _, source_id, _ in items}
    assert ids == {"dart:new"}


_OK = ('{"event_type": "기타", "sentiment_direction": 0, "related_codes": [], '
       '"is_stale_repeat": false, "first_seen_date": null, "price_impact_likely": false, '
       '"rationale": "테스트", "confidence": 60}')
_USAGE = {"input_tokens": 120, "cache_creation_input_tokens": 0,
          "cache_read_input_tokens": 600, "output_tokens": 90}


class FakeBatchAPI:
    """Message Batches 가짜. ``answer(prompt)`` → 응답 텍스트(None = errored), ``ends_after`` 번
    조회한 뒤에 끝난 걸로 본다."""

    def __init__(self, answer=lambda prompt: _OK, ends_after=0, fail_submit=False):
        self.answer, self.ends_after, self.fail_submit = answer, ends_after, fail_submit
        self.batches: dict[str, list[dict]] = {}
        self.polls: dict[str, int] = {}

    def submit(self, requests):
        if self.fail_submit:
            raise RuntimeError("503")
        batch_id = f"msgbatch_{len(self.batches)}"
        self.batches[batch_id] = requests
        self.polls[batch_id] = 0
        return batch_id

    def ended_at(self, batch_id):
        self.polls[batch_id] += 1
        return "2026-09-06T00:10:00+00:00" if self.polls[batch_id] > self.ends_after else None

    def results(self, batch_id):
        for r in self.batches[batch_id]:
            text = self.answer(r["params"]["messages"][0]["content"])
            yield BatchResult(r["custom_id"], text, _USAGE if text is not None else {})


def _collect(con, api, **kw):
    kw.setdefault("poll_timeout_s", 0)
    return collect(con, api, model_id="claude-haiku-5-5", today="20260906",
                   sleep=lambda s: None, **kw)


def test_build_request_caches_system_disables_thinking_and_omits_temperature():
    # Haiku 5.5 는 thinking 이 기본으로 켜져 있다 — 안 끄면 max_tokens 가 생각에 먹혀 빈 응답.
    # 고정 지시문은 cache_control 붙은 system 블록, 건마다 다른 부분만 user 로.
    req = build_request("r0", "user 본문", "claude-haiku-5-5")
    params = req["params"]
    assert req["custom_id"] == "r0"
    assert params["model"] == "claude-haiku-5-5"
    assert params["thinking"] == {"type": "disabled"}
    assert "temperature" not in params
    assert params["system"] == [{"type": "text", "text": SYSTEM_PROMPT,
                                 "cache_control": {"type": "ephemeral"}}]
    assert params["messages"] == [{"role": "user", "content": "user 본문"}]


def test_collect_submits_one_batch_and_writes_judgments_with_batch_end_time(tmp_path):
    con = connect(tmp_path / "t.db")
    _seed_disclosure(con, disclosure_id="dart:a", ticker="005930")
    _seed_disclosure(con, disclosure_id="dart:b", ticker="000660")
    api = FakeBatchAPI()

    stats = _collect(con, api)

    assert len(api.batches) == 1 and len(api.batches["msgbatch_0"]) == 2
    assert stats["target"] == 2 and stats["judged"] == 2 and stats["in_flight"] == 0
    assert stats["cache_read_input_tokens"] == 1200
    rows = con.execute("SELECT ticker, model_id, prompt_version, knowledge_date, judged_at "
                       "FROM news_judgments ORDER BY ticker").fetchall()
    assert [r["ticker"] for r in rows] == ["000660", "005930"]
    assert {(r["model_id"], r["prompt_version"], r["knowledge_date"], r["judged_at"]) for r in rows} == {
        ("claude-haiku-5-5", PROMPT_VERSION, "20260906", "2026-09-06T00:10:00+00:00")}
    ledger = con.execute("SELECT collected_at FROM news_judge_batches").fetchall()
    assert len(ledger) == 1 and ledger[0]["collected_at"] is not None


def test_collect_skips_already_judged_at_same_prompt_version(tmp_path):
    con = connect(tmp_path / "t.db")
    _seed_disclosure(con)
    upsert_news_judgments(con, [(
        "disclosure", "dart:x", "005930", "기타", 0, "[]", 0, None, False,
        "기존 판단", "claude-test", PROMPT_VERSION, "20260906", 60, "2026-09-06T00:00:00+00:00",
    )])
    api = FakeBatchAPI()

    stats = _collect(con, api)
    assert api.batches == {}
    assert stats["target"] == 0 and stats["judged"] == 0


def test_collect_leaves_unfinished_batch_and_next_run_collects_without_resubmitting(tmp_path):
    # 못 기다린 배치는 원장에 남고, 다음 실행은 그 기사를 다시 내지 않고(이중 과금) 먼저 수거한다.
    con = connect(tmp_path / "t.db")
    _seed_disclosure(con)
    api = FakeBatchAPI(ends_after=3)

    first = _collect(con, api)
    assert first["target"] == 1 and first["judged"] == 0 and first["in_flight"] == 1
    assert con.execute("SELECT count(*) AS n FROM news_judgments").fetchone()["n"] == 0

    second = _collect(con, api)
    assert len(api.batches) == 1          # 다시 내지 않았다
    assert second["target"] == 0 and second["in_flight"] == 1

    third = _collect(con, api)
    assert third["judged"] == 1 and third["in_flight"] == 0
    assert con.execute("SELECT count(*) AS n FROM news_judgments").fetchone()["n"] == 1


def test_collect_polls_until_batch_ends_within_timeout(tmp_path):
    con = connect(tmp_path / "t.db")
    _seed_disclosure(con)
    api = FakeBatchAPI(ends_after=2)
    sleeps = []
    t = iter(range(100))

    stats = collect(con, api, model_id="claude-haiku-5-5", today="20260906",
                    poll_timeout_s=50, poll_interval_s=7, sleep=sleeps.append, clock=lambda: next(t))
    assert stats["judged"] == 1 and stats["in_flight"] == 0
    assert sleeps == [7, 7]


def test_collect_counts_errored_and_malformed_results_without_writing_rows(tmp_path):
    con = connect(tmp_path / "t.db")
    _seed_disclosure(con, disclosure_id="dart:a", ticker="005930")
    _seed_disclosure(con, disclosure_id="dart:b", ticker="000660")
    api = FakeBatchAPI(answer=lambda prompt: None if "005930" in prompt else "JSON 아님")

    stats = _collect(con, api)
    assert stats["api_failures"] == 1 and stats["parse_failures"] == 1 and stats["judged"] == 0
    assert con.execute("SELECT count(*) AS n FROM news_judgments").fetchone()["n"] == 0

    # 실패한 기사는 판단이 없으니 다음 실행이 다시 낸다
    api.answer = lambda prompt: _OK
    again = _collect(con, api)
    assert again["target"] == 2 and again["judged"] == 2


def test_collect_records_nothing_when_submit_fails(tmp_path):
    # create 는 재시도하지 않는다(5xx 뒤 배치가 생겼을 수도 — 이중 과금). 실패는 그대로 올라간다.
    import pytest

    con = connect(tmp_path / "t.db")
    _seed_disclosure(con)
    with pytest.raises(RuntimeError):
        _collect(con, FakeBatchAPI(fail_submit=True))
    assert con.execute("SELECT count(*) AS n FROM news_judge_batches").fetchone()["n"] == 0


def test_collect_never_judges_old_disclosure_even_without_prior_judgment(tmp_path):
    # C2: 백필 가드. since 하한이 없으면 이 오래된 공시(발행 2026-08-01)도 오늘(2026-09-06)
    # 판단 대상에 잡혀 knowledge_date=오늘로 영구 기록된다.
    con = connect(tmp_path / "t.db")
    _seed_disclosure(con, disclosure_id="dart:old", ticker="005930",
                      published_at="2026-08-01T08:00:00")
    api = FakeBatchAPI()

    stats = _collect(con, api)
    assert api.batches == {}
    assert stats["target"] == 0
    assert con.execute("SELECT count(*) AS n FROM news_judgments").fetchone()["n"] == 0


def test_claude_batch_api_never_retries_create(monkeypatch):
    # SDK 기본 max_retries=2 는 5xx 뒤 create 를 다시 보낸다 — 배치가 이미 생겼으면 두 번 과금.
    import sys
    import types

    from collectors import news_judge

    seen = {}

    class _Batches:
        def __init__(self, retries):
            self.retries = retries

        def create(self, requests):
            seen["retries"] = self.retries
            seen["n"] = len(requests)
            return types.SimpleNamespace(id="msgbatch_x")

    class _Client:
        def __init__(self, api_key=None, retries=2):
            self.messages = types.SimpleNamespace(batches=_Batches(retries))

        def with_options(self, max_retries):
            return _Client(retries=max_retries)

    monkeypatch.setitem(sys.modules, "anthropic", types.SimpleNamespace(Anthropic=_Client))
    api = news_judge.ClaudeBatchAPI("k")
    assert api.submit([build_request("r0", "x", "claude-haiku-5-5")]) == "msgbatch_x"
    assert seen == {"retries": 0, "n": 1}


def test_main_requires_anthropic_api_key(monkeypatch):
    import pytest
    from collectors import news_judge

    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.setattr("sys.argv", ["news_judge", "--db", ":memory:"])
    with pytest.raises(SystemExit, match="ANTHROPIC_API_KEY"):
        news_judge.main()
