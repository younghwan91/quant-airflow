from collectors.news_judge import EVENT_TYPES, Judgment, build_prompt, collect, judge_item, parse_judgment
from collectors.storage import connect, upsert_disclosures, upsert_news_judgments


def _seed_disclosure(con, disclosure_id="dart:x", ticker="005930"):
    upsert_disclosures(con, [(
        disclosure_id, "dart", "제목", "https://dart.fss.or.kr/x",
        "삼성전자", ticker, "주요사항보고서",
        "2026-09-06T08:00:00", "2026-09-06T08:45:00",
    )])


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


def test_parse_judgment_rejects_boolean_sentiment():
    response = '{"event_type": "기타", "sentiment_direction": true, ' \
               '"related_codes": [], "is_stale_repeat": false, ' \
               '"first_seen_date": null, "price_impact_likely": false, "rationale": ""}'
    assert parse_judgment(response) is None


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


def test_main_requires_gemini_api_key(monkeypatch):
    import pytest
    from collectors import news_judge

    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.setattr("sys.argv", ["news_judge", "--db", ":memory:"])
    with pytest.raises(SystemExit, match="GEMINI_API_KEY"):
        news_judge.main()
