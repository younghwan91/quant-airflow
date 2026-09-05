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
