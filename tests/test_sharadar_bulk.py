"""collectors/sharadar_bulk.py — 벤더 벌크 다운로드(① RAW 계층).

여기서 지키는 건 세 가지다:

1. **안 바뀐 건 다시 받지 않는다** — 벤더 대역폭이 약 4.4MB/s 라 전량이 17분이다.
   `modified` 비교가 유일한 낭비 차단 장치다.
2. **받다 죽어도 반쪽 파일을 남기지 않는다** — 반쪽 zip 을 다음 실행이 "있다"고
   판단하면 그 테이블은 영구히 낡은 채로 남는다.
3. **키가 로그에 안 나온다** — 재발급 불가 상태이고 레포가 공개다.
"""

from __future__ import annotations

import json

import pytest

from collectors.sharadar_bulk import (
    DEFAULT_STALE_AFTER,
    ORPHAN_PART_MAX_AGE,
    SOCKET_TIMEOUT,
    _display_width,
    SUBSCRIBED_TABLES,
    CorruptDownload,
    bulk_url,
    download,
    file_sha256,
    is_stale,
    needs_download,
    plan_sync,
    read_manifest,
    record_check,
    render_report,
    stale_threshold,
    sweep_orphan_parts,
    sync,
    verify_zip,
    write_manifest,
)


# --------------------------------------------------------------- modified 비교


def test_downloads_when_nothing_local_yet(tmp_path):
    assert needs_download(tmp_path / "sep.csv.zip", "2026-08-15T03:56:19Z", manifest={})


def test_skips_when_vendor_timestamp_unchanged(tmp_path):
    path = tmp_path / "sep.csv.zip"
    path.write_bytes(b"PK\x03\x04zip")
    manifest = {"sep.csv.zip": {"modified": "2026-08-15T03:56:19Z", "size": path.stat().st_size}}

    assert not needs_download(path, "2026-08-15T03:56:19Z", manifest=manifest)


def test_downloads_when_vendor_publishes_a_newer_drop(tmp_path):
    path = tmp_path / "sep.csv.zip"
    path.write_bytes(b"PK\x03\x04zip")
    manifest = {"sep.csv.zip": {"modified": "2026-08-15T03:56:19Z", "size": path.stat().st_size}}

    assert needs_download(path, "2026-08-16T03:51:02Z", manifest=manifest)


def test_redownloads_when_file_vanished_even_if_manifest_says_current(tmp_path):
    """매니페스트만 믿으면, 파일이 지워진 걸 모르고 영원히 스킵한다."""
    manifest = {"sep.csv.zip": {"modified": "2026-08-15T03:56:19Z", "size": 5}}

    assert needs_download(tmp_path / "sep.csv.zip", "2026-08-15T03:56:19Z", manifest=manifest)


def test_redownloads_when_size_disagrees_with_manifest(tmp_path):
    """반쪽 파일 탐지 — 중단된 다운로드가 '최신'으로 남는 걸 막는다."""
    path = tmp_path / "sep.csv.zip"
    path.write_bytes(b"PK\x03")  # 3바이트, 매니페스트는 5바이트라고 주장
    manifest = {"sep.csv.zip": {"modified": "2026-08-15T03:56:19Z", "size": 5}}

    assert needs_download(path, "2026-08-15T03:56:19Z", manifest=manifest)


# --------------------------------------------------------------- 구독 목록 대조


def test_every_paid_dataset_is_checked_every_run():
    """구독분 14개가 전부 매일 대조 대상이다.

    주기를 나눠두면 weekly/monthly 를 부르는 DAG 이 없을 때 그 테이블은
    영원히 안 받아진다 — 실제로 holdings·holdings_investor·descriptions 가
    그 상태였다. 요구사항은 '항상 동기화' 이므로 주기 개념 자체가 위반이다.
    """
    paid = {
        "stocks", "daily", "fundamentals", "actions", "sp500", "tickers",
        "insiders", "holdings_ticker", "funds", "events", "metrics",
        "holdings", "holdings_investor", "descriptions",
    }

    assert set(SUBSCRIBED_TABLES) == paid
    assert len(SUBSCRIBED_TABLES) == len(set(SUBSCRIBED_TABLES))


def test_plan_covers_everything_the_vendor_offers():
    listing = {t: "2026-08-16T03:00:00Z" for t in SUBSCRIBED_TABLES}

    plan, missing = plan_sync(listing)

    assert set(plan) == set(SUBSCRIBED_TABLES)
    assert missing == ()


def test_plan_reports_tables_the_vendor_did_not_list():
    """조용히 빠지면 '변경 없어 건너뜀' 집계에도 안 잡혀 영원히 안 보인다."""
    listing = {"stocks": "2026-08-16T03:00:00Z"}

    plan, missing = plan_sync(listing)

    assert list(plan) == ["stocks"]
    assert "holdings" in missing
    assert "descriptions" in missing
    assert len(missing) == len(SUBSCRIBED_TABLES) - 1


def test_plan_ignores_tables_we_do_not_subscribe_to():
    """벤더가 새 테이블을 열어도 구독 목록에 없으면 받지 않는다."""
    listing = {"stocks": "2026-08-16T03:00:00Z", "somethingnew": "2026-08-16T03:00:00Z"}

    plan, _ = plan_sync(listing)

    assert "somethingnew" not in plan


# --------------------------------------------------------------- URL / 시크릿


def test_url_carries_the_bulk_flag():
    """bulk=true 가 빠지면 조용히 limit 10,000 짜리 JSON 이 온다 — 절단이다."""
    url = bulk_url("sep", api_key="SECRET")

    assert "bulk=true" in url
    assert "format=csv" in url
    assert "/data/sep?" in url


def test_repr_of_url_never_shows_the_key():
    """로그에 URL 을 찍는 순간을 대비한다 — 키는 재발급이 불가능하다."""
    from collectors.config import mask_secrets

    assert "SECRET" not in mask_secrets(bulk_url("sep", api_key="SECRET"))


def test_api_key_never_reaches_the_task_log():
    """requests 의 HTTPError 는 실패한 URL 을 통째로 담는다 — 키가 쿼리에 있다.

    2026-08-15 실행에서 실제로 400 하나에 키가 평문으로 로그에 남았다. 키는
    재발급이 불가능하고 이 레포는 공개다 — 스트리밍 길목에서 반드시 걸러야 한다.
    """
    from collectors.config import mask_secrets

    err = (
        "400 Client Error for url: "
        "https://api.sharadar.com/v1.0/data/sep?api_key=abc123SECRET&format=json&ticker=AAPL"
    )

    masked = mask_secrets(err)

    assert "abc123SECRET" not in masked
    assert "api_key=***" in masked
    assert "ticker=AAPL" in masked  # 진단에 필요한 건 남아야 한다


def test_masking_still_covers_dsn_passwords():
    """기존 DSN 마스킹을 깨지 않았는지 — 콜렉터 전체가 이 경로를 공유한다."""
    from collectors.config import mask_secrets

    fake = "💾 postgresql://airflow:hunter2@timescaledb:5432/quant"  # allowlist-secret
    masked = mask_secrets(fake)

    assert "hunter2" not in masked
    assert "timescaledb:5432/quant" in masked


# --------------------------------------------------------------- 매니페스트


def test_manifest_roundtrip(tmp_path):
    entries = {"sep.csv.zip": {"modified": "2026-08-15T03:56:19Z", "size": 123}}
    write_manifest(tmp_path, entries)

    assert read_manifest(tmp_path) == entries


def test_missing_manifest_reads_as_empty_not_an_error(tmp_path):
    """첫 실행에는 매니페스트가 없다 — 그게 정상이다."""
    assert read_manifest(tmp_path) == {}


def test_corrupt_manifest_reads_as_empty(tmp_path):
    """깨진 매니페스트 때문에 전체가 멈추면 안 된다 — 다시 받으면 그만이다."""
    (tmp_path / "manifest.json").write_text("{ this is not json")

    assert read_manifest(tmp_path) == {}


def test_manifest_is_written_atomically(tmp_path):
    """빌드 중 죽어도 반쪽 매니페스트가 남으면 안 된다."""
    write_manifest(tmp_path, {"a.csv.zip": {"modified": "x", "size": 1}})
    write_manifest(tmp_path, {"b.csv.zip": {"modified": "y", "size": 2}})

    assert list(read_manifest(tmp_path)) == ["b.csv.zip"]
    assert not list(tmp_path.glob("*.tmp")), "임시 파일이 남았다"


@pytest.mark.parametrize("table", ["stocks", "holdings", "descriptions"])
def test_manifest_json_is_human_readable(tmp_path, table):
    """운영 중에 사람이 읽고 판단하는 파일이다 — 한 줄로 뭉치면 안 된다."""
    write_manifest(tmp_path, {f"{table}.csv.zip": {"modified": "x", "size": 1}})

    text = (tmp_path / "manifest.json").read_text()

    assert "\n" in text
    assert json.loads(text)


# ------------------------------------------------------------ 확인 시각 · 정체


def test_first_check_starts_the_streak_at_zero():
    entry = record_check(None, "2026-08-16T03:56:19Z", now="2026-08-16T17:30:00Z")

    assert entry["vendor_modified"] == "2026-08-16T03:56:19Z"
    assert entry["checked_at"] == "2026-08-16T17:30:00Z"
    assert entry["unchanged_streak"] == 0


def test_streak_grows_while_the_vendor_timestamp_stands_still():
    entry = record_check(None, "2026-08-16T03:56:19Z", now="2026-08-16T17:30:00Z")
    entry = record_check(entry, "2026-08-16T03:56:19Z", now="2026-08-17T17:30:00Z")
    entry = record_check(entry, "2026-08-16T03:56:19Z", now="2026-08-18T17:30:00Z")

    assert entry["unchanged_streak"] == 2
    assert entry["checked_at"] == "2026-08-18T17:30:00Z"


def test_streak_resets_when_the_vendor_publishes():
    entry = {"vendor_modified": "2026-08-16T03:56:19Z", "unchanged_streak": 5}

    entry = record_check(entry, "2026-08-17T03:51:02Z", now="2026-08-17T17:30:00Z")

    assert entry["unchanged_streak"] == 0


def test_checked_at_advances_even_when_nothing_was_downloaded():
    """전송이 없어도 '오늘 확인했다' 는 남아야 한다 — 이게 동기화의 증거다."""
    entry = {"modified": "2026-08-16T03:56:19Z", "size": 953, "sha256": "abc"}

    updated = record_check(entry, "2026-08-16T03:56:19Z", now="2026-08-17T17:30:00Z")

    assert updated["checked_at"] == "2026-08-17T17:30:00Z"
    assert updated["modified"] == "2026-08-16T03:56:19Z"  # 로컬 파일 정보는 그대로
    assert updated["size"] == 953
    assert updated["sha256"] == "abc"


def test_record_check_does_not_mutate_the_input():
    """매니페스트를 제자리에서 고치면 실패 시 되돌릴 게 없다."""
    entry = {"vendor_modified": "2026-08-16T03:56:19Z", "unchanged_streak": 1}

    record_check(entry, "2026-08-16T03:56:19Z", now="2026-08-17T17:30:00Z")

    assert entry["unchanged_streak"] == 1


def test_local_modified_is_never_overwritten_by_a_vendor_sighting():
    """`modified` 는 로컬 파일의 값이다. 목록에서 본 값으로 덮으면 그 파일을
    영원히 안 받는다 — needs_download 가 `modified` 로 판단하기 때문이다."""
    entry = {"modified": "2026-08-16T03:56:19Z", "size": 953}

    updated = record_check(entry, "2026-08-17T03:51:02Z", now="2026-08-17T17:30:00Z")

    assert updated["modified"] == "2026-08-16T03:56:19Z"
    assert updated["vendor_modified"] == "2026-08-17T03:51:02Z"


# ------------------------------------------------------------------- 정체 판정


def test_daily_tables_are_stale_after_two_idle_checks():
    assert not is_stale("stocks", {"unchanged_streak": 1})
    assert is_stale("stocks", {"unchanged_streak": 2})


def test_quarterly_tables_tolerate_long_silence():
    """13F 원자료는 분기 공시다 — 8회 정체는 정상이다."""
    assert not is_stale("holdings", {"unchanged_streak": 7})
    assert is_stale("holdings", {"unchanged_streak": 8})


def test_the_static_field_dictionary_is_allowed_to_never_change():
    """descriptions 는 2026-07-31 이후 안 바뀌었다 — 정상이다."""
    assert not is_stale("descriptions", {"unchanged_streak": 29})


def test_an_unknown_table_falls_back_to_the_daily_threshold():
    assert stale_threshold("something_new") == DEFAULT_STALE_AFTER


def test_a_never_checked_entry_is_not_stale():
    """첫 실행에는 정체가 있을 수 없다."""
    assert not is_stale("stocks", {})


def test_every_subscribed_table_has_a_threshold():
    """임계값을 안 정한 테이블이 조용히 기본값으로 새면 안 된다."""
    for table in SUBSCRIBED_TABLES:
        assert stale_threshold(table) > 0


# ------------------------------------------------------------ 체크섬 · zip 검사


def _real_zip(path):
    """유효한 zip 을 만든다 — 테스트가 진짜 zip 구조를 통과하는지 봐야 한다."""
    import zipfile

    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr("stocks.csv", "ticker,date,close\nAAPL,2026-08-16,100\n")
    return path


def test_sha256_is_stable_and_lowercase_hex(tmp_path):
    path = tmp_path / "a.bin"
    path.write_bytes(b"sharadar")

    digest = file_sha256(path)

    assert digest == file_sha256(path)
    assert len(digest) == 64
    assert digest == digest.lower()


def test_sha256_differs_for_different_content(tmp_path):
    a, b = tmp_path / "a.bin", tmp_path / "b.bin"
    a.write_bytes(b"sharadar")
    b.write_bytes(b"sharadar ")

    assert file_sha256(a) != file_sha256(b)


def test_a_valid_zip_passes_verification(tmp_path):
    verify_zip(_real_zip(tmp_path / "ok.csv.zip"))  # 예외 없으면 통과


def test_a_truncated_zip_is_rejected(tmp_path):
    """벤더 대역폭이 느려 17분짜리 전송이 끊기는 일이 실제로 있었다."""
    path = _real_zip(tmp_path / "cut.csv.zip")
    data = path.read_bytes()
    path.write_bytes(data[: len(data) // 2])

    with pytest.raises(CorruptDownload):
        verify_zip(path)


def test_a_non_zip_payload_is_rejected(tmp_path):
    """벤더가 에러 JSON 을 200 으로 돌려주는 경우 — zip 이 아니다."""
    path = tmp_path / "err.csv.zip"
    path.write_bytes(b'{"error":"rate limited"}')

    with pytest.raises(CorruptDownload):
        verify_zip(path)


def test_redownloads_when_the_checksum_disagrees(tmp_path):
    """크기가 같아도 내용이 상하면 다시 받아야 한다."""
    path = tmp_path / "stocks.csv.zip"
    path.write_bytes(b"PK\x03\x04AAAA")
    manifest = {
        "stocks.csv.zip": {
            "modified": "2026-08-16T03:56:19Z",
            "size": path.stat().st_size,
            "sha256": "0" * 64,
        }
    }

    assert needs_download(path, "2026-08-16T03:56:19Z", manifest=manifest)


def test_matching_checksum_still_skips(tmp_path):
    path = tmp_path / "stocks.csv.zip"
    path.write_bytes(b"PK\x03\x04AAAA")
    manifest = {
        "stocks.csv.zip": {
            "modified": "2026-08-16T03:56:19Z",
            "size": path.stat().st_size,
            "sha256": file_sha256(path),
        }
    }

    assert not needs_download(path, "2026-08-16T03:56:19Z", manifest=manifest)


def test_a_legacy_entry_without_a_checksum_does_not_trigger_a_redownload(tmp_path):
    """기존 매니페스트에는 sha256 이 없다. 그걸로 재다운로드가 걸리면
    첫 실행에 4.6GB 를 전량 다시 받는다 — 그럴 이유가 없다."""
    path = tmp_path / "stocks.csv.zip"
    path.write_bytes(b"PK\x03\x04AAAA")
    manifest = {
        "stocks.csv.zip": {"modified": "2026-08-16T03:56:19Z", "size": path.stat().st_size}
    }

    assert not needs_download(path, "2026-08-16T03:56:19Z", manifest=manifest)


def test_download_returns_the_checksum_of_what_it_wrote(tmp_path):
    import io

    payload = _real_zip(tmp_path / "src.zip").read_bytes()

    def fake_opener(url, timeout=None):
        return io.BytesIO(payload)

    dest = tmp_path / "out" / "stocks.csv.zip"
    size, digest = download("stocks", dest, api_key="K", opener=fake_opener)

    assert size == len(payload)
    assert digest == file_sha256(dest)


def test_download_refuses_to_place_a_corrupt_payload(tmp_path):
    """반쪽 zip 이 목적지에 남으면 다음 실행이 그걸 정상으로 본다."""
    import io

    def fake_opener(url, timeout=None):
        return io.BytesIO(b"not a zip at all")

    dest = tmp_path / "out" / "stocks.csv.zip"

    with pytest.raises(CorruptDownload):
        download("stocks", dest, api_key="K", opener=fake_opener)

    assert not dest.exists()
    assert not list((tmp_path / "out").glob("*.part"))


# ------------------------------------------------------------------- 상태 보고


def _manifest(**tables):
    return {f"{t}.csv.zip": entry for t, entry in tables.items()}


def test_report_lists_every_subscribed_table():
    """14개 전부가 보여야 한다 — 빠진 줄이 곧 안 보이는 구멍이다."""
    text = render_report({}, missing=(), fetched=set(), now="2026-08-16T17:34:00Z")

    for table in SUBSCRIBED_TABLES:
        assert table in text


def test_report_marks_what_was_downloaded():
    manifest = _manifest(
        stocks={"vendor_modified": "2026-08-16T03:56:19Z", "size": 953210472,
                "unchanged_streak": 0}
    )

    text = render_report(manifest, missing=(), fetched={"stocks"}, now="2026-08-16T17:34:00Z")

    assert "새로 받음" in text


def test_report_flags_a_stalled_table():
    manifest = _manifest(
        stocks={"vendor_modified": "2026-08-10T03:56:19Z", "size": 1, "unchanged_streak": 4}
    )

    text = render_report(manifest, missing=(), fetched=set(), now="2026-08-16T17:34:00Z")

    assert "⚠️" in text
    assert "4회" in text


def test_report_does_not_flag_a_quietly_static_table():
    """descriptions 는 안 바뀌는 게 정상이다 — 매일 경고가 뜨면 아무도 안 본다."""
    manifest = _manifest(
        descriptions={"vendor_modified": "2026-07-31T02:10:44Z", "size": 1,
                      "unchanged_streak": 12}
    )

    text = render_report(manifest, missing=(), fetched=set(), now="2026-08-16T17:34:00Z")

    line = next(ln for ln in text.splitlines() if ln.startswith("descriptions"))
    assert "⚠️" not in line


def test_report_shows_tables_the_vendor_did_not_list():
    text = render_report({}, missing=("metrics",), fetched=set(), now="2026-08-16T17:34:00Z")

    line = next(ln for ln in text.splitlines() if ln.startswith("metrics"))
    assert "목록에 없음" in line


def test_report_totals_add_up_to_the_subscription():
    """최신 + 주의 + 누락 = 14. 안 맞으면 어딘가 빠진 것이다."""
    manifest = _manifest(
        stocks={"vendor_modified": "2026-08-16T03:56:19Z", "size": 1, "unchanged_streak": 0},
        holdings={"vendor_modified": "2026-07-15T04:02:11Z", "size": 1, "unchanged_streak": 9},
    )

    text = render_report(manifest, missing=("metrics",), fetched={"stocks"},
                         now="2026-08-16T17:34:00Z")

    assert f"{len(SUBSCRIBED_TABLES)}개 중" in text
    assert "주의 1" in text
    assert "누락 1" in text


def test_report_never_leaks_the_api_key():
    """운영 중 사람이 읽고 로그에도 남는 출력이다."""
    manifest = _manifest(
        stocks={"vendor_modified": "2026-08-16T03:56:19Z", "size": 1, "unchanged_streak": 0}
    )

    text = render_report(manifest, missing=(), fetched=set(), now="2026-08-16T17:34:00Z")

    assert "api_key" not in text


# --------------------------------------------------------------- sync 통합


def _fake_vendor(tables):
    """벤더 목록 + 벌크 zip 을 흉내내는 opener.

    `opener` 는 반드시 인자로 넘긴다 — 기본값이 정의 시점에 바인딩되므로
    `monkeypatch.setattr("...urllib.request.urlopen", ...)` 로는 안 바뀐다.
    """
    import io
    import zipfile

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("data.csv", "ticker,date\nAAPL,2026-08-16\n")
    payload = buf.getvalue()

    def opener(url, timeout=None):
        if "/bulk?" in url:
            items = [{"table": t, "modified": m, "history": "full"} for t, m in tables.items()]
            return io.BytesIO(json.dumps({"items": items}).encode())
        return io.BytesIO(payload)

    return opener


def test_sync_records_a_check_for_tables_it_did_not_download(tmp_path):
    """전송이 없어도 checked_at 이 남아야 한다 — 이게 동기화의 증거다."""
    opener = _fake_vendor({t: "2026-08-16T03:00:00Z" for t in SUBSCRIBED_TABLES})

    sync(tmp_path, api_key="K", now="2026-08-16T17:30:00Z", opener=opener)
    manifest = sync(tmp_path, api_key="K", now="2026-08-17T17:30:00Z", opener=opener)

    entry = manifest["stocks.csv.zip"]
    assert entry["checked_at"] == "2026-08-17T17:30:00Z"
    assert entry["unchanged_streak"] == 1


def test_sync_does_not_retransmit_when_nothing_changed(tmp_path):
    opener = _fake_vendor({t: "2026-08-16T03:00:00Z" for t in SUBSCRIBED_TABLES})

    sync(tmp_path, api_key="K", now="2026-08-16T17:30:00Z", opener=opener)
    before = (tmp_path / "stocks.csv.zip").stat().st_mtime_ns

    sync(tmp_path, api_key="K", now="2026-08-17T17:30:00Z", opener=opener)

    assert (tmp_path / "stocks.csv.zip").stat().st_mtime_ns == before


def test_sync_fetches_all_fourteen_on_a_cold_start(tmp_path):
    """주기 분할 시절 3개가 영원히 안 받아졌다 — 그 회귀를 고정한다."""
    opener = _fake_vendor({t: "2026-08-16T03:00:00Z" for t in SUBSCRIBED_TABLES})

    sync(tmp_path, api_key="K", now="2026-08-16T17:30:00Z", opener=opener)

    for table in SUBSCRIBED_TABLES:
        assert (tmp_path / f"{table}.csv.zip").exists(), f"{table} 가 안 받아졌다"


def test_sync_reports_a_table_the_vendor_dropped(tmp_path, capsys):
    opener = _fake_vendor(
        {t: "2026-08-16T03:00:00Z" for t in SUBSCRIBED_TABLES if t != "metrics"}
    )

    sync(tmp_path, api_key="K", now="2026-08-16T17:30:00Z", opener=opener)

    out = capsys.readouterr().out
    assert "metrics" in out
    assert "목록에 없음" in out


def test_sync_prints_the_status_table(tmp_path, capsys):
    opener = _fake_vendor({t: "2026-08-16T03:00:00Z" for t in SUBSCRIBED_TABLES})

    sync(tmp_path, api_key="K", now="2026-08-16T17:30:00Z", opener=opener)

    out = capsys.readouterr().out
    assert "Sharadar 동기화 상태" in out
    assert f"{len(SUBSCRIBED_TABLES)}개 중" in out


def test_report_columns_line_up_with_the_header():
    """한글은 두 칸을 차지한다 — f"{s:<20}" 는 문자 수로 세어 열이 어긋난다."""
    manifest = _manifest(
        stocks={"vendor_modified": "2026-08-16T03:56:19Z", "size": 1, "unchanged_streak": 0}
    )

    text = render_report(manifest, missing=(), fetched=set(), now="2026-08-16T17:34:00Z")
    header, row = text.splitlines()[1], text.splitlines()[2]

    assert _display_width(header[: header.index("벤더 modified")]) == _display_width(
        row[: row.index("2026")]
    )


# ------------------------------------------------- 실패 격리 · 고아 정리 · 절단


def test_one_broken_table_does_not_silence_the_other_thirteen(tmp_path, capsys):
    """holdings 하나가 죽어도 나머지가 대조되고 상태 표가 남아야 한다.

    예전엔 루프에 격리가 없어, 에러 하나면 뒤따르는 테이블이 확인조차 안 되고
    표에도 도달을 못 했다 — 즉 **문제가 있는 날에만** 이 모듈의 산출물이
    사라졌다. 정확히 반대로 동작해야 한다.
    """
    import io
    import zipfile

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("data.csv", "ticker,date\nAAPL,2026-08-16\n")
    payload = buf.getvalue()

    def opener(url, timeout=None):
        if "/bulk?" in url:
            items = [
                {"table": t, "modified": "2026-08-16T03:00:00Z", "history": "full"}
                for t in SUBSCRIBED_TABLES
            ]
            return io.BytesIO(json.dumps({"items": items}).encode())
        if "/data/holdings?" in url:
            raise OSError("vendor said 503")
        return io.BytesIO(payload)

    with pytest.raises(RuntimeError, match="holdings"):
        sync(tmp_path, api_key="K", now="2026-08-16T17:30:00Z", opener=opener)

    out = capsys.readouterr().out
    assert "Sharadar 동기화 상태" in out, "실패해도 표는 남아야 한다"
    assert "❌ 실패" in out
    for table in SUBSCRIBED_TABLES:
        if table != "holdings":
            assert (tmp_path / f"{table}.csv.zip").exists(), f"{table} 가 격리에 막혔다"


def test_a_truncated_transfer_is_caught_by_content_length(tmp_path):
    """sha256 은 받은 바이트로 계산해 그 바이트를 기록하므로 절단을 못 잡는다."""
    import email.message
    import io

    class _Resp(io.BytesIO):
        def __init__(self, data, declared):
            super().__init__(data)
            self.headers = email.message.Message()
            self.headers["Content-Length"] = str(declared)

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    payload = _real_zip(tmp_path / "src.zip").read_bytes()

    def fake_opener(url, timeout=None):
        return _Resp(payload, len(payload) + 4096)  # 벤더는 더 길다고 했다

    dest = tmp_path / "out" / "stocks.csv.zip"

    with pytest.raises(OSError, match="잘렸다"):
        download("stocks", dest, api_key="K", opener=fake_opener)

    assert not dest.exists()
    assert not list((tmp_path / "out").glob(".*.part"))


def test_orphan_parts_older_than_the_cutoff_are_swept(tmp_path):
    """SIGKILL 로 죽은 런이 남긴 985MB 짜리를 아무도 안 치우고 있었다."""
    import os
    import time

    orphan = tmp_path / ".stocks-dead1234.part"
    orphan.write_bytes(b"x" * 1024)
    old = time.time() - ORPHAN_PART_MAX_AGE - 60
    os.utime(orphan, (old, old))

    assert sweep_orphan_parts(tmp_path) == [orphan.name]
    assert not orphan.exists()


def test_a_part_from_a_live_run_is_never_swept(tmp_path):
    """나이 조건이 안전장치의 전부다 — 도는 런의 것을 지우면 다운로드가 날아간다."""
    fresh = tmp_path / ".daily-live5678.part"
    fresh.write_bytes(b"x" * 1024)

    assert sweep_orphan_parts(tmp_path) == []
    assert fresh.exists()


def test_socket_timeout_is_short_enough_to_fail_fast():
    """1800 이면 벤더가 멎었을 때 30분 매달렸다 죽고 재시도 10분이 더 붙는다."""
    assert SOCKET_TIMEOUT <= 300
