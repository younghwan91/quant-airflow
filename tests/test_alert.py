"""collectors/alert.py — 유일한 알림 출구. 규약은 spec 2026-10-05-alert-channel-design.md."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from collectors import alert
from collectors.alert import (
    BODY_LIMIT,
    EXC_LIMIT,
    format_message,
    format_task_failure,
    notify,
)

REPO = Path(__file__).resolve().parents[1]


@pytest.fixture
def alert_log(tmp_path, monkeypatch):
    """로컬 로그를 tmp 로 돌리고 URL 은 비운다 — 기본 상태는 '웹훅 없음'."""
    path = tmp_path / "alerts.log"
    monkeypatch.setenv("ALERT_LOG", str(path))
    monkeypatch.delenv("ALERT_WEBHOOK_URL", raising=False)
    return path


def test_format_message_has_host_emoji_and_fenced_body():
    msg = format_message("warn", "제목", "본문 한 줄", host="simnode")
    assert msg.startswith("[simnode] ⚠️ 제목\n")
    assert "```\n본문 한 줄\n```" in msg


def test_format_message_without_body_is_title_only():
    assert format_message("error", "제목", "", host="h") == "[h] 🔴 제목"
    assert format_message("info", "제목", "   \n", host="h") == "[h] ℹ️ 제목"


def test_format_message_truncates_then_fences_and_stays_under_discord_limit():
    body = "x" * (BODY_LIMIT + 500)
    msg = format_message("warn", "t", body, host="h")
    assert "…(잘림, 전체는 로그)" in msg
    assert msg.rstrip().endswith("```")  # 자른 뒤 감싸므로 블록이 닫혀 있다
    assert len(msg) <= 2000


def test_format_message_long_title_still_closes_fence_under_limit():
    """제목이 길어도 마지막에 통째로 자르지 않는다 — 예산을 제목 길이에서 뺀다."""
    msg = format_message("warn", "T" * 300, "b" * 5000, host="h")
    assert len(msg) <= 2000
    assert msg.rstrip().endswith("```")
    assert "T" * 200 + "…" in msg


def test_format_message_prefers_alert_host_env(monkeypatch):
    monkeypatch.setenv("ALERT_HOST", "simnode")
    assert format_message("info", "t", host=None).startswith("[simnode] ")


def test_format_message_masks_secrets_in_title_and_body():
    msg = format_message(
        "error", "postgresql://u:pw@h/db 실패",
        "cmd --db postgresql://kr_quant:secret@10.0.0.1:5433/kr_quant api_key=abc", host="h")
    assert "pw" not in msg and "secret" not in msg and "abc" not in msg
    assert "u:***@" in msg and "api_key=***" in msg


def test_format_task_failure_fields_and_exception_cap():
    title, body = format_task_failure(
        dag_id="daily_news", task_id="collect_dart_disclosures", run_id="scheduled__x",
        try_number=2, exc_text="E" * (EXC_LIMIT + 50), log_url="http://sim:8080/log")
    assert title == "Airflow daily_news.collect_dart_disclosures 최종 실패 (try 2)"
    assert "run_id: scheduled__x" in body
    assert "log: http://sim:8080/log" in body
    assert "E" * EXC_LIMIT + "…" in body and "E" * (EXC_LIMIT + 1) not in body


def test_notify_without_url_logs_locally_and_returns_false(alert_log, capsys):
    with patch("urllib.request.urlopen") as urlopen:
        ok = notify("warn", "제목", "본문")
    assert ok is False
    urlopen.assert_not_called()
    text = alert_log.read_text(encoding="utf-8")
    assert "warn 제목" in text and "본문" in text
    assert "[alert.py] logged-only warn: 제목" in capsys.readouterr().out


def test_notify_with_url_posts_discord_payload(alert_log, monkeypatch, capsys):
    monkeypatch.setenv("ALERT_WEBHOOK_URL", "https://discord.test/hook")
    ctx = MagicMock()
    ctx.__enter__.return_value = MagicMock()
    with patch("urllib.request.urlopen", return_value=ctx) as urlopen:
        ok = notify("error", "제목", "본문")
    assert ok is True
    req = urlopen.call_args[0][0]
    assert req.full_url == "https://discord.test/hook"
    assert req.get_header("Content-type") == "application/json"
    payload = json.loads(req.data.decode("utf-8"))
    assert set(payload) == {"content"} and len(payload["content"]) <= 2000
    assert "🔴 제목" in payload["content"]
    assert urlopen.call_args[1]["timeout"] == 10
    assert "[alert.py] sent error: 제목" in capsys.readouterr().out
    assert "error 제목" in alert_log.read_text(encoding="utf-8")  # URL 있어도 로컬 로그


def test_notify_logs_full_body_even_when_discord_copy_is_truncated(alert_log):
    body = "\n".join(f"line{i:04d}" for i in range(500))  # ~4,500자
    notify("warn", "긴 본문", body)
    text = alert_log.read_text(encoding="utf-8")
    assert "line0499" in text  # Discord 쪽은 1,800자에서 잘려도 로그는 전부
    assert "```" not in text  # 로그엔 펜스 같은 전송 포맷을 남기지 않는다


def test_notify_swallows_transport_errors(alert_log, monkeypatch, capsys):
    monkeypatch.setenv("ALERT_WEBHOOK_URL", "https://discord.test/hook")
    with patch("urllib.request.urlopen", side_effect=OSError("boom")):
        ok = notify("warn", "제목", "본문")
    assert ok is False
    assert "[alert.py] failed warn: 제목" in capsys.readouterr().out


def test_notify_swallows_unwritable_log(tmp_path, monkeypatch, capsys):
    (tmp_path / "file-not-dir").write_text("x")  # 디렉터리 자리에 파일 → mkdir 실패
    monkeypatch.setenv("ALERT_LOG", str(tmp_path / "file-not-dir" / "alerts.log"))
    monkeypatch.delenv("ALERT_WEBHOOK_URL", raising=False)
    assert notify("info", "t", "b") is False  # 예외 없이 끝난다
    assert "[alert.py] logged-only" in capsys.readouterr().out


def test_cli_exits_zero_and_reads_body_from_stdin(alert_log):
    r = subprocess.run(
        [sys.executable, str(REPO / "collectors" / "alert.py"), "warn", "CLI 제목"],
        input="stdin 본문\n", capture_output=True, text=True,
        env={"ALERT_LOG": str(alert_log), "PATH": "/usr/bin:/bin"},
    )
    assert r.returncode == 0, r.stderr
    assert "[alert.py] logged-only warn: CLI 제목" in r.stdout
    assert "stdin 본문" in alert_log.read_text(encoding="utf-8")


def test_cli_survives_non_utf8_stdin(alert_log):
    """크론 꼬리(rclone·pg_dump 출력)에 비 UTF-8 바이트가 섞여도 죽지 않는다."""
    r = subprocess.run(
        [sys.executable, str(REPO / "collectors" / "alert.py"), "error", "깨진 바이트"],
        input=b"ok line\n\xff\xfe broken \xe2\x28\xa1\n", capture_output=True,
        env={"ALERT_LOG": str(alert_log), "PATH": "/usr/bin:/bin", "LANG": "en_US.UTF-8"},
    )
    assert r.returncode == 0, r.stderr
    assert b"[alert.py] logged-only error" in r.stdout
    assert "ok line" in alert_log.read_text(encoding="utf-8")


def test_cli_with_no_args_still_exits_zero():
    r = subprocess.run([sys.executable, str(REPO / "collectors" / "alert.py")],
                       capture_output=True, text=True)
    assert r.returncode == 0
    assert "usage" in r.stderr


def test_module_has_no_third_party_imports():
    """호스트 python3(크론)에서도 돌아야 한다 — requests 등이 들어오면 거기서 깨진다."""
    src = (REPO / "collectors" / "alert.py").read_text(encoding="utf-8")
    for bad in ("import requests", "from requests", "import httpx", "import airflow", "from airflow"):
        assert bad not in src
    assert alert.__doc__  # 모듈 docstring 에 왜 stdlib 인지 적혀 있다
