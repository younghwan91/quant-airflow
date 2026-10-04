"""scripts/cron_run.sh · scripts/alert.sh — 크론 줄을 감싸는 래퍼.

bash 로 직접 실행한다 (CI 러너·simnode·trader 전부 bash 가 있다). `.env` 가 없는
워크트리에서도 돌아야 하므로 env 로 ALERT_LOG 를 넣고 URL 은 비운다.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
CRON_RUN = REPO / "scripts" / "cron_run.sh"
ALERT_SH = REPO / "scripts" / "alert.sh"


@pytest.fixture
def env(tmp_path):
    e = {k: v for k, v in os.environ.items() if k in ("PATH", "HOME", "LANG", "LC_ALL")}
    e["ALERT_LOG"] = str(tmp_path / "alerts.log")
    e["ALERT_WEBHOOK_URL"] = ""
    return e


def run(args, env, stdin=None):
    return subprocess.run(["bash", *args], env=env, input=stdin, capture_output=True, text=True)


def test_alert_sh_exits_zero_and_logs(env, tmp_path):
    r = run([str(ALERT_SH), "warn", "래퍼 제목"], env, stdin="본문\n")
    assert r.returncode == 0, r.stderr
    assert "[alert.py] logged-only warn: 래퍼 제목" in r.stdout
    assert "본문" in (tmp_path / "alerts.log").read_text(encoding="utf-8")


def test_cron_run_success_passes_output_through_and_sends_nothing(env, tmp_path):
    r = run([str(CRON_RUN), "demo", "--", "bash", "-c", "echo hello; echo err >&2"], env)
    assert r.returncode == 0
    assert "hello" in r.stdout and "err" in r.stdout  # 2>&1 로 합쳐 흘린다
    assert not (tmp_path / "alerts.log").exists()


def test_cron_run_failure_keeps_exit_code_and_alerts_with_tail(env, tmp_path):
    cmd = "for i in $(seq 1 40); do echo line$i; done; exit 3"
    r = run([str(CRON_RUN), "demo", "--", "bash", "-c", cmd], env)
    assert r.returncode == 3  # 래퍼는 종료코드를 바꾸지 않는다
    assert "line40" in r.stdout
    log = (tmp_path / "alerts.log").read_text(encoding="utf-8")
    assert "error demo 실패 (rc=3)" in log
    assert "line40" in log and "line11" in log and "line10\n" not in log  # 꼬리 30줄


def test_cron_run_skips_alert_when_command_already_alerted(env, tmp_path):
    cmd = "echo working; echo '[alert.py] logged-only warn: 안에서 보냈다'; exit 1"
    r = run([str(CRON_RUN), "demo", "--", "bash", "-c", cmd], env)
    assert r.returncode == 1
    assert "중복 알림 생략" in r.stdout
    assert not (tmp_path / "alerts.log").exists()


def test_cron_run_still_alerts_when_inner_alert_failed_to_deliver(env, tmp_path):
    """`failed` 마커는 전달 안 된 것 — 바깥이 다시 보낸다."""
    cmd = "echo '[alert.py] failed error: 백업 실패 — HTTPError: 429'; exit 1"
    r = run([str(CRON_RUN), "demo", "--", "bash", "-c", cmd], env)
    assert r.returncode == 1
    assert "중복 알림 생략" not in r.stdout
    assert "error demo 실패 (rc=1)" in (tmp_path / "alerts.log").read_text(encoding="utf-8")


def test_alert_sh_respects_explicitly_empty_url_over_dotenv(tmp_path):
    """빈 ALERT_WEBHOOK_URL 은 '보내지 마' 다 — .env 의 실제 URL 로 덮어쓰면 안 된다."""
    # alert.sh 는 자기 위치에서 REPO 를 잡으므로, .env 가 있는 가짜 레포를 만든다.
    fake = tmp_path / "repo"
    (fake / "scripts").mkdir(parents=True)
    (fake / "collectors").mkdir()
    for name in ("alert.py", "config.py", "__init__.py"):
        (fake / "collectors" / name).write_bytes((REPO / "collectors" / name).read_bytes())
    (fake / "scripts" / "alert.sh").write_bytes(ALERT_SH.read_bytes())
    (fake / ".env").write_text('ALERT_WEBHOOK_URL="https://discord.test/real"\n', encoding="utf-8")
    e = {"PATH": os.environ["PATH"], "ALERT_LOG": str(tmp_path / "a.log"), "ALERT_WEBHOOK_URL": ""}
    r = subprocess.run(["bash", str(fake / "scripts" / "alert.sh"), "info", "t"],
                       env=e, input="", capture_output=True, text=True)
    assert r.returncode == 0
    assert "[alert.py] logged-only info: t" in r.stdout  # .env 의 URL 로 안 보냈다


def test_cron_run_without_command_is_usage_error(env):
    r = run([str(CRON_RUN), "demo", "--"], env)
    assert r.returncode == 2
