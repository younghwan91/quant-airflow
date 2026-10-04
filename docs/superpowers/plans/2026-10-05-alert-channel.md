# 알림 통로 하나 — 구현 플랜

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Airflow 최종 실패·헬스체크 ⚠️·백업 비정상 종료·크론 래퍼 실패를 전송기 하나(`collectors/alert.py`)로 모아 Discord 웹훅으로 보내고, URL 이 없으면 로컬 로그에만 남긴다.

**Architecture:** stdlib 전용 파이썬 전송기 하나(`notify()` + CLI)를 `collectors/` 에 두고, bash 발생지는 얇은 래퍼 `scripts/alert.sh` 로, Airflow 는 `_common.py` 의 `on_failure_callback` 으로 그 전송기를 부른다. 모든 경로는 예외를 올리지 않고 종료코드를 바꾸지 않는다.

**Tech Stack:** Python 3.11 (stdlib: `urllib.request`, `json`, `socket`), bash, Airflow 2.10 `on_failure_callback`, Discord webhook (`POST {"content": ...}`), pytest, ruff 0.14.0.

**Spec:** `docs/superpowers/specs/2026-10-05-alert-channel-design.md`

## Global Constraints

- `collectors/alert.py` 는 **stdlib 만** import 한다 (`collectors.config.mask_secrets` 는 예외 — 그 모듈도 stdlib 만 쓴다, 호스트 python3 로 import 확인됨).
- `notify()`·`alert.sh`·콜백은 **절대 예외를 올리지 않고 종료코드 0** 이다. `cron_run.sh` 는 **원래 명령의 종료코드를 그대로** 돌려준다.
- `ALERT_WEBHOOK_URL` 이 비어 있으면 로컬 로그 + stdout 마커 `[alert.py] logged-only …` 만 남기고 통과한다.
- Discord `content` ≤ 2,000자. 본문 절단 한도 `BODY_LIMIT = 1800`, 예외 텍스트 `EXC_LIMIT = 300`.
- stdout 마커는 항상 `[alert.py] ` 로 시작한다 (`sent` | `logged-only` | `failed`). `cron_run.sh` 의 중복 억제가 이 접두를 본다.
- 재시도 횟수·지연·스케줄은 **하나도 바꾸지 않는다** (CLAUDE.md §1). 컨테이너 재생성·재기동은 **하지 않는다**.
- 커밋 메시지는 한국어, 마지막 줄 `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`. 커밋 이메일은 `chyohw97@gmail.com` (회사 메일 금지 — CI 가드).
- 테스트·린트 실행 명령 (워크트리 루트에서):
  - `uv run --quiet --with pytest --with-requirements docker/requirements.txt --python 3.11 python -m pytest tests/ -q`
  - `uv run --quiet --with 'ruff==0.14.0' --python 3.11 ruff check collectors/ dags/ tests/ scripts/`
  - 셸: `bash -n scripts/<파일>.sh`

---

### Task 1: `collectors/alert.py` — 전송기

**Files:**
- Create: `collectors/alert.py`
- Test: `tests/test_alert.py`

**Interfaces:**
- Consumes: `collectors.config.mask_secrets(text: str | None) -> str`
- Produces:
  - `format_message(level: str, title: str, body: str = "", *, host: str | None = None) -> str`
  - `format_task_failure(*, dag_id: str, task_id: str, run_id: str, try_number: int | str, exc_text: str, log_url: str) -> tuple[str, str]` (title, body)
  - `notify(level: str, title: str, body: str = "") -> bool`
  - `main(argv: list[str] | None = None) -> int` (항상 0)
  - 상수 `BODY_LIMIT = 1800`, `EXC_LIMIT = 300`

- [ ] **Step 1: 실패하는 테스트 작성**

`tests/test_alert.py`:

```python
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


def test_notify_swallows_transport_errors(alert_log, monkeypatch, capsys):
    monkeypatch.setenv("ALERT_WEBHOOK_URL", "https://discord.test/hook")
    with patch("urllib.request.urlopen", side_effect=OSError("boom")):
        ok = notify("warn", "제목", "본문")
    assert ok is False
    assert "[alert.py] failed warn: 제목" in capsys.readouterr().out


def test_notify_swallows_unwritable_log(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("ALERT_LOG", str(tmp_path / "file-not-dir" ))
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
```

- [ ] **Step 2: 실패 확인**

Run: `uv run --quiet --with pytest --with-requirements docker/requirements.txt --python 3.11 python -m pytest tests/test_alert.py -q`
Expected: `ModuleNotFoundError: No module named 'collectors.alert'` (collection error)

- [ ] **Step 3: 구현**

`collectors/alert.py`:

```python
"""운영 알림 전송기 — 이 레포에서 사람에게 뭔가를 보내는 **유일한** 출구.

왜 생겼나: 2026-10-04 까지 이 레포의 사고는 전부 로그 파일에만 남았다 — 리플리카
슬롯이 사흘 죽어 있던 것(09-19~21), 공시 수집이 재시도까지 실패한 것(09-29), 헬스체크
오보 3주. 발견 경로가 없었다. 설계는 docs/superpowers/specs/2026-10-05-alert-channel-design.md.

규약 — 발생지(크론·Airflow 콜백·헬스체크·백업)가 믿고 부를 수 있으려면:

- **절대 예외를 올리지 않는다.** 알림이 본작업을 죽이면 알림이 사고가 된다.
  :func:`notify` 는 어떤 실패도 삼키고 ``False`` 를 돌려준다. CLI 는 항상 0 으로 끝난다.
- **stdlib 만 쓴다.** 호스트 python3(크론)와 Airflow 컨테이너에서 같은 파일이 돈다.
  ``requests`` 를 쓰면 호스트에서 ImportError 로 죽는다. ``collectors.config`` 도
  stdlib 만 import 하므로 ``mask_secrets`` 는 가져다 쓴다.
- **항상 로컬 로그에도 쓴다** (``ALERT_LOG``, 기본 ``~/logs/quant-airflow/alerts.log``).
  URL 이 있든 없든. 이게 감사 추적이고, URL 이 없는 기간의 유일한 기록이다.
- **stdout 에 마커 한 줄** ``[alert.py] sent|logged-only|failed <level>: <title>`` 을
  찍는다. ``scripts/cron_run.sh`` 가 이 접두를 보고 "안에서 이미 알렸다" 를 판단해
  중복 알림을 막는다 — 바꾸면 그쪽도 같이 바꿔야 한다.
- 본문은 :func:`collectors.config.mask_secrets` 를 거친다. CalledProcessError 메시지나
  백업 로그 꼬리에 DSN 이 섞여 올 수 있고, 이 레포는 공개 레포다.

CLI:
    python3 collectors/alert.py <warn|error|info> <title> [body]   # body 없으면 stdin
"""

from __future__ import annotations

import json
import os
import socket
import sys
import urllib.request
from datetime import datetime
from pathlib import Path

try:
    from .config import mask_secrets
except ImportError:  # `python3 collectors/alert.py` 로 직접 실행 — 패키지 컨텍스트가 없다
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from collectors.config import mask_secrets

#: Discord ``content`` 는 2,000자. 제목 줄·코드 펜스 여유를 빼고 본문은 여기서 자른다.
BODY_LIMIT = 1800
#: Airflow 예외 문자열 상한 — 트레이스백 전체는 태스크 로그에 있다.
EXC_LIMIT = 300
_DISCORD_LIMIT = 2000
_TIMEOUT_SEC = 10

_EMOJI = {"warn": "⚠️", "error": "🔴", "info": "ℹ️"}


def format_message(level: str, title: str, body: str = "", *, host: str | None = None) -> str:
    """``[host] emoji title`` 한 줄 + (본문이 있으면) 코드 블록. 순수 함수.

    두 노드가 한 채널을 쓰므로 host 가 첫 글자다. 본문은 **자른 뒤** 펜스로 감싼다 —
    감싼 뒤 자르면 닫는 ``\`\`\``` 이 날아가 다음 메시지까지 코드로 보인다.
    """
    host = host or socket.gethostname()
    head = f"[{host}] {_EMOJI.get(level, _EMOJI['info'])} {mask_secrets(title)}"
    text = mask_secrets(body or "").strip()
    if not text:
        return head
    if len(text) > BODY_LIMIT:
        text = text[:BODY_LIMIT] + "\n…(잘림, 전체는 로그)"
    return f"{head}\n```\n{text}\n```"[:_DISCORD_LIMIT]


def format_task_failure(
    *, dag_id: str, task_id: str, run_id: str, try_number: int | str,
    exc_text: str, log_url: str,
) -> tuple[str, str]:
    """Airflow 최종 실패 콜백용 (title, body). ``dags/_common.py`` 가 context 에서 꺼내 넘긴다.

    Airflow 없이 테스트하려고 여기 둔다 — ``_common`` 은 ``airflow.models`` 를 import 해
    CI(에어플로 미설치)에서 import 자체가 안 된다.
    """
    title = f"Airflow {dag_id}.{task_id} 최종 실패 (try {try_number})"
    exc = (exc_text or "").strip()
    if len(exc) > EXC_LIMIT:
        exc = exc[:EXC_LIMIT] + "…"
    body = f"run_id: {run_id}\nexception: {exc or '(없음)'}\nlog: {log_url or '(없음)'}"
    return title, body


def _log_path() -> Path:
    return Path(os.environ.get("ALERT_LOG") or Path.home() / "logs" / "quant-airflow" / "alerts.log")


def _append_log(level: str, title: str, message: str) -> None:
    """로컬 감사 로그 한 건 — 실패는 삼킨다(stdout 마커는 어차피 남는다)."""
    try:
        path = _log_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with path.open("a", encoding="utf-8") as f:
            f.write(f"[{stamp}] {level} {mask_secrets(title)}\n")
            for line in message.splitlines()[1:]:
                f.write(f"    {line}\n")
    except Exception:
        pass


def notify(level: str, title: str, body: str = "") -> bool:
    """알림 한 건. 웹훅 전송 성공이면 True — URL 없음·전송 실패·그 외 전부 False.

    예외는 **어떤 경우에도** 밖으로 나가지 않는다 (모듈 docstring).
    """
    try:
        message = format_message(level, title, body)
        _append_log(level, title, message)
        url = os.environ.get("ALERT_WEBHOOK_URL", "").strip()
        if not url:
            print(f"[alert.py] logged-only {level}: {mask_secrets(title)}", flush=True)
            return False
        req = urllib.request.Request(
            url,
            data=json.dumps({"content": message}).encode("utf-8"),
            headers={"Content-Type": "application/json", "User-Agent": "quant-airflow-alert/1"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=_TIMEOUT_SEC):
            pass
        print(f"[alert.py] sent {level}: {mask_secrets(title)}", flush=True)
        return True
    except Exception as e:  # noqa: BLE001 — 규약: 알림은 본작업을 죽이지 않는다
        try:
            print(f"[alert.py] failed {level}: {mask_secrets(title)} — {type(e).__name__}: {e}",
                  flush=True)
        except Exception:
            pass
        return False


def main(argv: list[str] | None = None) -> int:
    """CLI. 항상 0 — 크론 줄의 종료코드를 바꾸지 않는다."""
    args = sys.argv[1:] if argv is None else argv
    if len(args) < 2:
        print("usage: alert.py <warn|error|info> <title> [body]  (body 없으면 stdin)", file=sys.stderr)
        return 0
    level, title = args[0], args[1]
    if len(args) > 2:
        body = " ".join(args[2:])
    else:
        body = "" if sys.stdin.isatty() else sys.stdin.read()
    notify(level, title, body)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `uv run --quiet --with pytest --with-requirements docker/requirements.txt --python 3.11 python -m pytest tests/test_alert.py -q`
Expected: 12 passed

- [ ] **Step 5: 린트**

Run: `uv run --quiet --with 'ruff==0.14.0' --python 3.11 ruff check collectors/ tests/`
Expected: All checks passed!

- [ ] **Step 6: 커밋**

```bash
git add collectors/alert.py tests/test_alert.py
git commit -F - <<'EOF'
feat(alert): 운영 알림 전송기 collectors/alert.py — 유일한 출구

Discord 웹훅 POST 하나. URL(ALERT_WEBHOOK_URL)이 없으면 로컬 로그
(ALERT_LOG)에만 쓰고 stdout 에 `[alert.py] logged-only` 마커를 남긴다 —
URL 은 아직 없고, 코드는 지금 배포돼야 하므로 그게 기본 상태다.

절대 예외를 올리지 않고 stdlib 만 쓴다: 크론(호스트 python3)·Airflow
컨테이너 양쪽에서 같은 파일이 돌아야 하고, 알림이 본작업을 죽이면 알림이
사고가 된다. 본문은 mask_secrets 를 거친다 — 공개 레포고 채널은 더 넓다.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
EOF
```

---

### Task 2: `scripts/alert.sh` + `scripts/cron_run.sh` — bash 래퍼

**Files:**
- Create: `scripts/alert.sh`
- Create: `scripts/cron_run.sh`
- Test: `tests/test_cron_run.py`

**Interfaces:**
- Consumes: `collectors/alert.py` CLI (`python3 collectors/alert.py <level> <title> [body]`, stdin 본문, 마커 `[alert.py] `)
- Produces:
  - `scripts/alert.sh <level> <title> [body]` — stdin 본문, 항상 종료 0. `.env` 의 `ALERT_WEBHOOK_URL`·`ALERT_LOG` 두 키만 읽어 export (이미 env 에 있으면 그 값 우선).
  - `scripts/cron_run.sh <이름> -- <명령...>` — 명령 출력을 stdout 으로 그대로 흘리고, rc≠0 이면 꼬리 30줄로 `error` 알림, **원래 rc 로 종료**. 꼬리에 `[alert.py]` 마커가 있으면 알림 생략.

- [ ] **Step 1: 실패하는 테스트 작성**

`tests/test_cron_run.py`:

```python
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
    assert "line40" in log and "line11" in log and "line10" not in log  # 꼬리 30줄


def test_cron_run_skips_alert_when_command_already_alerted(env, tmp_path):
    cmd = "echo working; echo '[alert.py] logged-only warn: 안에서 보냈다'; exit 1"
    r = run([str(CRON_RUN), "demo", "--", "bash", "-c", cmd], env)
    assert r.returncode == 1
    assert "중복 알림 생략" in r.stdout
    assert not (tmp_path / "alerts.log").exists()


def test_cron_run_without_command_is_usage_error(env):
    r = run([str(CRON_RUN), "demo", "--"], env)
    assert r.returncode == 2
```

- [ ] **Step 2: 실패 확인**

Run: `uv run --quiet --with pytest --with-requirements docker/requirements.txt --python 3.11 python -m pytest tests/test_cron_run.py -q`
Expected: 5 failed (`bash: .../scripts/alert.sh: No such file or directory`)

- [ ] **Step 3: `scripts/alert.sh` 작성**

```bash
#!/usr/bin/env bash
# alert.sh <warn|error|info> <title> [body]   — body 없으면 stdin
#
# collectors/alert.py 의 bash 입구. 크론·헬스체크·백업처럼 파이썬 패키지 컨텍스트가
# 없는 곳에서 부른다. 다른 레포(scalp-it 등)도 이걸 부르면 된다 — 그쪽은 이미
# ../quant-airflow/.env 를 읽고 있다.
#
# 규약 (collectors/alert.py 와 같다): **항상 종료 0.** 알림이 본작업의 종료코드를
# 바꾸면 안 된다. python3 가 없어도 0.
#
# .env 는 통째로 source 하지 않는다 — KIWOOM/DART 키가 무관한 자식에 퍼진다
# (daily_health_check.sh 의 env_get 과 같은 이유). 필요한 두 키만 읽는다. 값에 '=' 가
# 들어갈 수 있어 -f2- 이고, .env 에 따옴표로 감싼 값이 있어 양끝 따옴표를 뗀다.
# 이미 환경에 있으면(cron_run.sh → alert.sh, 테스트) 그 값을 우선한다.
set -u
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

env_get() {
    grep "^$1=" "$REPO/.env" 2>/dev/null | tail -n 1 | cut -d= -f2- | sed -e 's/^"//' -e 's/"$//'
}

if [ -z "${ALERT_WEBHOOK_URL:-}" ]; then ALERT_WEBHOOK_URL="$(env_get ALERT_WEBHOOK_URL)"; fi
if [ -z "${ALERT_LOG:-}" ]; then ALERT_LOG="$(env_get ALERT_LOG)"; fi
export ALERT_WEBHOOK_URL ALERT_LOG

if ! command -v python3 >/dev/null 2>&1; then
    echo "[alert.py] failed ${1:-?}: ${2:-?} — python3 없음" >&2
    exit 0
fi
python3 "$REPO/collectors/alert.py" "$@"
exit 0
```

- [ ] **Step 4: `scripts/cron_run.sh` 작성**

```bash
#!/usr/bin/env bash
# cron_run.sh <이름> -- <명령...>
#
# 크론 줄 하나를 감싼다: 명령 출력은 그대로 stdout 으로 흘리고(크론의 `>> log 2>&1`
# 리다이렉트가 지금처럼 동작한다), 종료코드가 0 이 아니면 출력 꼬리 30줄을 붙여
# alert.sh 로 알린 뒤 **원래 종료코드 그대로** 끝난다. 크론 로그 입장에선 아무것도
# 안 바뀐다.
#
# 이게 잡는 것은 스크립트 **안의** 핸들러가 돌기 전에 죽는 경우다 — 2026-09-12 백업이
# `cd: No such file or directory` 로 조용히 실패한 유형(인터프리터 없음·권한·경로).
# 스크립트 안 핸들러(헬스체크 ⚠️ 모음, 백업 EXIT 트랩)가 이미 알린 경우는 꼬리 안의
# `[alert.py]` 마커로 알아보고 중복으로 보내지 않는다. 안 핸들러를 빼지 않는 이유:
# 그쪽은 **왜** 죽었는지 안다, 여기는 꼬리만 안다.
set -u
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

name="${1:-}"
[ -n "$name" ] || { echo "cron_run.sh: 이름이 없다 — cron_run.sh <이름> -- <명령...>" >&2; exit 2; }
shift
[ "${1:-}" = "--" ] && shift
[ $# -gt 0 ] || { echo "cron_run.sh: 명령이 없다 — cron_run.sh <이름> -- <명령...>" >&2; exit 2; }

tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT

"$@" 2>&1 | tee "$tmp"
rc=${PIPESTATUS[0]}

if [ "$rc" -ne 0 ]; then
    tail_out="$(tail -n 30 "$tmp")"
    if printf '%s\n' "$tail_out" | grep -q '^\[alert\.py\] '; then
        echo "[cron_run] $name rc=$rc — 안에서 이미 알렸다(마커), 중복 알림 생략"
    else
        printf '%s\n' "$tail_out" | "$REPO/scripts/alert.sh" error "$name 실패 (rc=$rc)"
    fi
fi
exit "$rc"
```

- [ ] **Step 5: 실행 권한 + 문법**

Run: `chmod +x scripts/alert.sh scripts/cron_run.sh && bash -n scripts/alert.sh && bash -n scripts/cron_run.sh && echo OK`
Expected: OK

- [ ] **Step 6: 테스트 통과 확인**

Run: `uv run --quiet --with pytest --with-requirements docker/requirements.txt --python 3.11 python -m pytest tests/test_cron_run.py -q`
Expected: 5 passed

- [ ] **Step 7: 커밋**

```bash
git add scripts/alert.sh scripts/cron_run.sh tests/test_cron_run.py
git commit -F - <<'EOF'
feat(alert): bash 래퍼 alert.sh · cron_run.sh

alert.sh 는 .env 의 ALERT_WEBHOOK_URL/ALERT_LOG 두 키만 읽어 collectors/alert.py
를 부른다(항상 종료 0). cron_run.sh 는 크론 줄을 감싸 rc≠0 이면 출력 꼬리
30줄로 알리고 원래 rc 로 끝난다 — 09-12 백업처럼 스크립트 안 핸들러가 돌기
전에 죽는 유형을 잡는다. 꼬리에 `[alert.py]` 마커가 있으면 안에서 이미 알린
것이라 중복으로 보내지 않는다.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
EOF
```

---

### Task 3: `dags/_common.py` 최종 실패 콜백 + 6개 태스크 배선

**Files:**
- Modify: `dags/_common.py` (DEFAULT_TASK_KW 바로 위에 콜백 추가, 딕셔너리에 키 추가)
- Modify: `dags/daily_sharadar.py:35,77,88`
- Modify: `dags/earnings_backfill.py:55,74`
- Modify: `dags/weekly_delisted_stocks.py:66,79,95`

**Interfaces:**
- Consumes: `collectors.alert.format_task_failure`, `collectors.alert.notify`
- Produces: `dags._common.alert_task_failure(context) -> None`; `DEFAULT_TASK_KW` 에 `"on_failure_callback": alert_task_failure` 포함

CI 는 Airflow 를 설치하지 않아 `_common` 을 import 하는 pytest 는 못 쓴다. 검증은 (a) 순수 함수 테스트(Task 1 에서 끝남), (b) AST 파싱(CI 와 동일한 방식), (c) 배포 후 실제 실패에서 `alerts.log` 확인(Task 7).

- [ ] **Step 1: `_common.py` 에 콜백 추가**

`from collectors.proc import stream_subprocess  # noqa: E402` 바로 아래에:

```python
from collectors.alert import format_task_failure, notify  # noqa: E402


def alert_task_failure(context) -> None:
    """``on_failure_callback`` — 재시도를 다 쓴 **최종 실패**에만 알린다.

    Airflow 는 이 콜백을 태스크가 failed 로 확정될 때만 부른다(재시도 시엔
    ``on_retry_callback``). 그래서 이것 하나로 "재시도로 안 낫는 실패만 사람에게"가
    된다 — 2026-09-29 16:05 공시 수집(CardinalityViolation)은 attempt 2 가 죽은 10:15 에
    한 번 왔을 것이고, 그 실패는 닷새 뒤에야 로그에서 발견됐다. ``upstream_failed`` 는
    실행된 적이 없어 콜백이 안 불린다 — 상류가 이미 알렸으니 맞다.

    메시지 조립은 ``collectors.alert.format_task_failure`` (순수 함수, Airflow 없이
    테스트)에 두고 여기서는 context 에서 값만 꺼낸다. 콜백이 던지면 Airflow 는 로그에
    남기고 넘어가지만 깔끔하게 삼킨다 — 알림이 태스크 상태를 건드리면 안 된다.
    """
    try:
        ti = context["task_instance"]
        title, body = format_task_failure(
            dag_id=ti.dag_id,
            task_id=ti.task_id,
            run_id=str(context.get("run_id") or ""),
            try_number=ti.try_number,
            # run_collector 의 CalledProcessError 는 이미 _masked(cmd) 지만, alert.py 가
            # 한 번 더 mask_secrets 를 거친다.
            exc_text=str(context.get("exception") or ""),
            log_url=str(getattr(ti, "log_url", "") or ""),
        )
        notify("error", title, body)
    except Exception as e:  # noqa: BLE001 — 알림 실패는 태스크 실패를 덮어쓰지 않는다
        print(f"[alert_task_failure] 알림 실패를 삼킨다: {type(e).__name__}: {e}", flush=True)
```

그리고 `DEFAULT_TASK_KW` 를:

```python
#: 콜렉터 태스크의 기본 재시도 정책. 12개 DAG 의 @task 18개 중 11개가 이 값을
#: 글자 그대로 반복하고 있었다 — 공통값을 여기 두면 나머지 7개(sharadar 의
#: retries=2, earnings_backfill 의 30분, 폐지 백필의 20분)가 "일부러 다른 값"
#: 으로 눈에 띈다. 반복된 리터럴 사이에서는 그 의도가 안 보인다.
#:
#: on_failure_callback 도 여기 산다(2026-10-05). 다른 값이 필요한 태스크는
#: ``@task(**{**DEFAULT_TASK_KW, "retries": 2})`` 처럼 **덮어쓰기**로 적는다 —
#: ``@task(retries=2)`` 로 따로 쓰면 콜백이 조용히 빠진다.
DEFAULT_TASK_KW = {
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "on_failure_callback": alert_task_failure,
}
```

(기존 `DEFAULT_TASK_KW = {"retries": 1, "retry_delay": timedelta(minutes=10)}` 한 줄과 그 위 4줄 주석을 위 블록으로 교체. 콜백 함수는 이 딕셔너리보다 **위**에 있어야 한다.)

- [ ] **Step 2: 6개 태스크를 덮어쓰기 꼴로**

값은 하나도 바꾸지 않는다. 기본값(retries 1, 10분)과 같은 항목은 적지 않는다.

`dags/daily_sharadar.py`:
- 35행 `from _common import run_collector, sharadar_env` → `from _common import DEFAULT_TASK_KW, run_collector, sharadar_env`
- 77행 `@task(retries=2, retry_delay=timedelta(minutes=10))` → `@task(**{**DEFAULT_TASK_KW, "retries": 2})`
- 88행 `@task(retries=1, retry_delay=timedelta(minutes=15))` → `@task(**{**DEFAULT_TASK_KW, "retry_delay": timedelta(minutes=15)})`

`dags/earnings_backfill.py`:
- 55행 → `from _common import DEFAULT_TASK_KW, dart_env, run_collector, timescale_dsn`
- 74행 `@task(retries=2, retry_delay=timedelta(minutes=30))` → `@task(**{**DEFAULT_TASK_KW, "retries": 2, "retry_delay": timedelta(minutes=30)})`

`dags/weekly_delisted_stocks.py` (import 는 이미 있음):
- 66·79·95행 `@task(retries=1, retry_delay=timedelta(minutes=20))` → `@task(**{**DEFAULT_TASK_KW, "retry_delay": timedelta(minutes=20)})` (세 줄 모두)

세 파일 모두 `timedelta` 는 여전히 쓰이므로 import 를 지우지 않는다. 바뀐 줄 바로 위에 원래 있던 주석(왜 그 값인지)은 그대로 둔다.

- [ ] **Step 3: AST 파싱 + 린트 (CI 와 같은 검사)**

Run:
```bash
python3 - <<'PY'
import ast, pathlib
for p in sorted(pathlib.Path("dags").glob("*.py")):
    ast.parse(p.read_text(encoding="utf-8"), filename=str(p))
print("dags parse OK")
PY
uv run --quiet --with 'ruff==0.14.0' --python 3.11 ruff check dags/ collectors/
grep -n "@task(retries" dags/*.py || echo "직접 retries= 를 적은 태스크 없음"
```
Expected: `dags parse OK`, `All checks passed!`, `직접 retries= 를 적은 태스크 없음`

- [ ] **Step 4: 재시도 값이 안 바뀌었는지 눈으로 확인**

Run: `git diff -U0 dags/ | grep '^[-+]' | grep -v '^+++\|^---'`
Expected: 각 `-` 줄의 retries/retry_delay 값이 짝이 되는 `+` 줄에 (기본값이라 생략됐거나) 그대로 있다. `schedule=` 줄은 diff 에 없다.

- [ ] **Step 5: 커밋**

```bash
git add dags/_common.py dags/daily_sharadar.py dags/earnings_backfill.py dags/weekly_delisted_stocks.py
git commit -F - <<'EOF'
feat(dags): 태스크 최종 실패를 on_failure_callback 으로 알린다

DEFAULT_TASK_KW 에 alert_task_failure 를 넣는다. Airflow 는 이 콜백을 재시도를
다 쓴 최종 실패에만 부르므로 "재시도로 안 낫는 실패만 사람에게"가 된다 —
09-29 공시 수집 실패(attempt 1·2 동일 예외)는 닷새 뒤 로그에서 발견됐다.

직접 retries= 를 적던 6개 태스크(daily_sharadar 2, earnings_backfill 1,
weekly_delisted_stocks 3)는 {**DEFAULT_TASK_KW, ...} 덮어쓰기 꼴로 바꿔
콜백이 따라가게 한다. 재시도 횟수·지연 값은 그대로다.

메시지 조립은 collectors.alert.format_task_failure(순수 함수)에 있어 Airflow
없이 테스트된다. 콜백 본문은 예외를 삼킨다 — 알림이 태스크 상태를 건드리지
않는다.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
EOF
```

---

### Task 4: `scripts/daily_health_check.sh` — ⚠️ 모아서 실행당 1건

**Files:**
- Modify: `scripts/daily_health_check.sh` (17행 `log()` 아래, 101행, 122·124·126·143·156·169·172·173·198·200행, 216행 앞)

**Interfaces:**
- Consumes: `scripts/alert.sh warn <title>` (stdin 본문)
- Produces: 함수 `warn()`; 배열 `WARNINGS`; 스크립트 끝에서 경보가 있으면 알림 1건

- [ ] **Step 1: `warn()` 추가**

17행 `log() { ... }` 바로 아래에:

```bash
# ⚠️ 는 log 가 아니라 warn 으로 찍는다 — 같은 줄을 로그에 남기면서 모아뒀다가
# 스크립트 끝에서 **한 번에 한 건**으로 알린다(scripts/alert.sh). 실행당 1건인 이유:
# 리플리카 경보처럼 두 줄이 세트로 나오는 걸 쪼개 보내면 채널이 시끄럽고, 하루 두 번
# 도는 점검이라 같은 경보가 두 번 올 수 있는 건 허용한다(spec 2026-10-05 "범위 밖").
# URL 이 없는 동안은 ~/logs/quant-airflow/alerts.log 에만 쌓인다.
WARNINGS=()
warn() { log "⚠️ $*"; WARNINGS+=("$*"); }
```

- [ ] **Step 2: 기존 ⚠️ 줄을 전부 warn 으로**

Run: `sed -i 's/log "⚠️ /warn "/g' scripts/daily_health_check.sh && grep -c 'warn "' scripts/daily_health_check.sh`
Expected: 11 (122·124·126·143·156·169·172·173·198·200 의 10줄 + 아래 Step 3 전이면 10 — Step 3 뒤 다시 세면 12)

101행 `" 2>&1 || log "커버리지 점검 실패 (DB 연결 안 됨?)"` → `" 2>&1 || warn "커버리지 점검 실패 (DB 연결 안 됨?)"`

- [ ] **Step 3: `daily_bars` 누락 ≥ 100 경보**

`report_coverage()` 안, 커버리지 `ts_psql -c "..." 2>&1 || warn ...` 블록 **바로 뒤**(함수 닫는 `}` 앞)에:

```bash
    # 표는 사람이 보지만 알림은 숫자가 있어야 한다. daily_bars 만 임계값을 둔다 —
    # 정상은 한 자리(상폐 제외 후 09-22 실측 3), 거래정지가 몰려도 수십이다. 100 을
    # 넘으면 "16:00 수집이 통째로 빠졌다"는 뜻이고, 그건 지금까지 표로만 찍히고
    # 끝났다. 다른 테이블 임계값은 다음 하위 프로젝트(DAG 실패 자가치유)의 몫이다.
    local bars_missing
    bars_missing=$(ts_psql -tAc "
SELECT COUNT(*) FROM stocks s
 WHERE NOT EXISTS (SELECT 1 FROM delisted_stocks x WHERE x.code = s.code)
   AND NOT EXISTS (SELECT 1 FROM daily_bars d WHERE d.code = s.code AND d.date = '$today')
" 2>/dev/null | tr -d '[:space:]')
    if [ -n "$bars_missing" ] && [ "$bars_missing" -ge 100 ] 2>/dev/null; then
        warn "daily_bars $today 누락 ${bars_missing}종목 — 16:00 수집이 통째로 빠졌을 가능성"
    fi
```

- [ ] **Step 4: 끝에서 1건 전송**

216행 `exit 0` 을 다음으로 교체:

```bash
if [ ${#WARNINGS[@]} -gt 0 ]; then
    printf '%s\n' "${WARNINGS[@]}" | scripts/alert.sh warn "헬스체크 경보 ${#WARNINGS[@]}건 ($today)"
fi
exit 0
```

(스크립트가 상단에서 `cd "$(dirname "$0")/.."` 하므로 `scripts/alert.sh` 는 레포 루트 기준이다.)

- [ ] **Step 5: 문법 + 리플리카에 대고 실행**

Run: `bash -n scripts/daily_health_check.sh && echo syntax OK`
Expected: syntax OK

trader 리플리카로 실제 실행(09-23 세션과 같은 방식 — 스크래치 사본에 `BACKUP_DB_CONTAINER=quant-airflow-timescaledb-1` 를 넣고 날짜를 지난 영업일로 고정):

```bash
S=$(mktemp -d) && mkdir -p "$S/scripts" "$S/collectors"
cp .env "$S/.env" 2>/dev/null || cp /home/young/git/quant-airflow/.env "$S/.env"
echo 'BACKUP_DB_CONTAINER=quant-airflow-timescaledb-1' >> "$S/.env"
echo "ALERT_LOG=$S/alerts.log" >> "$S/.env"
cp scripts/daily_health_check.sh scripts/alert.sh "$S/scripts/" && cp collectors/alert.py collectors/config.py collectors/__init__.py "$S/collectors/"
sed -i 's/^today=.*/today=${FAKE_TODAY:-$(date +%Y-%m-%d)}/; s/^    dow=$(date +%u)/    dow=$(date -d "$today" +%u)/; s/if \[ "$(date +%H)" -lt 17 \]/if [ "${FAKE_HOUR:-$(date +%H)}" -lt 17 ]/' "$S/scripts/daily_health_check.sh"
FAKE_TODAY=2026-10-02 FAKE_HOUR=18 bash "$S/scripts/daily_health_check.sh"; echo "rc=$?"; echo "--- alerts.log ---"; cat "$S/alerts.log" 2>/dev/null || echo "(경보 없음 — 로그 파일 없음)"
```
Expected: rc=0. 메타DB 가 없어 `복제 슬롯 조회 실패` 경보가 1건 이상 나오므로 마지막에 `[alert.py] logged-only warn: 헬스체크 경보 N건 (2026-10-02)` 가 찍히고 `alerts.log` 에 그 줄들이 들여쓰기로 들어 있다.

- [ ] **Step 6: 커밋**

```bash
git add scripts/daily_health_check.sh
git commit -F - <<'EOF'
feat(health): ⚠️ 줄을 모아 실행당 알림 1건 + daily_bars 누락 ≥100 경보

지금까지 헬스체크의 ⚠️ 는 로그 파일에만 남았다 — 리플리카 슬롯이 09-19~21
사흘 죽어 있는 동안 매 실행 두 줄씩 찍혔지만 아무도 못 봤다. warn() 이 같은
줄을 로그에 남기면서 모아두고, 끝에서 scripts/alert.sh 로 한 건 보낸다.

daily_bars 누락은 표로만 찍혀 "16:00 수집이 통째로 빠진 날" 도 조용했다.
상폐 제외 후 정상은 한 자리라 100 을 바닥으로 둔다.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
EOF
```

---

### Task 5: `scripts/backup_to_gdrive.sh` — 비정상 종료 알림

**Files:**
- Modify: `scripts/backup_to_gdrive.sh:54` (`trap 'rm -rf "$TMPDIR"' EXIT`)

**Interfaces:**
- Consumes: `scripts/alert.sh error <title>` (stdin 본문), 환경변수 `BACKUP_LOG` (크론 줄이 넣는다 — Task 6)
- Produces: rc≠0 로 끝나면 `백업 실패 (rc=N, BACKUP_ONLY=…)` 알림 1건, 본문은 `BACKUP_LOG` 꼬리 30줄(없으면 제목만)

- [ ] **Step 1: 트랩을 함수로**

54행 `trap 'rm -rf "$TMPDIR"' EXIT` 를 다음으로 교체:

```bash
# 비정상 종료는 사람에게 간다. 2026-09-12 의 `cd: No such file` 처럼 크론 로그에
# 한 줄 남고 끝나던 실패가 이 레포 사고의 전형이었다(spec 2026-10-05). rc 를 **먼저**
# 잡는다 — rm 이 성공하면 $? 가 0 으로 덮인다. 본문은 자기 stdout 을 모을 수 없어
# (크론이 파일로 보낸다) 크론 줄이 넘겨준 BACKUP_LOG 의 꼬리를 읽는다; 없으면 제목만.
# set -e 아래라 트랩 안의 실패가 다시 트랩을 부르지 않게 전부 `|| true` 다.
on_exit() {
  local rc=$?
  rm -rf "$TMPDIR"
  if [ "$rc" -ne 0 ]; then
    { [ -n "${BACKUP_LOG:-}" ] && [ -r "$BACKUP_LOG" ] && tail -n 30 "$BACKUP_LOG"; } 2>/dev/null \
      | "$REPO/scripts/alert.sh" error "백업 실패 (rc=$rc, BACKUP_ONLY=${BACKUP_ONLY:-all})" || true
  fi
}
trap on_exit EXIT
```

- [ ] **Step 2: 문법 + 실패 경로 실제 실행**

Run:
```bash
bash -n scripts/backup_to_gdrive.sh && echo syntax OK
# 잘못된 BACKUP_ONLY 로 exit 2 경로를 태운다 — 덤프·업로드는 하나도 안 돈다.
L=$(mktemp); echo "이전 로그 줄" > "$L"
ALERT_LOG=$(mktemp -d)/alerts.log BACKUP_LOG="$L" BACKUP_ONLY=bogus bash scripts/backup_to_gdrive.sh >> "$L" 2>&1; echo "rc=$?"
grep -c "error 백업 실패 (rc=2, BACKUP_ONLY=bogus)" "$(dirname "$ALERT_LOG")"/alerts.log 2>/dev/null || tail -n 5 "$L"
```
Expected: syntax OK, `rc=2`, 그리고 `[alert.py] logged-only error: 백업 실패 (rc=2, BACKUP_ONLY=bogus)` 가 `$L` 안에 있다 (ALERT_LOG 를 커맨드 치환 안에서 만들어 변수로 못 잡으므로 `$L` 의 꼬리로 확인한다). 이 워크트리에는 `.env` 가 없을 수 있다 — 그러면 `. ./.env` 에서 먼저 죽어 rc=1 이 나오고 알림 제목이 `rc=1` 이 된다; 그것도 트랩이 동작한 증거다.

- [ ] **Step 3: 커밋**

```bash
git add scripts/backup_to_gdrive.sh
git commit -F - <<'EOF'
feat(backup): 비정상 종료를 EXIT 트랩에서 알린다

09-12 의 `cd: No such file` 처럼 크론 로그에 한 줄 남고 끝나던 실패가 이
스크립트 사고의 전형이었다. rc 를 먼저 잡고 정리한 뒤 rc≠0 이면
scripts/alert.sh 로 보낸다. 본문은 크론 줄이 넘기는 BACKUP_LOG 의 꼬리 30줄.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
EOF
```

---

### Task 6: 배선 — compose · `.env.example` · crontab · operations.md

**Files:**
- Modify: `docker-compose.airflow.yml` (`x-airflow-common-env` 블록 끝, `TIMESCALE_PASSWORD` 줄 아래)
- Modify: `.env.example` (`# --- Airflow ---` 블록 위)
- Modify: `deploy/crontab.simnode` (헬스체크 2줄, 백업 1줄)
- Modify: `docs/operations.md` (`## 시크릿 처리` 절 앞에 `## 알림` 절 추가, `## 저장소 구조` 트리에 두 스크립트·alert.py 추가)

**Interfaces:**
- Consumes: Task 2 의 `cron_run.sh` 인자 규약, Task 5 의 `BACKUP_LOG`
- Produces: 컨테이너 env `ALERT_WEBHOOK_URL`·`ALERT_LOG` (재생성 후 유효)

- [ ] **Step 1: compose env**

`docker-compose.airflow.yml` 의 `  TIMESCALE_PASSWORD: ${TIMESCALE_PASSWORD}` 줄 아래에:

```yaml
  # 운영 알림(docs/superpowers/specs/2026-10-05-alert-channel-design.md). 비어 있으면
  # on_failure_callback 은 태스크 로그에 `[alert.py] logged-only` 만 남긴다 — URL 을
  # 아직 못 받은 지금의 의도된 상태다. 값을 .env 에 넣은 뒤엔 `up -d` 로 env 를
  # 재적용해야 한다(컨테이너 재생성 — CLAUDE.md §1, 승인 필요).
  ALERT_WEBHOOK_URL: ${ALERT_WEBHOOK_URL:-}
  ALERT_LOG: /opt/airflow/logs/alerts.log
```

Run: `docker compose -f docker-compose.airflow.yml config --quiet 2>&1 | head -3; echo "rc=$?"`
Expected: 경고는 있어도(`.env` 변수 미설정) 문법 오류 없이 rc=0. (`.env` 가 없어 `required variable` 오류가 나면 `env ALERT_WEBHOOK_URL= AIRFLOW_META_USER=x AIRFLOW_META_PASSWORD=x AIRFLOW_META_DB=x AIRFLOW__CORE__FERNET_KEY=x AIRFLOW__WEBSERVER__SECRET_KEY=x TIMESCALE_DB=x TIMESCALE_USER=x TIMESCALE_PASSWORD=x KR_QUANT_SQLITE_PATH=x docker compose -f docker-compose.airflow.yml config --quiet` 로 다시.)

- [ ] **Step 2: `.env.example`**

`# --- Airflow ---` 줄 **위**에:

```
# --- 운영 알림 (Discord 웹훅) ---
# 채널 설정 → 연동 → 웹훅 → 새 웹훅 → URL 복사. 비어 있으면 알림은 로컬 로그
# (ALERT_LOG, 기본 ~/logs/quant-airflow/alerts.log)에만 남는다. 두 호스트 .env 둘 다.
ALERT_WEBHOOK_URL=

```

- [ ] **Step 3: crontab**

`deploy/crontab.simnode` 의 세 줄을 교체 (스케줄·로그 경로는 그대로):

```
35 11 * * * /home/young/git/quant-airflow/scripts/cron_run.sh health-check -- /home/young/git/quant-airflow/scripts/daily_health_check.sh >> /home/young/logs/quant-airflow/daily_health_check.log 2>&1
10 18 * * * /home/young/git/quant-airflow/scripts/cron_run.sh health-check -- /home/young/git/quant-airflow/scripts/daily_health_check.sh >> /home/young/logs/quant-airflow/daily_health_check.log 2>&1
```

```
0 19 * * * BACKUP_LOG=/home/young/logs/quant-airflow/backup_to_gdrive.cron.log /home/young/git/quant-airflow/scripts/cron_run.sh backup -- /home/young/git/quant-airflow/scripts/backup_to_gdrive.sh >> /home/young/logs/quant-airflow/backup_to_gdrive.cron.log 2>&1
```

각 블록 위 주석에 한 단락 추가:

```
# 2026-10-05 부터 세 줄 다 scripts/cron_run.sh 로 감싼다 — 스크립트가 뜨기 전에 죽는
# 실패(09-12 `cd: No such file`)도 알림이 가게. 백업 줄의 BACKUP_LOG 는 EXIT 트랩이
# 알림 본문으로 쓸 꼬리를 읽는 경로다(같은 파일로 리다이렉트되고 있다).
```

Run (cron-install 의 검증 규칙 — 5필드 + 명령, `VAR=value` 단독 줄 거부 — 를 통과하는지):
```bash
python3 - <<'PY'
import re
ok = True
for n, line in enumerate(open("deploy/crontab.simnode", encoding="utf-8"), 1):
    s = line.strip()
    if not s or s.startswith("#"):
        continue
    parts = s.split(None, 5)
    if len(parts) < 6 or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*=.*", s):
        ok = False; print("BAD", n, s)
print("crontab lines OK" if ok else "crontab BAD")
PY
```
Expected: `crontab lines OK`

- [ ] **Step 4: operations.md**

`## 시크릿 처리` 바로 앞에 절 추가:

```markdown
## 알림 — 사고는 로그가 아니라 사람에게 간다

설계: `docs/superpowers/specs/2026-10-05-alert-channel-design.md`. 전송기는
`collectors/alert.py` 하나(Discord 웹훅 POST, stdlib 만), bash 입구는
`scripts/alert.sh`, 크론 줄 래퍼는 `scripts/cron_run.sh`.

| 발생지 | 언제 | 어떻게 |
|---|---|---|
| Airflow 태스크 | **재시도를 다 쓴 최종 실패**만 | `dags/_common.py` `DEFAULT_TASK_KW["on_failure_callback"]` |
| `daily_health_check.sh` | `⚠️` 가 하나라도 있으면 실행당 1건 | `warn()` 모음 → `alert.sh` |
| `backup_to_gdrive.sh` | rc≠0 로 끝날 때 | EXIT 트랩 → `alert.sh` (본문: `BACKUP_LOG` 꼬리) |
| simnode 크론 3줄 | 스크립트가 뜨기 전에 죽을 때 | `cron_run.sh` (안에서 이미 알렸으면 `[alert.py]` 마커로 생략) |

**URL 이 없으면** 모든 경로가 `~/logs/quant-airflow/alerts.log`(컨테이너는
`logs/alerts.log`)에만 쓰고 종료 0 이다. 알림 코드가 본작업을 죽이는 일은 없다.

**URL 을 넣는 날 (양쪽 호스트):**

1. Discord 채널 설정 → 연동 → 웹훅 → URL 을 `quant-airflow/.env` 에
   `ALERT_WEBHOOK_URL=…` 로. simnode·trader 둘 다.
2. simnode 에서 `scripts/alert.sh info "알림 통로 개통"` — 채널에 오면 끝.
3. Airflow 컨테이너는 env 를 기동 때 읽으므로 **승인 받고** 장 마감 후
   `docker compose -f docker-compose.airflow.yml up -d` (스케줄러·웹서버 재생성).
   그 전까지 콜백은 `logged-only` 로 동작한다 — 의도된 상태다.

성공 알림·재시도 알림·중복 억제는 없다 — 시끄러워지면 그때 넣는다.
```

`## 저장소 구조` 트리의 `collectors/` 아래에 `  alert.py             #   운영 알림 전송기(Discord 웹훅, stdlib 만) — 유일한 출구`, `scripts/` 아래에 `  alert.sh / cron_run.sh # 알림 bash 입구 · 크론 줄 래퍼(rc≠0 이면 꼬리 30줄 알림)` 추가.

- [ ] **Step 5: 전체 테스트 + 린트 + 셸 문법**

Run:
```bash
uv run --quiet --with pytest --with-requirements docker/requirements.txt --python 3.11 python -m pytest tests/ -q 2>&1 | tail -2
uv run --quiet --with 'ruff==0.14.0' --python 3.11 ruff check collectors/ dags/ tests/ scripts/
for f in scripts/alert.sh scripts/cron_run.sh scripts/daily_health_check.sh scripts/backup_to_gdrive.sh; do bash -n "$f" || echo "BAD $f"; done; echo "sh OK"
```
Expected: `258 passed` (241 + 12 + 5), `All checks passed!`, `sh OK`

- [ ] **Step 6: 커밋**

```bash
git add docker-compose.airflow.yml .env.example deploy/crontab.simnode docs/operations.md
git commit -F - <<'EOF'
chore(alert): 배선 — compose env · .env.example · crontab 래핑 · operations.md

compose 에 ALERT_WEBHOOK_URL/ALERT_LOG 를 통과시킨다(재생성은 하지 않는다 —
URL 이 오는 날 승인 후). simnode 크론 3줄을 cron_run.sh 로 감싼다. operations.md
에 발생지 4곳과 "URL 을 넣는 날 할 일" 을 적는다.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
EOF
```

---

### Task 7: 통합 — main 머지·푸시, simnode 에서 실측

**Files:** 없음 (운영 확인)

**Interfaces:**
- Consumes: Task 1~6 전부

- [ ] **Step 1: 브랜치 정리 확인**

Run: `git log --oneline main..HEAD && git status --short`
Expected: 커밋 7개(spec + plan + Task1~6), 작업트리 깨끗.

- [ ] **Step 2: main 으로 fast-forward 머지 후 푸시**

superpowers:finishing-a-development-branch 를 따른다. 머지는 메인 체크아웃(`/home/young/git/quant-airflow`, sparse)에서 `git merge --ff-only worktree-alerts`, 푸시는 pre-push ci-local 이 돈다 — 그 워크트리(`.git/ci-local/worktree`)는 이번 세션에서 sparse 를 풀어뒀다. 통과 로그 5줄(Install·Lint·DAG 파싱·시크릿 스캔·Test)을 확인한다.

- [ ] **Step 3: simnode 가 받았는지**

Run: `ssh 192.168.45.9 'cd ~/git/quant-airflow && git log --oneline -1 && ls -l scripts/alert.sh scripts/cron_run.sh'`
Expected: HEAD 가 푸시한 SHA, 두 스크립트가 실행 가능(`-rwxr-xr-x`). 아니면 `pull-all` 이 07:50 에 가져온다 — 기다리거나 `ssh 192.168.45.9 '~/.local/bin/pull-all ~/git/quant-airflow'`.

- [ ] **Step 4: simnode 실측 (spec "검증" 절)**

```bash
ssh 192.168.45.9 'cd ~/git/quant-airflow && scripts/alert.sh warn "알림 통로 배선 확인 (URL 없음)" && tail -n 3 ~/logs/quant-airflow/alerts.log'
ssh 192.168.45.9 'cd ~/git/quant-airflow && scripts/cron_run.sh 테스트 -- false; echo "rc=$?"; tail -n 2 ~/logs/quant-airflow/alerts.log'
ssh 192.168.45.9 '~/.local/bin/cron-install --diff 2>&1 | head -30'
```
Expected: 첫 줄 `[alert.py] logged-only warn: …` + 로그에 줄. 둘째 `rc=1` + 로그에 `error 테스트 실패 (rc=1)`. 셋째 diff 에 quant-airflow 블록 3줄이 `cron_run.sh` 로 바뀌어 보이고 거부 메시지가 없다 — 그러면 `cron-install` 을 실행해 반영한다(07:50 을 안 기다려도 된다; crontab 편집은 §1 금지 항목이 아니다).

- [ ] **Step 5: Airflow 가 새 `_common` 을 파싱했는지**

```bash
ssh 192.168.45.9 'docker exec quant-airflow-airflow-meta-db-1 psql -U airflow -d airflow_meta -tAc "SELECT count(*) FROM import_error;"'
ssh 192.168.45.9 'docker exec quant-airflow-airflow-scheduler-1 python -c "import sys; sys.path.insert(0, \"/opt/airflow/dags\"); import _common; print(_common.DEFAULT_TASK_KW[\"on_failure_callback\"].__name__)"'
```
Expected: `0`, `alert_task_failure`. (스케줄러의 DAG 재파싱은 최대 30분 — `MIN_FILE_PROCESS_INTERVAL=1800`. import_error 는 그 뒤에 다시 본다.)

- [ ] **Step 6: 다음 18:10 헬스체크 확인 (하루 뒤)**

Run: `ssh 192.168.45.9 'grep -n "\[alert.py\]\|\[cron_run\]" ~/logs/quant-airflow/daily_health_check.log | tail -n 5'`
Expected: 경보가 있는 날은 `[alert.py] logged-only warn: 헬스체크 경보 N건`, 없는 날은 아무 줄도 없음 — 둘 중 하나가 **의도대로**. `[cron_run]` 중복 생략 줄이 보이면 마커 억제가 동작한 것이다.

- [ ] **Step 7: 메모리 갱신**

`~/.claude/projects/-home-young-git-quant-airflow/memory/` 에 프로젝트 메모 하나: "알림 통로 1번 완료(2026-10-05), URL 미설정 — 들어오면 operations.md '알림' 절 3단계. 다음은 2 배포=푸시, 3 DAG 자가치유, 4 리플리카 복구." `MEMORY.md` 에 한 줄.

---

## Self-Review

**Spec coverage**
- 구성요소 1 전송기 → Task 1 ✔ (stdlib·예외 삼킴·로컬 로그·마커·마스킹·절단·타임아웃·CLI)
- 구성요소 2 래퍼 → Task 2 ✔ (env 두 키·종료 0·꼬리 30줄·rc 보존·마커 중복 억제)
- 구성요소 3 콜백 → Task 3 ✔ (DEFAULT_TASK_KW·6개 태스크 덮어쓰기·값 불변·순수 함수 분리)
- 구성요소 4 헬스체크 → Task 4 ✔ (warn 모음·1건·daily_bars ≥100·건너뜀은 log)
- 구성요소 5 백업 → Task 5 ✔ (rc 선점·BACKUP_LOG 꼬리·`|| true`)
- 구성요소 6 배선 → Task 6 ✔ (compose·.env.example·crontab 3줄·operations.md 3단계)
- 배포·검증 절 → Task 7 ✔
- 범위 밖 항목(중복 억제·성공 알림·retry 콜백·scalp-it 크론)은 어느 태스크에도 없다 ✔

**Placeholder scan** — "TBD/TODO/적절히/나중에 채움" 없음. 모든 코드 스텝에 실제 코드. Task 7 은 운영 명령과 기대 출력이 있다.

**Type consistency**
- `format_task_failure` 키워드 인자 6개 — Task 1 정의 = Task 3 호출 ✔
- `notify(level, title, body) -> bool` — Task 1 = Task 3 ✔
- 마커 접두 `[alert.py] ` — Task 1 출력 = Task 2 `grep '^\[alert\.py\] '` = Task 2 테스트 문자열 ✔
- `alert.sh <level> <title>` stdin 본문 — Task 2 정의 = Task 4·5 호출 ✔
- `BACKUP_LOG` — Task 5 읽음 = Task 6 크론 줄 ✔
- 테스트 수 258 = 241 + 12(Task 1) + 5(Task 2) ✔
