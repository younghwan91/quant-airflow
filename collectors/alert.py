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
    감싼 뒤 자르면 닫는 펜스가 날아가 다음 메시지까지 코드로 보인다.
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
