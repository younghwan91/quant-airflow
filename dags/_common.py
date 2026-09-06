"""Shared helpers for DAG subprocess invocation — DSN/credential injection.

Not a DAG itself (no ``@dag``-decorated function), so Airflow's DagFileProcessor
parses it and finds zero DAGs — harmless, same as any other non-DAG .py file
in the dags folder. Importable as a plain top-level module (``from _common
import ...``) because Airflow adds each dag file's own directory (here,
``dags/``) to ``sys.path`` before exec'ing it.

Previously each DAG file duplicated ``_timescale_dsn()``/``_kiwoom_env()``/
``_dart_env()``/``_run()`` verbatim — a DSN format change meant editing 10
files. Centralized here instead.
"""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import timedelta

from airflow.models import Variable

# collectors/는 /opt/airflow 밑에 있지만, Airflow는 DAG 파싱 시 각 dag 파일
# 자신의 디렉터리(dags/)만 sys.path에 넣는다 — /opt/airflow 자체는 자동으로
# 안 잡힌다. 그래서 task 본문에서는 다들 sys.path.insert(0, "/opt/airflow")를
# 직접 해왔다. 여기서는 모듈 로드 시점(=DAG 파싱
# 시점)에 필요하므로 import 전에 미리 넣는다.
sys.path.insert(0, "/opt/airflow")

from collectors.config import DART_KEY_ENV_VARS  # noqa: E402
from collectors.proc import stream_subprocess  # noqa: E402


#: 콜렉터 태스크의 기본 재시도 정책. 12개 DAG 의 @task 18개 중 11개가 이 값을
#: 글자 그대로 반복하고 있었다 — 공통값을 여기 두면 나머지 7개(sharadar 의
#: retries=2, earnings_backfill 의 30분, 폐지 백필의 20분)가 "일부러 다른 값"
#: 으로 눈에 띈다. 반복된 리터럴 사이에서는 그 의도가 안 보인다.
DEFAULT_TASK_KW = {"retries": 1, "retry_delay": timedelta(minutes=10)}


def timescale_dsn() -> str:
    return (
        f"postgresql://{os.environ['TIMESCALE_USER']}:{os.environ['TIMESCALE_PASSWORD']}"
        f"@{os.environ['TIMESCALE_HOST']}:{os.environ.get('TIMESCALE_PORT', '5432')}"
        f"/{os.environ['TIMESCALE_DB']}"
    )


def kiwoom_env() -> dict[str, str]:
    # Credentials live only in Airflow's Fernet-encrypted Variables store,
    # not in container env — injected here for the collector subprocess only.
    env = os.environ.copy()
    env["KIWOOM_APP_KEY"] = Variable.get("KIWOOM_APP_KEY")
    env["KIWOOM_APP_SECRET"] = Variable.get("KIWOOM_APP_SECRET")
    return env


#: 보조 키 이름들 — 정본은 ``collectors.config.DART_KEY_ENV_VARS`` 하나다.
#: 여기에 목록을 따로 적어두면 collect_keys 쪽과 갈라지고, 갈라지면 키가 조용히
#: 도달하지 않는다(그 사고 기록은 정본 쪽 주석에 있다). 첫 항목은 아래에서
#: 필수 키로 따로 꺼내므로 뺀다.
_DART_KEY_VARS = DART_KEY_ENV_VARS[1:]


def dart_env() -> dict[str, str]:
    # DART 키는 Fernet 암호화 Variables에만 있음 — 수집 subprocess에만 주입.
    # 보조키가 있으면 함께 주입 → collector가 일한도(020) 시 다음 키로 로테이션.
    env = os.environ.copy()
    env[DART_KEY_ENV_VARS[0]] = Variable.get(DART_KEY_ENV_VARS[0])
    for name in _DART_KEY_VARS:
        value = Variable.get(name, default_var=None)
        if value:
            env[name] = value
    return env


def claude_env() -> dict[str, str]:
    # Claude 키도 다른 자격증명과 같이 Fernet Variables에만 둔다.
    # news_judge.py가 Gemini에서 전환(2026-09-06) — 근거는 collectors/news_judge.py
    # 의 _claude_generate() docstring.
    env = os.environ.copy()
    env["ANTHROPIC_API_KEY"] = Variable.get("ANTHROPIC_API_KEY")
    return env


def sharadar_env() -> dict[str, str]:
    """Sharadar 직판 API 키 + opt_portfolio 를 import 할 PYTHONPATH.

    키는 다른 자격증명과 같이 Fernet Variables 에만 둔다. PYTHONPATH 는
    weekly_price_adjust 가 kr-quant 에 쓰는 것과 같은 수법이다 — opt_portfolio
    도 ro 마운트만 하고 pip install 하지 않으므로 src 레이아웃을 직접 가리킨다.
    ``/opt/airflow`` 를 함께 넣는 건 자식이 ``collectors.sharadar_us`` 를
    ``python -m`` 으로 찾아야 하기 때문이다.
    """
    env = os.environ.copy()
    env["SHARADAR_API_KEY"] = Variable.get("SHARADAR_API_KEY")
    env["PYTHONPATH"] = "/opt/airflow:/opt/opt-portfolio/src"
    return env


_SECRET_OPTS = ("--db", "--dsn")


def _masked(cmd: list[str]) -> str:
    """``--db <DSN>``처럼 비밀값을 받는 옵션의 값을 가린 커맨드 문자열.

    ``timescale_dsn()``은 비밀번호를 포함하므로 그대로 찍으면 태스크 로그에
    평문으로 남는다(실측). Fernet Variables로 자격증명을 감춰둔 의미가 없어짐.
    """
    out: list[str] = []
    mask_next = False
    for arg in cmd:
        out.append("***" if mask_next else arg)
        mask_next = arg in _SECRET_OPTS
    return " ".join(out)


def run_collector(
    cmd: list[str],
    *,
    env: dict[str, str] | None = None,
    cwd: str = "/opt/airflow",
) -> None:
    """콜렉터를 실행하고 그 출력을 태스크 로그로 스트리밍한다.

    ``cwd``는 kr-quant 쪽 스크립트를 돌리는 DAG(weekly_price_adjust)가
    ``/opt/kr-quant``를 쓰므로 인자로 받는다.

    ``subprocess.run(cmd)``처럼 stdout을 넘겨주지 않으면 자식은 OS 레벨 fd 1에
    직접 쓰는데, Airflow의 캡처(``logging_mixin``)는 파이썬 레벨 ``sys.stdout``만
    가로채므로 콜렉터 출력이 통째로 유실된다 — ``earnings_backfill``이 3.5시간
    돌고도 로그를 19줄만 남기던 실측 원인. 콜렉터가 심어둔 진행 로그
    (``[2016Q1] 누적 rows=...`` 등)가 하나도 안 보여 장애 진단이 불가능했다.
    PIPE로 받아 ``print``로 되찍어야 로그에 들어간다.

    ``check=True``와 동일하게 실패 시 ``CalledProcessError``를 던진다.
    """
    print(f"$ {_masked(cmd)}", flush=True)
    # 스트리밍·버퍼링·줄 단위 마스킹은 collectors.proc 이 담당한다 — 콜렉터 쪽에서도
    # 같은 보장이 필요해서(sharadar_build 가 opt-factor 를 띄운다) 그 레이어로 내렸다.
    rc = stream_subprocess(cmd, env=env, cwd=cwd)
    if rc != 0:
        # 마스킹된 cmd로 던진다 — CalledProcessError.__str__가 cmd를 그대로 찍어
        # 원본을 넘기면 실패할 때마다 트레이스백에 DSN 비밀번호가 남는다(실측).
        raise subprocess.CalledProcessError(rc, _masked(cmd))
