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

from airflow.models import Variable

# collectors/는 /opt/airflow 밑에 있지만, Airflow는 DAG 파싱 시 각 dag 파일
# 자신의 디렉터리(dags/)만 sys.path에 넣는다 — /opt/airflow 자체는 자동으로
# 안 잡힌다. 그래서 task 본문에서는 다들 sys.path.insert(0, "/opt/airflow")를
# 직접 해왔다. 여기서는 모듈 로드 시점(=DAG 파싱
# 시점)에 필요하므로 import 전에 미리 넣는다.
sys.path.insert(0, "/opt/airflow")

from collectors.proc import stream_subprocess  # noqa: E402


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


#: collectors.dart_earnings.collect_keys() 가 읽는 이름들과 **한 쌍이다.**
#: 한쪽에만 키를 추가하면 조용히 도달하지 않는다 — 실제로 그런 상태였다:
#: docker-compose 는 3개를 Variables 에 시딩하고 collect_keys 는 _4 까지 읽는데,
#: 여기서 _2 까지만 주입해 **3번 키가 콜렉터에 영원히 닿지 않았다.** 한도가
#: 60,000/일이 아니라 40,000/일이었고 EARNINGS_PIPELINE_PLAN.md 의 산수와도
#: 어긋나 있었다.
_DART_KEY_VARS = ("DART_API_KEY_2", "DART_API_KEY_3", "DART_API_KEY_4")


def dart_env() -> dict[str, str]:
    # DART 키는 Fernet 암호화 Variables에만 있음 — 수집 subprocess에만 주입.
    # 보조키가 있으면 함께 주입 → collector가 일한도(020) 시 다음 키로 로테이션.
    env = os.environ.copy()
    env["DART_API_KEY"] = Variable.get("DART_API_KEY")
    for name in _DART_KEY_VARS:
        value = Variable.get(name, default_var=None)
        if value:
            env[name] = value
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
