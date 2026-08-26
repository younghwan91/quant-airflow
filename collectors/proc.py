"""자식 프로세스 실행 + 출력 스트리밍 — 로그 유실과 시크릿 유출을 한 곳에서 막는다.

``dags/_common.run_collector`` 와 ``collectors.sharadar_build.build`` 가 같은 다섯
가지 성질(PIPE, ``stderr=STDOUT``, ``bufsize=1``, ``PYTHONUNBUFFERED``, 줄 단위
``mask_secrets``)을 각자 구현하고 있었다. ``_common`` 은 ``dags/`` 아래에 있고
``airflow.models.Variable`` 을 import 하므로 **콜렉터 쪽에서 구조적으로 재사용할 수
없었고**, 그래서 사본이 생겼다. 즉 "콜렉터를 새로 추가해도 자동으로 보호된다" 는
마스킹 보장이 DAG 에서 띄운 자식에게만 성립했다.

레이어 방향을 바로잡아 여기(collectors)로 내린다 — DAG 는 배선, 콜렉터는 동작.
``_common.run_collector`` 는 이제 자격증명 주입과 커맨드 에코만 얹는 얇은 껍데기다.
"""

from __future__ import annotations

import os
import subprocess

from .config import mask_secrets


def stream_subprocess(
    cmd: list[str],
    *,
    env: dict[str, str] | None = None,
    cwd: str | None = None,
    prefix: str = "",
) -> int:
    """``cmd`` 를 돌리며 출력을 줄 단위로 되찍고 반환코드를 돌려준다.

    ``subprocess.run(cmd)`` 처럼 stdout 을 넘겨주지 않으면 자식은 OS 레벨 fd 1 에
    직접 쓰는데, Airflow 의 캡처(``logging_mixin``)는 파이썬 레벨 ``sys.stdout`` 만
    가로채므로 자식 출력이 통째로 유실된다 — ``earnings_backfill`` 이 3.5시간 돌고도
    로그를 19줄만 남기던 실측 원인이다. PIPE 로 받아 ``print`` 로 되찍어야 한다.

    ``PYTHONUNBUFFERED`` + ``bufsize=1``: 자식의 stdout 이 파이프면 파이썬은 tty 와
    달리 블록 버퍼링을 한다. 강제로 라인 버퍼링시켜야 긴 잡의 진행상황이 끝날 때
    몰아서가 아니라 실시간으로 보인다.

    모든 줄은 :func:`.config.mask_secrets` 를 통과한다 — DSN 비밀번호와 URL 쿼리의
    ``api_key=`` 를 가린다(Sharadar 는 키를 쿼리로 받고, requests 의 HTTPError 는
    실패한 URL 을 통째로 메시지에 담는다).

    Args:
        prefix: 되찍는 줄 앞에 붙일 들여쓰기/표식.

    Returns:
        자식의 반환코드. 실패 처리(예외 종류)는 호출부가 정한다.
    """
    proc = subprocess.Popen(
        cmd,
        cwd=cwd,
        env={**(os.environ if env is None else env), "PYTHONUNBUFFERED": "1"},
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,  # 실패 원인이 stderr 로만 나오는 자식이 있어 합류시킨다
        text=True,
        bufsize=1,  # 라인 버퍼
    )
    with proc:
        assert proc.stdout is not None
        for line in proc.stdout:
            print(prefix + mask_secrets(line.rstrip()), flush=True)
    return proc.returncode
