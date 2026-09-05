"""Credential loading and authenticated client construction.

Keys are read from the environment first, then from a ``.env`` file at the
repo root (never committed). Nothing here hardcodes secrets.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # 런타임 import 는 make_api 안에서 — 아래 주석 참고.
    from kiwoom_client import KiwoomAPI


#: DART API 키를 담는 환경변수 이름들 — **우선순위 순서**다.
#: 키마다 하루 20,000콜 한도가 따로 붙으므로, 앞 키가 020(한도)을 맞으면 다음
#: 키로 넘어간다. ``collectors.dart_earnings.collect_keys`` 가 subprocess 안에서
#: 이 이름들을 읽고, ``dags/_common.dart_env`` 가 Airflow Variables 에서 꺼내
#: 같은 이름으로 주입한다 — **두 쪽이 한 쌍이라 목록이 갈라지면 조용히 도달하지
#: 않는다.** 실제로 그런 상태였다: 주입 쪽이 _2 까지만 알아서 3번 키가 콜렉터에
#: 영원히 닿지 않았고, 한도가 60,000/일이 아니라 40,000/일이었다. 그래서 목록은
#: 여기 하나뿐이다.
DART_KEY_ENV_VARS: tuple[str, ...] = (
    "DART_API_KEY", "DART_API_KEY_2", "DART_API_KEY_3", "DART_API_KEY_4",
)


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def load_keys(env_path: str | Path | None = None) -> tuple[str, str]:
    """Return (app_key, app_secret) from env vars or ``.env``.

    Raises:
        RuntimeError: if either credential is missing.
    """
    app_key = os.environ.get("KIWOOM_APP_KEY", "")
    app_secret = os.environ.get("KIWOOM_APP_SECRET", "")

    path = Path(env_path) if env_path else (repo_root() / ".env")
    if (not app_key or not app_secret) and path.exists():
        for line in path.read_text().splitlines():
            line = line.strip()
            if line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            value = value.strip().strip('"').strip("'")
            if key.strip() == "KIWOOM_APP_KEY" and not app_key:
                app_key = value
            elif key.strip() == "KIWOOM_APP_SECRET" and not app_secret:
                app_secret = value

    if not app_key or not app_secret:
        raise RuntimeError(
            "KIWOOM_APP_KEY / KIWOOM_APP_SECRET 가 환경변수나 .env 에 없습니다. "
            ".env.example 를 참고해 .env 를 채우세요."
        )
    return app_key, app_secret


def make_api(is_mock: bool = True, *, login: bool = True, **kwargs) -> "KiwoomAPI":
    """Build a KiwoomAPI client (per-TR rate limiting on by default) and log in.

    Args:
        is_mock: Use the mock server (True) or production (False).
        login: Issue an access token immediately.
        **kwargs: Forwarded to ``KiwoomAPI`` (e.g. rate_limit, max_retries).
    """
    # 지연 import: 이 모듈에는 mask_dsn/load_keys 처럼 키움과 무관한 헬퍼도 있는데,
    # 최상단에서 클라이언트를 끌어오면 네이버·DART 전용 수집기와 그 테스트까지
    # kiwoom_client 설치를 요구하게 된다.
    from kiwoom_client import KiwoomAPI

    app_key, app_secret = load_keys()
    api = KiwoomAPI(app_key=app_key, app_secret=app_secret, is_mock=is_mock, **kwargs)
    if login:
        api.login()
    return api


_DSN_RE = re.compile(r"(?P<head>[a-zA-Z][a-zA-Z0-9+.-]*://[^:/@\s]+:)(?P<pw>[^@/\s]+)(?P<tail>@)")


def mask_dsn(dsn: str | None) -> str:
    """DSN의 비밀번호를 가린 표시용 문자열.

    콜렉터들이 시작 시 접속 대상을 `💾 {args.db}`로 찍는데, 그대로 두면
    비밀번호가 stdout에 남는다 — Airflow 태스크 로그로 흘러들어가고(2026-07-17
    실측), 터미널 스크롤백/CI 로그에도 남는다.
    """
    return _DSN_RE.sub(lambda m: f"{m['head']}***{m['tail']}", str(dsn or ""))


# `api_key=` / `apikey=` / `token=` 를 쿼리스트링에서 가린다. Sharadar 직판은 키를
# URL 파라미터로 받는데, requests 의 HTTPError 메시지가 `resp.url` 을 통째로
# 담으므로 4xx/5xx 한 번이면 트레이스백에 키가 평문으로 남는다.
_QUERY_SECRET_RE = re.compile(r"(?i)\b(api_?key|token)=([^&\s\"']+)")


def mask_secrets(text: str | None) -> str:
    """로그로 흘려보내기 전 DSN 비밀번호와 URL 쿼리의 API 키를 함께 가린다."""
    return _QUERY_SECRET_RE.sub(lambda m: f"{m[1]}=***", mask_dsn(text))
