"""DART 공통 — 키 로테이션과 상태 코드 하나로.

DART 는 키마다 하루 20,000콜 한도가 따로 붙고, 소진되면 응답 ``status`` 가
``"020"`` 이 된다. 키를 여러 개 두고 소진 시 다음 키로 넘어가는 게 이 레포의
표준 대응인데, **그 로직이 네 군데에 각각 쓰여 있었다**:

- ``dart_earnings._fetch_with_rotation``     (분기 재무, 종목별)
- ``dart_earnings._fetch_multi_with_rotation`` (분기 재무, 100종목 묶음)
- ``dart_earnings.load_corp_map_with_rotation`` (corp_code 맵) — 여기는 예외
  메시지를 **문자열로 매칭**했다. 메시지 템플릿에 "한도초과(020)/키오류(010)"
  안내문이 늘 박혀 있어 ``"020" in str(e)`` 가 010 에러에도 참이 되는 실측
  버그가 있었고, 그래서 ``"status='020'"`` 이라는 더 좁은 문자열로 고쳤다 —
  즉 **docstring 을 고치면 제어흐름이 바뀌는 상태**였다.
- ``dart_shares.shares_series`` — 로테이션이 **아예 없었다**. `keys[0]` 하나만
  써서 하루 한도가 20,000 으로 묶였고, 상장 종목 과거 주식수 백필(2,595종목 ×
  ~9.5콜 ≈ 24,650콜)이 한도를 넘어 2일에 나눠 돌려야 했다. 그 분할이
  ``ORDER BY code`` 와 겹쳐 **시대별로 기울어진 중간 상태**를 만들었다
  (2026-08-27: 2023~24년 상장분의 87%가 미처리로 남았다).

새 엔드포인트를 붙일 때마다 이 루프를 또 베끼는 대신, 여기 하나만 쓴다.
"""

from __future__ import annotations

from typing import Callable

#: 일한도 소진. DART 가 응답 ``status`` 로 준다.
QUOTA_EXHAUSTED = "020"


class DartQuotaExhausted(Exception):
    """모든 키가 일한도(020)를 소진했다.

    ``load_corp_map`` 처럼 payload 가 아니라 예외로 실패를 알리는 경로가 이걸
    던지면, 호출부가 메시지 문자열을 뒤지지 않고 타입으로 구분할 수 있다.
    """


def rotate_on_quota(
    fetch: Callable[[str], dict],
    keys: list[str],
    ki: list[int],
    *,
    label: str = "",
) -> dict:
    """``fetch(key)`` 를 부르고, ``status=="020"`` 이면 다음 키로 넘어가 다시 부른다.

    Args:
        fetch: 키 하나를 받아 DART payload(dict)를 돌려주는 호출. 실패를 빈 dict
            로 주는 기존 관례를 그대로 받는다(빈 dict 는 status 가 없으므로
            로테이션 대상이 아니다 — 네트워크 실패로 남은 키를 태우지 않는다).
        keys: 우선순위 순 키 목록.
        ki: 현재 키 인덱스를 담은 1칸 리스트. **호출 간에 유지된다** — 한 번
            소진된 키로 매번 되돌아가면 종목마다 한도 오류를 한 번씩 더 맞는다.
        label: 로그에 붙일 설명(어느 엔드포인트인지).

    Returns:
        마지막 payload. 모든 키가 소진됐으면 ``status=="020"`` 인 payload 를
        그대로 돌려준다 — 호출부가 "없음"과 "한도"를 구분할 수 있어야 하므로
        여기서 빈 dict 로 뭉개지 않는다.
    """
    payload = fetch(keys[ki[0]])
    while payload.get("status") == QUOTA_EXHAUSTED and ki[0] + 1 < len(keys):
        ki[0] += 1
        suffix = f" ({label})" if label else ""
        print(f"DART 키 일한도(020) 도달 → 키{ki[0] + 1}로 로테이션{suffix}", flush=True)
        payload = fetch(keys[ki[0]])
    return payload


def rotate_on_quota_raising(
    fetch: Callable[[str], object],
    keys: list[str],
) -> object:
    """예외로 실패를 알리는 경로용 로테이션.

    ``fetch`` 가 :class:`DartQuotaExhausted` 를 던지면 다음 키로 넘어간다. 그
    외 예외는 그대로 올린다(키를 바꿔도 안 풀리는 오류에 남은 키를 태우지
    않는다). 전부 소진되면 마지막 예외를 다시 던진다.
    """
    last: Exception | None = None
    for key in keys:
        try:
            return fetch(key)
        except DartQuotaExhausted as e:
            last = e
    raise last if last else DartQuotaExhausted("DART 키가 하나도 없다")
