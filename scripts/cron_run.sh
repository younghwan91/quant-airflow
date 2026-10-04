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
# `[alert.py] sent|logged-only` 마커로 알아보고 중복으로 보내지 않는다. `failed` 는
# 전달이 안 된 것이라(웹훅 429/5xx) 여기서 한 번 더 시도한다. 안 핸들러를 빼지 않는
# 이유: 그쪽은 **왜** 죽었는지 안다, 여기는 꼬리만 안다.
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
    if printf '%s\n' "$tail_out" | grep -qE '^\[alert\.py\] (sent|logged-only) '; then
        echo "[cron_run] $name rc=$rc — 안에서 이미 알렸다(마커), 중복 알림 생략"
    else
        printf '%s\n' "$tail_out" | "$REPO/scripts/alert.sh" error "$name 실패 (rc=$rc)"
    fi
fi
exit "$rc"
