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
