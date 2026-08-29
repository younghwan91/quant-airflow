#!/usr/bin/env bash
# 오늘 돌아야 할 DAG가 전부 끝나면 스택을 내린다 — 수집이 끝났는데 서버가
# 몇 시간씩 떠 있지 않도록 기존 22:00 고정 종료 cron을 대체한 스크립트다.
# 런이 걸려서 안 끝나는 경우를 대비해 22:00 KST 안전장치는 유지한다.
#
# **대기 대상을 Airflow에게 직접 묻는 이유(2026-07-17 재작성):**
# 이전 버전은 대기할 DAG를 셸 배열에 하드코딩했다(daily_collection_catchup,
# daily_collection, daily_short_credit, weekly_listed_shares). 그런데
# `is_done()`은 "오늘 런이 없으면 아직 안 끝난 것"으로 판정했고,
# daily_collection_catchup은 paused여서 런을 영원히 만들지 않았다. 결과적으로
# 루프가 절대 완료되지 못하고 **매일 22:00 안전장치로만 종료**됐다 — 이 스크립트가
# 대체하려던 바로 그 고정 종료로 조용히 되돌아가 있었다(실측: 수집이 17:10에
# 끝난 날도 종료는 22:00:47).
#
# 하드코딩 목록은 DAG를 pause/unpause하거나 추가할 때마다 이 파일과 어긋난다.
# 그래서 "오늘 더 뜰 런이 있는가 / 지금 도는 런이 있는가"를 Airflow 메타DB에
# 직접 묻는다. paused DAG는 자동으로 빠지고, 새 DAG·월간 DAG도 그대로 반영된다.
#
# **지평선을 인자로 받는 이유(2026-08-25):** 가동 521분 중 작업이 138분(27%)이고
# 나머지 380분(73%)이 통째로 유휴였다 — 오전 수집(10:00~11:00)과 저녁 수집
# (16:00~) 사이에 5시간이 비어 있다. 오전 DAG는 전날 확정 데이터라 미룰 수 없고
# 저녁 DAG는 장 마감(15:30) 이후에만 데이터가 나와 앞당길 수 없다. 스케줄을
# 붙이는 건 원리적으로 불가능하고, **스택을 하루 두 번 띄우는 게 유일한 답이다.**
# 그래서 "오늘 자정까지" 고정이던 지평선을 인자로 받는다:
#
#   ./scripts/wait_and_stop.sh            # 예전과 동일 — 오늘 자정까지
#   ./scripts/wait_and_stop.sh --until 11:30   # 오전 창: 11:30 까지 예정된 것만
set -euo pipefail
cd "$(dirname "$0")/.."

POLL_INTERVAL=60
DEADLINE=$(date -d "22:00" +%s)

# 지평선 — 이 시각까지 생성될 런만 "오늘 할 일"로 센다. 기본은 오늘 자정.
HORIZON_SQL="(date_trunc('day', now() AT TIME ZONE 'Asia/Seoul') + interval '1 day') AT TIME ZONE 'Asia/Seoul'"
horizon_label="오늘 자정"
if [ "${1:-}" = "--until" ] && [ -n "${2:-}" ]; then
    HORIZON_SQL="(date_trunc('day', now() AT TIME ZONE 'Asia/Seoul') + interval '$2') AT TIME ZONE 'Asia/Seoul'"
    horizon_label="$2"
    # 안전장치는 지평선 **+2시간**이다. 지평선과 같게 두면 정상적으로 도는
    # 48분짜리 런을 한창일 때 잘라버린다 — 안전장치는 걸린 런을 위한 것이지
    # 창을 닫는 수단이 아니다. 정상 종료는 아래 루프가 "할 일 0" 을 보고 한다.
    # `date -d "11:30 + 2 hours"` 는 19:30 을 준다(파싱이 우리 뜻과 다르다).
    # 초 단위로 더한다.
    DEADLINE=$(( $(date -d "$2" +%s) + 7200 ))
fi

today=$(date +%Y-%m-%d)

log() { echo "[$(date '+%F %T')] $*"; }

meta_user=$(grep '^AIRFLOW_META_USER' .env | cut -d= -f2)
meta_db=$(grep '^AIRFLOW_META_DB' .env | cut -d= -f2)

# 실패 시 빈 문자열 → 호출부에서 "판단 불가"로 보고 계속 대기(안전장치까지).
# 조회가 안 된다고 스택을 내려버리면 수집 중인 런이 잘리므로, 모르면 기다린다.
meta_q() {
    docker compose exec -T airflow-meta-db \
        psql -U "$meta_user" -d "$meta_db" -tAc "$1" 2>/dev/null | tr -d '[:space:]'
}

# 지금 돌고 있거나 큐에 있는 런 수. 0이어야 아무것도 안 자르고 내릴 수 있다.
#
# 날짜 조건이 붙은 이유: 22:00 안전장치에 잘린 런은 state='running' 인 채로
# 메타DB에 남는다. 날짜를 안 보면 그 유령 한 건이 **다음 날부터 영원히** 조기
# 종료를 막아 매일 22:00까지 뜨게 된다.
#
# **`start_date` 만 보면 안 된다 (2026-08-29 실측 사고).** 방금 만들어진
# state='queued' 런은 `start_date` 가 NULL 이다 — 아직 시작을 안 했으니까.
# NULL 은 `> now() - interval '1 day'` 에 안 걸리므로 그 런은 "도는 게 없다"로
# 세어진다. 그날 토요일 저녁 창에서 정확히 그렇게 됐다:
#
#   17:20:02  스택 기동
#   17:30:00  스케줄러가 daily_sharadar 런 생성(queued, start_date=NULL)
#             → 그 순간 next_dagrun_create_after 가 다음 화요일로 넘어가
#               pending 도 0 이 된다
#   17:30:03  running=0, pending=0 → "모두 완료" 로 스택 종료
#
# 첫 폴에서 3초 만에 창을 닫아 **금요일 미국장 드롭을 통째로 놓쳤다.** 창이
# DAG 스케줄 시각과 겹칠 때만 나는 사고라 평일(16:05 기동, 16:00 DAG)에서는
# 안 났고, 그래서 토요일 저녁 창을 처음 켠 날 바로 터졌다.
#
# `queued_at` 으로 폴백한다 — 큐에 들어간 시각이라 큐 상태에서도 NULL 이 아니다.
running_runs() {
    meta_q "SELECT count(*) FROM dag_run
             WHERE state IN ('running','queued')
               AND coalesce(start_date, queued_at) > now() - interval '1 day';"
}

# 오늘 안에 더 생성될 런이 있는 unpaused DAG 수. next_dagrun_create_after가
# 오늘 자정(KST) 이전이면 아직 오늘 할 일이 남았다는 뜻. schedule=None(수동)
# DAG는 next_dagrun_create_after가 NULL이라 자동 제외된다.
pending_dags() {
    meta_q "
SELECT count(*) FROM dag
 WHERE NOT is_paused
   AND is_active
   AND next_dagrun_create_after IS NOT NULL
   AND next_dagrun_create_after < $HORIZON_SQL;"
}

# 아직 안 끝난 DAG 이름 — 로그용.
pending_names() {
    meta_q "
SELECT string_agg(dag_id, ',') FROM dag
 WHERE NOT is_paused
   AND is_active
   AND next_dagrun_create_after IS NOT NULL
   AND next_dagrun_create_after < $HORIZON_SQL;"
}

# 종목별 테이블은 자체 일수 상한이 있어(supply_demand/credit/short ~100d,
# sector_index ~10d) daily_bars처럼 2024년까지 백필되지 않는다 — 여기서는 각
# 테이블에 *오늘* 행이 없는 종목이 몇 개인지만 보고해서, 2026-07-08의
# 트랜잭션 연쇄 실패(473종목 조용히 누락) 같은 게 로그에 남게 한다.
report_coverage() {
    local user db
    user=$(grep '^TIMESCALE_USER' .env | cut -d= -f2)
    db=$(grep '^TIMESCALE_DB' .env | cut -d= -f2)
    log "=== 커버리지 점검 ($today) ==="
    docker compose exec -T timescaledb psql -U "$user" -d "$db" -c "
SELECT 'daily_bars' AS tbl, COUNT(*) AS missing_today FROM stocks s
    WHERE NOT EXISTS (SELECT 1 FROM daily_bars d WHERE d.code=s.code AND d.date='$today')
UNION ALL
SELECT 'supply_demand', COUNT(*) FROM stocks s
    WHERE NOT EXISTS (SELECT 1 FROM supply_demand d WHERE d.code=s.code AND d.date='$today')
UNION ALL
SELECT 'credit_balance', COUNT(*) FROM stocks s
    WHERE NOT EXISTS (SELECT 1 FROM credit_balance d WHERE d.code=s.code AND d.date='$today')
UNION ALL
SELECT 'short_selling', COUNT(*) FROM stocks s
    WHERE NOT EXISTS (SELECT 1 FROM short_selling d WHERE d.code=s.code AND d.date='$today')
UNION ALL
SELECT 'sector_index', COUNT(*) FROM (SELECT DISTINCT code FROM sector_index) si
    WHERE NOT EXISTS (SELECT 1 FROM sector_index x WHERE x.code=si.code AND x.date='$today')
UNION ALL
SELECT 'shares_outstanding', COUNT(*) FROM stocks s
    WHERE NOT EXISTS (SELECT 1 FROM shares_outstanding_history d WHERE d.code=s.code AND d.date >= current_date - interval '7 days');
" 2>&1 || log "커버리지 점검 실패 (DB 연결 안 됨?)"
}

# 오늘 실패한 태스크 — **"완료"와 "성공"은 다르다.**
#
# 이 스크립트의 종료 조건은 "오늘 예정된 런이 더 없고 도는 것도 없다" 이지,
# "다 성공했다" 가 아니다. 실패한 런도 끝난 런이라 pending 에서 빠지므로,
# 전부 빨간불이어도 로그에는 `예정 DAG 모두 완료 — 컨테이너 종료` 가 찍힌다.
#
# 실제로 그렇게 지나갔다: 2026-08-27, daily_earnings 가 재시도까지 두 번 실패한
# (TypeError 로 즉사) 상태에서 18:24 에 "모두 완료" 로 종료됐다. 커버리지 점검의
# 6개 테이블에 earnings 가 없어서 거기서도 안 잡혔다. 로그만 보면 정상이었다.
#
# 커버리지 점검(종목 단위 결측)으로는 이걸 못 대체한다 — earnings 는 비수기에
# 0행이 정상이고, consensus 는 커버리지 있는 ~650종목만 대상이라 "전 종목 대비
# 결측"이 항상 크게 나온다. 둘 다 결측 수로는 정상/이상을 가를 수 없다.
# 그래서 테이블이 아니라 **Airflow 의 태스크 상태**를 직접 본다.
report_failures() {
    local failed
    # 구분자에 공백을 쓰지 않는다 — meta_q 가 결과의 모든 공백을 지운다.
    failed=$(meta_q "
SELECT string_agg(DISTINCT dag_id || '.' || task_id, ',')
  FROM task_instance
 WHERE state = 'failed'
   AND start_date >= (date_trunc('day', now() AT TIME ZONE 'Asia/Seoul')) AT TIME ZONE 'Asia/Seoul';")
    if [ -z "$failed" ]; then
        log "오늘 실패한 태스크: 없음"
    else
        log "⚠️ 오늘 실패한 태스크: $failed"
    fi
}

shutdown() {
    report_coverage
    report_failures
    log "$1"
    # **timescaledb 를 내리지 않는다.** 이 컨테이너는 이 레포 전용이 아니다 —
    # scalp-it 의 장중 틱 수집이 같은 DB를 쓰고, crontab 의 db_guard.sh 가
    # 평일 08:00~15:55 5분마다 살아 있는지 지킨다. 예전처럼 `docker compose stop`
    # 으로 전부 내리면, 오전 창을 11:30에 닫는 순간 **장중에 남의 수집 DB를
    # 죽인다.** 종료 대상은 airflow 4종(스케줄러·웹서버·init·메타DB)으로 좁힌다 —
    # 메타DB 는 airflow 전용이라 같이 내려도 되고, 실측 확인했다(첫 창 종료 때
    # 목록에서 빠져 있어 24시간 떠 있었다).
    # webserver 는 `profiles: ["ui"]` 라 안 떠 있을 수 있다 — 그때 이름을 그냥
    # 넘기면 compose 가 에러를 내므로 `--profile ui` 로 인식시킨다(안 떠 있으면
    # 무해한 no-op).
    docker compose --profile ui stop \
        airflow-scheduler airflow-webserver airflow-init airflow-meta-db
    exit 0
}

log "대기 시작 — ${horizon_label}까지 예정된 unpaused DAG가 모두 끝나면 종료"

# "할 일 0" 을 **연속 두 번** 봐야 내린다. 한 번으로 내리면 스케줄러가 런을
# 만들기 직전의 찰나가 그대로 종료 근거가 된다 — `running_runs` 주석의 그 사고는
# NULL `start_date` 가 원인이었지만, 창 기동 시각과 DAG 스케줄 시각이 겹치는 한
# "아직 안 만들어졌다" 도 같은 모양으로 0/0 을 만든다. 폴 간격만큼(60초) 늦게
# 내리는 게 창 하나를 통째로 날리는 것보다 싸다.
idle_polls=0
while :; do
    running=$(running_runs)
    pending=$(pending_dags)

    if [ -n "$running" ] && [ -n "$pending" ]; then
        if [ "$running" -eq 0 ] && [ "$pending" -eq 0 ]; then
            idle_polls=$((idle_polls + 1))
            if [ "$idle_polls" -ge 2 ]; then
                shutdown "${horizon_label}까지 예정 DAG 모두 완료 — 컨테이너 종료"
            fi
            log "할 일 0 (${idle_polls}/2) — 한 번 더 확인하고 내린다"
        else
            idle_polls=0
        fi
        log "진행 중 런=$running, ${horizon_label}까지 남은 DAG=$pending ($(pending_names))"
    else
        # 메타DB 조회 실패(기동 중 등) — 모르면 기다린다.
        log "메타DB 조회 불가 — 재시도"
    fi

    if [ "$(date +%s)" -ge "$DEADLINE" ]; then
        shutdown "안전장치 시각 도달 — 미완료인 채로 종료"
    fi

    sleep "$POLL_INTERVAL"
done
