#!/usr/bin/env bash
# 24/7 상시 구동 전환(2026-09-11) 이후 잃어버린 관측성을 되살리는 스크립트.
#
# wait_and_stop.sh 의 report_coverage/report_failures/report_paused 는
# **스택 종료 시에만** 실행됐다 — 24/7 로 바뀌어 스택이 이제 안 내려가므로
# 그 세 점검이 통째로 안 도는 채로 며칠이 지났다(daily_price_adjust·
# monthly_listed_shares_backfill 이 paused 로 6일 조용히 안 돈 전례와 같은
# 유형의 구멍). 이 스크립트는 셋을 shutdown 로직에서 떼어내 독립 실행되게 한다
# — 컨테이너를 내리지 않는다, 그냥 읽고 로그만 남긴다.
#
# 크론에서 하루 두 번 부른다(오전 창 마감 무렵 · 저녁 창 마감 무렵) — 창이라는
# 개념은 없어졌지만 "그 무렵 상태를 사람이 볼 수 있게" 라는 목적은 그대로다.
set -uo pipefail
cd "$(dirname "$0")/.."

today=$(date +%Y-%m-%d)
log() { echo "[$(date '+%F %T')] $*"; }

# .env 전체를 source 하지 않는다 — 필요한 키만 읽는다. -f2- 인 이유: 값에 '=' 가
# 들어간 키(base64 비밀번호 등)가 있어 -f2 로는 잘린다.
env_get() { grep "^$1=" .env | cut -d= -f2-; }
meta_user=$(env_get AIRFLOW_META_USER)
meta_db=$(env_get AIRFLOW_META_DB)
ts_user=$(env_get TIMESCALE_USER)
ts_db=$(env_get TIMESCALE_DB)
# simnode 의 PRIMARY 컨테이너(docker-compose.replica.yml — 이름만 replica).
# backup_to_gdrive.sh 와 같은 .env 키를 본다.
ts_container=$(env_get BACKUP_DB_CONTAINER)
ts_container=${ts_container:-quant-airflow-timescaledb-replica-1}

meta_q() {
    docker exec quant-airflow-airflow-meta-db-1 \
        psql -U "$meta_user" -d "$meta_db" -tAc "$1" 2>/dev/null
}

# 인자는 psql 옵션 그대로(-c "..." 또는 -tAc "...").
ts_psql() {
    docker exec "$ts_container" psql -U "$ts_user" -d "$ts_db" "$@"
}

report_coverage() {
    # 커버리지 6개 테이블은 전부 평일 장중 수집물이다. 주말엔 daily_bars 등이
    # 원래 0행이라 missing_today 가 전종목 수(2648)로 매주 늑대소년을 울린다
    # (2026-09-13 scalp-it-f4 실측). 토(6)/일(7)은 건너뛴다.
    local dow
    dow=$(date +%u)
    if [ "$dow" -ge 6 ]; then
        log "=== 커버리지 점검 ($today) === 주말이라 건너뜀 (dow=$dow)"
        return
    fi
    # 같은 이유로 오전 실행(11:35)도 건너뛴다 — daily_collection 이 16:00 이라
    # 그 전엔 daily_bars·supply_demand 가 매일 전종목(2648) 누락으로 찍혔다
    # (2026-09-14·15 11:35 로그 실측). 18:10 실행만 본다.
    if [ "$(date +%H)" -lt 17 ]; then
        log "=== 커버리지 점검 ($today) === 16:00 수집 전이라 건너뜀"
        return
    fi
    log "=== 커버리지 점검 ($today) ==="
    ts_psql -c "
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
SELECT 'sector_index', COUNT(*) FROM (SELECT DISTINCT code FROM sector_index WHERE date >= current_date - 30) si
    WHERE NOT EXISTS (SELECT 1 FROM sector_index x WHERE x.code=si.code AND x.date='$today')
UNION ALL
SELECT 'shares_outstanding', COUNT(*) FROM stocks s
    WHERE NOT EXISTS (SELECT 1 FROM shares_outstanding_history d WHERE d.code=s.code AND d.date >= current_date - interval '7 days');
" 2>&1 || log "커버리지 점검 실패 (DB 연결 안 됨?)"
}

report_failures() {
    local failed
    failed=$(meta_q "
SELECT string_agg(DISTINCT dag_id || '.' || task_id, ',')
  FROM task_instance
 WHERE state = 'failed'
   AND start_date >= (date_trunc('day', now() AT TIME ZONE 'Asia/Seoul')) AT TIME ZONE 'Asia/Seoul';" | tr -d '[:space:]')
    if [ -z "$failed" ]; then
        log "오늘 실패한 태스크: 없음"
    else
        log "⚠️ 오늘 실패한 태스크: $failed"
    fi
}

report_paused() {
    local paused
    paused=$(meta_q "
SELECT string_agg(dag_id, ',' ORDER BY dag_id)
  FROM dag
 WHERE is_paused AND is_active;" | tr -d '[:space:]')
    if [ -z "$paused" ]; then
        log "paused 인 DAG: 없음"
    else
        log "⚠️ paused 라 안 도는 DAG: $paused  (안 돌릴 거면 schedule=None 으로 코드에 적을 것)"
    fi
}

report_replication() {
    # docker-compose.replica.yml 의 재발 방지 메모: "복제 지연/슬롯 비활성
    # 상태를 감시하는 알림이 없다" — trader 리플리카가 WAL 재시딩까지 간
    # 2026-09-11~12 사고의 근본 원인이었다. 슬롯이 죽어 있거나(active=f) WAL
    # 이 안전 여유 없이 쌓이면 다음 사고 전에 여기서 잡는다.
    local slot
    slot=$(ts_psql -tAc \
        "SELECT slot_name || ':active=' || active || ':wal_status=' || wal_status FROM pg_replication_slots;" 2>/dev/null)
    if [ -z "$slot" ]; then
        log "⚠️ 복제 슬롯 조회 실패 (DB 연결 안 됨?)"
    else
        log "복제 슬롯: $slot"
        echo "$slot" | grep -q "active=f" && log "⚠️ 복제 슬롯 비활성 — trader 리플리카가 스트리밍을 안 받고 있을 수 있다"
        echo "$slot" | grep -qi "wal_status=lost\|wal_status=extended" && log "⚠️ wal_status 이상 — WAL 세그먼트 유실 위험"
    fi
    return 0
}

report_theme_freshness() {
    # scalp-it-7c 설계(2026-09-13, scalp-it-f4 경유 전달) — 09-11 저녁
    # scalp-theme-snapshot 이 레포 이전 도중 DSN 을 못 찾아 빈 값으로 exit 0
    # 나며 조용히 죽었던 사고의 재발 감지. theme_members 가 하루라도 밀리면
    # 월요일 08:55 짝꿍 선정(cli_pair_detect.py)이 묵은 테마로 대장을 고른다.
    #
    # 거래일 판정에 별도 캘린더/공휴일 목록을 쓰지 않는다 — daily_bars 자체가
    # 거래일에만 행이 생기는 사실상의 거래일 달력이라, "오늘이 거래일인가"를
    # 묻는 대신 daily_bars 최신일과 theme_members 최신일의 **차이**만 본다.
    # 공휴일 다음날도 bars_max 가 같이 안 올라가 있으면 gap 이 그대로 0이라
    # 오탐이 원천적으로 안 난다. daily_bars 자체가 밀리는 경우는 report_coverage
    # 가 따로 잡는다(이중 방어).
    local row theme_max bars_max gap
    row=$(ts_psql -tAc "
WITH m AS (SELECT (SELECT max(snapshot_date)::date FROM theme_members) AS t,
                  (SELECT max(date)::date FROM daily_bars) AS b)
SELECT coalesce(t::text, ''), coalesce(b::text, ''), coalesce((b - t)::text, '') FROM m
" 2>/dev/null)
    IFS='|' read -r theme_max bars_max gap <<< "$row"
    if [ -z "$theme_max" ] || [ -z "$bars_max" ] || [ -z "$gap" ]; then
        log "⚠️ 테마 스냅샷 신선도 점검 실패 (DB 연결 안 됨? theme_members/daily_bars 조회 불가)"
    elif [ "$gap" -gt 0 ] 2>/dev/null; then
        log "⚠️ theme_members: 최신 $theme_max · daily_bars 최신 $bars_max 보다 ${gap}일 묵었다"
    else
        log "theme_members 신선도: 최신 $theme_max (daily_bars $bars_max 와 일치)"
    fi
    return 0
}

report_coverage
report_failures
report_paused
report_replication
report_theme_freshness
exit 0
