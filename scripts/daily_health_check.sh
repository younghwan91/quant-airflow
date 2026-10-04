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

# ⚠️ 는 log 가 아니라 warn 으로 찍는다 — 같은 줄을 로그에 남기면서 모아뒀다가
# 스크립트 끝에서 **한 번에 한 건**으로 알린다(scripts/alert.sh). 실행당 1건인 이유:
# 리플리카 경보처럼 두 줄이 세트로 나오는 걸 쪼개 보내면 채널이 시끄럽고, 하루 두 번
# 도는 점검이라 같은 경보가 두 번 올 수 있는 건 허용한다(spec 2026-10-05 "범위 밖").
# URL 이 없는 동안은 ~/logs/quant-airflow/alerts.log 에만 쌓인다.
WARNINGS=()
warn() { log "⚠️ $*"; WARNINGS+=("$*"); }

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
    # 세 가지를 여기서 바로잡는다 — 셋 다 "매일 울리는 늑대소년" 유형이라
    # 주말·오전 건너뛰기와 같은 계보다(2026-09-23 실측).
    #
    # (1) **유니버스에서 상폐 종목을 뺀다.** stocks 는 키움 전종목 목록의
    #     upsert 라 상폐돼도 행이 안 지워진다 — 09-22 기준 26종목이 stocks 와
    #     delisted_stocks 양쪽에 있다(더존비즈온·현대홈쇼핑·신세계푸드 등
    #     6~9월 상폐분). 이것들은 앞으로 영원히 일봉이 안 들어오므로
    #     missing_today 가 상폐될 때마다 한 칸씩 커지는 누적 잡음이 된다
    #     (09-21 26 → 09-22 29). 빼고 나면 29 → 3 으로 떨어지고, 남는 3은
    #     "아직 delisted 리스트에 안 잡힌 거래정지" 라 실제로 볼 값이다.
    #
    # (2) **credit_balance 는 전 거래일로 묻는다.** daily_short_credit 은
    #     화~토 10:00 에 전날치를 받는다(공시가 T+1~2 지연) — 그런데 18:10
    #     점검이 '$today' 로 물어 **매 영업일 전종목(2651) 누락**으로 찍혔다.
    #     100% 오보였다. 거래일 달력은 따로 두지 않고 daily_bars 를 쓴다
    #     (report_theme_freshness 와 같은 규약) — 공휴일이 끼어도 저절로 맞는다.
    #
    # (3) **short_selling 은 종목 누락으로 못 센다.** KRX 는 그날 공매도가
    #     실제로 있었던 종목만 공시해서 전종목의 80% 언저리(2,076~2,255행)만
    #     나온다 — 전거래일 기준으로 고쳐도 422종목이 "누락" 으로 남는데 그게
    #     정상이다. 그래서 여기서 빼고 아래 report_short_selling 에서 행 수로 본다.
    ts_psql -c "
WITH univ AS (
    SELECT s.code FROM stocks s
     WHERE NOT EXISTS (SELECT 1 FROM delisted_stocks x WHERE x.code = s.code)
), prev AS (
    SELECT MAX(date) AS d FROM daily_bars WHERE date < '$today'
)
SELECT 'daily_bars' AS tbl, '$today' AS asof, COUNT(*) AS missing FROM univ s
    WHERE NOT EXISTS (SELECT 1 FROM daily_bars d WHERE d.code=s.code AND d.date='$today')
UNION ALL
SELECT 'supply_demand', '$today', COUNT(*) FROM univ s
    WHERE NOT EXISTS (SELECT 1 FROM supply_demand d WHERE d.code=s.code AND d.date='$today')
UNION ALL
SELECT 'credit_balance', (SELECT d::text FROM prev), COUNT(*) FROM univ s
    WHERE NOT EXISTS (SELECT 1 FROM credit_balance d WHERE d.code=s.code AND d.date=(SELECT d FROM prev))
UNION ALL
SELECT 'sector_index', '$today', COUNT(*) FROM (SELECT DISTINCT code FROM sector_index WHERE date >= current_date - 30) si
    WHERE NOT EXISTS (SELECT 1 FROM sector_index x WHERE x.code=si.code AND x.date='$today')
UNION ALL
SELECT 'shares_outstanding', '최근7일', COUNT(*) FROM univ s
    WHERE NOT EXISTS (SELECT 1 FROM shares_outstanding_history d WHERE d.code=s.code AND d.date >= current_date - interval '7 days');
" 2>&1 || warn "커버리지 점검 실패 (DB 연결 안 됨?)"

    # 표는 사람이 보지만 알림은 숫자가 있어야 한다. daily_bars 만 임계값을 둔다 —
    # 정상은 한 자리(상폐 제외 후 09-22 실측 3), 거래정지가 몰려도 수십이다. 100 을
    # 넘으면 "16:00 수집이 통째로 빠졌다"는 뜻이고, 그건 지금까지 표로만 찍히고
    # 끝났다. 다른 테이블 임계값은 다음 하위 프로젝트(DAG 실패 자가치유)의 몫이다.
    local bars_missing
    bars_missing=$(ts_psql -tAc "
SELECT COUNT(*) FROM stocks s
 WHERE NOT EXISTS (SELECT 1 FROM delisted_stocks x WHERE x.code = s.code)
   AND NOT EXISTS (SELECT 1 FROM daily_bars d WHERE d.code = s.code AND d.date = '$today')
" 2>/dev/null | tr -d '[:space:]')
    if [ -n "$bars_missing" ] && [ "$bars_missing" -ge 100 ] 2>/dev/null; then
        warn "daily_bars $today 누락 ${bars_missing}종목 — 16:00 수집이 통째로 빠졌을 가능성"
    fi
}

report_short_selling() {
    # 위 (3) 의 짝 — 종목 누락 대신 "전 거래일 행 수" 로 본다. 절대 임계값
    # 대신 최근 10거래일 중앙값의 50% 를 바닥으로 쓴다: 공매도 행 수는
    # 2,076~2,255 사이에서 평소에도 10% 가까이 출렁여 고정 임계값이면 튜닝을
    # 계속 해야 한다. 수집기가 죽거나(0행) 소스가 반쪽만 주는 경우를 잡는 게
    # 목적이지 일상 변동을 보려는 게 아니다.
    local row n med
    row=$(ts_psql -tAc "
WITH prev AS (SELECT MAX(date) AS d FROM daily_bars WHERE date < '$today'),
     recent AS (SELECT date, COUNT(*) c FROM short_selling
                 WHERE date >= (SELECT d FROM prev) - interval '20 days'
                   AND date <= (SELECT d FROM prev) GROUP BY 1)
SELECT coalesce((SELECT d::text FROM prev), ''),
       coalesce((SELECT c FROM recent WHERE date = (SELECT d FROM prev)), 0),
       coalesce((SELECT round(percentile_cont(0.5) WITHIN GROUP (ORDER BY c)) FROM recent), 0)
" 2>/dev/null)
    IFS='|' read -r asof n med <<< "$row"
    if [ -z "${asof:-}" ]; then
        warn "공매도 점검 실패 (DB 연결 안 됨?)"
    elif [ "$n" -eq 0 ] 2>/dev/null; then
        warn "공매도: $asof 행이 0 — daily_short_credit 이 안 돌았거나 소스가 막혔다"
    elif [ $((n * 2)) -lt "$med" ] 2>/dev/null; then
        warn "공매도: $asof ${n}행 — 최근 중앙값 ${med}행의 절반 미만이라 반쪽 수집 의심"
    else
        log "공매도: $asof ${n}행 (최근 중앙값 ${med}행)"
    fi
    return 0
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
        warn "오늘 실패한 태스크: $failed"
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
        warn "paused 라 안 도는 DAG: $paused  (안 돌릴 거면 schedule=None 으로 코드에 적을 것)"
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
        warn "복제 슬롯 조회 실패 (DB 연결 안 됨?)"
    else
        log "복제 슬롯: $slot"
        echo "$slot" | grep -q "active=f" && warn "복제 슬롯 비활성 — trader 리플리카가 스트리밍을 안 받고 있을 수 있다"
        echo "$slot" | grep -qi "wal_status=lost\|wal_status=extended" && warn "wal_status 이상 — WAL 세그먼트 유실 위험"
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
        warn "테마 스냅샷 신선도 점검 실패 (DB 연결 안 됨? theme_members/daily_bars 조회 불가)"
    elif [ "$gap" -gt 0 ] 2>/dev/null; then
        warn "theme_members: 최신 $theme_max · daily_bars 최신 $bars_max 보다 ${gap}일 묵었다"
    else
        log "theme_members 신선도: 최신 $theme_max (daily_bars $bars_max 와 일치)"
    fi
    return 0
}

report_coverage
# report_coverage 와 달리 주말·오전에도 돈다 — 전 거래일을 보는 점검이라
# "오늘 장이 섰나" 와 무관하고, 토 10:00 의 daily_short_credit 실행분(금요일치)도
# 그날 안에 확인된다.
report_short_selling
report_failures
report_paused
report_replication
report_theme_freshness
if [ ${#WARNINGS[@]} -gt 0 ]; then
    printf '%s\n' "${WARNINGS[@]}" | scripts/alert.sh warn "헬스체크 경보 ${#WARNINGS[@]}건 ($today)"
fi
exit 0
