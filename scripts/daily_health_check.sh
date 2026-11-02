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
    #
    # 평일 휴장일(한글날·성탄절 등)엔 daily_bars 에 오늘 행이 원래 없어 전종목이
    # 누락으로 보인다 — 그대로 두면 연 15번쯤 "수집 실패" 오보다. 그렇다고
    # MAX(date) < today 면 건너뛰는 식으로 막으면 **수집이 통째로 실패한 날도** 같이
    # 조용해진다(그날도 MAX 는 어제다). 휴장과 실패를 가르는 건 같은 DB 의 ticks 다:
    # scalp-it 이 장중 실시간으로 쌓으므로 오늘 틱이 있으면 장이 열린 날이고, 그런데
    # 일봉이 없으면 수집 실패다. 휴장일엔 틱도 없어 조용히 넘어간다. 공휴일 달력을
    # 따로 안 두는 이 스크립트의 규약(report_theme_freshness 참고)과도 맞다.
    local bars_missing market_open
    market_open=$(ts_psql -tAc "
SELECT EXISTS (SELECT 1 FROM ticks WHERE ts >= '$today'::date AND ts < '$today'::date + 1)
" 2>/dev/null | tr -d '[:space:]')
    bars_missing=$(ts_psql -tAc "
SELECT COUNT(*) FROM stocks s
 WHERE NOT EXISTS (SELECT 1 FROM delisted_stocks x WHERE x.code = s.code)
   AND NOT EXISTS (SELECT 1 FROM daily_bars d WHERE d.code = s.code AND d.date = '$today')
" 2>/dev/null | tr -d '[:space:]')
    if [ "$market_open" = "t" ] && [ -n "$bars_missing" ] && [ "$bars_missing" -ge 100 ] 2>/dev/null; then
        warn "daily_bars $today 누락 ${bars_missing}종목 — 오늘 틱은 있는데 일봉이 없다, 16:00 수집 실패 의심"
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
    # 2026-10-07 trader 처분으로 리플리카와 trader_replica 슬롯을 없앴다 — 슬롯 0개가 정상이다.
    # 조회 실패(종료코드)와 0행을 구분한다. 남은 슬롯이 있으면 그건 아무도 안 받는 슬롯이라
    # WAL 을 무한정 붙잡는다 — active=f 경고가 그걸 잡는다.
    local slot
    if ! slot=$(ts_psql -tAc \
        "SELECT slot_name || ':active=' || active || ':wal_status=' || wal_status FROM pg_replication_slots;" 2>/dev/null); then
        warn "복제 슬롯 조회 실패 (DB 연결 안 됨?)"
    elif [ -z "$slot" ]; then
        log "복제 슬롯: 없음 (단일 호스트)"
    else
        log "복제 슬롯: $slot"
        echo "$slot" | grep -q "active=f" && warn "복제 슬롯 비활성 — 받는 리플리카가 없는데 WAL 을 붙잡고 있다 (pg_drop_replication_slot 검토)"
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

# 드라이브 백업(19:00 전체 경로)이 마지막으로 성공한 시각. 2026-10-24~31 에
# 스테이징 디렉터리가 사라져 백업이 8일 연속 실패했는데, 알림이 alerts.log 에만
# 쌓이고 이 점검은 백업을 안 봐서 아무도 몰랐다. 표시는 backup_to_gdrive.sh 의
# EXIT 트랩이 BACKUP_ONLY=all 성공 때만 남긴다. 36시간 = 하루 한 번 + 여유.
report_backup_freshness() {
    local marker="${BACKUP_OK_MARKER:-$HOME/logs/quant-airflow/backup_last_ok}"
    local last age_h
    if [ ! -r "$marker" ]; then
        warn "드라이브 백업: 성공 기록 없음 ($marker)"
        return 0
    fi
    last=$(head -1 "$marker")
    age_h=$(( ($(date +%s) - $(stat -c %Y "$marker")) / 3600 ))
    if [ "$age_h" -ge 36 ]; then
        warn "드라이브 백업: 마지막 성공 $last (${age_h}시간 전) — backup_to_gdrive.cron.log 확인"
    else
        log "드라이브 백업: 마지막 성공 $last"
    fi
    return 0
}

# 전진 모의매매(swing-it paper_daily.sh, 평일 07:43) 감시. paper_daily.sh **밖**에서 보는 이유: 크론이 죽거나
# 워크트리가 사라지면 paper_daily.sh 는 자기 실패를 적을 수조차 없다 — 장부가 커밋 전에 워크트리째 사라져
# 열흘치가 빈 전례(2026-10-13~23)가 그랬다. 장부 내용은 날짜·팔 이름만 읽는다.
#
# 기대 날짜 = "마지막 크론일(토·일이면 금요일) **전**" 의 마지막 거래일(sector_index 001). 크론은 전 거래일 행을 쓴다:
# 월요일 아침에야 금요일 행이 생기므로 토·일엔 금요일을 기대하면 매주 오보다. 18:10 엔 오늘 지수 행이 이미
# 있어 단순 max(date) 도 오보다. 휴장일은 지수 행이 없어 저절로 맞는다(report_theme_freshness 와 같은 규약).
# 지수까지 통째로 멈추면 이 기대도 같이 밀려 조용해지므로, 그 뒤 틱이 있는지 따로 본다(report_coverage 와 같은 근거).
PAPER_LEDGERS=(leader21 leader_c20 leader_rs leader_m leader_p leader_m_ana)
# pead_rank_F1 첫 행 전: 이 시각(2026-11-06 11:35 실행까지)까지는 장부 대신 그날 로그의 "시작 전" 을 받는다.
PEAD_FIRST_ROW_CUTOFF="2026-11-06 12:00"

report_paper_ledger() {
    local wt="${PAPER_WORKTREE:-$HOME/git/.harness-wt/swing-it/feature-combined-strategy}"
    local paper_log="${PAPER_LOG:-$HOME/logs/swing-it-paper.log}"
    local dir="$wt/research/paper"
    local dow cron_day expected ticks_after
    dow=$(date +%u)
    cron_day=$today
    [ "$dow" -ge 6 ] && cron_day=$(date -d "$today -$((dow - 5)) days" +%F)

    # 로그는 어제까지 쌓여 있다 — 오늘 실행 구간("== 오늘 ..." 첫 줄부터)만 본다. 토·일엔 실행이 없어 건너뛴다.
    local today_log="" pead_log=""
    if [ "$dow" -lt 6 ]; then
        if [ -r "$paper_log" ]; then
            today_log=$(awk -v h="== $today " 'index($0, h) == 1 { f = 1 } f' "$paper_log")
            pead_log=$(printf '%s\n' "$today_log" | awk -v h="== $today " 'index($0, h) == 1 { f = ($NF == "pead_rank_F1") } f')
        fi
        if [ -z "$today_log" ]; then
            warn "모의매매: 오늘 paper_daily 실행 흔적 없음 ($paper_log) — 크론 확인"
        else
            local failed
            failed=$(printf '%s\n' "$today_log" | grep -E '^실패:' | tr '\n' ';')
            if [ -n "$failed" ]; then
                warn "모의매매: 오늘 로그에 실패 — $failed"
            fi
        fi
    fi

    if [ ! -d "$dir" ]; then
        warn "모의매매: 워크트리 없음 ($wt) — 장부 손실 위험, 크론이 이 경로를 부른다"
        return 0
    fi

    expected=$(ts_psql -qtA -c "SET statement_timeout='30s'" -c "
SELECT max(date) FROM sector_index
 WHERE code = '001' AND date < '$cron_day' AND date >= '$cron_day'::date - 30
" 2>/dev/null | tr -d '[:space:]')
    if [ -z "$expected" ]; then
        warn "모의매매: 기대 거래일 조회 실패 (DB 연결 안 됨? sector_index 001)"
        return 0
    fi
    ticks_after=$(ts_psql -qtA -c "SET statement_timeout='30s'" -c "
SELECT EXISTS (SELECT 1 FROM ticks WHERE ts >= '$expected'::date + 1 AND ts < '$cron_day'::date)
" 2>/dev/null | tr -d '[:space:]')
    if [ "$ticks_after" = "t" ]; then
        warn "모의매매: sector_index 001 이 $expected 에서 멈췄는데 그 뒤 틱이 있다 — 달력이 밀려 장부 점검도 같이 밀린다"
    fi

    # 팔별 최신 날짜. 행은 언제나 {"date": ..., "arm": ... 로 시작한다(json.dumps 키 순서).
    arm_dates() {
        sed -nE 's/^\{"date": "([0-9-]{10})", "arm": "([^"]+)".*/\2 \1/p' "$1" 2>/dev/null \
            | sort | awk '{ last[$1] = $2 } END { for (a in last) print a, last[a] }'
    }
    local stale="" name file line arm got
    for name in "${PAPER_LEDGERS[@]}"; do
        file="$dir/${name}_ledger.jsonl"
        if [ ! -s "$file" ]; then
            stale+=" ${name}=장부없음"
            continue
        fi
        while read -r arm got; do
            [ "$got" = "$expected" ] || stale+=" ${arm}=${got}"
        done < <(arm_dates "$file")
    done
    if [ -n "$stale" ]; then
        warn "모의매매: 장부 날짜 어긋남 (기대 $expected):$stale"
    else
        log "모의매매 leader 장부: 전부 $expected"
    fi

    local pead_max=""
    if [ -s "$dir/pead_rank_f1_ledger.jsonl" ]; then
        pead_max=$(arm_dates "$dir/pead_rank_f1_ledger.jsonl" | awk '$1 == "pead_rank_F1" { print $2 }')
    fi
    if [ -n "$pead_max" ]; then
        if [ "$pead_max" = "$expected" ]; then
            log "모의매매 pead_rank_F1 장부: 최신 $pead_max"
        else
            warn "모의매매: pead_rank_F1 장부 최신 $pead_max ≠ 기대 $expected"
        fi
    elif [[ "$(date '+%F %H:%M')" > "$PEAD_FIRST_ROW_CUTOFF" ]]; then
        warn "모의매매: pead_rank_F1 장부에 행이 없다 (기대 $expected) — 첫 행 시한 지남"
    elif [ "$dow" -ge 6 ]; then
        log "모의매매 pead_rank_F1: 첫 행 전 (주말이라 로그 안 봄)"
    elif printf '%s\n' "$pead_log" | grep -q '^시작 전'; then
        log "모의매매 pead_rank_F1: 첫 행 전 (오늘 로그 '시작 전')"
    else
        warn "모의매매: pead_rank_F1 첫 행 전인데 오늘 로그에 '시작 전' 이 없다 — 그 단계가 안 돌았다"
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
report_backup_freshness
report_paper_ledger
if [ ${#WARNINGS[@]} -gt 0 ]; then
    printf '%s\n' "${WARNINGS[@]}" | scripts/alert.sh warn "헬스체크 경보 ${#WARNINGS[@]}건 ($today)"
fi
exit 0
