#!/usr/bin/env bash
# 국내(kr-quant DB)·미국(Sharadar) 시세 데이터를 구글 드라이브에 백업한다.
#
# ticks_full 은 minute_bars/quotes/ticks 를 포함해 34GB 남짓이라 매일 올리면
# 낭비다. 그 셋을 뺀 core 만 매일 올리고, 무거운 통짜는 일요일 한 번만.
# 둘 다 날짜별 파일로 쌓는다(덮어쓰지 않는다) — 며칠 전 상태로 되돌릴 수 있게.
#
# Sharadar 는 성격이 둘로 나뉜다: us.duckdb/us_micro.duckdb 는 daily_sharadar DAG 가
# 매일 통째로 재빌드하므로 그 결과물도 core 처럼 매일 날짜별로 쌓는다. 반대로
# 원본 벌크 zip(*.csv.zip, 수 GB)은 리서치가 손으로 새로 받았을 때만 바뀌므로
# 매일 다시 올릴 이유가 없다 — latest/ 하나만 두고 rclone sync 로 덮어쓴다
# (여기는 날짜별 축적이 아니라 거울 복사다. 바뀐 파일만 다시 올라간다).
#
# ⚠️ 위 Sharadar 단락은 **2026-09-22 부로 자동 경로에서 빠졌다**(아래 case 문 참고).
# 구독 해지로 재빌드가 멈춘 뒤에도 백업만 계속 돌아 같은 파일을 날짜 폴더만
# 바꿔 매일 2.3GB 씩 올리고 있었다. 설명 자체는 구독을 다시 틀 때를 위해 남겨둔다.
#
# 보존기간 없음(Drive 5TB 중 4.8TB 여유 — 사용자 확인, 2026-09-06) — 자동 삭제 안 함.
# 다만 "안 지운다" 는 **중복을 쌓아도 된다는 뜻이 아니다.** 소스가 안 바뀌는데
# 날짜 폴더만 늘어나는 건 되돌릴 이력이 아니라 같은 파일의 사본일 뿐이다.
set -euo pipefail

# 레포 경로는 이 스크립트 위치에서 유도한다 — 2026-09-11 레포를
# ~/Documents/git 에서 ~/git 으로 옮기면서 여기 하드코딩돼 있던 옛 경로가
# 죽었고, 그 바람에 09-12 19:00 백업이 `cd: No such file or directory` 로
# 조용히 실패했다(크론 로그에 한 줄만 남는다). 경로를 다시 옮겨도 안 깨지게.
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REMOTE="gdrive:2.4. 트레이딩/3. stocks 주식/quant-airflow-backup"

cd "$REPO"
# .env 를 통째로 export(set -a)하지 않는다 — KIWOOM_APP_KEY/DART_API_KEY 같은
# 시크릿까지 rclone·gzip·docker 같은 무관한 자식 프로세스에 넘어간다.
# docs/operations.md 는 "수집 subprocess 에만 주입" 이 원칙이다. 필요한
# TIMESCALE_* 는 스크립트 안에서 변수로만 쓰고, docker exec 에는 -e 로 명시 전달한다.
. ./.env

DBUSER="$TIMESCALE_USER"
DBPASS="$TIMESCALE_PASSWORD"
DBNAME="$TIMESCALE_DB"

# 덤프를 뜰 컨테이너 = PRIMARY. 2026-09-13 부터 백업은 simnode 에서만 돈다
# (deploy/crontab.simnode) — 기본값은 simnode 의 PRIMARY 인
# `quant-airflow-timescaledb-replica-1`(docker-compose.replica.yml, 이름만 replica).
# 다른 호스트로 옮기면 `.env` 의 BACKUP_DB_CONTAINER 로 덮어쓴다(daily_health_check.sh
# 도 같은 키를 본다). 이 줄은 `.env` 소싱 **뒤**에 있어야 한다.
CONTAINER="${BACKUP_DB_CONTAINER:-quant-airflow-timescaledb-replica-1}"

# 덤프를 임시로 쓸 위치. **디스크여야 한다** — simnode 의 `/tmp` 는 tmpfs(RAM,
# 31GB)라 ticks_full 통짜 덤프를 거기에 쓰면 RAM 을 먹고, 같은 호스트에서 도는
# PRIMARY DB 의 페이지 캐시를 밀어낸다(전체를 램에 캐시하는 게 DB 를 이 호스트로
# 옮긴 이유였다). `.env` 의 BACKUP_STAGING_PARENT 로 디스크 경로를 준다. 없으면
# 기존대로 mktemp 기본값(/tmp)을 쓴다 — trader 처럼 /tmp 가 디스크인 호스트용.
TMPDIR="$(mktemp -d ${BACKUP_STAGING_PARENT:+-p "$BACKUP_STAGING_PARENT"})"
# 비정상 종료는 사람에게 간다. 2026-09-12 의 `cd: No such file` 처럼 크론 로그에
# 한 줄 남고 끝나던 실패가 이 레포 사고의 전형이었다(spec 2026-10-05). rc 를 **먼저**
# 잡는다 — rm 이 성공하면 $? 가 0 으로 덮인다. 본문은 자기 stdout 을 모을 수 없어
# (크론이 파일로 보낸다) 크론 줄이 넘겨준 BACKUP_LOG 의 꼬리를 읽는다; 없으면 제목만.
# set -e 아래라 트랩 안의 실패가 다시 트랩을 부르지 않게 전부 `|| true` 다.
on_exit() {
  local rc=$?
  rm -rf "$TMPDIR" || true
  if [ "$rc" -ne 0 ]; then
    { [ -n "${BACKUP_LOG:-}" ] && [ -r "$BACKUP_LOG" ] && tail -n 30 "$BACKUP_LOG"; } 2>/dev/null \
      | "$REPO/scripts/alert.sh" error "백업 실패 (rc=$rc, BACKUP_ONLY=${BACKUP_ONLY:-all})" || true
  fi
}
trap on_exit EXIT

DATE="$(date +%F)"
DOW="$(date +%u)"
# quote_events — scalp-it 가 2026-09-16 부터 0D 수신 전부를 쌓는다(quotes 의 ~2.7배 행).
HEAVY_TABLES=(minute_bars quotes quote_events ticks)

# ⚠️ TimescaleDB 하이퍼테이블은 실데이터가 부모 테이블이 아니라
# _timescaledb_internal._hyper_*_chunk 에 쪼개져 들어간다. pg_dump 의
# --exclude-table=minute_bars 는 그 청크 이름까지는 안 잡아서, 이걸로만
# 걸러내면 "core" 라면서 무거운 데이터를 그대로 다 퍼오는 사고가 난다
# (실제로 한 번 그렇게 34GB 다 긁을 뻔했다) — 청크 이름을 직접 조회해서 뺀다.
#
# ⚠️⚠️ 그런데 **압축된 청크는 그 목록에 안 나온다.** 압축을 켜면 실데이터가
# _hyper_*_chunk 에서 빠져나와 별도 관계 compress_hyper_*_chunk 로 옮겨가고,
# timescaledb_information.chunks 의 chunk_name 은 여전히 빈 _hyper_*_chunk 만
# 가리킨다. 그래서 위 조회만 쓰면 압축 데이터가 "core" 에 그대로 실린다 —
# 2026-09-13 실측: core 덤프 비압축 9.6GB 중 8.4GB(87%)가 quotes(4.10GB) ·
# ticks(2.28GB) · minute_bars(2.0GB) 의 압축 청크였다. 압축 이전인 09-10 은
# 420MB 였고 09-11 부터 1.84GB 로 뛴 게 이것이다. 에러는 안 난다 — 파일이
# 커질 뿐이라 아무도 안 본다. 압축 청크는 카탈로그를 조인해서 같이 뺀다.
heavy_chunk_excludes() {
  local in_list
  in_list="$(printf "'%s'," "${HEAVY_TABLES[@]}")"
  in_list="${in_list%,}"
  docker exec -e PGPASSWORD="$DBPASS" "$CONTAINER" \
    psql -U "$DBUSER" -d "$DBNAME" -t -A -c "
      SELECT format('%I.%I', chunk_schema, chunk_name)
      FROM timescaledb_information.chunks
      WHERE hypertable_name IN ($in_list)
      UNION ALL
      SELECT format('%I.%I', cc.schema_name, cc.table_name)
      FROM _timescaledb_catalog.chunk c
      JOIN _timescaledb_catalog.hypertable h ON h.id = c.hypertable_id
      JOIN _timescaledb_catalog.chunk cc ON cc.id = c.compressed_chunk_id
      WHERE h.table_name IN ($in_list)"
}

dump_core() {
  local out="$TMPDIR/core-$DATE.sql.gz"
  local excludes=()
  for t in "${HEAVY_TABLES[@]}"; do excludes+=(--exclude-table="$t"); done
  # process substitution(< <(...))의 실패는 set -e 로 안 잡힌다 — 여기서 chunk
  # 조회가 죽으면 while 루프는 그냥 빈 스트림을 본 것처럼 넘어가고, core 덤프가
  # 위 3개 정적 이름만으로 진행돼 실제 청크(_hyper_*_chunk)를 하나도 못 뺀다.
  # 명령 치환으로 바꿔 실패를 여기서 끊는다.
  local chunks
  chunks="$(heavy_chunk_excludes)" || {
    echo "[$(date '+%F %T')] ⚠️ heavy chunk 조회 실패 — core 백업 중단 (안전을 위해)" >&2
    return 1
  }
  while IFS= read -r chunk; do
    [ -n "$chunk" ] && excludes+=(--exclude-table="$chunk")
  done <<< "$chunks"
  docker exec -e PGPASSWORD="$DBPASS" "$CONTAINER" \
    pg_dump -U "$DBUSER" -d "$DBNAME" "${excludes[@]}" | gzip > "$out"

  # **뜬 것을 그 자리에서 검증한다 — 올리기 전에.** 위 제외 목록이 조용히 비거나
  # (조회는 성공했는데 0행) 카탈로그 모양이 바뀌어도 pg_dump 는 성공으로 끝나고
  # 파일만 커진다. 그게 09-11~09-13 에 실제로 일어난 일이고, 3일간 아무 신호도
  # 없었다. 무거운 청크로 들어가는 COPY 가 한 줄이라도 있으면 올리지 않는다.
  # 제외 목록은 `스키마.관계` 로 오고, 덤프의 COPY 대상도 같은 모양이다. 둘 다
  # 스키마를 떼고 관계명만으로 교집합을 본다. `^COPY` 로 못박는 게 중요하다 —
  # 청크 이름은 _timescaledb_catalog.chunk 의 **데이터 행**으로도 덤프에 등장하며
  # 그건 정상이다(카탈로그는 core 에 있어야 한다). 데이터가 실렸는지만 본다.
  local excluded_names copied leaked
  excluded_names="$(printf '%s\n' "$chunks" | sed 's/^[^.]*\.//' | LC_ALL=C sort -u)"
  copied="$(zcat "$out" | awk '/^COPY /{sub(/^[^.]*\./,"",$2); print $2}' | LC_ALL=C sort -u)"
  leaked="$(printf '%s\n' "$copied" | grep -Fxf <(printf '%s\n' "$excluded_names") - || true)"
  if [ -n "$leaked" ]; then
    echo "[$(date '+%F %T')] ⚠️ core 덤프에 제외 대상 청크 데이터가 들어갔다 — 업로드 중단." >&2
    echo "$leaked" | head -5 >&2
    echo "    (압축 청크는 timescaledb_information.chunks 에 안 나온다 — heavy_chunk_excludes 확인)" >&2
    return 1
  fi

  rclone copyto "$out" "$REMOTE/core/core-$DATE.sql.gz"
  echo "[$(date '+%F %T')] core 백업 완료 — $REMOTE/core/core-$DATE.sql.gz ($(du -h "$out" | cut -f1))"
}

# 그날 틱·호가만 장 마감 뒤에 올린다(평일 16:15 크론, `BACKUP_ONLY=ticks-today`).
# core 는 무거운 4테이블을 빼고 통짜는 일요일에만 뜨므로, 그 사이 DB 디스크가 죽으면
# 최대 6일치 틱·호가가 사라진다 — 키움에서 소급 수집이 안 되는 유일한 데이터다.
# 2026-10-07 trader 처분으로 리플리카(두 번째 사본)가 없어지면서 넣었다. 분봉은 키움에서
# 다시 받을 수 있어 뺀다. 행이 0 이면(휴장일) 올리지 않는다.
dump_ticks_today() {
  local t out n total=0
  for t in ticks quotes quote_events; do
    out="$TMPDIR/$t-$DATE.csv.gz"
    docker exec -e PGPASSWORD="$DBPASS" "$CONTAINER" \
      psql -U "$DBUSER" -d "$DBNAME" -v ON_ERROR_STOP=1 -Atc \
      "COPY (SELECT * FROM $t WHERE ts >= '$DATE 00:00+09' AND ts < '$DATE 00:00+09'::timestamptz + interval '1 day') TO STDOUT WITH CSV HEADER" \
      | gzip > "$out"
    n=$(( $(zcat "$out" | wc -l) - 1 ))
    [ "$n" -lt 0 ] && n=0
    total=$(( total + n ))
    if [ "$n" -gt 0 ]; then
      rclone copyto "$out" "$REMOTE/ticks_daily/$DATE/$t.csv.gz"
      echo "[$(date '+%F %T')] $t $DATE ${n}행 백업 — $REMOTE/ticks_daily/$DATE/$t.csv.gz ($(du -h "$out" | cut -f1))"
    else
      echo "[$(date '+%F %T')] $t $DATE 0행 — 업로드 생략"
    fi
  done
  # 평일인데 틱이 0행이면 수집이 죽은 날이다 — 백업 문제가 아니지만 여기서도 알린다.
  if [ "$total" -eq 0 ] && [ "$DOW" -le 5 ]; then
    echo "[$(date '+%F %T')] ⚠️ 평일인데 오늘 틱·호가가 0행 (휴장일이 아니면 수집 확인)" >&2
  fi
}

dump_full() {
  local out="$TMPDIR/full-$DATE.sql.gz"
  docker exec -e PGPASSWORD="$DBPASS" "$CONTAINER" \
    pg_dump -U "$DBUSER" -d "$DBNAME" | gzip > "$out"
  rclone copyto "$out" "$REMOTE/ticks_full/full-$DATE.sql.gz"
  echo "[$(date '+%F %T')] 전체(ticks 포함) 백업 완료 — $REMOTE/ticks_full/full-$DATE.sql.gz ($(du -h "$out" | cut -f1))"
}

SHARADAR_DIR="/home/young/data"

dump_sharadar_duckdb() {
  local dest="$REMOTE/sharadar/duckdb/$DATE"
  local copied=0
  for f in us.duckdb us_micro.duckdb us_micro.duckdb.manifest.json; do
    local src="$SHARADAR_DIR/$f"
    if [ -f "$src" ]; then
      rclone copyto "$src" "$dest/$f"
      copied=$((copied + 1))
    fi
  done
  # 파일이 하나도 없는데 "완료" 를 찍으면 daily_sharadar 재빌드 실패를 백업
  # 성공으로 위장하는 꼴이다("초록불 = 성공 아니다", CLAUDE.md §5).
  if [ "$copied" -eq 0 ]; then
    echo "[$(date '+%F %T')] ⚠️ sharadar duckdb 파일 없음 — daily_sharadar 결과물 확인 필요 (백업 스킵)" >&2
    return 1
  fi
  echo "[$(date '+%F %T')] sharadar duckdb 백업 완료 (${copied}개 파일) — $dest"
}

dump_sharadar_bulk() {
  # raw/ 는 sharadar_bulk.py 가 받는 중인 .part 임시본이 섞여 있어 뺀다.
  rclone sync "$SHARADAR_DIR/sharadar" "$REMOTE/sharadar/bulk/latest" \
    --exclude "raw/**" --exclude "README.md"
  echo "[$(date '+%F %T')] sharadar 벌크(latest) 동기화 완료 — $REMOTE/sharadar/bulk/latest"
}

# 한 단계만 다시 돌릴 수 있게 한다(`BACKUP_ONLY=core ./scripts/backup_to_gdrive.sh`).
# 한 단계가 틀려서 다시 올려야 할 때 4.5GB 통짜 덤프까지 같이 다시 뜨면 한 시간이
# 날아간다 — 2026-09-13 에 압축청크 제외 픽스를 core 에만 반영해야 해서 실제로
# 필요했다. 기본값 all 은 크론이 쓰는 기존 동작 그대로다.
case "${BACKUP_ONLY:-all}" in
  core)          dump_core ;;
  sharadar)      dump_sharadar_duckdb ;;
  full)          dump_full ;;
  ticks-today)   dump_ticks_today ;;
  sharadar-bulk) dump_sharadar_bulk ;;
  all)
    dump_core
    # ⚠️ sharadar 두 단계(duckdb·bulk)는 2026-09-22 부터 자동 경로에서 뺐다.
    # 구독 해지(2026-09-14, `daily_sharadar` 가 schedule=None)로 us.duckdb ·
    # us_micro.duckdb 가 더는 재빌드되지 않는데 백업만 매일 돌아, **같은 파일을
    # 날짜 폴더만 바꿔 하루 2.3GB 씩 올리고 있었다.** 09-22 실측(md5):
    #   - us_micro.duckdb: 09-13~09-21 9개 폴더가 전부 4856ecc… 동일(원본 mtime 09-11 17:44)
    #   - us.duckdb:       15개 폴더 전부 8d28be… 동일(원본 mtime 2026-08-12)
    #   - bulk/latest:     14개 파일이 bulk/data_2026_08_11 과 md5 전부 일치
    # 중복 약 29GB 를 09-22 에 지웠다(백업 총량 64GB → 35.7GB). 남긴 것은 고유
    # 판본뿐이다 — duckdb/{2026-09-06,08,09,10,13} · bulk/data_2026_08_11.
    # 날짜 폴더는 **그 판본이 처음 나타난 날**만 남겼다. 그래야 "D 시점 상태" 를
    # 찾을 때 D 이하 최신 폴더를 고르는 방식이 여전히 맞는 판본을 준다.
    #
    # 함수는 지우지 않는다 — 구독을 다시 트면 `BACKUP_ONLY=sharadar` ·
    # `BACKUP_ONLY=sharadar-bulk` 로 손으로 돌릴 수 있고, 아래 두 줄의 주석만
    # 풀면 자동 경로로 돌아온다.
    #   dump_sharadar_duckdb
    #
    # 일요일(요일번호 7)에만 무거운 전체 덤프. $DOW 는 스크립트
    # 시작 시점에 $DATE 와 함께 고정해뒀다 — 여기서 다시 date +%u 를 부르면 앞의
    # 두 덤프가 자정을 넘겨 걸릴 때 $DATE(전날)와 요일 판정(다음날 기준)이 어긋나
    # 주간 전체 덤프가 조용히 스킵되거나 날짜가 안 맞는 파일명으로 올라갈 수 있다.
    if [ "$DOW" = "7" ]; then
      dump_full
      #   dump_sharadar_bulk
    fi
    ;;
  *)
    echo "BACKUP_ONLY 값이 이상하다: ${BACKUP_ONLY} (core|sharadar|full|sharadar-bulk|ticks-today|all)" >&2
    exit 2 ;;
esac
