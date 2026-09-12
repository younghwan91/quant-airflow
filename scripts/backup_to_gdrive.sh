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
# 보존기간 없음(Drive 5TB 중 4.8TB 여유 — 사용자 확인, 2026-09-06) — 자동 삭제 안 함.
set -euo pipefail

# 레포 경로는 이 스크립트 위치에서 유도한다 — 2026-09-11 레포를
# ~/Documents/git 에서 ~/git 으로 옮기면서 여기 하드코딩돼 있던 옛 경로가
# 죽었고, 그 바람에 09-12 19:00 백업이 `cd: No such file or directory` 로
# 조용히 실패했다(크론 로그에 한 줄만 남는다). 경로를 다시 옮겨도 안 깨지게.
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONTAINER="quant-airflow-timescaledb-1"
REMOTE="gdrive:2.4. 트레이딩/3. stocks 주식/quant-airflow-backup"
TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

cd "$REPO"
# .env 를 통째로 export(set -a)하지 않는다 — KIWOOM_APP_KEY/DART_API_KEY 같은
# 시크릿까지 rclone·gzip·docker 같은 무관한 자식 프로세스에 넘어간다.
# docs/operations.md 는 "수집 subprocess 에만 주입" 이 원칙이다. 필요한
# TIMESCALE_* 는 스크립트 안에서 변수로만 쓰고, docker exec 에는 -e 로 명시 전달한다.
. ./.env

DBUSER="$TIMESCALE_USER"
DBPASS="$TIMESCALE_PASSWORD"
DBNAME="$TIMESCALE_DB"

DATE="$(date +%F)"
DOW="$(date +%u)"
HEAVY_TABLES=(minute_bars quotes ticks)

# ⚠️ TimescaleDB 하이퍼테이블은 실데이터가 부모 테이블이 아니라
# _timescaledb_internal._hyper_*_chunk 에 쪼개져 들어간다. pg_dump 의
# --exclude-table=minute_bars 는 그 청크 이름까지는 안 잡아서, 이걸로만
# 걸러내면 "core" 라면서 무거운 데이터를 그대로 다 퍼오는 사고가 난다
# (실제로 한 번 그렇게 34GB 다 긁을 뻔했다) — 청크 이름을 직접 조회해서 뺀다.
heavy_chunk_excludes() {
  local in_list
  in_list="$(printf "'%s'," "${HEAVY_TABLES[@]}")"
  in_list="${in_list%,}"
  docker exec -e PGPASSWORD="$DBPASS" "$CONTAINER" \
    psql -U "$DBUSER" -d "$DBNAME" -t -A -c "
      SELECT format('%I.%I', chunk_schema, chunk_name)
      FROM timescaledb_information.chunks
      WHERE hypertable_name IN ($in_list)"
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
  rclone copyto "$out" "$REMOTE/core/core-$DATE.sql.gz"
  echo "[$(date '+%F %T')] core 백업 완료 — $REMOTE/core/core-$DATE.sql.gz ($(du -h "$out" | cut -f1))"
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

dump_core
dump_sharadar_duckdb

# 일요일(요일번호 7)에만 무거운 전체 덤프 + 벌크 zip 동기화. $DOW 는 스크립트
# 시작 시점에 $DATE 와 함께 고정해뒀다 — 여기서 다시 date +%u 를 부르면 앞의
# 두 덤프가 자정을 넘겨 걸릴 때 $DATE(전날)와 요일 판정(다음날 기준)이 어긋나
# 주간 전체 덤프가 조용히 스킵되거나 날짜가 안 맞는 파일명으로 올라갈 수 있다.
if [ "$DOW" = "7" ]; then
  dump_full
  dump_sharadar_bulk
fi
