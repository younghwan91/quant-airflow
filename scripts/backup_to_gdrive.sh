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

REPO="/home/young/Documents/git/quant-airflow"
CONTAINER="quant-airflow-timescaledb-1"
REMOTE="gdrive:2.4. 트레이딩/3. stocks 주식/quant-airflow-backup"
TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

cd "$REPO"
set -a; . ./.env; set +a

DBUSER="$TIMESCALE_USER"
DBPASS="$TIMESCALE_PASSWORD"
DBNAME="$TIMESCALE_DB"

DATE="$(date +%F)"
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
  while IFS= read -r chunk; do
    [ -n "$chunk" ] && excludes+=(--exclude-table="$chunk")
  done < <(heavy_chunk_excludes)
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
  for f in us.duckdb us_micro.duckdb us_micro.duckdb.manifest.json; do
    local src="$SHARADAR_DIR/$f"
    [ -f "$src" ] && rclone copyto "$src" "$dest/$f"
  done
  echo "[$(date '+%F %T')] sharadar duckdb 백업 완료 — $dest"
}

dump_sharadar_bulk() {
  # raw/ 는 sharadar_bulk.py 가 받는 중인 .part 임시본이 섞여 있어 뺀다.
  rclone sync "$SHARADAR_DIR/sharadar" "$REMOTE/sharadar/bulk/latest" \
    --exclude "raw/**" --exclude "README.md"
  echo "[$(date '+%F %T')] sharadar 벌크(latest) 동기화 완료 — $REMOTE/sharadar/bulk/latest"
}

dump_core
dump_sharadar_duckdb

# 일요일(요일번호 7)에만 무거운 전체 덤프 + 벌크 zip 동기화
if [ "$(date +%u)" = "7" ]; then
  dump_full
  dump_sharadar_bulk
fi
