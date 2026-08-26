"""Sharadar 스토어 재구축 — ② BUILD · ③ GATE · ④ PUBLISH.

설계는 `docs/superpowers/specs/2026-08-15-sharadar-bulk-rebuild-design.md`.

raw 아카이브(collectors.sharadar_bulk 가 받아둔 벌크 zip)로부터 **새 DuckDB
파일을 짓고**, 검증을 통과하면 기존 스토어 자리에 **원자적으로 갈아끼운다**.

이 구조가 사는 이유 셋:

1. **락 충돌이 없다.** 빌드는 별도 파일에 하고 공개는 `os.replace` 다. POSIX
   rename 은 원자적이고, 이미 열려 있는 리더는 옛 inode 를 계속 안전하게 읽는다
   — 연구(`opt-factor optimize`)가 도는 중에도 배포된다. 증분 upsert 방식은
   여기서 매번 죽었다.
2. **나쁜 스토어가 공개되지 않는다.** 게이트를 통과 못 하면 아무것도 안 바꾼다.
   기존 스토어가 그대로 서비스된다.
3. **되돌릴 수 있다.** 직전 세대를 남긴다.

**정규화는 재구현하지 않는다.** `opt-factor ingest --provider csv` 를 그대로
부른다 — `_csv_daily`(백만달러 환산)·`_csv_tickers`(is_delisted 리네임)·
`_csv_fundamentals`(PIT 위반 제외) 는 전부 실제 버그에서 나온 코드다. DuckDB
SQL 로 옮기면 빠르겠지만, 동등성 테스트 없이는 그 버그들이 되살아난다.

실행:
    python -m collectors.sharadar_build --raw-dir /opt/us-data/sharadar/raw \\
        --store /opt/us-data/us_micro.duckdb
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
import time

from pathlib import Path

from .proc import stream_subprocess

# 벌크 파일명 → CLI 의 --kind. 이름이 어긋나는 지점이 둘 있다(stocks→prices,
# holdings_ticker→institutions) — 벤더의 파일명과 스토어의 테이블명이 다르다.
TABLE_KINDS: dict[str, str] = {
    "stocks": "prices",
    "daily": "daily",
    "fundamentals": "fundamentals",
    "insiders": "insiders",
    "holdings_ticker": "institutions",
    "actions": "actions",
    "sp500": "sp500",
    "tickers": "tickers",
}

# 게이트가 비어 있지 않기를 요구하는 스토어 테이블.
EXPECTED_TABLES = ("prices", "fundamentals", "tickers", "actions", "sp500", "insiders", "institutions")

# 행수가 이 비율 넘게 줄면 벤더 파일이 잘렸다고 보고 공개를 막는다. 벤더가
# 중복·오류 행을 정정하면 소폭 감소는 정상이라 0 으로 둘 수는 없다.
MAX_ROW_REGRESSION = 0.05


class GateFailure(Exception):
    """검증 실패 — 이 스토어는 공개하면 안 된다."""


def build_command(table: str, *, raw: str, store: str) -> list[str]:
    """`opt-factor ingest --provider csv` 커맨드.

    콘솔 스크립트는 컨테이너에 설치돼 있지 않다(opt_portfolio 는 ro 마운트 +
    PYTHONPATH 로만 쓴다) — `-c` 로 진입점을 직접 부른다.
    """
    return [
        sys.executable,
        "-c",
        "from opt_portfolio.factor.cli import main; raise SystemExit(main())",
        "ingest",
        "--store", store,
        "--provider", "csv",
        "--kind", TABLE_KINDS[table],
        "--csv", raw,
    ]


def build(raw_dir: Path, out: Path, *, tables: tuple[str, ...] = tuple(TABLE_KINDS)) -> None:
    """raw → 새 스토어. 하나라도 실패하면 예외 — 반쪽 스토어는 공개 후보가 아니다."""
    raw_dir, out = Path(raw_dir), Path(out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.unlink(missing_ok=True)  # 재시도 시 이전 시도의 잔재 위에 쌓지 않는다

    for table in tables:
        raw = raw_dir / f"{table}.csv.zip"
        if not raw.exists():
            raise FileNotFoundError(
                f"raw 파일이 없습니다: {raw}\n"
                "collectors.sharadar_bulk 를 먼저 돌려 벌크를 받으세요."
            )
        started = time.monotonic()
        cmd = build_command(table, raw=str(raw), store=str(out))
        print(f"▶ {table} → {TABLE_KINDS[table]}", flush=True)
        rc = stream_subprocess(cmd, prefix="   ")
        if rc != 0:
            raise RuntimeError(f"{table} 빌드 실패 (rc={rc})")
        print(f"   {time.monotonic() - started:.1f}초", flush=True)


def _counts(store: Path, tables) -> dict[str, int | None]:
    import duckdb

    conn = duckdb.connect(str(store), read_only=True)
    try:
        present = {
            r[0] for r in conn.execute("SELECT table_name FROM information_schema.tables").fetchall()
        }
        return {
            t: (conn.execute(f"SELECT count(*) FROM {t}").fetchone()[0] if t in present else None)
            for t in tables
        }
    finally:
        conn.close()


def validate(new: Path, current: Path, *, expected=EXPECTED_TABLES) -> dict[str, int]:
    """공개 전 게이트. 통과하면 새 스토어의 행수를 돌려준다.

    직전 스토어가 없으면(첫 빌드) 비교는 건너뛰고 '비어 있지 않은가'만 본다.
    """
    new, current = Path(new), Path(current)
    fresh = _counts(new, expected)

    missing = [t for t, n in fresh.items() if n is None]
    if missing:
        raise GateFailure(f"테이블이 없습니다: {', '.join(missing)}")
    empty = [t for t, n in fresh.items() if not n]
    if empty:
        raise GateFailure(f"테이블이 0행입니다: {', '.join(empty)}")

    # 비교 기준은 **직전 빌드의 매니페스트**다. 살아 있는 스토어를 열면 안 된다 —
    # DuckDB 는 단일 라이터라 연구 프로세스가 쓰기로 잡고 있으면 read_only 연결도
    # 실패한다(2026-08-15 실측: 8개 테이블 빌드가 전부 성공했는데 게이트가 여기서
    # 막혀 전체가 죽었다). 공개(os.replace)는 락이 필요 없는데 비교가 필요하게
    # 만든 것이 설계 결함이었다.
    baseline = read_build_manifest(current)
    if baseline:
        before = baseline.get("counts", {})
        shrunk = []
        for table, now in fresh.items():
            was = before.get(table)
            if was and now < was * (1 - MAX_ROW_REGRESSION):
                shrunk.append(f"{table} {was:,}→{now:,} ({now / was - 1:+.1%})")
        if shrunk:
            raise GateFailure(
                "행수가 크게 줄었습니다 — 벤더 파일 절단이 의심됩니다: " + "; ".join(shrunk)
            )

        # 행수만으로는 부족하다. 낡은 raw 로 빌드하면 행수는 멀쩡한데 날짜가
        # 뒤로 간다(실측: 8/12 raw 로 prices 최신일이 08-14 → 08-10 후퇴,
        # 행수 게이트는 -0.07% 라 통과했다).
        now_newest, was_newest = _max_date(new, "prices", "date"), baseline.get("newest")
        if now_newest and was_newest and str(now_newest) < str(was_newest):
            raise GateFailure(
                f"prices 최신일이 후퇴했습니다: {was_newest} → {now_newest} — "
                "raw 아카이브가 낡았습니다(먼저 collectors.sharadar_bulk 를 돌리세요)"
            )
    elif current.exists():
        print(
            "⚠️  직전 빌드 매니페스트가 없어 회귀 비교를 건너뜁니다 "
            "— 이번 공개가 기준선을 만듭니다",
            flush=True,
        )

    return {t: n for t, n in fresh.items() if n is not None}


def _max_date(store: Path, table: str, column: str):
    import duckdb

    conn = duckdb.connect(str(store), read_only=True)
    try:
        present = {
            r[0] for r in conn.execute("SELECT table_name FROM information_schema.tables").fetchall()
        }
        if table not in present:
            return None
        return conn.execute(f"SELECT max({column}) FROM {table}").fetchone()[0]
    finally:
        conn.close()


def manifest_path(store: Path) -> Path:
    """스토어 옆에 두는 빌드 계보 파일 — 게이트의 비교 기준선."""
    return Path(store).with_name(f"{Path(store).name}.manifest.json")


def read_build_manifest(store: Path) -> dict:
    """직전 빌드 기록. 없거나 깨졌으면 빈 dict — 비교를 건너뛸 뿐이다."""
    try:
        with open(manifest_path(store), encoding="utf-8") as fh:
            data = json.load(fh)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def write_build_manifest(store: Path, counts: dict, *, newest=None) -> None:
    """공개한 스토어의 수치를 남긴다. 다음 실행의 게이트가 이걸 읽는다."""
    path = manifest_path(store)
    payload = {
        "counts": {k: int(v) for k, v in counts.items()},
        "newest": str(newest) if newest is not None else None,
    }
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=".manifest-", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2, sort_keys=True)
            fh.write("\n")
        os.chmod(tmp, 0o644)
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise


def publish(new: Path, dest: Path, *, keep: int = 2, counts=None, newest=None) -> None:
    """새 스토어를 제자리에 갈아끼운다.

    ``os.replace`` 는 원자적이다. 이 순간 스토어를 열고 있던 프로세스는 옛
    inode 를 계속 읽으므로 연구가 중간에 깨지지 않고, 다음에 여는 쪽부터 새
    데이터를 본다. DuckDB 배타 락을 피해 가는 것이 아니라, 애초에 마주치지 않는다.
    """
    new, dest = Path(new), Path(dest)
    if dest.exists():
        stamp = time.strftime("%Y%m%dT%H%M%S", time.localtime())
        os.replace(dest, dest.with_name(f"{dest.name}.prev{stamp}"))
        backups = sorted(dest.parent.glob(f"{dest.name}.prev*"))
        for stale in backups[:-keep] if keep else backups:
            stale.unlink(missing_ok=True)
    os.chmod(new, 0o644)
    os.replace(new, dest)
    if counts is not None:
        write_build_manifest(dest, counts, newest=newest)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-dir", default=os.environ.get("US_RAW_DIR", "/opt/us-data/sharadar/raw"))
    parser.add_argument("--store", default=os.environ.get("US_DATA_STORE", "/opt/us-data/us_micro.duckdb"))
    parser.add_argument("--keep", type=int, default=2, help="롤백용으로 남길 직전 세대 수")
    parser.add_argument("--no-publish", action="store_true", help="빌드·검증만 하고 공개하지 않는다")
    args = parser.parse_args(argv)

    store = Path(args.store)
    staging = store.with_name(f".{store.name}.building")

    started = time.monotonic()
    build(Path(args.raw_dir), staging)
    counts = validate(staging, store)
    print("게이트 통과 — " + ", ".join(f"{t}={n:,}" for t, n in sorted(counts.items())), flush=True)

    if args.no_publish:
        print(f"--no-publish — {staging} 에 남겨둡니다", flush=True)
        return 0

    # 공개하면 staging 파일이 사라지므로 최신일을 먼저 읽어둔다.
    newest = _max_date(staging, "prices", "date")
    publish(staging, store, keep=args.keep, counts=counts, newest=newest)
    print(f"✅ 공개 완료: {store}  (총 {time.monotonic() - started:.1f}초)", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
