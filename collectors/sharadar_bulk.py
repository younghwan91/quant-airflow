"""Sharadar 벌크 스냅샷 다운로더 — 파이프라인의 ① RAW 계층.

설계 근거는 `docs/superpowers/specs/2026-08-15-sharadar-bulk-rebuild-design.md`.
요약하면: 증분 API 수집(종목 22,000개를 30개씩 ~730회 순회)은 벤더의 티커
200자 제한과 소켓 타임아웃 때문에 구조적으로 깨진다. 벌크는 테이블당 요청
1회라 둘 다 해당이 없고, SEP 전체 이력(4,626만 행) 적재가 37초다 — 같은
데이터를 증분으로 받다 70분 쓰고 실패한 것과 대비된다.

**벌크에는 필터가 안 먹는다.** `lastupdated.gte` 를 붙여도 전량이 온다(실측).
그래서 이 모듈은 "증분 다운로드"를 하지 않는다. 대신 벤더 목록의 `modified`
타임스탬프를 로컬 매니페스트와 비교해 **안 바뀐 파일을 아예 안 받는다** — 벤더
대역폭이 약 4.4MB/s 라 전량이 17분이고, 이게 유일한 낭비 차단 장치다.

**확인은 매일 14개 전부, 전송은 바뀐 것만.** 주기를 나누면 그 주기를 부르는
DAG 이 없을 때 해당 테이블이 영원히 안 받아진다 — 실제로 holdings·
holdings_investor·descriptions 가 그 상태였다. 확인 사실은 `checked_at` 으로
남고, 매 실행 끝에 14개 상태가 표로 출력된다.

낡음(벤더 미갱신)은 막지 않는다 — 벌크가 매번 전체 이력을 주므로 다음 실행에
저절로 채워진다. 막는 것은 손상(절단·체크섬 불일치)뿐이다.

실행:
    python -m collectors.sharadar_bulk --raw-dir /opt/us-data/sharadar/raw
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import tempfile
import time
import unicodedata
import urllib.parse
import urllib.request
import zipfile

from pathlib import Path

_API_ROOT = "https://api.sharadar.com/v1.0"

# 구독 중인 전량. **주기를 나누지 않는다** — 요구사항이 "항상 동기화" 이고,
# 나눠두면 그 주기를 부르는 DAG 이 없을 때 해당 테이블은 영원히 안 받아진다
# (실제로 holdings·holdings_investor·descriptions 가 그 상태였다).
#
# 매일 14개를 다 확인해도 전송은 안 늘어난다 — `modified` 가 그대로면 0바이트다.
# 늘어나는 비용은 목록 조회 1회뿐이고, 13F 542MB 는 실제로 바뀌는 분기당
# 한 번만 내려온다. 주석의 크기는 2026-08-15 실측이다.
SUBSCRIBED_TABLES = (
    "stocks",            # SEP  — 주가. 953MB
    "daily",             # DAILY— 시총/EV. 733MB
    "fundamentals",      # SF1  — 분기 재무. 626MB
    "funds",             # SFP  — ETF·펀드 가격. 286MB
    "insiders",          # SF2  — Form 4. 234MB
    "holdings",          # SF3  — 13F 원자료. 542MB
    "holdings_investor", # SF3B — 13F 투자자별
    "holdings_ticker",   # SF3A — 13F 티커 집계. 18MB
    "events",            # 11MB
    "actions",           # 9MB
    "metrics",           # 1.4MB
    "sp500",             # 270KB
    "tickers",           # 4.8MB
    "descriptions",      # 필드 사전. 2026-07-31 이후 무변경
)

MANIFEST_NAME = "manifest.json"

#: 소켓 연산 하나당 타임아웃(초). **전체 소요가 아니라 read() 한 번의 상한이다.**
#: 1800 이었을 때, 벤더가 연결을 연 채 멎으면 태스크가 30분 매달렸다가 죽고
#: 재시도 대기 10분이 더 붙었다 — 정상 전량 다운로드가 17분인데 스톨 한 번에
#: 1시간을 썼다. 4MiB 블록 하나가 120초 안에 안 오면 죽은 연결로 본다(실측
#: 최저 속도 1.9MB/s 기준 4MiB 는 2.2초).
SOCKET_TIMEOUT = 120

#: 고아 `.part` 를 치우는 나이 기준(초). mkstemp 임시 파일은 파이썬 예외에는
#: `except BaseException` 이 정리하지만 **SIGKILL·컨테이너 정지·OOM 에는 안
#: 돈다** — 이 서버는 매일 `docker compose stop` 으로 스택을 내리므로 다운로드
#: 중 강제 종료가 이론적 시나리오가 아니다. 한 번에 최대 985MB 가 0600 으로
#: 영구 잔존한다. 나이 조건이 필수다 — 지금 도는 런의 것을 지우면 안 된다.
ORPHAN_PART_MAX_AGE = 6 * 3600

# `modified` 가 몇 번 연속으로 그대로면 눈에 띄게 표시할지. **빌드를 막지
# 않는다** — 낡음은 벌크가 매번 전체 이력을 주므로 다음 실행에 저절로 채워진다.
# 막는 건 손상(절단·행수 급감·데이터 후퇴)뿐이고 그건 게이트의 몫이다.
#
# 미국 거래일 캘린더를 두는 대신 정체 횟수로 근사한다 — 연휴에는 정체가
# 자연히 길어져 관용적이 된다. 대신 "벤더가 상시 하루씩 늦다" 는 만성 지연은
# 이 방식으로 못 잡는다. 알려진 한계다.
DEFAULT_STALE_AFTER = 2
STALE_AFTER: dict[str, int] = {
    "insiders": 4,           # Form 4 — 공시가 없는 날이 있다
    "holdings_ticker": 4,    # SF3A — 13F 집계
    "holdings": 8,           # SF3  — 13F 원자료는 분기 공시다
    "holdings_investor": 8,  # SF3B
    "descriptions": 30,      # 필드 사전. 2026-07-31 이후 무변경
}


class CorruptDownload(Exception):
    """받은 파일이 온전한 zip 이 아니다 — 목적지에 두면 안 된다."""


def file_sha256(path: Path, *, chunk: int = 1 << 22) -> str:
    """파일 해시. 4.6GB 전량이라도 10초 안쪽이라 매 실행 계산해도 된다."""
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(chunk), b""):
            digest.update(block)
    return digest.hexdigest()


def verify_zip(path: Path) -> None:
    """중앙 디렉터리를 읽어 절단을 잡는다.

    `testzip()` 은 전량 압축 해제라 953MB 에 쓸 수 없다. 중앙 디렉터리는
    zip 끝에 있으므로, 읽히면 전송이 끝까지 온 것이다.
    """
    try:
        with zipfile.ZipFile(path) as zf:
            if not zf.namelist():
                raise CorruptDownload(f"빈 zip 입니다: {path}")
    except zipfile.BadZipFile as exc:
        raise CorruptDownload(f"온전한 zip 이 아닙니다: {path} ({exc})") from exc


def stale_threshold(table: str) -> int:
    return STALE_AFTER.get(table, DEFAULT_STALE_AFTER)


def is_stale(table: str, entry: dict) -> bool:
    return int(entry.get("unchanged_streak", 0)) >= stale_threshold(table)


def record_check(entry: dict | None, vendor_modified: str, *, now: str) -> dict:
    """이번 실행에서 벤더와 대조한 사실을 기록한 **새** 항목을 반환한다.

    `modified`/`size`/`sha256` 은 **로컬 파일**의 정보라 여기서 건드리지 않는다.
    그것들은 실제로 받았을 때만 바뀐다. 목록에서 본 값은 `vendor_modified` 다
    — 둘을 섞으면 파일을 안 받고도 최신으로 착각해 영원히 스킵한다.
    """
    updated = dict(entry or {})
    if updated.get("vendor_modified") == vendor_modified:
        updated["unchanged_streak"] = int(updated.get("unchanged_streak", 0)) + 1
    else:
        updated["unchanged_streak"] = 0
    updated["vendor_modified"] = vendor_modified
    updated["checked_at"] = now
    return updated


def bulk_url(table: str, *, api_key: str) -> str:
    """벌크 zip 다운로드 URL.

    `bulk=true` 가 빠지면 벤더는 조용히 `limit` 기본값(10,000행)짜리 JSON 을
    돌려준다 — 실패가 아니라 **절단**이라 알아채기 어렵다.
    """
    query = urllib.parse.urlencode(
        {"api_key": api_key, "format": "csv", "bulk": "true"}
    )
    return f"{_API_ROOT}/data/{table}?{query}"


def fetch_listing(*, api_key: str, opener=urllib.request.urlopen) -> dict[str, str]:
    """`{테이블: modified}` — 전체 이력 파일만. 5Y/10Y 변형은 안 쓴다."""
    url = f"{_API_ROOT}/bulk?{urllib.parse.urlencode({'api_key': api_key})}"
    with opener(url, timeout=60) as resp:
        payload = json.load(resp)
    return {
        item["table"]: item["modified"]
        for item in payload.get("items", [])
        if item.get("history") == "full"
    }


def plan_sync(listing: dict[str, str]) -> tuple[dict[str, str], tuple[str, ...]]:
    """`(받을_계획, 벤더가_안_준_테이블)`.

    누락을 **반환값으로** 드러낸다. 예전에는 `if table in listing` 로 조용히
    걸러서, stocks 하나가 목록에서 빠져도 로그 어디에도 안 남았다 — 그리고
    다음 빌드는 어제 zip 으로 정상 완주했다.
    """
    plan = {table: listing[table] for table in SUBSCRIBED_TABLES if table in listing}
    missing = tuple(table for table in SUBSCRIBED_TABLES if table not in listing)
    return plan, missing


# ----------------------------------------------------------------- 매니페스트


def read_manifest(raw_dir: Path) -> dict[str, dict]:
    """직전 다운로드 기록. 없거나 깨졌으면 빈 dict — 다시 받으면 그만이다."""
    path = Path(raw_dir) / MANIFEST_NAME
    try:
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def write_manifest(raw_dir: Path, entries: dict[str, dict]) -> None:
    """원자적으로 쓴다 — 빌드 중 죽어도 반쪽 매니페스트가 남으면 안 된다."""
    raw_dir = Path(raw_dir)
    raw_dir.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=raw_dir, prefix=".manifest-", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(entries, fh, indent=2, sort_keys=True, ensure_ascii=False)
            fh.write("\n")
        os.chmod(tmp, 0o644)  # mkstemp 는 0600 — 운영 중 사람이 읽는 파일이다
        os.replace(tmp, raw_dir / MANIFEST_NAME)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise


def needs_download(path: Path, vendor_modified: str, *, manifest: dict[str, dict]) -> bool:
    """이 파일을 (다시) 받아야 하는가.

    네 가지를 모두 본다. 매니페스트만 믿으면 파일이 지워진 걸 모르고 영원히
    스킵하고, 파일 존재만 믿으면 중단된 반쪽 zip 을 '최신'으로 오인한다.
    크기가 같아도 내용이 상할 수 있어 체크섬까지 본다.
    """
    path = Path(path)
    if not path.exists():
        return True
    record = manifest.get(path.name)
    if not record:
        return True
    if record.get("modified") != vendor_modified:
        return True
    if record.get("size") != path.stat().st_size:
        return True
    recorded = record.get("sha256")
    # 기존 매니페스트에는 sha256 이 없다. 없다고 다시 받으면 첫 실행에
    # 4.6GB 를 전량 재전송한다 — 미검증으로 두고 다음에 받을 때 채운다.
    if recorded and file_sha256(path) != recorded:
        return True
    return False


def sweep_orphan_parts(raw_dir: Path, *, max_age: int = ORPHAN_PART_MAX_AGE) -> list[str]:
    """죽은 런이 남긴 `.part` 를 치운다. 반환값은 지운 파일 이름들.

    `download()` 의 `except BaseException` 은 파이썬 예외만 잡는다. SIGKILL 로
    죽으면 최대 985MB 짜리 임시 파일이 그대로 남고 아무도 안 치운다.

    **나이 조건이 안전장치의 전부다** — 지금 도는 런의 `.part` 를 지우면 그
    다운로드가 통째로 날아간다. 가장 큰 테이블도 10분 안에 끝나므로 6시간이면
    "이건 확실히 죽은 런의 것" 이다.
    """
    raw_dir = Path(raw_dir)
    if not raw_dir.is_dir():
        return []
    cutoff = time.time() - max_age
    removed = []
    for part in raw_dir.glob(".*.part"):
        try:
            if part.stat().st_mtime >= cutoff:
                continue
            size = part.stat().st_size
            part.unlink()
        except OSError:
            # 다른 런이 방금 치웠거나 권한이 없다 — 청소는 부수 작업이므로
            # 여기서 동기화를 죽이지 않는다.
            continue
        removed.append(part.name)
        print(f"🧹 고아 임시 파일 삭제: {part.name} ({size/1e6:.1f}MB)", flush=True)
    return removed


def download(
    table: str, dest: Path, *, api_key: str, opener=urllib.request.urlopen
) -> tuple[int, str]:
    """벌크 zip 을 받아 원자적으로 배치한다. 반환값은 `(바이트수, sha256)`.

    임시 파일에 받고, **온전한 zip 인지 확인한 뒤에만** `os.replace` 한다 —
    반쪽 zip 이 목적지에 남으면 다음 실행이 그걸 정상으로 보고 그 테이블만
    영구히 낡은 채로 남는다.
    """
    dest = Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=dest.parent, prefix=f".{table}-", suffix=".part")
    written = 0
    try:
        with os.fdopen(fd, "wb") as out, opener(
            bulk_url(table, api_key=api_key), timeout=SOCKET_TIMEOUT
        ) as resp:
            # 헤더가 없는 응답 객체(테스트 더미, 일부 프록시)도 있으므로 방어한다.
            headers = getattr(resp, "headers", None)
            declared = headers.get("Content-Length") if headers is not None else None
            while True:
                block = resp.read(1 << 22)
                if not block:
                    break
                out.write(block)
                written += len(block)
        # 절단을 여기서 잡는다. 아래 sha256 은 **받은 바이트로 계산해 그 바이트를
        # 기록하는 자기참조**라 전송 손상을 원리적으로 못 잡는다(그건 받은 뒤
        # 디스크에서 썩는 것만 잡는다). 벤더가 길이를 알려줄 때는 그게 가장
        # 이르고 명확한 절단 신호다.
        if declared is not None and int(declared) != written:
            raise OSError(
                f"{table}: 전송이 잘렸다 — Content-Length {int(declared):,}B "
                f"≠ 받은 {written:,}B"
            )
        verify_zip(Path(tmp))
        digest = file_sha256(Path(tmp))
        # mkstemp 는 0600 으로 만든다. raw 아카이브는 컨테이너(airflow)가 쓰고
        # 호스트의 연구 도구가 읽으므로, 읽기는 열어둔다.
        os.chmod(tmp, 0o644)
        os.replace(tmp, dest)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise
    return written, digest


def _display_width(text: str) -> int:
    """터미널에서 차지하는 칸 수. 한글·전각 문장부호는 2칸이다."""
    return sum(2 if unicodedata.east_asian_width(ch) in "WF" else 1 for ch in text)


def _pad(text: str, width: int, *, right: bool = False) -> str:
    """표 정렬용 패딩. `f"{s:<20}"` 은 **문자 수**로 세어 한글 열이 어긋난다."""
    fill = " " * max(0, width - _display_width(text))
    return fill + text if right else text + fill


def render_report(
    manifest: dict[str, dict],
    *,
    missing: tuple[str, ...],
    fetched: set[str],
    now: str,
    failed: dict[str, str] | None = None,
) -> str:
    """14개 전부의 동기화 상태를 한 표로.

    요구사항은 "매일 최신인지 **확인**되어야 한다" 이고, 확인의 산출물이
    이 표다. 줄이 빠지면 그게 곧 안 보이는 구멍이므로 구독 목록 전체를
    무조건 훑는다 — 매니페스트에 있는 것만 찍지 않는다.
    """
    lines = [
        f"=== Sharadar 동기화 상태 ({now}) ===",
        _pad("테이블", 20) + _pad("벤더 modified", 24)
        + _pad("크기", 10, right=True) + _pad("정체", 6, right=True) + "  판정",
    ]
    failed = failed or {}
    fresh = warn = 0
    for table in SUBSCRIBED_TABLES:
        entry = manifest.get(f"{table}.csv.zip", {})
        if table in failed:
            lines.append(
                _pad(table, 20) + _pad(str(entry.get("vendor_modified", "—")), 24)
                + _pad("—", 10, right=True) + _pad("—", 6, right=True)
                + f"  ❌ 실패 ({failed[table][:40]})"
            )
            warn += 1
            continue
        if table in missing:
            lines.append(
                _pad(table, 20) + _pad("—", 24) + _pad("—", 10, right=True)
                + _pad("—", 6, right=True) + "  ⚠️ 벤더 목록에 없음"
            )
            continue
        streak = int(entry.get("unchanged_streak", 0))
        size_mb = f"{int(entry.get('size', 0)) / 1e6:.1f}MB"
        if table in fetched:
            verdict = "⬇ 새로 받음"
            fresh += 1
        elif is_stale(table, entry):
            verdict = f"⚠️ {streak}회 연속 정체"
            warn += 1
        else:
            verdict = "✓ 최신"
            fresh += 1
        lines.append(
            _pad(table, 20) + _pad(str(entry.get("vendor_modified", "—")), 24)
            + _pad(size_mb, 10, right=True) + _pad(str(streak), 6, right=True)
            + f"  {verdict}"
        )
    lines.append(
        f"{len(SUBSCRIBED_TABLES)}개 중 최신 {fresh} · 주의 {warn} · "
        f"누락 {len(missing)} · 실패 {len(failed)}"
    )
    return "\n".join(lines)


def sync(
    raw_dir: Path,
    *,
    api_key: str,
    now: str | None = None,
    opener=urllib.request.urlopen,
) -> dict[str, dict]:
    """구독 14개를 벤더 최신본과 맞춘다. 반환값은 갱신된 매니페스트.

    **전송이 없는 테이블도 반드시 `record_check` 를 거친다** — 안 그러면
    "오늘 확인했다" 가 안 남아, 확인한 것인지 확인 자체를 안 한 것인지
    구별할 수 없다. 요구사항의 절반이 여기 걸려 있다.
    """
    raw_dir = Path(raw_dir)
    now = now or time.strftime("%Y-%m-%dT%H:%M:%S%z")
    sweep_orphan_parts(raw_dir)
    listing = fetch_listing(api_key=api_key, opener=opener)
    plan, missing = plan_sync(listing)
    manifest = read_manifest(raw_dir)

    if missing:
        print(f"⚠️  벤더 목록에 없는 테이블: {', '.join(missing)}", flush=True)

    fetched: set[str] = set()
    failed: dict[str, str] = {}
    total_bytes = 0
    for table, modified in plan.items():
        dest = raw_dir / f"{table}.csv.zip"
        # **테이블마다 격리한다.** 예전엔 holdings 하나가 HTTP 에러를 내면
        # 뒤따르는 metrics·sp500·tickers·descriptions 가 대조조차 안 됐고,
        # 더 나쁘게는 아래 상태 표에 도달을 못 해 "매 실행 14개 확인" 이라는
        # 이 모듈의 핵심 산출물이 **문제가 있는 날에만** 안 남았다.
        try:
            if needs_download(dest, modified, manifest=manifest):
                started = time.monotonic()
                size, digest = download(table, dest, api_key=api_key, opener=opener)
                elapsed = time.monotonic() - started
                rate = size / elapsed / 1e6 if elapsed else 0
                print(
                    f"⬇  {table:18s} {size/1e6:8.1f}MB  {elapsed:6.1f}s  "
                    f"{rate:5.1f}MB/s  ({modified})",
                    flush=True,
                )
                manifest[dest.name] = {
                    **manifest.get(dest.name, {}),
                    "modified": modified,
                    "size": size,
                    "sha256": digest,
                }
                fetched.add(table)
                total_bytes += size
            manifest[dest.name] = record_check(manifest.get(dest.name), modified, now=now)
        except Exception as exc:  # noqa: BLE001 — 표를 찍고 나서 다시 던진다
            failed[table] = f"{type(exc).__name__}: {exc}"
            print(f"❌ {table}: {failed[table]}", flush=True)
        # 매 테이블마다 기록한다 — 17분짜리 작업이 중간에 죽어도 여기까지는 남는다.
        write_manifest(raw_dir, manifest)

    print(
        render_report(manifest, missing=missing, fetched=fetched, failed=failed, now=now),
        flush=True,
    )
    print(
        f"✅ 전송 {total_bytes/1e6:.0f}MB · 새로 받음 {len(fetched)}개 · "
        f"확인만 {len(plan) - len(fetched) - len(failed)}개 · 실패 {len(failed)}개",
        flush=True,
    )
    if failed:
        # 표를 남긴 뒤에 죽는다 — Airflow 가 실패로 기록해야 재시도가 걸린다.
        raise RuntimeError(
            f"{len(failed)}개 테이블 동기화 실패: {', '.join(sorted(failed))}"
        )
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--raw-dir",
        default=os.environ.get("US_RAW_DIR", "/opt/us-data/sharadar/raw"),
    )
    args = parser.parse_args(argv)

    api_key = os.environ.get("SHARADAR_API_KEY")
    if not api_key:
        print("❌ SHARADAR_API_KEY 가 없습니다", file=sys.stderr)
        return 2

    sync(Path(args.raw_dir), api_key=api_key)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
