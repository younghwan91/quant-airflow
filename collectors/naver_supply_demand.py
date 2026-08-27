"""상장폐지 종목의 수급을 네이버에서 **부분** 백필 — 생존편향 마지막 층.

키움 ``ka10059`` 는 폐지 코드에 ``return_code=0``(성공) + **0행**을 준다(2026-08-15
실측, ``ka10081`` 일봉과 같은 조용한 빈 응답). 네이버 ``frgn.naver`` 는 폐지분도
주지만 — 2009년 폐지 종목까지 확인 — 항목이 일부뿐이다.

**부분이라는 말의 정확한 뜻:**

    채운다:   close, acc_trde_qty, institution, foreign_
    못 채운다: individual, etc_corp, 기관 세부 8종, flu_rt

그래서 **개인 순매매를 쓰는 연구는 이 데이터로 재현할 수 없다**(예: contrarian_retail).
못 채우는 컬럼은 ``NULL`` 로 남긴다 — 0 은 "순매매 없음"이고 NULL 은 "모름"이라,
0으로 채우면 폐지 종목이 "그날 개인이 안 샀다"로 읽혀 신호가 조용히 왜곡된다.

**외국인 정의가 키움과 다르다.** 네이버 값은 개인+외국인+기관+기타법인 합이 0이 되도록
맞춘 수치이고, 키움 ``foreign_`` 은 순수 외국인이라 잔차가 남는다. 삼성전자 2026-08-12
실측: 키움 5,818,519 vs 네이버 5,802,466 — 차이 16,053 이 그날의 잔차와 정확히 일치한다.
그래서 행마다 ``source`` 를 남긴다(migration 006).

CLI:
    python -m collectors.naver_supply_demand --db <DSN>            # 폐지 종목 전체
    python -m collectors.naver_supply_demand --db <DSN> --limit 20 # 표본
    python -m collectors.naver_supply_demand --db <DSN> --dry-run  # 조회만
"""

from __future__ import annotations

import argparse
import re
import time
import urllib.error
import urllib.request

from .storage import (
    CHECKED_NAVER_FLOW,
    _upsert,
    connect,
    default_db_path,
    fetchall,
    mark_checked,
)

URL = "https://finance.naver.com/item/frgn.naver"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")

# 우리 일봉 이력의 시작. 이보다 앞선 구간은 겹치는 시세가 없어 쓸 데가 없다.
HISTORY_START = "2016-09-09"

# 데이터 행: 날짜 셀로 시작해 </tr> 까지. 숫자 셀은 tah p11 span 안에 있고
# 순서가 [종가, 전일비, 거래량, 기관순매매, 외국인순매매, 외국인보유주수] 다.
_ROW_RE = re.compile(
    r'<span class="tah p10 gray03">(\d{4})\.(\d{2})\.(\d{2})</span>(.*?)</tr>', re.S)
_NUM_RE = re.compile(r'<span class="tah p11[^"]*">\s*([+\-]?[\d,]+)\s*</span>')

SD_NAVER_COLS = ["code", "date", "close", "acc_trde_qty",
                 "institution", "foreign_", "source"]


def _to_int(s: str) -> int | None:
    txt = s.replace(",", "").replace("+", "").strip()
    try:
        return int(txt)
    except ValueError:
        return None


def parse_flow(html: str, code: str, *, since: str = HISTORY_START) -> list[tuple]:
    """frgn.naver 본문 → ``SD_NAVER_COLS`` 순서의 행 리스트 (순수함수).

    ``since`` 이전 날짜는 버린다. 숫자 셀이 6개 미만인 행(헤더·광고 등)도 버린다 —
    위치로 읽으므로 개수가 안 맞으면 값이 밀린다.
    """
    out: list[tuple] = []
    for y, m, d, body in _ROW_RE.findall(html):
        date = f"{y}-{m}-{d}"
        if date < since:
            continue
        nums = _NUM_RE.findall(body)
        if len(nums) < 6:
            continue
        close, _prev_diff, volume, inst, foreign = (
            _to_int(nums[0]), nums[1], _to_int(nums[2]),
            _to_int(nums[3]), _to_int(nums[4]))
        if close is None or close <= 0:
            continue
        out.append((code, date, close, volume, inst, foreign, "naver"))
    return out


def fetch_page(code: str, page: int, *, retries: int = 3, timeout: int = 30) -> str:
    """한 페이지 HTML. 실패 시 빈 문자열(호출부가 '없음'과 동일 취급)."""
    req = urllib.request.Request(f"{URL}?code={code}&page={page}",
                                 headers={"User-Agent": UA})
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:  # noqa: S310 — 고정 호스트
                return r.read().decode("euc-kr", "ignore")
        except (urllib.error.URLError, TimeoutError, OSError):
            if attempt < retries - 1:
                time.sleep(1.5 * (attempt + 1))
    return ""


def fetch_flow(code: str, *, since: str = HISTORY_START, max_pages: int = 120,
               sleep: float = 0.25) -> list[tuple]:
    """페이지를 넘기며 ``since`` 까지 수집. 한 페이지는 ~20 거래일.

    멈추는 조건 두 가지: 페이지가 비었거나(더 과거 데이터 없음), 그 페이지의 가장
    오래된 날짜가 ``since`` 보다 이르다(구간 끝). ``max_pages`` 는 안전장치다 —
    120페이지 ≈ 2,400 거래일 ≈ 10년으로 우리 구간을 덮는다.
    """
    rows: list[tuple] = []
    for page in range(1, max_pages + 1):
        html = fetch_page(code, page)
        if not html:
            break
        page_rows = parse_flow(html, code, since=since)
        oldest_on_page = _oldest_date(html)
        rows.extend(page_rows)
        time.sleep(sleep)
        if not oldest_on_page or oldest_on_page < since:
            break
    # 페이지 경계 중복 제거(같은 날짜가 두 페이지에 걸치는 경우).
    seen: set[str] = set()
    uniq = []
    for r in rows:
        if r[1] in seen:
            continue
        seen.add(r[1])
        uniq.append(r)
    return uniq


def _oldest_date(html: str) -> str:
    dates = [f"{y}-{m}-{d}" for y, m, d, _ in _ROW_RE.findall(html)]
    return min(dates) if dates else ""


def _mark_checked(con, codes: list[str], today: str) -> None:
    """네이버가 빈 응답을 준 코드를 기록해 다음 회차부터 건너뛴다.

    `naver_delisted_bars._mark_checked` / `dart_shares._mark_checked` 와 같은
    패턴이다. 이게 없으면 빈 응답 코드는 supply_demand 행이 안 생겨 제외 조건에도
    안 걸리고 **영원히 재조회된다** — `fetch_flow` 가 종목당 최대 120페이지를
    넘기므로(max_pages) 그런 코드 하나가 매주 수십~120요청을 태운다. 2026-08-15
    첫 실행이 175.3분 걸린 게 그 페이지 비용의 크기다.
    """
    mark_checked(con, CHECKED_NAVER_FLOW, codes, today)


def _targets(con, *, refetch: bool = False) -> list[str]:
    """폐지 시세는 있는데 수급이 없는 종목. 이미 있는 코드는 건너뛴다(재실행 안전).

    빈 응답으로 판명돼 ``naver_sd_checked`` 가 찍힌 코드도 제외한다 —
    :func:`_mark_checked` 주석 참고. ``refetch=True`` 면 그 마커를 무시한다.
    """
    checked_filter = "" if refetch else (
        "  AND NOT EXISTS (SELECT 1 FROM backfill_markers m "
        f"                  WHERE m.code = b.code AND m.source = '{CHECKED_NAVER_FLOW}') "
    )
    sql = (
        "SELECT DISTINCT b.code FROM daily_bars b "
        "WHERE b.source = 'naver' "
        "  AND NOT EXISTS (SELECT 1 FROM supply_demand s WHERE s.code = b.code) "
        f"{checked_filter}"
        "ORDER BY b.code"
    )
    return [r[0] for r in fetchall(con, sql)]


def _write(con, records: list[tuple]) -> int:
    """부분 행 삽입. 기존 키움 행은 절대 덮지 않는다(정의가 다른 값이라 섞이면 안 된다)."""
    if not records:
        return 0
    return _upsert(con, "supply_demand", SD_NAVER_COLS, records, on_conflict="nothing")


def main() -> int:
    ap = argparse.ArgumentParser(description="폐지 종목 수급 부분 백필 (네이버)")
    ap.add_argument("--db", default=None)
    ap.add_argument("--limit", type=int, default=0, help="상위 N종목만 (0=전체)")
    ap.add_argument("--sleep", type=float, default=0.25, help="페이지 간 대기(초)")
    ap.add_argument("--since", default=HISTORY_START, help="이 날짜 이후만 (YYYY-MM-DD)")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument(
        "--refetch", action="store_true",
        help="naver_sd_checked 로 제외된 코드까지 다시 조회",
    )
    args = ap.parse_args()

    from .config import mask_dsn

    con = connect(args.db or default_db_path())
    codes = _targets(con, refetch=args.refetch)
    if args.limit:
        codes = codes[: args.limit]
    print(f"🔌 {mask_dsn(args.db)} | 대상 {len(codes)}종목 | since={args.since}"
          f"{' | DRY-RUN' if args.dry_run else ''}", flush=True)

    found = empty = written = 0
    exhausted: list[str] = []
    t0 = time.time()
    for i, code in enumerate(codes, 1):
        rows = fetch_flow(code, since=args.since, sleep=args.sleep)
        if not rows:
            empty += 1
            exhausted.append(code)
        else:
            found += 1
            if not args.dry_run:
                written += _write(con, rows)
        if i % 25 == 0 or i == len(codes):
            el = time.time() - t0
            rate = i / el if el else 0
            print(f"  [{i}/{len(codes)}] 확보={found} 없음={empty} 기록={written}행 "
                  f"| {rate:.1f}종목/s ETA {(len(codes)-i)/rate/60 if rate else 0:.1f}분",
                  flush=True)

    if not args.dry_run:
        _mark_checked(con, exhausted, time.strftime("%Y-%m-%d"))
    con.close()
    print(f"DONE targets={len(codes)} 확보={found} 없음={empty} 기록={written} "
          f"완료표시={len(exhausted) if not args.dry_run else 0}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
