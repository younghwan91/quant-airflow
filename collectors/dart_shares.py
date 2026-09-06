"""상장폐지 종목의 상장주식수를 DART에서 백필 — 생존편향의 마지막 데이터 층.

``shares_outstanding_history`` 는 시가총액의 분모다. 폐지 종목이 여기 없으면 cap 기반
유니버스(``features/universe.py`` 의 ``cap_band``/``cap_rank``)가 **여전히 생존자만
담는다** — 시세·실적을 아무리 메워도 그 경로는 편향이 남는다(GUARDRAILS §4 공백 2).

**KRX 경로는 막혔다.** ``krx_shares.py`` 가 쓰는 ``MDCSTAT01501``(전종목 시세)은
날짜 파라미터·전종목·무인증이라 원래 이 용도에 이상적이었으나, KRX 가 MDCSTAT 계열에
회원 로그인을 걸면서 **응답이 0행**이 됐다(2026-08-15 실측. ``krx_delisted.py`` 의
2026-07 감사 기록과 일치).

**DART ``stockTotqySttus``(주식의 총수 현황)는 폐지 기업도 준다.** 실측 커버리지:

    사업보고서만 조회        → 12종목 중 5 (42%)
    분기·반기까지, 3개 연도  → 12종목 중 11 (92%)

폐지 직전 해에는 사업보고서를 못 낸 경우가 많아서 분기보고서까지 훑어야 한다.
미확보 1건은 우선주 코드라 DART corp_code 매핑 자체가 없다.

**필드 선택.** 삼성전자 2025 사업보고서 기준::

    istc_totqy       5,919,637,922   발행주식총수   ← 우리 상장주식수 규약에 대응
    tesstk_co           91,828,987   자기주식수
    distb_stock_co   5,827,808,935   유통주식수     ← 쓰면 시총이 과소 계상된다

``distb_stock_co`` 는 자기주식을 뺀 값이라 시가총액 분모로 쓰면 안 된다.

**시점 정합.** 응답의 ``stlm_dt`` 가 그 수치의 기준일이고 ``rcept_no`` 앞 8자리가
공시 접수일이다. 둘이 다르므로(예: 2025-12-31 기준을 2026-03-10 에 공시) 백테스트가
"언제 알 수 있었나"를 물으면 접수일을 봐야 한다 — earnings 의 avail/knowledge 축과
같은 문제다. 이 수집기는 **기준일(stlm_dt)을 date 로** 넣는다. 기존 행(키움/KRX)이
"그날의 상장주식수"라는 같은 의미이고, 그래야 ``market_cap_asof`` 의 backward as-of
조회가 섞이지 않는다. 접수일은 ``knowledge_date`` 로 함께 남겨 나중에 PIT 를 조일 수
있게 한다(migration 005).

fetch/parse·보고서 폴백(사업→3분기→반기→1분기)·corp_code 매핑은
krx-fundamentals-client의 ``DartScraper.fetch_shares_outstanding``으로 옮겼다
(2026-09-06) — dart_earnings.py가 DartScraper로 옮겨간 것과 같은 이유다. 이
파일은 연도별 시계열 조립·키 로테이션·대상 선정·DB 적재·CLI만 맡는다.
``collectors/dart.py``(자체 로테이션 유틸)는 이 파일이 마지막 소비자였으므로
같이 지운다 — async DartQuotaExceededError 기반 로테이션은
``dart_earnings.py``가 이미 쓰는 패턴을 그대로 재사용한다.

라이브러리가 돌려주는 ``stlm_dt``/``knowledge_date``는 대시 없는 ``YYYYMMDD``다 —
이 테이블(``shares_outstanding_history.date``)은 기존 행(키움/KRX)과 문자열 비교로
as-of 조회를 하므로(``market_cap_asof``, ``_listed_targets``의 ``date < '2026-01-01'``)
``YYYY-MM-DD``로 변환해서 적재한다.

CLI:
    python -m collectors.dart_shares --db <DSN>              # 폐지 종목 전체
    python -m collectors.dart_shares --db <DSN> --limit 20   # 표본
    python -m collectors.dart_shares --db <DSN> --dry-run    # 조회만
"""

from __future__ import annotations

import argparse
import asyncio
import time

from krx_fundamentals_client import DartQuotaExceededError, DartScraper

from .storage import (
    CHECKED_DART_SHARES,
    CHECKED_DART_SHARES_LISTED,
    connect,
    default_db_path,
    fetchall,
    mark_checked,
)


def _dashed(yyyymmdd: str) -> str:
    """``"20251231"`` → ``"2025-12-31"``. 이미 대시가 있으면 그대로 둔다."""
    if len(yyyymmdd) == 8 and yyyymmdd.isdigit():
        return f"{yyyymmdd[:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:]}"
    return yyyymmdd


async def _fetch_shares_with_rotation(
    scrapers: dict[str, DartScraper], keys: list[str], ki: list[int],
    ticker: str, year: int,
) -> tuple[int, str, str] | None:
    """한 (종목, 연도) 조회, 일한도(020)를 만나면 다음 키로 로테이션.

    ``dart_earnings._fetch_with_rotation``과 같은 계약이다 — ``ki``는 1칸
    리스트로 현재 키 인덱스를 담고, 호출부가 종목 루프 전체에서 재사용해야
    이미 소진된 키로 매번 되돌아가지 않는다. 반환은
    ``(shares_outstanding, stlm_dt, knowledge_date)``(둘 다 ``YYYY-MM-DD``)
    또는 그 연도에 자료가 없으면 ``None``.
    """
    while True:
        scraper = scrapers[keys[ki[0]]]
        try:
            result = await scraper.fetch_shares_outstanding(ticker, year)
        except DartQuotaExceededError:
            if ki[0] + 1 >= len(keys):
                return None
            ki[0] += 1
            print(f"DART 키 일한도(020) 도달 → 키{ki[0] + 1}로 로테이션 (stockTotqySttus)",
                  flush=True)
            continue
        if result is None or not result.shares_outstanding:
            return None
        return result.shares_outstanding, _dashed(result.stlm_dt), _dashed(result.knowledge_date)


async def shares_series(
    scrapers: dict[str, DartScraper], keys: list[str], ticker: str,
    first_year: int, last_year: int, *, ki: list[int] | None = None,
    sleep: float = 0.15,
) -> list[tuple[int, str, str]]:
    """연도별 ``(발행주식총수, 기준일, 접수일)`` **시계열**. 없으면 빈 리스트.

    **왜 시계열이어야 하나(2026-08-15 실측으로 배움).** 처음엔 "마지막으로 알려진
    주식수" 1건만 받았는데 쓸모가 없었다. ``market_cap_asof`` 는 ``date <= 조회일``
    로 역방향 as-of 를 하는데, 종목당 1행뿐이고 그 행의 기준일이 폐지일보다 뒤인
    경우가 35%(413건 중 145건)였다 — 그 종목의 **모든 거래일에서 시총이 NULL** 이
    된다. 나머지도 생애의 꼬리 구간만 덮는다. 유니버스에 넣으려면 거래 기간을
    가로지르는 점들이 있어야 한다.

    연도마다 사업보고서를 먼저 보고, 없으면 분기·반기를 훑는 폴백은
    ``DartScraper.fetch_shares_outstanding`` 내부가 담당한다(연 1점 확보).

    **키 로테이션을 쓴다(2026-08-28).** 예전엔 ``keys[0]`` 하나만 받아서 하루
    한도가 20,000콜로 묶였다. 상장 종목 과거 주식수 백필이 2,595종목 × ~9.5콜 ≈
    24,650콜이라 한도를 넘었고, 이틀에 나눠 돌린 그 분할이 ``ORDER BY code`` 와
    겹쳐 **시대별로 기울어진 중간 상태**를 만들었다 — 2023~24년 상장분의 87%가
    미처리로 남아, 그걸 모르고 소비하면 유니버스가 시대마다 다르게 좁혀진다.
    키 3개면 60,000콜/일이라 한 번에 끝난다.

    ``ki`` 는 현재 키 인덱스를 담은 1칸 리스트다. 종목 루프를 도는 호출부가
    **같은 리스트를 넘겨 재사용해야** 한다 — 종목마다 새로 만들면 이미 소진된
    키로 매번 되돌아가 한도 오류를 한 번씩 더 맞는다.
    """
    ki = ki if ki is not None else [0]
    out: list[tuple[int, str, str]] = []
    for year in range(first_year, last_year + 1):
        point = await _fetch_shares_with_rotation(scrapers, keys, ki, ticker, year)
        await asyncio.sleep(sleep)
        if point:
            out.append(point)
    return out


def _targets(con, *, refetch: bool = False) -> list[tuple[str, str, str]]:
    """``(code, 첫 거래일, 마지막 거래일)`` — 폐지 시세는 있는데 주식수가 없는 종목.

    거래 구간을 함께 돌려주는 이유: 그 구간을 가로지르는 주식수 점들이 있어야
    ``market_cap_asof`` 의 역방향 as-of 가 값을 찾는다(:func:`shares_series` 참고).

    이미 주식수가 있는 코드는 건너뛴다(재실행 안전). 시세가 없는 폐지 종목은 대상이
    아니다 — 시총을 계산할 가격 자체가 없다.

    **``dart_checked`` 가 찍힌 코드도 건너뛴다.** 그게 없을 때는 "DART 에 자료가
    없음"(missing)이나 "corp_code 매핑 없음"(no_corp)인 종목이 어디에도 기록되지
    않아, 주식수가 영원히 안 생기고 따라서 이 쿼리에 영원히 걸렸다 — 실측 42종목이
    주당 2.2분을 성과 0행으로 태우고 있었다. 상장폐지는 과거 사실이라 한 번
    자료가 없으면 영원히 없다. 다시 훑으려면 ``--refetch``.
    """
    checked_filter = "" if refetch else (
        "  AND NOT EXISTS (SELECT 1 FROM backfill_markers m "
        f"                  WHERE m.code = b.code AND m.source = '{CHECKED_DART_SHARES}') "
    )
    sql = (
        "SELECT b.code, min(b.date), max(b.date) FROM daily_bars b "
        "WHERE b.source = 'naver' "
        "  AND NOT EXISTS (SELECT 1 FROM shares_outstanding_history s "
        "                  WHERE s.code = b.code) "
        f"{checked_filter}"
        "GROUP BY b.code ORDER BY b.code"
    )
    return [(r[0], str(r[1]), str(r[2])) for r in fetchall(con, sql)]


def _listed_targets(con, *, from_year: int, to_year: int) -> list[tuple[str, str, str]]:
    """``(code, 시작연도, 종료연도)`` — **현재 상장 종목** 중 과거 주식수가 없는 것.

    :func:`_targets` 의 폐지 종목 짝이다. 폐지분은 생존편향 때문에 이미 채웠는데,
    정작 **살아 있는 유니버스의 과거 시총이 계산되지 않는다.** 실측 2026-08-27:

        SELECT count(*) FROM stocks k WHERE EXISTS (
          SELECT 1 FROM shares_outstanding_history s
          WHERE s.code=k.code AND s.date <= '2018-06-01' AND s.shares_outstanding > 0)
        → 5

    2016~2025 에 있는 190~261 종목은 **전부 폐지분(source='dart')** 이라 상장
    마스터와 교집합이 없다. 키움 주간 피드(ka10001)는 현재 스냅샷만 주므로
    2026년부터만 쌓인다. 결과적으로 시총을 분모로 쓰는 백테스트는 2026년 이전
    구간에서 대부분 NaN 이 되고, **값이 나오는 소수는 폐지 종목 쪽으로 치우친
    표본**이다 — 조용히 틀리는 종류다(kr-quant 기관수급 알파가 이걸로 VOID 됐다).

    **재개 단위는 종목이다.** ``2026-01-01`` 이전 주식수 점이 하나라도 있으면
    건너뛴다 — 연 단위로 따지지 않으므로, 중간에 끊긴 종목은 다시 전 연도를
    조회한다. DART 호출이 비싸지만(종목·연도당 1~4콜) 종목 단위 스킵만으로도
    이어받기는 성립한다.
    """
    ph_from = f"{from_year}-01-01"
    ph_to = f"{to_year}-12-31"
    # **정렬을 코드순으로 두면 안 된다.** 한국 종목코드는 상장 시기순에 가까워서,
    # `ORDER BY code` + `--limit` 으로 자른 부분집합이 곧 **시대 표본추출**이 된다.
    # 실측 2026-08-27: 2,595종목 중 앞 2,000개를 먼저 돌렸더니 2016년 상장분은
    # 97% 완료인데 2023~24년 상장분은 87%가 미처리로 남았다. 그 상태를 모르고
    # 소비하면 유니버스가 시대마다 다르게 좁혀진다 — 폴드 간 비교가 구성 변화를
    # 알아채지 못하는, 조용히 틀리는 종류다.
    #
    # md5 로 결정론적 셔플한다. 재개·재현성은 코드순과 똑같이 유지되면서
    # (같은 입력 → 같은 순서), 중간에 멈춘 부분집합은 시대별로 균일해진다.
    sql = (
        "SELECT b.code, min(b.date), max(b.date) FROM daily_bars b "
        "WHERE b.source <> 'naver' AND b.date >= ? AND b.date <= ? "
        "GROUP BY b.code "
        "HAVING NOT EXISTS (SELECT 1 FROM shares_outstanding_history s "
        "                   WHERE s.code = b.code AND s.date < '2026-01-01') "
        # 조회했는데 DART 에 보고서가 아예 없던 코드는 뺀다. 이 절이 없어서
        # 60종목을 매 회차 다시 조회했다(종목당 최대 4보고서 × 연수).
        f"   AND NOT EXISTS (SELECT 1 FROM backfill_markers m "
        f"                   WHERE m.code = b.code AND m.source = '{CHECKED_DART_SHARES_LISTED}') "
        "ORDER BY md5(b.code)"
    )
    return [(r[0], str(r[1]), str(r[2])) for r in fetchall(con, sql, (ph_from, ph_to))]


def _write(con, records: list[tuple]) -> int:
    """(code, date, shares, knowledge_date, source) 삽입. 기존 행은 보존."""
    if not records:
        return 0
    from .storage import _upsert
    return _upsert(con, "shares_outstanding_history",
                   ["code", "date", "shares_outstanding", "knowledge_date", "source"],
                   records, on_conflict="nothing")


async def _run(args: argparse.Namespace) -> int:
    from .config import mask_dsn
    from .dart_earnings import collect_keys

    keys = collect_keys()
    if not keys:
        raise SystemExit("환경변수 DART_API_KEY 필요")

    # **대상을 먼저 센다.** corp_map 로드(수 MB zip)는 그 다음이다(스크레이퍼가
    # 첫 fetch 때 알아서 지연 로드한다).
    #
    # 순서가 반대였을 때: 이 수집기는 마커 덕에 정상 상태에서 대상이 0이라 몇 초
    # 만에 끝나야 하는데, 할 일이 없어도 corp_map 을 먼저 받다가 DART 가 잠깐
    # 불안정하면 그대로 실패했다. 실측 2026-08-28 — 대상 0종목인 상태에서
    # `RuntimeError: DART corpCode 오류 (status='800')` (800 = 시스템 점검)으로 죽었다.
    # 월간 유지보수 DAG 가 "할 일 없음"을 벤더 가용성에 의존해 알아내면 안 된다.
    con = connect(args.db or default_db_path())
    if args.listed:
        targets = _listed_targets(con, from_year=args.from_year, to_year=args.to_year)
    else:
        targets = _targets(con, refetch=args.refetch)
    if args.limit:
        targets = targets[: args.limit]
    if not targets:
        con.close()
        print("대상 0종목 — 받을 게 없다(백필 완료 상태). corp_map 로드 없이 종료.",
              flush=True)
        return 0

    scrapers = {key: DartScraper(api_key=key) for key in keys}
    print(f"🔌 {mask_dsn(args.db)} | 대상 {len(targets)}종목"
          f"{' | DRY-RUN' if args.dry_run else ''}", flush=True)

    # 키 인덱스는 종목 루프 전체에서 공유한다 — 종목마다 [0] 으로 되돌리면
    # 이미 소진된 키에 매번 한 번씩 더 부딪힌다.
    ki = [0]
    found = missing = written = 0
    # 이번 회차에 "DART 에 자료 없음"으로 판명된 코드 — 끝에 한 번에 마킹한다.
    # (corp_code 매핑이 없는 경우도 fetch_shares_outstanding이 None을 돌려주므로
    # missing과 구분하지 않는다 — 어차피 재조회 대상에서 빠지는 처리는 동일하다.)
    exhausted: list[str] = []
    t0 = time.time()
    try:
        for i, (code, first, last) in enumerate(targets, 1):
            first_year, last_year = int(first[:4]), int(last[:4])
            if args.listed:
                first_year = max(first_year, args.from_year)
                last_year = min(last_year, args.to_year)
            series = await shares_series(scrapers, keys, code, first_year, last_year,
                                          ki=ki, sleep=args.sleep)
            if not series:
                missing += 1
                exhausted.append(code)
                continue
            found += 1
            if not args.dry_run:
                written += _write(con, [(code, stlm, shares, rcept, "dart")
                                        for shares, stlm, rcept in series])
            if i % 50 == 0 or i == len(targets):
                el = time.time() - t0
                rate = i / el if el else 0
                print(f"  [{i}/{len(targets)}] 확보={found} 못찾음={missing} "
                      f"기록={written}행 | {rate:.1f}종목/s "
                      f"ETA {(len(targets)-i)/rate/60 if rate else 0:.1f}분", flush=True)
    finally:
        for scraper in scrapers.values():
            await scraper.close()

    # 마커는 (code, source) 테이블이라 상장·폐지가 같은 경로를 쓴다. 예전엔 이게
    # delisted_stocks 의 컬럼이라 --listed 는 남길 자리가 없었고, 그래서 자료가 없는
    # 60종목을 매 회차 다시 조회했다.
    if not args.dry_run:
        source = CHECKED_DART_SHARES_LISTED if args.listed else CHECKED_DART_SHARES
        mark_checked(con, source, exhausted, time.strftime("%Y-%m-%d"))
    con.close()
    print(f"DONE targets={len(targets)} 확보={found} "
          f"못찾음={missing} 기록={written}행 "
          # --listed 는 마킹하지 않는다(위 참고). 그런데 이 줄이 그걸 반영하지
          # 않아 실행 결과에 `완료표시=60` 이 찍혔다 — 한 건도 안 찍었는데.
          # 로그가 하지도 않은 일을 보고하면 다음 사람이 "왜 또 조회하지?" 를
          # 코드가 아니라 DB 에서 찾게 된다.
          f"완료표시={len(exhausted) if not args.dry_run else 0}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="폐지 종목 상장주식수 백필 (DART)")
    ap.add_argument("--db", default=None)
    ap.add_argument("--limit", type=int, default=0, help="상위 N종목만 (0=전체)")
    ap.add_argument("--sleep", type=float, default=0.15, help="DART 호출 간 대기(초)")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument(
        "--refetch", action="store_true",
        help="dart_checked 로 제외된 코드까지 전부 다시 조회 (DART 자료가 늘었을 때)",
    )
    ap.add_argument(
        "--listed", action="store_true",
        help="폐지 종목 대신 **현재 상장 종목**의 과거 주식수를 백필한다 "
             "(_listed_targets 참고). 시총 분모가 2026년 이전에 없는 문제를 메운다.",
    )
    ap.add_argument("--from-year", type=int, default=2016, help="--listed 백필 시작 연도")
    ap.add_argument("--to-year", type=int, default=2025, help="--listed 백필 종료 연도")
    args = ap.parse_args()

    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
