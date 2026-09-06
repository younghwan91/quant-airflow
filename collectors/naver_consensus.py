"""Collect analyst consensus (목표주가·투자의견) from Naver Finance.

Kiwoom's broker API has no analyst consensus; Naver Finance exposes it (FnGuide-
sourced) via the mobile integration endpoint, no auth required. This is the
forward-looking signal PEAD lacks — target-price implied upside and, once a time
series accumulates, **consensus revisions** (the re-rating that drives mega-caps
where post-earnings drift is arbitraged away). See docs/pead-strategy.md.

The endpoint is a **current snapshot**, so this collector is meant to run daily,
appending a date-stamped row per code to build the revision time series over
time. Writes CSV: date,code,target_mean,recomm_mean,base_date.

fetch/parse는 krx-fundamentals-client의 ``NaverConsensusScraper``로 옮겼다
(2026-09-06) — DART 실적이 ``DartScraper``로 옮겨간 것과 같은 이유다. 이
파일은 유니버스 선정·동시성 제한·DB 적재·CLI만 맡는다(dart_earnings.py와
같은 역할 분담).

``est_year``는 그 라이브러리에서 4자리 연도(int, 예: 2026)로 바뀌었다 —
예전엔 이 파일이 자체 파싱해서 "202612"(연월) 문자열을 그대로 남겼지만,
그 값을 파싱해 쓰는 다운스트림 소비자가 없어(레포 전체 검색 확인) 정보
손실 없이 연도만 남기는 쪽으로 단순화됐다. ``consensus.est_year``는 TEXT라
스키마 변경은 필요 없다(포맷만 "202612" → "2026").
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import os
from datetime import date

from krx_fundamentals_client.scrapers.naver import NaverConsensusScraper

from .storage import universe_query


def _universe_query(args: argparse.Namespace) -> tuple[str, dict]:
    """SQL (+ params) selecting the code universe: all ``daily_bars`` codes or top-N liquid.

    ``--all-codes`` uses a plain ``DISTINCT code`` scan with no recency window, so a
    stock that just IPO'd today (and so has only today's row in ``daily_bars``) is
    included from day one — no special-casing needed for newly listed codes.

    ``--covered-days N`` 은 **애널리스트 커버리지가 있는 종목만** 고른다.
    전종목(2,627개)을 매일 도는 건 요청의 73% 가 헛돈다는 뜻이다 — 실제 적재는
    하루 652~660행이고 나머지 ~1,930 종목은 세 필드가 전부 None 이라 조용히
    버려진다. 커버리지는 하루아침에 생기지 않으므로 매일 확인할 이유가 없다.
    **새로 커버리지가 붙은 종목은 주 1회 전종목 스윕이 잡는다** — 그 스윕이
    없으면 이 필터는 한 번 빠진 종목을 영원히 놓친다.
    """
    covered_days = getattr(args, "covered_days", 0)
    if covered_days:
        return (
            "SELECT DISTINCT code FROM consensus "
            "WHERE date >= CURRENT_DATE - make_interval(days => %(d)s) ORDER BY code",
            {"d": covered_days},
        )
    return universe_query(all_codes=args.all_codes, top_n=args.top_n)


async def _fetch_both(
    scraper: NaverConsensusScraper, sem: asyncio.Semaphore, code: str,
) -> tuple[str, float | None, float | None, str | None, float | None, float | None, int | None]:
    """Both endpoints for one code — the unit of work bounded by ``sem``.

    ``BaseScraper.fetch``가 요청마다 자체 throttle(0.3~1.0초 랜덤)을 이미
    거니, 여기서 추가로 sleep을 넣지 않는다 — 동시 실행 개수만 세마포어로
    제한한다(예전 ThreadPoolExecutor(workers=N)과 같은 역할).
    """
    async with sem:
        tm, rm, bd = await scraper.fetch_consensus(code)
        fe, pe, ey = await scraper.fetch_estimate(code)
        return code, tm, rm, bd, fe, pe, ey


async def _run(args: argparse.Namespace) -> int:
    import pandas as pd

    from .storage import connect, default_db_path, upsert_consensus
    con = connect(args.db or str(default_db_path()))
    sql, params = _universe_query(args)
    top = pd.read_sql_query(sql, con, params=params)
    codes = top["code"].tolist()

    today = date.today().isoformat()
    done: set[str] = set()
    if args.db_table:
        existing = pd.read_sql_query(
            "SELECT code FROM consensus WHERE date = %(d)s", con, params={"d": today})
        done = set(existing["code"])
    else:
        con.close()
        if os.path.exists(args.out):
            for r in csv.reader(open(args.out)):
                if r and r[0] == today:
                    done.add(r[1])  # (date, code) already collected today

    pending = [c for c in codes if c not in done]

    f = open(args.out, "a", newline="") if args.out else None
    w = csv.writer(f) if f else None
    n = 0
    # DB 경로: future 완료마다 개별 upsert(~2,600회 라운드트립) 대신 _CHUNK_SIZE개
    # 완료마다 배치 upsert. 루프 종료 후 1회(메가배치)는 크래시 시 전체 진행분을
    # 잃으므로 청크 단위 중간 커밋으로 손실을 최대 1청크 분량으로 제한한다.
    # as_completed 순서라 청크 경계는 종목 순서가 아니라 완료 순서 기준이지만,
    # 각 레코드가 독립적이라(같은 (code,date) 재수집이 아님) 문제 없다.
    _CHUNK_SIZE = 100
    db_buffer: list[tuple] = []

    def flush_db() -> None:
        nonlocal n
        if db_buffer:
            upsert_consensus(con, db_buffer)
            n += len(db_buffer)
            db_buffer.clear()

    scraper = NaverConsensusScraper()
    sem = asyncio.Semaphore(args.workers)
    try:
        tasks = [asyncio.ensure_future(_fetch_both(scraper, sem, code)) for code in pending]
        for i, fut in enumerate(asyncio.as_completed(tasks), 1):
            code, tm, rm, bd, fe, pe, ey = await fut
            if tm is None and rm is None and fe is None:
                continue

            est_year = str(ey) if ey is not None else None
            if args.db_table:
                db_buffer.append((code, today, tm, rm, bd, fe, pe, est_year))
                if len(db_buffer) >= _CHUNK_SIZE:
                    flush_db()
            else:
                def _s(x: object) -> object:
                    return x if x is not None else ""
                w.writerow([today, code, _s(tm), _s(rm), bd or "", _s(fe), _s(pe), est_year or ""])
                n += 1
            if i % 50 == 0:
                if f:
                    f.flush()
                print(f"[{i}/{len(pending)}] rows={n}", flush=True)
    finally:
        await scraper.close()
    flush_db()
    if f:
        f.close()
    if args.db_table:
        con.close()
    print(f"DONE date={today} rows={n}", flush=True)
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="네이버 애널리스트 컨센서스 수집 (목표주가·투자의견, 일별 스냅샷)")
    ap.add_argument("--out", default=None, help="출력 CSV (일별 append)")
    ap.add_argument("--top-n", type=int, default=800, help="유동성 상위 N종목")
    ap.add_argument("--all-codes", action="store_true", help="유동성 상위 N 대신 daily_bars 전종목 사용")
    ap.add_argument(
        "--covered-days", type=int, default=0,
        help="최근 N일 안에 컨센서스가 실제로 잡힌 종목만 조회 (0=끔). "
             "전종목의 73%%는 매일 빈 응답이라 헛돈다 — 단 주 1회는 --all-codes 로 "
             "전종목을 훑어야 신규 커버리지를 놓치지 않는다.",
    )
    ap.add_argument("--db-table", action="store_true", help="CSV 대신 consensus 테이블에 직접 upsert")
    ap.add_argument("--db", default=None)
    ap.add_argument(
        "--workers", type=int, default=8,
        help="동시 fetch 개수(asyncio.Semaphore). 네이버는 무인증·독립 레이트리밋이고 "
             "요청마다 BaseScraper 가 자체 throttle 하므로 4는 과보수적이었다.",
    )
    args = ap.parse_args()
    if not args.db_table and not args.out:
        ap.error("--out is required unless --db-table is set")

    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
