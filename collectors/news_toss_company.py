"""토스 종목별 뉴스 피드 → news_articles + news_company_feed (+ 종목별 수집 원장).

토스 ``/api/v2/news/companies/{code}`` 는 종목 하나의 뉴스를 과거로 페이지 넘겨
준다(최대 100페이지·1만 건, 101페이지는 HTTP 400). daily_news 가 받는 하이라이트
피드(최신 수십 건, 페이지 없음)와 달리 **종목별로, 빠짐없이** 받는다. 스윙 연구와
daytrade-it 전진 홀드아웃이 이 데이터를 쓴다(migrations/015 의 "왜" 참고).

두 경로:

- 일일 증분(기본): 마지막으로 **실패 없이 끝난** 수집의 날짜부터 오늘까지, 매
  평일의 유니버스(전 거래일 ``daily_bars_adjusted.trade_value`` 상위 300, 069500
  제외 — daytrade-it 크론과 같은 규칙)의 합집합을 받는다. 한 종목이라도 실패하면
  원장에 ``failed`` 를 남기고 끝에 예외를 던진다 — 다음 런은 실패 없던 런부터
  다시 받으므로 공백이 생기지 않는다(재실행 멱등).
- ``--import-archive``: daytrade-it 이 파일로 쌓은 아카이브(``<code>.jsonl`` +
  ``.done`` + 증분 ``ledger.jsonl``)를 한 번 옮긴다.

기사 id 는 krx-news-client 의 ``make_article_id('toss', build_article_url(newsId))``
— daily_news 와 같은 기사면 같은 id 다. **이미 있는 id 는 건너뛴다**: 토스가 기사를
고치면 createdAt 이 바뀌어(실측 1,171 중 17) PK(id, published_at) 로는 두 행이
되는데, 나중 판은 lookahead 라 먼저 본 판을 남긴다.

요청 간격은 1.2초 이상(daytrade-it 크론과 같은 값). 연속 실패가 5번이면 차단을
의심하고 멈춘다 — 계속 두드리면 차단만 길어진다.

CLI:
    python -m collectors.news_toss_company --db <DSN>
    python -m collectors.news_toss_company --db <DSN> --import-archive DIR [--ledger FILE]
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import time
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any

from krx_news_client.scrapers.base import make_article_id
from krx_news_client.scrapers.toss import build_article_url

from .storage import (
    fetchall,
    insert_news_company_feed,
    upsert_news_articles,
    upsert_news_company_feed_fetches,
)

KST = dt.timezone(dt.timedelta(hours=9))
URL = "https://wts-info-api.tossinvest.com/api/v2/news/companies/{code}"
UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128 Safari/537.36"
PAGE_SIZE = 100
MAX_PAGES = 100            # 토스 오프셋 한계: number * size <= 10,000
MIN_INTERVAL = 1.2         # 요청 시작 간격(초)
MAX_CONSECUTIVE_FAILURES = 5
UNIVERSE_SIZE = 300
MARKET = "069500"          # 유니버스 계산에만 쓰이는 지수 ETF — 뉴스는 받지 않는다
SOURCE_FEED = "toss_company_feed"
SOURCE_ARCHIVE = "toss_archive_import"


# --- 순수 함수 ------------------------------------------------------------------

def article_id(news_id: str) -> str:
    return make_article_id("toss", build_article_url(news_id))


def parse_created(text: str) -> dt.datetime:
    """토스 ``createdAt`` 은 오프셋 없는 KST 벽시계다("2026-09-05T18:10:12")."""
    t = dt.datetime.fromisoformat(text)
    return t if t.tzinfo else t.replace(tzinfo=KST)


def select_universe(day_rows: Iterable[tuple[str, float]], size: int = UNIVERSE_SIZE) -> list[str]:
    """거래대금 내림차순, 동률은 코드 오름차순 상위 ``size``, 069500 제외.

    daytrade-it ``universe_coverage.select_universe`` + 크론의 ``!= MARKET`` 과 같다.
    """
    ranked = sorted(((c, v) for c, v in day_rows if v and v > 0), key=lambda r: (-r[1], r[0]))
    return [c for c, _ in ranked[:size] if c != MARKET]


def universes(rows: Iterable[tuple[Any, str, float]], since: dt.date, until: dt.date,
              size: int = UNIVERSE_SIZE) -> dict[dt.date, list[str]]:
    """{평일: 그날 유니버스} — 각 평일의 직전 거래일 거래대금으로 고른다."""
    by_day: dict[dt.date, list[tuple[str, float]]] = {}
    for d, code, v in rows:
        d = d if isinstance(d, dt.date) else dt.date.fromisoformat(str(d)[:10])
        by_day.setdefault(d, []).append((code, float(v)))
    traded = sorted(by_day)
    out: dict[dt.date, list[str]] = {}
    d = since
    while d <= until:
        if d.weekday() < 5:
            prev = [t for t in traded if t < d]
            if not prev:
                raise RuntimeError(f"{d} 의 유니버스를 만들 전 거래일 시세가 없다")
            out[d] = select_universe(by_day[prev[-1]], size)
        d += dt.timedelta(days=1)
    return out


def page_rows(body: list[dict], since_at: dt.datetime) -> tuple[list[dict], bool]:
    """이 페이지에서 ``since_at`` 이후 행과, 다음 페이지가 필요한지.

    토스 목록은 대략 최신순일 뿐이라(daytrade-it d2edb9a) 한 행이라도 since 이후면
    다음 페이지를 본다 — 통째로 오래된 페이지나 빈 페이지에서 멈춘다.
    """
    keep, any_new = [], False
    for item in body:
        created = item.get("createdAt")
        if not item.get("id") or not item.get("title") or not created:
            continue
        if parse_created(created) >= since_at:
            keep.append(item)
            any_new = True
    return keep, any_new


def first_seen_for(created: dt.datetime, done_at: dt.datetime,
                   runs: list[tuple[dt.datetime, dt.datetime]]) -> dt.datetime:
    """아카이브 행을 처음 받은 시각의 근사 — 백필 완료 시각, 그 뒤 기사는 그걸 처음 담은 증분 런.

    ``runs`` 는 성공한 증분 런의 (시작, 끝) 시작순. 어느 런에도 안 걸리면(이론상
    없음) 기사 시각을 그대로 쓴다 — 받기 전일 수는 없으니 하한으로는 맞다.
    """
    if created <= done_at:
        return done_at
    for _start, end in runs:
        if end >= created:
            return end
    return created


# --- DB ------------------------------------------------------------------------

def existing_ids(con: Any, ids: list[str], chunk: int = 1000) -> set[str]:
    found: set[str] = set()
    for i in range(0, len(ids), chunk):
        part = ids[i:i + chunk]
        ph = ",".join("?" * len(part))
        found.update(r[0] for r in fetchall(con, f"SELECT id FROM news_articles WHERE id IN ({ph})", tuple(part)))
    return found


def trade_value_rows(con: Any, since: dt.date, until: dt.date) -> list[tuple]:
    return fetchall(
        con,
        "SELECT date, code, trade_value FROM daily_bars_adjusted "
        "WHERE date >= ? AND date < ? AND trade_value > 0",
        ((since - dt.timedelta(days=14)).isoformat(), until.isoformat()),
    )


def last_clean_run_day(con: Any) -> dt.date | None:
    """실패가 하나도 없던 마지막 수집 런의 KST 날짜(아카이브 원장 포함)."""
    rows = fetchall(
        con,
        "SELECT run_started FROM news_company_feed_fetches f WHERE status <> 'failed' "
        "AND NOT EXISTS (SELECT 1 FROM news_company_feed_fetches g "
        "WHERE g.run_started = f.run_started AND g.status = 'failed') "
        "ORDER BY run_started DESC LIMIT 1",
        (),
    )
    if not rows:
        return None
    t = rows[0][0]
    t = t if isinstance(t, dt.datetime) else dt.datetime.fromisoformat(str(t))
    return t.astimezone(KST).date()


def write_items(con: Any, items: list[dict], source: str) -> dict[str, int]:
    """``items``: {news_id, code, created, title, summary, publisher, first_seen}.

    기사는 id 당 하나(가장 이른 createdAt 판), 이미 있는 id 는 건너뛴다.
    (기사, 종목) 쌍은 전부 넣되 이미 있는 쌍은 그대로 둔다.
    """
    by_article: dict[str, dict] = {}
    for it in items:
        aid = article_id(it["news_id"])
        cur = by_article.get(aid)
        if cur is None or it["created"] < cur["created"]:
            by_article[aid] = it
    have = existing_ids(con, list(by_article))
    new_ids = [a for a in by_article if a not in have]
    article_rows = [
        (
            aid, "toss", "stock", by_article[aid]["title"].strip(),
            build_article_url(by_article[aid]["news_id"]),
            None,  # 피드는 본문을 주지 않는다 — 빈 문자열이 아니라 "모름"
            by_article[aid]["summary"], by_article[aid]["publisher"],
            by_article[aid]["created"].isoformat(), by_article[aid]["first_seen"].isoformat(),
        )
        for aid in new_ids
    ]
    upsert_news_articles(con, article_rows)
    inserted = set(new_ids)
    pairs: dict[tuple[str, str], tuple] = {}
    for it in items:
        aid = article_id(it["news_id"])
        key = (aid, it["code"])
        prev = pairs.get(key)
        if prev is None or it["first_seen"].isoformat() < prev[2]:
            pairs[key] = (aid, it["code"], it["first_seen"].isoformat(), source, aid in inserted)
    feed_new = insert_news_company_feed(con, list(pairs.values()))
    return {"articles_seen": len(by_article), "articles_new": len(new_ids),
            "pairs_seen": len(pairs), "pairs_new": feed_new}


def item_from_raw(raw: dict, code: str, first_seen: dt.datetime) -> dict:
    src = raw.get("source")
    publisher = src.get("name") or src.get("code") if isinstance(src, dict) else src
    return {
        "news_id": str(raw["id"]),
        "code": code,
        "created": parse_created(raw["createdAt"]),
        "title": raw["title"],
        "summary": raw.get("summary") or None,
        "publisher": publisher or None,
        "first_seen": first_seen,
    }


# --- 일일 증분 --------------------------------------------------------------------

class Fetcher:
    """요청 간격·차단 의심 가드를 지키는 단일 스레드 페이지 수집기."""

    def __init__(self, get: Callable[..., Any] | None = None, sleep: Callable[[float], None] = time.sleep,
                 clock: Callable[[], float] = time.monotonic, interval: float = MIN_INTERVAL) -> None:
        if get is None:
            import httpx  # noqa: PLC0415 — 테스트는 get 을 주입한다

            client = httpx.Client(headers={"User-Agent": UA, "Referer": "https://tossinvest.com/"}, timeout=20)
            get = client.get
        self._get, self._sleep, self._clock, self._interval = get, sleep, clock, interval
        self._last = -1e9

    def page(self, code: str, number: int) -> tuple[int, list[dict]]:
        wait = self._last + self._interval - self._clock()
        if wait > 0:
            self._sleep(wait)
        self._last = self._clock()
        r = self._get(URL.format(code=code), params={"number": number, "size": PAGE_SIZE})
        if r.status_code != 200:
            return r.status_code, []
        result = (r.json() or {}).get("result") or {}
        body = result.get("body")
        if not isinstance(body, list):
            raise RuntimeError(f"{code} p{number}: 예상과 다른 응답 형식")
        return 200, body

    def code(self, code: str, since_at: dt.datetime) -> tuple[str, list[dict]]:
        """(status, raw rows) — status: done | page_cap. 그 밖의 오류는 예외."""
        rows: list[dict] = []
        for number in range(1, MAX_PAGES + 1):
            status, body = self.page(code, number)
            if status == 400 and number > 1:
                return "page_cap", rows
            if status == 404 and number == 1:
                return "done", rows   # 피드 자체가 없는 종목 — 0건이 맞다
            if status != 200:
                raise RuntimeError(f"{code} p{number}: HTTP {status}")
            keep, more = page_rows(body, since_at)
            rows.extend(keep)
            if not body or not more:
                return "done", rows
        return "page_cap", rows


def collect(con: Any, *, since: dt.date | None = None, until: dt.date | None = None,
            fetcher: Fetcher | None = None, now: Callable[[], dt.datetime] | None = None,
            size: int = UNIVERSE_SIZE) -> dict[str, int]:
    now = now or (lambda: dt.datetime.now(KST))
    run_started = now()
    until = until or run_started.date()
    since = since or last_clean_run_day(con) or (until - dt.timedelta(days=3))
    uni = universes(trade_value_rows(con, since, until + dt.timedelta(days=1)), since, until, size)
    codes = sorted({c for cs in uni.values() for c in cs})
    since_at = dt.datetime(since.year, since.month, since.day, tzinfo=KST)
    fetcher = fetcher or Fetcher()
    print(f"▶ 토스 종목별 뉴스: {since}..{until} 평일 {len(uni)}일, 종목 {len(codes)}", flush=True)

    items: list[dict] = []
    fetch_rows: list[tuple] = []
    failed: list[str] = []
    streak = 0
    for i, code in enumerate(codes, 1):
        try:
            status, raw = fetcher.code(code, since_at)
            streak = 0
        except Exception as exc:  # noqa: BLE001 — 종목 하나의 실패는 원장에 남기고 계속
            print(f"⚠️ {code}: {exc}", flush=True)
            failed.append(code)
            fetch_rows.append((code, run_started.isoformat(), since.isoformat(), "failed", None, 0,
                               now().isoformat(), SOURCE_FEED))
            streak += 1
            if streak >= MAX_CONSECUTIVE_FAILURES:
                print(f"⛔ 연속 실패 {streak}회 — 차단 의심, 여기서 멈춘다", flush=True)
                break
            continue
        seen = now()
        got = [item_from_raw(r, code, seen) for r in raw]
        items.extend(got)
        oldest = min((g["created"] for g in got), default=None)
        fetch_rows.append((code, run_started.isoformat(), since.isoformat(), status,
                           oldest.isoformat() if oldest else None, len(got), seen.isoformat(), SOURCE_FEED))
        if i % 50 == 0:
            print(f"  {i}/{len(codes)} 종목, 누적 {len(items)}행", flush=True)

    stats = write_items(con, items, SOURCE_FEED)
    upsert_news_company_feed_fetches(con, fetch_rows)
    stats.update(codes=len(codes), fetched_codes=len(fetch_rows) - len(failed), failed=len(failed))
    print(f"✅ 토스 종목별 뉴스: {stats}", flush=True)
    if len(fetch_rows) < len(codes):
        raise RuntimeError(f"{len(codes) - len(fetch_rows)}개 종목을 시도하지 못했다(연속 실패로 중단)")
    if failed:
        raise RuntimeError(f"{len(failed)}개 종목 실패: {', '.join(failed[:10])}")
    if not items:
        # 300종목 하루치가 통째로 0건이면 응답이 이상한 것이다 — 빈 응답을 성공으로
        # 처리하던 daily_krx_shares(22회 연속 rows=0) 의 재발을 막는다.
        raise RuntimeError("모든 종목이 0건 — 응답 형식이 바뀌었거나 차단됐다")
    return stats


# --- 아카이브 1회 이전 -------------------------------------------------------------------

def load_ledger(path: Path | None) -> list[dict]:
    if path is None or not path.exists():
        return []
    runs = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    return sorted((r for r in runs if r.get("status") == "ok"), key=lambda r: r["run_started"])


def import_archive(con: Any, archive: Path, ledger: Path | None, size: int = UNIVERSE_SIZE,
                   batch_codes: int = 200) -> dict[str, int]:
    runs = load_ledger(ledger)
    spans = [(dt.datetime.fromisoformat(r["run_started"]), dt.datetime.fromisoformat(r["run_finished"]))
             for r in runs]
    done_files = sorted(archive.glob("*.done"))
    if not done_files:
        raise RuntimeError(f"{archive} 에 .done 이 없다 — 아카이브 경로를 확인")
    total = {"articles_seen": 0, "articles_new": 0, "pairs_seen": 0, "pairs_new": 0, "codes": 0}
    first_seen_counts: dict[tuple[str, dt.datetime], int] = {}
    batch: list[dict] = []
    fetch_rows: list[tuple] = []

    def flush() -> None:
        nonlocal batch
        if batch:
            s = write_items(con, batch, SOURCE_ARCHIVE)
            for k in ("articles_seen", "articles_new", "pairs_seen", "pairs_new"):
                total[k] += s[k]
            batch = []

    # 같은 기사가 여러 종목 파일에 다른 createdAt 으로 있을 수 있다(토스가 고친 기사).
    # 기사 행은 전역에서 가장 이른 판으로 — 종목 묶음마다 따로 고르면 묶음 순서가 이긴다.
    earliest: dict[str, str] = {}
    for done in done_files:
        data = archive / f"{done.stem}.jsonl"
        if not data.exists():
            continue
        for line in data.read_text().splitlines():
            if line.strip():
                r = json.loads(line)
                nid, c = r.get("id"), r.get("createdAt")
                if nid and c and (nid not in earliest or parse_created(c) < parse_created(earliest[nid])):
                    earliest[nid] = c

    for n, done in enumerate(done_files, 1):
        code = done.stem
        meta = json.loads(done.read_text())
        done_at = dt.datetime.fromisoformat(meta["finished_at"])
        if done_at.tzinfo is None:
            done_at = done_at.replace(tzinfo=KST)
        data = archive / f"{code}.jsonl"
        raws = [json.loads(line) for line in data.read_text().splitlines() if line.strip()] if data.exists() else []
        oldest = None
        n_backfill = 0
        for r in raws:
            if not r.get("id") or not r.get("title") or not r.get("createdAt"):
                continue
            created = parse_created(r["createdAt"])
            seen = first_seen_for(created, done_at, spans)
            item = item_from_raw(r, code, seen)
            item["created"] = parse_created(earliest[str(r["id"])])
            batch.append(item)
            if seen == done_at:
                n_backfill += 1
            else:
                first_seen_counts[(code, seen)] = first_seen_counts.get((code, seen), 0) + 1
            if created <= done_at and (oldest is None or created < oldest):
                oldest = created
        status = {"done": "done", "page_cap": "page_cap"}.get(meta.get("status"), meta.get("status", "done"))
        fetch_rows.append((code, done_at.isoformat(), meta["since"], status,
                           oldest.isoformat() if oldest else None, n_backfill, done_at.isoformat(),
                           SOURCE_ARCHIVE))
        total["codes"] += 1
        if n % batch_codes == 0:
            flush()
            print(f"  {n}/{len(done_files)} 종목, 기사 신규 {total['articles_new']:,}", flush=True)
    flush()

    # 증분 런: 크론이 받은 종목 = 그 런의 [since, until] 평일 유니버스 합집합(같은 규칙으로 재계산).
    end_to_start = {e: s for s, e in spans}
    for r in runs:
        since = dt.date.fromisoformat(r["since"])
        until = dt.date.fromisoformat(r["until"])
        uni = universes(trade_value_rows(con, since, until + dt.timedelta(days=1)), since, until, size)
        codes = sorted({c for cs in uni.values() for c in cs})
        if len(codes) != r.get("codes"):
            print(f"⚠️ 증분 런 {r['run_started']}: 재계산 유니버스 {len(codes)} ≠ 원장 {r.get('codes')}"
                  " (거래대금 확정치 갱신으로 경계 종목이 바뀌었을 수 있다)", flush=True)
        end = dt.datetime.fromisoformat(r["run_finished"])
        start = end_to_start[end]
        for code in codes:
            fetch_rows.append((code, start.isoformat(), since.isoformat(), "done", None,
                               first_seen_counts.get((code, end), 0), end.isoformat(), SOURCE_ARCHIVE))
    upsert_news_company_feed_fetches(con, fetch_rows)
    total["fetch_rows"] = len(fetch_rows)
    print(f"✅ 아카이브 이전: {total}", flush=True)
    return total


def main() -> int:
    from .storage import connect  # noqa: PLC0415

    p = argparse.ArgumentParser(description="토스 종목별 뉴스 피드 → TimescaleDB")
    p.add_argument("--db", default=None, help="DSN (postgresql://... 또는 sqlite 경로)")
    p.add_argument("--since", help="이 날짜부터 다시 받는다(기본: 실패 없던 마지막 런의 날짜)")
    p.add_argument("--until", help="이 날짜까지(기본: 오늘 KST)")
    p.add_argument("--import-archive", type=Path, help="daytrade-it 아카이브 디렉터리(<code>.jsonl/.done)")
    p.add_argument("--ledger", type=Path, help="아카이브 증분 원장(ledger.jsonl)")
    a = p.parse_args()
    con = connect(a.db)
    try:
        if a.import_archive:
            import_archive(con, a.import_archive, a.ledger)
        else:
            collect(con,
                    since=dt.date.fromisoformat(a.since) if a.since else None,
                    until=dt.date.fromisoformat(a.until) if a.until else None)
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
