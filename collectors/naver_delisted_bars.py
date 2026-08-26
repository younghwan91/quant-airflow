"""Backfill daily bars for **delisted** stocks from Naver — closes the survivorship gap.

``daily_bars`` is collected from Kiwoom ``ka10081`` over ``fetch_stock_list()``, which
returns only **currently listed** codes. A stock that went bankrupt or got merged never
enters that loop, so its price history is absent and every backtest silently measures
only the companies that survived — the single biggest hidden return inflator
(``features/universe.py``, GUARDRAILS §3).

**Kiwoom cannot fill this gap.** Measured 2026-08-13: ``ka10081`` on a delisted code
answers ``return_code: 0`` ("정상적으로 처리되었습니다") with one row of empty strings.
It does not error — a naive backfill loop would write nothing and still look green.

Naver's ``siseJson`` endpoint does serve delisted history, back to at least 2003, and
returns the **whole date range in one request** rather than page-by-page. Its OHLCV was
verified identical to our stored Kiwoom bars on overlapping listed codes, including
across Samsung's 2018-05 50:1 split (both vendors serve split-adjusted series).

**One honest gap:** Naver gives no 거래대금. ``trade_value`` is therefore approximated as
``close * volume / 1e6`` (the table's 백만원 unit). Measured against 20,000 real rows the
error is 0.70% median / 3.55% p95 / 7.83% p99 — immaterial for the ADV liquidity floor
this column feeds, but the rows are tagged ``source='naver'`` so the approximation is
never mistaken for a reported figure.

CLI:
    python -m collectors.naver_delisted_bars --db <DSN>            # 전체 폐지종목
    python -m collectors.naver_delisted_bars --db <DSN> --limit 20 # 표본
    python -m collectors.naver_delisted_bars --db <DSN> --dry-run  # 조회만, 미기록
"""

from __future__ import annotations

import argparse
import re
import time
import urllib.error
import urllib.request

from .storage import (
    CHECKED_NAVER_BARS,
    DAILY_BAR_COLUMNS,
    _is_pg,
    _upsert,
    connect,
    default_db_path,
    fetchall,
    mark_checked,
)

SISE_URL = "https://api.finance.naver.com/siseJson.naver"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
REFERER = "https://finance.naver.com/"

# 우리 일봉 이력의 시작. 이보다 앞선 폐지 종목은 백필해도 겹치는 구간이 없다.
HISTORY_START = "20160909"

# 응답은 JSON 이 아니라 작은따옴표/개행이 섞인 JS 배열 리터럴이라 정규식으로 읽는다.
# ["20160104", 141980, 148434, 141980, 145853, 6493, 11.21]
_ROW_RE = re.compile(
    r'\["(\d{8})",\s*(-?[\d.]+),\s*(-?[\d.]+),\s*(-?[\d.]+),\s*(-?[\d.]+),\s*(-?\d+)'
)

# storage 의 정본에서 파생한다 — 손으로 베끼면 daily_bars 에 컬럼이 하나 늘 때
# 두 목록이 갈라지고, 삽입이 위치 기반이라 값이 엉뚱한 컬럼으로 들어간다.
DAILY_BAR_SOURCE_COLUMNS = [*DAILY_BAR_COLUMNS, "source"]


def parse_sise(body: str, code: str) -> list[tuple]:
    """siseJson 본문 → ``DAILY_BAR_SOURCE_COLUMNS`` 순서의 행 리스트 (순수함수).

    거래량 0인 날(거래정지 등)은 버리지 않는다 — 상장은 되어 있었다는 사실 자체가
    유니버스 판정에 필요하고, 여기서 지우면 "사라진 것처럼" 보여 생존편향 스멜테스트를
    오히려 교란한다. 다만 종가가 0/음수인 행은 값이 없는 것이므로 버린다.

    **거래정지일 정규화.** 네이버는 정지일을 ``시가=고가=저가=0, 종가=기준가`` 로
    주는데, 키움은 같은 날을 ``OHLC=종가`` 로 저장한다(실측: 거래량 0인 키움 행
    118,072건 중 118,070건이 OHLC=종가). 0을 그대로 넣으면 고가/저가를 읽는 돌파·ATR·
    손절 로직이 한 테이블 안에서 소스에 따라 다른 값을 보게 되므로 키움 규약에 맞춘다.

    정규화는 **필드별**이다. 예전엔 ``o <= 0 and h <= 0 and low <= 0`` 일 때만,
    즉 "셋 다 0" 인 완전한 정지일 모양에만 걸었다. 그러면 한 필드만 비정상인 행
    (예: ``low`` 만 음수)이 그 분기를 안 타고, 아래 ``min(low, close)`` 가 음수를
    그대로 남긴다 — ``daily_bars`` 에 음수 저가가 실린다. 조건을 ``or`` 로 푸는 건
    답이 아니다: 그러면 한 필드만 나쁜 행에서 **멀쩡한 나머지 둘까지** close 로
    뭉개 정보를 파괴한다. 필드별로 보면 셋 다 0인 정지일은 셋 다 close 로 떨어져
    기존과 결과가 완전히 같고(회귀 없음), 부분 오염은 그 필드만 처리된다.

    실측(2026-08-27) 기준 ``daily_bars`` 에 그런 행은 0건이다 — kiwoom 5,262,031 ·
    naver 459,847 행 전부 ``low``/``open``/``high`` 가 양수다. 즉 이 가드는 이미
    들어온 오염을 고치는 게 아니라 앞으로의 회귀를 막는 쪽이다.
    """
    out: list[tuple] = []
    for dt, o, h, low, c, v in _ROW_RE.findall(body):
        close = float(c)
        if close <= 0:
            continue
        o, h, low = float(o), float(h), float(low)
        # 값이 없는 필드는 close 로 (키움 규약). 정지일(셋 다 0)이 여기 흡수된다.
        o = o if o > 0 else close
        h = h if h > 0 else close
        low = low if low > 0 else close
        # 소스 자체가 종가를 고가/저가 밖으로 주는 행이 드물게 있다(정리매매 동전주 등).
        # 봉의 정의상 불가능한 값이라 범위만 종가까지 넓힌다(종가는 실제 체결가라 보존).
        h, low = max(h, close), min(low, close)
        volume = int(v)
        date = f"{dt[:4]}-{dt[4:6]}-{dt[6:]}"
        # 거래대금 근사: 종가×거래량, 백만원 단위(테이블 규약). 실측 오차 중앙값 0.7%.
        trade_value = int(round(close * volume / 1e6))
        out.append((
            code, date, int(o), int(h), int(low), int(close),
            volume, trade_value, "naver",
        ))
    return out


def fetch_sise(code: str, start: str = HISTORY_START, end: str | None = None,
               *, retries: int = 3, timeout: int = 30) -> str:
    """siseJson 원문을 가져온다. 실패 시 빈 문자열(호출부가 '데이터 없음'과 동일 취급)."""
    end = end or time.strftime("%Y%m%d")
    url = (f"{SISE_URL}?symbol={code}&requestType=1"
           f"&startTime={start}&endTime={end}&timeframe=day")
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Referer": REFERER})
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:  # noqa: S310 — 고정 호스트
                return r.read().decode("utf-8", "ignore")
        except (urllib.error.URLError, TimeoutError, OSError):
            if attempt < retries - 1:
                time.sleep(1.5 * (attempt + 1))
    return ""


def _delisted_codes(con, *, refetch: bool = False) -> list[str]:
    """백필 대상 코드.

    6자리 숫자 코드이면서 유가증권/코스닥인 것만 — 폐지 목록에는 채권·ELW 등
    비표준 코드가 섞여 있고(약 1,800건), 코넥스는 우리 유니버스가 아니다.

    **이미 받은 종목은 뺀다.** 이 수집기는 주간 DAG 로 매주 도는데, 삽입이
    ``on_conflict="nothing"`` 이라 재조회분은 전부 버려진다. 빼지 않으면 매주
    2,200회의 외부 요청을 보내고 그중 새로 쌓이는 건 신규 폐지분(보통 10건 미만)
    뿐이다. ``refetch=True`` 면 전량 다시 받는다(구간을 늘려 재수집할 때).

    구간 밖(마지막 거래일 < HISTORY_START)도 뺀다 — 응답이 비어 있을 게 확실한데
    요청 비용은 그대로 든다. 다만 ``last_trade_date`` 는 daily_bars 에서 파생하므로
    **애초에 바가 없는 코드는 전부 NULL** 이라 이 필터에 안 걸린다(실측: 1,758개 중
    1,751개가 그렇게 남아 매주 빈 응답을 받았다). 그래서 한 번 조회해 구간 내 데이터가
    없던 코드는 ``naver_checked`` 에 날짜를 남기고 다음부터 건너뛴다 — 상장폐지는 과거
    사실이라 한 번 없으면 영원히 없다.
    """
    start_date = f"{HISTORY_START[:4]}-{HISTORY_START[4:6]}-{HISTORY_START[6:]}"
    # 6자리 숫자 판정은 백엔드마다 다르다 — pg 는 POSIX 정규식, sqlite 는 GLOB.
    six_digit = ("code ~ '^[0-9]{6}$'" if _is_pg(con)
                 else "length(code) = 6 AND code GLOB '[0-9][0-9][0-9][0-9][0-9][0-9]'")
    where = [
        six_digit,
        "market IN ('유가증권', '코스닥')",
        f"(last_trade_date IS NULL OR last_trade_date >= '{start_date}')",
    ]
    if not refetch:
        where.append("code NOT IN (SELECT DISTINCT code FROM daily_bars WHERE source = 'naver')")
        where.append("naver_checked IS NULL")
    sql = f"SELECT code FROM delisted_stocks WHERE {' AND '.join(where)} ORDER BY code"  # noqa: S608 — 조건은 전부 모듈 상수
    return [r[0] for r in fetchall(con, sql)]


def _mark_checked(con, codes: list[str], today: str) -> None:
    """더 받을 게 없는 코드를 기록해 다음 회차부터 건너뛴다.

    두 경우다: (1) 구간 내 데이터가 아예 없음, (2) 데이터는 있으나 전부 기존 행과
    겹쳐 새로 쌓을 게 없음. (2)를 빼먹으면 그 코드는 ``source='naver'`` 행이 생기지
    않아 제외 조건에도 안 걸리고 영원히 재조회된다(실측 7건).
    """
    mark_checked(con, CHECKED_NAVER_BARS, codes, today)


def _insert_bars(con, records: list[tuple]) -> int:
    """폐지 종목 행 삽입. 이미 있는 (code, date)는 건드리지 않는다.

    ``on_conflict="nothing"`` 인 이유: 겹치는 구간이 있다면 그건 키움이 상장 중에
    수집한 실측치이고, 네이버 근사 거래대금으로 덮어쓸 이유가 없다.
    """
    return _upsert(con, "daily_bars", DAILY_BAR_SOURCE_COLUMNS, records,
                   on_conflict="nothing")


def main() -> int:
    ap = argparse.ArgumentParser(description="폐지 종목 일봉 백필 (네이버)")
    ap.add_argument("--db", default=None, help="DSN (미지정 시 기본 SQLite 경로)")
    ap.add_argument("--limit", type=int, default=0, help="상위 N종목만 (0=전체)")
    ap.add_argument("--sleep", type=float, default=0.25, help="요청 간 대기(초)")
    ap.add_argument("--start", default=HISTORY_START, help="시작일 YYYYMMDD")
    ap.add_argument("--dry-run", action="store_true", help="조회만 하고 기록하지 않음")
    ap.add_argument("--refetch", action="store_true",
                    help="이미 받은 종목도 다시 조회 (기본은 신규 폐지분만)")
    args = ap.parse_args()

    from .config import mask_dsn

    con = connect(args.db or default_db_path())
    codes = _delisted_codes(con, refetch=args.refetch)
    if args.limit:
        codes = codes[: args.limit]

    print(f"🔌 {mask_dsn(args.db)} | 대상 {len(codes)}종목 | start={args.start}"
          f"{' | DRY-RUN' if args.dry_run else ''}")

    empty = rows_seen = written = 0
    done_codes: list[str] = []
    today = time.strftime("%Y-%m-%d")
    t0 = time.time()
    for i, code in enumerate(codes, 1):
        body = fetch_sise(code, start=args.start)
        rows = parse_sise(body, code) if body else []
        if not rows:
            empty += 1
            # 응답 자체가 실패한 경우(body 빈 문자열)는 표시하지 않는다 — 일시적
            # 네트워크 오류를 "데이터 없음"으로 굳히면 영영 다시 안 받는다.
            if body:
                done_codes.append(code)
        else:
            rows_seen += len(rows)
            if not args.dry_run:
                n = _insert_bars(con, rows)
                written += n
                if n == 0:
                    done_codes.append(code)   # 전부 기존 행과 겹침 — 더 받을 게 없다
        if i % 100 == 0 or i == len(codes):
            el = time.time() - t0
            rate = i / el if el else 0
            print(f"  [{i}/{len(codes)}] 데이터있음={i - empty} 없음={empty} "
                  f"행={rows_seen:,} 기록={written:,} "
                  f"| {rate:.1f}종목/s ETA {(len(codes)-i)/rate/60 if rate else 0:.1f}분",
                  flush=True)
        time.sleep(args.sleep)

    if not args.dry_run:
        _mark_checked(con, done_codes, today)
    con.close()
    print(f"DONE codes={len(codes)} 데이터있음={len(codes) - empty} 없음={empty} "
          f"행={rows_seen} 기록={written} 완료표시={len(done_codes) if not args.dry_run else 0}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
