"""Backfill historical 상장주식수 (listed shares) from KRX — no auth, point-in-time.

The Kiwoom ``ka10001`` collector (``listed_shares.py``) can only ever return the
**current** shares-outstanding snapshot, so ``shares_outstanding_history`` starts
accumulating from its first production run and every ``market_cap_asof`` lookup
for an earlier backtest date silently uses today's share count — wrong across any
pre-run split/buyback (a lookahead/correctness gap the ka10001 docstring flags as
unfixable with that source).

KRX ``MDCSTAT01501`` (전종목 시세) closes that gap: it is **date-parametrized**
(``trdDd``), whole-universe in one CSV, needs no auth, and its rows carry
상장주식수 alongside 시가총액·종가. So we can backfill ``shares_outstanding_history``
for any historical trading day — giving a survivorship-free, point-in-time share
base and the denominator for a '시가총액 대비 수급 비율' (cap-normalized flow) feature.

``parse_snapshot`` is pure (CSV in → ``(code, shares)`` out) and unit-tested.
CLI: ``kq-collect-krx-shares --date 2026-07-03`` (one day) or ``--from/--to`` to
walk the ``daily_bars`` trading calendar and backfill a range.
"""

from __future__ import annotations

import argparse
import csv
import http.cookiejar
import io
import time
import urllib.parse
import urllib.request

from .storage import fetchall

OTP_URL = "http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd"
DOWNLOAD_URL = "http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd"
REFERER = "http://data.krx.co.kr/contents/MDC/MDI/mdiLoader"
UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
MARKET_CODE = {"KOSPI": "STK", "KOSDAQ": "KSQ"}

# KRX rejects OTP requests that arrive without a session cookie (the OTP endpoint
# returns the literal ``LOGOUT``). A cookie-jar opener primed by one GET to the
# loader page establishes that session, then OTP + download reuse it.
_OPENER = urllib.request.build_opener(
    urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
_primed = False


def _prime() -> None:
    global _primed
    if _primed:
        return
    # GET the site root (200 + Set-Cookie) to seed the session cookie the OTP
    # endpoint requires. The MDI loader path itself 404s without query params,
    # so hit the root, which reliably sets the cookie.
    try:
        req = urllib.request.Request("http://data.krx.co.kr/", headers={"User-Agent": UA})
        _OPENER.open(req, timeout=30).read()
        _primed = True
    except Exception:
        pass  # best-effort; OTP call will still be attempted


def _to_int(s: object) -> int | None:
    txt = str(s or "").replace(",", "").strip().strip('"')
    if txt in ("", "-", "N/A"):
        return None
    try:
        return int(float(txt))
    except ValueError:
        return None


def parse_snapshot(csv_text: str) -> list[tuple[str, int]]:
    """Extract ``[(code, shares_outstanding), ...]`` from an MDCSTAT01501 CSV.

    Args:
        csv_text: Decoded 전종목 시세 CSV (header row + one row per stock).

    Returns:
        ``(종목코드, 상장주식수)`` pairs, skipping rows with no share count.
        Empty when the CSV is missing the required columns or has no data rows.
    """
    reader = csv.reader(io.StringIO(csv_text.strip()))
    rows = list(reader)
    if len(rows) < 2:
        return []
    col = {name.strip().strip('"'): idx for idx, name in enumerate(rows[0])}
    if "종목코드" not in col or "상장주식수" not in col:
        return []
    out: list[tuple[str, int]] = []
    for row in rows[1:]:
        try:
            code = row[col["종목코드"]].strip().strip('"')
            shares = _to_int(row[col["상장주식수"]])
        except IndexError:
            continue
        if code and shares is not None:
            out.append((code, shares))
    return out


def _post(url: str, data: dict, *, retries: int = 3) -> bytes | None:
    _prime()
    body = urllib.parse.urlencode(data).encode()
    req = urllib.request.Request(url, data=body, headers={
        "User-Agent": UA, "Referer": REFERER, "X-Requested-With": "XMLHttpRequest"})
    for _ in range(retries):
        try:
            with _OPENER.open(req, timeout=30) as r:
                return r.read()
        except Exception:
            time.sleep(1.0)
    return None


def _decode(raw: bytes) -> str:
    for enc in ("euc-kr", "cp949", "utf-8-sig", "utf-8"):
        try:
            return raw.decode(enc)
        except (UnicodeDecodeError, LookupError):
            continue
    return raw.decode("utf-8", errors="replace")


def fetch_snapshot(mkt_id: str, trd_dd: str) -> list[tuple[str, int]]:
    """Fetch ``(code, shares)`` for one market on ``trd_dd`` (YYYYMMDD)."""
    params = {
        "locale": "ko_KR", "mktId": mkt_id, "trdDd": trd_dd,
        "share": "1", "money": "1", "csvxls_isNo": "false",
        "name": "fileDown", "url": "dbms/MDC/STAT/standard/MDCSTAT01501",
    }
    otp_raw = _post(OTP_URL, params)
    if otp_raw is None:
        return []
    otp = otp_raw.decode().strip()
    # KRX 가 MDCSTAT 계열에 회원 로그인을 걸면서 OTP 자리에 'LOGOUT' 을 돌려준다
    # (2026-08-15 실측). 그걸 그대로 다운로드에 넘기면 빈 응답이 오고, 이 함수는
    # 빈 리스트를 반환해 **초록불로 0행**이 된다 — 실제로 이 DAG 는 22회 실행 내내
    # rows=0 이면서 한 번도 실패로 잡히지 않았다. 조용히 틀리느니 터지는 게 낫다.
    if otp.upper() == "LOGOUT" or len(otp) < 16:
        raise RuntimeError(
            f"KRX OTP 발급 거부(응답={otp!r}) — MDCSTAT 계열이 회원 로그인을 요구한다. "
            f"이 소스는 현재 사용 불가다. 폐지 종목 주식수는 collectors/dart_shares.py 로 받는다."
        )
    csv_raw = _post(DOWNLOAD_URL, {"code": otp})
    return parse_snapshot(_decode(csv_raw)) if csv_raw else []


def _trading_dates(con: object, start: str | None, end: str | None) -> list[str]:
    # Postgres returns native `date` objects (not str) for a DATE column —
    # str() gives the same 'YYYY-MM-DD' either way, so downstream `.replace("-", "")`
    # always hits the str method instead of accidentally calling date.replace().
    rows = fetchall(
        con,
        "SELECT DISTINCT date FROM daily_bars WHERE date >= ? AND date <= ? ORDER BY date",
        (start or "1900-01-01", end or "2999-12-31"),
    )
    return [str(r[0]) for r in rows]


def main() -> int:
    ap = argparse.ArgumentParser(
        description="KRX 상장주식수 백필 (MDCSTAT01501, 무인증 point-in-time)")
    ap.add_argument("--date", help="단일 거래일 YYYY-MM-DD (미지정 시 --from/--to 범위)")
    ap.add_argument("--from", dest="from_", help="백필 시작일 YYYY-MM-DD")
    ap.add_argument("--to", dest="to_", help="백필 종료일 YYYY-MM-DD")
    ap.add_argument("--db", default=None)
    ap.add_argument("--sleep", type=float, default=1.5, help="KRX 요청 간 대기(초)")
    args = ap.parse_args()

    from .storage import connect, default_db_path, upsert_shares_outstanding
    con = connect(args.db or str(default_db_path()))

    if args.date:
        dates = [args.date]
    else:
        dates = _trading_dates(con, args.from_, args.to_)
    if not dates:
        print("no trading dates to process", flush=True)
        con.close()
        return 0

    total = 0
    for iso in dates:
        trd_dd = iso.replace("-", "")
        rows: list[tuple[str, str, int]] = []
        for mkt_id in MARKET_CODE.values():
            for code, shares in fetch_snapshot(mkt_id, trd_dd):
                rows.append((code, iso, shares))
            time.sleep(args.sleep)
        if rows:
            # source="krx" 명시 — 안 주면 DDL 기본값 'kiwoom' 이 박혀
            # KRX 로 받은 행이 키움 행으로 둔갑한다(실측: krx 소스 행이 0건이었다).
            total += upsert_shares_outstanding(con, rows, source="krx")
        print(f"[{iso}] codes={len(rows)} total={total}", flush=True)
    con.close()
    print(f"DONE dates={len(dates)} rows={total}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
