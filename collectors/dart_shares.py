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

**재개 단위는 (종목, 연도)다(2026-11-02).** 예전엔 코드 단위였다 — 그 코드에 행이 하나라도 있으면
건너뛰고, 조회가 끝나면 "자료 없음" 마커를 코드에 찍었다. 그런데 파서가 ``se == "보통주"`` 만 받아서
(``의결권이 있는\\n주식(보통주)``·``의결권있는 주식`` 등은 값이 있어도 ``None``) 값을 버렸고, 일한도 소진·
통신 오류도 ``None`` 이라 전부 "자료 없음" 마커가 됐다 — 009150·034730 은 영구히 재조회되지 않았고 코드-연도
1,873개가 비었다. 지금 규칙:

- **마커는 DART status 013 뿐** — 보고서 넷(사업·3분기·반기·1분기) 모두 013 일 때 ``dart_shares_013:<연도>``
  (:func:`year_marker`). 파싱 실패·통신 오류·일한도는 마커를 안 찍어 다음 실행이 다시 본다.
- 일한도를 다 쓰면 거기서 멈춘다(지금까지 받은 것은 남긴다) — 빈 결과로 넘기지 않는다. 종료 코드 2.
- 옛 코드 단위 마커(``dart_shares_listed``·``dart_shares_delisted``)는 그대로 둔다(지우지 않는다). 기본 실행은
  존중하고 ``--refetch`` 는 무시한다. ``--refetch`` 는 분기 폴백 행만 있는 해(옛 파서가 사업보고서 라벨을 못 읽은
  결과)도 다시 본다. 연도 마커는 ``--refetch`` 도 존중한다 — 그래야 한도에 걸린 재실행이 이어받는다.

**F1 전진 팔 보호.** ``date`` < 2026 인데 ``knowledge_date`` ≥ :data:`F1_ARM_START` 인 행은 쓰지 않고 로그만 남긴다
(:func:`f1_held_out`). 그런 행은 전진 구간 도중에 옛 기준일 값으로 주식수를 덮어 ``split_consistent_cap``
(swing-it ``research/paper/pead_paper.py``)의 시총을 사전등록 뒤에 바꾼다.

**첫 공시 도장(``--original``, 표 ``shares_original_filing``, migration 039).** ``stockTotqySttus`` 의 ``rcept_no`` 는
**마지막 정정본** 것이라 ``knowledge_date`` 가 늦다(017670 2021: 원본 2022-03-16, 저장 08-12 · 비교 가능 17,425행 중
3,397행, 30일 넘게 2,202행). 표가 (code, date) 한 행이고 ``ON CONFLICT DO NOTHING`` 이라 기존 행을 고치지 않는다 —
늦은 행마다 ``earnings_disclosures`` 의 원본 접수번호로 원문을 읽어 옆 표에 "처음 공개된 날·그때 값"을 더한다.
원문을 못 읽거나 기준일·크기가 안 맞으면 건너뛴다(늦은 도장이 남는다 — 틀린 값보다 없는 값).

CLI:
    python -m collectors.dart_shares --db <DSN>              # 폐지 종목 전체
    python -m collectors.dart_shares --db <DSN> --limit 20   # 표본
    python -m collectors.dart_shares --db <DSN> --dry-run    # 조회만

    # 대량 백필: 조회(DB 안 씀, 재개 가능) → 계획 확인 → 행 수 단언과 함께 한 트랜잭션으로 적재
    python -m collectors.dart_shares --listed --refetch --dry-run --ledger L.jsonl
    python -m collectors.dart_shares --apply-ledger L.jsonl --dry-run
    python -m collectors.dart_shares --apply-ledger L.jsonl --expect-rows N --expect-markers M
    python -m collectors.dart_shares --original --dry-run --ledger O.jsonl --key-env DART_API_KEY_2
    python -m collectors.dart_shares --apply-original O.jsonl --expect-rows N
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import time
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import date as Date
from pathlib import Path

from krx_fundamentals_client import DartQuotaExceededError, DartScraper

from .storage import (
    CHECKED_DART_SHARES,
    CHECKED_DART_SHARES_LISTED,
    CHECKED_DART_SHARES_YEAR,
    _is_pg,
    connect,
    default_db_path,
    fetchall,
    mark_checked,
)

#: F1 전진 팔 첫 결정일(swing-it ``research/paper/pead_paper.py`` ``ARM_START``) — 모듈 docstring.
F1_ARM_START = "2026-11-05"
#: DART 가 "공시 없음" 으로 답하는 status. 이것만 마커가 된다.
NO_FILING_STATUS = "013"
#: 한 해에 보는 보고서 수(사업·3분기·반기·1분기, 라이브러리 ``SHARES_REPORT_FALLBACK``).
N_REPORTS = 4
#: 원문 값이 저장된 정정본 값에서 이 배수 밖이면 파서가 다른 표를 읽은 것으로 보고 버린다.
ORIGINAL_MAX_RATIO = 2.0


def _dashed(yyyymmdd: str) -> str:
    """``"20251231"`` → ``"2025-12-31"``. 이미 대시가 있으면 그대로 둔다."""
    if len(yyyymmdd) == 8 and yyyymmdd.isdigit():
        return f"{yyyymmdd[:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:]}"
    return yyyymmdd


def year_marker(year: int) -> str:
    """연도 마커의 ``backfill_markers.source`` — 그 해 보고서가 DART 에 하나도 없다(013)."""
    return f"{CHECKED_DART_SHARES_YEAR}:{year}"


def f1_held_out(stlm: str, known: str) -> bool:
    """이 행을 쓰면 F1 전진 구간의 주식수를 옛 기준일 값으로 덮는가(모듈 docstring)."""
    return stlm < "2026-01-01" and known >= F1_ARM_START


@dataclass(frozen=True)
class YearResult:
    """한 (종목, 연도) 조회 결과. ``status``: ``row`` · ``no_filing``(보고서 넷 다 013) · ``no_value``(그 밖 전부)."""

    year: int
    status: str
    point: tuple[int, str, str] | None = None
    statuses: tuple[str, ...] = ()


async def _fetch_shares_with_rotation(
    scrapers: dict[str, DartScraper], keys: list[str], ki: list[int],
    ticker: str, year: int,
) -> YearResult:
    """한 (종목, 연도) 조회, 일한도(020)를 만나면 다음 키로 로테이션.

    ``dart_earnings._fetch_with_rotation``과 같은 계약이다 — ``ki``는 1칸
    리스트로 현재 키 인덱스를 담고, 호출부가 종목 루프 전체에서 재사용해야
    이미 소진된 키로 매번 되돌아가지 않는다. ``point`` 는
    ``(shares_outstanding, stlm_dt, knowledge_date)``(둘 다 ``YYYY-MM-DD``).

    **키를 다 쓰면 ``DartQuotaExceededError`` 를 그대로 던진다.** ``None`` 으로 돌려주던 때는 한도 소진이 "자료 없음"
    마커가 됐다. status 는 ``on_status`` 로 받아 013(공시 없음)과 나머지를 가른다 — 파싱 실패(000 인데 값 없음)·통신
    오류(라이브러리가 삼키고 다음 보고서로)·매핑 없음은 013 이 넷이 안 되므로 ``no_value`` 다.
    """
    while True:
        scraper = scrapers[keys[ki[0]]]
        statuses: list[str] = []
        try:
            result = await scraper.fetch_shares_outstanding(
                ticker, year, on_status=lambda status, _context: statuses.append(status))
        except DartQuotaExceededError:
            if ki[0] + 1 >= len(keys):
                raise
            ki[0] += 1
            print(f"DART 키 일한도(020) 도달 → 키{ki[0] + 1}로 로테이션 (stockTotqySttus)",
                  flush=True)
            continue
        if result is None or not result.shares_outstanding:
            no_filing = len(statuses) == N_REPORTS and all(st == NO_FILING_STATUS for st in statuses)
            return YearResult(year, "no_filing" if no_filing else "no_value", None, tuple(statuses))
        point = (result.shares_outstanding, _dashed(result.stlm_dt), _dashed(result.knowledge_date))
        if not (point[1] and point[2]):  # 기준일·접수번호 없는 값은 point-in-time 에 쓸 수 없다
            return YearResult(year, "no_value", None, tuple(statuses))
        return YearResult(year, "row", point, tuple(statuses))


async def shares_series(
    scrapers: dict[str, DartScraper], keys: list[str], ticker: str,
    years: Iterable[int], *, ki: list[int] | None = None,
    sleep: float = 0.15,
) -> list[YearResult]:
    """연도별 조회 결과 **시계열**.

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
    out: list[YearResult] = []
    for year in years:
        out.append(await _fetch_shares_with_rotation(scrapers, keys, ki, ticker, year))
        await asyncio.sleep(sleep)
    return out


#: 발행주식총수 상한 — 삼성전자가 ~5.97e9 로 최대다. 넘으면 단위 오류다.
MAX_PLAUSIBLE_SHARES = 10_000_000_000
#: 같은 종목 다른 해의 중앙값보다 이 배수 이상 크고 1,000 의 배수면 단위 오류로 본다.
UNIT_ERROR_RATIO = 500


def drop_unit_errors(series: list[tuple[int, str, str]],
                     reference: list[int] = ()) -> tuple[list[tuple[int, str, str]], list[tuple[int, str, str]]]:
    """(남길 점, 버린 점) — 회사가 "주식의 총수"를 천 주·백만 주 단위로 잘못 낸 해를 뺀다.

    **DART 원천이 틀린다(2026-10-12 실측, migration 022).** stockTotqySttus 가 082640 2019 를
    "161,358,585,000,000", 063170 2023 을 "17,774,267,000"(2022 는 "17,774,267")으로 준다 —
    27종목 31행, 거의 정확히 ×1,000·×1,000,000. 시총이 수천~수백만 배로 튄다.
    나눠서 "고치지" 않는다(원천과 다른 값을 만들지 않는다 — 틀린 값보다 없는 값).
    ``reference`` 는 DB 에 이미 있는 그 종목 값(키움 등) — 점이 하나뿐일 때 비교 기준이 된다.
    1e10 상한만으로는 ×1,000 된 소형주(500만 주 → 50억 주)를 못 잡아서 비율도 본다.
    """
    pool = sorted([sh for sh, *_ in series] + list(reference))
    median = pool[len(pool) // 2] if pool else 0
    keep, drop = [], []
    for point in series:
        shares = point[0]
        bad = shares > MAX_PLAUSIBLE_SHARES or (
            median and shares % 1000 == 0 and shares >= UNIT_ERROR_RATIO * median)
        (drop if bad else keep).append(point)
    return keep, drop


def _targets(
    con, *, listed: bool = False, from_year: int = 0, to_year: int = 9999, refetch: bool = False,
) -> list[tuple[str, list[int]]]:
    """``(code, [조회할 연도…])`` — 그 해에 시세가 있는데 주식수 행이 없는 (종목, 연도).

    ``listed`` 가 거짓이면 폐지 시세(``source='naver'``)가 있는 코드, 참이면 키움 등 상장 시세가 있는 코드다.

    폐지 쪽: 시세가 없는 폐지 종목은 대상이 아니다 — 시총을 계산할 가격 자체가 없다. **상장 쪽이 있어야 하는 이유**
    실측 2026-08-27: ``s.date <= '2018-06-01'`` 인 주식수를 가진 상장 종목이 5개뿐이었다 — 키움 주간 피드(ka10001)는
    현재 스냅샷만 주므로 2026년부터만 쌓이고, 시총을 분모로 쓰는 백테스트는 2026년 이전에서 대부분 NaN 이 되고 값이
    나오는 소수는 폐지 종목 쪽으로 치우친다 — 조용히 틀리는 종류다(kr-quant 기관수급 알파가 이걸로 VOID 됐다).

    **재개 단위는 (종목, 연도)다**(모듈 docstring). 빠지는 경우:

    - 그 해에 주식수 행이 있다. 단 ``refetch`` 면 ``-12-31`` 행 없이 분기 폴백 행만 있는 해는 다시 본다.
    - 연도 마커(013). ``refetch`` 도 존중한다.
    - 옛 코드 단위 마커. ``refetch`` 면 무시한다 — 지우지는 않는다. 42종목이 주당 2.2분, 60종목이 회차마다 성과
      0행으로 돌던 것을 막으려고 생겼는데, 파싱 실패·한도 소진까지 "자료 없음"으로 찍은 것이 이번 공백의 원인이다.
    """
    # **코드순이면 안 된다.** 한국 종목코드는 상장 시기순에 가까워서, `ORDER BY code` + `--limit` 으로 자른 부분집합이
    # 곧 **시대 표본추출**이 된다. 실측 2026-08-27: 2,595종목 중 앞 2,000개를 먼저 돌렸더니 2016년 상장분은 97% 완료인데
    # 2023~24년 상장분은 87%가 미처리로 남았다. md5 셔플이면 재현성은 같고 중간에 멈춘 부분집합은 시대별로 균일하다.
    cond = "source <> 'naver'" if listed else "source = 'naver'"
    traded = fetchall(
        con,
        f"SELECT code, substr(CAST(date AS TEXT), 1, 4) FROM daily_bars WHERE {cond} GROUP BY 1, 2",  # noqa: S608
    )
    rows: dict[tuple[str, int], list[str]] = {}
    for code, day in fetchall(con, "SELECT code, date FROM shares_outstanding_history"):
        rows.setdefault((code, int(str(day)[:4])), []).append(str(day))
    done: set[tuple[str, int]] = set()
    for code, source in fetchall(
        con, "SELECT code, source FROM backfill_markers WHERE source LIKE ?",
        (f"{CHECKED_DART_SHARES_YEAR}:%",),
    ):
        done.add((code, int(source.rsplit(":", 1)[1])))
    legacy: set[str] = set()
    if not refetch:
        legacy = {r[0] for r in fetchall(
            con, "SELECT code FROM backfill_markers WHERE source = ?",
            (CHECKED_DART_SHARES_LISTED if listed else CHECKED_DART_SHARES,),
        )}
    out: dict[str, list[int]] = {}
    for code, year_text in traded:
        year = int(year_text)
        if not from_year <= year <= to_year or code in legacy or (code, year) in done:
            continue
        have = rows.get((code, year))
        if have and not (refetch and not any(d.endswith("-12-31") for d in have)):
            continue
        out.setdefault(code, []).append(year)
    return sorted(((c, sorted(y)) for c, y in out.items()),
                  key=lambda item: hashlib.md5(item[0].encode()).hexdigest())  # noqa: S324 — 셔플용


def _write(con, records: list[tuple]) -> int:
    """(code, date, shares, knowledge_date, source) 삽입. 기존 행은 보존."""
    if not records:
        return 0
    from .storage import _upsert
    return _upsert(con, "shares_outstanding_history",
                   ["code", "date", "shares_outstanding", "knowledge_date", "source"],
                   records, on_conflict="nothing")


def _ledger_line(code: str, r: YearResult) -> str:
    shares, stlm, known = r.point or (None, None, None)
    return json.dumps({"code": code, "year": r.year, "status": r.status, "shares": shares, "date": stlm,
                       "knowledge_date": known, "statuses": list(r.statuses)}, ensure_ascii=False) + "\n"


def _read_ledger(path: str | Path) -> list[dict]:
    from .dart_doc_run import read_jsonl

    p = Path(path)
    return read_jsonl(p) if p.exists() else []


async def _run(args: argparse.Namespace) -> int:
    from .config import mask_dsn
    from .dart_earnings import collect_keys

    keys = [os.environ[args.key_env]] if args.key_env else collect_keys()
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
    # 2016 = daily_bars 이력의 시작, 2025 = 2026~ 은 weekly_listed_shares 몫(monthly DAG 주석). 폐지 쪽은 생애 전체.
    from_year = args.from_year if args.from_year is not None else (2016 if args.listed else 0)
    to_year = args.to_year if args.to_year is not None else (2025 if args.listed else 9999)
    targets = _targets(con, listed=args.listed, from_year=from_year, to_year=to_year, refetch=args.refetch)
    # 읽기 트랜잭션을 열어 둔 채 몇 시간 네트워크를 돌지 않는다 — idle in transaction 이 압축 정책 뒤로 전 DAG 를 막는다(2026-10-14).
    con.rollback()
    ledger = None
    if args.ledger:
        seen = {(r["code"], r["year"]) for r in _read_ledger(args.ledger) if r["status"] != "no_value"}
        targets = [(c, [y for y in ys if (c, y) not in seen]) for c, ys in targets]
        targets = [(c, ys) for c, ys in targets if ys]
    if args.limit:
        targets = targets[: args.limit]
    if not targets:
        con.close()
        print("대상 0건 — 받을 게 없다(백필 완료 상태). corp_map 로드 없이 종료.", flush=True)
        return 0

    if args.ledger:
        ledger = open(args.ledger, "a", encoding="utf-8")  # noqa: SIM115 — finally 에서 닫는다
    scrapers = {key: DartScraper(api_key=key) for key in keys}
    n_years = sum(len(ys) for _, ys in targets)
    print(f"🔌 {mask_dsn(args.db)} | 대상 {len(targets)}종목 {n_years}코드-연도"
          f"{' | DRY-RUN' if args.dry_run else ''}", flush=True)

    # 키 인덱스는 종목 루프 전체에서 공유한다 — 종목마다 [0] 으로 되돌리면
    # 이미 소진된 키에 매번 한 번씩 더 부딪힌다.
    ki = [0]
    tally: Counter = Counter()
    written = 0
    t0 = time.time()
    exit_code = 0
    today = time.strftime("%Y-%m-%d")
    try:
        for i, (code, years) in enumerate(targets, 1):
            try:
                results = await shares_series(scrapers, keys, code, years, ki=ki, sleep=args.sleep)
            except DartQuotaExceededError:
                print(f"DART 일한도 — 키 {len(keys)}개 소진. {code} 앞에서 멈춘다(마커 안 찍음). 다음 실행이 이어받는다.",
                      flush=True)
                exit_code = 2
                break
            tally.update(r.status for r in results)
            if ledger:
                ledger.writelines(_ledger_line(code, r) for r in results)
                ledger.flush()
            series = [r.point for r in results if r.point]
            if series:
                reference = [r[0] for r in fetchall(
                    con, "SELECT shares_outstanding FROM shares_outstanding_history WHERE code=?", (code,))
                    if r[0]]
                con.rollback()  # 위와 같은 이유 — --dry-run 은 커밋할 일이 없다
                series, dropped = drop_unit_errors(series, reference)
                for shares, stlm, _ in dropped:
                    print(f"⚠️ {code} {stlm}: 발행주식총수 {shares:,} — DART 단위 오류로 보고 버린다", flush=True)
            keep = []
            for shares, stlm, rcept in series:
                if f1_held_out(stlm, rcept):
                    tally["f1_held_out"] += 1
                    print(f"⚠️ {code} {stlm}: 접수일 {rcept} ≥ {F1_ARM_START} — F1 전진 보호로 쓰지 않는다", flush=True)
                else:
                    keep.append((code, stlm, shares, rcept, "dart"))
            if not args.dry_run:
                written += _write(con, keep)
                for r in results:
                    if r.status == "no_filing":
                        mark_checked(con, year_marker(r.year), [code], today)
            if i % 50 == 0 or i == len(targets):
                el = time.time() - t0
                rate = i / el if el else 0
                print(f"  [{i}/{len(targets)}] {dict(tally)} 기록={written}행 | {rate:.1f}종목/s "
                      f"ETA {(len(targets)-i)/rate/60 if rate else 0:.1f}분", flush=True)
    finally:
        if ledger:
            ledger.close()
        for scraper in scrapers.values():
            await scraper.close()

    con.close()
    print(f"DONE targets={len(targets)}종목 {dict(tally)} 기록={written}행", flush=True)
    return exit_code


# ── 원장 적재(대량 백필) ─────────────────────────────────────────────────────


@dataclass
class LedgerPlan:
    """원장 → 넣을 행. 기존 행은 어떤 경우에도 건드리지 않는다."""

    records: list[tuple] = field(default_factory=list)
    conflicts: list[tuple] = field(default_factory=list)  # 이미 있는 (code, date) — 값이 다를 수 있다
    held_out: list[tuple] = field(default_factory=list)  # F1 전진 보호
    unit_errors: list[tuple] = field(default_factory=list)
    markers: list[tuple[str, int]] = field(default_factory=list)
    no_value: list[tuple[str, int]] = field(default_factory=list)


def plan_ledger(rows: Iterable[dict], existing: Iterable[tuple]) -> LedgerPlan:
    """``rows``: 원장 줄(같은 (종목, 연도)는 뒤 줄이 이긴다). ``existing``: 표의 (code, date, shares)."""
    latest = {(r["code"], r["year"]): r for r in rows}
    have: dict[str, dict[str, int]] = {}
    for code, day, shares in existing:
        have.setdefault(code, {})[str(day)] = shares
    plan = LedgerPlan()
    by_code: dict[str, list[tuple[int, str, str]]] = {}
    for (code, year), r in sorted(latest.items()):
        if r["status"] == "row":
            by_code.setdefault(code, []).append((r["shares"], r["date"], r["knowledge_date"]))
        elif r["status"] == "no_filing":
            plan.markers.append((code, year))
        else:
            plan.no_value.append((code, year))
    for code, series in by_code.items():
        mine = have.get(code, {})
        keep, dropped = drop_unit_errors(series, [v for v in mine.values() if v])
        plan.unit_errors += [(code, *p) for p in dropped]
        seen: set[str] = set()
        for shares, stlm, known in keep:
            if stlm in mine or stlm in seen:
                plan.conflicts.append((code, stlm, shares, known))
            elif f1_held_out(stlm, known):
                plan.held_out.append((code, stlm, shares, known))
            else:
                seen.add(stlm)
                plan.records.append((code, stlm, shares, known, "dart"))
    return plan


def plan_report(plan: LedgerPlan, *, sample: int = 30) -> str:
    """연도별 건수·표본 — 적재 전에 사람이 보는 표."""
    per_year: dict[int, Counter] = {}
    for r in plan.records:
        per_year.setdefault(int(r[1][:4]), Counter())["new_rows"] += 1
    for name, items in (("no_filing", plan.markers), ("no_value", plan.no_value)):
        for _, year in items:
            per_year.setdefault(year, Counter())[name] += 1
    for name, items in (("conflict", plan.conflicts), ("f1_held_out", plan.held_out), ("unit_error", plan.unit_errors)):
        for r in items:
            per_year.setdefault(int(r[1][:4]), Counter())[name] += 1
    cols = ("new_rows", "no_filing", "no_value", "conflict", "f1_held_out", "unit_error")
    lines = ["year " + " ".join(f"{c:>11}" for c in cols)]
    for year in sorted(per_year):
        lines.append(f"{year} " + " ".join(f"{per_year[year][c]:>11}" for c in cols))
    lines.append("합계 " + " ".join(f"{sum(y[c] for y in per_year.values()):>11}" for c in cols))
    picked = sorted(plan.records, key=lambda r: hashlib.md5(f"{r[0]}{r[1]}".encode()).hexdigest())[:sample]  # noqa: S324
    lines.append(f"표본 {len(picked)}행 (code, date, shares, knowledge_date, source):")
    lines += [f"  {r}" for r in picked]
    for name, items in (("F1 보호로 안 쓴 행", plan.held_out), ("단위 오류로 버린 행", plan.unit_errors)):
        lines.append(f"{name} {len(items)}:")
        lines += [f"  {r}" for r in items]
    return "\n".join(lines)


def _table_totals(con, table: str, value: str) -> tuple[int, int]:
    row = fetchall(con, f"SELECT count(*), coalesce(sum({value}), 0) FROM {table}")[0]  # noqa: S608
    return int(row[0]), int(row[1])


def insert_new_rows(con, table: str, cols: list[str], records: list[tuple], *, value: str, expect: int) -> int:
    """기존 행은 건드리지 않고 ``records`` 만 넣는다 — 한 트랜잭션, 행 수·합계 단언(CLAUDE.md §1).

    ``records`` 키가 하나라도 이미 있거나 ``expect`` 와 다르면 되돌리고 ``AssertionError``. ``cols`` 앞 둘은 (code, date),
    ``value`` 열은 ``records`` 의 세 번째 값이어야 한다. 합계까지 보는 이유: 행 수만 맞고 기존 행 값이 바뀌는 경로를 막는다.
    """
    if len(records) != expect:
        raise AssertionError(f"{table}: 넣을 행 {len(records)} ≠ 예상 {expect} — 아무것도 쓰지 않았다")
    if not records:
        return 0
    pg = _is_pg(con)
    try:
        n0, s0 = _table_totals(con, table, value)
        if pg:
            import psycopg2.extras  # noqa: PLC0415

            sql = f"INSERT INTO {table}({','.join(cols)}) VALUES %s ON CONFLICT (code, date) DO NOTHING"  # noqa: S608
            inserted = 0
            with con.cursor() as cur:
                for start in range(0, len(records), 500):
                    page = records[start:start + 500]
                    psycopg2.extras.execute_values(cur, sql, page, page_size=len(page))
                    inserted += cur.rowcount
        else:
            marks = ",".join("?" * len(cols))
            inserted = con.executemany(
                f"INSERT OR IGNORE INTO {table}({','.join(cols)}) VALUES({marks})", records).rowcount  # noqa: S608
        n1, s1 = _table_totals(con, table, value)
        added = sum(int(r[2]) for r in records)
        if inserted != expect or n1 - n0 != expect or s1 - s0 != added:
            raise AssertionError(
                f"{table}: 삽입 {inserted}, 행 수 {n0}→{n1}, 합계 차 {s1 - s0} ≠ 예상 {expect}행·{added} — 되돌린다")
    except Exception:
        con.rollback()
        raise
    con.commit()
    return inserted


def _set_write_limits(con) -> None:
    if _is_pg(con):
        # 압축 청크에 넣는다 — 죽은 쿼리가 압축 정책 뒤로 전 DAG 를 막은 일(2026-10-14)이 있다.
        fetchall(con, "SELECT set_config('statement_timeout', '600s', false), set_config('lock_timeout', '30s', false)")


def apply_ledger(args: argparse.Namespace) -> int:
    con = connect(args.db or default_db_path())
    plan = plan_ledger(_read_ledger(args.apply_ledger),
                       fetchall(con, "SELECT code, date, shares_outstanding FROM shares_outstanding_history"))
    print(plan_report(plan), flush=True)
    if args.dry_run:
        con.close()
        return 0
    if args.expect_rows is None or args.expect_markers is None:
        raise SystemExit("--expect-rows·--expect-markers 필요 — --dry-run 표를 보고 적는다")
    if len(plan.markers) != args.expect_markers:
        raise SystemExit(f"마커 {len(plan.markers)} ≠ 예상 {args.expect_markers} — 아무것도 쓰지 않았다")
    _set_write_limits(con)
    n = insert_new_rows(con, "shares_outstanding_history",
                        ["code", "date", "shares_outstanding", "knowledge_date", "source"],
                        plan.records, value="shares_outstanding", expect=args.expect_rows)
    today = time.strftime("%Y-%m-%d")
    by_year: dict[int, list[str]] = {}
    for code, year in plan.markers:
        by_year.setdefault(year, []).append(code)
    for year, codes in by_year.items():
        mark_checked(con, year_marker(year), codes, today)
    con.close()
    print(f"DONE 행 {n} 마커 {len(plan.markers)}", flush=True)
    return 0


# ── 첫 공시 도장(shares_original_filing) ─────────────────────────────────────


def period_of(day: str) -> str | None:
    """기준일 → ``earnings_disclosures.period``. 분기말이 아니면 ``None``(비12월 결산사 표는 그 표에 없다)."""
    month = int(day[5:7])
    return f"{day[:4]}Q{month // 3}" if month % 3 == 0 else None


def late_rows(main: Iterable[tuple], disclosures: Iterable[tuple], done: Iterable[tuple]) -> list[dict]:
    """``knowledge_date`` 가 원본 접수일보다 늦은 dart 행 → 원문을 읽을 목록.

    ``main``: (code, date, shares, knowledge_date). ``disclosures``: (code, period, rcept_no, rcept_dt). ``done``: 옆 표의 (code, date).
    """
    first = {(c, p): (no, str(dt)) for c, p, no, dt in disclosures}
    finished = {(c, str(d)) for c, d in done}
    out = []
    for code, day, shares, known in main:
        day, known = str(day), str(known)
        hit = first.get((code, period_of(day))) if period_of(day) else None
        if hit and hit[1] < known and (code, day) not in finished and not f1_held_out(day, hit[1]):
            out.append({"code": code, "date": day, "shares": shares, "knowledge_date": known,
                        "rcept_no": hit[0], "first_public_date": hit[1]})
    return sorted(out, key=lambda r: hashlib.md5(f"{r['code']}{r['date']}".encode()).hexdigest())  # noqa: S324


def original_value(parsed, target: dict) -> tuple[int | None, str]:
    """원문 파싱 결과(:class:`dart_doc.Shares` | None) → (원본 값, 사유). 값이 ``None`` 이면 건너뛴다(늦은 도장이 남는다)."""
    if parsed is None or not parsed.common or parsed.common <= 0:
        return None, "no_table"
    if parsed.asof is not None and parsed.asof != Date.fromisoformat(target["date"]):
        return None, "asof_mismatch"
    ratio = parsed.common / target["shares"]
    if not 1 / ORIGINAL_MAX_RATIO <= ratio <= ORIGINAL_MAX_RATIO:
        return None, "ratio_outlier"
    if parsed.asof is None and parsed.common != target["shares"]:  # 기준일을 못 찾았으면 정정본과 같을 때만 믿는다
        return None, "asof_unknown"
    return parsed.common, "ok"


def fetch_originals(args: argparse.Namespace) -> int:  # pragma: no cover — 네트워크(판정은 original_value·late_rows)
    from . import dart_doc as dd
    from .dart_doc_run import DEFAULT_CACHE, Dart, DartBudget, DartQuota, DartTransient

    if not args.ledger or not args.key_env:
        raise SystemExit("--original 은 --ledger·--key-env 필요 — 원장에 받고 --apply-original 로 넣는다")
    con = connect(args.db or default_db_path())
    targets = late_rows(
        fetchall(con, "SELECT code, date, shares_outstanding, knowledge_date FROM shares_outstanding_history "
                      "WHERE source = 'dart' AND knowledge_date IS NOT NULL AND shares_outstanding > 0"),
        fetchall(con, "SELECT code, period, rcept_no, rcept_dt FROM earnings_disclosures WHERE kind = 'periodic'"),
        fetchall(con, "SELECT code, date FROM shares_original_filing"),
    )
    con.close()
    seen = {(r["code"], r["date"]) for r in _read_ledger(args.ledger) if r["why"] != "transient"}
    targets = [t for t in targets if (t["code"], t["date"]) not in seen]
    if args.limit:
        targets = targets[: args.limit]
    print(f"첫 공시 원문 대상 {len(targets)}행", flush=True)
    dart = Dart(os.environ[args.key_env], DEFAULT_CACHE, key_name=args.key_env, max_calls=args.max_calls,
                pause=args.sleep)
    tally: Counter = Counter()
    t0 = time.time()
    code = 0
    with open(args.ledger, "a", encoding="utf-8") as ledger:
        for i, t in enumerate(targets, 1):
            value, why = None, "no_doc"
            try:
                raw = dart.document(t["rcept_no"])
            except (DartQuota, DartBudget) as e:
                print(f"멈춘다({type(e).__name__}: {e}) — 같은 명령으로 이어받는다", flush=True)
                code = 2
                break
            except DartTransient:
                raw, why = None, "transient"
            if raw is not None:
                try:
                    files = dd.doc_files(raw)
                    blocks = dd.parse_blocks(files[0][1])
                    parsed = dd.extract_shares(blocks[dd.report_body_start(blocks):], Date.fromisoformat(t["date"]))
                    value, why = original_value(parsed, t)
                except Exception as e:  # noqa: BLE001 — 원문 한 건이 깨져도 그 행만 건너뛴다
                    why = f"parse_error:{type(e).__name__}"
            tally[why.split(":")[0]] += 1
            ledger.write(json.dumps({**t, "shares_original": value, "why": why}, ensure_ascii=False) + "\n")
            ledger.flush()
            if i % 200 == 0 or i == len(targets):
                rate = i / max(time.time() - t0, 1e-9)
                print(f"  [{i}/{len(targets)}] {dict(tally)} 오늘 호출 {dart.today} ETA {(len(targets)-i)/rate/60:.0f}분", flush=True)
    dart.flush()
    print(f"DONE {dict(tally)}", flush=True)
    return code


def apply_originals(args: argparse.Namespace) -> int:
    con = connect(args.db or default_db_path())
    have = {(c, str(d)) for c, d in fetchall(con, "SELECT code, date FROM shares_original_filing")}
    rows = {(r["code"], r["date"]): r for r in _read_ledger(args.apply_original)}
    records = [(r["code"], r["date"], r["shares_original"], r["first_public_date"], r["rcept_no"])
               for k, r in sorted(rows.items()) if r["shares_original"] and k not in have]
    print(f"원장 {len(rows)}행 · 사유 {dict(Counter(r['why'].split(':')[0] for r in rows.values()))} · 넣을 행 {len(records)} · "
          f"값이 정정본과 다름 {sum(r['shares_original'] != r['shares'] for r in rows.values() if r['shares_original'])}", flush=True)
    if args.dry_run:
        con.close()
        return 0
    if args.expect_rows is None:
        raise SystemExit("--expect-rows 필요 — --dry-run 표를 보고 적는다")
    _set_write_limits(con)
    n = insert_new_rows(con, "shares_original_filing",
                        ["code", "date", "shares_original", "first_public_date", "rcept_no"],
                        records, value="shares_original", expect=args.expect_rows)
    con.close()
    print(f"DONE 행 {n}", flush=True)
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="상장주식수 백필 (DART) — 기본은 폐지 종목")
    ap.add_argument("--db", default=None, help="sqlite 경로 또는 DSN(없으면 환경변수 KR_QUANT_DB, 그것도 없으면 로컬 sqlite)")
    ap.add_argument("--limit", type=int, default=0, help="상위 N종목만 (0=전체)")
    ap.add_argument("--sleep", type=float, default=0.15, help="DART 호출 간 대기(초)")
    ap.add_argument("--dry-run", action="store_true", help="DB 에 쓰지 않는다")
    ap.add_argument(
        "--refetch", action="store_true",
        help="옛 코드 단위 마커를 무시하고(지우지 않는다) 분기 폴백 행만 있는 해도 다시 조회한다. 연도 마커(013)는 존중",
    )
    ap.add_argument(
        "--listed", action="store_true",
        help="폐지 종목 대신 **현재 상장 종목**의 과거 주식수를 백필한다 "
             "(_targets 참고). 시총 분모가 2026년 이전에 없는 문제를 메운다.",
    )
    ap.add_argument("--from-year", type=int, default=None, help="시작 연도(기본 --listed 2016, 폐지 생애 전체)")
    ap.add_argument("--to-year", type=int, default=None, help="종료 연도(기본 --listed 2025, 폐지 생애 전체)")
    ap.add_argument("--ledger", help="조회 결과를 JSONL 로 남기고 이어받는다(no_value·transient 는 다시 본다)")
    ap.add_argument("--key-env", help="이 환경변수의 키만 쓴다(DAG 몫을 남기려고 뒤쪽 키로 돌릴 때)")
    ap.add_argument("--apply-ledger", help="원장을 표에 넣는다(--dry-run 이면 계획 표만)")
    ap.add_argument("--expect-rows", type=int, default=None, help="넣을 행 수 단언 — 다르면 아무것도 안 쓴다")
    ap.add_argument("--expect-markers", type=int, default=None, help="찍을 연도 마커 수 단언")
    ap.add_argument("--original", action="store_true", help="늦은 도장 행의 원본 공시 원문을 읽어 원장에 남긴다")
    ap.add_argument("--apply-original", help="첫 공시 원장을 shares_original_filing 에 넣는다")
    ap.add_argument("--max-calls", type=int, default=15000, help="--original 키당 하루 호출 상한")
    args = ap.parse_args()
    # DSN 은 환경변수로 받는 게 낫다 — 명령줄 DSN 은 ps 에 비밀번호가 찍힌다(data_checks 와 같은 이유). --db 는 DAG 호환.
    args.db = args.db or os.environ.get("KR_QUANT_DB")

    if args.apply_ledger:
        return apply_ledger(args)
    if args.apply_original:
        return apply_originals(args)
    if args.original:
        return fetch_originals(args)
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
