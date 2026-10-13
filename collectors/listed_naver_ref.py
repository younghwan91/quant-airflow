"""상장 종목 네이버 수정주가 2007-11~2016-09 → daily_bars_listed_naver (migration 037).

**왜.** ``daily_bars_adj_v2`` 는 원주가(``daily_bars_raw``) × ``corporate_actions`` 계수다. 키움이 이벤트를 안 준
날(대부분 거래정지 뒤 재개일의 감자·병합 — 003720 2011-05-04 원주가 38,200 → 4,390)은 수정주가 ÷ 원주가 비율의
계단으로 찾는데(``adjusted_v2.infer_events``), 키움 수정주가(``daily_bars``)는 2016-09 부터만 있다. 그 앞 구간의
기준으로 네이버 siseJson(키움 수정주가와 같은 값 — ``naver_delisted_bars`` 머리말)을 받아 둔다. 수익률 원천이
아니고 ``daily_bars`` 에도 넣지 않는다(그 표에 2016 이전 구간을 만들지 않는다 — 028 과 같은 이유).

대상 = ``daily_bars_raw`` 에 2016-09-05 이전 행이 있는 코드. 종목당 요청 1회(전 구간 한 번에). 받아 본 코드는
``backfill_markers`` 로 남겨 재실행 때 건너뛴다(자료 유무 무관, 네트워크 실패는 남기지 않는다).

CLI:
    python -m collectors.listed_naver_ref --db <DSN>
    python -m collectors.listed_naver_ref --db <DSN> --codes 003720 --refetch
"""

from __future__ import annotations

import argparse
import time
from typing import Any

from .naver_delisted_bars import fetch_sise, parse_sise
from .storage import CHECKED_LISTED_NAVER, DAILY_BAR_COLUMNS, _upsert, checked_codes, connect, default_db_path, fetchall, mark_checked

START, END = "20071101", "20160930"   # daily_bars(키움 수정주가) 가 2016-09-05 부터 — 겹치게 받는다
TABLE = "daily_bars_listed_naver"


def to_records(body: str, code: str) -> list[tuple]:
    return [(*r[:-1], "naver_adj") for r in parse_sise(body, code)]


def target_codes(con: Any) -> list[str]:
    return [r[0] for r in fetchall(con, "SELECT DISTINCT code FROM daily_bars_raw WHERE date < '2016-09-05' ORDER BY 1")]


def collect(con: Any, codes: list[str], *, fetch=fetch_sise, sleep: float = 0.15) -> dict[str, int]:
    stats = {"codes": len(codes), "with_data": 0, "empty": 0, "failed": 0, "rows": 0}
    done: list[str] = []
    today = time.strftime("%Y-%m-%d")
    t0 = time.time()
    for i, code in enumerate(codes, 1):
        body = fetch(code, start=START, end=END)
        if not body:
            stats["failed"] += 1                     # 네트워크 실패 — 표시하지 않아 다음에 다시 받는다
        else:
            rows = to_records(body, code)
            stats["with_data" if rows else "empty"] += 1
            stats["rows"] += _upsert(con, TABLE, [*DAILY_BAR_COLUMNS, "source"], rows) if rows else 0
            done.append(code)
        if i % 200 == 0 or i == len(codes):
            mark_checked(con, CHECKED_LISTED_NAVER, done, today)
            done = []
            print(f"  [{i}/{len(codes)}] {stats} | {i / (time.time() - t0):.1f}종목/s", flush=True)
        time.sleep(sleep)
    return stats


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--db", default=None)
    ap.add_argument("--codes", default="")
    ap.add_argument("--refetch", action="store_true")
    ap.add_argument("--sleep", type=float, default=0.15)
    a = ap.parse_args()
    con = connect(a.db or default_db_path())
    codes = [c for c in a.codes.split(",") if c] or target_codes(con)
    if not a.refetch:
        seen = checked_codes(con, CHECKED_LISTED_NAVER)
        codes = [c for c in codes if c not in seen]
    stats = collect(con, codes, sleep=a.sleep)
    print(f"✅ 완료: {stats}")
    if stats["codes"] and stats["rows"] == 0 and stats["with_data"] == 0:
        raise SystemExit("0행 — 빈 응답을 성공으로 넘기지 않는다(siseJson 이 바뀌었는지 확인)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
