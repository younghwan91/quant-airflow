"""생존편향 데모 — 키·DB·네트워크 없이 돈다.

합성 데이터(실제 종목 아님)를 sqlite 에 적재한 뒤, 같은 전략("2018-01-02 에 유니버스
전체를 동일가중으로 사서 보유")을 두 가지 유니버스로 계산합니다.

  1. 지금 상장된 종목만 (폐지 종목이 빠진 데이터 — 흔한 무료 데이터의 모양)
  2. 그때 거래되던 종목 전부 (이 파이프라인이 `delisted_stocks` 로 복원하는 모양)

실행: python scripts/demo_survivorship.py
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from collectors.storage import connect  # noqa: E402

START = "2018-01-02"
END = "2025-12-30"

# (code, name, 시작가, 마지막 거래일, 마지막 가격, 상장폐지 여부) — 모두 합성
UNIVERSE = [
    ("900001", "생존A", 10_000, END, 31_000, False),
    ("900002", "생존B", 20_000, END, 52_000, False),
    ("900003", "생존C", 15_000, END, 24_000, False),
    ("900004", "생존D", 30_000, END, 33_000, False),
    ("900005", "폐지E", 12_000, "2020-03-20", 900, True),
    ("900006", "폐지F", 25_000, "2021-06-11", 1_500, True),
    ("900007", "폐지G", 8_000, "2019-11-05", 400, True),
]


def build(con) -> None:
    for code, name, p0, last_date, p1, delisted in UNIVERSE:
        if delisted:
            con.execute("INSERT INTO delisted_stocks(code, name, market, last_trade_date) "
                        "VALUES (?,?,?,?)", (code, name, "synthetic", last_date))
        else:
            con.execute("INSERT INTO stocks(code, name, market) VALUES (?,?,?)",
                        (code, name, "synthetic"))
        for date, close in ((START, p0), (last_date, p1)):
            con.execute("INSERT INTO daily_bars(code, date, close, source) VALUES (?,?,?,?)",
                        (code, date, close, "synthetic"))
    con.commit()


def mean_return(con, codes: list[str]) -> float:
    rets = []
    for code in codes:
        closes = [r[0] for r in con.execute(
            "SELECT close FROM daily_bars WHERE code=? ORDER BY date", (code,))]
        rets.append(closes[-1] / closes[0] - 1)
    return sum(rets) / len(rets)


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        con = connect(Path(tmp) / "demo.db")
        build(con)
        alive = [r[0] for r in con.execute("SELECT code FROM stocks ORDER BY code")]
        dead = [r[0] for r in con.execute("SELECT code FROM delisted_stocks ORDER BY code")]
        r_alive = mean_return(con, alive)
        r_all = mean_return(con, alive + dead)
    print("[합성 데이터] 2018-01-02 동일가중 매수 후 보유")
    print(f"  지금 상장된 {len(alive)}종목만      : {r_alive:+.1%}")
    print(f"  폐지 {len(dead)}종목 포함 {len(alive) + len(dead)}종목   : {r_all:+.1%}")
    print(f"  생존편향              : {r_alive - r_all:+.1%}p 과대평가")


if __name__ == "__main__":
    main()
