"""README 히어로 이미지 — 상장폐지 종목이 데이터에 남아 있다(생존편향).

연도별로 일봉(`daily_bars_adj_v2`)이 있는 종목 수를 "지금도 상장" / "이후 상장폐지" 로 나눠 누적막대로 그린다.
폐지 판정 = 현재 마스터 `stocks` 에 없고 `delisted_stocks` ∪ `kind_delistings` 에 있는 코드.
ETF 3종(어느 쪽에도 없음)은 제외. 2007 은 11-22 부터라 막대에서 뺀다(합계에는 포함).

실행(레포 루트, 읽기 전용 SELECT 만):
    set -a; . ./.env; set +a
    .venv/bin/python scripts/make_hero.py              # DB → docs/images/hero-data.csv → PNG 2장
    .venv/bin/python scripts/make_hero.py --from-csv   # DB 없이 CSV 로 재생성
"""
from __future__ import annotations

import argparse
import csv
import os
import pathlib
import sys

import numpy as np

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from hero_style import caption, render_both  # noqa: E402

ROOT = pathlib.Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "docs" / "images"
CSV_PATH = OUT_DIR / "hero-data.csv"
ASOF = "2026-10-19"
FIRST_FULL_YEAR = 2008

SQL = """
WITH u AS (
    SELECT DISTINCT code, EXTRACT(year FROM date)::int AS year
    FROM daily_bars_adj_v2
    WHERE close IS NOT NULL
), d AS (
    SELECT code FROM delisted_stocks
    UNION
    SELECT code FROM kind_delistings
), s AS (
    SELECT code FROM stocks
), yearly AS (
    SELECT year::text AS year,
           count(*) FILTER (WHERE code IN (SELECT code FROM s)) AS listed,
           count(*) FILTER (WHERE code NOT IN (SELECT code FROM s) AND code IN (SELECT code FROM d)) AS delisted
    FROM u GROUP BY year
), total AS (
    SELECT 'all' AS year,
           count(DISTINCT code) FILTER (WHERE code IN (SELECT code FROM s)) AS listed,
           count(DISTINCT code) FILTER (WHERE code NOT IN (SELECT code FROM s) AND code IN (SELECT code FROM d)) AS delisted
    FROM u
)
SELECT * FROM yearly UNION ALL SELECT * FROM total ORDER BY year
"""


def _connect(dsn: str):
    try:
        import psycopg  # type: ignore

        conn = psycopg.connect(dsn)
        conn.read_only = True
        return conn
    except ImportError:
        import psycopg2  # type: ignore

        conn = psycopg2.connect(dsn)
        conn.set_session(readonly=True, autocommit=True)
        return conn


def load_from_db() -> list[dict]:
    dsn = os.environ.get("KR_QUANT_DB")
    if not dsn:
        sys.exit("KR_QUANT_DB 가 없다 — `set -a; . ./.env; set +a` 뒤에 실행")
    conn = _connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(SQL)
            rows = [{"year": r[0], "listed": int(r[1]), "delisted": int(r[2])} for r in cur.fetchall()]
    finally:
        conn.close()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    with CSV_PATH.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["year", "listed", "delisted"])
        w.writeheader()
        w.writerows(rows)
    return rows


def load_from_csv() -> list[dict]:
    with CSV_PATH.open() as f:
        return [{"year": r["year"], "listed": int(r["listed"]), "delisted": int(r["delisted"])} for r in csv.DictReader(f)]


def draw_factory(rows: list[dict]):
    total = next(r for r in rows if r["year"] == "all")
    years = [r for r in rows if r["year"] != "all" and int(r["year"]) >= FIRST_FULL_YEAR]
    x = np.array([int(r["year"]) for r in years])
    listed = np.array([r["listed"] for r in years])
    delisted = np.array([r["delisted"] for r in years])
    n_all = total["listed"] + total["delisted"]
    pct_all = 100 * total["delisted"] / n_all
    share = delisted / (listed + delisted)
    i_peak = int(share.argmax())

    def draw(fig, ax, p):
        w = 0.72
        edge = dict(edgecolor=p["surface"], linewidth=1.0)
        ax.bar(x, listed, width=w, color=p["blue"], label="Still listed today", **edge)
        ax.bar(x, delisted, width=w, bottom=listed, color=p["amber"], label="Delisted since", **edge)

        ax.set_title(
            f"{pct_all:.0f}% of the stocks that ever traded here are delisted — they stay in the data",
            pad=26,
        )
        ax.text(
            0, 1.03,
            "Korean stocks (KOSPI + KOSDAQ) with daily bars in each year, by listing status today",
            transform=ax.transAxes, color=p["ink2"], fontsize=11, ha="left", va="bottom",
        )

        # 직접 라벨 하나: 전체 중 상폐 비율
        xi, yi = x[i_peak], listed[i_peak] + delisted[i_peak]
        ax.annotate(
            f"{total['delisted']:,} of {n_all:,} stocks ({pct_all:.1f}%) are delisted today.\n"
            f"A survivor-only dataset would drop up to {100 * share[i_peak]:.0f}% of a year's universe ({xi}).",
            xy=(xi, yi), xytext=(x[0] + 1.6, listed.max() * 1.04),
            ha="left", va="center", fontsize=11, color=p["ink"],
            arrowprops=dict(arrowstyle="-", color=p["ink3"], lw=1, shrinkB=4,
                            connectionstyle="arc3,rad=0.0"),
        )

        ax.set_xticks(x)
        ax.set_xticklabels([str(v) for v in x], rotation=0)
        ax.set_xlim(x[0] - 0.7, x[-1] + 0.7)
        ax.set_ylim(0, (listed + delisted).max() * 1.22)
        ax.yaxis.set_major_formatter(lambda v, _: f"{int(v):,}")
        ax.set_ylabel("Stocks with daily bars")
        ax.grid(axis="x", visible=False)
        ax.legend(loc="upper left", ncol=2, bbox_to_anchor=(0.0, 1.0))
        caption(
            fig,
            f"Source: kr_quant TimescaleDB — daily_bars_adj_v2 × stocks / delisted_stocks ∪ kind_delistings "
            f"(2007-11-22 to 2026-10-13; 2026 is year-to-date). Queried {ASOF}.",
            p,
        )

    return draw


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--from-csv", action="store_true", help=f"DB 대신 {CSV_PATH.relative_to(ROOT)} 에서 읽는다")
    args = ap.parse_args()
    rows = load_from_csv() if args.from_csv else load_from_db()
    paths = render_both(draw_factory(rows), out_dir=str(OUT_DIR), name="hero", figsize=(12, 5))
    for pth in paths:
        print(pth)


if __name__ == "__main__":
    main()
