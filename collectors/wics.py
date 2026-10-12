"""WICS 섹터·산업군(중분류) 구성 이력 → wics_membership (migrations/020).

wiseindex ``GetIndexComponets`` 는 과거 날짜(``dt``)를 받고 **그날의** 구성으로 답한다 —
오늘 분류를 과거에 덮은 게 아니다(2026-10-12 실측: NC 036570 은 2019-01-02 G45 IT,
2026-10-08 G50. G5020 미디어와엔터테인먼트는 2019 에 아예 없다). KRX 업종분류는
data.krx.co.kr 가 로그인을 요구해 못 쓴다.

스냅샷은 매주 첫 거래일(``daily_bars`` 의 005930 거래일). 하루에 섹터 10개 + 중분류
:data:`INDUSTRIES` + WI26 :data:`WI26` 을 묻고, 섹터 구성원마다 둘을 붙인다. 둘 다 같이 두는
이유는 어느 하나도 전 종목을 덮지 못해서다(migrations/020 실측 — 2019 미디어는 중분류가 없고,
2026 은 WI26 에서 90종목이 빠진다). 둘 다 없는 구성원은 경고한다 — 새 코드가 생겼다는 뜻이다.
섹터는 반드시 섹터 지수 응답에서 받는다 — WI26 응답의 ``SEC_CD`` 는 지수의 섹터다.
그 거래일에 섹터 응답이 통째로 비면 예외다(빈 응답을 성공으로 넘기지 않는다, CLAUDE.md §5).
이미 있는 날짜는 건너뛰어 재실행이 이어 받는다.

CLI:
    python -m collectors.wics --db <DSN> [--since 2019-01-01] [--until YYYY-MM-DD]
"""

from __future__ import annotations

import argparse
import datetime as dt
import time
from collections.abc import Callable, Iterable
from typing import Any

from .storage import connect, fetchall, upsert_wics_membership

URL = "https://www.wiseindex.com/Index/GetIndexComponets"
UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128 Safari/537.36"
MIN_INTERVAL = 0.3
SOURCE = "wiseindex"

SECTORS = ("G10", "G15", "G20", "G25", "G30", "G35", "G40", "G45", "G50", "G55")
#: 2019-01-02·2026-10-08 에 G{섹터}{10..60} 을 다 찔러 응답이 있던 코드(2026-10-12).
INDUSTRIES = (
    "G1010", "G1510", "G2010", "G2020", "G2030", "G2510", "G2520", "G2530", "G2550", "G2560",
    "G3010", "G3020", "G3030", "G3510", "G3520", "G4010", "G4020", "G4030", "G4040", "G4050",
    "G4510", "G4520", "G4530", "G4535", "G4540", "G5010", "G5020", "G5510",
)
#: 같은 날 WI100..WI990 을 10 간격으로 찔러 응답이 있던 코드(2026-10-12).
WI26 = (
    "WI100", "WI110", "WI200", "WI210", "WI220", "WI230", "WI240", "WI250", "WI260", "WI300",
    "WI310", "WI320", "WI330", "WI340", "WI400", "WI410", "WI500", "WI510", "WI520", "WI600",
    "WI610", "WI620", "WI630", "WI640", "WI700", "WI800",
)


def _strip(name: str | None) -> str | None:
    return name.removeprefix("WICS ").removeprefix("WI26 ").strip() if name else name


def week_starts(days: Iterable[dt.date]) -> list[dt.date]:
    """거래일들 → ISO 주마다 첫 거래일."""
    first: dict[tuple[int, int], dt.date] = {}
    for d in sorted(days):
        first.setdefault(d.isocalendar()[:2], d)
    return sorted(first.values())


class Fetcher:
    def __init__(self, get: Callable[..., Any] | None = None, sleep: Callable[[float], None] = time.sleep,
                 clock: Callable[[], float] = time.monotonic) -> None:
        if get is None:
            import httpx  # noqa: PLC0415 — 테스트는 get 을 주입한다

            get = httpx.Client(headers={"User-Agent": UA}, timeout=30).get
        self._get, self._sleep, self._clock, self._last = get, sleep, clock, -1e9
        self.calls = 0

    def members(self, day: dt.date, sec_cd: str) -> list[dict]:
        wait = self._last + MIN_INTERVAL - self._clock()
        if wait > 0:
            self._sleep(wait)
        self._last = self._clock()
        self.calls += 1
        r = self._get(URL, params={"ceil_yn": 0, "dt": day.strftime("%Y%m%d"), "sec_cd": sec_cd})
        r.raise_for_status()
        return (r.json() or {}).get("list") or []


def snapshot(fetcher: Fetcher, day: dt.date) -> tuple[list[tuple], int]:
    """하루치 행과, 중분류·WI26 둘 다 못 붙인 구성원 수."""
    sector: dict[str, tuple[str, str | None]] = {}
    for s in SECTORS:
        for x in fetcher.members(day, s):
            sector[x["CMP_CD"]] = (x.get("SEC_CD") or s, _strip(x.get("SEC_NM_KOR")))
    if not sector:
        raise RuntimeError(f"{day}: WICS 섹터 응답이 전부 비었다 — 거래일인데 0건")
    groups: dict[str, dict[str, tuple[str, str | None]]] = {"ind": {}, "wi": {}}
    for kind, codes in (("ind", INDUSTRIES), ("wi", WI26)):
        for g in codes:
            for x in fetcher.members(day, g):
                groups[kind][x["CMP_CD"]] = (g, _strip(x.get("IDX_NM_KOR")))
    rows, unassigned = [], 0
    for code, (sec_cd, sec_nm) in sorted(sector.items()):
        ind_cd, ind_nm = groups["ind"].get(code, (None, None))
        wi_cd, wi_nm = groups["wi"].get(code, (None, None))
        if ind_cd is None and wi_cd is None:
            unassigned += 1
        rows.append((day.isoformat(), code, sec_cd, sec_nm, ind_cd, ind_nm, wi_cd, wi_nm, SOURCE))
    return rows, unassigned


def _as_date(v: Any) -> dt.date:
    return v if isinstance(v, dt.date) else dt.date.fromisoformat(str(v)[:10])


def collect(con: Any, fetcher: Fetcher, *, since: dt.date, until: dt.date) -> dict[str, int]:
    days = [_as_date(r[0]) for r in fetchall(
        con, "SELECT date FROM daily_bars WHERE code='005930' AND date >= ? AND date <= ?",
        (since.isoformat(), until.isoformat()))]
    targets = week_starts(days)
    have = {_as_date(r[0]) for r in fetchall(con, "SELECT DISTINCT date FROM wics_membership", ())}
    todo = [d for d in targets if d not in have]
    stats = {"weeks": len(targets), "already": len(targets) - len(todo), "written": 0, "rows": 0, "unassigned": 0}
    started = time.monotonic()
    for i, day in enumerate(todo, 1):
        rows, unassigned = snapshot(fetcher, day)
        if unassigned:
            print(f"⚠️ {day}: 중분류·WI26 둘 다 없는 종목 {unassigned}개 — 새 코드?", flush=True)
        stats["rows"] += upsert_wics_membership(con, rows)
        stats["unassigned"] += unassigned
        stats["written"] += 1
        if i % 10 == 0:
            print(f"{i}/{len(todo)} 주 · {day} {len(rows)}종목 · {time.monotonic() - started:.0f}s", flush=True)
    return stats


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--db", required=True)
    ap.add_argument("--since", default="2019-01-01")
    ap.add_argument("--until", default=dt.date.today().isoformat())
    args = ap.parse_args()
    con = connect(args.db)
    stats = collect(con, Fetcher(), since=dt.date.fromisoformat(args.since),
                    until=dt.date.fromisoformat(args.until))
    print(" | ".join(f"{k} {v}" for k, v in stats.items()), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
