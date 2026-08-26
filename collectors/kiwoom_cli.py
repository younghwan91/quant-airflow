"""전종목 스윕 콜렉터들의 공통 CLI 뼈대 — 인자, 세션, 유니버스, 배너.

``daily_bars`` / ``supply_demand`` / ``short_credit`` / ``listed_shares`` /
``combined`` 의 ``main()`` 은 앞부분 ~30줄이 글자 그대로 같았다: 같은 여섯 개
플래그(``--prod/--market/--limit/--db/--all-kinds/--rate``), 같은
``connect`` + ``make_api(..., max_retries=5)``, 같은 "시장 → 종목 목록 → 보통주
필터 → ``--limit`` 자르기 → ``upsert_stocks``", 같은 ``🔌``/``💾`` 배너.

다섯 벌이면 ``--rate`` 기본값이나 마스킹 규칙 하나 바꾸는 데 다섯 파일을 고쳐야
하고, 실제로 그 사이에 차이가 새어들어와 있었다(``listed_shares`` 만
``--all-kinds`` 가 없어 항상 보통주만 받는다). 여기 모아두면 그런 차이가 곧
"일부러 다르게 준 인자" 로 드러난다.

``sector_index`` 는 종목이 아니라 업종을 도므로 유니버스 부분을 공유하지 않는다.
"""

from __future__ import annotations

import argparse
from typing import Any

from .config import make_api, mask_dsn
from .storage import connect, default_db_path, upsert_stocks


def add_common_args(
    parser: argparse.ArgumentParser, *, all_kinds: bool = True
) -> argparse.ArgumentParser:
    """전종목 스윕 콜렉터가 공통으로 받는 플래그를 붙인다.

    Args:
        all_kinds: ``--all-kinds`` 를 노출할지. ``listed_shares`` 는 보통주만
            받으므로 끈다 — 플래그가 없으면 :func:`build_universe` 가 보통주
            필터를 항상 적용한다.
    """
    parser.add_argument("--prod", action="store_true", help="실서버 사용 (기본: 모의)")
    parser.add_argument("--market", choices=["kospi", "kosdaq", "all"], default="all")
    parser.add_argument("--limit", type=int, default=0, help="앞에서 N종목만 (테스트)")
    parser.add_argument("--db", default=str(default_db_path()))
    if all_kinds:
        parser.add_argument(
            "--all-kinds", action="store_true",
            help="ETF/ETN/리츠/우선주 등 모두 포함 (기본: 보통주만)",
        )
    parser.add_argument(
        "--rate", type=float, default=0.9,
        help="TR당 요청 속도(req/s). 긴 전수 수집의 429 방지를 위해 기본 0.9",
    )
    return parser


def open_session(args: argparse.Namespace) -> tuple[Any, Any]:
    """``(con, api)`` 를 연다.

    ``max_retries=5``: 장시간 단일-TR 반복이라 보수적으로 — 약간 느린 속도와
    넉넉한 재시도로 429 를 흡수한다.
    """
    con = connect(args.db)
    api = make_api(is_mock=not args.prod, rate_limit=args.rate, max_retries=5)
    return con, api


def build_universe(api: Any, con: Any, args: argparse.Namespace) -> list[dict]:
    """수집 대상 종목을 정하고 ``stocks`` 마스터에 기록한 뒤 돌려준다.

    ``--all-kinds`` 를 안 받는 콜렉터(플래그 자체가 없는 경우)는 항상 보통주만
    받는다 — ``getattr`` 기본값이 그 규약이다.
    """
    # 지연 import: ``supply_demand`` 는 이 모듈을 자기 main() 에서 쓰므로 최상단에서
    # 끌어오면 순환한다. 유니버스 함수의 정본은 그쪽이라 옮기지 않고 여기서 늦춘다.
    from .supply_demand import fetch_stock_list, is_common_stock  # noqa: PLC0415

    markets = ["kospi", "kosdaq"] if args.market == "all" else [args.market]
    stocks = fetch_stock_list(api, markets)
    if not getattr(args, "all_kinds", False):
        stocks = [s for s in stocks if is_common_stock(s)]
    if args.limit:
        stocks = stocks[: args.limit]
    upsert_stocks(con, stocks)
    return stocks


def print_banner(args: argparse.Namespace, stocks: list[dict], window: str = "") -> None:
    """``🔌 서버 | 시장 | 종목 수 | 창`` + ``💾 DSN`` 두 줄.

    DSN 은 반드시 :func:`.config.mask_dsn` 을 거친다 — 그대로 찍으면 비밀번호가
    Airflow 태스크 로그와 CI 스크롤백에 평문으로 남는다(실측).
    """
    server = "실서버" if args.prod else "모의"
    line = f"🔌 {server} | 시장={args.market} | 종목 {len(stocks)}개"
    if window:
        line += f" | {window}"
    print(line)
    print(f"💾 {mask_dsn(args.db)}\n")
