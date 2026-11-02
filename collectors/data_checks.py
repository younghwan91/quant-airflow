"""시세 데이터 품질 검사 — 확정치(``daily_finalize``) 뒤에 돌고, 하나라도 깨지면 비0 종료.

2026-09-14 KRX 애프터마켓(16:00~20:00 연속매매, ETF·ETN 제외) 개장으로 키움 ka10081/ka10080 의
접미사 없는 KRX 코드 응답에 애프터마켓 체결이 섞였다. 그날부터 ``daily_bars``·``daily_bars_adjusted``·
``daily_bars_raw``·``supply_demand.close``·``acc_trde_qty`` 의 고가·저가·종가·거래량은 **정규장 값이
아니다**(시가만 정규장). 이걸 한 달 가까이 아무도 몰랐고, 알아챈 뒤에도 "오진" 이라는 틀린 판정이
docstring 에 남았다(``daily_finalize``). 원천 정의가 조용히 바뀌는 것을 다음엔 그날 아침에 잡으려고
세 검사를 코드로 둔다.

1. **세션 범위**(``check_session``) — 그날 ``minute_bars`` 봉 시각이 알려진 세션 안이어야 한다.
   실측(2025-08-01~2026-10-12, 289거래일):
     정규장·동시호가·장후 시간외  09:00~15:35 (15:30 종가 단일가, 15:32·15:35 시간외 종가 봉)
     애프터마켓(2026-09-14~)       16:00~19:59, 20:00~20:02 지연 체결 1~4행이 21거래일 중 12일
     개장 1시간 지연일              첫 봉 10:00 — 2026-01-02 신년 개장일(10:00~15:35, 마감은 그대로)
     개장·마감 1시간 지연일          2025-11-13 수능일(10:00~16:35, 15:21~15:29 에도 연속매매 봉 ~6,700행)
   08:00~08:59·15:36~15:59 봉은 0 이다. 애프터마켓 창은 지연 체결 여유를 두고 20:10 까지 본다.
   **창 밖 봉이 몇 행 있다고 실패하지 않는다** — 창 밖이 :data:`OUTSIDE_FAIL_ROWS` 행 이상이거나
   :data:`OUTSIDE_FAIL_CODES` 종목 이상일 때만 새 세션이 생긴 것으로 본다(NXT 프리마켓이 섞이면 08:00~08:50
   매분 ~1,000종목이라 수만 행이다). 1~2행짜리 떠돌이 봉으로 매일 아침 경보가 울리면 경보를 안 보게 된다.
   개장 지연(첫 봉 ≥10:00)과 마감 지연(15:21~15:29 봉 ≥100행 — 평일엔 0~1행)은 따로 판정한다
   — 신년 개장일은 개장만 늦다.

   **06:30 에 T 의 애프터마켓 봉은 일부만 있다.** scalp-it 분봉 수집(평일 15:45)이 T 의 16:00~ 봉을 다 못 받고
   다음 거래일 런이 채운다 — 2026-10-13 01:30 실측 10-12 애프터마켓 17,721행·1,121종목(평소 ~135,000행·
   ~2,280종목, ~13%). 그래서 T 만 보면 애프터마켓 쪽 새 세션(20:10 뒤 등)을 놓칠 수 있다(거짓 음성). 이를 덮으려고
   세션 검사는 **T 와 T-1 둘 다** 본다 — T-1 은 06:30 에 다 적재돼 있다(10-08 134,462행). 08:00~ 프리마켓처럼
   정규장 앞뒤의 새 세션은 T 에서 바로 잡힌다.
2. **기준가 항등식**(``check_base_price_identity``) — t 의 정규장 종가(분봉 15:30~15:32 마지막 봉, 마감 지연일 16:30~16:32)는
   t+1 의 ``supply_demand`` 에서 ``틱반올림(close / (1 + flu_rt/1e4))`` 로 복원된다. ``flu_rt`` 의 기준가는
   애프터마켓 이후에도 전일 **정규장** 종가이고, 바뀐 건 ``close`` 쪽이다(09-14 이후 99.98% 정확).
   불일치가 문턱을 넘으면 ``flu_rt`` 정의나 수집이 또 바뀐 것이다. 권리락 등 기준가 조정 종목이 남는 몫이다.
   복원은 :func:`base_close`(호가 표 단위부터 1원까지 격자 폴백 — 스팩·개편 전 코스닥) — DB 의 ``regular_close`` 뷰
   (migration 031 ``krx_base_close``)와 같은 계산이다. 이 검사가 터지면 그 뷰도 틀린 값을 내고 있다.
   **전 거래일만 본다** — ``minute_bars`` 과거분은 수정주가라(뒤에 일어난 분할·병합이 소급) 오래된 날짜로 돌리면
   그만큼 어긋난다(2025-08 12% → 2026-07 2.5% → 2026-09~ 0.07%, 001520 2025-08-01 분봉 1,310 = 수정 일봉, 원 종가 655).
3. **원주가 카나리아**(``check_raw_price_canary``) — ``daily_bars_raw`` 005930 2017-03-31 은 2,060,000
   (액면분할 전 원 종가)이어야 한다. 행이 없으면 통과(백필 범위 밖일 수 있다). 수정주가가 섞여 들어오면
   41,200 이 된다(``daily_bars`` 와 같은 값).

4. **조정주가 대 원주가**(``check_adjusted_vs_raw``) — T-1 까지 최근 :data:`ADJ_WINDOW_DAYS` 거래일의 종목-일마다
   ``daily_bars_adjusted`` 의 비 r_adj = adj(t)/adj(prev) 와 ``daily_bars_raw`` 원주가 비 r_raw 를 대조한다(prev = 그 종목의
   직전 행). ``corporate_actions`` 에 (prev, t] 의 ``upd_rt`` ≠ 0 이벤트가 없고 원주가 쪽이 이어져 있으면(:func:`raw_in_band`)
   두 비는 같아야 한다 — ``|r_adj/r_raw − 1|`` > :data:`ADJ_DEV_MAX` 인 칸이 이음매다. 건너뛰는 칸:
     원주가 행이 없다   폐지 종목 — ``daily_bars_raw`` 는 상장 종목만. 실패가 아니다
     이벤트가 있다      크기 대조는 swing-it 합격 검사 몫
     원주가가 끊겼다    t 의 등락률(``supply_demand.flu_rt``, KRX 기준가 대비)이 가격제한폭 ±30% 밖이거나(정리매매·재상장),
                        기준가가 전일 원 종가에서 :data:`ADJ_BASE_GAP_MAX`(전일이 2026-09-14~ 이면 애프터마켓 등락 몫을
                        두고 :data:`ADJ_BASE_GAP_MAX_AFTERMARKET`) 넘게 떨어졌다(이벤트 없는 기준가 재설정).
                        수급 행이 없으면 원주가 비가 [0.7, 1.3] 밖인지로 본다.
   **띠를 원주가 비가 아니라 등락률로 재는 이유** — ① 2026-09-14~ 전일 원 종가는 애프터마켓 포함 통합값이라 기준가(전일
   정규장 종가)와 다르다. 원주가 비로 재면 09-30~10-29 상하한가 25칸이 ±30.1~43.6% 로 띠 밖에 나가고, 그중
   ``price_adjust`` 가 지운 0035S0 2026-09-30(기준가 대비 +29.9%, 원주가 비 +43.6%, 조정주가 0%)을 놓친다(§3
   ``regular_close`` 함정). ② 원주가 비 띠 [0.7, 1/0.7] 로 재면 이력에서 24칸이 더 걸린다 — 15칸은 기준가가 전일 원 종가와
   20~100% 다르고(키움이 이벤트를 빠뜨린 재설정) r_adj 가 등락률과 같아 조정주가가 맞는 칸이고(예 298000 2026-06-15 원주가
   38,900 → 55,000 = +41%, 기준가 77,800, 조정주가 77,800 → 55,000 = flu_rt −29.3%), 9칸은 수급 행이 없고 원주가 비가
   +30~43% 라 가격제한폭 밖이다(판단 불가).
   **T 는 보지 않는다** — 06:30 의 T 원주가는 T 19:00 ``daily_bars_raw`` 스냅숏(애프터마켓 19:00 까지)이라 확정 일봉과
   다르다(2026-10-30 실측 2,588종목 중 1,813 다름, 99분위 4.9%·최대 11.1%). T-1 은 T 19:00 런이 다시 받아 확정 일봉과 같다
   (10-29 2,585종목 0 다름). 비교하는 두 종가는 2026-09-14~ 둘 다 통합 종가라 같은 정의끼리의 비다 — ``regular_close`` 와
   섞지 않는다(섞으면 정의 차이가 이음매로 보인다). ``trade_value``(백만원)는 쓰지 않는다.

   실측(2026-11-02 읽기 전용, 2016-09~2026-10-30 월별로 이 판정을 그대로): 대조 533만 칸 중 이음매 172칸·91일 —
   2019-02-27 34·2021-07-29 31·2024-01-08 19칸에 몰리고(``daily_bars`` 수집 이음매가 그대로 넘어옴, 예 258830 2021-07-29
   원주가 24,500 → 24,500 인데 조정주가 24,500 → 4,900) 나머지는 해마다 2~17칸(2025 7·2026 8) 흩어진다 — swing-it ``price_adjust`` 가 진짜
   ±30% 상하한가를 지운 칸(예 001380 2025-03-26 원주가 3,500 → 2,450 인데 조정주가 4,909 → 4,909)과 **지금도 새로 생기는**
   수집 이음매(117670 2026-10-06 조정주가 10,420 → 5,155 · 원주가 10,420 → 10,310 — 10-21 권리락 −50% 를 키움이 소급 수정했고
   16:00 수집의 15일 창이 10-06 부터만 다시 써서 그 앞이 옛 기준으로 남았다). 창 20거래일은 이 15일 경계를 덮는다.
   **지금 이 검사를 실패로 두면 거의 매일 빨갛다** — 20거래일 창으로 매일 돌렸다면 2025년 242런 중 96, 2026년(10-30 까지)
   202런 중 109, 2026-09-15 뒤로는 30런 전부가 빨갰다. 매일 빨간 경보는 안 보게 된다. 그래서 swing-it ``price_adjust`` 수정이
   배포되기 전에는 :data:`ADJUSTED_STRICT` = False 로 ``[WARN]`` 만 찍었다(표본 부족 — 재계산이 비었다 — 은 그래도 실패).
   2026-11-02 swing-it PR #12 배포·재계산 뒤 ``[WARN]`` 이 모두 사라져 True 로 뒤집었다(옛 표는
   ``legacy.daily_bars_adjusted_pre_seamfix_20261102``).
5. **조정주가 카나리아**(``check_adjusted_canaries``) — 위 실측의 알려진 이음매 세 칸 + 심판 없는 병합 한 칸(이력 전체를 매일 훑지 않고 대표 칸만).
   고쳐지면 r_adj 가 025620·258830 2021-07-29 |r| < 0.3(수집 이음매), 001380 2025-03-26 r ≈ −0.30(지워진 하한가), 052670 2026-02-09 r ≈ −0.80(거래정지 중 병합의 재개 첫날 — 원주가·심판이 없어 `adjusted_vs_raw` 가 못 본다)이 된다.
   ``ADJUSTED_STRICT`` = False 면 깨진 칸은 ``[WARN]``, 고쳐진 칸은 그렇다고 찍는다.

   **뒤집는 조건**: swing-it ``price_adjust`` 수정이 배포돼(``daily_bars_adjusted`` 는 매일 전량 재계산이라 다음 06:30 에 반영)
   ``python -m collectors.data_checks`` 의 ``[WARN]`` 이 모두 사라지면 :data:`ADJUSTED_STRICT` 를 True 로 바꾼다 — 그 뒤엔
   최근 창의 새 이음매와 카나리아가 되살아나는 것이 둘 다 실패다. 최근 창이 깨끗하지 않은데 카나리아만 고쳐졌으면 그 칸들을
   먼저 본다(수정이 한 부류만 고쳤다).

``minute_bars`` 는 scalp-it 이 쓰는 테이블이다(DDL 도 거기). 그날 분봉이 비면 두 검사가 표본 0 으로 실패한다
— 빈 표본을 통과로 넘기지 않는다(CLAUDE.md §5).

실패는 원인별로 구분해 찍는다(:data:`KIND_LABEL`) — 접속 실패(종료 코드 3, DB·환경변수 문제라 데이터와 무관) /
표본 부족(수집이 빠지거나 반쪽) / 항등식 불일치 / 세션 이탈 / 카나리아(수정주가 혼입) / 조정주가 이음매 / 조정주가 카나리아.
데이터 검사 실패는 종료 코드 1. ``[WARN]`` 은 통과로 센다(종료 0).

DSN 은 명령줄로 받지 않는다(``ps`` 에 비밀번호가 찍힌다) — 환경변수 ``KR_QUANT_DB`` 로 받는다. ``--db`` 는 sqlite 경로 전용.

판정은 순수 함수(DB 없이 테스트), 조회는 :func:`storage.fetchall`(sqlite·Postgres 양쪽, ``?`` 파라미터)에 둔다.
"""

from __future__ import annotations

import argparse
import datetime as dt
import math
import os
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from decimal import Decimal
from fractions import Fraction
from typing import Any
from zoneinfo import ZoneInfo

from . import storage
from .config import mask_secrets

#: 애프터마켓 개장일. 이 날 전에는 16:00 이후 봉이 있으면 안 된다.
AFTERMARKET_START = dt.date(2026, 9, 14)

#: (시작, 끝) 분 단위, 양끝 포함. 근거는 모듈 docstring 의 실측.
REGULAR_WINDOW = (dt.time(9, 0), dt.time(15, 35))
#: 애프터마켓 끝은 20:00 이지만 지연 체결 봉이 20:00~20:02 에 1~4행씩 찍힌다 — 여유를 둔다.
AFTERMARKET_WINDOW = (dt.time(16, 0), dt.time(20, 10))
#: 창 밖 봉이 이 행 수 이상이거나 이 종목 수 이상이면 새 세션 — 그 밑은 떠돌이 봉으로 통과(모듈 docstring).
OUTSIDE_FAIL_ROWS = 50
OUTSIDE_FAIL_CODES = 20
#: 첫 봉이 이 시각 이후면 개장 지연일(수능·신년) — 정규장 창 시작을 10:00 으로.
DELAYED_OPEN = dt.time(10, 0)
#: 평소 종가 단일가(15:20~15:30)라 체결이 없는 구간. 여기 봉이 많으면 마감 지연일(수능)이다.
LATE_CONTINUOUS = (dt.time(15, 21), dt.time(15, 29))
CLOSE_DELAY_MIN_ROWS = 100
DELAY_SHIFT = dt.timedelta(hours=1)

#: 정규장 종가 봉(15:30 종가 단일가, 드물게 15:32). 마감 지연일엔 1시간 늦춘다.
CLOSE_BAR_WINDOW = (dt.time(15, 30), dt.time(15, 32))

#: 호가가격단위 개편일(2023-01-25). 기준가를 정한 날(t) 기준으로 표를 고른다.
TICK_REFORM = dt.date(2023, 1, 25)
#: (이 가격 미만, 단위). 개편 뒤 — 코스피·코스닥 공통.
TICK_BANDS = ((2_000, 1), (5_000, 5), (20_000, 10), (50_000, 50), (200_000, 100), (500_000, 500))
#: 개편 전 코스피 표(가장 굵은 쪽 — 코스닥 5만원 이상 100원은 :func:`base_close` 격자 폴백이 잡는다).
TICK_BANDS_PRE_REFORM = ((1_000, 1), (5_000, 5), (10_000, 10), (50_000, 50), (100_000, 100), (500_000, 500))
TOP_TICK = 1_000
#: 기준가 복원 때 표 단위부터 시도하는 격자(굵은 것부터).
BASE_TICK_GRID = (1_000, 500, 100, 50, 10, 5, 1)
#: flu_rt(bp) 재현 허용 폭 — 키움 flu_rt 는 (close/기준가 − 1)×1e4 의 반올림이다.
FLU_RT_TOL_BP = Fraction(1, 2)

IDENTITY_MAX_MISMATCH = 0.01
#: 항등식 표본이 이보다 적으면 실패 — 분봉이 반쪽만 들어왔거나 조인이 깨진 것이다(평소 1,400 안팎).
IDENTITY_MIN_PAIRS = 300

CANARY = ("005930", dt.date(2017, 3, 31), 2_060_000)

#: 조정주가 대 원주가 대조 창(T-1 까지 거래일 수). 16:00 수집이 다시 쓰는 15일 창 경계를 덮는다(모듈 docstring 4).
ADJ_WINDOW_DAYS = 20
#: |r_adj/r_raw − 1| 문턱 — 실측 이음매는 −80%·+1,539% 처럼 크고 상하한가 지움은 ±23~43%.
ADJ_DEV_MAX = 0.2
#: 원주가 비가 이 밖이면(가격제한폭 ±30% 밖 — 거래정지 재개·빠진 이벤트의 기준가 재설정) 대조하지 않는다.
ADJ_RAW_BAND = (0.7, 1.3)
#: 기준가(전일 정규장 종가)가 전일 원 종가에서 이만큼 넘게 떨어지면 이벤트 없는 기준가 재설정으로 보고 대조하지 않는다.
#: 전일이 애프터마켓 전이면 두 값은 같아야 한다(재설정 실측 20~100%). 전일이 2026-09-14~ 이면 전일 원 종가가 애프터마켓
#: 끝 가격이라 정규장 종가와 그 세션 등락만큼 다르다 — 10월 실측 최대 35%(087010 10-07 정규장 130,600 · 통합 96,900).
#: 그 뒤로는 ×2·÷2 재설정(+100%·−50%)만 가려낸다.
ADJ_BASE_GAP_MAX = 0.15
ADJ_BASE_GAP_MAX_AFTERMARKET = 0.45
#: 대조 칸이 이보다 적으면 실패 — 조정주가 재계산이 비었거나 원주가가 안 들어왔다(평소 창 하나 ~5만 칸).
ADJ_MIN_PAIRS = 300

#: (code, t, 고쳐졌을 때 r_adj 하한, 상한) — 모듈 docstring 5. r_adj = adj(t)/adj(직전 행) − 1.
ADJUSTED_CANARIES = (
    ("025620", dt.date(2021, 7, 29), -0.3, 0.3),    # 원주가 1,780 → 1,810, 조정주가 1,780 → 29,179(이음매)
    ("258830", dt.date(2021, 7, 29), -0.3, 0.3),    # 원주가 24,500 → 24,500, 조정주가 24,500 → 4,900(이음매)
    ("001380", dt.date(2025, 3, 26), -0.31, -0.29),  # 원주가 3,500 → 2,450(하한가), 조정주가 4,909 → 4,909(지움)
    ("052670", dt.date(2026, 2, 9), -0.85, -0.75),  # 심판 없는 병합 1500:1(상장주식수 29,129,064 → 19,419) 재개 첫날, swing-it PR #14 전 ×300
)
#: True = swing-it price_adjust 수정(PR #12, 2026-11-02 배포) 뒤 — 새 이음매·되살아난 카나리아는 실패. 모듈 docstring 5.
ADJUSTED_STRICT = True

#: Postgres 세션의 쿼리 하나 시간 제한(:func:`main`).
STATEMENT_TIMEOUT = "120s"

#: DSN 을 받는 환경변수(다른 레포가 이미 쓰는 이름).
DSN_ENV = "KR_QUANT_DB"
_PG_PREFIXES = ("postgresql://", "postgres://")

#: 실패 원인 구분 — 메시지 머리와 끝 요약에 쓴다.
KIND_LABEL = {
    "connect": "접속 실패",
    "sample": "표본 부족",
    "identity": "항등식 불일치",
    "session": "세션 이탈",
    "canary": "카나리아",
    "seam": "조정주가 이음매",
    "adjusted_canary": "조정주가 카나리아",
}
EXIT_DATA_FAIL = 1
EXIT_CONNECT_FAIL = 3


@dataclass(frozen=True)
class CheckResult:
    name: str
    ok: bool
    detail: str
    #: 실패 원인(:data:`KIND_LABEL` 키). 통과면 그 검사의 종류.
    kind: str = ""
    #: 통과지만 알려진 결함이 남아 있다 — ``[WARN]`` 으로 찍고 종료 코드는 0.
    warn: bool = False


# ── 순수 함수 ──────────────────────────────────────────────────────────────


def _shift(t: dt.time, delta: dt.timedelta) -> dt.time:
    return (dt.datetime.combine(dt.date.min, t) + delta).time()


def day_profile(bar_counts: Mapping[dt.time, int]) -> tuple[bool, bool]:
    """(개장 지연, 마감 지연). 신년 개장일은 개장만, 수능일은 둘 다 1시간 늦다."""
    open_delayed = min(bar_counts) >= DELAYED_OPEN
    lo, hi = LATE_CONTINUOUS
    close_delayed = sum(n for t, n in bar_counts.items() if lo <= t <= hi) >= CLOSE_DELAY_MIN_ROWS
    return open_delayed, close_delayed


def session_windows(day: dt.date, bar_counts: Mapping[dt.time, int]) -> list[tuple[dt.time, dt.time]]:
    """그날 허용 세션 창 목록. 지연일이면 해당 끝만 1시간 늦춘다(마감 지연이면 애프터마켓 끝도)."""
    open_delayed, close_delayed = day_profile(bar_counts)
    zero = dt.timedelta(0)
    open_d = DELAY_SHIFT if open_delayed else zero
    close_d = DELAY_SHIFT if close_delayed else zero
    windows = [(_shift(REGULAR_WINDOW[0], open_d), _shift(REGULAR_WINDOW[1], close_d))]
    if day >= AFTERMARKET_START:
        windows.append((AFTERMARKET_WINDOW[0], _shift(AFTERMARKET_WINDOW[1], close_d)))
    return windows


def close_bar_window(bar_counts: Mapping[dt.time, int]) -> tuple[dt.time, dt.time]:
    """정규장 종가 봉을 찾을 창 — 마감 지연일(수능)만 16:30~16:32."""
    d = DELAY_SHIFT if bar_counts and day_profile(bar_counts)[1] else dt.timedelta(0)
    return _shift(CLOSE_BAR_WINDOW[0], d), _shift(CLOSE_BAR_WINDOW[1], d)


def outside_minutes(day: dt.date, bar_counts: Mapping[dt.time, int]) -> dict[dt.time, int]:
    """그날 허용 세션 창 밖 분 → 봉 수."""
    if not bar_counts:
        return {}
    windows = session_windows(day, bar_counts)
    return {t: n for t, n in bar_counts.items() if not any(a <= t <= b for a, b in windows)}


def check_session(
    day: dt.date,
    bar_counts: Mapping[dt.time, int],
    outside_codes: int | None = None,
    *,
    fail_rows: int = OUTSIDE_FAIL_ROWS,
    fail_codes: int = OUTSIDE_FAIL_CODES,
) -> CheckResult:
    """``bar_counts``: 그날 분봉 시각(분) → 봉 수. ``outside_codes``: 창 밖 봉의 서로 다른 종목 수
    (없으면 행 수로 — 종목 수 ≤ 행 수라 보수적이다). 창 밖이 ``fail_rows`` 행 이상이거나 ``fail_codes``
    종목 이상이면 실패, 그 밑은 떠돌이 봉으로 통과(상세에 남긴다)."""
    name = f"session {day}"
    if not bar_counts:
        return CheckResult(name, False, "minute_bars 0행 — 분봉 수집이 빠졌다(scalp-it)", "sample")
    windows = session_windows(day, bar_counts)
    outside = outside_minutes(day, bar_counts)
    total = sum(bar_counts.values())
    span = ", ".join(f"{a:%H:%M}~{b:%H:%M}" for a, b in windows)
    if not outside:
        return CheckResult(name, True, f"{total}행 모두 세션 안({span})", "session")
    rows = sum(outside.values())
    codes = rows if outside_codes is None else outside_codes
    sample = ", ".join(f"{t:%H:%M}×{n}" for t, n in sorted(outside.items())[:10])
    head = f"알려진 세션({span}) 밖 봉 {rows}/{total}행·{codes}종목: {sample}"
    if rows >= fail_rows or codes >= fail_codes:
        return CheckResult(
            name, False,
            head + f" (문턱 {fail_rows}행 또는 {fail_codes}종목) — 새 세션이 생겼다(원천 정의 변경)."
            " docs/schema.md 의 분봉 설명과 이 검사를 같이 고친다",
            "session",
        )
    return CheckResult(name, True, head + f" — 떠돌이 봉(문턱 {fail_rows}행·{fail_codes}종목 미만), 통과", "session")


def krx_tick(price: float | Fraction, day: dt.date | None = None) -> int:
    """``day`` 시점 그 가격대의 **가장 굵은** KRX 호가단위(``day`` 없으면 2023-01-25 개편 뒤 표).

    개편 뒤는 코스피·코스닥 공통, 그 전은 코스피 표다 — 개편 전 코스닥 5만원 이상(100원)과 2025-03~ 스팩(1원)은
    이 표보다 가늘고, :func:`base_close` 의 격자 폴백이 그 몫을 잡는다. SQL 판은 migration 031 ``krx_tick_size``.
    """
    bands = TICK_BANDS if day is None or day >= TICK_REFORM else TICK_BANDS_PRE_REFORM
    for bound, tick in bands:
        if price < bound:
            return tick
    return TOP_TICK


def _exact(x: float | int | Decimal | Fraction) -> Fraction:
    """DB 값(정수·numeric·real)을 SQL numeric 처럼 십진 그대로 — float 의 이진 꼬리를 끌고 오지 않는다."""
    if isinstance(x, (int, Decimal, Fraction)):
        return Fraction(x)
    return Fraction(repr(float(x)))


def _round_half_up(x: Fraction) -> int:
    """SQL ``round(numeric)`` 과 같은 반올림(양수에서 0.5 는 올림)."""
    return math.floor(x + Fraction(1, 2)) if x >= 0 else -math.floor(-x + Fraction(1, 2))


def tick_round(price: float | Fraction, day: dt.date | None = None) -> int:
    tick = krx_tick(price, day)
    return _round_half_up(_exact(price) / tick) * tick


def base_close(close: float | int | Decimal, flu_rt_bp: float | int | Decimal, day: dt.date | None = None) -> int | None:
    """t+1 의 ``supply_demand`` 한 행(close, flu_rt)에서 t(=``day``) 의 기준가 = 정규장 종가를 복원한다.

    원값 ``close / (1 + flu_rt/1e4)`` 를 그 가격대 표 단위부터 1원까지 더 가는 격자로 반올림해 보며 flu_rt 를
    ±0.5bp 안에서 재현하는 첫 값을 고른다(키움 flu_rt 는 반올림 — 2025-01~2026-09 99.92% 일치). 진짜 기준가가 굵은
    격자 위에 있으면 그 반올림이 곧 진짜 값이고, 아니면 굵은 후보는 2bp 넘게 어긋나 flu_rt 를 못 맞춘다 — 그래서
    시장 구분 없이 스팩·개편 전 코스닥도 맞는다. None: 1원 격자로도 못 맞추거나(기준가 재설정) 이웃 격자값도 맞아
    유일하지 않다(flu_rt ≈ −100% 처럼 bp 정밀도가 호가단위보다 굵을 때).
    SQL 판은 migration 031 ``krx_base_close`` — 같은 결과여야 한다.
    """
    if close is None or flu_rt_bp is None:
        return None
    c, f = _exact(close), _exact(flu_rt_bp)
    if c <= 0 or f <= -10_000:
        return None
    raw = c * 10_000 / (10_000 + f)
    std = krx_tick(raw, day)
    for g in BASE_TICK_GRID:
        if g > std:
            continue
        cand = _round_half_up(raw / g) * g
        if cand > 0 and _reproduces(c, f, cand):
            # 이웃 격자값도 맞추면 flu_rt 정밀도가 모자란 것(flu_rt ≈ −100% 등) — 고르지 않는다
            if _reproduces(c, f, cand + g) or (cand > g and _reproduces(c, f, cand - g)):
                return None
            return cand
    return None


def _reproduces(close: Fraction, flu_rt_bp: Fraction, base: int) -> bool:
    return abs((close / base - 1) * 10_000 - flu_rt_bp) <= FLU_RT_TOL_BP


def derive_regular_close(
    rows: Iterable[tuple[str, dt.date, float | None, float | None]],
    base_resets: Iterable[tuple[str, dt.date]] = (),
) -> dict[tuple[str, dt.date], tuple[int | None, str]]:
    """migration 031 ``regular_close`` 뷰의 2026-09-14~ 갈래를 파이썬으로 — (code, date) → (정규장 종가, src).

    ``rows``: ``supply_demand`` 키움 행 (code, date, close, flu_rt) — 거래일 달력도 이 날짜들로 만든다.
    ``base_resets``: ``corporate_actions`` 에서 upd_rt ≠ 0 인 (code, 새 기준 첫 거래일).
    src 는 identity·pending·no_next·gap·base_reset·unmatched(뜻은 031 머리말).
    """
    by_code: dict[str, list[tuple[dt.date, float | None, float | None]]] = {}
    for code, day, close, flu in rows:
        by_code.setdefault(code, []).append((day, close, flu))
    days = sorted({d for series in by_code.values() for d, _, _ in series})
    next_day = dict(zip(days, [*days[1:], None], strict=True))
    resets = set(base_resets)
    out: dict[tuple[str, dt.date], tuple[int | None, str]] = {}
    for code, series in by_code.items():
        series.sort(key=lambda r: r[0])
        for i, (day, _, _) in enumerate(series):
            nxt = series[i + 1] if i + 1 < len(series) else None
            if next_day[day] is None:
                out[code, day] = (None, "pending")
            elif nxt is None:
                out[code, day] = (None, "no_next")
            elif nxt[0] != next_day[day]:
                out[code, day] = (None, "gap")
            elif (code, nxt[0]) in resets:
                out[code, day] = (None, "base_reset")
            else:
                base = base_close(nxt[1], nxt[2], day)
                out[code, day] = (base, "identity" if base is not None else "unmatched")
    return out


def check_base_price_identity(
    prev_day: dt.date,
    day: dt.date,
    regular_close: Mapping[str, float],
    next_rows: Iterable[tuple[str, float | None, float | None]],
    *,
    max_mismatch: float = IDENTITY_MAX_MISMATCH,
    min_pairs: int = IDENTITY_MIN_PAIRS,
) -> CheckResult:
    """``regular_close``: prev_day 정규장 종가(분봉). ``next_rows``: day 의 (code, close, flu_rt)."""
    name = f"identity {prev_day}→{day}"
    pairs = 0
    bad: list[tuple[str, float, int | None]] = []
    for code, close, flu in next_rows:
        if code not in regular_close or close is None or flu is None or close <= 0:
            continue
        pairs += 1
        implied = base_close(close, flu, prev_day)
        if implied != regular_close[code]:
            bad.append((code, regular_close[code], implied))
    if pairs < min_pairs:
        return CheckResult(
            name, False,
            f"대조 표본 {pairs} < {min_pairs} — {prev_day} 분봉 종가 봉이나 {day} 수급이 반쪽이다",
            "sample",
        )
    rate = len(bad) / pairs
    sample = ", ".join(f"{c} 분봉 {int(r):,} vs 복원 {'없음' if i is None else f'{i:,}'}" for c, r, i in bad[:5])
    detail = f"불일치 {len(bad)}/{pairs} = {rate:.2%} (문턱 {max_mismatch:.0%})" + (f"; 예 {sample}" if bad else "")
    if rate > max_mismatch:
        return CheckResult(
            name, False,
            detail + " — supply_demand.flu_rt 의 기준가 정의나 close 수집이 바뀌었다",
            "identity",
        )
    return CheckResult(name, True, detail, "identity")


def check_raw_price_canary(close: float | None) -> CheckResult:
    code, day, expected = CANARY
    name = f"canary daily_bars_raw {code} {day}"
    if close is None:
        return CheckResult(name, True, "행 없음 — 통과(백필 범위 밖)", "canary")
    if int(close) != expected:
        return CheckResult(
            name, False,
            f"close {int(close):,} ≠ {expected:,} — 무수정 주가 테이블에 수정주가가 섞였다(upd_stkpc_tp)",
            "canary",
        )
    return CheckResult(name, True, f"close {expected:,}", "canary")


@dataclass(frozen=True)
class AdjCell:
    """한 종목의 이웃한 두 조정주가 행(prev → t)과 같은 날의 원주가(없으면 None).

    ``flu_rt``·``base``: t 의 키움 ``supply_demand`` 등락률(bp)과 그 기준가(= ``close/(1+flu_rt/1e4)``, 전일 정규장
    종가) — 가격제한폭과 기준가 재설정 판정에 쓴다(모듈 docstring 4). 수급 행이 없으면 None."""

    code: str
    prev: dt.date
    day: dt.date
    adj_prev: float
    adj: float
    raw_prev: float | None
    raw: float | None
    flu_rt: float | None = None
    base: float | None = None


def pair_adjusted_cells(
    adj_rows: Iterable[tuple[str, dt.date, float | None]],
    raw_close: Mapping[tuple[str, dt.date], float | None],
    start: dt.date,
    supply: Mapping[tuple[str, dt.date], tuple[float | None, float | None]] | None = None,
) -> list[AdjCell]:
    """``adj_rows``(code, date, close)에서 종목마다 이웃한 두 행을 짝짓는다 — t ≥ ``start`` 인 칸만.

    prev 는 그 종목의 직전 조정주가 행이다(거래일 달력의 전날이 아니다 — 거래정지로 빈 날을 건너뛴다).
    조정주가가 NULL·0 이하인 행은 짝에서 뺀다. ``supply``: (code, date) → 키움 수급 (close, flu_rt)."""
    supply = supply or {}
    by_code: dict[str, list[tuple[dt.date, float]]] = {}
    for code, day, close in adj_rows:
        if close is not None and close > 0:
            by_code.setdefault(code, []).append((day, float(close)))
    cells: list[AdjCell] = []
    for code, series in by_code.items():
        series.sort()
        for (p, ap), (d, a) in zip(series, series[1:], strict=False):
            if d < start:
                continue
            rp, r = raw_close.get((code, p)), raw_close.get((code, d))
            sd_close, flu = supply.get((code, d), (None, None))
            base = None
            if sd_close is not None and flu is not None and sd_close > 0 and flu > -10_000:
                base = float(sd_close) / (1 + float(flu) / 1e4)
            cells.append(AdjCell(code, p, d, ap, a,
                                 None if rp is None else float(rp), None if r is None else float(r),
                                 None if base is None else float(flu), base))
    return cells


def raw_in_band(c: AdjCell, band: tuple[float, float] = ADJ_RAW_BAND) -> bool:
    """원주가 쪽이 이어져 있고 가격제한폭 안인가 — 아니면 대조하지 않는다(모듈 docstring 4).

    수급 등락률이 있으면 KRX 정의 그대로 본다: t 종가/기준가(``1 + flu_rt/1e4``)가 ``band`` 안이고, 기준가가 전일 원
    종가에서 :data:`ADJ_BASE_GAP_MAX`(전일이 2026-09-14~ 이면 :data:`ADJ_BASE_GAP_MAX_AFTERMARKET`) 넘게 떨어져 있지
    않다(넘으면 이벤트 없는 기준가 재설정 — 298000 2026-06-15 ×2). 수급 행이 없으면 원주가 비로 잰다."""
    lo, hi = band
    if c.flu_rt is not None and c.base is not None:
        gap = ADJ_BASE_GAP_MAX if c.prev < AFTERMARKET_START else ADJ_BASE_GAP_MAX_AFTERMARKET
        return lo <= 1 + c.flu_rt / 1e4 <= hi and abs(c.base / c.raw_prev - 1) <= gap
    return lo <= c.raw / c.raw_prev <= hi


def _has_event(events: Mapping[str, list[dt.date]], code: str, prev: dt.date, day: dt.date) -> bool:
    return any(prev < e <= day for e in events.get(code, ()))


def check_adjusted_vs_raw(
    first: dt.date,
    last: dt.date,
    cells: Iterable[AdjCell],
    base_resets: Iterable[tuple[str, dt.date]] = (),
    *,
    dev_max: float = ADJ_DEV_MAX,
    raw_band: tuple[float, float] = ADJ_RAW_BAND,
    min_pairs: int = ADJ_MIN_PAIRS,
    strict: bool | None = None,
) -> CheckResult:
    """``cells``: :func:`pair_adjusted_cells`. ``base_resets``: ``corporate_actions`` upd_rt ≠ 0 의 (code, date).

    이벤트도 없고 원주가 비가 ``raw_band`` 안인데 |r_adj/r_raw − 1| > ``dev_max`` 인 칸이 하나라도 있으면 이음매 —
    ``strict``(기본 :data:`ADJUSTED_STRICT`)면 실패, 아니면 ``warn`` 통과(모듈 docstring 4). 원주가가 없는 칸(폐지 종목)은
    건너뜀이다. 대조 칸이 ``min_pairs`` 보다 적으면 ``strict`` 와 무관하게 실패."""
    strict = ADJUSTED_STRICT if strict is None else strict
    name = f"adjusted_vs_raw {first}~{last}"
    events: dict[str, list[dt.date]] = {}
    for code, day in base_resets:
        events.setdefault(code, []).append(day)
    pairs = no_raw = with_event = out_band = 0
    bad: list[tuple[AdjCell, float, float]] = []
    for c in cells:
        if c.raw_prev is None or c.raw is None or c.raw_prev <= 0 or c.raw <= 0:
            no_raw += 1
            continue
        if _has_event(events, c.code, c.prev, c.day):
            with_event += 1
            continue
        if not raw_in_band(c, raw_band):
            out_band += 1
            continue
        r_raw = c.raw / c.raw_prev
        pairs += 1
        r_adj = c.adj / c.adj_prev
        if abs(r_adj / r_raw - 1) > dev_max:
            bad.append((c, r_adj, r_raw))
    skipped = f"건너뜀: 이벤트 {with_event}·가격제한폭 밖/기준가 재설정 {out_band}·원주가 없음(폐지) {no_raw}"
    if pairs < min_pairs:
        return CheckResult(
            name, False,
            f"대조 칸 {pairs} < {min_pairs}({skipped}) — daily_bars_adjusted 재계산이 비었거나 daily_bars_raw 가 안 들어왔다",
            "sample",
        )
    sample = ", ".join(
        f"{c.code} {c.day} 조정 {c.adj_prev:,.0f}→{c.adj:,.0f}({r_adj - 1:+.1%}) vs 원 {c.raw_prev:,.0f}→{c.raw:,.0f}({r_raw - 1:+.1%})"
        for c, r_adj, r_raw in bad[:5]
    )
    detail = f"이음매 {len(bad)}/{pairs}칸(문턱 |r_adj/r_raw−1| > {dev_max:.0%}; {skipped})"
    if bad:
        detail += (f"; 예 {sample} — 원주가로 설명 안 되는 조정주가 점프(daily_bars 수집 이음매이거나 swing-it"
                   " price_adjust 가 상하한가를 지웠다)")
        if strict:
            return CheckResult(name, False, detail + ". collectors/data_checks.py 모듈 docstring 4·5", "seam")
        return CheckResult(name, True, detail + " — swing-it price_adjust 수정 배포 전이라 경고만(ADJUSTED_STRICT)",
                           "seam", warn=True)
    return CheckResult(name, True, detail, "seam")


def check_adjusted_canaries(
    observed: Mapping[tuple[str, dt.date], float | None],
    *,
    strict: bool | None = None,
) -> list[CheckResult]:
    """``observed``: (code, t) → r_adj(None = 행 없음). :data:`ADJUSTED_CANARIES` 마다 한 결과.

    ``strict``(기본 :data:`ADJUSTED_STRICT`)가 False 면(swing-it 수정 배포 전) 깨진 칸은 ``warn`` 통과, True 면 실패
    (모듈 docstring 5)."""
    strict = ADJUSTED_STRICT if strict is None else strict
    out: list[CheckResult] = []
    for code, day, lo, hi in ADJUSTED_CANARIES:
        name = f"canary daily_bars_adjusted {code} {day}"
        r = observed.get((code, day))
        if r is None:
            out.append(CheckResult(name, True, "행 없음 — 통과(조정주가 범위 밖)", "adjusted_canary"))
        elif lo <= r <= hi:
            note = "" if strict else " — 고쳐졌다(최근 창 [WARN] 도 사라지면 ADJUSTED_STRICT 를 True 로)"
            out.append(CheckResult(name, True, f"r_adj {r:+.1%} ∈ [{lo:+.0%}, {hi:+.0%}]{note}", "adjusted_canary"))
        else:
            head = f"r_adj {r:+.1%} ∉ [{lo:+.0%}, {hi:+.0%}] — 조정주가 이음매(모듈 docstring 4)"
            if strict:
                out.append(CheckResult(name, False, head + " 가 되살아났다", "adjusted_canary"))
            else:
                out.append(CheckResult(name, True, head + " 그대로 — swing-it price_adjust 수정 배포 전이라 경고만",
                                       "adjusted_canary", warn=True))
    return out


# ── 조회(sqlite·Postgres 공용) ──────────────────────────────────────────────


def _as_date(v: Any) -> dt.date:
    if isinstance(v, dt.datetime):
        return v.date()
    if isinstance(v, dt.date):
        return v
    return dt.date.fromisoformat(str(v)[:10])


def _as_datetime(v: Any) -> dt.datetime:
    return v if isinstance(v, dt.datetime) else dt.datetime.fromisoformat(str(v))


def _ts(day: dt.date, t: dt.time) -> str:
    return f"{day.isoformat()} {t:%H:%M:%S}"


def trading_days(con: Any, before: dt.date, n: int) -> list[dt.date]:
    """``before`` 전의 최근 거래일 n 개(오름차순). 거래일 달력은 ``supply_demand`` 의 날짜다."""
    out: list[dt.date] = []
    cursor = before
    for _ in range(n):
        row = storage.fetchone(con, "SELECT MAX(date) FROM supply_demand WHERE date < ?", (cursor.isoformat(),))
        if not row or row[0] is None:
            break
        cursor = _as_date(row[0])
        out.append(cursor)
    return sorted(out)


def fetch_bar_counts(con: Any, day: dt.date) -> dict[dt.time, int]:
    rows = storage.fetchall(
        con,
        "SELECT ts, COUNT(*) FROM minute_bars WHERE ts >= ? AND ts < ? GROUP BY ts",
        (day.isoformat(), (day + dt.timedelta(days=1)).isoformat()),
    )
    counts: dict[dt.time, int] = {}
    for ts, n in rows:
        t = _as_datetime(ts).time().replace(second=0, microsecond=0)
        counts[t] = counts.get(t, 0) + int(n)
    return counts


def fetch_outside_codes(con: Any, day: dt.date, minutes: Iterable[dt.time]) -> int:
    """창 밖 분(``minutes``)에 봉이 있는 서로 다른 종목 수."""
    stamps = [_ts(day, t) for t in sorted(minutes)]
    if not stamps:
        return 0
    marks = ",".join("?" * len(stamps))
    row = storage.fetchone(con, f"SELECT COUNT(DISTINCT code) FROM minute_bars WHERE ts IN ({marks})", tuple(stamps))
    return int(row[0]) if row and row[0] is not None else 0


def session_result(con: Any, day: dt.date) -> CheckResult:
    counts = fetch_bar_counts(con, day)
    outside = outside_minutes(day, counts)
    return check_session(day, counts, fetch_outside_codes(con, day, outside) if outside else 0)


def fetch_regular_close(con: Any, day: dt.date, window: tuple[dt.time, dt.time]) -> dict[str, float]:
    """day 의 종목별 정규장 종가 = ``window``(:func:`close_bar_window`) 안 마지막 분봉 종가."""
    lo, hi = window
    rows = storage.fetchall(
        con,
        "SELECT m.code, m.close FROM minute_bars m"
        " JOIN (SELECT code, MAX(ts) AS mts FROM minute_bars WHERE ts >= ? AND ts <= ? GROUP BY code) x"
        "   ON m.code = x.code AND m.ts = x.mts",
        (_ts(day, lo), _ts(day, hi)),
    )
    return {code: float(close) for code, close in rows if close is not None}


def fetch_supply_rows(con: Any, day: dt.date) -> list[tuple[str, float | None, float | None]]:
    return storage.fetchall(
        con,
        "SELECT code, close, flu_rt FROM supply_demand WHERE date = ? AND source = 'kiwoom'",
        (day.isoformat(),),
    )


def fetch_canary_close(con: Any) -> float | None:
    code, day, _ = CANARY
    row = storage.fetchone(con, "SELECT close FROM daily_bars_raw WHERE code = ? AND date = ?", (code, day.isoformat()))
    return None if not row else row[0]


def adjusted_vs_raw_result(con: Any, days: list[dt.date]) -> CheckResult:
    """``days``: 오름차순 거래일 — 첫날은 직전 행 몫으로만 읽고 대조는 둘째 날부터 끝까지."""
    if len(days) < 2:
        return CheckResult("adjusted_vs_raw", False, f"대조할 거래일 부족({len(days)}일)", "sample")
    lo, first, last = days[0].isoformat(), days[1], days[-1]
    adj = storage.fetchall(
        con, "SELECT code, date, close FROM daily_bars_adjusted WHERE date >= ? AND date <= ?", (lo, last.isoformat()),
    )
    raw = storage.fetchall(
        con, "SELECT code, date, close FROM daily_bars_raw WHERE date >= ? AND date <= ?", (lo, last.isoformat()),
    )
    resets = storage.fetchall(
        con, "SELECT code, date FROM corporate_actions WHERE date > ? AND date <= ? AND upd_rt <> 0",
        (lo, last.isoformat()),
    )
    supply = storage.fetchall(
        con, "SELECT code, date, close, flu_rt FROM supply_demand WHERE date >= ? AND date <= ? AND source = 'kiwoom'",
        (first.isoformat(), last.isoformat()),
    )
    cells = pair_adjusted_cells(
        [(code, _as_date(d), c) for code, d, c in adj],
        {(code, _as_date(d)): c for code, d, c in raw},
        first,
        {(code, _as_date(d)): (c, f) for code, d, c, f in supply},
    )
    return check_adjusted_vs_raw(first, last, cells, [(code, _as_date(d)) for code, d in resets])


def fetch_adjusted_canaries(con: Any) -> dict[tuple[str, dt.date], float | None]:
    """카나리아 칸의 r_adj — 그 종목의 t 와 직전 조정주가 행(30일 안)으로."""
    out: dict[tuple[str, dt.date], float | None] = {}
    for code, day, _, _ in ADJUSTED_CANARIES:
        rows = storage.fetchall(
            con,
            "SELECT date, close FROM daily_bars_adjusted WHERE code = ? AND date >= ? AND date <= ? ORDER BY date DESC LIMIT 2",
            (code, (day - dt.timedelta(days=30)).isoformat(), day.isoformat()),
        )
        ok = len(rows) == 2 and _as_date(rows[0][0]) == day and rows[0][1] and rows[1][1]
        out[code, day] = float(rows[0][1]) / float(rows[1][1]) - 1 if ok else None
    return out


def run_checks(con: Any, asof: dt.date) -> list[CheckResult]:
    """``asof`` 전 마지막 거래일 T 를 본다: T-1·T 의 세션, (T-1 → T) 항등식, 카나리아, T-1 까지
    :data:`ADJ_WINDOW_DAYS` 거래일의 조정주가 대 원주가, 조정주가 카나리아.

    T-1 세션도 보는 이유: 06:30 에 T 의 애프터마켓 봉은 ~13% 만 적재돼 있다(모듈 docstring) — T-1 은 다 있다.
    조정주가 대조가 T 를 빼는 이유: T 의 원주가는 19:00 스냅숏이다(모듈 docstring 4).
    """
    days = trading_days(con, asof, ADJ_WINDOW_DAYS + 2)
    if len(days) < 2:
        return [CheckResult("trading days", False, f"{asof} 전 supply_demand 거래일이 2개 미만", "sample")]
    prev_day, day = days[-2:]
    return [
        session_result(con, prev_day),
        session_result(con, day),
        check_base_price_identity(
            prev_day, day,
            fetch_regular_close(con, prev_day, close_bar_window(fetch_bar_counts(con, prev_day))),
            fetch_supply_rows(con, day),
        ),
        check_raw_price_canary(fetch_canary_close(con)),
        adjusted_vs_raw_result(con, days[:-1]),
        *check_adjusted_canaries(fetch_adjusted_canaries(con)),
    ]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--db", help=f"sqlite 경로(테스트용). 없으면 환경변수 {DSN_ENV} 의 DSN — DSN 은 명령줄로 받지 않는다(ps 노출)")
    ap.add_argument("--asof", type=dt.date.fromisoformat, default=None,
                    help="이 날짜 전 마지막 거래일을 본다(기본: 오늘 KST)")
    ap.add_argument("--days", type=int, default=1,
                    help="--asof 부터 거꾸로 거래일 N 개를 차례로 본다(이력 점검용, 기본 1)")
    args = ap.parse_args(argv)
    if args.db and args.db.startswith(_PG_PREFIXES):
        ap.error(f"--db 에 DSN 을 넘기지 않는다(ps 에 비밀번호가 찍힌다) — 환경변수 {DSN_ENV} 로 넘긴다")
    target = args.db or os.environ.get(DSN_ENV)
    if not target:
        print(f"[FAIL:{KIND_LABEL['connect']}] --db 도 환경변수 {DSN_ENV} 도 없다", flush=True)
        return EXIT_CONNECT_FAIL

    # 컨테이너 시계는 UTC 일 수 있다 — 06:30 KST 는 UTC 로 전날이라 하루 묵은 날을 보게 된다.
    asof = args.asof or dt.datetime.now(ZoneInfo("Asia/Seoul")).date()
    try:
        con = storage.connect(target)
        if storage._is_pg(con):
            con.set_session(readonly=True)
            # 태스크 시간 제한(10분)이 프로세스를 죽여도 서버 쪽 쿼리는 남는다 — 죽은 클라이언트의 29시간 쿼리가
            # 압축 정책 뒤로 전 DAG·백업을 막은 일(2026-10-14)이 있다. 평소 쿼리 하나 1초 안팎.
            storage.fetchall(con, "SELECT set_config('statement_timeout', ?, false)", (STATEMENT_TIMEOUT,))
        targets = [asof]
        if args.days > 1:
            targets = [d + dt.timedelta(days=1) for d in trading_days(con, asof, args.days)]
        results: list[CheckResult] = []
        seen: set[str] = set()
        for t in targets:
            # 카나리아는 날짜와 무관, 세션은 이웃 날짜끼리 겹친다 — 같은 이름은 한 번만
            for r in run_checks(con, t):
                if r.name not in seen:
                    seen.add(r.name)
                    results.append(r)
    except Exception as e:  # noqa: BLE001 — 접속·조회 실패를 데이터 실패와 구분해 찍는다
        # 예외 문자열에 DSN 이 들어갈 수 있다 — 가리고 첫 줄만
        msg = mask_secrets(str(e)).strip().splitlines()[:1]
        print(f"[FAIL:{KIND_LABEL['connect']}] DB 접속·조회 실패: {type(e).__name__}: {''.join(msg)}", flush=True)
        return EXIT_CONNECT_FAIL
    for r in results:
        tag = ("WARN" if r.warn else "OK") if r.ok else f"FAIL:{KIND_LABEL.get(r.kind, r.kind)}"
        print(f"[{tag}] {r.name}: {r.detail}", flush=True)
    failed = [r for r in results if not r.ok]
    if failed:
        by_kind: dict[str, int] = {}
        for r in failed:
            label = KIND_LABEL.get(r.kind, r.kind)
            by_kind[label] = by_kind.get(label, 0) + 1
        kinds = ", ".join(f"{k} {n}" for k, n in by_kind.items())
        print(f"데이터 품질 검사 실패 {len(failed)}/{len(results)} — {kinds}", flush=True)
        return EXIT_DATA_FAIL
    warned = sum(r.warn for r in results)
    print(f"데이터 품질 검사 통과 {len(results)}건" + (f"(경고 {warned})" if warned else ""), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
