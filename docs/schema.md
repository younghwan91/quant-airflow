# 데이터 스키마 · TimescaleDB 설계 · 마이그레이션

[← README](../README.md)

전체 DDL 은 [`sql/init_timescale.sql`](../sql/init_timescale.sql) 에 있다. 시계열
테이블은 TimescaleDB hypertable(PK `(code, date)`)이고, 그 외는 일반 테이블이다.

## 시세·수급 (hypertable)

| 테이블 | 내용 |
|---|---|
| `daily_bars` | 일봉 OHLCV + 거래대금. `source`='kiwoom'(상장 종목) / 'naver'(폐지 종목 백필 — 거래대금은 close×volume 근사). **`trade_value` 는 백만원 단위다**(두 source 모두, `minute_bars.trade_value` 는 원) — 원으로 읽어 문턱이 1e6 배 틀어진 사고가 있다(daytrade-it volume_entry, 2026-10-09) **원 일봉이 아니다** — 키움 수정주가(upd_stkpc_tp=1)로 받아 수집 시점보다 앞선 병합·분할만큼 과거가 소급 조정돼 있다(005930 2017-03-31 41,200 vs 실제 2,060,000). 그날 주식수와 곱하는 시총은 `daily_bars_raw` 로. **2026-09-14~ 고가·저가·종가·거래량은 KRX 애프터마켓 포함 통합값, 시가만 정규장**(아래 [애프터마켓](#2026-09-14-krx-애프터마켓--일봉수급은-정규장-값이-아니다)) |
| `daily_bars_raw` | **무수정 주가** 일봉(upd_stkpc_tp=0, 026) — 상장 종목 + ETF(278530·069500·229200), 2016-09-09~. 시총 = `close` × 그날 `shares_outstanding_history`. 폐지 종목은 키움이 안 줘서 없다(그쪽 원 종가는 `supply_demand.close`). `daily_bars_raw` DAG(평일 19:00)가 최근 5일을 다시 받는다. **2026-09-14~ 고가·저가·종가·거래량은 KRX 애프터마켓 포함 통합값, 시가만 정규장**(아래 [애프터마켓](#2026-09-14-krx-애프터마켓--일봉수급은-정규장-값이-아니다)). 005930 2017-03-31 = 2,060,000 이 카나리아(`collectors/data_checks.py`) |
| `corporate_actions` | 키움 ka10081 **수정주가 이벤트**(027) — 이벤트가 찍힌 날만, `upd_stkpc_tp`(비트 합: 1 유상·2 무상·4 배당락·8 분할·16 병합·32 합병·64 감자·256 권리락 — 8 만 실측 확인)·`upd_rt`(%) 원천 그대로. 무상증자 비율은 권리락일의 256 행에 있고, 3~4주 뒤 2(bonus) 행은 신주 상장일 표시라 `upd_rt`=0 이다(725건 중 721, 10-13 확인). `date` 는 새 기준 첫 거래일, 그 이전 가격 × (1 + upd_rt/100). 005930 2018-05-04 = 8, −98.00. 일봉 백필·`daily_bars_raw` DAG 가 채운다(16:00 수집은 안 넣는다), 상장 종목만 |
| `daily_bars_delisted_naver` | **폐지 종목** 일봉 2006~폐지일(028, 네이버 siseJson). **수정가격**(`source='naver_adj'`) — 폐지 종목은 수정 이벤트가 없어 원 가격으로 못 돌린다(시총에 주의). `trade_value` 는 close×volume 근사(백만원). 2016-09 이후는 `daily_bars` 의 naver 행과 겹친다. `daily_bars` 에 넣지 않은 이유: 2016 이전이 폐지 종목만인 유니버스가 된다 |
| `kind_delistings` | KIND 상장폐지 현황 원문(028) — 코드·폐지일·사유, 1991~. 2006~2008 이 연 16~35건뿐이라 완전하지 않다 — 폐지 유니버스는 이것 ∪ `delisted_stocks` 로 |
| `naver_investor_trend` | 네이버 m.stock 일별 **외국인 보유율(%)·외국인/기관/개인 순매수(주)**·원 종가·거래량(032, `naver_mstock trend`) — 2008-01~, 폐지 종목 포함(백필 전용, 매일 수집 아님). 순매수는 네이버 정의(합 0 맞춤)라 `supply_demand` 키움 값과 다르다. 옛 구간 개인은 NULL |
| `naver_research_reports` | 네이버 증권사 리포트 1건 1행 — 날짜·증권사·제목·의견·목표가(032, `naver_mstock research`). `write_date` 는 날짜만(시각 없음 → as-of 는 다음 거래일). `price_at_write` = 작성일 **전 거래일 종가**(2020~). `prev_goal_price` 는 **항상 NULL** — 원천 `prevGoalPrice` 가 직전 목표가가 아니라 전 거래일 종가라 버린다(035); 직전 목표가는 같은 (code, broker) 의 이전 날짜 리포트에서. `broker` 는 네이버 라벨이라 게시 당시 이름과 지금 이름이 섞여 있다(현대차증권·DB금융투자·신한투자증권·하나증권은 옛 시기도 지금 이름). 대형사(삼성 2023-06~·NH·KB) 리포트는 거의 없다. `research_id` 는 네이버 게시 순번(날짜와 0.4% 어긋남, 같은 날 안의 순서는 발간 시각이 아니다). 매일 05:40 `daily_naver_research` 가 최근 10일 증분. EPS 컨센서스 이력은 무료 원천이 없어 없다 |
| `program_trading` | 키움 ka90013 종목별 일별 **프로그램 매매 합계**(033) — 매수·매도·순매수 금액(백만원)·수량(주). 차익·비차익 분리는 TR 에 없다. 2016-09~ 백필(`flow_history --what program`), **상장 생존 종목만**(폐지 코드는 키움이 빈 응답) |
| `daily_bars_adj_v2` | **원주가 × 이벤트 누적계수** 수정 일봉(029·030, `collectors/adjusted_v2.py`) — `daily_bars_raw` × `corporate_actions`(`upd_rt`≠0 전부 + 키움이 빠뜨린 날은 `daily_bars`÷원주가 비율 계단으로 추정 159건 — 2016-09 이전은 `daily_bars_listed_naver` 기준), 2026-09-14~ OHLCV 는 분봉 09:00~15:32(KRX 정규장 — 일봉 종가는 KRX 애프터마켓 20:00 까지의 통합값), 폐지 종목은 `daily_bars_delisted_naver` 수정가격 그대로. `raw_close` = 그날 KRX 원 종가(시총용, 폐지는 NULL), `factor`, `close_src`(kiwoom_raw·minute_krx·halted(09-14~ 거래정지일, 일봉 그대로)·naver_adj·no_krx_source·minute_basis_mismatch·pre_listing(KRX 상장 전 K-OTC·프리보드 시세, 8종목) — 뒤 셋은 OHLCV NULL). 합격 검사 4종(swing-it 87955ba) 결과는 030 머리말. 2007-11-22~(038 — 2016 이전은 네이버 수정주가 기준으로 빠진 이벤트를 추정(037), 거래정지 뒤 재개일 점프 ~50·폐지 정리매매 밖 81 은 남는다 → 정지 뒤 첫날 수익률은 버린다; 옛 2016-09~ 판은 `daily_bars_adj_v2_2016` 보관), 아직 매일 갱신하지 않는다(재계산은 `python -m collectors.adjusted_v2`) |
| `daily_bars_listed_naver` | 상장 종목 네이버 **수정**주가 2007-11~2016-09(037, `collectors/listed_naver_ref.py`) — `adjusted_v2.infer_events` 가 `daily_bars`(키움 수정주가, 2016-09~) 없는 구간에서 빠진 이벤트를 찾는 기준. 수익률·시총 원천으로 읽지 않는다 |
| `daily_bars_adjusted` | 액면분할 백조정 일봉. 평일 `daily_price_adjust` + 토요일 `weekly_price_adjust` 가 **전량 재계산**한다(back-adjust 는 종목별 전체 이력을 봐야 해서 증분이 불가능하다 — 실측 6분 23초 / 피크 RSS 5.2GB). `source` 는 `daily_bars` 에서 전파. `daily_bars` 를 그대로 조정하므로 **2026-09-14~ 고가·저가·종가·거래량은 KRX 애프터마켓 포함 통합값, 시가만 정규장**(아래 [애프터마켓](#2026-09-14-krx-애프터마켓--일봉수급은-정규장-값이-아니다)) |
| `supply_demand` | 투자자별 순매매 **수량(주)** — 금액이 아니다(`amt_qty_tp="2"`). `flu_rt` 는 **키움이 준 등락률 × 100(bp)**: 175 = +1.75%. 기준가는 애프터마켓 뒤에도 **전일 정규장 종가**다 — `close/(1+flu_rt/1e4)` 를 틱 반올림하면 전일 정규장 종가(분봉 15:30)와 같다 — `regular_close` 뷰가 이것이다(09-14 이후 99.97%, 005930 10-07: close 269,000 · flu_rt −110 → 272,000 = 10-06 15:30 봉, 통합 종가는 273,000). 2026-09-15 무렵부터 `flu_rt` 가 종가 대비 수익률과 어긋나는 것은 `close` 가 애프터마켓 포함 통합값으로 바뀌었기 때문이다(전에 여기 적었던 "기준가가 바뀌었다"는 오진). 그래서 `flu_rt` 를 수익률로 쓰지 않는다 — 수익률은 `daily_bars_adjusted` 에서. `close`·`acc_trde_qty` 는 **2026-09-14~ 고가·저가·종가·거래량은 KRX 애프터마켓 포함 통합값, 시가만 정규장**(아래 [애프터마켓](#2026-09-14-krx-애프터마켓--일봉수급은-정규장-값이-아니다)). `natn`(국가)은 실측상 늘 0. `source`='kiwoom'(전체) / 'naver'(폐지 부분 백필 — 기관·외국인만, 개인·기관세부는 NULL → **지표마다 유니버스가 달라진다**) |
| `short_selling` | 공매도 추이(수량·잔고·비율·평균가). 매일 수집은 2024-05~, 그 앞 2016-09~ 는 `flow_history --what short` 백필 — **백필 구간은 상장 생존 종목만**(키움이 폐지 코드에 빈 응답, 2026-10 기준 상장 종목) |
| `credit_balance` | 신용잔고(신규·상환·잔고·비율). 매일 수집은 2026-02~, 그 앞 2016-09~ 는 `flow_history --what credit` 백필(ka10013 `dt`=기준일) — **백필 구간은 상장 생존 종목만** |
| `sector_index` | 업종지수 OHLCV |
| `shares_outstanding_history` | 상장주식수 이력(point-in-time 시총 계산용). `source` 가 kiwoom(주간 스냅샷)/krx(중단)/dart(과거 백필)를 구분. 키움은 현재 스냅샷만 주므로 **2016~2025 구간은 DART `--listed` 백필로 채운다** |
| `consensus` | 네이버 애널리스트 컨센서스(목표가·투자의견·EPS) |
| `news_articles` | krx-news-client(pip)로 수집한 뉴스(현재 토스만). PK `(id, published_at)` — `id`=`make_article_id(source,url)`라 재크롤링해도 같은 행을 갱신한다. 백테스팅+실매매, 추후 LLM 매매판단용. 두 경로가 같은 id 로 들어온다 — `daily_news`(하이라이트 피드, 최신 수십 건)와 `daily_toss_company_news`(종목별 피드, 2023-03-16~). 후자는 **이미 있는 id 를 건너뛴다**(토스가 고친 기사는 createdAt 이 바뀌어 PK 로는 두 행이 된다 — 먼저 본 판을 남긴다). 종목별 피드 기사는 `content` 가 NULL(피드는 본문을 안 준다) |

### 2026-09-14 KRX 애프터마켓 — 일봉·수급은 정규장 값이 아니다

2026-09-14 KRX 애프터마켓(16:00~20:00 연속매매, ETF·ETN 제외, 시간외 단일가 폐지)이 열렸다. 키움 ka10081(일봉)·
ka10080(분봉)은 접미사 없는 KRX 코드 응답에 애프터마켓 체결을 포함한다(NXT 가 아니다 — NXT 프리마켓 08:00~08:50 봉은 없다).
그래서 그날부터:

| 테이블·컬럼 | 상태 |
|---|---|
| `daily_bars`·`daily_bars_adjusted`·`daily_bars_raw` 의 `high`·`low`·`close`·`volume`·`trade_value` | 애프터마켓 포함 통합값(종가가 정규장 종가와 다른 종목-일 86%). **`open` 만 정규장** |
| `supply_demand.close`·`acc_trde_qty` | 같은 통합값 |
| `supply_demand.flu_rt` | 기준가 = 전일 **정규장** 종가 그대로 — 아래 항등식이 성립한다 |
| `minute_bars`(scalp-it 소관, DDL 도 거기) | 16:00~19:59(드물게 20:00~20:02) 봉이 생겼다. 정규장 09:00~15:20, 15:30 종가 단일가 봉, 15:32·15:35 장후 시간외 봉. 08:00~08:59·15:36~15:59 봉은 없다. 개장 1시간 지연일(신년 첫 거래일 10:00~15:35, 수능일 10:00~16:35). **과거분은 수정주가다** — 수집 뒤에 일어난 분할·병합이 소급돼 있어(001520 2025-08-01 15:30 봉 1,310 = 수정 일봉, 원 종가 655) 원 가격으로 읽으면 안 된다. 아래 항등식 대조가 2025-08 12% → 2026-07 2.5% → 2026-09~ 0.07% 로 어긋나는 게 그 몫이다 |

네이버 일별시세도 같은 통합값이라 그것과 맞는다고 정규장 값인 것은 아니다(`daily_finalize` docstring 의 옛 "오진" 판정은 이 때문에 틀렸다).
정규장 종가가 필요하면 **`regular_close` 뷰**(아래 절, migration 031)를 읽는다. 그 뷰가 쓰는 두 길:

- 분봉: 그날 15:30~15:32 마지막 봉 `close`(scalp-it 소관, 일부 종목만, 과거분은 수정주가).
- 항등식: **정규장 종가(t) = 틱반올림(`supply_demand.close`(t+1) / (1 + `flu_rt`(t+1)/1e4))** — 09-14 이후 분봉과 99.97% 일치
  (나머지 11건도 분봉 쪽이 뒤의 권리락으로 소급 수정된 것). 호가단위는 그 날(t) 기준 — 2023-01-25 개편 뒤 1·5·10·50·100·500·1,000원
  (경계 2천·5천·2만·5만·20만·50만), 그 전 코스피 1천·5천·1만·5만·10만·50만 경계. 표보다 가는 단위(개편 전 코스닥 5만원 이상 100원,
  2025-03~ 스팩 1원)는 `flu_rt` 를 재현하는 더 가는 격자로 잡는다(`krx_base_close`).

### `regular_close` 뷰 — 정규장 종가 (migration 031)

`regular_close(code, date, close, src)` — 키움 수급 행(`source LIKE 'kiwoom%'`)이 있는 종목-일. 일반 뷰(머티리얼라이즈 아님)라 수급이 들어오는 즉시 반영된다.

| 구간 | `close` | `src` |
|---|---|---|
| 2026-09-14 전 | `supply_demand.close` 그대로(그때는 그게 정규장 종가) | `sd_close` |
| 2026-09-14~ | `krx_base_close(close(t+1), flu_rt(t+1), t)` — 위 항등식 | `identity` |
| 〃, 값 없음(NULL) | t 가 수급 마지막 거래일 — t+1 수급(T+1 16:00 잠정 수집)이 들어오면 채워진다 | `pending` |
| 〃 | 그 종목의 다음 행이 없다(폐지 직전 마지막 날) | `no_next` |
| 〃 | 다음 행이 거래일 달력(09-14~ 수급 날짜)의 다음 날이 아니다 — 어느 날의 기준가인지 모른다 | `gap` |
| 〃 | t+1 에 `corporate_actions` 이벤트(`upd_rt` ≠ 0: 병합·분할·권리락 등) — 기준가가 수정 기준가다 | `base_reset` |
| 〃 | 1원 격자로도 `flu_rt` 를 못 맞추거나, 이웃 격자값도 맞아 유일하지 않다(`flu_rt` ≈ −100%) | `unmatched` |

- **지연:** t 의 값은 t+1 수급이 들어온 뒤(T+1 16:00 잠정 수집)에야 나온다. 잠정 행도 `close`·`flu_rt` 가 같은 응답이라 항등식이 성립할 것이지만 아직 확정 행으로만 대조했다. 그날 저녁에 그날 정규장 종가가 필요하면 분봉.
- **정확도(2026-10-13 실측, 읽기 전용):** 09-14~10-12 `identity` 41,949 · `pending` 2,625 · `base_reset` 31 · `no_next` 8 · `unmatched` 1.
  분봉 15:30~15:32 마지막 봉과 36,001쌍 중 35,990 일치(99.97%) — 어긋난 11건은 009730(10-12 권리락 −18.27%)·220100(09-17 권리락 −16.67%) 분봉이
  수정주가로 소급된 것이라 항등식 쪽이 원 가격이다. 09-14 전 구간에서 항등식을 `supply_demand.close` 와 대조하면(이벤트일 2,260 제외)
  2017-01~2023-01-24 99.991%, 2023-01-25~2024 99.995%, 2025-01~2026-09-11 99.974%.
- **남은 한계:** ① `corporate_actions` 에 없는 기준가 재설정(키움이 이벤트를 빠뜨린 날·폐지 종목)은 틀린 값이 `identity` 로 나온다 —
  09-14 전 대조로 연 25~60건(0.002~0.005%), 예 298000 2026-06-15 기준가 77,800 = 전일 종가 38,900 × 2. ② 2025-03~ `supply_demand.close` 가
  호가 격자 밖인 행 ~220(스팩 등 — 2,037원처럼 5원 단위가 아닌 값)은 그날 `sd_close` 가 다음 날 기준가와 0.1~0.3% 다르다 — 분봉이 없어
  어느 쪽이 KRX 종가인지 판단 불가. ③ 거래정지일의 값은 직전 종가(기준가 유지)다. ④ 네이버 폐지 백필 행(`source='naver'`)은 없다(`flu_rt` 없음).
- **속도:** 날짜 조건은 `lead()` 창 아래로 내려가지 않아 09-14~ 전체를 매번 창 계산한다(종목 조건은 내려간다). 운영 DB 같은 모양 질의:
  하루 전 종목 0.06~0.08s, 09-14~ 전부 0.1s, 한 종목 전 기간 0.08s(첫 실행 0.6s). 스크래치 DB 에서 1년치(67만 행)로 늘려도 하루 0.44s · 1년 전부 3.1s.
  해마다 그만큼 느려진다 — 하루 조회가 수 초를 넘으면 `MATERIALIZED VIEW` + `daily_finalize` 뒤 `REFRESH` 태스크(DAG 변경, §1 승인)로 바꾼다.
- **함수:** `krx_tick_size(price, d)`(그 날 그 가격대의 가장 굵은 호가단위)·`krx_tick_round(price, d)`·`krx_base_close(close, flu_rt, d)`.
  파이썬 판은 `collectors/data_checks.py` 의 `krx_tick`·`tick_round`·`base_close`·`derive_regular_close` — 표·격자·날짜가 어긋나면
  `tests/test_data_checks.py` 가 깨진다. `daily_finalize.check_data_quality` 의 항등식 검사가 같은 `base_close` 를 매일 분봉과 대조한다.

`daily_finalize` 의 마지막 태스크 `check_data_quality`(`collectors/data_checks.py`, 상류가 실패해도 돈다)가 매일 전 거래일 T 를 검사한다 —
T-1·T 분봉이 위 세션(애프터마켓은 지연 체결 여유로 20:10 까지) 밖에 50행 이상 또는 20종목 이상 있으면(1~2행 떠돌이 봉은 통과),
항등식 불일치가 1% 를 넘으면(표본 300 미만도), `daily_bars_raw` 카나리아가 틀리면 실패한다. 새 세션이 생기거나 `flu_rt` 정의가 바뀌면 여기서 먼저 터진다.
실패 줄은 원인을 머리에 단다(`[FAIL:접속 실패]` 종료 3 / `표본 부족`·`항등식 불일치`·`세션 이탈`·`카나리아` 종료 1).
06:30 에 T 의 애프터마켓 봉은 ~13% 만 적재돼 있어(scalp-it 15:45 수집, 다음 거래일 런이 채운다) T 의 세션 검사는 애프터마켓 쪽 새 세션을
놓칠 수 있다 — 그래서 다 적재된 T-1 도 본다.

## 관계형 (일반 테이블, 시계열 아님)

| 테이블 | 내용 |
|---|---|
| `news_article_tickers` | `news_articles` 관련 종목 — `(article_id, ticker)` 정규화 테이블. 종목별 뉴스 전체 조회용. 토스가 기사에 태그한 종목(`stockCodes`)이라 대부분의 기사에 없다 |
| `news_company_feed` | 토스 **종목별 피드**에 그 기사가 떴다 — `(article_id, code)`. 태그와 정의가 달라 위 표와 섞지 않는다. 연구의 "관련 종목·팬아웃" 이 이것이다. `inserted_article` = 이 쌍이 `news_articles` 행도 새로 만들었나(롤백 표시) |
| `news_company_feed_fetches` | 종목별 수집 원장 `(code, run_started)` — `status` done/page_cap/failed, `oldest_at`(page_cap 이면 실제 하한). 피드는 1만 건까지만 줘서 대형주는 오래된 기사가 없다 — **"그날 0건" 과 "그날 못 받음" 을 이 원장으로 구분한다** |
| `judges` | 기사 LLM 판정기 등록표 — 이름 하나 = 모델·system 프롬프트·user 템플릿·파라미터 한 벌(**바뀌면 새 이름**). 프롬프트 원문을 같이 저장해 재현한다. `owner_repo` 가 그 judge 의 행을 쓰는 유일한 레포 |
| `article_judgments` | 기사 LLM 판정 원장 `(article_id, code, judge)` — 모든 트레이더 레포가 읽는다. `output` JSONB(judge 마다 형식이 다르다), 성공 판정은 덮어쓰지 않고 실패 행(`output` NULL)만 재시도로 바뀐다. `judged_at`(+`judged_at_exact`=FALSE 면 배치 제출 시각) — 백필 판정이 과거 시점 백테스트에 "그때 알던 것" 으로 섞이지 않게. 실매매 경로가 읽는 `news_judgments`(012)는 별개로 그대로 |

## 펀더멘털·마스터 (일반 테이블)

| 테이블 | 내용 |
|---|---|
| `stocks` | 종목 마스터(코드·이름·시장·섹터) |
| `earnings` | DART 분기 실적(순이익·매출·영업이익, 당기/전년동기), lookahead-safe `avail_date` + 정정 이력을 보존하는 `knowledge_date`(PK: code, period, knowledge_date) |
| `earnings_disclosures` | 실적이 **실제로 알려진 날** — DART 원본 공시 접수일, `(code, period, kind)` 마다 가장 이른 것. `kind` = `prelim`(영업(잠정)실적 공정공시)·`pl_change`(매출액또는손익구조 30%/15% 변경)·`periodic`(사업·반기·분기보고서). `earnings` 와 `(code, period)` 로 조인 — `avail_date` 는 법정 마감 근사라 늦다(005930 2020Q1: avail 05-15, prelim 04-07). 날짜만 있고 잠정은 장 마감 뒤 제출이 많아 **다음 거래일부터** 거래 가능으로 본다. 비12월 결산·정정·자회사 공시는 없다. `collectors/dart_filing_dates.py`, 2019~ |
| `earnings_dart_doc` | **2007~2015 DART 정기보고서 원문에서 읽은 분기 순이익·발행주식총수**(034, `collectors/dart_doc.py`·`dart_doc_run.py`). `earnings`(2016~, API)와 따로 둔다 — 정의·기준이 다르다. 행 = 공시 한 건, PK `(code, period, knowledge_date)`, 원 단위. 읽는 법은 아래 [earnings_dart_doc](#earnings_dart_doc--20072015-원문-실적-migration-034) |
| `wics_membership` | 종목의 WICS 섹터(G10..G55)·산업군(중분류 G1010..G5510) **주간 스냅샷**(매주 첫 거래일), 2019~. wiseindex 가 그날 구성으로 답해 point-in-time 이다(NC 036570: 2019 G45 → 2026 G50). 분류 셋을 같이 둔다 — `sector_cd`(섹터 지수, 완전)·`industry_cd`(중분류, 2019 미디어 등 NULL)·`wi26_cd`(WI26 26업종, 2026 은 일부 NULL). 어느 쪽도 전 종목을 덮지 못한다(migration 020 실측). WICS 지수에 없는 종목(스팩·리츠 등)은 행이 없다. `collectors/wics.py` |
| `index_changes` | KOSPI 200·KOSDAQ 150·KRX 300·KOSPI 100·50 **정기변경 편입·편출** — KOSDAQ 150 2015-12~, KOSPI 200 2016-06~(2019 까지 6월만), KRX 300 2018-06~, KOSPI 100·50 은 2019-06 까지만 같은 공지에 있다. `effective_date` = 정기변경일(동시만기일 다음 거래일 — 그날부터 새 구성, 패시브 매매는 **전 거래일 종가**), `announce_date` = 공지 게시일(시각 없음). KRX 지수운영공지 첨부 엑셀에서. 수시변경(특례편입·분할·합병·신규상장)은 없다 — `index_notices` 원문 |
| `index_constituents` | 정기변경 후 **전체 명단**(그 `effective_date` 부터 다음 정기변경 전까지, 사이 수시변경 미반영). 명단 크기가 200/150/300/100/50 이 아니면 수집기가 예외 |
| `index_notices` | KRX 지수운영공지(index.krx.co.kr, 로그인 불요) 원문 전부 — 제목·본문·첨부명, 2015-01~. 수시변경은 여기서 찾는다 |
| `delisted_stocks` | 상장폐지 종목 마스터(생존편향 보정). 과거 시세는 `daily_bars` 에 `source='naver'` 로 들어간다 |
| `backfill_markers` | `(code, source)` — "조회해봤는데 자료가 없더라"를 기록해 백필이 같은 코드를 매 회차 다시 훑지 않게 한다 |
| `news_judgments` | LLM이 news_articles/disclosures를 읽고 낸 판단. PK `(source_type, source_id, ticker, prompt_version)` — prompt_version이 바뀌면 새 행, 기존 행은 절대 안 고침(재현성). `confidence`(LLM 자체 확신도 0~100)·`judged_at`(응답 시각, 레이턴시 측정용)은 013 이후 행에만 있다(NULL=013 이전). **`judged_at`은 TIMESTAMPTZ(UTC 저장)** — scalp-it이 `ticks.ts`(naive KST)와 조인하다 9시간 밀리는 걸 실측했다(2026-09-06). naive KST 컬럼과 조인하는 쪽은 항상 KST로 변환해야 한다. **2026-10-12 부터 Haiku 5.5·`prompt_version` v2 만** — v1(Haiku 4.5) 14,534행은 018 로 `legacy.news_judgments_haiku45` 에 보관 |
| `news_judge_batches` | `news_judge.py` 의 Message Batches 원장 `(batch_id)` — `items` JSONB = custom_id `r{i}` → `[source_type, source_id, ticker]`, `collected_at` NULL = 아직 수거 안 함. create 직후 곧바로 쓴다 — 태스크가 못 기다리거나 죽어도 다음 실행이 수거하고, 그 안의 기사를 다시 내지 않는다(이중 과금 방지) |

### `earnings_dart_doc` — 2007~2015 원문 실적 (migration 034)

- **knowledge_date = 그 레코드를 실은 공시의 접수일**(접수번호 앞 8자리). 시각이 없고 정기보고서는 장 마감 뒤
  제출이 많아 **다음 거래일부터** 쓴다. 정정공시는 정정본 접수일로 **새 행**이다(`is_correction`, `rcept_no`).
- **as-of 로 읽기**: 시점 T 에 알 수 있던 값 = `(code, period)` 마다 `knowledge_date < T`(다음 거래일 규칙) 인 행 중
  `knowledge_date` 가 가장 늦은 것. 순이익 값이 하나도 없는 정정본은 적재하지 않으므로 최신 행이 원본 값을 NULL 로 가리지 않는다.
  ```sql
  SELECT DISTINCT ON (code, period) * FROM earnings_dart_doc
   WHERE knowledge_date < :asof ORDER BY code, period, knowledge_date DESC;
  ```
- **netinc / basis** = `earnings` 와 같은 정의: 연결 당기순이익(비지배 포함, `cfs`) → 없으면 별도(`ofs`). 연결이
  요약재무정보뿐이고 별도는 손익계산서가 있으면 별도가 먼저(XBRL 이 본표를 따른다). **netinc_ctrl / basis_ctrl** =
  지배기업 소유주 귀속(`ctrl`) → 연결 합계 → 별도. 값은 그 분기 몫: Q1~Q3 = 3개월, **Q4 = 사업연도 전체**(`earnings` 와 같다).
  기준별 3개월·누적은 `{ctrl,cfs,ofs}_q`·`_ytd`(Q4 `_q` 는 연간 − 3분기 누적), 차분으로 만든 3개월은 `*_derived`.
  차분은 같은 기준·같은 누적 시작일(`ytd_start`, 설립 첫해만 1월 1일이 아님)끼리만 한다.
- **전년 동기(`netinc_prior`·`netinc_ctrl_prior`·`{ctrl,cfs,ofs}_prior_q`)** = 같은 보고서·**같은 표**의 비교 열(재작성 반영) —
  `earnings.netinc_prior`(fnlttMultiAcnt `frmtrm_amount`)와 같은 뜻: Q1~Q3 전년 같은 분기 3개월, Q4 전년 연간. 3개월 비교 열이
  없거나(누적만) 그 분기 값이 차분이면 NULL, 사정은 `prior_basis_note`. 당기 적재 뒤 `dart_doc_run reparse-prior --update`
  (캐시 원문만, DART 호출 0)로 채운다. 2016 겹침 `earnings.netinc_prior` 대조 98.3%(대조 가능한 분모).
- **비12월 결산 미지원**: 3·6월 결산사는 넣지 않는다(달력분기 라벨이 회계분기와 어긋난다). `earnings` 는 이들을 회계분기
  그대로 달력 라벨로 담고 있어 두 표를 이으면 그 종목은 이음매가 생긴다. 증권·보험사는 2013 까지 3월 결산이라 빠진다.
- **2008(K-GAAP) 기준 혼합**: 2010 까지 분기보고서는 별도만, 사업보고서(Q4)만 연결이다(분기 연결 의무 없음). 한 해 안에서
  Q1~Q3 `basis='ofs'`, Q4 `'cfs'` 가 섞인다 — 시계열로 쓸 때 `basis` 를 같이 본다(같은 기준 열 `ofs_*` 는 연간까지 이어진다).
  2011 K-IFRS 부터 분기도 연결.
- 검증(2016 겹침): `earnings` 와 대조 가능한 보고서 기준 97.9%, 같은 공시를 다르게 읽은 것 0. `flags` 의
  `cfs_ctrl_inconsistent`(회사 표의 지배+비지배 ≠ 합계 — ctrl 을 비웠다)·`unit_rescaled`·`*_unit_conflict` 는 걸러 쓸 수 있다.

## TimescaleDB 설계 노트

- **청크 크기** — 모든 hypertable 이 `chunk_time_interval = 1년` 을 쓴다. 기본 7일
  청크는 이 데이터 볼륨(~2,600종목 × 250거래일/년 ≈ 65만 행/년)에 비해 지나치게
  잘게 쪼개져 청크 메타데이터 오버헤드가 커지고, 여러 해에 걸친 스캔이 느려진다.
- **압축** — 시세·수급·컨센서스 hypertable 은 30일이 지난 청크를 컬럼형으로 자동
  압축한다(`compress_segmentby = 'code'`). 최근 데이터는 행 기반으로 남겨 잦은
  upsert 를 빠르게 처리한다. **7일이 아니라 30일인 이유**(`sql/migrations/008`):
  수집기가 매일 쓰는 창이 일봉·수급 15일, 공매도·신용 10일이라 경계가 7일이면 그
  창의 절반 이상이 이미 압축된 청크를 때린다 — upsert 마다 세그먼트
  압축해제→갱신→재압축이 돌았다(pg_stat 실측: 압축 청크에서 `n_tup_ins ≈
  n_tup_del`, `n_live_tup = 0`).
- **`daily_bars_adjusted` 는 압축 제외** — `daily_price_adjust`/`weekly_price_adjust`
  가 테이블 전체를 upsert 로 재작성하므로 압축을 걸면 압축해제→재압축 순환만
  반복된다. 라이브 DB 에서 실측 압축률이 739MB → 901MB 로 음수였다 — 008 에서
  전량 `decompress_chunk` + `compress = false` 로 영구 해제했다.
- **DB 쓰기** — 수집기는 `psycopg2.extras.execute_values` 로 배치 upsert 하고, 긴
  전수 수집은 청크 단위(100종목)로 중간 커밋해 크래시 시 손실을 제한한다.

## 스키마 마이그레이션

`sql/init_timescale.sql` 은 **새 DB 를 세우는** 스크립트다. 이미 데이터가 있는 DB 는
[`sql/migrations/`](../sql/migrations) 를 순서대로 적용한다.

```bash
psql "$KR_QUANT_DB" -v ON_ERROR_STOP=1 -f sql/migrations/001_earnings_knowledge_date.sql
```

| 마이그레이션 | 내용 |
|---|---|
| `001_earnings_knowledge_date` | `earnings` PK 를 `(code, period, knowledge_date)` 로 확장 — 정정공시가 덮어쓰지 않고 새 버전으로 쌓인다 |
| `002_daily_bars_source` | `daily_bars.source` 추가 — 폐지 종목 시세를 네이버에서 받으면서 행별 출처를 남긴다(거래대금이 실측/근사로 갈림) |
| `003_daily_bars_adjusted_source` | `daily_bars_adjusted.source` 추가 — 002 의 짝. 백테스트가 읽는 건 조정가 테이블이라 거기까지 전파돼야 근사 거래대금을 식별할 수 있다. **적용 후 조정가를 재생성해야 기존 행이 채워진다** |
| `004_delisted_naver_checked` | `delisted_stocks.naver_checked` — 네이버에 자료가 없는 코드를 표시해 주간 재조회를 막는다(실측 주간 1,758회 → 0회) |
| `005_shares_source_knowledge` | `shares_outstanding_history` 에 `source`·`knowledge_date` 추가 — 기준일 ≠ 공시일 |
| `006_supply_demand_source` | `supply_demand.source` 추가 — 네이버 폐지 수급은 항목이 일부뿐이라, 읽는 쪽이 NULL(모름)과 0(순매매 없음)을 구분해야 한다 |
| `007_delisted_backfill_markers` | `delisted_stocks.dart_checked`·`naver_sd_checked` — 004 와 같은 병 |
| `008_compression_and_lookahead_cleanup` | 압축 경계 7일 → 30일 · `daily_bars_adjusted` 압축 영구 해제 · `shares_outstanding_history` 의 lookahead 행 삭제 |
| `009_backfill_markers` | 004·007 의 마커 컬럼을 `backfill_markers(code, source)` 테이블로 옮긴다. 마커가 `delisted_stocks` 에 있으면 **상장 종목에는 쓸 수가 없었다** — 소스가 늘 때마다 컬럼을 붙이는 대신 자리를 바꿨다 |
| `010_news_articles` | `news_articles`/`news_article_tickers` 신설 — krx-news-client(토스) 뉴스를 백테스팅+실매매용으로 영속 저장. `id` 자연키 upsert라 krx-news-rest-api 옛 Redis 캐시(ZSET member=article JSON)가 갖던 dedup 버그가 없다 |
| `011_disclosures` | `disclosures` 신설 — krx-news-client(DART) 공시를 백테스팅+실매매용으로 영속 저장 |
| `012_news_judgments` | `news_judgments` 신설 — LLM 뉴스/공시 판단, 장전/장중 DAG가 채움 |
| `013_news_judgments_confidence_judged_at` | `news_judgments`에 `confidence`(LLM 확신도 0~100)·`judged_at`(응답 시각, UTC) 추가 — scalp-it 세션 요청(오탐 필터링·레이턴시 측정용), 둘 다 nullable(013 이전 행은 소급 불가) |
| `014_fix_collected_at_9h` | **데이터 정정** — `news_articles`·`disclosures` 의 `collected_at` 이 2026-09-11 이전 행(928·2,631)에서 9시간 일찍 저장돼 있던 것을 +9h. 원인은 krx-news-client 의 naive `datetime.now()`(그쪽 86cc785 에서 수정). 검증·롤백은 파일 하단 |
| `015_news_company_feed` | `news_company_feed`·`news_company_feed_fetches` 신설, 토스 종목별 뉴스 아카이브(2023-03-16~, daytrade-it 파일)를 DB 로 이전 — 그전엔 아카이브 태그에 고정된 분리 워크트리의 비관리 크론이 파일에만 쌓았다. 기사 본체는 `news_articles` 에 같은 id 규칙으로 |
| `016_article_judgments` | `judges`·`article_judgments` 신설 — 파일 네 곳에 흩어져 있던 기사 LLM 판정(4.5 유니버스 156,605·5.5 지속성 36,171 등)을 한 원장으로. `news_judgments` 는 실매매 경로라 그대로 |
| `017_dedupe_news_articles_edited` | **데이터 정정** — `news_articles` 의 같은 id 두 행 4건(토스가 고쳐 createdAt 이 바뀐 기사를 daily_news 가 새 행으로 넣었다)에서 먼저 본 판만 남긴다. 같은 날 `news_toss.py` 도 "이미 다른 시각으로 있는 id 는 건너뛴다" 로 고쳐 재발을 막았다. 롤백은 파일 하단(10-08 core 덤프) |
| `018_archive_haiku45_judgments` | **데이터 보관** — Haiku 4.5 판정을 운영 테이블에서 `legacy` 스키마로 옮긴다: `news_judgments` v1 14,534 → `legacy.news_judgments_haiku45`, `article_judgments`·`judges` 의 `v2-haiku45` 157,834·1 → `legacy.article_judgments_haiku45`·`legacy.judges_haiku45`. 이후 판정은 Haiku 5.5 만(`news_judge.py` v2). `legacy.*` 는 코드가 읽지 않는 보관용이라 `init_timescale.sql` 에 없다. 롤백은 파일 하단 |
| `019_news_judge_batches` | `news_judge_batches` 신설 — `news_judge.py` v2 가 동기 루프에서 Message Batches 로(기사 LLM 분석 = Haiku 5.5 + 캐싱 + 배치 의무, 2026-10-12). 배치는 최대 24시간이라 못 기다린 배치를 다음 실행이 수거하고, retries=1 재시도가 같은 기사를 다시 내지 않게 한다 |
| `020_earnings_disclosures_wics` | `earnings_disclosures`·`wics_membership` 신설 — swing-it 결합 전략 연구(설계 구간 2020~2023)가 실제 실적 공시일과 섹터 이력을 요청. `earnings` 행은 건드리지 않는다 |
| `021_index_changes` | `index_notices`·`index_changes`·`index_constituents` 신설 — 지수 편입·편출 수급 검증용(swing-it 요청). data.krx 는 로그인이 필요하지만 KRX 지수 사이트 공지 게시판은 아니다 |
| `022_archive_dart_shares_unit_errors` | **데이터 정정** — `shares_outstanding_history` 의 DART 단위 오류 31행(27종목, ×1,000·×1,000,000)을 `legacy.shares_outstanding_dart_unit_errors` 로 옮기고 삭제(사용자 승인). 원천(stockTotqySttus)이 회사 제출 그대로 틀린다 — `dart_shares.py` 의 `drop_unit_errors` 가 재발을 막는다. 롤백은 파일 하단 |
| `023_archive_earnings_unit_errors` | **데이터 정정** — `earnings` 의 DART 단위 오류 20행(그 종목 중앙값 500배 이상·1e11 원 초과를 앞뒤 분기와 대조해 확정)을 `legacy.earnings_unit_errors` 로(사용자 승인). 008700 처럼 **여러 해 줄곧 1/1,000** 인 종목은 이 기준이 거꾸로 걸려 제외 — 별도 조사. 다음 해 같은 분기의 `*_prior` 에 오류가 남을 수 있다 |
| `024_prune_delisted_from_stocks` | `stocks`(상장 마스터)에 남은 폐지 31종목(2026-06-26~10-07 폐지)을 `legacy.stocks_delisted_20261012` 로(사용자 승인). `upsert_stocks` 가 빼지 않아 다시 쌓인다 |
| `025_archive_earnings_scale_shift` | **데이터 정정** — 여러 해 줄곧 원화가 아닌 단위로 들어온 2종목 41행(008700 2019Q1~ ×1/1,000, 241560 두산밥캣 2023Q4~ ×1/1,300 달러 보고)을 `legacy.earnings_scale_shift` 로(사용자 승인). `dart_earnings._known_bad` 가 023·025 로 뺀 분기를 다시 받지 않는다 |
| `026_daily_bars_raw` | `daily_bars_raw` 신설 — `daily_bars` 가 수정주가라 과거 시총이 수 배 부풀던 것(swing-it 실측 월말 종목-월 3~14%). `daily_bars` 는 덮지 않는다(사용자 승인) |
| `027_corporate_actions` | `corporate_actions` 신설 — 수정주가를 ±30% 점프 탐지(가짜 이음매 423 종목-일) 대신 원주가 × 이벤트 누적계수로 다시 만들기 위해(swing-it 요청). ka10081 이 행마다 `upd_stkpc_tp`·`upd_rt` 를 준다(10-12 프로브) |
| `028_delisted_history` | `daily_bars_delisted_naver`·`kind_delistings` 신설 — 2006~2016 이력 확장의 생존편향 쪽(사용자 승인 2026-10-12). 키움은 폐지 코드를 안 준다 |
| `029_daily_bars_adj_v2` | `daily_bars_adj_v2_build` 신설 — `daily_bars_adjusted` 의 가짜 이음매(423 종목-일)와 2026-09-14~ KRX 애프터마켓 포함 종가를 원주가 × 이벤트 계수·KRX 분봉으로 대신한다(사용자 승인 10-12, 기존 표 보존) |
| `030_daily_bars_adj_v2_rename` | `daily_bars_adj_v2_build` → `daily_bars_adj_v2` — 합격 검사 4종 통과(카나리아·이음매 날 최대 30.0%·이벤트↔키움 비율 2,397/2,400) |
| `031_regular_close` | `regular_close` 뷰 + `krx_tick_size`·`krx_tick_round`·`krx_base_close` 함수 — 2026-09-14~ 정규장 종가를 기준가 항등식으로 파생(사용자 승인 10-13 "항등식 파생부터"). 테이블·기존 행은 건드리지 않는다 |
| `032_naver_mstock` | `naver_investor_trend`·`naver_research_reports` 신설 — 2008~ 수급·보유율·리포트 목표가(swing-it 요청). 키움 ka10008 은 50일, 옛 frgn.naver 는 개편으로 사라짐 |
| `033_program_trading` | `program_trading` 신설 — 종목별 일별 프로그램 매매(swing-it 요청, 2016-09~) |
| `034_earnings_dart_doc` | `earnings_dart_doc` 신설 — 2007~2015 DART 정기보고서 원문 파싱 실적·발행주식총수(2006~2016 이력 확장 완료 조건 C, 2016 겹침 97.9%). `earnings` 와 정의가 달라 섞지 않는다 |
| `035_naver_research_prev_goal_null` | `naver_research_reports.prev_goal_price` 전부 NULL — 원천 값이 전 거래일 종가(price_at_write 와 100% 같음)였다(사용자 승인 10-13) |
| `036_daily_bars_adj_v2_build_2008` | `daily_bars_adj_v2_build` 재생성 — 원주가 2007-11-22~ 확장분까지 v2 전량 재계산(검사 4종 뒤 이름 교체, 운영 표 제자리 갱신 금지) |
| `037_daily_bars_listed_naver` | `daily_bars_listed_naver` 신설 — 상장 종목 네이버 수정주가 2007-11~2016-09, v2 의 빠진 수정 이벤트(거래정지 뒤 재개일 감자·병합) 추정 기준. 수익률 원천 아님 |
| `038_daily_bars_adj_v2_swap_2008` | `daily_bars_adj_v2_build`(2007-11-22~, 1,001만행) → `daily_bars_adj_v2`, 옛 표 → `daily_bars_adj_v2_2016`. 검사 4종 결과는 038 머리말 |

> ⚠️ 001 은 코드가 먼저 나가고 DB 적용이 3일 늦었다. 그 사이 `daily_earnings` 가
> 초록불이었던 건 비수기라 `rows=0` 이어서 DB 를 건드리기 전에 빠져나갔기 때문이지,
> 스키마가 맞아서가 아니다. **스키마를 바꾸는 커밋은 DB 적용까지가 한 단위다.**
