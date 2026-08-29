# 미국(Sharadar) 파이프라인

[← README](../README.md)

이 저장소에서 유일한 비한국 파이프라인이다. 한국 파이프라인과 스케줄러·인프라를
공유한다 — 저장소 이름이 `quant-airflow` 인 이유다.

| DAG | 스케줄(KST) | 하는 일 |
|---|---|---|
| `daily_sharadar` | 화~토 17:30 | 벌크 스냅샷 동기화 → 스토어 재구축 → 검증 → 원자적 공개. 테이블별로 실패를 격리해 **한 개가 죽어도 나머지 13개의 상태 표는 남는다** |

설계 근거는
[`docs/superpowers/specs/2026-08-15-sharadar-bulk-rebuild-design.md`](superpowers/specs/2026-08-15-sharadar-bulk-rebuild-design.md).

## 증분이 아니라 재구축이다

처음엔 API 증분(종목 22,000개를 30개씩 ~730회 순회)으로 만들었다가 실측에서 버렸다.

- 벤더 티커 제한이 개수(30)가 아니라 **문자열 200자**라 우선주 티커 30개면 무조건
  `400` 이었다(fundamentals 매번 실패).
- 소켓 타임아웃이 재시도되지 않아 70분짜리 작업이 딸꾹질 한 번에 전멸했다.
- 스토어를 직접 upsert 하므로 연구(`opt-factor optimize`)와 DuckDB 락이 충돌했다.

벌크는 테이블당 요청 1회라 앞의 둘이 해당 없고, **새 파일에 지어 `os.replace` 로
갈아끼우므로** 셋째도 사라진다 — 연구가 도는 중에 배포해도 기존 리더는 옛 inode 를
계속 안전하게 읽는다.

```
① RAW      /opt/us-data/sharadar/raw/  ← 매일 14개 전부 대조, `modified` 가 그대로면 안 받는다
② BUILD    raw → .us_micro.duckdb.building   (검증된 --provider csv 경로 재사용)
③ GATE     테이블 결측·0행·행수 5% 이상 감소면 공개 중단
④ PUBLISH  os.replace → us_micro.duckdb   (직전 2세대는 .prev* 로 보존)
```

## 동기화 확인

구독 14개를 매일 전부 벤더 목록과 대조하고, 매 실행 끝에 상태 표(벤더
타임스탬프·정체 횟수·판정)를 찍는다. 전송이 없어도 `checked_at` 이 갱신돼 "오늘
확인했다"가 남는다. 낡음(벤더 미갱신)은 막지 않는다 — 벌크가 매번 전체 이력을 주므로
다음 실행에 저절로 채워진다. 막는 것은 손상(절단·체크섬 불일치·행수 급감·최신일
후퇴)뿐이다. 설계는
[`2026-08-16-sharadar-sync-verification-design.md`](superpowers/specs/2026-08-16-sharadar-sync-verification-design.md).

## 알아둘 것

- **적재 로직은 이 저장소에 없다.** `portfolio-research`(sibling)의 `opt-factor ingest`
  를 부른다 — `weekly_price_adjust` 가 kr-quant 를 쓰는 것과 같은 구조(ro 마운트 +
  `PYTHONPATH`, pip install 없음). `_csv_daily`(백만달러 환산)·`_csv_tickers`
  (`is_delisted` 리네임)·`_csv_fundamentals`(PIT 위반 제외)는 실제 버그에서 나온
  코드라 **DuckDB SQL 로 재구현하지 않는다**(동등성 테스트 없이는 금지).
- **스토어**: `~/data/us_micro.duckdb`(2.7GB, 컨테이너에선 `/opt/us-data/`).
  가격 21,963종목·4,630만행, 1997~. 폐지 종목 포함. 경로는 `.env` 의 `US_DATA_DIR`.
- **17:30 인 이유**: 벤더 드롭이 그날 다 끝난 뒤라야 받을 게 있다. ⚠️ **테이블별 시각은
  믿을 게 못 된다** — 2026-08-15 실측(stocks 16:40 / fundamentals 16:49 / funds 16:54)이
  08-27 재측정에서 크게 이동했다(stocks 12:40 / fundamentals 12:25 / funds 12:47, 반대로
  holdings_ticker 는 01:39 → 14:42). 개별 표가 아니라 **그날의 마지막 드롭**이 기준이고,
  관측된 최악값이 17:28(2026-08-25)로 현재 스케줄보다 2분 이르다. 그래서 17:30 은
  앞당기지 않는다(표본 3일이라 근거는 얇다). 화~토인 건 미국 장이 없는 날엔 새로 받을 게
  없어서다(금요일 세션은 토요일 드롭에 실려 온다).
- **실측 소요(2026-08-25 정기 런)**: DAG 전체 31.6분 — 다운로드 3,307MB 약 9분(14개 중
  `새로 받음 10 · 확인만 4`, `modified` 스킵이 실제로 걸린다), 빌드·검증·공개 22.2분.
  9일 공백을 메운 날은 14개를 전부 받아 3,565MB / 15.3분이었다.
- **아직 스토어에 안 들어가는 것**: funds(SFP)·holdings(SF3)·holdings_investor(SF3B)·
  events. 구독분이라 raw 아카이브에는 받아두지만, `portfolio-research` 에 테이블이 없어
  적재는 못 한다. `metrics` 는 종목당 1행 최신 스냅샷뿐이라(히스토리 없음) 백테스트에
  쓰면 look-ahead 다.
