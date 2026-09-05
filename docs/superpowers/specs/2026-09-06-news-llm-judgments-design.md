# 뉴스/공시 LLM 판단 — `news_judgments`

**결정일**: 2026-09-06
**상태**: 브레인스토밍 완료, 사용자 승인 대기 (스펙 리뷰 단계)
**관련**: [[dart-client-refactor-pending]] 아님 — 별개 신규 서브시스템

## 왜 만드는가

`daily_news` DAG(migrations 010/011)가 이미 토스 뉴스(`news_articles`)와 DART
공시(`disclosures`)를 TimescaleDB에 쌓고 있다. 사용자는 이 데이터를 **단기
트레이딩**(스윙~장전 진입 판단, 초단타 아님 — 아래 "범위 밖" 참고) 신호로
쓰고 싶어 하는데, 지금은 원문 텍스트만 있고 구조화된 판단(감성·이벤트
유형·이미 시장이 아는 재료인지)이 없어 그대로는 신호로 못 쓴다.

이 문서는 그 판단을 LLM으로 만들어 저장하는 새 테이블·DAG 태스크를
설계한다. 설계 과정에서 실제 컨슈머가 될 두 세션(`kr-quant`, `scalp-it`)의
의견을 구했고, 특히 `scalp-it`의 피드백(실제 트레이더 인터뷰 문서
`scalp-it` 레포 `docs/research/manju/46-manju-answers.md` 기반)이 필드셋을
바꿨다 — 그 근거는 아래 "필드셋" 절에 그대로 남긴다.

## 소유권 — 왜 quant-airflow인가

`kr-quant`는 README에 "이 DB를 읽기 전용으로 쓴다"고 명시돼 있고, 이 판단은
`earnings.knowledge_date`와 같은 point-in-time 규약(값이 아니라 **그 값을
언제 알았는지**가 신호의 일부)이 필요하다 — 그 규약을 지키려면 판단을
수집 시점에 최대한 가깝게 만들고 DAG가 그 시점을 찍어야 한다. DAG·스키마는
이 레포 소관이므로(`CLAUDE.md` §0/§2), 스키마와 판단 태스크 둘 다
quant-airflow가 맡는다. `kr-quant`/`scalp-it` 둘 다 이 테이블을 다른 테이블과
똑같이 읽기 전용으로 조인만 한다.

## 데이터 모델

새 테이블 `news_judgments` — `news_articles`/`disclosures` 둘 다 같은 모양의
판단이 필요하므로 폴리모픽하게 둔다(소스별로 테이블을 나누지 않는다 — 둘 다
컨슈머 입장에선 "이 종목에 대해 이 시점에 이런 판단이 나왔다"로 동일하게
쓰인다).

| 컬럼 | 타입 | 설명 |
|---|---|---|
| `source_type` | text | `'news'` \| `'disclosure'` |
| `source_id` | text | `news_articles.id` 또는 `disclosures.id` |
| `ticker` | text | 이 판단이 어느 종목에 대한 것인지(뉴스는 `news_article_tickers`로 여러 종목 가능, 공시는 발행사 1곳이라 1:1 — `news_dart.py` 기존 설계와 동일) |
| `event_type` | text | 닫힌 enum: `실적`/`유상증자`/`자사주`/`최대주주변경`/`소송`/`가이던스`/`규제`/`기타` — DART 표준 분류를 그대로 쓰지 않는다(외부에 채택할 만한 표준이 없다는 리서치 결론, 아래 참고) |
| `sentiment_direction` | int | -1/0/+1 |
| `related_codes` | text[] (Postgres) / JSON 문자열 (sqlite) | **이 재료로 같이 엮일 동일 테마 종목**(대장→2등주 페어트레이드용). `scalp-it` 피드백으로 추가 — 만쥬가 장전에 수동으로 하는 테마 그룹핑을 대신한다. sqlite는 배열 타입이 없으므로 `storage.py`의 `SCHEMA`에서는 JSON 인코딩된 text 컬럼으로 두고 읽기/쓰기 헬퍼에서 직렬화/역직렬화한다(다른 컬럼과 타입이 갈리는 유일한 지점이라 구현 계획에 명시적으로 남긴다) |
| `is_stale_repeat` | bool | 이 종목·이 재료가 최근 N일 내 이미 보도된 재탕인가 (아래 "재탕 판별" 참고) |
| `first_seen_date` | text | `is_stale_repeat`가 true일 때, 이 재료를 최초로 본 날짜(YYYYMMDD) |
| `price_impact_likely` | bool | 이 판단이 단기 가격에 영향 줄 만한가 |
| `rationale` | text | 자유서술 — 스코어링 대상이 아니라 감사(audit)/디버깅용 |
| `model_id` | text | 예: `"gemini-..."` — 정확한 모델 문자열은 구현 시점에 확인해 핀 |
| `prompt_version` | text | 프롬프트/모델이 바뀌면 새 버전으로, 기존 행은 절대 안 고침 |
| `knowledge_date` | text | YYYYMMDD, **판단이 실제로 이뤄진 시점**(DAG 런타임) — 백필로 소급 안 됨 |

**Upsert 키**: `(source_type, source_id, ticker, prompt_version)` — 이 레포의
"(code, date) upsert, 멱등" 규약(`CLAUDE.md` §3)을 그대로 일반화한 것이다.
같은 `prompt_version`으로 재실행해도 중복 판단 없이 그대로 스킵/덮어쓰기.

### surprise_score를 넣지 않는 이유

리서치 초안엔 `surprise_score`(0-100, "이미 시장이 알고 있었나")가 있었는데,
`scalp-it` 피드백을 반영하면서 **`is_stale_repeat` + `first_seen_date`로
대체**한다 — surprise를 연속값 점수로 잘게 매기는 것보다 "최근 N일 내 재탕
여부"라는 이진 판별이 만쥬 인터뷰(§11, "기계가 절대 못 한다"고 명시된 지점)가
실제로 쓰는 판단에 더 가깝고, 다운스트림(추격 금지 게이트)에서도 이진값이
쓰기 쉽다.

### 시총/유동성 대비 임팩트 크기를 넣지 않는 이유

`scalp-it`의 대장 낙점 로직(등락률 랭킹 기반)엔 안 쓰인다고 확인받았다 —
애초에 v1 필드셋에 넣지도 않았다.

## 실행 흐름

세 개의 트리거 시점, 모두 **기존 태스크 함수를 재사용**(수집기는 이미
자연키 upsert라 몇 번을 다시 돌아도 안전):

```
평일 08:45  premarket_news_judgment (신규 DAG)
             ├─ news_toss.py, news_dart.py 수집 (daily_news와 동일 태스크)
             └─ news_judge.py 판단 (신규)
             → 09:00 시가 진입 판단용 신호. 만쥬의 "08:50까지 테마 매핑
               완성"과 거의 일치 — 장전 테마 그룹 시딩 용도로 정확히 맞다.

평일 10:05·16:05  daily_news (기존, 태스크만 추가)
             ├─ news_toss.py, news_dart.py 수집 (기존)
             └─ news_judge.py 판단 (신규 태스크 추가)
             → 스윙성 신호. scalp-it 핵심 트리거(틱 단위)엔 못 쓰지만,
               is_stale_repeat 게이트 용도로는 쓴다(우선순위 낮음).

(범위 밖)     장중 초단위 반응
             scalp-it 핵심 진입은 "대장 상한가 3호가 3~5초" 틱 단위인데,
             수집기가 폴링 기반(news_toss/news_dart 둘 다 웹훅 아님)이라
             근본적으로 못 따라간다 — scalp-it도 이미 "기계화 불가"로
             분류해 사람 판단 영역으로 남겨둔 지점과 일치. 별도 프로젝트로
             미룬다(상시 구동 프로세스 + 실시간 이벤트 소스 확보부터 필요,
             이 레포의 배치 DAG 아키텍처와 안 맞는 다른 종류의 시스템).
```

`premarket_news_judgment`는 `daily_news`와 별개 DAG다 — Airflow DAG는
스케줄이 하나라, 08:45/10:05/16:05처럼 분(minute)이 다른 세 시점을 한 DAG로
못 묶는다. 태스크 로직은 공유 함수로 두 DAG가 같이 부른다.

## 모듈 구조

```
sql/migrations/012_news_judgments.sql   신규 (왜/검증쿼리/롤백 포함, 기존 형식)
sql/init_timescale.sql                  같은 테이블 반영
docs/schema.md                          표·마이그레이션 목록 갱신
collectors/storage.py                   sqlite SCHEMA에 news_judgments 추가,
                                         upsert_news_judgments() 추가
collectors/news_judge.py                신규 콜렉터
dags/premarket_news_judgment.py         신규 DAG (08:45)
dags/daily_news.py                      판단 태스크 추가 (기존 DAG 수정)
```

`news_judge.py` 내부 — 순수 함수와 네트워크 I/O를 분리하는 이 레포의 기존
관례(`dart_earnings.py` 마이그레이션 이후 패턴)를 그대로 따른다:

- `build_prompt(item) -> str` — 순수, 네트워크 없이 단위테스트.
- `parse_judgment(llm_response: str) -> Judgment | None` — 순수. `event_type`을
  닫힌 enum에 검증하고 범위를 벗어나거나 파싱 불가한 응답은 `None`(호출부로
  예외를 던지지 않는다 — LLM의 이상한 출력은 버그가 아니라 예상된 잡음).
- `judge_item(client, item) -> Judgment | None` — 실제 API 호출, 위 두 순수
  함수를 감싸는 얇은 wrapper. `client`가 어떤 제공사든(Gemini 기본, `model_id`가
  행마다 남으니 교체는 설정값이지 재설계가 아니다) 이 함수 하나로 국한.
- `collect(con)` — 재개 가능한 루프: `(source_type, source_id, ticker,
  prompt_version)` 기준으로 아직 안 판단된 항목을 골라 `judge_item` 호출,
  건별로 즉시 upsert(끝에 몰아쓰지 않는다 — 중간에 죽어도 이미 판단한 건
  안 잃는다, `dart_earnings.py`의 재개 스타일과 동일).

`ANTHROPIC_API_KEY`/`GEMINI_API_KEY`(사용하는 쪽) — 신규 Airflow Fernet
Variable, `dags/_common.py`의 기존 `*_env()` 헬퍼 패턴으로 subprocess에만
주입(태스크 로그에 안 찍힘, `_masked()` 적용).

### 재탕 판별(`is_stale_repeat`) 구현 메모

`first_seen_date`는 판단 시점에 "같은 (ticker, event_type 유사 재료)가
최근 N일 내 `news_judgments`에 이미 있었는가"를 조회해서 채운다 — LLM에게
과거 이력을 통째로 프롬프트에 넣는 게 아니라, DB 조회(SQL)로 후보를 좁힌
뒤 그 결과를 LLM 판단에 참고 정보로 넘기는 방식을 권장한다(비용·재현성
둘 다 낫다). N(며칠을 "최근"으로 볼지)은 구현 단계에서 정한다 — 이벤트
유형별로 다를 수 있다(예: 유상증자 재료는 실적보다 오래 유효).

## 에러 처리

두 실패 종류를 다르게 다룬다:

- **파싱 실패**(LLM 출력이 JSON 깨짐/enum 밖 값) → `parse_judgment`가
  `None`, 항목 스킵+로그. **태스크 실패로 안 센다** — 확률적 모델의 잡음이고,
  아무것도 안 쓰였으니 다음 실행의 재개 쿼리가 같은 `prompt_version`으로
  다시 시도한다.
- **API 레벨 실패**(rate limit, 인증 오류, 네트워크) → **실패로 센다**
  (`dart_earnings.py`와 같은 "실패를 성공으로 보고하지 않는다" 가드). 실행
  로그에 `대상 N건 | 판단 M건 | API실패 K건`을 찍고 K>0이면 태스크가 exit 1
  → Airflow `retries=1, 10분`(README 기존 규칙)이 작동한다.

## 백필 범위

**v1은 DAG 도입 시점부터 새로 들어오는 항목만 판단한다. 기존에 쌓인
news_articles/disclosures 과거분은 백필하지 않는다.** 이유는 latency 논리와
대칭이다 — 이 신호의 가치는 "시장이 알기 전에 먼저 판단했는가"인데, 과거
뉴스를 지금 판단하면 "그때 알았던 것"이 아니라 "지금 모델로 다시 읽은 것"이라
point-in-time 의미가 없어진다(`CLAUDE.md` §3 "오늘 값을 과거로 복사하지
않는다"와 같은 부류의 오염). 과거 데이터로 회고적 피처를 만들고 싶으면
그건 "재현 불가능한 회고적 판단"이라는 걸 명시하고 별도로 논의한다.

## 검증

마이그레이션 적용 후:

```sql
-- 테이블·컬럼 확인
SELECT column_name, data_type FROM information_schema.columns
WHERE table_name = 'news_judgments';

-- upsert 멱등성: 같은 (source_type, source_id, ticker, prompt_version) 재실행 후 행수 불변
SELECT count(*) FROM news_judgments
GROUP BY source_type, source_id, ticker, prompt_version HAVING count(*) > 1;
-- → 0행이어야 한다
```

DAG 도입 후 실측으로 확인할 것(`CLAUDE.md` §5 "초록불=성공 아니다" 가드):

- `premarket_news_judgment`가 실제로 08:45에 도는지(`logs/dag_id=premarket_news_judgment/`)
- `news_judgments.knowledge_date`의 최신값이 매 평일 갱신되는지
- API실패 카운트가 로그에 0으로 찍히는 날이 대부분인지(계속 실패면 조용히
  방치된 것 — 태스크 실패로 exit 1이 나야 사람이 본다)

## 컨슈머

`kr-quant`, `scalp-it` 둘 다 같은 TimescaleDB를 읽기 전용으로 직접 조회한다
(둘 다 기존에 이미 이 DB를 그렇게 쓰고 있음 — 별도 API/피드 서빙 인프라는
추가하지 않는다). 조인 예:

```sql
SELECT d.*, j.event_type, j.sentiment_direction, j.related_codes, j.is_stale_repeat
FROM disclosures d
JOIN news_judgments j
  ON j.source_type = 'disclosure' AND j.source_id = d.id
WHERE j.ticker = '005930';
```
