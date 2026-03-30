# Statground NAVER Book Collector - Go Conversion

이 디렉터리는 첨부된 Python 수집/통계 배치 코드를 Go 프로젝트로 변환한 결과물입니다.

포함된 Go 실행 파일:

- `cmd/collect` → `collect.py`
- `cmd/collect_manual` → `collect_manual.py`
- `cmd/batch_aladin_publisher_seed` → `batch_aladin_publisher_seed.py`
- `cmd/batch_raw_naver_stats` → `batch_raw_naver_stats.py`
- `cmd/optimize_raw_naver_random_partition` → `optimize_raw_naver_random_partition.py`

## 설계 요약

- ClickHouse 연동은 전용 드라이버 대신 HTTP 인터페이스 + `JSONEachRow` 기반으로 구성했습니다.
- UUID는 Go 내부 구현으로 UUID v7 문자열을 생성합니다.
- 표준 라이브러리 중심으로 작성해서 `go build ./...` 검증이 가능합니다.
- GitHub Actions도 Python 설치 대신 Go toolchain으로 실행되도록 바꿨습니다.

## 실행 예시

```bash
go run ./cmd/collect
go run ./cmd/collect_manual
go run ./cmd/batch_aladin_publisher_seed
go run ./cmd/batch_raw_naver_stats
go run ./cmd/optimize_raw_naver_random_partition
```

## 환경 변수

기본적으로 원본 Python 코드와 같은 환경 변수를 사용합니다.

필수:

- `CH_HOST`
- `CH_PORT`
- `CH_USER`
- `CH_PASSWORD`
- `CH_DATABASE`
- `NAVER_API_KEYS` (수집 배치에서 필요)

추가 배치 변수:

- `COLLECT_MODE`, `BATCH_SIZE`, `SAMPLE_ROWS`, `NAVER_DISPLAY`, `REQS_PER_TERM`
- `MANUAL_KEYWORD`
- `ALADIN_PUBLISHER_LIST_URL`, `ALADIN_MAX_WORKERS`, `ALADIN_CACHE_TABLE`
- `PUBLISHER_SAMPLE_N`, `NAVER_MAX_WORKERS`, `NAVER_SLEEP_MIN`, `NAVER_SLEEP_MAX`
- `OPTIMIZE_ENABLED`, `OPTIMIZE_RECENT_PARTITIONS`, `OPTIMIZE_FINAL`, `OPTIMIZE_TABLE`

## 차이점

원본 `collect.py`는 `KoNLPy/Okt + NLTK`로 제목 키워드를 추출했지만, Go 버전은 운영 단순화를 위해 휴리스틱 토큰 추출로 대체했습니다.

즉:

- `mixed`
- `keyword`

모드에서 term 선택 결과는 Python 원본과 완전히 동일하지 않을 수 있습니다.

## ClickHouse 주의

이 프로젝트는 ClickHouse를 분석/집계 전용 계층으로 사용하고, 삽입 대상 테이블은 `raw_naver` / `raw_aladin_publisher_cache`를 전제로 작성했습니다.
실제 운영 스키마에 따라 nullable/컬럼명/정렬키 차이가 있으면 소폭 보정이 필요할 수 있습니다.
