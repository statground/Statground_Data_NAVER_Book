# Statground NAVER Book Rust Conversion

이 디렉터리는 기존 Python 기반 NAVER Book 수집/통계 배치를 Rust로 옮긴 버전입니다.

## 바이너리 매핑

- `collect.py` -> `cargo run --release --bin collect`
- `collect_manual.py` -> `cargo run --release --bin collect_manual`
- `batch_aladin_publisher_seed.py` -> `cargo run --release --bin batch_aladin_publisher_seed`
- `batch_raw_naver_stats.py` -> `cargo run --release --bin batch_raw_naver_stats`
- `optimize_raw_naver_random_partition.py` -> `cargo run --release --bin optimize_raw_naver_random_partition`

## 설계 포인트

- ClickHouse 연동은 공식 HTTP 인터페이스 + `JSONEachRow` 기반으로 단순화했습니다.
- NAVER API 호출은 `reqwest` blocking client 기반입니다.
- UUID 정책은 플랫폼 기준에 맞춰 `UUID v7`를 사용합니다.
- 통계 PNG 생성은 `plotters` 기반입니다.
- GitHub Actions는 Python 설치 없이 Rust toolchain만 설치해서 실행하도록 변경했습니다.

## 차이점

기존 Python `collect.py`는 Konlpy/Okt + NLTK 기반으로 제목 키워드를 만들었지만,
이 Rust 버전은 빌드/배포 복잡도를 낮추기 위해 정규식 기반 한국어/영문 토큰 휴리스틱으로 대체했습니다.
즉, **동작 목적은 동일하지만 키워드 생성 결과는 완전 동일하지 않을 수 있습니다.**

## 실행에 필요한 환경변수

기존 GitHub Actions와 동일한 환경변수를 그대로 사용합니다.

- ClickHouse: `CH_HOST`, `CH_PORT`, `CH_USER`, `CH_PASSWORD`, `CH_DATABASE`
- NAVER: `NAVER_API_KEYS`
- 수집기/통계기/최적화기 세부 변수는 기존 workflow 파일을 그대로 참고하면 됩니다.
