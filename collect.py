import os
import json
import random
import requests
import clickhouse_connect
import uuid6
import pandas as pd
import pytz
import re
from datetime import datetime

# 형태소/영문 품사 태깅 (GitHub Actions 환경에서 동작하던 구성을 유지)
from konlpy.tag import Okt
import nltk
from nltk import pos_tag, word_tokenize

# nltk 리소스 다운로드 (최초 실행 시)
nltk.download("punkt")
nltk.download("averaged_perceptron_tagger")

KST = pytz.timezone("Asia/Seoul")
NAVER_URL = "https://openapi.naver.com/v1/search/book.json"

# ─────────────────────────────
# 환경 변수
# ─────────────────────────────
CH_HOST = os.environ["CH_HOST"]
CH_PORT = int(os.environ["CH_PORT"])
CH_USER = os.environ["CH_USER"]
CH_PASSWORD = os.environ["CH_PASSWORD"]
CH_DATABASE = os.environ["CH_DATABASE"]
NAVER_API_KEYS = json.loads(os.environ["NAVER_API_KEYS"])

TABLE_NAME = "raw_naver"

client = clickhouse_connect.get_client(
    host=CH_HOST,
    port=CH_PORT,
    username=CH_USER,
    password=CH_PASSWORD,
    database=CH_DATABASE
)

okt = Okt()


# ─────────────────────────────
# ClickHouse 스키마 조회 (존재 컬럼만 insert)
# ─────────────────────────────
def get_table_columns(database: str, table: str) -> set[str]:
    q = f"""
        SELECT name
        FROM system.columns
        WHERE database = '{database}'
          AND table = '{table}'
    """
    df = client.query_df(q)
    if df is None or df.empty or "name" not in df.columns:
        return set()
    return set(df["name"].astype(str).tolist())


TABLE_COLUMNS = get_table_columns(CH_DATABASE, TABLE_NAME)


def filter_row_by_existing_columns(row: dict) -> dict:
    return {k: v for k, v in row.items() if k in TABLE_COLUMNS}


# ─────────────────────────────
# NAVER API 헤더 (요청마다 랜덤 키)
# ─────────────────────────────
def pick_api_headers() -> dict:
    api = random.choice(NAVER_API_KEYS)
    return {
        "X-Naver-Client-Id": api["client_id"],
        "X-Naver-Client-Secret": api["client_secret"],
    }


def fetch_items(keyword: str, sort: str, start: int, display: int = 100) -> list[dict]:
    headers = pick_api_headers()
    params = {
        "query": keyword,
        "display": display,
        "start": start,
        "sort": sort,  # sim | date
    }
    r = requests.get(NAVER_URL, headers=headers, params=params, timeout=20)
    if r.status_code != 200:
        return []
    data = r.json()
    return data.get("items", []) or []


# ─────────────────────────────
# 키워드 생성: raw_naver title 100개 랜덤 → 형태소/품사 → 조건 필터 → 랜덤
# ─────────────────────────────
def generate_keyword() -> str:
    try:
        df = client.query_df("SELECT title FROM raw_naver ORDER BY rand() LIMIT 100")
    except Exception:
        df = pd.DataFrame()

    if df is None or df.empty or "title" not in df.columns:
        return random.choice(["통계", "데이터", "Statistics", "Data"])

    keywords: list[str] = []

    for title in df["title"].tolist():
        if not isinstance(title, str):
            continue

        if re.search("[가-힣]", title):
            # 한글: 명사, 2글자 이상
            nouns = okt.nouns(title)
            keywords += [n for n in nouns if len(n) >= 2]
        else:
            # 영문: 명사(대략 NN*), 4글자 이상
            tokens = word_tokenize(title)
            tagged = pos_tag(tokens)
            keywords += [w for w, t in tagged if t.startswith("NN") and len(w) >= 4]

    if not keywords:
        return random.choice(["통계", "데이터", "Statistics", "Data"])

    return random.choice(keywords)


# ─────────────────────────────
# ISBN -> 기존 uuid / created_at / created_log 매핑
# (UUID 불변 + 최초 created_at 유지)
# ─────────────────────────────
def build_existing_map(isbns: list[str]) -> dict[str, dict]:
    isbns = [i for i in isbns if isinstance(i, str) and i.strip()]
    if not isbns:
        return {}

    # ISBN은 보통 숫자/공백이라 따옴표 escape가 거의 필요 없지만, 최소 안전 처리(따옴표 제거)
    safe_isbns = [i.replace("'", "") for i in isbns]
    quoted = ",".join([f"'{i}'" for i in safe_isbns])

    # version 컬럼이 있으면 최신 판정을 version 기준으로
    has_version = "version" in TABLE_COLUMNS
    has_updated_at = "updated_at" in TABLE_COLUMNS
    order_expr = "version DESC" if has_version else ("updated_at DESC" if has_updated_at else "created_at DESC")

    # ClickHouse: 동일 isbn 여러 행 중 최신 1개만 뽑기 (LIMIT 1 BY isbn)
    q = f"""
        SELECT
            isbn,
            uuid,
            created_at,
            created_log
        FROM {TABLE_NAME}
        WHERE isbn IN ({quoted})
        ORDER BY {order_expr}
        LIMIT 1 BY isbn
    """

    try:
        df = client.query_df(q)
    except Exception:
        return {}

    if df is None or df.empty:
        return {}
    needed = {"isbn", "uuid", "created_at", "created_log"}
    if not needed.issubset(set(df.columns)):
        return {}

    m = {}
    for _, row in df.iterrows():
        m[str(row["isbn"])] = {
            "uuid": str(row["uuid"]),
            "created_at": row["created_at"],
            "created_log": str(row["created_log"]) if row["created_log"] is not None else "",
        }
    return m


# ─────────────────────────────
# 수집 본체
# ─────────────────────────────
def collect():
    keyword = generate_keyword()
    sorts = ["sim", "date"]

    for sort in sorts:
        start = 1

        while start <= 1000:
            items = fetch_items(keyword=keyword, sort=sort, start=start, display=100)
            if not items:
                break

            now = datetime.now(KST)
            version = int(now.timestamp())

            batch_isbns = [it.get("isbn") for it in items if it.get("isbn")]
            existing_map = build_existing_map(batch_isbns)

            rows = []
            for it in items:
                isbn = it.get("isbn")
                if not isbn:
                    continue

                existing = existing_map.get(isbn)

                if existing:
                    book_uuid = existing["uuid"]
                    created_at_value = existing["created_at"]
                    created_log_value = existing["created_log"]
                else:
                    book_uuid = str(uuid6.uuid7())
                    created_at_value = now
                    created_log_value = "github_actions_auto"

                row = {
                    "uuid": book_uuid,

                    # 테이블에 있으면 사용, 없으면 filter에서 제거됨
                    "version": version,

                    # ✅ 최초 값 유지(사이트맵/정렬 안정성에 도움)
                    "created_at": created_at_value,
                    "created_log": created_log_value,

                    # ✅ 매 수집마다 갱신
                    "updated_at": now,
                    "updated_log": f"auto_upsert|keyword={keyword}|sort={sort}",

                    "title": it.get("title") or "",
                    "link": it.get("link") or "",
                    "image": it.get("image") or "",
                    "author": it.get("author") or "",
                    "discount": int(it["discount"]) if it.get("discount") else None,
                    "publisher": it.get("publisher") or "",
                    "isbn": isbn,
                    "description": it.get("description") or "",
                    "pubdate": it.get("pubdate") or "",
                }

                rows.append(filter_row_by_existing_columns(row))

            if rows:
                df_ins = pd.DataFrame(rows)
                client.insert_df(TABLE_NAME, df_ins)

            if len(items) < 100:
                break

            start += 100


if __name__ == "__main__":
    collect()
