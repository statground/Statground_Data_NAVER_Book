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

from konlpy.tag import Okt
import nltk
from nltk import pos_tag, word_tokenize

nltk.download("punkt")
nltk.download("punkt_tab")  # ✅ 추가: Resource punkt_tab not found 해결
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
# 테이블 컬럼 조회
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
# API 키 랜덤 선택
# ─────────────────────────────
def pick_api_headers() -> dict:
    api = random.choice(NAVER_API_KEYS)
    return {
        "X-Naver-Client-Id": api["client_id"],
        "X-Naver-Client-Secret": api["client_secret"],
    }


def fetch_items(keyword: str, sort: str, start: int, display: int = 100):
    headers = pick_api_headers()
    params = {
        "query": keyword,
        "display": display,
        "start": start,
        "sort": sort,
    }
    r = requests.get(NAVER_URL, headers=headers, params=params, timeout=20)
    if r.status_code != 200:
        return []
    data = r.json()
    return data.get("items", []) or []


# ─────────────────────────────
# 키워드 생성
# ─────────────────────────────
def generate_keyword() -> str:
    fallback = ["통계", "데이터", "Statistics", "Data"]

    try:
        df = client.query_df(
            "SELECT title, author, publisher FROM raw_naver ORDER BY rand() LIMIT 100"
        )
    except Exception:
        df = pd.DataFrame()

    if df is None or df.empty:
        return random.choice(fallback)

    source = random.choice(["title", "author", "publisher"])

    # author
    if source == "author":
        candidates = []
        if "author" in df.columns:
            for v in df["author"].tolist():
                if isinstance(v, str) and v.strip():
                    parts = [p.strip() for p in v.split("^") if p.strip()]
                    candidates.extend(parts)
        return random.choice(candidates) if candidates else random.choice(fallback)

    # publisher
    if source == "publisher":
        candidates = []
        if "publisher" in df.columns:
            for v in df["publisher"].tolist():
                if isinstance(v, str) and v.strip():
                    candidates.append(v.strip())
        return random.choice(candidates) if candidates else random.choice(fallback)

    # title → 형태소 분석
    titles = []
    if "title" in df.columns:
        for v in df["title"].tolist():
            if isinstance(v, str) and v.strip():
                titles.append(v.strip())

    if not titles:
        return random.choice(fallback)

    keywords = []
    for title in titles:
        if re.search("[가-힣]", title):
            nouns = okt.nouns(title)
            keywords.extend([n for n in nouns if len(n) >= 2])
        else:
            tokens = word_tokenize(title)
            tagged = pos_tag(tokens)
            keywords.extend([w for w, t in tagged if t.startswith("NN") and len(w) >= 4])

    return random.choice(keywords) if keywords else random.choice(fallback)


# ─────────────────────────────
# 기존 UUID / created_at 유지
# ─────────────────────────────
def build_existing_map(isbns):
    isbns = [i for i in isbns if isinstance(i, str) and i.strip()]
    if not isbns:
        return {}

    safe_isbns = [i.replace("'", "") for i in isbns]
    quoted = ",".join([f"'{i}'" for i in safe_isbns])

    has_version = "version" in TABLE_COLUMNS
    order_expr = "version DESC" if has_version else "created_at DESC"

    q = f"""
        SELECT isbn, uuid, created_at, created_log
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

    result = {}
    for _, row in df.iterrows():
        result[str(row["isbn"])] = {
            "uuid": str(row["uuid"]),
            "created_at": row["created_at"],
            "created_log": row["created_log"],
        }

    return result


# ─────────────────────────────
# 수집 실행
# ─────────────────────────────
def collect():
    keyword = generate_keyword()

    for sort in ["sim", "date"]:
        start = 1

        while start <= 1000:
            items = fetch_items(keyword, sort, start, 100)
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
                    "version": version,
                    "created_at": created_at_value,
                    "created_log": created_log_value,
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
