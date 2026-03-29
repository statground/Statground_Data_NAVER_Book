import os
import json
import random
import requests
import clickhouse_connect
import uuid6
import pandas as pd
import pytz
from datetime import datetime

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
MANUAL_KEYWORD = os.environ["MANUAL_KEYWORD"].strip()

# ─────────────────────────────
# ClickHouse 연결
# ─────────────────────────────
client = clickhouse_connect.get_client(
    host=CH_HOST,
    port=CH_PORT,
    username=CH_USER,
    password=CH_PASSWORD,
    database=CH_DATABASE
)

TABLE_NAME = "raw_naver"


def pick_api_headers() -> dict:
    """수집 실행(요청)마다 NAVER_API_KEYS 중 랜덤 선택"""
    api = random.choice(NAVER_API_KEYS)
    return {
        "X-Naver-Client-Id": api["client_id"],
        "X-Naver-Client-Secret": api["client_secret"],
    }


def get_table_columns(database: str, table: str) -> set[str]:
    """테이블 컬럼 목록을 조회해서, 존재하는 컬럼만 INSERT 하도록 한다."""
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


def fetch_items(keyword: str, sort: str, start: int, display: int = 100) -> list[dict]:
    """네이버 API 호출 (JSON)"""
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


def build_uuid_map(isbns: list[str]) -> dict[str, str]:
    """
    ISBN -> UUID 매핑
    - 결과가 0건이거나, clickhouse-connect가 빈 DF에 컬럼 메타를 안 주는 경우를 방어한다.
    """
    isbns = [i for i in isbns if isinstance(i, str) and i.strip()]
    if not isbns:
        return {}

    # ✅ ISBN은 일반적으로 숫자/공백 형태라 따옴표 escape가 필요 없음
    quoted = ",".join([f"'{i}'" for i in isbns])

    q = f"""
        SELECT isbn, uuid
        FROM {TABLE_NAME}
        WHERE isbn IN ({quoted})
        LIMIT 100000
    """

    try:
        df = client.query_df(q)
    except Exception:
        return {}

    # ✅ 여기서 KeyError 방지
    if df is None or df.empty:
        return {}
    if ("isbn" not in df.columns) or ("uuid" not in df.columns):
        return {}

    return dict(zip(df["isbn"].astype(str), df["uuid"].astype(str)))


def filter_row_by_existing_columns(row: dict) -> dict:
    """테이블에 존재하는 컬럼만 남겨서 insert_df가 스키마 불일치로 터지지 않게 한다."""
    return {k: v for k, v in row.items() if k in TABLE_COLUMNS}


def collect_manual(keyword: str):
    if not keyword:
        raise ValueError("MANUAL_KEYWORD가 비어 있습니다.")

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
            uuid_map = build_uuid_map(batch_isbns)

            rows = []
            for it in items:
                isbn = it.get("isbn")
                if not isbn:
                    continue

                # ✅ ISBN이 이미 있으면 UUID 재사용, 없으면 신규 UUID v7
                book_uuid = uuid_map.get(isbn, str(uuid6.uuid7()))

                row = {
                    "uuid": book_uuid,

                    # version 컬럼이 있는 테이블에서만 사용 (테이블에 없으면 filter에서 자동 제거됨)
                    "version": version,

                    "created_at": now,
                    "created_log": "github_actions_manual",
                    "updated_at": now,
                    "updated_log": f"manual_upsert|keyword={keyword}|sort={sort}",

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
    collect_manual(MANUAL_KEYWORD)
