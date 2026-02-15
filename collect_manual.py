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

# 환경 변수
CH_HOST = os.environ["CH_HOST"]
CH_PORT = int(os.environ["CH_PORT"])
CH_USER = os.environ["CH_USER"]
CH_PASSWORD = os.environ["CH_PASSWORD"]
CH_DATABASE = os.environ["CH_DATABASE"]
NAVER_API_KEYS = json.loads(os.environ["NAVER_API_KEYS"])
MANUAL_KEYWORD = os.environ["MANUAL_KEYWORD"]

client = clickhouse_connect.get_client(
    host=CH_HOST,
    port=CH_PORT,
    username=CH_USER,
    password=CH_PASSWORD,
    database=CH_DATABASE
)

def collect_manual(keyword):

    api = random.choice(NAVER_API_KEYS)

    headers = {
        "X-Naver-Client-Id": api["client_id"],
        "X-Naver-Client-Secret": api["client_secret"]
    }

    sorts = ["sim", "date"]

    for sort in sorts:

        start = 1

        while start <= 1000:

            params = {
                "query": keyword,
                "display": 100,
                "start": start,
                "sort": sort
            }

            r = requests.get(
                "https://openapi.naver.com/v1/search/book.json",
                headers=headers,
                params=params,
                timeout=15
            )

            if r.status_code != 200:
                break

            data = r.json()
            items = data.get("items", [])

            if not items:
                break

            now = datetime.now(KST)
            version = int(now.timestamp())

            # 🔥 batch ISBN 조회 (UUID 재사용)
            isbns = [item.get("isbn") for item in items if item.get("isbn")]
            if not isbns:
                break

            query = f"""
                SELECT isbn, uuid FROM raw_naver
                WHERE isbn IN ({','.join([f"'{i}'" for i in isbns])})
            """

            existing_df = client.query_df(query)
            uuid_map = dict(zip(existing_df["isbn"], existing_df["uuid"]))

            rows = []

            for item in items:

                isbn = item.get("isbn")
                if not isbn:
                    continue

                if isbn in uuid_map:
                    book_uuid = uuid_map[isbn]
                else:
                    book_uuid = uuid6.uuid7()

                rows.append({
                    "uuid": book_uuid,
                    "version": version,
                    "created_at": now,
                    "created_log": "github_actions_manual",
                    "updated_at": now,
                    "updated_log": "manual_upsert",
                    "title": item.get("title"),
                    "link": item.get("link"),
                    "image": item.get("image"),
                    "author": item.get("author"),
                    "discount": int(item["discount"]) if item.get("discount") else None,
                    "publisher": item.get("publisher"),
                    "isbn": isbn,
                    "description": item.get("description"),
                    "pubdate": item.get("pubdate")
                })

            if rows:
                df_insert = pd.DataFrame(rows)
                client.insert_df("raw_naver", df_insert)

            if len(items) < 100:
                break

            start += 100


if __name__ == "__main__":
    collect_manual(MANUAL_KEYWORD)
