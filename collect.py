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

nltk.download('punkt')
nltk.download('averaged_perceptron_tagger')

KST = pytz.timezone("Asia/Seoul")

# 환경 변수
CH_HOST = os.environ["CH_HOST"]
CH_PORT = int(os.environ["CH_PORT"])
CH_USER = os.environ["CH_USER"]
CH_PASSWORD = os.environ["CH_PASSWORD"]
CH_DATABASE = os.environ["CH_DATABASE"]
NAVER_API_KEYS = json.loads(os.environ["NAVER_API_KEYS"])

client = clickhouse_connect.get_client(
    host=CH_HOST,
    port=CH_PORT,
    username=CH_USER,
    password=CH_PASSWORD,
    database=CH_DATABASE
)

okt = Okt()

# 1️⃣ 키워드 생성
def generate_keyword():

    df = client.query_df(
        "SELECT title FROM raw_naver ORDER BY rand() LIMIT 100"
    )

    if df.empty:
        return random.choice(["통계","데이터","Statistics","Data"])

    keywords = []

    for title in df["title"]:
        if re.search("[가-힣]", title):
            nouns = okt.nouns(title)
            keywords += [n for n in nouns if len(n) >= 2]
        else:
            tokens = word_tokenize(title)
            tagged = pos_tag(tokens)
            keywords += [
                w for w, t in tagged
                if t.startswith("NN") and len(w) >= 4
            ]

    if not keywords:
        return random.choice(["통계","데이터","Statistics","Data"])

    return random.choice(keywords)


# 2️⃣ 수집
def collect():

    keyword = generate_keyword()
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
                params=params
            )

            if r.status_code != 200:
                break

            data = r.json()
            items = data.get("items", [])
            if not items:
                break

            rows = []
            now = datetime.now(KST)
            version = int(now.timestamp())

            for item in items:
                isbn = item.get("isbn")
                if not isbn:
                    continue

                rows.append({
                    "uuid": uuid6.uuid7(),
                    "version": version,
                    "created_at": now,
                    "created_log": "github_actions",
                    "updated_at": now,
                    "updated_log": "auto_upsert",
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

            client.insert("raw_naver", rows)

            if len(items) < 100:
                break

            start += 100


if __name__ == "__main__":
    collect()
