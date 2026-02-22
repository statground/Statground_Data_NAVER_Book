#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from datetime import datetime
import pytz
import clickhouse_connect

KST = pytz.timezone("Asia/Seoul")

def _require_env(name: str) -> str:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        raise RuntimeError(f"Missing required environment variable: {name}")
    return v

CH_HOST = _require_env("CH_HOST")
CH_PORT = int(_require_env("CH_PORT"))
CH_USER = _require_env("CH_USER")
CH_PASSWORD = _require_env("CH_PASSWORD")
CH_DATABASE = _require_env("CH_DATABASE")

TABLE_NAME = "raw_naver"
OUTPUT_MD = "stats/raw_naver_stats.md"

client = clickhouse_connect.get_client(
    host=CH_HOST,
    port=CH_PORT,
    username=CH_USER,
    password=CH_PASSWORD,
    database=CH_DATABASE
)

def q_scalar(sql: str) -> int:
    r = client.query(sql)
    return int(r.result_rows[0][0]) if r.result_rows else 0

def q_rows(sql: str):
    return client.query(sql).result_rows

def md_table(headers, rows):
    out = []
    out.append("| " + " | ".join(headers) + " |")
    out.append("| " + " | ".join(["---"]*len(headers)) + " |")
    for r in rows:
        out.append("| " + " | ".join(str(x) for x in r) + " |")
    return "\n".join(out)

def main():
    now = datetime.now(KST)

    total_books = q_scalar(f"SELECT count() FROM {TABLE_NAME}")
    total_publishers = q_scalar(f"""
        SELECT countDistinct(publisher)
        FROM {TABLE_NAME}
        WHERE publisher IS NOT NULL AND length(trim(publisher)) > 0
    """)

    total_authors = q_scalar(f"""
        SELECT countDistinct(author_one)
        FROM (
            SELECT trim(author_one) AS author_one
            FROM {TABLE_NAME}
            ARRAY JOIN splitByChar('^', ifNull(author, '')) AS author_one
            WHERE length(trim(author_one)) > 0
        )
    """)

    by_year = q_rows(f"""
        SELECT toYear(created_at) AS y, count()
        FROM {TABLE_NAME}
        GROUP BY y ORDER BY y
    """)

    by_month = q_rows(f"""
        SELECT toYYYYMM(created_at) AS ym, count()
        FROM {TABLE_NAME}
        GROUP BY ym ORDER BY ym
    """)

    by_day = q_rows(f"""
        SELECT toDate(created_at) AS d, count()
        FROM {TABLE_NAME}
        GROUP BY d ORDER BY d
    """)

    by_hour = q_rows(f"""
        SELECT toStartOfHour(created_at) AS h, count()
        FROM {TABLE_NAME}
        GROUP BY h ORDER BY h
    """)

    os.makedirs("stats", exist_ok=True)

    lines = []
    lines.append("# raw_naver 집계")
    lines.append(f"- 업데이트(KST): {now.strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")

    lines.append("## 전체")
    lines.append(md_table(["지표","값"], [
        ("총 책 수", f"{total_books:,}"),
        ("저자 수(^ 분리)", f"{total_authors:,}"),
        ("출판사 수", f"{total_publishers:,}")
    ]))
    lines.append("")

    lines.append("## 연별")
    lines.append(md_table(["연도","책 수"], by_year))
    lines.append("")

    lines.append("## 월별")
    lines.append(md_table(["연월","책 수"], by_month))
    lines.append("")

    lines.append("## 일별")
    lines.append(md_table(["날짜","책 수"], by_day))
    lines.append("")

    lines.append("## 시간별")
    lines.append(md_table(["시간","책 수"], by_hour))

    with open(OUTPUT_MD,"w",encoding="utf-8") as f:
        f.write("\n".join(lines))

if __name__ == "__main__":
    main()
