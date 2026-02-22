#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from datetime import datetime
import pytz
import clickhouse_connect

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

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
OUT_DIR = "stats"
OUTPUT_MD = os.path.join(OUT_DIR, "raw_naver_stats.md")

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

def annotate(ax, bars):
    for b in bars:
        h = b.get_height()
        ax.annotate(f"{int(h):,}",
                    (b.get_x()+b.get_width()/2, h),
                    xytext=(0,3),
                    textcoords="offset points",
                    ha="center", va="bottom",
                    fontsize=8)

def plot_bar(title, labels, values, path, rotate=0, limit=None):
    if limit and len(labels) > limit:
        labels = labels[-limit:]
        values = values[-limit:]
    fig = plt.figure(figsize=(12,5))
    ax = fig.add_subplot(111)
    x = range(len(labels))
    bars = ax.bar(x, values)
    ax.set_title(title)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=rotate, ha="right" if rotate else "center")
    annotate(ax, bars)
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)

def main():
    now = datetime.now(KST)
    os.makedirs(OUT_DIR, exist_ok=True)

    total = q_scalar(f"SELECT count() FROM {TABLE_NAME}")
    publishers = q_scalar(f"""
        SELECT countDistinct(publisher)
        FROM {TABLE_NAME}
        WHERE publisher IS NOT NULL AND length(trim(publisher)) > 0
    """)
    authors = q_scalar(f"""
        SELECT countDistinct(author_one)
        FROM (
            SELECT trim(author_one) AS author_one
            FROM {TABLE_NAME}
            ARRAY JOIN splitByChar('^', ifNull(author, '')) AS author_one
            WHERE length(trim(author_one)) > 0
        )
    """)

    by_year = q_rows(f"SELECT toYear(created_at), count() FROM {TABLE_NAME} GROUP BY 1 ORDER BY 1")
    by_month = q_rows(f"SELECT toYYYYMM(created_at), count() FROM {TABLE_NAME} GROUP BY 1 ORDER BY 1")
    by_day = q_rows(f"SELECT toDate(created_at), count() FROM {TABLE_NAME} GROUP BY 1 ORDER BY 1")
    by_hour = q_rows(f"SELECT toStartOfHour(created_at), count() FROM {TABLE_NAME} GROUP BY 1 ORDER BY 1")

    plot_bar("Total Overview",
             ["Books","Authors","Publishers"],
             [total, authors, publishers],
             os.path.join(OUT_DIR,"raw_naver_totals.png"))

    if by_year:
        plot_bar("Yearly", [str(y) for y,_ in by_year], [c for _,c in by_year],
                 os.path.join(OUT_DIR,"raw_naver_by_year.png"))

    if by_month:
        plot_bar("Monthly (last 24)", [str(y) for y,_ in by_month], [c for _,c in by_month],
                 os.path.join(OUT_DIR,"raw_naver_by_month.png"), rotate=45, limit=24)

    if by_day:
        plot_bar("Daily (last 60)", [str(y) for y,_ in by_day], [c for _,c in by_day],
                 os.path.join(OUT_DIR,"raw_naver_by_day.png"), rotate=45, limit=60)

    if by_hour:
        plot_bar("Hourly (last 48)", [str(y) for y,_ in by_hour], [c for _,c in by_hour],
                 os.path.join(OUT_DIR,"raw_naver_by_hour.png"), rotate=45, limit=48)

    md = []
    md.append("# 수집 데이터 집계")
    md.append("")
    md.append(f"- 업데이트 시각(KST): {now.strftime('%Y-%m-%d %H:%M:%S')}")
    md.append("")
    md.append("## 핵심 지표")
    md.append(f"- 총 수집 건수: **{total:,}**")
    md.append(f"- 저자 수: **{authors:,}**")
    md.append(f"- 출판사 수: **{publishers:,}**")
    md.append("")
    md.append("## 차트")
    md.append("")
    md.append("![Totals](raw_naver_totals.png)")
    md.append("")
    md.append("![Year](raw_naver_by_year.png)")
    md.append("")
    md.append("![Month](raw_naver_by_month.png)")
    md.append("")
    md.append("![Day](raw_naver_by_day.png)")
    md.append("")
    md.append("![Hour](raw_naver_by_hour.png)")

    with open(OUTPUT_MD,"w",encoding="utf-8") as f:
        f.write("\n".join(md))

if __name__ == "__main__":
    main()
