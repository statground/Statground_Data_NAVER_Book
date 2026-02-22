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

def _fmt_int(v) -> str:
    try:
        return f"{int(v):,}"
    except Exception:
        return str(v)

def annotate_bars(ax, bars, fontsize=8, y_offset=3):
    for b in bars:
        h = b.get_height()
        ax.annotate(_fmt_int(h),
                    (b.get_x()+b.get_width()/2, h),
                    xytext=(0, y_offset),
                    textcoords="offset points",
                    ha="center", va="bottom",
                    fontsize=fontsize)

def plot_bar(title, labels, values, path, rotate=0, limit=None):
    if limit and len(labels) > limit:
        labels = labels[-limit:]
        values = values[-limit:]
    fig = plt.figure(figsize=(12,5))
    ax = fig.add_subplot(111)
    x = range(len(labels))
    bars = ax.bar(x, values)
    ax.set_title(title)
    ax.set_ylabel("Count")
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=rotate, ha="right" if rotate else "center")
    annotate_bars(ax, bars, fontsize=7, y_offset=2)
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)

def main():
    now = datetime.now(KST)
    os.makedirs(OUT_DIR, exist_ok=True)

    # Overall totals (keep: books, authors, publishers)
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

    # Time series: books only
    by_year = q_rows(f"SELECT toYear(created_at), count() FROM {TABLE_NAME} GROUP BY 1 ORDER BY 1")
    by_month = q_rows(f"SELECT toYYYYMM(created_at), count() FROM {TABLE_NAME} GROUP BY 1 ORDER BY 1")
    by_day = q_rows(f"SELECT toDate(created_at), count() FROM {TABLE_NAME} GROUP BY 1 ORDER BY 1")
    by_hour = q_rows(f"SELECT toStartOfHour(created_at), count() FROM {TABLE_NAME} GROUP BY 1 ORDER BY 1")

    # Charts (filenames kept)
    # totals
    fig = plt.figure(figsize=(9, 4.5))
    ax = fig.add_subplot(111)
    bars = ax.bar(["Books(rows)", "Authors", "Publishers"], [total_books, total_authors, total_publishers])
    ax.set_title("Total Overview")
    ax.set_ylabel("Count")
    annotate_bars(ax, bars, fontsize=9, y_offset=3)
    fig.tight_layout()
    fig.savefig(os.path.join(OUT_DIR, "raw_naver_totals.png"), dpi=150)
    plt.close(fig)

    if by_year:
        plot_bar("Yearly (Books)",
                 [str(y) for y, _ in by_year],
                 [c for _, c in by_year],
                 os.path.join(OUT_DIR, "raw_naver_by_year.png"))

    if by_month:
        plot_bar("Monthly (Books, last 24)",
                 [str(y) for y, _ in by_month],
                 [c for _, c in by_month],
                 os.path.join(OUT_DIR, "raw_naver_by_month.png"),
                 rotate=45, limit=24)

    if by_day:
        plot_bar("Daily (Books, last 60)",
                 [str(y) for y, _ in by_day],
                 [c for _, c in by_day],
                 os.path.join(OUT_DIR, "raw_naver_by_day.png"),
                 rotate=45, limit=60)

    if by_hour:
        hours = [datetime.fromisoformat(str(h)).strftime("%Y-%m-%d %H") for h, _ in by_hour]
        vals = [c for _, c in by_hour]
        plot_bar("Hourly (Books, last 48)",
                 hours,
                 vals,
                 os.path.join(OUT_DIR, "raw_naver_by_hour.png"),
                 rotate=45, limit=48)

    # Markdown (no engine/db/table wording; no 'naver' wording in text; filenames kept)
    md = []
    md.append("# 수집 데이터 집계")
    md.append("")
    md.append(f"- 업데이트 시각(KST): {now.strftime('%Y-%m-%d %H:%M:%S')}")
    md.append("")
    md.append("## 핵심 지표")
    md.append(f"- 총 수집 건수: **{total_books:,}**")
    md.append(f"- 저자 수: **{total_authors:,}**")
    md.append(f"- 출판사 수: **{total_publishers:,}**")
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
    md.append("")

    with open(OUTPUT_MD, "w", encoding="utf-8") as f:
        f.write("\n".join(md))

if __name__ == "__main__":
    main()
