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

# User requested distinct colors
COLOR_BOOKS = "#1f77b4"       # blue
COLOR_AUTHORS = "#2ca02c"     # green
COLOR_PUBLISHERS = "#ff7f0e"  # orange

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
                    (b.get_x() + b.get_width()/2, h),
                    xytext=(0, y_offset),
                    textcoords="offset points",
                    ha="center", va="bottom",
                    fontsize=fontsize)

def plot_series_bar(title, x_labels, values, path, color, rotate=0, limit=None):
    if limit and len(x_labels) > limit:
        x_labels = x_labels[-limit:]
        values = values[-limit:]
    fig = plt.figure(figsize=(12, 5))
    ax = fig.add_subplot(111)
    x = list(range(len(x_labels)))
    bars = ax.bar(x, values, color=color)
    ax.set_title(title)
    ax.set_ylabel("Count")
    ax.set_xticks(x)
    ax.set_xticklabels(x_labels, rotation=rotate, ha="right" if rotate else "center")
    annotate_bars(ax, bars, fontsize=7, y_offset=2)
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)

def main():
    now = datetime.now(KST)
    os.makedirs(OUT_DIR, exist_ok=True)

    base_isbn = "isbn IS NOT NULL AND length(trim(isbn)) > 0"
    base_pub = "publisher IS NOT NULL AND length(trim(publisher)) > 0"

    # Totals (distinct)
    total_books = q_scalar(f"SELECT uniqExact(isbn) FROM {TABLE_NAME} WHERE {base_isbn}")
    total_publishers = q_scalar(f"SELECT countDistinct(publisher) FROM {TABLE_NAME} WHERE {base_pub}")
    total_authors = q_scalar(f"""
        SELECT countDistinct(author_one)
        FROM (
            SELECT trim(author_one) AS author_one
            FROM {TABLE_NAME}
            ARRAY JOIN splitByChar('^', ifNull(author, '')) AS author_one
            WHERE length(trim(author_one)) > 0
        )
    """)

    # New inflow: first seen timestamp
    # Books
    y_books = q_rows(f"""
        WITH first_seen AS (
            SELECT isbn, min(created_at) AS first_at
            FROM {TABLE_NAME}
            WHERE {base_isbn}
            GROUP BY isbn
        )
        SELECT toYear(first_at), count() FROM first_seen GROUP BY 1 ORDER BY 1
    """)
    m_books = q_rows(f"""
        WITH first_seen AS (
            SELECT isbn, min(created_at) AS first_at
            FROM {TABLE_NAME}
            WHERE {base_isbn}
            GROUP BY isbn
        )
        SELECT toYYYYMM(first_at), count() FROM first_seen GROUP BY 1 ORDER BY 1
    """)
    d_books = q_rows(f"""
        WITH first_seen AS (
            SELECT isbn, min(created_at) AS first_at
            FROM {TABLE_NAME}
            WHERE {base_isbn}
            GROUP BY isbn
        )
        SELECT toDate(first_at), count() FROM first_seen GROUP BY 1 ORDER BY 1
    """)
    h_books = q_rows(f"""
        WITH first_seen AS (
            SELECT isbn, min(created_at) AS first_at
            FROM {TABLE_NAME}
            WHERE {base_isbn}
            GROUP BY isbn
        )
        SELECT toStartOfHour(first_at), count() FROM first_seen GROUP BY 1 ORDER BY 1
    """)

    # Publishers
    y_pubs = q_rows(f"""
        WITH first_seen AS (
            SELECT publisher, min(created_at) AS first_at
            FROM {TABLE_NAME}
            WHERE {base_pub}
            GROUP BY publisher
        )
        SELECT toYear(first_at), count() FROM first_seen GROUP BY 1 ORDER BY 1
    """)
    m_pubs = q_rows(f"""
        WITH first_seen AS (
            SELECT publisher, min(created_at) AS first_at
            FROM {TABLE_NAME}
            WHERE {base_pub}
            GROUP BY publisher
        )
        SELECT toYYYYMM(first_at), count() FROM first_seen GROUP BY 1 ORDER BY 1
    """)
    d_pubs = q_rows(f"""
        WITH first_seen AS (
            SELECT publisher, min(created_at) AS first_at
            FROM {TABLE_NAME}
            WHERE {base_pub}
            GROUP BY publisher
        )
        SELECT toDate(first_at), count() FROM first_seen GROUP BY 1 ORDER BY 1
    """)
    h_pubs = q_rows(f"""
        WITH first_seen AS (
            SELECT publisher, min(created_at) AS first_at
            FROM {TABLE_NAME}
            WHERE {base_pub}
            GROUP BY publisher
        )
        SELECT toStartOfHour(first_at), count() FROM first_seen GROUP BY 1 ORDER BY 1
    """)

    # Authors (split '^')
    y_auth = q_rows(f"""
        WITH exploded AS (
            SELECT trim(author_one) AS author_one, created_at
            FROM {TABLE_NAME}
            ARRAY JOIN splitByChar('^', ifNull(author, '')) AS author_one
            WHERE length(trim(author_one)) > 0
        ),
        first_seen AS (
            SELECT author_one, min(created_at) AS first_at
            FROM exploded
            GROUP BY author_one
        )
        SELECT toYear(first_at), count() FROM first_seen GROUP BY 1 ORDER BY 1
    """)
    m_auth = q_rows(f"""
        WITH exploded AS (
            SELECT trim(author_one) AS author_one, created_at
            FROM {TABLE_NAME}
            ARRAY JOIN splitByChar('^', ifNull(author, '')) AS author_one
            WHERE length(trim(author_one)) > 0
        ),
        first_seen AS (
            SELECT author_one, min(created_at) AS first_at
            FROM exploded
            GROUP BY author_one
        )
        SELECT toYYYYMM(first_at), count() FROM first_seen GROUP BY 1 ORDER BY 1
    """)
    d_auth = q_rows(f"""
        WITH exploded AS (
            SELECT trim(author_one) AS author_one, created_at
            FROM {TABLE_NAME}
            ARRAY JOIN splitByChar('^', ifNull(author, '')) AS author_one
            WHERE length(trim(author_one)) > 0
        ),
        first_seen AS (
            SELECT author_one, min(created_at) AS first_at
            FROM exploded
            GROUP BY author_one
        )
        SELECT toDate(first_at), count() FROM first_seen GROUP BY 1 ORDER BY 1
    """)
    h_auth = q_rows(f"""
        WITH exploded AS (
            SELECT trim(author_one) AS author_one, created_at
            FROM {TABLE_NAME}
            ARRAY JOIN splitByChar('^', ifNull(author, '')) AS author_one
            WHERE length(trim(author_one)) > 0
        ),
        first_seen AS (
            SELECT author_one, min(created_at) AS first_at
            FROM exploded
            GROUP BY author_one
        )
        SELECT toStartOfHour(first_at), count() FROM first_seen GROUP BY 1 ORDER BY 1
    """)

    # Totals chart (keep existing filename) with colored bars per metric
    fig = plt.figure(figsize=(9, 4.5))
    ax = fig.add_subplot(111)
    bars = ax.bar(
        ["Books(uniq isbn)", "Authors(uniq)", "Publishers(uniq)"],
        [total_books, total_authors, total_publishers],
        color=[COLOR_BOOKS, COLOR_AUTHORS, COLOR_PUBLISHERS]
    )
    ax.set_title("Total Overview")
    ax.set_ylabel("Count")
    annotate_bars(ax, bars, fontsize=9, y_offset=3)
    fig.tight_layout()
    fig.savefig(os.path.join(OUT_DIR, "raw_naver_totals.png"), dpi=150)
    plt.close(fig)

    def fmt_hour(v):
        return datetime.fromisoformat(str(v)).strftime("%Y-%m-%d %H")

    # Split charts (Books keep original filenames)
    # Year
    if y_books:
        plot_series_bar("Yearly (New Books)", [str(k) for k,_ in y_books], [v for _,v in y_books],
                        os.path.join(OUT_DIR, "raw_naver_by_year.png"), color=COLOR_BOOKS)
    if y_auth:
        plot_series_bar("Yearly (New Authors)", [str(k) for k,_ in y_auth], [v for _,v in y_auth],
                        os.path.join(OUT_DIR, "raw_naver_by_year_authors.png"), color=COLOR_AUTHORS)
    if y_pubs:
        plot_series_bar("Yearly (New Publishers)", [str(k) for k,_ in y_pubs], [v for _,v in y_pubs],
                        os.path.join(OUT_DIR, "raw_naver_by_year_publishers.png"), color=COLOR_PUBLISHERS)

    # Month (last 24)
    if m_books:
        plot_series_bar("Monthly (New Books, last 24)", [str(k) for k,_ in m_books], [v for _,v in m_books],
                        os.path.join(OUT_DIR, "raw_naver_by_month.png"), rotate=45, limit=24, color=COLOR_BOOKS)
    if m_auth:
        plot_series_bar("Monthly (New Authors, last 24)", [str(k) for k,_ in m_auth], [v for _,v in m_auth],
                        os.path.join(OUT_DIR, "raw_naver_by_month_authors.png"), rotate=45, limit=24, color=COLOR_AUTHORS)
    if m_pubs:
        plot_series_bar("Monthly (New Publishers, last 24)", [str(k) for k,_ in m_pubs], [v for _,v in m_pubs],
                        os.path.join(OUT_DIR, "raw_naver_by_month_publishers.png"), rotate=45, limit=24, color=COLOR_PUBLISHERS)

    # Day (last 60)
    if d_books:
        plot_series_bar("Daily (New Books, last 60)", [str(k) for k,_ in d_books], [v for _,v in d_books],
                        os.path.join(OUT_DIR, "raw_naver_by_day.png"), rotate=45, limit=60, color=COLOR_BOOKS)
    if d_auth:
        plot_series_bar("Daily (New Authors, last 60)", [str(k) for k,_ in d_auth], [v for _,v in d_auth],
                        os.path.join(OUT_DIR, "raw_naver_by_day_authors.png"), rotate=45, limit=60, color=COLOR_AUTHORS)
    if d_pubs:
        plot_series_bar("Daily (New Publishers, last 60)", [str(k) for k,_ in d_pubs], [v for _,v in d_pubs],
                        os.path.join(OUT_DIR, "raw_naver_by_day_publishers.png"), rotate=45, limit=60, color=COLOR_PUBLISHERS)

    # Hour (last 48)
    if h_books:
        plot_series_bar("Hourly (New Books, last 48)", [fmt_hour(k) for k,_ in h_books], [v for _,v in h_books],
                        os.path.join(OUT_DIR, "raw_naver_by_hour.png"), rotate=45, limit=48, color=COLOR_BOOKS)
    if h_auth:
        plot_series_bar("Hourly (New Authors, last 48)", [fmt_hour(k) for k,_ in h_auth], [v for _,v in h_auth],
                        os.path.join(OUT_DIR, "raw_naver_by_hour_authors.png"), rotate=45, limit=48, color=COLOR_AUTHORS)
    if h_pubs:
        plot_series_bar("Hourly (New Publishers, last 48)", [fmt_hour(k) for k,_ in h_pubs], [v for _,v in h_pubs],
                        os.path.join(OUT_DIR, "raw_naver_by_hour_publishers.png"), rotate=45, limit=48, color=COLOR_PUBLISHERS)

    # Markdown with big sections per metric: Books -> Authors -> Publishers
    md = []
    md.append("# 수집 데이터 집계")
    md.append("")
    md.append(f"- 업데이트 시각(KST): {now.strftime('%Y-%m-%d %H:%M:%S')}")
    md.append("")
    md.append("## 전체")
    md.append("")
    md.append(f"- 총 고유 ISBN 수: **{total_books:,}**")
    md.append(f"- 저자 수: **{total_authors:,}**")
    md.append(f"- 출판사 수: **{total_publishers:,}**")
    md.append("")
    md.append("![Totals](raw_naver_totals.png)")
    md.append("")
    md.append("## Books")
    md.append("")
    md.append("### 연별 신규 유입")
    md.append("![Books Year](raw_naver_by_year.png)")
    md.append("")
    md.append("### 월별 신규 유입 (최근 24개월)")
    md.append("![Books Month](raw_naver_by_month.png)")
    md.append("")
    md.append("### 일별 신규 유입 (최근 60일)")
    md.append("![Books Day](raw_naver_by_day.png)")
    md.append("")
    md.append("### 시간별 신규 유입 (최근 48시간)")
    md.append("![Books Hour](raw_naver_by_hour.png)")
    md.append("")
    md.append("## Authors")
    md.append("")
    md.append("### 연별 신규 유입")
    md.append("![Authors Year](raw_naver_by_year_authors.png)")
    md.append("")
    md.append("### 월별 신규 유입 (최근 24개월)")
    md.append("![Authors Month](raw_naver_by_month_authors.png)")
    md.append("")
    md.append("### 일별 신규 유입 (최근 60일)")
    md.append("![Authors Day](raw_naver_by_day_authors.png)")
    md.append("")
    md.append("### 시간별 신규 유입 (최근 48시간)")
    md.append("![Authors Hour](raw_naver_by_hour_authors.png)")
    md.append("")
    md.append("## Publishers")
    md.append("")
    md.append("### 연별 신규 유입")
    md.append("![Publishers Year](raw_naver_by_year_publishers.png)")
    md.append("")
    md.append("### 월별 신규 유입 (최근 24개월)")
    md.append("![Publishers Month](raw_naver_by_month_publishers.png)")
    md.append("")
    md.append("### 일별 신규 유입 (최근 60일)")
    md.append("![Publishers Day](raw_naver_by_day_publishers.png)")
    md.append("")
    md.append("### 시간별 신규 유입 (최근 48시간)")
    md.append("![Publishers Hour](raw_naver_by_hour_publishers.png)")
    md.append("")
    md.append("> 시계열은 각 항목(ISBN/저자/출판사)의 '최초 등장 시각' 기준 신규 유입을 집계합니다.")
    md.append("")

    with open(OUTPUT_MD, "w", encoding="utf-8") as f:
        f.write("\n".join(md))

if __name__ == "__main__":
    main()
