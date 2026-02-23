#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Batch: raw_naver stats (GitHub Actions)

What it does
- Connects to ClickHouse
- Computes totals (distinct ISBN / Authors / Publishers)
- Computes "new inflow" time series by first_seen(min(created_at)) for each entity
- Fills missing buckets (year/month/day/hour) with zeros so charts don't skip gaps
- Generates PNG charts + Markdown report under ./stats

Env vars required
- CH_HOST, CH_PORT, CH_USER, CH_PASSWORD, CH_DATABASE
"""

import os
import math
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

def _simplify_xticklabels(x_labels, max_labels=24):
    """
    Return a label list of same length where most labels are blank,
    keeping about max_labels labels evenly spaced.
    """
    n = len(x_labels)
    if n <= max_labels:
        return x_labels
    step = max(1, int(math.ceil(n / max_labels)))
    out = []
    for i, lab in enumerate(x_labels):
        out.append(lab if (i % step == 0 or i == n - 1) else "")
    return out

def plot_series_bar(title, x_labels, values, path, color, rotate=0, max_labels=24):
    """
    Draw a simple bar chart.
    - Always plots full series.
    - If many points, x-axis labels are simplified (blanked) to improve readability.
    """
    fig = plt.figure(figsize=(12, 5))
    ax = fig.add_subplot(111)
    x = list(range(len(x_labels)))
    bars = ax.bar(x, values, color=color)
    ax.set_title(title)
    ax.set_ylabel("Count")

    # Simplify labels for readability
    display_labels = _simplify_xticklabels(x_labels, max_labels=max_labels)
    ax.set_xticks(x)
    ax.set_xticklabels(display_labels, rotation=rotate, ha="right" if rotate else "center")

    # For very long series, avoid cluttering the bar annotations
    if len(values) <= 120:
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

    # ---------- New inflow series (first seen timestamp) ----------
    # Helper notes:
    # - Build first_seen per entity
    # - Compute min/max bounds
    # - Generate full timeline with numbers()
    # - LEFT JOIN counts, fill missing with 0

    # Books
    y_books = q_rows(f"""
        WITH first_seen AS (
            SELECT isbn, min(created_at) AS first_at
            FROM {TABLE_NAME}
            WHERE {base_isbn}
            GROUP BY isbn
        ),
        bounds AS (
            SELECT toStartOfYear(min(first_at)) AS min_y, toStartOfYear(max(first_at)) AS max_y
            FROM first_seen
        ),
        counts AS (
            SELECT toYear(first_at) AS y, count() AS c
            FROM first_seen
            GROUP BY y
        )
        SELECT
            toYear(addYears(b.min_y, n.number)) AS y,
            ifNull(cnt.c, 0) AS c
        FROM bounds b
        CROSS JOIN numbers(dateDiff('year', b.min_y, b.max_y) + 1) AS n
        LEFT JOIN counts cnt ON cnt.y = y
        ORDER BY y
    """)
    m_books = q_rows(f"""
        WITH first_seen AS (
            SELECT isbn, min(created_at) AS first_at
            FROM {TABLE_NAME}
            WHERE {base_isbn}
            GROUP BY isbn
        ),
        bounds AS (
            SELECT toStartOfMonth(min(first_at)) AS min_m, toStartOfMonth(max(first_at)) AS max_m
            FROM first_seen
        ),
        counts AS (
            SELECT toStartOfMonth(first_at) AS m, count() AS c
            FROM first_seen
            GROUP BY m
        ),
        timeline AS (
            SELECT addMonths(b.min_m, n.number) AS m
            FROM bounds b
            CROSS JOIN numbers(dateDiff('month', b.min_m, b.max_m) + 1) AS n
        )
        SELECT
            toYYYYMM(t.m) AS yyyymm,
            ifNull(cnt.c, 0) AS c
        FROM timeline t
        LEFT JOIN counts cnt ON cnt.m = t.m
        ORDER BY yyyymm
    """)
    d_books = q_rows(f"""
        WITH first_seen AS (
            SELECT isbn, min(created_at) AS first_at
            FROM {TABLE_NAME}
            WHERE {base_isbn}
            GROUP BY isbn
        ),
        bounds AS (
            SELECT toDate(min(first_at)) AS min_d, toDate(max(first_at)) AS max_d
            FROM first_seen
        ),
        counts AS (
            SELECT toDate(first_at) AS d, count() AS c
            FROM first_seen
            GROUP BY d
        ),
        timeline AS (
            SELECT addDays(b.min_d, n.number) AS d
            FROM bounds b
            CROSS JOIN numbers(dateDiff('day', b.min_d, b.max_d) + 1) AS n
        )
        SELECT
            t.d AS d,
            ifNull(cnt.c, 0) AS c
        FROM timeline t
        LEFT JOIN counts cnt ON cnt.d = t.d
        ORDER BY d
    """)
    h_books = q_rows(f"""
        WITH first_seen AS (
            SELECT isbn, min(created_at) AS first_at
            FROM {TABLE_NAME}
            WHERE {base_isbn}
            GROUP BY isbn
        ),
        bounds AS (
            SELECT toStartOfHour(min(first_at)) AS min_t, toStartOfHour(max(first_at)) AS max_t
            FROM first_seen
        ),
        counts AS (
            SELECT toStartOfHour(first_at) AS t, count() AS c
            FROM first_seen
            GROUP BY t
        ),
        timeline AS (
            SELECT addHours(b.min_t, n.number) AS t
            FROM bounds b
            CROSS JOIN numbers(dateDiff('hour', b.min_t, b.max_t) + 1) AS n
        )
        SELECT
            tl.t AS t,
            ifNull(cnt.c, 0) AS c
        FROM timeline tl
        LEFT JOIN counts cnt ON cnt.t = tl.t
        ORDER BY t
    """)

    # Publishers
    y_pubs = q_rows(f"""
        WITH first_seen AS (
            SELECT publisher, min(created_at) AS first_at
            FROM {TABLE_NAME}
            WHERE {base_pub}
            GROUP BY publisher
        ),
        bounds AS (
            SELECT toStartOfYear(min(first_at)) AS min_y, toStartOfYear(max(first_at)) AS max_y
            FROM first_seen
        ),
        counts AS (
            SELECT toYear(first_at) AS y, count() AS c
            FROM first_seen
            GROUP BY y
        )
        SELECT
            toYear(addYears(b.min_y, n.number)) AS y,
            ifNull(cnt.c, 0) AS c
        FROM bounds b
        CROSS JOIN numbers(dateDiff('year', b.min_y, b.max_y) + 1) AS n
        LEFT JOIN counts cnt ON cnt.y = y
        ORDER BY y
    """)
    m_pubs = q_rows(f"""
        WITH first_seen AS (
            SELECT publisher, min(created_at) AS first_at
            FROM {TABLE_NAME}
            WHERE {base_pub}
            GROUP BY publisher
        ),
        bounds AS (
            SELECT toStartOfMonth(min(first_at)) AS min_m, toStartOfMonth(max(first_at)) AS max_m
            FROM first_seen
        ),
        counts AS (
            SELECT toStartOfMonth(first_at) AS m, count() AS c
            FROM first_seen
            GROUP BY m
        ),
        timeline AS (
            SELECT addMonths(b.min_m, n.number) AS m
            FROM bounds b
            CROSS JOIN numbers(dateDiff('month', b.min_m, b.max_m) + 1) AS n
        )
        SELECT
            toYYYYMM(t.m) AS yyyymm,
            ifNull(cnt.c, 0) AS c
        FROM timeline t
        LEFT JOIN counts cnt ON cnt.m = t.m
        ORDER BY yyyymm
    """)
    d_pubs = q_rows(f"""
        WITH first_seen AS (
            SELECT publisher, min(created_at) AS first_at
            FROM {TABLE_NAME}
            WHERE {base_pub}
            GROUP BY publisher
        ),
        bounds AS (
            SELECT toDate(min(first_at)) AS min_d, toDate(max(first_at)) AS max_d
            FROM first_seen
        ),
        counts AS (
            SELECT toDate(first_at) AS d, count() AS c
            FROM first_seen
            GROUP BY d
        ),
        timeline AS (
            SELECT addDays(b.min_d, n.number) AS d
            FROM bounds b
            CROSS JOIN numbers(dateDiff('day', b.min_d, b.max_d) + 1) AS n
        )
        SELECT
            t.d AS d,
            ifNull(cnt.c, 0) AS c
        FROM timeline t
        LEFT JOIN counts cnt ON cnt.d = t.d
        ORDER BY d
    """)
    h_pubs = q_rows(f"""
        WITH first_seen AS (
            SELECT publisher, min(created_at) AS first_at
            FROM {TABLE_NAME}
            WHERE {base_pub}
            GROUP BY publisher
        ),
        bounds AS (
            SELECT toStartOfHour(min(first_at)) AS min_t, toStartOfHour(max(first_at)) AS max_t
            FROM first_seen
        ),
        counts AS (
            SELECT toStartOfHour(first_at) AS t, count() AS c
            FROM first_seen
            GROUP BY t
        ),
        timeline AS (
            SELECT addHours(b.min_t, n.number) AS t
            FROM bounds b
            CROSS JOIN numbers(dateDiff('hour', b.min_t, b.max_t) + 1) AS n
        )
        SELECT
            tl.t AS t,
            ifNull(cnt.c, 0) AS c
        FROM timeline tl
        LEFT JOIN counts cnt ON cnt.t = tl.t
        ORDER BY t
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
        ),
        bounds AS (
            SELECT toStartOfYear(min(first_at)) AS min_y, toStartOfYear(max(first_at)) AS max_y
            FROM first_seen
        ),
        counts AS (
            SELECT toYear(first_at) AS y, count() AS c
            FROM first_seen
            GROUP BY y
        )
        SELECT
            toYear(addYears(b.min_y, n.number)) AS y,
            ifNull(cnt.c, 0) AS c
        FROM bounds b
        CROSS JOIN numbers(dateDiff('year', b.min_y, b.max_y) + 1) AS n
        LEFT JOIN counts cnt ON cnt.y = y
        ORDER BY y
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
        ),
        bounds AS (
            SELECT toStartOfMonth(min(first_at)) AS min_m, toStartOfMonth(max(first_at)) AS max_m
            FROM first_seen
        ),
        counts AS (
            SELECT toStartOfMonth(first_at) AS m, count() AS c
            FROM first_seen
            GROUP BY m
        ),
        timeline AS (
            SELECT addMonths(b.min_m, n.number) AS m
            FROM bounds b
            CROSS JOIN numbers(dateDiff('month', b.min_m, b.max_m) + 1) AS n
        )
        SELECT
            toYYYYMM(t.m) AS yyyymm,
            ifNull(cnt.c, 0) AS c
        FROM timeline t
        LEFT JOIN counts cnt ON cnt.m = t.m
        ORDER BY yyyymm
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
        ),
        bounds AS (
            SELECT toDate(min(first_at)) AS min_d, toDate(max(first_at)) AS max_d
            FROM first_seen
        ),
        counts AS (
            SELECT toDate(first_at) AS d, count() AS c
            FROM first_seen
            GROUP BY d
        ),
        timeline AS (
            SELECT addDays(b.min_d, n.number) AS d
            FROM bounds b
            CROSS JOIN numbers(dateDiff('day', b.min_d, b.max_d) + 1) AS n
        )
        SELECT
            t.d AS d,
            ifNull(cnt.c, 0) AS c
        FROM timeline t
        LEFT JOIN counts cnt ON cnt.d = t.d
        ORDER BY d
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
        ),
        bounds AS (
            SELECT toStartOfHour(min(first_at)) AS min_t, toStartOfHour(max(first_at)) AS max_t
            FROM first_seen
        ),
        counts AS (
            SELECT toStartOfHour(first_at) AS t, count() AS c
            FROM first_seen
            GROUP BY t
        ),
        timeline AS (
            SELECT addHours(b.min_t, n.number) AS t
            FROM bounds b
            CROSS JOIN numbers(dateDiff('hour', b.min_t, b.max_t) + 1) AS n
        )
        SELECT
            tl.t AS t,
            ifNull(cnt.c, 0) AS c
        FROM timeline tl
        LEFT JOIN counts cnt ON cnt.t = tl.t
        ORDER BY t
    """)

    # ---------- Totals chart ----------
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
        # ClickHouse DateTime/DateTime64 renders to string; keep hour precision
        return datetime.fromisoformat(str(v)).strftime("%Y-%m-%d %H")

    # ---------- Split charts (full period) ----------
    # Year
    if y_books:
        plot_series_bar("Yearly (New Books)", [str(k) for k, _ in y_books], [v for _, v in y_books],
                        os.path.join(OUT_DIR, "raw_naver_by_year.png"), color=COLOR_BOOKS, max_labels=24)
    if y_auth:
        plot_series_bar("Yearly (New Authors)", [str(k) for k, _ in y_auth], [v for _, v in y_auth],
                        os.path.join(OUT_DIR, "raw_naver_by_year_authors.png"), color=COLOR_AUTHORS, max_labels=24)
    if y_pubs:
        plot_series_bar("Yearly (New Publishers)", [str(k) for k, _ in y_pubs], [v for _, v in y_pubs],
                        os.path.join(OUT_DIR, "raw_naver_by_year_publishers.png"), color=COLOR_PUBLISHERS, max_labels=24)

    # Month (full)
    if m_books:
        plot_series_bar("Monthly (New Books)", [str(k) for k, _ in m_books], [v for _, v in m_books],
                        os.path.join(OUT_DIR, "raw_naver_by_month.png"), rotate=45, color=COLOR_BOOKS, max_labels=24)
    if m_auth:
        plot_series_bar("Monthly (New Authors)", [str(k) for k, _ in m_auth], [v for _, v in m_auth],
                        os.path.join(OUT_DIR, "raw_naver_by_month_authors.png"), rotate=45, color=COLOR_AUTHORS, max_labels=24)
    if m_pubs:
        plot_series_bar("Monthly (New Publishers)", [str(k) for k, _ in m_pubs], [v for _, v in m_pubs],
                        os.path.join(OUT_DIR, "raw_naver_by_month_publishers.png"), rotate=45, color=COLOR_PUBLISHERS, max_labels=24)

    # Day (full)
    if d_books:
        plot_series_bar("Daily (New Books)", [str(k) for k, _ in d_books], [v for _, v in d_books],
                        os.path.join(OUT_DIR, "raw_naver_by_day.png"), rotate=45, color=COLOR_BOOKS, max_labels=24)
    if d_auth:
        plot_series_bar("Daily (New Authors)", [str(k) for k, _ in d_auth], [v for _, v in d_auth],
                        os.path.join(OUT_DIR, "raw_naver_by_day_authors.png"), rotate=45, color=COLOR_AUTHORS, max_labels=24)
    if d_pubs:
        plot_series_bar("Daily (New Publishers)", [str(k) for k, _ in d_pubs], [v for _, v in d_pubs],
                        os.path.join(OUT_DIR, "raw_naver_by_day_publishers.png"), rotate=45, color=COLOR_PUBLISHERS, max_labels=24)

    # Hour (full)
    if h_books:
        plot_series_bar("Hourly (New Books)", [fmt_hour(k) for k, _ in h_books], [v for _, v in h_books],
                        os.path.join(OUT_DIR, "raw_naver_by_hour.png"), rotate=45, color=COLOR_BOOKS, max_labels=24)
    if h_auth:
        plot_series_bar("Hourly (New Authors)", [fmt_hour(k) for k, _ in h_auth], [v for _, v in h_auth],
                        os.path.join(OUT_DIR, "raw_naver_by_hour_authors.png"), rotate=45, color=COLOR_AUTHORS, max_labels=24)
    if h_pubs:
        plot_series_bar("Hourly (New Publishers)", [fmt_hour(k) for k, _ in h_pubs], [v for _, v in h_pubs],
                        os.path.join(OUT_DIR, "raw_naver_by_hour_publishers.png"), rotate=45, color=COLOR_PUBLISHERS, max_labels=24)

    # ---------- Markdown report ----------
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
    md.append("### 월별 신규 유입")
    md.append("![Books Month](raw_naver_by_month.png)")
    md.append("")
    md.append("### 일별 신규 유입")
    md.append("![Books Day](raw_naver_by_day.png)")
    md.append("")
    md.append("### 시간별 신규 유입")
    md.append("![Books Hour](raw_naver_by_hour.png)")
    md.append("")
    md.append("## Authors")
    md.append("")
    md.append("### 연별 신규 유입")
    md.append("![Authors Year](raw_naver_by_year_authors.png)")
    md.append("")
    md.append("### 월별 신규 유입")
    md.append("![Authors Month](raw_naver_by_month_authors.png)")
    md.append("")
    md.append("### 일별 신규 유입")
    md.append("![Authors Day](raw_naver_by_day_authors.png)")
    md.append("")
    md.append("### 시간별 신규 유입")
    md.append("![Authors Hour](raw_naver_by_hour_authors.png)")
    md.append("")
    md.append("## Publishers")
    md.append("")
    md.append("### 연별 신규 유입")
    md.append("![Publishers Year](raw_naver_by_year_publishers.png)")
    md.append("")
    md.append("### 월별 신규 유입")
    md.append("![Publishers Month](raw_naver_by_month_publishers.png)")
    md.append("")
    md.append("### 일별 신규 유입")
    md.append("![Publishers Day](raw_naver_by_day_publishers.png)")
    md.append("")
    md.append("### 시간별 신규 유입")
    md.append("![Publishers Hour](raw_naver_by_hour_publishers.png)")
    md.append("")
    md.append("> 시계열은 각 항목(ISBN/저자/출판사)의 '최초 등장 시각' 기준 신규 유입을 집계합니다. (빈 구간은 0으로 채움)")
    md.append("")

    with open(OUTPUT_MD, "w", encoding="utf-8") as f:
        f.write("\n".join(md))

if __name__ == "__main__":
    main()
