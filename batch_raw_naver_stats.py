#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Batch: raw_naver stats (GitHub Actions)

- ClickHouse raw_naver 통계 리포트 생성
- '최초 등장 시각(min(created_at))' 기준 신규 유입 시계열 산출
- 연/월/일/시간 버킷을 전체 구간으로 생성하고, 누락 버킷은 0으로 채움
- 차트 x축 라벨은 너무 길면 간소화(일정 간격만 표시)
- ./stats 아래 PNG + Markdown 생성

Env vars:
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
    n = len(x_labels)
    if n <= max_labels:
        return x_labels
    step = max(1, int(math.ceil(n / max_labels)))
    return [lab if (i % step == 0 or i == n - 1) else "" for i, lab in enumerate(x_labels)]

def plot_series_bar(title, x_labels, values, path, color, rotate=0, max_labels=24):
    fig = plt.figure(figsize=(12, 5))
    ax = fig.add_subplot(111)
    x = list(range(len(x_labels)))
    bars = ax.bar(x, values, color=color)
    ax.set_title(title)
    ax.set_ylabel("Count")

    display_labels = _simplify_xticklabels(x_labels, max_labels=max_labels)
    ax.set_xticks(x)
    ax.set_xticklabels(display_labels, rotation=rotate, ha="right" if rotate else "center")

    # bar annotation은 너무 많으면 지저분해서 적당히 제한
    if len(values) <= 120:
        annotate_bars(ax, bars, fontsize=7, y_offset=2)

    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)

def cumulative(values):
    total = 0
    out = []
    for v in values:
        total += int(v)
        out.append(total)
    return out

def plot_series_with_cumulative(title, x_labels, values, path, color, rotate=0, max_labels=24):
    '''
    Draw New Inflow (bar) + Cumulative (line, secondary y-axis).
    - values: new inflow series
    - cumulative is computed in python (running sum)
    '''
    cum_values = cumulative(values)

    fig = plt.figure(figsize=(12, 5))
    ax1 = fig.add_subplot(111)

    x = list(range(len(x_labels)))

    # New inflow (bar)
    bars = ax1.bar(x, values, color=color, alpha=0.6)
    ax1.set_title(title)
    ax1.set_ylabel("New Inflow")

    # Cumulative (line) on secondary axis
    ax2 = ax1.twinx()
    ax2.plot(x, cum_values, color=color, linewidth=2)
    ax2.set_ylabel("Cumulative")

    display_labels = _simplify_xticklabels(x_labels, max_labels=max_labels)
    ax1.set_xticks(x)
    ax1.set_xticklabels(display_labels, rotation=rotate, ha="right" if rotate else "center")

    # bar annotation은 너무 많으면 지저분해서 적당히 제한
    if len(values) <= 120:
        annotate_bars(ax1, bars, fontsize=7, y_offset=2)

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

    # =======================
    # New inflow series (0 fill, fast)
    #
    # 핵심:
    # - system.numbers(무한 테이블) 사용 금지 → 느려지고 비용 큼
    # - ARRAY JOIN range(N) 사용 (N이 정확히 정해짐) → 빠르고 안정적
    # - first_seen 비어있으면 params가 0 rows가 되도록 HAVING count()>0 처리
    # =======================

    # Books
    y_books = q_rows(f"""
        WITH first_seen AS (
            SELECT isbn, min(created_at) AS first_at
            FROM {TABLE_NAME}
            WHERE {base_isbn}
            GROUP BY isbn
        ),
        params AS (
            SELECT
                toStartOfYear(min(first_at)) AS min_y,
                dateDiff('year', toStartOfYear(min(first_at)), toStartOfYear(max(first_at))) AS diff_y
            FROM first_seen
            HAVING count() > 0
        ),
        counts AS (
            SELECT toYear(first_at) AS y, count() AS c
            FROM first_seen
            GROUP BY y
        ),
        timeline AS (
            SELECT toYear(addYears(p.min_y, n)) AS y
            FROM params p
            ARRAY JOIN range(p.diff_y + 1) AS n
        )
        SELECT t.y AS y, ifNull(c.c, 0) AS c
        FROM timeline t
        LEFT JOIN counts c ON c.y = t.y
        ORDER BY y
    """)

    m_books = q_rows(f"""
        WITH first_seen AS (
            SELECT isbn, min(created_at) AS first_at
            FROM {TABLE_NAME}
            WHERE {base_isbn}
            GROUP BY isbn
        ),
        params AS (
            SELECT
                toStartOfMonth(min(first_at)) AS min_m,
                dateDiff('month', toStartOfMonth(min(first_at)), toStartOfMonth(max(first_at))) AS diff_m
            FROM first_seen
            HAVING count() > 0
        ),
        counts AS (
            SELECT toStartOfMonth(first_at) AS m, count() AS c
            FROM first_seen
            GROUP BY m
        ),
        timeline AS (
            SELECT addMonths(p.min_m, n) AS m
            FROM params p
            ARRAY JOIN range(p.diff_m + 1) AS n
        )
        SELECT toYYYYMM(t.m) AS yyyymm, ifNull(c.c, 0) AS c
        FROM timeline t
        LEFT JOIN counts c ON c.m = t.m
        ORDER BY yyyymm
    """)

    d_books = q_rows(f"""
        WITH first_seen AS (
            SELECT isbn, min(created_at) AS first_at
            FROM {TABLE_NAME}
            WHERE {base_isbn}
            GROUP BY isbn
        ),
        params AS (
            SELECT
                toDate(min(first_at)) AS min_d,
                dateDiff('day', toDate(min(first_at)), toDate(max(first_at))) AS diff_d
            FROM first_seen
            HAVING count() > 0
        ),
        counts AS (
            SELECT toDate(first_at) AS d, count() AS c
            FROM first_seen
            GROUP BY d
        ),
        timeline AS (
            SELECT addDays(p.min_d, n) AS d
            FROM params p
            ARRAY JOIN range(p.diff_d + 1) AS n
        )
        SELECT t.d AS d, ifNull(c.c, 0) AS c
        FROM timeline t
        LEFT JOIN counts c ON c.d = t.d
        ORDER BY d
    """)

    h_books = q_rows(f"""
        WITH first_seen AS (
            SELECT isbn, min(created_at) AS first_at
            FROM {TABLE_NAME}
            WHERE {base_isbn}
            GROUP BY isbn
        ),
        params AS (
            SELECT
                toStartOfHour(min(first_at)) AS min_t,
                dateDiff('hour', toStartOfHour(min(first_at)), toStartOfHour(max(first_at))) AS diff_h
            FROM first_seen
            HAVING count() > 0
        ),
        counts AS (
            SELECT toStartOfHour(first_at) AS t, count() AS c
            FROM first_seen
            GROUP BY t
        ),
        timeline AS (
            SELECT addHours(p.min_t, n) AS t
            FROM params p
            ARRAY JOIN range(p.diff_h + 1) AS n
        )
        SELECT t.t AS t, ifNull(c.c, 0) AS c
        FROM timeline t
        LEFT JOIN counts c ON c.t = t.t
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
        params AS (
            SELECT
                toStartOfYear(min(first_at)) AS min_y,
                dateDiff('year', toStartOfYear(min(first_at)), toStartOfYear(max(first_at))) AS diff_y
            FROM first_seen
            HAVING count() > 0
        ),
        counts AS (
            SELECT toYear(first_at) AS y, count() AS c
            FROM first_seen
            GROUP BY y
        ),
        timeline AS (
            SELECT toYear(addYears(p.min_y, n)) AS y
            FROM params p
            ARRAY JOIN range(p.diff_y + 1) AS n
        )
        SELECT t.y AS y, ifNull(c.c, 0) AS c
        FROM timeline t
        LEFT JOIN counts c ON c.y = t.y
        ORDER BY y
    """)

    m_pubs = q_rows(f"""
        WITH first_seen AS (
            SELECT publisher, min(created_at) AS first_at
            FROM {TABLE_NAME}
            WHERE {base_pub}
            GROUP BY publisher
        ),
        params AS (
            SELECT
                toStartOfMonth(min(first_at)) AS min_m,
                dateDiff('month', toStartOfMonth(min(first_at)), toStartOfMonth(max(first_at))) AS diff_m
            FROM first_seen
            HAVING count() > 0
        ),
        counts AS (
            SELECT toStartOfMonth(first_at) AS m, count() AS c
            FROM first_seen
            GROUP BY m
        ),
        timeline AS (
            SELECT addMonths(p.min_m, n) AS m
            FROM params p
            ARRAY JOIN range(p.diff_m + 1) AS n
        )
        SELECT toYYYYMM(t.m) AS yyyymm, ifNull(c.c, 0) AS c
        FROM timeline t
        LEFT JOIN counts c ON c.m = t.m
        ORDER BY yyyymm
    """)

    d_pubs = q_rows(f"""
        WITH first_seen AS (
            SELECT publisher, min(created_at) AS first_at
            FROM {TABLE_NAME}
            WHERE {base_pub}
            GROUP BY publisher
        ),
        params AS (
            SELECT
                toDate(min(first_at)) AS min_d,
                dateDiff('day', toDate(min(first_at)), toDate(max(first_at))) AS diff_d
            FROM first_seen
            HAVING count() > 0
        ),
        counts AS (
            SELECT toDate(first_at) AS d, count() AS c
            FROM first_seen
            GROUP BY d
        ),
        timeline AS (
            SELECT addDays(p.min_d, n) AS d
            FROM params p
            ARRAY JOIN range(p.diff_d + 1) AS n
        )
        SELECT t.d AS d, ifNull(c.c, 0) AS c
        FROM timeline t
        LEFT JOIN counts c ON c.d = t.d
        ORDER BY d
    """)

    h_pubs = q_rows(f"""
        WITH first_seen AS (
            SELECT publisher, min(created_at) AS first_at
            FROM {TABLE_NAME}
            WHERE {base_pub}
            GROUP BY publisher
        ),
        params AS (
            SELECT
                toStartOfHour(min(first_at)) AS min_t,
                dateDiff('hour', toStartOfHour(min(first_at)), toStartOfHour(max(first_at))) AS diff_h
            FROM first_seen
            HAVING count() > 0
        ),
        counts AS (
            SELECT toStartOfHour(first_at) AS t, count() AS c
            FROM first_seen
            GROUP BY t
        ),
        timeline AS (
            SELECT addHours(p.min_t, n) AS t
            FROM params p
            ARRAY JOIN range(p.diff_h + 1) AS n
        )
        SELECT t.t AS t, ifNull(c.c, 0) AS c
        FROM timeline t
        LEFT JOIN counts c ON c.t = t.t
        ORDER BY t
    """)

    # Authors
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
        params AS (
            SELECT
                toStartOfYear(min(first_at)) AS min_y,
                dateDiff('year', toStartOfYear(min(first_at)), toStartOfYear(max(first_at))) AS diff_y
            FROM first_seen
            HAVING count() > 0
        ),
        counts AS (
            SELECT toYear(first_at) AS y, count() AS c
            FROM first_seen
            GROUP BY y
        ),
        timeline AS (
            SELECT toYear(addYears(p.min_y, n)) AS y
            FROM params p
            ARRAY JOIN range(p.diff_y + 1) AS n
        )
        SELECT t.y AS y, ifNull(c.c, 0) AS c
        FROM timeline t
        LEFT JOIN counts c ON c.y = t.y
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
        params AS (
            SELECT
                toStartOfMonth(min(first_at)) AS min_m,
                dateDiff('month', toStartOfMonth(min(first_at)), toStartOfMonth(max(first_at))) AS diff_m
            FROM first_seen
            HAVING count() > 0
        ),
        counts AS (
            SELECT toStartOfMonth(first_at) AS m, count() AS c
            FROM first_seen
            GROUP BY m
        ),
        timeline AS (
            SELECT addMonths(p.min_m, n) AS m
            FROM params p
            ARRAY JOIN range(p.diff_m + 1) AS n
        )
        SELECT toYYYYMM(t.m) AS yyyymm, ifNull(c.c, 0) AS c
        FROM timeline t
        LEFT JOIN counts c ON c.m = t.m
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
        params AS (
            SELECT
                toDate(min(first_at)) AS min_d,
                dateDiff('day', toDate(min(first_at)), toDate(max(first_at))) AS diff_d
            FROM first_seen
            HAVING count() > 0
        ),
        counts AS (
            SELECT toDate(first_at) AS d, count() AS c
            FROM first_seen
            GROUP BY d
        ),
        timeline AS (
            SELECT addDays(p.min_d, n) AS d
            FROM params p
            ARRAY JOIN range(p.diff_d + 1) AS n
        )
        SELECT t.d AS d, ifNull(c.c, 0) AS c
        FROM timeline t
        LEFT JOIN counts c ON c.d = t.d
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
        params AS (
            SELECT
                toStartOfHour(min(first_at)) AS min_t,
                dateDiff('hour', toStartOfHour(min(first_at)), toStartOfHour(max(first_at))) AS diff_h
            FROM first_seen
            HAVING count() > 0
        ),
        counts AS (
            SELECT toStartOfHour(first_at) AS t, count() AS c
            FROM first_seen
            GROUP BY t
        ),
        timeline AS (
            SELECT addHours(p.min_t, n) AS t
            FROM params p
            ARRAY JOIN range(p.diff_h + 1) AS n
        )
        SELECT t.t AS t, ifNull(c.c, 0) AS c
        FROM timeline t
        LEFT JOIN counts c ON c.t = t.t
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
        return datetime.fromisoformat(str(v)).strftime("%Y-%m-%d %H")

    # ---------- Split charts (full period) ----------
    if y_books:
        plot_series_with_cumulative("Yearly (Books: New + Cumulative)", [str(k) for k, _ in y_books], [v for _, v in y_books],
                        os.path.join(OUT_DIR, "raw_naver_by_year.png"), color=COLOR_BOOKS, max_labels=24)
    if y_auth:
        plot_series_with_cumulative("Yearly (Authors: New + Cumulative)", [str(k) for k, _ in y_auth], [v for _, v in y_auth],
                        os.path.join(OUT_DIR, "raw_naver_by_year_authors.png"), color=COLOR_AUTHORS, max_labels=24)
    if y_pubs:
        plot_series_with_cumulative("Yearly (Publishers: New + Cumulative)", [str(k) for k, _ in y_pubs], [v for _, v in y_pubs],
                        os.path.join(OUT_DIR, "raw_naver_by_year_publishers.png"), color=COLOR_PUBLISHERS, max_labels=24)

    if m_books:
        plot_series_with_cumulative("Monthly (Books: New + Cumulative)", [str(k) for k, _ in m_books], [v for _, v in m_books],
                        os.path.join(OUT_DIR, "raw_naver_by_month.png"), rotate=45, color=COLOR_BOOKS, max_labels=24)
    if m_auth:
        plot_series_with_cumulative("Monthly (Authors: New + Cumulative)", [str(k) for k, _ in m_auth], [v for _, v in m_auth],
                        os.path.join(OUT_DIR, "raw_naver_by_month_authors.png"), rotate=45, color=COLOR_AUTHORS, max_labels=24)
    if m_pubs:
        plot_series_with_cumulative("Monthly (Publishers: New + Cumulative)", [str(k) for k, _ in m_pubs], [v for _, v in m_pubs],
                        os.path.join(OUT_DIR, "raw_naver_by_month_publishers.png"), rotate=45, color=COLOR_PUBLISHERS, max_labels=24)

    if d_books:
        plot_series_with_cumulative("Daily (Books: New + Cumulative)", [str(k) for k, _ in d_books], [v for _, v in d_books],
                        os.path.join(OUT_DIR, "raw_naver_by_day.png"), rotate=45, color=COLOR_BOOKS, max_labels=24)
    if d_auth:
        plot_series_with_cumulative("Daily (Authors: New + Cumulative)", [str(k) for k, _ in d_auth], [v for _, v in d_auth],
                        os.path.join(OUT_DIR, "raw_naver_by_day_authors.png"), rotate=45, color=COLOR_AUTHORS, max_labels=24)
    if d_pubs:
        plot_series_with_cumulative("Daily (Publishers: New + Cumulative)", [str(k) for k, _ in d_pubs], [v for _, v in d_pubs],
                        os.path.join(OUT_DIR, "raw_naver_by_day_publishers.png"), rotate=45, color=COLOR_PUBLISHERS, max_labels=24)

    if h_books:
        plot_series_with_cumulative("Hourly (Books: New + Cumulative)", [fmt_hour(k) for k, _ in h_books], [v for _, v in h_books],
                        os.path.join(OUT_DIR, "raw_naver_by_hour.png"), rotate=45, color=COLOR_BOOKS, max_labels=24)
    if h_auth:
        plot_series_with_cumulative("Hourly (Authors: New + Cumulative)", [fmt_hour(k) for k, _ in h_auth], [v for _, v in h_auth],
                        os.path.join(OUT_DIR, "raw_naver_by_hour_authors.png"), rotate=45, color=COLOR_AUTHORS, max_labels=24)
    if h_pubs:
        plot_series_with_cumulative("Hourly (Publishers: New + Cumulative)", [fmt_hour(k) for k, _ in h_pubs], [v for _, v in h_pubs],
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

    # 핵심: Monthly 3종만 상단에 노출 (스크롤 최소화)
    md.append("## 📊 Monthly Overview (New + Cumulative)")
    md.append("")
    md.append("### Books")
    md.append("![Books Month](raw_naver_by_month.png)")
    md.append("")
    md.append("### Authors")
    md.append("![Authors Month](raw_naver_by_month_authors.png)")
    md.append("")
    md.append("### Publishers")
    md.append("![Publishers Month](raw_naver_by_month_publishers.png)")
    md.append("")

    # Details (fold)
    md.append("<details>")
    md.append("<summary>📅 Yearly Details</summary>")
    md.append("")
    md.append("### Books")
    md.append("![Books Year](raw_naver_by_year.png)")
    md.append("")
    md.append("### Authors")
    md.append("![Authors Year](raw_naver_by_year_authors.png)")
    md.append("")
    md.append("### Publishers")
    md.append("![Publishers Year](raw_naver_by_year_publishers.png)")
    md.append("")
    md.append("</details>")
    md.append("")

    md.append("<details>")
    md.append("<summary>📆 Daily Details</summary>")
    md.append("")
    md.append("### Books")
    md.append("![Books Day](raw_naver_by_day.png)")
    md.append("")
    md.append("### Authors")
    md.append("![Authors Day](raw_naver_by_day_authors.png)")
    md.append("")
    md.append("### Publishers")
    md.append("![Publishers Day](raw_naver_by_day_publishers.png)")
    md.append("")
    md.append("</details>")
    md.append("")

    md.append("<details>")
    md.append("<summary>⏱ Hourly Details</summary>")
    md.append("")
    md.append("### Books")
    md.append("![Books Hour](raw_naver_by_hour.png)")
    md.append("")
    md.append("### Authors")
    md.append("![Authors Hour](raw_naver_by_hour_authors.png)")
    md.append("")
    md.append("### Publishers")
    md.append("![Publishers Hour](raw_naver_by_hour_publishers.png)")
    md.append("")
    md.append("</details>")
    md.append("")

    md.append("> 시계열은 각 항목(ISBN/저자/출판사)의 '최초 등장 시각' 기준 신규 유입을 집계합니다. (빈 구간은 0으로 채움)")
    md.append("> 모든 시계열 차트는 **신규 유입(막대) + 누적(선)** 을 함께 표시합니다.")
    md.append("")

    with open(OUTPUT_MD, "w", encoding="utf-8") as f:
        f.write("\n".join(md))
if __name__ == "__main__":
    main()
