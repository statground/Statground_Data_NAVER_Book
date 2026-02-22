#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
raw_naver stats -> charts (PNG) + markdown (MD)
- Charts: totals, yearly, monthly, daily, hourly
- Each chart shows value labels.
- Runs in GitHub Actions; outputs to stats/
"""

import os
from datetime import datetime
import pytz
import clickhouse_connect

import matplotlib
matplotlib.use("Agg")  # headless
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

TABLE_NAME = (os.getenv("CH_TABLE") or "raw_naver").strip()
OUT_DIR = (os.getenv("OUT_DIR") or "stats").strip()
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

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def annotate_bars(ax, bars, fmt="{:,}", rotation=0, fontsize=9, y_offset=3):
    for b in bars:
        h = b.get_height()
        ax.annotate(fmt.format(int(h)) if h == int(h) else str(h),
                    xy=(b.get_x() + b.get_width() / 2, h),
                    xytext=(0, y_offset),
                    textcoords="offset points",
                    ha="center", va="bottom",
                    rotation=rotation,
                    fontsize=fontsize)

def annotate_points(ax, x, y, fmt="{:,}", fontsize=9, y_offset=6):
    for xi, yi in zip(x, y):
        ax.annotate(fmt.format(int(yi)) if yi == int(yi) else str(yi),
                    (xi, yi),
                    textcoords="offset points",
                    xytext=(0, y_offset),
                    ha="center",
                    fontsize=fontsize)

def plot_totals(total_books: int, total_authors: int, total_publishers: int, path: str):
    labels = ["Books(rows)", "Authors(distinct)", "Publishers(distinct)"]
    values = [total_books, total_authors, total_publishers]

    fig = plt.figure(figsize=(9, 4.5))
    ax = fig.add_subplot(111)
    bars = ax.bar(labels, values)
    ax.set_title("raw_naver Totals")
    ax.set_ylabel("Count")
    ax.tick_params(axis='x', labelrotation=0)
    annotate_bars(ax, bars, rotation=0)
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)

def plot_bar(title: str, x_labels, values, path: str, rotate=0, max_labels=None):
    if max_labels is not None and len(x_labels) > max_labels:
        # keep most recent max_labels
        x_labels = x_labels[-max_labels:]
        values = values[-max_labels:]

    fig = plt.figure(figsize=(12, 5))
    ax = fig.add_subplot(111)
    x = list(range(len(x_labels)))
    bars = ax.bar(x, values)
    ax.set_title(title)
    ax.set_ylabel("Books")
    ax.set_xticks(x)
    ax.set_xticklabels(x_labels, rotation=rotate, ha="right" if rotate else "center")
    annotate_bars(ax, bars, rotation=90 if rotate else 0, fontsize=8, y_offset=2)
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)

def plot_line(title: str, x_labels, values, path: str, rotate=0, max_labels=None):
    if max_labels is not None and len(x_labels) > max_labels:
        x_labels = x_labels[-max_labels:]
        values = values[-max_labels:]
    fig = plt.figure(figsize=(12, 5))
    ax = fig.add_subplot(111)
    x = list(range(len(x_labels)))
    ax.plot(x, values, marker="o")
    ax.set_title(title)
    ax.set_ylabel("Books")
    ax.set_xticks(x)
    ax.set_xticklabels(x_labels, rotation=rotate, ha="right" if rotate else "center")
    annotate_points(ax, x, values, fontsize=8, y_offset=6)
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)

def main():
    now = datetime.now(KST)

    ensure_dir(OUT_DIR)

    # Totals
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

    # Series
    by_year = q_rows(f"""
        SELECT toYear(created_at) AS y, count() AS c
        FROM {TABLE_NAME}
        GROUP BY y
        ORDER BY y ASC
    """)
    by_month = q_rows(f"""
        SELECT toYYYYMM(created_at) AS ym, count() AS c
        FROM {TABLE_NAME}
        GROUP BY ym
        ORDER BY ym ASC
    """)
    by_day = q_rows(f"""
        SELECT toDate(created_at) AS d, count() AS c
        FROM {TABLE_NAME}
        GROUP BY d
        ORDER BY d ASC
    """)
    by_hour = q_rows(f"""
        SELECT toStartOfHour(created_at) AS h, count() AS c
        FROM {TABLE_NAME}
        GROUP BY h
        ORDER BY h ASC
    """)

    # Chart paths
    p_totals = os.path.join(OUT_DIR, "raw_naver_totals.png")
    p_year = os.path.join(OUT_DIR, "raw_naver_by_year.png")
    p_month = os.path.join(OUT_DIR, "raw_naver_by_month.png")
    p_day = os.path.join(OUT_DIR, "raw_naver_by_day.png")
    p_hour = os.path.join(OUT_DIR, "raw_naver_by_hour.png")

    # Render charts (choose sensible label limits for readability)
    plot_totals(total_books, total_authors, total_publishers, p_totals)

    if by_year:
        years = [str(y) for (y, _) in by_year]
        yvals = [int(c) for (_, c) in by_year]
        plot_bar("Books by Year", years, yvals, p_year, rotate=0)

    if by_month:
        months = [str(ym) for (ym, _) in by_month]
        mvals = [int(c) for (_, c) in by_month]
        # Show last 24 months for readability
        plot_line("Books by Month (last 24)", months, mvals, p_month, rotate=45, max_labels=24)

    if by_day:
        days = [str(d) for (d, _) in by_day]
        dvals = [int(c) for (_, c) in by_day]
        # Show last 60 days
        plot_line("Books by Day (last 60)", days, dvals, p_day, rotate=45, max_labels=60)

    if by_hour:
        hours = [str(h) for (h, _) in by_hour]
        hvals = [int(c) for (_, c) in by_hour]
        # Show last 48 hours
        plot_line("Books by Hour (last 48)", hours, hvals, p_hour, rotate=45, max_labels=48)

    # Markdown
    md = []
    md.append("# raw_naver 집계 (차트)")
    md.append("")
    md.append(f"- 기준: ClickHouse `{CH_DATABASE}.{TABLE_NAME}` (OLAP/분석 전용, SSOT 아님)")
    md.append(f"- 업데이트 시각(KST): {now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    md.append("")
    md.append("## 핵심 지표")
    md.append("")
    md.append(f"- 총 책 수(rows): **{total_books:,}**")
    md.append(f"- 저자 수(distinct, '^' 분리): **{total_authors:,}**")
    md.append(f"- 출판사 수(distinct): **{total_publishers:,}**")
    md.append("")
    md.append("## 차트")
    md.append("")
    md.append("### 전체")
    md.append(f"![raw_naver totals]({os.path.basename(p_totals)})")
    md.append("")
    md.append("### 연별")
    md.append(f"![raw_naver by year]({os.path.basename(p_year)})")
    md.append("")
    md.append("### 월별 (최근 24개월)")
    md.append(f"![raw_naver by month]({os.path.basename(p_month)})")
    md.append("")
    md.append("### 일별 (최근 60일)")
    md.append(f"![raw_naver by day]({os.path.basename(p_day)})")
    md.append("")
    md.append("### 시간별 (최근 48시간)")
    md.append(f"![raw_naver by hour]({os.path.basename(p_hour)})")
    md.append("")

    with open(OUTPUT_MD, "w", encoding="utf-8") as f:
        f.write("\n".join(md))

if __name__ == "__main__":
    main()
