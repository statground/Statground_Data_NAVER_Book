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
                    (b.get_x() + b.get_width()/2, h),
                    xytext=(0, y_offset),
                    textcoords="offset points",
                    ha="center", va="bottom",
                    fontsize=fontsize)

def annotate_points(ax, xs, ys, y_offset=6, fontsize=7):
    for x, y in zip(xs, ys):
        ax.annotate(_fmt_int(y), (x, y),
                    textcoords="offset points",
                    xytext=(0, y_offset),
                    ha="center",
                    fontsize=fontsize)

def take_last(labels, series_map, limit):
    if limit is None or len(labels) <= limit:
        return labels, series_map
    labels2 = labels[-limit:]
    series2 = {k: v[-limit:] for k, v in series_map.items()}
    return labels2, series2

def plot_grouped_bars(title, x_labels, series_map, path, rotate=0, limit=None):
    x_labels, series_map = take_last(x_labels, series_map, limit)
    names = list(series_map.keys())
    n = len(names)
    if n == 0:
        return

    fig = plt.figure(figsize=(12, 5))
    ax = fig.add_subplot(111)

    x = list(range(len(x_labels)))
    total_w = 0.8
    bar_w = total_w / n
    offsets = [(-total_w/2) + (i + 0.5) * bar_w for i in range(n)]

    for i, name in enumerate(names):
        vals = series_map[name]
        bars = ax.bar([xi + offsets[i] for xi in x], vals, width=bar_w, label=name)
        annotate_bars(ax, bars, fontsize=7, y_offset=2)

    ax.set_title(title)
    ax.set_ylabel("Count")
    ax.set_xticks(x)
    ax.set_xticklabels(x_labels, rotation=rotate, ha="right" if rotate else "center")
    ax.legend(loc="best")
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)

def plot_multi_line(title, x_labels, series_map, path, rotate=0, limit=None):
    x_labels, series_map = take_last(x_labels, series_map, limit)
    names = list(series_map.keys())
    if not names:
        return

    fig = plt.figure(figsize=(12, 5))
    ax = fig.add_subplot(111)
    x = list(range(len(x_labels)))

    for idx, name in enumerate(names):
        vals = series_map[name]
        ax.plot(x, vals, marker="o", label=name)
        annotate_points(ax, x, vals, y_offset=6 + idx * 8, fontsize=7)

    ax.set_title(title)
    ax.set_ylabel("Count")
    ax.set_xticks(x)
    ax.set_xticklabels(x_labels, rotation=rotate, ha="right" if rotate else "center")
    ax.legend(loc="best")
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)

def build_aligned_series(keys_sorted, rows):
    m = {k: int(v) for (k, v) in rows}
    return [m.get(k, 0) for k in keys_sorted]

def main():
    now = datetime.now(KST)
    os.makedirs(OUT_DIR, exist_ok=True)

    # Overall distinct totals
    base_isbn = "isbn IS NOT NULL AND length(trim(isbn)) > 0"
    base_pub = "publisher IS NOT NULL AND length(trim(publisher)) > 0"

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

    # ✅ 신규 유입 (최초 등장 시각 기준)
    # Books: first ISBN seen time
    y_books = q_rows(f"""
        WITH first_seen AS (
            SELECT isbn, min(created_at) AS first_at
            FROM {TABLE_NAME}
            WHERE {base_isbn}
            GROUP BY isbn
        )
        SELECT toYear(first_at) AS k, count() AS v
        FROM first_seen
        GROUP BY k ORDER BY k
    """)
    m_books = q_rows(f"""
        WITH first_seen AS (
            SELECT isbn, min(created_at) AS first_at
            FROM {TABLE_NAME}
            WHERE {base_isbn}
            GROUP BY isbn
        )
        SELECT toYYYYMM(first_at) AS k, count() AS v
        FROM first_seen
        GROUP BY k ORDER BY k
    """)
    d_books = q_rows(f"""
        WITH first_seen AS (
            SELECT isbn, min(created_at) AS first_at
            FROM {TABLE_NAME}
            WHERE {base_isbn}
            GROUP BY isbn
        )
        SELECT toDate(first_at) AS k, count() AS v
        FROM first_seen
        GROUP BY k ORDER BY k
    """)
    h_books = q_rows(f"""
        WITH first_seen AS (
            SELECT isbn, min(created_at) AS first_at
            FROM {TABLE_NAME}
            WHERE {base_isbn}
            GROUP BY isbn
        )
        SELECT toStartOfHour(first_at) AS k, count() AS v
        FROM first_seen
        GROUP BY k ORDER BY k
    """)

    # Publishers: first publisher seen time (distinct publisher)
    y_pubs = q_rows(f"""
        WITH first_seen AS (
            SELECT publisher, min(created_at) AS first_at
            FROM {TABLE_NAME}
            WHERE {base_pub}
            GROUP BY publisher
        )
        SELECT toYear(first_at) AS k, count() AS v
        FROM first_seen
        GROUP BY k ORDER BY k
    """)
    m_pubs = q_rows(f"""
        WITH first_seen AS (
            SELECT publisher, min(created_at) AS first_at
            FROM {TABLE_NAME}
            WHERE {base_pub}
            GROUP BY publisher
        )
        SELECT toYYYYMM(first_at) AS k, count() AS v
        FROM first_seen
        GROUP BY k ORDER BY k
    """)
    d_pubs = q_rows(f"""
        WITH first_seen AS (
            SELECT publisher, min(created_at) AS first_at
            FROM {TABLE_NAME}
            WHERE {base_pub}
            GROUP BY publisher
        )
        SELECT toDate(first_at) AS k, count() AS v
        FROM first_seen
        GROUP BY k ORDER BY k
    """)
    h_pubs = q_rows(f"""
        WITH first_seen AS (
            SELECT publisher, min(created_at) AS first_at
            FROM {TABLE_NAME}
            WHERE {base_pub}
            GROUP BY publisher
        )
        SELECT toStartOfHour(first_at) AS k, count() AS v
        FROM first_seen
        GROUP BY k ORDER BY k
    """)

    # Authors: split '^' then first author seen time
    y_auth = q_rows(f"""
        WITH exploded AS (
            SELECT
                trim(author_one) AS author_one,
                created_at
            FROM {TABLE_NAME}
            ARRAY JOIN splitByChar('^', ifNull(author, '')) AS author_one
            WHERE length(trim(author_one)) > 0
        ),
        first_seen AS (
            SELECT author_one, min(created_at) AS first_at
            FROM exploded
            GROUP BY author_one
        )
        SELECT toYear(first_at) AS k, count() AS v
        FROM first_seen
        GROUP BY k ORDER BY k
    """)
    m_auth = q_rows(f"""
        WITH exploded AS (
            SELECT
                trim(author_one) AS author_one,
                created_at
            FROM {TABLE_NAME}
            ARRAY JOIN splitByChar('^', ifNull(author, '')) AS author_one
            WHERE length(trim(author_one)) > 0
        ),
        first_seen AS (
            SELECT author_one, min(created_at) AS first_at
            FROM exploded
            GROUP BY author_one
        )
        SELECT toYYYYMM(first_at) AS k, count() AS v
        FROM first_seen
        GROUP BY k ORDER BY k
    """)
    d_auth = q_rows(f"""
        WITH exploded AS (
            SELECT
                trim(author_one) AS author_one,
                created_at
            FROM {TABLE_NAME}
            ARRAY JOIN splitByChar('^', ifNull(author, '')) AS author_one
            WHERE length(trim(author_one)) > 0
        ),
        first_seen AS (
            SELECT author_one, min(created_at) AS first_at
            FROM exploded
            GROUP BY author_one
        )
        SELECT toDate(first_at) AS k, count() AS v
        FROM first_seen
        GROUP BY k ORDER BY k
    """)
    h_auth = q_rows(f"""
        WITH exploded AS (
            SELECT
                trim(author_one) AS author_one,
                created_at
            FROM {TABLE_NAME}
            ARRAY JOIN splitByChar('^', ifNull(author, '')) AS author_one
            WHERE length(trim(author_one)) > 0
        ),
        first_seen AS (
            SELECT author_one, min(created_at) AS first_at
            FROM exploded
            GROUP BY author_one
        )
        SELECT toStartOfHour(first_at) AS k, count() AS v
        FROM first_seen
        GROUP BY k ORDER BY k
    """)

    # Totals chart (keep filename)
    fig = plt.figure(figsize=(9, 4.5))
    ax = fig.add_subplot(111)
    bars = ax.bar(["Books(uniq isbn)", "Authors(uniq)", "Publishers(uniq)"], [total_books, total_authors, total_publishers])
    ax.set_title("Total Overview")
    ax.set_ylabel("Count")
    annotate_bars(ax, bars, fontsize=9, y_offset=3)
    fig.tight_layout()
    fig.savefig(os.path.join(OUT_DIR, "raw_naver_totals.png"), dpi=150)
    plt.close(fig)

    # Yearly (grouped bars)
    years = sorted({k for k,_ in y_books} | {k for k,_ in y_auth} | {k for k,_ in y_pubs})
    years_lbl = [str(k) for k in years]
    series_year = {
        "Books": build_aligned_series(years, y_books),
        "Authors": build_aligned_series(years, y_auth),
        "Publishers": build_aligned_series(years, y_pubs),
    }
    plot_grouped_bars("Yearly (New)", years_lbl, series_year, os.path.join(OUT_DIR, "raw_naver_by_year.png"))

    # Monthly (multi line, last 24)
    months = sorted({k for k,_ in m_books} | {k for k,_ in m_auth} | {k for k,_ in m_pubs})
    months_lbl = [str(k) for k in months]
    series_month = {
        "Books": build_aligned_series(months, m_books),
        "Authors": build_aligned_series(months, m_auth),
        "Publishers": build_aligned_series(months, m_pubs),
    }
    plot_multi_line("Monthly (New, last 24)", months_lbl, series_month, os.path.join(OUT_DIR, "raw_naver_by_month.png"), rotate=45, limit=24)

    # Daily (multi line, last 60)
    days = sorted({k for k,_ in d_books} | {k for k,_ in d_auth} | {k for k,_ in d_pubs})
    days_lbl = [str(k) for k in days]
    series_day = {
        "Books": build_aligned_series(days, d_books),
        "Authors": build_aligned_series(days, d_auth),
        "Publishers": build_aligned_series(days, d_pubs),
    }
    plot_multi_line("Daily (New, last 60)", days_lbl, series_day, os.path.join(OUT_DIR, "raw_naver_by_day.png"), rotate=45, limit=60)

    # Hourly (multi line, last 48) with label format YYYY-MM-DD HH
    hours = sorted({k for k,_ in h_books} | {k for k,_ in h_auth} | {k for k,_ in h_pubs})
    hours_lbl = [datetime.fromisoformat(str(k)).strftime("%Y-%m-%d %H") for k in hours]
    series_hour = {
        "Books": build_aligned_series(hours, h_books),
        "Authors": build_aligned_series(hours, h_auth),
        "Publishers": build_aligned_series(hours, h_pubs),
    }
    plot_multi_line("Hourly (New, last 48)", hours_lbl, series_hour, os.path.join(OUT_DIR, "raw_naver_by_hour.png"), rotate=45, limit=48)

    # Markdown (no engine/db/table wording; no 'naver' wording in text; filenames kept)
    md = []
    md.append("# 수집 데이터 집계")
    md.append("")
    md.append(f"- 업데이트 시각(KST): {now.strftime('%Y-%m-%d %H:%M:%S')}")
    md.append("")
    md.append("## 핵심 지표")
    md.append(f"- 총 고유 ISBN 수: **{total_books:,}**")
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
    md.append("> Year/Month/Day/Hour는 각 항목(ISBN/저자/출판사)의 '최초 등장 시각' 기준 신규 유입을 집계합니다.")
    md.append("")

    with open(OUTPUT_MD, "w", encoding="utf-8") as f:
        f.write("\n".join(md))

if __name__ == "__main__":
    main()
