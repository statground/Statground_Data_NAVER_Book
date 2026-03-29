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
from urllib.parse import quote
from datetime import datetime
import pytz
import clickhouse_connect
from concurrent.futures import ThreadPoolExecutor, as_completed

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


def _github_raw_base() -> str | None:
    repo = (os.getenv("GITHUB_REPOSITORY") or "").strip()
    ref_name = (os.getenv("GITHUB_REF_NAME") or "").strip()
    if not repo or not ref_name:
        return None
    return f"https://raw.githubusercontent.com/{repo}/{quote(ref_name, safe='')}"

def md_image(filename: str, alt: str) -> str:
    rel_path = f"{OUT_DIR}/{filename}".replace(os.sep, "/")
    base = _github_raw_base()
    if base:
        return f"![{alt}]({base}/{quote(rel_path, safe='/')})"
    return f"![{alt}]({filename})"


# User requested distinct colors
COLOR_BOOKS = "#1f77b4"       # blue
COLOR_AUTHORS = "#2ca02c"     # green
COLOR_PUBLISHERS = "#ff7f0e"  # orange

def make_client():
    return clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_PORT,
        username=CH_USER,
        password=CH_PASSWORD,
        database=CH_DATABASE
    )

def q_scalar(sql: str) -> int:
    c = make_client()
    try:
        r = c.query(sql)
        return int(r.result_rows[0][0]) if r.result_rows else 0
    finally:
        try:
            c.close()
        except Exception:
            pass

def q_rows(sql: str):
    c = make_client()
    try:
        return c.query(sql).result_rows
    finally:
        try:
            c.close()
        except Exception:
            pass


def q_value(sql: str):
    c = make_client()
    try:
        r = c.query(sql)
        return r.result_rows[0][0] if r.result_rows else None
    finally:
        try:
            c.close()
        except Exception:
            pass

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
    data_updated_at = q_value(f"SELECT max(updated_at) FROM {TABLE_NAME}")
    if isinstance(data_updated_at, datetime):
        if data_updated_at.tzinfo is None:
            data_updated_at = KST.localize(data_updated_at)
        else:
            data_updated_at = data_updated_at.astimezone(KST)
    else:
        data_updated_at = None

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

    # =======================
    # New inflow series (0 fill, fast)
    # (Threaded query execution)
    # =======================

    sql_y_books = f"""
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
    """

    sql_m_books = f"""
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
    """

    sql_d_books = f"""
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
    """

    sql_h_books = f"""
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
    """

    sql_y_pubs = f"""
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
    """

    sql_m_pubs = f"""
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
    """

    sql_d_pubs = f"""
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
    """

    sql_h_pubs = f"""
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
    """

    sql_y_auth = f"""
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
    """

    sql_m_auth = f"""
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
    """

    sql_d_auth = f"""
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
    """

    sql_h_auth = f"""
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
    """

    # Execute in parallel (each query opens its own CH client)
    max_workers = int(os.getenv("STATS_WORKERS", "6"))
    tasks = {
        "y_books": sql_y_books,
        "m_books": sql_m_books,
        "d_books": sql_d_books,
        "h_books": sql_h_books,
        "y_pubs": sql_y_pubs,
        "m_pubs": sql_m_pubs,
        "d_pubs": sql_d_pubs,
        "h_pubs": sql_h_pubs,
        "y_auth": sql_y_auth,
        "m_auth": sql_m_auth,
        "d_auth": sql_d_auth,
        "h_auth": sql_h_auth
    }
    results = {}
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        future_map = {ex.submit(q_rows, sql): key for key, sql in tasks.items()}
        for fut in as_completed(future_map):
            key = future_map[fut]
            results[key] = fut.result()

    y_books = results.get("y_books", [])
    m_books = results.get("m_books", [])
    d_books = results.get("d_books", [])
    h_books = results.get("h_books", [])

    y_pubs = results.get("y_pubs", [])
    m_pubs = results.get("m_pubs", [])
    d_pubs = results.get("d_pubs", [])
    h_pubs = results.get("h_pubs", [])

    y_auth = results.get("y_auth", [])
    m_auth = results.get("m_auth", [])
    d_auth = results.get("d_auth", [])
    h_auth = results.get("h_auth", [])

    def fmt_hour(v):
        return datetime.fromisoformat(str(v)).strftime("%Y-%m-%d %H")


    # =======================
    # Published date based stats (pubdate)
    # - pubdate is String (may be YYYY, YYYYMM, YYYYMMDD, or other)
    # - We treat:
    #   * exact-year: length>=4 (YYYY)
    #   * exact-month: length>=6 (YYYYMM) where month 01-12
    #   * exact-day: length>=8 (YYYYMMDD) where date is valid
    # - For monthly/daily, year-only / year-month-only items are summarized separately (UNKNOWN buckets)
    # =======================

    pub_digits_expr = "replaceRegexpAll(trim(pubdate), '[^0-9]', '')"
    pub_len_expr = f"length({pub_digits_expr})"
    pub_year_expr = f"toUInt16OrZero(substring({pub_digits_expr}, 1, 4))"
    pub_month_expr = f"toUInt8OrZero(substring({pub_digits_expr}, 5, 2))"
    pub_day_expr = f"toUInt8OrZero(substring({pub_digits_expr}, 7, 2))"
    pub_valid_year = f"({pub_len_expr} >= 4) AND ({pub_year_expr} BETWEEN 1000 AND 2100)"

    # Totals by granularity
    total_pub_year = q_scalar(f"""
        SELECT uniqExact(isbn)
        FROM {TABLE_NAME}
        WHERE {base_isbn} AND {pub_valid_year}
    """)
    total_pub_year_only = q_scalar(f"""
        SELECT uniqExact(isbn)
        FROM {TABLE_NAME}
        WHERE {base_isbn} AND {pub_valid_year} AND {pub_len_expr} = 4
    """)
    total_pub_ym = q_scalar(f"""
        SELECT uniqExact(isbn)
        FROM {TABLE_NAME}
        WHERE {base_isbn} AND {pub_valid_year} AND {pub_len_expr} = 6
    """)
    total_pub_ymd = q_scalar(f"""
        SELECT uniqExact(isbn)
        FROM {TABLE_NAME}
        WHERE {base_isbn} AND {pub_valid_year} AND {pub_len_expr} >= 8
    """)
    total_pub_missing = q_scalar(f"""
        SELECT uniqExact(isbn)
        FROM {TABLE_NAME}
        WHERE {base_isbn} AND NOT ({pub_valid_year})
    """)

    # Yearly distribution (books / publishers / authors)
    sql_y_pub_books = f"""
        WITH base AS (
            SELECT DISTINCT isbn,
                {pub_year_expr} AS y
            FROM {TABLE_NAME}
            WHERE {base_isbn} AND {pub_valid_year}
        ),
        params AS (
            SELECT toStartOfYear(toDate(min(y) * 10000 + 101)) AS min_y,
                   dateDiff('year', toStartOfYear(toDate(min(y) * 10000 + 101)), toStartOfYear(toDate(max(y) * 10000 + 101))) AS diff_y
            FROM base
            HAVING count() > 0
        ),
        counts AS (
            SELECT y, count() AS c
            FROM base
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
    """

    sql_y_pub_pubs = f"""
        WITH base AS (
            SELECT DISTINCT publisher,
                {pub_year_expr} AS y
            FROM {TABLE_NAME}
            WHERE {base_pub} AND {pub_valid_year}
        ),
        params AS (
            SELECT toStartOfYear(toDate(min(y) * 10000 + 101)) AS min_y,
                   dateDiff('year', toStartOfYear(toDate(min(y) * 10000 + 101)), toStartOfYear(toDate(max(y) * 10000 + 101))) AS diff_y
            FROM base
            HAVING count() > 0
        ),
        counts AS (
            SELECT y, count() AS c
            FROM base
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
    """

    sql_y_pub_auth = f"""
        WITH base AS (
            SELECT DISTINCT trim(author_one) AS author_one,
                {pub_year_expr} AS y
            FROM {TABLE_NAME}
            ARRAY JOIN splitByChar('^', ifNull(author, '')) AS author_one
            WHERE length(trim(author_one)) > 0 AND {pub_valid_year}
        ),
        params AS (
            SELECT toStartOfYear(toDate(min(y) * 10000 + 101)) AS min_y,
                   dateDiff('year', toStartOfYear(toDate(min(y) * 10000 + 101)), toStartOfYear(toDate(max(y) * 10000 + 101))) AS diff_y
            FROM base
            HAVING count() > 0
        ),
        counts AS (
            SELECT y, count() AS c
            FROM base
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
    """

    # Monthly exact distribution
    pub_valid_month = f"({pub_len_expr} >= 6) AND {pub_valid_year} AND ({pub_month_expr} BETWEEN 1 AND 12)"
    pub_month_start = f"toDateOrNull(concat(toString({pub_year_expr}), '-', lpad(toString({pub_month_expr}), 2, '0'), '-01'))"

    sql_m_pub_books = f"""
        WITH base AS (
            SELECT DISTINCT isbn,
                {pub_month_start} AS m
            FROM {TABLE_NAME}
            WHERE {base_isbn} AND {pub_valid_month}
        ),
        params AS (
            SELECT toStartOfMonth(min(m)) AS min_m,
                   dateDiff('month', toStartOfMonth(min(m)), toStartOfMonth(max(m))) AS diff_m
            FROM base
            HAVING count() > 0
        ),
        counts AS (
            SELECT toYYYYMM(m) AS ym, count() AS c
            FROM base
            GROUP BY ym
        ),
        timeline AS (
            SELECT toYYYYMM(addMonths(p.min_m, n)) AS ym
            FROM params p
            ARRAY JOIN range(p.diff_m + 1) AS n
        )
        SELECT t.ym AS ym, ifNull(c.c, 0) AS c
        FROM timeline t
        LEFT JOIN counts c ON c.ym = t.ym
        ORDER BY ym
    """

    sql_m_pub_pubs = f"""
        WITH base AS (
            SELECT DISTINCT publisher,
                {pub_month_start} AS m
            FROM {TABLE_NAME}
            WHERE {base_pub} AND {pub_valid_month}
        ),
        params AS (
            SELECT toStartOfMonth(min(m)) AS min_m,
                   dateDiff('month', toStartOfMonth(min(m)), toStartOfMonth(max(m))) AS diff_m
            FROM base
            HAVING count() > 0
        ),
        counts AS (
            SELECT toYYYYMM(m) AS ym, count() AS c
            FROM base
            GROUP BY ym
        ),
        timeline AS (
            SELECT toYYYYMM(addMonths(p.min_m, n)) AS ym
            FROM params p
            ARRAY JOIN range(p.diff_m + 1) AS n
        )
        SELECT t.ym AS ym, ifNull(c.c, 0) AS c
        FROM timeline t
        LEFT JOIN counts c ON c.ym = t.ym
        ORDER BY ym
    """

    sql_m_pub_auth = f"""
        WITH base AS (
            SELECT DISTINCT trim(author_one) AS author_one,
                {pub_month_start} AS m
            FROM {TABLE_NAME}
            ARRAY JOIN splitByChar('^', ifNull(author, '')) AS author_one
            WHERE length(trim(author_one)) > 0 AND {pub_valid_month}
        ),
        params AS (
            SELECT toStartOfMonth(min(m)) AS min_m,
                   dateDiff('month', toStartOfMonth(min(m)), toStartOfMonth(max(m))) AS diff_m
            FROM base
            HAVING count() > 0
        ),
        counts AS (
            SELECT toYYYYMM(m) AS ym, count() AS c
            FROM base
            GROUP BY ym
        ),
        timeline AS (
            SELECT toYYYYMM(addMonths(p.min_m, n)) AS ym
            FROM params p
            ARRAY JOIN range(p.diff_m + 1) AS n
        )
        SELECT t.ym AS ym, ifNull(c.c, 0) AS c
        FROM timeline t
        LEFT JOIN counts c ON c.ym = t.ym
        ORDER BY ym
    """

    # Daily exact distribution
    pub_valid_day = f"({pub_len_expr} >= 8) AND {pub_valid_month} AND ({pub_day_expr} BETWEEN 1 AND 31)"
    pub_day_date = f"toDateOrNull(concat(toString({pub_year_expr}), '-', lpad(toString({pub_month_expr}), 2, '0'), '-', lpad(toString({pub_day_expr}), 2, '0')))"

    sql_d_pub_books = f"""
        WITH base AS (
            SELECT DISTINCT isbn,
                {pub_day_date} AS d
            FROM {TABLE_NAME}
            WHERE {base_isbn} AND {pub_valid_day}
        ),
        params AS (
            SELECT min(d) AS min_d,
                   dateDiff('day', min(d), max(d)) AS diff_d
            FROM base
            HAVING count() > 0
        ),
        counts AS (
            SELECT toDate(d) AS d, count() AS c
            FROM base
            GROUP BY d
        ),
        timeline AS (
            SELECT addDays(p.min_d, n) AS d
            FROM params p
            ARRAY JOIN range(p.diff_d + 1) AS n
        )
        SELECT toString(t.d) AS d, ifNull(c.c, 0) AS c
        FROM timeline t
        LEFT JOIN counts c ON c.d = t.d
        ORDER BY d
    """

    sql_d_pub_pubs = f"""
        WITH base AS (
            SELECT DISTINCT publisher,
                {pub_day_date} AS d
            FROM {TABLE_NAME}
            WHERE {base_pub} AND {pub_valid_day}
        ),
        params AS (
            SELECT min(d) AS min_d,
                   dateDiff('day', min(d), max(d)) AS diff_d
            FROM base
            HAVING count() > 0
        ),
        counts AS (
            SELECT toDate(d) AS d, count() AS c
            FROM base
            GROUP BY d
        ),
        timeline AS (
            SELECT addDays(p.min_d, n) AS d
            FROM params p
            ARRAY JOIN range(p.diff_d + 1) AS n
        )
        SELECT toString(t.d) AS d, ifNull(c.c, 0) AS c
        FROM timeline t
        LEFT JOIN counts c ON c.d = t.d
        ORDER BY d
    """

    sql_d_pub_auth = f"""
        WITH base AS (
            SELECT DISTINCT trim(author_one) AS author_one,
                {pub_day_date} AS d
            FROM {TABLE_NAME}
            ARRAY JOIN splitByChar('^', ifNull(author, '')) AS author_one
            WHERE length(trim(author_one)) > 0 AND {pub_valid_day}
        ),
        params AS (
            SELECT min(d) AS min_d,
                   dateDiff('day', min(d), max(d)) AS diff_d
            FROM base
            HAVING count() > 0
        ),
        counts AS (
            SELECT toDate(d) AS d, count() AS c
            FROM base
            GROUP BY d
        ),
        timeline AS (
            SELECT addDays(p.min_d, n) AS d
            FROM params p
            ARRAY JOIN range(p.diff_d + 1) AS n
        )
        SELECT toString(t.d) AS d, ifNull(c.c, 0) AS c
        FROM timeline t
        LEFT JOIN counts c ON c.d = t.d
        ORDER BY d
    """

    # UNKNOWN buckets (Books only)
    unknown_month_books = q_rows(f"""
        SELECT {pub_year_expr} AS y, uniqExact(isbn) AS c
        FROM {TABLE_NAME}
        WHERE {base_isbn} AND {pub_valid_year} AND {pub_len_expr} = 4
        GROUP BY y
        ORDER BY y
    """)
    unknown_day_books = q_rows(f"""
        SELECT concat(toString({pub_year_expr}), '-', lpad(toString({pub_month_expr}), 2, '0')) AS ym,
               uniqExact(isbn) AS c
        FROM {TABLE_NAME}
        WHERE {base_isbn} AND {pub_valid_month} AND {pub_len_expr} = 6
        GROUP BY ym
        ORDER BY ym
    """)

    # Run published distribution queries in parallel
    pub_tasks = {
        "y_pub_books": sql_y_pub_books,
        "y_pub_pubs": sql_y_pub_pubs,
        "y_pub_auth": sql_y_pub_auth,
        "m_pub_books": sql_m_pub_books,
        "m_pub_pubs": sql_m_pub_pubs,
        "m_pub_auth": sql_m_pub_auth,
        "d_pub_books": sql_d_pub_books,
        "d_pub_pubs": sql_d_pub_pubs,
        "d_pub_auth": sql_d_pub_auth,
    }
    pub_results = {}
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        future_map = {ex.submit(q_rows, sql): key for key, sql in pub_tasks.items()}
        for fut in as_completed(future_map):
            key = future_map[fut]
            try:
                pub_results[key] = fut.result()
            except Exception as e:
                print(f"[WARN] pubdate stats query failed: {key} err={e}")
                pub_results[key] = []

    y_pub_books = pub_results.get("y_pub_books", [])
    y_pub_pubs = pub_results.get("y_pub_pubs", [])
    y_pub_auth = pub_results.get("y_pub_auth", [])
    m_pub_books = pub_results.get("m_pub_books", [])
    m_pub_pubs = pub_results.get("m_pub_pubs", [])
    m_pub_auth = pub_results.get("m_pub_auth", [])
    d_pub_books = pub_results.get("d_pub_books", [])
    d_pub_pubs = pub_results.get("d_pub_pubs", [])
    d_pub_auth = pub_results.get("d_pub_auth", [])


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


    # ---------- Published-date charts ----------
    # (Distribution by pubdate; bar + cumulative for readability)
    if y_pub_books:
        plot_series_with_cumulative("Yearly (Books by Published Date)", [str(k) for k, _ in y_pub_books], [v for _, v in y_pub_books],
                        os.path.join(OUT_DIR, "raw_naver_pub_by_year_books.png"), color=COLOR_BOOKS)
    if m_pub_books:
        plot_series_with_cumulative("Monthly (Books by Published Date)", [str(k) for k, _ in m_pub_books], [v for _, v in m_pub_books],
                        os.path.join(OUT_DIR, "raw_naver_pub_by_month_books.png"), rotate=45, color=COLOR_BOOKS, max_labels=24)
    if d_pub_books:
        plot_series_with_cumulative("Daily (Books by Published Date)", [str(k) for k, _ in d_pub_books], [v for _, v in d_pub_books],
                        os.path.join(OUT_DIR, "raw_naver_pub_by_day_books.png"), rotate=45, color=COLOR_BOOKS, max_labels=24)

    if y_pub_pubs:
        plot_series_with_cumulative("Yearly (Publishers by Published Date)", [str(k) for k, _ in y_pub_pubs], [v for _, v in y_pub_pubs],
                        os.path.join(OUT_DIR, "raw_naver_pub_by_year_publishers.png"), color=COLOR_PUBLISHERS)
    if m_pub_pubs:
        plot_series_with_cumulative("Monthly (Publishers by Published Date)", [str(k) for k, _ in m_pub_pubs], [v for _, v in m_pub_pubs],
                        os.path.join(OUT_DIR, "raw_naver_pub_by_month_publishers.png"), rotate=45, color=COLOR_PUBLISHERS, max_labels=24)
    if d_pub_pubs:
        plot_series_with_cumulative("Daily (Publishers by Published Date)", [str(k) for k, _ in d_pub_pubs], [v for _, v in d_pub_pubs],
                        os.path.join(OUT_DIR, "raw_naver_pub_by_day_publishers.png"), rotate=45, color=COLOR_PUBLISHERS, max_labels=24)

    if y_pub_auth:
        plot_series_with_cumulative("Yearly (Authors by Published Date)", [str(k) for k, _ in y_pub_auth], [v for _, v in y_pub_auth],
                        os.path.join(OUT_DIR, "raw_naver_pub_by_year_authors.png"), color=COLOR_AUTHORS)
    if m_pub_auth:
        plot_series_with_cumulative("Monthly (Authors by Published Date)", [str(k) for k, _ in m_pub_auth], [v for _, v in m_pub_auth],
                        os.path.join(OUT_DIR, "raw_naver_pub_by_month_authors.png"), rotate=45, color=COLOR_AUTHORS, max_labels=24)
    if d_pub_auth:
        plot_series_with_cumulative("Daily (Authors by Published Date)", [str(k) for k, _ in d_pub_auth], [v for _, v in d_pub_auth],
                        os.path.join(OUT_DIR, "raw_naver_pub_by_day_authors.png"), rotate=45, color=COLOR_AUTHORS, max_labels=24)


    # ---------- Markdown report ----------
    md = []
    md.append("# 수집 데이터 집계")
    md.append("")
    md.append(f"- 데이터 기준 최종 수정 시각(KST): {data_updated_at.strftime('%Y-%m-%d %H:%M:%S')}" if data_updated_at else "- 데이터 기준 최종 수정 시각(KST): N/A")
    md.append("")
    md.append("## 전체")
    md.append("")
    md.append(f"- 총 고유 ISBN 수: **{total_books:,}**")
    md.append(f"- 저자 수: **{total_authors:,}**")
    md.append(f"- 출판사 수: **{total_publishers:,}**")
    md.append("")
    md.append(md_image("raw_naver_totals.png", "Totals"))
    md.append("")

    md.append("## 출간일(pubdate) 기준 통계")
    md.append("")
    md.append(f"- 출간연도(YYYY 이상) 파싱 가능 ISBN: **{total_pub_year:,}**")
    md.append(f"  - 연도만(YYYY): **{total_pub_year_only:,}**")
    md.append(f"  - 연/월(YYYYMM): **{total_pub_ym:,}**")
    md.append(f"  - 연/월/일(YYYYMMDD+): **{total_pub_ymd:,}**")
    md.append(f"- 출간일 파싱 불가/없음 ISBN: **{total_pub_missing:,}**")
    md.append("")
    if y_pub_books:
        md.append("### Books (Published Date)")
        md.append(md_image("raw_naver_pub_by_year_books.png", "Books Published Year"))
        md.append("")
    if m_pub_books:
        md.append(md_image("raw_naver_pub_by_month_books.png", "Books Published Month"))
        md.append("")
    if d_pub_books:
        md.append(md_image("raw_naver_pub_by_day_books.png", "Books Published Day"))
        md.append("")
    # UNKNOWN buckets summary (Books)
    if unknown_month_books:
        md.append("### Books (Published Date) - UNKNOWN month (year-only)")
        md.append("")
        md.append("| Year | ISBN Count |")
        md.append("|---:|---:|")
        for y, c in unknown_month_books:
            md.append(f"| {y} | {int(c):,} |")
        md.append("")
    if unknown_day_books:
        md.append("### Books (Published Date) - UNKNOWN day (year-month only)")
        md.append("")
        md.append("| Year-Month | ISBN Count |")
        md.append("|---:|---:|")
        for ym, c in unknown_day_books:
            md.append(f"| {ym} | {int(c):,} |")
        md.append("")
    md.append("<details>")
    md.append("<summary>📚 Published Date Details (Authors/Publishers)</summary>")
    md.append("")
    md.append("### Authors")
    md.append(md_image("raw_naver_pub_by_year_authors.png", "Authors Published Year"))
    md.append("")
    md.append(md_image("raw_naver_pub_by_month_authors.png", "Authors Published Month"))
    md.append("")
    md.append(md_image("raw_naver_pub_by_day_authors.png", "Authors Published Day"))
    md.append("")
    md.append("### Publishers")
    md.append(md_image("raw_naver_pub_by_year_publishers.png", "Publishers Published Year"))
    md.append("")
    md.append(md_image("raw_naver_pub_by_month_publishers.png", "Publishers Published Month"))
    md.append("")
    md.append(md_image("raw_naver_pub_by_day_publishers.png", "Publishers Published Day"))
    md.append("")
    md.append("</details>")
    md.append("")


    # 핵심: Monthly 3종만 상단에 노출 (스크롤 최소화)
    md.append("## 📊 Monthly Overview (New + Cumulative)")
    md.append("")
    md.append("### Books")
    md.append(md_image("raw_naver_by_month.png", "Books Month"))
    md.append("")
    md.append("### Authors")
    md.append(md_image("raw_naver_by_month_authors.png", "Authors Month"))
    md.append("")
    md.append("### Publishers")
    md.append(md_image("raw_naver_by_month_publishers.png", "Publishers Month"))
    md.append("")

    # Details (fold)
    md.append("<details>")
    md.append("<summary>📅 Yearly Details</summary>")
    md.append("")
    md.append("### Books")
    md.append(md_image("raw_naver_by_year.png", "Books Year"))
    md.append("")
    md.append("### Authors")
    md.append(md_image("raw_naver_by_year_authors.png", "Authors Year"))
    md.append("")
    md.append("### Publishers")
    md.append(md_image("raw_naver_by_year_publishers.png", "Publishers Year"))
    md.append("")
    md.append("</details>")
    md.append("")

    md.append("<details>")
    md.append("<summary>📆 Daily Details</summary>")
    md.append("")
    md.append("### Books")
    md.append(md_image("raw_naver_by_day.png", "Books Day"))
    md.append("")
    md.append("### Authors")
    md.append(md_image("raw_naver_by_day_authors.png", "Authors Day"))
    md.append("")
    md.append("### Publishers")
    md.append(md_image("raw_naver_by_day_publishers.png", "Publishers Day"))
    md.append("")
    md.append("</details>")
    md.append("")

    md.append("<details>")
    md.append("<summary>⏱ Hourly Details</summary>")
    md.append("")
    md.append("### Books")
    md.append(md_image("raw_naver_by_hour.png", "Books Hour"))
    md.append("")
    md.append("### Authors")
    md.append(md_image("raw_naver_by_hour_authors.png", "Authors Hour"))
    md.append("")
    md.append("### Publishers")
    md.append(md_image("raw_naver_by_hour_publishers.png", "Publishers Hour"))
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
