use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EntityKind {
    Books,
    Authors,
    Publishers,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Granularity {
    Year,
    Month,
    Day,
    Hour,
}

pub type Series = Vec<(String, u64)>;

#[derive(Debug, Clone, Default)]
pub struct StatsSnapshot {
    pub data_updated_at: Option<String>,
    pub total_books: u64,
    pub total_authors: u64,
    pub total_publishers: u64,
    pub total_pub_year: u64,
    pub total_pub_year_only: u64,
    pub total_pub_ym: u64,
    pub total_pub_ymd: u64,
    pub total_pub_missing: u64,
    pub created_series: HashMap<(EntityKind, Granularity), Series>,
    pub pubdate_series: HashMap<(EntityKind, Granularity), Series>,
    pub unknown_month_books: Series,
    pub unknown_day_books: Series,
}

pub const TABLE_NAME: &str = "raw_naver";
pub const OUT_DIR: &str = "stats";
pub const OUTPUT_MD: &str = "stats/raw_naver_stats.md";

pub fn base_isbn_expr() -> &'static str {
    "isbn IS NOT NULL AND length(trim(isbn)) > 0"
}

pub fn base_publisher_expr() -> &'static str {
    "publisher IS NOT NULL AND length(trim(publisher)) > 0"
}

pub fn total_books_sql(table: &str) -> String {
    format!("SELECT uniqExact(isbn) FROM {} WHERE {}", table, base_isbn_expr())
}

pub fn total_publishers_sql(table: &str) -> String {
    format!(
        "SELECT countDistinct(publisher) FROM {} WHERE {}",
        table,
        base_publisher_expr()
    )
}

pub fn total_authors_sql(table: &str) -> String {
    format!(
        r#"
SELECT countDistinct(author_one)
FROM (
    SELECT trim(author_one) AS author_one
    FROM {table}
    ARRAY JOIN splitByChar('^', ifNull(author, '')) AS author_one
    WHERE length(trim(author_one)) > 0
)
"#
    )
}

pub fn max_updated_at_sql(table: &str) -> String {
    format!("SELECT max(updated_at) FROM {}", table)
}

fn first_seen_base_cte(table: &str, entity: EntityKind) -> String {
    match entity {
        EntityKind::Books => format!(
            "SELECT isbn AS entity, min(created_at) AS first_at FROM {} WHERE {} GROUP BY entity",
            table,
            base_isbn_expr()
        ),
        EntityKind::Publishers => format!(
            "SELECT publisher AS entity, min(created_at) AS first_at FROM {} WHERE {} GROUP BY entity",
            table,
            base_publisher_expr()
        ),
        EntityKind::Authors => format!(
            r#"SELECT trim(author_one) AS entity, min(created_at) AS first_at
FROM {table}
ARRAY JOIN splitByChar('^', ifNull(author, '')) AS author_one
WHERE length(trim(author_one)) > 0
GROUP BY entity"#
        ),
    }
}

pub fn build_first_seen_series_sql(table: &str, entity: EntityKind, granularity: Granularity) -> String {
    let base_cte = first_seen_base_cte(table, entity);
    match granularity {
        Granularity::Year => format!(
            r#"
WITH first_seen AS (
    {base_cte}
),
params AS (
    SELECT
        toStartOfYear(min(first_at)) AS min_t,
        dateDiff('year', toStartOfYear(min(first_at)), toStartOfYear(max(first_at))) AS diff_t
    FROM first_seen
    HAVING count() > 0
),
counts AS (
    SELECT toYear(first_at) AS k, count() AS c
    FROM first_seen
    GROUP BY k
),
timeline AS (
    SELECT toYear(addYears(p.min_t, n)) AS k
    FROM params p
    ARRAY JOIN range(p.diff_t + 1) AS n
)
SELECT toString(t.k) AS k, toUInt64(ifNull(c.c, 0)) AS c
FROM timeline t
LEFT JOIN counts c ON c.k = t.k
ORDER BY k
"#
        ),
        Granularity::Month => format!(
            r#"
WITH first_seen AS (
    {base_cte}
),
params AS (
    SELECT
        toStartOfMonth(min(first_at)) AS min_t,
        dateDiff('month', toStartOfMonth(min(first_at)), toStartOfMonth(max(first_at))) AS diff_t
    FROM first_seen
    HAVING count() > 0
),
counts AS (
    SELECT toYYYYMM(first_at) AS k, count() AS c
    FROM first_seen
    GROUP BY k
),
timeline AS (
    SELECT toYYYYMM(addMonths(p.min_t, n)) AS k
    FROM params p
    ARRAY JOIN range(p.diff_t + 1) AS n
)
SELECT toString(t.k) AS k, toUInt64(ifNull(c.c, 0)) AS c
FROM timeline t
LEFT JOIN counts c ON c.k = t.k
ORDER BY k
"#
        ),
        Granularity::Day => format!(
            r#"
WITH first_seen AS (
    {base_cte}
),
params AS (
    SELECT
        min(toDate(first_at)) AS min_t,
        dateDiff('day', min(toDate(first_at)), max(toDate(first_at))) AS diff_t
    FROM first_seen
    HAVING count() > 0
),
counts AS (
    SELECT toDate(first_at) AS k, count() AS c
    FROM first_seen
    GROUP BY k
),
timeline AS (
    SELECT addDays(p.min_t, n) AS k
    FROM params p
    ARRAY JOIN range(p.diff_t + 1) AS n
)
SELECT toString(t.k) AS k, toUInt64(ifNull(c.c, 0)) AS c
FROM timeline t
LEFT JOIN counts c ON c.k = t.k
ORDER BY k
"#
        ),
        Granularity::Hour => format!(
            r#"
WITH first_seen AS (
    {base_cte}
),
params AS (
    SELECT
        toStartOfHour(min(first_at)) AS min_t,
        dateDiff('hour', toStartOfHour(min(first_at)), toStartOfHour(max(first_at))) AS diff_t
    FROM first_seen
    HAVING count() > 0
),
counts AS (
    SELECT toStartOfHour(first_at) AS k, count() AS c
    FROM first_seen
    GROUP BY k
),
timeline AS (
    SELECT addHours(p.min_t, n) AS k
    FROM params p
    ARRAY JOIN range(p.diff_t + 1) AS n
)
SELECT toString(t.k) AS k, toUInt64(ifNull(c.c, 0)) AS c
FROM timeline t
LEFT JOIN counts c ON c.k = t.k
ORDER BY k
"#
        ),
    }
}

pub fn pubdate_digits_expr() -> &'static str {
    "replaceRegexpAll(trim(pubdate), '[^0-9]', '')"
}

pub fn pubdate_len_expr() -> String {
    format!("length({})", pubdate_digits_expr())
}

pub fn pubdate_year_expr() -> String {
    format!("toUInt16OrZero(substring({}, 1, 4))", pubdate_digits_expr())
}

pub fn pubdate_month_expr() -> String {
    format!("toUInt8OrZero(substring({}, 5, 2))", pubdate_digits_expr())
}

pub fn pubdate_day_expr() -> String {
    format!("toUInt8OrZero(substring({}, 7, 2))", pubdate_digits_expr())
}

pub fn pub_valid_year_expr() -> String {
    let len = pubdate_len_expr();
    let year = pubdate_year_expr();
    format!("({len} >= 4) AND ({year} BETWEEN 1000 AND 2100)")
}

pub fn pub_valid_month_expr() -> String {
    let len = pubdate_len_expr();
    let month = pubdate_month_expr();
    let valid_year = pub_valid_year_expr();
    format!("({len} >= 6) AND {valid_year} AND ({month} BETWEEN 1 AND 12)")
}

pub fn pub_valid_day_expr() -> String {
    let len = pubdate_len_expr();
    let day = pubdate_day_expr();
    let valid_month = pub_valid_month_expr();
    format!("({len} >= 8) AND {valid_month} AND ({day} BETWEEN 1 AND 31)")
}

pub fn pub_month_start_expr() -> String {
    let year = pubdate_year_expr();
    let month = pubdate_month_expr();
    format!(
        "toDateOrNull(concat(toString({year}), '-', lpad(toString({month}), 2, '0'), '-01'))"
    )
}

pub fn pub_day_date_expr() -> String {
    let year = pubdate_year_expr();
    let month = pubdate_month_expr();
    let day = pubdate_day_expr();
    format!(
        "toDateOrNull(concat(toString({year}), '-', lpad(toString({month}), 2, '0'), '-', lpad(toString({day}), 2, '0')))"
    )
}

pub fn total_pub_year_sql(table: &str) -> String {
    format!(
        "SELECT uniqExact(isbn) FROM {} WHERE {} AND {}",
        table,
        base_isbn_expr(),
        pub_valid_year_expr()
    )
}

pub fn total_pub_year_only_sql(table: &str) -> String {
    format!(
        "SELECT uniqExact(isbn) FROM {} WHERE {} AND {} AND {} = 4",
        table,
        base_isbn_expr(),
        pub_valid_year_expr(),
        pubdate_len_expr()
    )
}

pub fn total_pub_ym_sql(table: &str) -> String {
    format!(
        "SELECT uniqExact(isbn) FROM {} WHERE {} AND {} AND {} = 6",
        table,
        base_isbn_expr(),
        pub_valid_year_expr(),
        pubdate_len_expr()
    )
}

pub fn total_pub_ymd_sql(table: &str) -> String {
    format!(
        "SELECT uniqExact(isbn) FROM {} WHERE {} AND {} AND {} >= 8",
        table,
        base_isbn_expr(),
        pub_valid_year_expr(),
        pubdate_len_expr()
    )
}

pub fn total_pub_missing_sql(table: &str) -> String {
    format!(
        "SELECT uniqExact(isbn) FROM {} WHERE {} AND NOT ({})",
        table,
        base_isbn_expr(),
        pub_valid_year_expr()
    )
}

fn pubdate_base_cte(table: &str, entity: EntityKind, bucket_expr: &str, where_expr: &str) -> String {
    match entity {
        EntityKind::Books => format!(
            "SELECT DISTINCT isbn AS entity, {bucket_expr} AS bucket FROM {table} WHERE {} AND {where_expr}",
            base_isbn_expr()
        ),
        EntityKind::Publishers => format!(
            "SELECT DISTINCT publisher AS entity, {bucket_expr} AS bucket FROM {table} WHERE {} AND {where_expr}",
            base_publisher_expr()
        ),
        EntityKind::Authors => format!(
            r#"SELECT DISTINCT trim(author_one) AS entity, {bucket_expr} AS bucket
FROM {table}
ARRAY JOIN splitByChar('^', ifNull(author, '')) AS author_one
WHERE length(trim(author_one)) > 0 AND {where_expr}"#
        ),
    }
}

pub fn build_pubdate_series_sql(table: &str, entity: EntityKind, granularity: Granularity) -> String {
    match granularity {
        Granularity::Year => {
            let year = pubdate_year_expr();
            let valid_year = pub_valid_year_expr();
            let base_cte = pubdate_base_cte(table, entity, &year, &valid_year);
            format!(
                r#"
WITH base AS (
    {base_cte}
),
params AS (
    SELECT
        toStartOfYear(toDate(min(bucket) * 10000 + 101)) AS min_t,
        dateDiff('year', toStartOfYear(toDate(min(bucket) * 10000 + 101)), toStartOfYear(toDate(max(bucket) * 10000 + 101))) AS diff_t
    FROM base
    HAVING count() > 0
),
counts AS (
    SELECT bucket AS k, count() AS c
    FROM base
    GROUP BY k
),
timeline AS (
    SELECT toYear(addYears(p.min_t, n)) AS k
    FROM params p
    ARRAY JOIN range(p.diff_t + 1) AS n
)
SELECT toString(t.k) AS k, toUInt64(ifNull(c.c, 0)) AS c
FROM timeline t
LEFT JOIN counts c ON c.k = t.k
ORDER BY k
"#
            )
        }
        Granularity::Month => {
            let month_start = pub_month_start_expr();
            let valid_month = pub_valid_month_expr();
            let base_cte = pubdate_base_cte(table, entity, &month_start, &valid_month);
            format!(
                r#"
WITH base AS (
    {base_cte}
),
params AS (
    SELECT
        toStartOfMonth(min(bucket)) AS min_t,
        dateDiff('month', toStartOfMonth(min(bucket)), toStartOfMonth(max(bucket))) AS diff_t
    FROM base
    HAVING count() > 0
),
counts AS (
    SELECT toYYYYMM(bucket) AS k, count() AS c
    FROM base
    GROUP BY k
),
timeline AS (
    SELECT toYYYYMM(addMonths(p.min_t, n)) AS k
    FROM params p
    ARRAY JOIN range(p.diff_t + 1) AS n
)
SELECT toString(t.k) AS k, toUInt64(ifNull(c.c, 0)) AS c
FROM timeline t
LEFT JOIN counts c ON c.k = t.k
ORDER BY k
"#
            )
        }
        Granularity::Day => {
            let day_date = pub_day_date_expr();
            let valid_day = pub_valid_day_expr();
            let base_cte = pubdate_base_cte(table, entity, &day_date, &valid_day);
            format!(
                r#"
WITH base AS (
    {base_cte}
),
params AS (
    SELECT
        min(bucket) AS min_t,
        dateDiff('day', min(bucket), max(bucket)) AS diff_t
    FROM base
    HAVING count() > 0
),
counts AS (
    SELECT toDate(bucket) AS k, count() AS c
    FROM base
    GROUP BY k
),
timeline AS (
    SELECT addDays(p.min_t, n) AS k
    FROM params p
    ARRAY JOIN range(p.diff_t + 1) AS n
)
SELECT toString(t.k) AS k, toUInt64(ifNull(c.c, 0)) AS c
FROM timeline t
LEFT JOIN counts c ON c.k = t.k
ORDER BY k
"#
            )
        }
        Granularity::Hour => String::new(),
    }
}

pub fn unknown_month_books_sql(table: &str) -> String {
    let year = pubdate_year_expr();
    format!(
        "SELECT toString({year}) AS k, toUInt64(uniqExact(isbn)) AS c FROM {} WHERE {} AND {} AND {} = 4 GROUP BY k ORDER BY k",
        table,
        base_isbn_expr(),
        pub_valid_year_expr(),
        pubdate_len_expr(),
    )
}

pub fn unknown_day_books_sql(table: &str) -> String {
    let year = pubdate_year_expr();
    let month = pubdate_month_expr();
    format!(
        "SELECT concat(toString({year}), '-', lpad(toString({month}), 2, '0')) AS k, toUInt64(uniqExact(isbn)) AS c FROM {} WHERE {} AND {} AND {} = 6 GROUP BY k ORDER BY k",
        table,
        base_isbn_expr(),
        pub_valid_month_expr(),
        pubdate_len_expr(),
    )
}

pub fn series_key(entity: EntityKind, granularity: Granularity) -> (EntityKind, Granularity) {
    (entity, granularity)
}

pub fn format_hour_label(raw: &str) -> String {
    let value = raw.trim();
    if value.len() >= 13 {
        value[..13].to_string()
    } else {
        value.to_string()
    }
}

pub fn generate_markdown(snapshot: &StatsSnapshot) -> String {
    let mut md = Vec::new();

    let get_pub = |entity: EntityKind, granularity: Granularity| {
        snapshot
            .pubdate_series
            .get(&(entity, granularity))
            .cloned()
            .unwrap_or_default()
    };

    let y_pub_books = get_pub(EntityKind::Books, Granularity::Year);
    let m_pub_books = get_pub(EntityKind::Books, Granularity::Month);
    let d_pub_books = get_pub(EntityKind::Books, Granularity::Day);

    md.push("# 수집 데이터 집계".to_string());
    md.push(String::new());
    md.push(format!(
        "- 데이터 기준 최종 수정 시각(KST): {}",
        snapshot
            .data_updated_at
            .clone()
            .unwrap_or_else(|| "N/A".to_string())
    ));
    md.push(String::new());
    md.push("## 전체".to_string());
    md.push(String::new());
    md.push(format!("- 총 고유 ISBN 수: **{:,}**", snapshot.total_books));
    md.push(format!("- 저자 수: **{:,}**", snapshot.total_authors));
    md.push(format!("- 출판사 수: **{:,}**", snapshot.total_publishers));
    md.push(String::new());
    md.push("![Totals](raw_naver_totals.png)".to_string());
    md.push(String::new());

    md.push("## 출간일(pubdate) 기준 통계".to_string());
    md.push(String::new());
    md.push(format!("- 출간연도(YYYY 이상) 파싱 가능 ISBN: **{:,}**", snapshot.total_pub_year));
    md.push(format!("  - 연도만(YYYY): **{:,}**", snapshot.total_pub_year_only));
    md.push(format!("  - 연/월(YYYYMM): **{:,}**", snapshot.total_pub_ym));
    md.push(format!("  - 연/월/일(YYYYMMDD+): **{:,}**", snapshot.total_pub_ymd));
    md.push(format!("- 출간일 파싱 불가/없음 ISBN: **{:,}**", snapshot.total_pub_missing));
    md.push(String::new());

    if !y_pub_books.is_empty() {
        md.push("### Books (Published Date)".to_string());
        md.push("![Books Published Year](raw_naver_pub_by_year_books.png)".to_string());
        md.push(String::new());
    }
    if !m_pub_books.is_empty() {
        md.push("![Books Published Month](raw_naver_pub_by_month_books.png)".to_string());
        md.push(String::new());
    }
    if !d_pub_books.is_empty() {
        md.push("![Books Published Day](raw_naver_pub_by_day_books.png)".to_string());
        md.push(String::new());
    }

    if !snapshot.unknown_month_books.is_empty() {
        md.push("### Books (Published Date) - UNKNOWN month (year-only)".to_string());
        md.push(String::new());
        md.push("| Year | ISBN Count |".to_string());
        md.push("|---:|---:|".to_string());
        for (year, count) in &snapshot.unknown_month_books {
            md.push(format!("| {} | {:,} |", year, count));
        }
        md.push(String::new());
    }

    if !snapshot.unknown_day_books.is_empty() {
        md.push("### Books (Published Date) - UNKNOWN day (year-month only)".to_string());
        md.push(String::new());
        md.push("| Year-Month | ISBN Count |".to_string());
        md.push("|---:|---:|".to_string());
        for (ym, count) in &snapshot.unknown_day_books {
            md.push(format!("| {} | {:,} |", ym, count));
        }
        md.push(String::new());
    }

    md.push("<details>".to_string());
    md.push("<summary>📚 Published Date Details (Authors/Publishers)</summary>".to_string());
    md.push(String::new());
    md.push("### Authors".to_string());
    md.push("![Authors Published Year](raw_naver_pub_by_year_authors.png)".to_string());
    md.push(String::new());
    md.push("![Authors Published Month](raw_naver_pub_by_month_authors.png)".to_string());
    md.push(String::new());
    md.push("![Authors Published Day](raw_naver_pub_by_day_authors.png)".to_string());
    md.push(String::new());
    md.push("### Publishers".to_string());
    md.push("![Publishers Published Year](raw_naver_pub_by_year_publishers.png)".to_string());
    md.push(String::new());
    md.push("![Publishers Published Month](raw_naver_pub_by_month_publishers.png)".to_string());
    md.push(String::new());
    md.push("![Publishers Published Day](raw_naver_pub_by_day_publishers.png)".to_string());
    md.push(String::new());
    md.push("</details>".to_string());
    md.push(String::new());

    md.push("## 📊 Monthly Overview (New + Cumulative)".to_string());
    md.push(String::new());
    md.push("### Books".to_string());
    md.push("![Books Month](raw_naver_by_month.png)".to_string());
    md.push(String::new());
    md.push("### Authors".to_string());
    md.push("![Authors Month](raw_naver_by_month_authors.png)".to_string());
    md.push(String::new());
    md.push("### Publishers".to_string());
    md.push("![Publishers Month](raw_naver_by_month_publishers.png)".to_string());
    md.push(String::new());

    md.push("<details>".to_string());
    md.push("<summary>📅 Yearly Details</summary>".to_string());
    md.push(String::new());
    md.push("### Books".to_string());
    md.push("![Books Year](raw_naver_by_year.png)".to_string());
    md.push(String::new());
    md.push("### Authors".to_string());
    md.push("![Authors Year](raw_naver_by_year_authors.png)".to_string());
    md.push(String::new());
    md.push("### Publishers".to_string());
    md.push("![Publishers Year](raw_naver_by_year_publishers.png)".to_string());
    md.push(String::new());
    md.push("</details>".to_string());
    md.push(String::new());

    md.push("<details>".to_string());
    md.push("<summary>📆 Daily Details</summary>".to_string());
    md.push(String::new());
    md.push("### Books".to_string());
    md.push("![Books Day](raw_naver_by_day.png)".to_string());
    md.push(String::new());
    md.push("### Authors".to_string());
    md.push("![Authors Day](raw_naver_by_day_authors.png)".to_string());
    md.push(String::new());
    md.push("### Publishers".to_string());
    md.push("![Publishers Day](raw_naver_by_day_publishers.png)".to_string());
    md.push(String::new());
    md.push("</details>".to_string());
    md.push(String::new());

    md.push("<details>".to_string());
    md.push("<summary>⏱ Hourly Details</summary>".to_string());
    md.push(String::new());
    md.push("### Books".to_string());
    md.push("![Books Hour](raw_naver_by_hour.png)".to_string());
    md.push(String::new());
    md.push("### Authors".to_string());
    md.push("![Authors Hour](raw_naver_by_hour_authors.png)".to_string());
    md.push(String::new());
    md.push("### Publishers".to_string());
    md.push("![Publishers Hour](raw_naver_by_hour_publishers.png)".to_string());
    md.push(String::new());
    md.push("</details>".to_string());
    md.push(String::new());

    md.push("> 시계열은 각 항목(ISBN/저자/출판사)의 '최초 등장 시각' 기준 신규 유입을 집계합니다. (빈 구간은 0으로 채움)".to_string());
    md.push("> 모든 시계열 차트는 **신규 유입(막대) + 누적(선)** 을 함께 표시합니다.".to_string());

    md.join("\n")
}
