use anyhow::Result;
use statground_naver_book::charts::{
    draw_bar_with_cumulative, draw_totals_chart, COLOR_AUTHORS, COLOR_BOOKS, COLOR_PUBLISHERS,
};
use statground_naver_book::clickhouse_http::{ClickHouseConfig, ClickHouseHttp};
use statground_naver_book::env::env_string;
use statground_naver_book::stats::{
    build_first_seen_series_sql, build_pubdate_series_sql, format_hour_label, generate_markdown,
    max_updated_at_sql, series_key, total_authors_sql, total_books_sql, total_pub_missing_sql,
    total_pub_year_only_sql, total_pub_year_sql, total_pub_ym_sql, total_pub_ymd_sql,
    total_publishers_sql, unknown_day_books_sql, unknown_month_books_sql, EntityKind,
    Granularity, StatsSnapshot, OUT_DIR, OUTPUT_MD, TABLE_NAME,
};
use std::fs;
use std::path::PathBuf;

fn query_series(ch: &ClickHouseHttp, sql: &str) -> Result<Vec<(String, u64)>> {
    ch.query_pairs(sql, "k", "c")
}

fn maybe_draw_created_chart(
    snapshot: &StatsSnapshot,
    entity: EntityKind,
    granularity: Granularity,
    title: &str,
    filename: &str,
    color: plotters::style::RGBColor,
    max_labels: usize,
) -> Result<()> {
    let series = snapshot
        .created_series
        .get(&series_key(entity, granularity))
        .cloned()
        .unwrap_or_default();
    if series.is_empty() {
        return Ok(());
    }

    let labels = if granularity == Granularity::Hour {
        series
            .iter()
            .map(|(k, _)| format_hour_label(k))
            .collect::<Vec<_>>()
    } else {
        series.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>()
    };
    let values = series.iter().map(|(_, v)| *v).collect::<Vec<_>>();
    draw_bar_with_cumulative(&PathBuf::from(OUT_DIR).join(filename), title, &labels, &values, color, max_labels)
}

fn maybe_draw_pubdate_chart(
    snapshot: &StatsSnapshot,
    entity: EntityKind,
    granularity: Granularity,
    title: &str,
    filename: &str,
    color: plotters::style::RGBColor,
    max_labels: usize,
) -> Result<()> {
    let series = snapshot
        .pubdate_series
        .get(&series_key(entity, granularity))
        .cloned()
        .unwrap_or_default();
    if series.is_empty() {
        return Ok(());
    }

    let labels = series.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>();
    let values = series.iter().map(|(_, v)| *v).collect::<Vec<_>>();
    draw_bar_with_cumulative(&PathBuf::from(OUT_DIR).join(filename), title, &labels, &values, color, max_labels)
}

fn main() -> Result<()> {
    let ch = ClickHouseHttp::new(ClickHouseConfig::from_env_required()?)?;
    let table = env_string("RAW_NAVER_TABLE", TABLE_NAME);

    fs::create_dir_all(OUT_DIR)?;

    let mut snapshot = StatsSnapshot::default();
    snapshot.data_updated_at = ch.scalar_string(&max_updated_at_sql(&table))?;
    snapshot.total_books = ch.scalar_i64(&total_books_sql(&table))?.max(0) as u64;
    snapshot.total_authors = ch.scalar_i64(&total_authors_sql(&table))?.max(0) as u64;
    snapshot.total_publishers = ch.scalar_i64(&total_publishers_sql(&table))?.max(0) as u64;
    snapshot.total_pub_year = ch.scalar_i64(&total_pub_year_sql(&table))?.max(0) as u64;
    snapshot.total_pub_year_only = ch.scalar_i64(&total_pub_year_only_sql(&table))?.max(0) as u64;
    snapshot.total_pub_ym = ch.scalar_i64(&total_pub_ym_sql(&table))?.max(0) as u64;
    snapshot.total_pub_ymd = ch.scalar_i64(&total_pub_ymd_sql(&table))?.max(0) as u64;
    snapshot.total_pub_missing = ch.scalar_i64(&total_pub_missing_sql(&table))?.max(0) as u64;

    for entity in [EntityKind::Books, EntityKind::Authors, EntityKind::Publishers] {
        for granularity in [
            Granularity::Year,
            Granularity::Month,
            Granularity::Day,
            Granularity::Hour,
        ] {
            let sql = build_first_seen_series_sql(&table, entity, granularity);
            let rows = query_series(&ch, &sql)?;
            snapshot.created_series.insert(series_key(entity, granularity), rows);
        }
    }

    for entity in [EntityKind::Books, EntityKind::Authors, EntityKind::Publishers] {
        for granularity in [Granularity::Year, Granularity::Month, Granularity::Day] {
            let sql = build_pubdate_series_sql(&table, entity, granularity);
            let rows = query_series(&ch, &sql)?;
            snapshot.pubdate_series.insert(series_key(entity, granularity), rows);
        }
    }

    snapshot.unknown_month_books = query_series(&ch, &unknown_month_books_sql(&table))?;
    snapshot.unknown_day_books = query_series(&ch, &unknown_day_books_sql(&table))?;

    let total_labels = vec!["Books".to_string(), "Authors".to_string(), "Publishers".to_string()];
    let total_values = vec![snapshot.total_books, snapshot.total_authors, snapshot.total_publishers];
    draw_totals_chart(&PathBuf::from(OUT_DIR).join("raw_naver_totals.png"), &total_labels, &total_values)?;

    maybe_draw_created_chart(
        &snapshot,
        EntityKind::Books,
        Granularity::Year,
        "Yearly (Books: New + Cumulative)",
        "raw_naver_by_year.png",
        COLOR_BOOKS,
        24,
    )?;
    maybe_draw_created_chart(
        &snapshot,
        EntityKind::Authors,
        Granularity::Year,
        "Yearly (Authors: New + Cumulative)",
        "raw_naver_by_year_authors.png",
        COLOR_AUTHORS,
        24,
    )?;
    maybe_draw_created_chart(
        &snapshot,
        EntityKind::Publishers,
        Granularity::Year,
        "Yearly (Publishers: New + Cumulative)",
        "raw_naver_by_year_publishers.png",
        COLOR_PUBLISHERS,
        24,
    )?;

    maybe_draw_created_chart(
        &snapshot,
        EntityKind::Books,
        Granularity::Month,
        "Monthly (Books: New + Cumulative)",
        "raw_naver_by_month.png",
        COLOR_BOOKS,
        24,
    )?;
    maybe_draw_created_chart(
        &snapshot,
        EntityKind::Authors,
        Granularity::Month,
        "Monthly (Authors: New + Cumulative)",
        "raw_naver_by_month_authors.png",
        COLOR_AUTHORS,
        24,
    )?;
    maybe_draw_created_chart(
        &snapshot,
        EntityKind::Publishers,
        Granularity::Month,
        "Monthly (Publishers: New + Cumulative)",
        "raw_naver_by_month_publishers.png",
        COLOR_PUBLISHERS,
        24,
    )?;

    maybe_draw_created_chart(
        &snapshot,
        EntityKind::Books,
        Granularity::Day,
        "Daily (Books: New + Cumulative)",
        "raw_naver_by_day.png",
        COLOR_BOOKS,
        24,
    )?;
    maybe_draw_created_chart(
        &snapshot,
        EntityKind::Authors,
        Granularity::Day,
        "Daily (Authors: New + Cumulative)",
        "raw_naver_by_day_authors.png",
        COLOR_AUTHORS,
        24,
    )?;
    maybe_draw_created_chart(
        &snapshot,
        EntityKind::Publishers,
        Granularity::Day,
        "Daily (Publishers: New + Cumulative)",
        "raw_naver_by_day_publishers.png",
        COLOR_PUBLISHERS,
        24,
    )?;

    maybe_draw_created_chart(
        &snapshot,
        EntityKind::Books,
        Granularity::Hour,
        "Hourly (Books: New + Cumulative)",
        "raw_naver_by_hour.png",
        COLOR_BOOKS,
        24,
    )?;
    maybe_draw_created_chart(
        &snapshot,
        EntityKind::Authors,
        Granularity::Hour,
        "Hourly (Authors: New + Cumulative)",
        "raw_naver_by_hour_authors.png",
        COLOR_AUTHORS,
        24,
    )?;
    maybe_draw_created_chart(
        &snapshot,
        EntityKind::Publishers,
        Granularity::Hour,
        "Hourly (Publishers: New + Cumulative)",
        "raw_naver_by_hour_publishers.png",
        COLOR_PUBLISHERS,
        24,
    )?;

    maybe_draw_pubdate_chart(
        &snapshot,
        EntityKind::Books,
        Granularity::Year,
        "Yearly (Books by Published Date)",
        "raw_naver_pub_by_year_books.png",
        COLOR_BOOKS,
        24,
    )?;
    maybe_draw_pubdate_chart(
        &snapshot,
        EntityKind::Books,
        Granularity::Month,
        "Monthly (Books by Published Date)",
        "raw_naver_pub_by_month_books.png",
        COLOR_BOOKS,
        24,
    )?;
    maybe_draw_pubdate_chart(
        &snapshot,
        EntityKind::Books,
        Granularity::Day,
        "Daily (Books by Published Date)",
        "raw_naver_pub_by_day_books.png",
        COLOR_BOOKS,
        24,
    )?;

    maybe_draw_pubdate_chart(
        &snapshot,
        EntityKind::Publishers,
        Granularity::Year,
        "Yearly (Publishers by Published Date)",
        "raw_naver_pub_by_year_publishers.png",
        COLOR_PUBLISHERS,
        24,
    )?;
    maybe_draw_pubdate_chart(
        &snapshot,
        EntityKind::Publishers,
        Granularity::Month,
        "Monthly (Publishers by Published Date)",
        "raw_naver_pub_by_month_publishers.png",
        COLOR_PUBLISHERS,
        24,
    )?;
    maybe_draw_pubdate_chart(
        &snapshot,
        EntityKind::Publishers,
        Granularity::Day,
        "Daily (Publishers by Published Date)",
        "raw_naver_pub_by_day_publishers.png",
        COLOR_PUBLISHERS,
        24,
    )?;

    maybe_draw_pubdate_chart(
        &snapshot,
        EntityKind::Authors,
        Granularity::Year,
        "Yearly (Authors by Published Date)",
        "raw_naver_pub_by_year_authors.png",
        COLOR_AUTHORS,
        24,
    )?;
    maybe_draw_pubdate_chart(
        &snapshot,
        EntityKind::Authors,
        Granularity::Month,
        "Monthly (Authors by Published Date)",
        "raw_naver_pub_by_month_authors.png",
        COLOR_AUTHORS,
        24,
    )?;
    maybe_draw_pubdate_chart(
        &snapshot,
        EntityKind::Authors,
        Granularity::Day,
        "Daily (Authors by Published Date)",
        "raw_naver_pub_by_day_authors.png",
        COLOR_AUTHORS,
        24,
    )?;

    let markdown = generate_markdown(&snapshot);
    fs::write(OUTPUT_MD, markdown)?;

    println!("[STATS] wrote markdown={} and charts under {}", OUTPUT_MD, OUT_DIR);
    Ok(())
}
