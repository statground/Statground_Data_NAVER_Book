use anyhow::{Context, Result};
use rand::seq::SliceRandom;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use statground_naver_book::clickhouse_http::{ClickHouseConfig, ClickHouseHttp};
use statground_naver_book::env::{env_string, env_u32, env_usize};
use statground_naver_book::naver::NaverClient;
use statground_naver_book::raw_naver::{get_table_columns, sample_rows_for_terms, upsert_naver_items};
use statground_naver_book::term_extract::{generate_keyword, pick_unique_terms, sanitize_keyword};
use statground_naver_book::time_helpers::{format_clickhouse_datetime, now_kst};
use std::collections::HashSet;

const TABLE_NAME: &str = "raw_naver";

fn collect_for_term(
    ch: &ClickHouseHttp,
    naver: &NaverClient,
    table_columns: &HashSet<String>,
    term: &str,
    mode: &str,
    display: u32,
    reqs_per_term: usize,
) -> Result<()> {
    let mut rng = rand::thread_rng();
    let sorts = if reqs_per_term <= 1 {
        vec![["sim", "date"].choose(&mut rng).copied().unwrap_or("sim")]
    } else {
        vec!["sim", "date"]
    };

    for sort in sorts {
        let items = naver.fetch_items(term, sort, 1, display)?;
        if items.is_empty() {
            continue;
        }

        let now = now_kst();
        let now_ch = format_clickhouse_datetime(now);
        let version = now.timestamp();
        let updated_log = format!("auto_upsert|mode={mode}|term={term}|sort={sort}");
        let inserted = upsert_naver_items(
            ch,
            TABLE_NAME,
            table_columns,
            &items,
            version,
            &now_ch,
            "github_actions_auto",
            &updated_log,
        )?;

        println!(
            "[COLLECT] mode={} term='{}' sort={} items={} inserted={}",
            mode,
            term,
            sort,
            items.len(),
            inserted
        );
    }

    Ok(())
}

fn main() -> Result<()> {
    let ch = ClickHouseHttp::new(ClickHouseConfig::from_env_required()?)?;
    let naver = NaverClient::from_env()?;

    let mode = env_string("COLLECT_MODE", "mixed").to_lowercase();
    let batch_size = env_usize("BATCH_SIZE", 1000);
    let sample_rows = env_usize("SAMPLE_ROWS", 8000);
    let display = env_u32("NAVER_DISPLAY", 100);
    let reqs_per_term = env_usize("REQS_PER_TERM", 1);
    let max_workers = env_usize("MAX_WORKERS", 8);

    let database = ch
        .cfg
        .database
        .clone()
        .context("CH_DATABASE is required")?;
    let table_columns = get_table_columns(&ch, &database, TABLE_NAME)?;

    let samples = match sample_rows_for_terms(&ch, TABLE_NAME, if mode == "mixed" { 100 } else { sample_rows }) {
        Ok(rows) => rows,
        Err(err) => {
            eprintln!("[WARN] failed to sample rows for term generation: {err}");
            Vec::new()
        }
    };

    if mode == "mixed" {
        let term = sanitize_keyword(Some(&generate_keyword(&samples)));
        collect_for_term(&ch, &naver, &table_columns, &term, &mode, display, reqs_per_term)?;
        return Ok(());
    }

    let mut terms = pick_unique_terms(&samples, &mode, batch_size);
    terms.shuffle(&mut rand::thread_rng());

    let pool = ThreadPoolBuilder::new()
        .num_threads(max_workers.max(1))
        .build()
        .context("failed to build rayon thread pool")?;

    pool.install(|| {
        terms
            .par_iter()
            .try_for_each(|term| collect_for_term(&ch, &naver, &table_columns, term, &mode, display, reqs_per_term))
    })?;

    Ok(())
}
