use anyhow::{bail, Context, Result};
use statground_naver_book::clickhouse_http::{ClickHouseConfig, ClickHouseHttp};
use statground_naver_book::env::{env_string, require_env};
use statground_naver_book::naver::NaverClient;
use statground_naver_book::raw_naver::{get_table_columns, manual_upsert_naver_items};
use statground_naver_book::time_helpers::{format_clickhouse_datetime, now_kst};

const TABLE_NAME: &str = "raw_naver";

fn main() -> Result<()> {
    let ch = ClickHouseHttp::new(ClickHouseConfig::from_env_required()?)?;
    let naver = NaverClient::from_env()?;
    let keyword = require_env("MANUAL_KEYWORD")?;
    if keyword.trim().is_empty() {
        bail!("MANUAL_KEYWORD is empty");
    }

    let database = ch
        .cfg
        .database
        .clone()
        .context("CH_DATABASE is required")?;
    let table_columns = get_table_columns(&ch, &database, TABLE_NAME)?;

    for sort in ["sim", "date"] {
        let mut start = 1_u32;
        while start <= 1000 {
            let items = naver.fetch_items(&keyword, sort, start, 100)?;
            if items.is_empty() {
                break;
            }

            let now = now_kst();
            let now_ch = format_clickhouse_datetime(now);
            let version = now.timestamp();
            let updated_log = format!("manual_upsert|keyword={}|sort={}", keyword, sort);
            let inserted = manual_upsert_naver_items(
                &ch,
                TABLE_NAME,
                &table_columns,
                &items,
                version,
                &now_ch,
                "github_actions_manual",
                &updated_log,
            )?;

            println!(
                "[MANUAL] keyword='{}' sort={} start={} items={} inserted={}",
                keyword,
                sort,
                start,
                items.len(),
                inserted
            );

            if items.len() < 100 {
                break;
            }
            start += 100;
        }
    }

    Ok(())
}
