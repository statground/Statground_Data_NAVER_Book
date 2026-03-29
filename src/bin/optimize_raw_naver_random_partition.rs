use anyhow::Result;
use rand::seq::SliceRandom;
use statground_naver_book::clickhouse_http::{ClickHouseConfig, ClickHouseHttp};
use statground_naver_book::env::{env_i64, optional_env};

fn main() -> Result<()> {
    if env_i64("OPTIMIZE_ENABLED", 1) != 1 {
        println!("[OPTIMIZE] skipped (OPTIMIZE_ENABLED!=1)");
        return Ok(());
    }

    let cfg = match ClickHouseConfig::from_env_with_fallbacks() {
        Ok(cfg) => cfg,
        Err(err) => {
            println!("[OPTIMIZE] {}. skip.", err);
            return Ok(());
        }
    };
    let ch = ClickHouseHttp::new(cfg.clone())?;

    let table = optional_env("OPTIMIZE_TABLE").unwrap_or_else(|| {
        cfg.database
            .as_ref()
            .filter(|db| !db.trim().is_empty())
            .map(|db| format!("{}.raw_naver", db))
            .unwrap_or_else(|| "raw_naver".to_string())
    });

    let recent_n = env_i64("OPTIMIZE_RECENT_PARTITIONS", 6).max(1);
    let do_final = env_i64("OPTIMIZE_FINAL", 1) == 1;

    let sql = format!(
        r#"
SELECT DISTINCT toYYYYMM(created_at) AS p
FROM {table}
WHERE created_at >= now() - INTERVAL 365 DAY
ORDER BY p DESC
LIMIT {recent_n}
"#
    );
    let mut parts = ch.query_single_column_strings(&sql, "p")?;
    if parts.is_empty() {
        println!("[OPTIMIZE] no partitions found for table={}. skip.", table);
        return Ok(());
    }

    let chosen = parts
        .choose(&mut rand::thread_rng())
        .cloned()
        .unwrap_or_else(|| parts[0].clone());
    let stmt = if do_final {
        format!("OPTIMIZE TABLE {} PARTITION {} FINAL", table, chosen)
    } else {
        format!("OPTIMIZE TABLE {} PARTITION {}", table, chosen)
    };

    println!("[OPTIMIZE] partitions(recent={})={:?} -> chosen={}", recent_n, parts, chosen);
    println!("[OPTIMIZE] running: {}", stmt);
    ch.command(&stmt)?;
    println!("[OPTIMIZE] done.");
    Ok(())
}
