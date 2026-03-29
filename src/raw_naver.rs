use crate::clickhouse_http::{join_string_literals, json_value_to_string, ClickHouseHttp};
use crate::naver::NaverBookItem;
use anyhow::{anyhow, Context, Result};
use serde_json::{Map, Number, Value};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

#[derive(Debug, Clone, Default)]
pub struct ExistingRawNaver {
    pub uuid: String,
    pub created_at: String,
    pub created_log: String,
}

#[derive(Debug, Clone, Default)]
pub struct TermSampleRow {
    pub title: String,
    pub author: String,
    pub publisher: String,
}

pub fn get_table_columns(ch: &ClickHouseHttp, database: &str, table: &str) -> Result<HashSet<String>> {
    let sql = format!(
        "SELECT name FROM system.columns WHERE database = '{}' AND table = '{}' ORDER BY position",
        database.replace('\\', "\\\\").replace('\'', "\\'"),
        table.replace('\\', "\\\\").replace('\'', "\\'"),
    );
    Ok(ch
        .query_single_column_strings(&sql, "name")?
        .into_iter()
        .collect::<HashSet<_>>())
}

pub fn filter_row_by_existing_columns(row: Map<String, Value>, columns: &HashSet<String>) -> Map<String, Value> {
    row.into_iter()
        .filter(|(k, _)| columns.contains(k))
        .collect::<Map<String, Value>>()
}

pub fn sample_rows_for_terms(ch: &ClickHouseHttp, table: &str, limit: usize) -> Result<Vec<TermSampleRow>> {
    let sql = format!(
        "SELECT title, author, publisher FROM {} ORDER BY rand() LIMIT {}",
        table, limit
    );
    let rows = ch.query_json_rows(&sql)?;
    Ok(rows
        .into_iter()
        .map(|row| TermSampleRow {
            title: row.get("title").map(json_value_to_string).unwrap_or_default(),
            author: row.get("author").map(json_value_to_string).unwrap_or_default(),
            publisher: row.get("publisher").map(json_value_to_string).unwrap_or_default(),
        })
        .collect())
}

pub fn build_uuid_map(ch: &ClickHouseHttp, table: &str, isbns: &[String]) -> Result<HashMap<String, String>> {
    let filtered: Vec<String> = isbns
        .iter()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .collect();
    if filtered.is_empty() {
        return Ok(HashMap::new());
    }

    let sql = format!(
        "SELECT isbn, uuid FROM {} WHERE isbn IN ({}) LIMIT 100000",
        table,
        join_string_literals(&filtered),
    );
    let rows = ch.query_json_rows(&sql)?;
    let mut out = HashMap::new();
    for row in rows {
        let isbn = row.get("isbn").map(json_value_to_string).unwrap_or_default();
        let uuid = row.get("uuid").map(json_value_to_string).unwrap_or_default();
        if !isbn.is_empty() && !uuid.is_empty() {
            out.insert(isbn, uuid);
        }
    }
    Ok(out)
}

pub fn build_existing_map(
    ch: &ClickHouseHttp,
    table: &str,
    has_version_column: bool,
    isbns: &[String],
) -> Result<HashMap<String, ExistingRawNaver>> {
    let filtered: Vec<String> = isbns
        .iter()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .collect();
    if filtered.is_empty() {
        return Ok(HashMap::new());
    }

    let order_expr = if has_version_column { "version DESC" } else { "created_at DESC" };
    let sql = format!(
        "SELECT isbn, uuid, created_at, created_log FROM {} WHERE isbn IN ({}) ORDER BY {} LIMIT 1 BY isbn",
        table,
        join_string_literals(&filtered),
        order_expr,
    );

    let rows = ch.query_json_rows(&sql)?;
    let mut out = HashMap::new();
    for row in rows {
        let isbn = row.get("isbn").map(json_value_to_string).unwrap_or_default();
        if isbn.is_empty() {
            continue;
        }
        out.insert(
            isbn,
            ExistingRawNaver {
                uuid: row.get("uuid").map(json_value_to_string).unwrap_or_default(),
                created_at: row.get("created_at").map(json_value_to_string).unwrap_or_default(),
                created_log: row.get("created_log").map(json_value_to_string).unwrap_or_default(),
            },
        );
    }

    Ok(out)
}

pub fn build_raw_naver_row(
    item: &NaverBookItem,
    uuid: &str,
    version: i64,
    created_at: &str,
    created_log: &str,
    updated_at: &str,
    updated_log: &str,
) -> Result<Map<String, Value>> {
    let isbn = item.isbn.clone().unwrap_or_default().trim().to_string();
    if isbn.is_empty() {
        return Err(anyhow!("isbn is required to build raw_naver row"));
    }

    let mut row = Map::new();
    row.insert("uuid".to_string(), Value::String(uuid.to_string()));
    row.insert("version".to_string(), Value::Number(Number::from(version)));
    row.insert("created_at".to_string(), Value::String(created_at.to_string()));
    row.insert("created_log".to_string(), Value::String(created_log.to_string()));
    row.insert("updated_at".to_string(), Value::String(updated_at.to_string()));
    row.insert("updated_log".to_string(), Value::String(updated_log.to_string()));
    row.insert(
        "title".to_string(),
        Value::String(item.title.clone().unwrap_or_default()),
    );
    row.insert(
        "link".to_string(),
        Value::String(item.link.clone().unwrap_or_default()),
    );
    row.insert(
        "image".to_string(),
        Value::String(item.image.clone().unwrap_or_default()),
    );
    row.insert(
        "author".to_string(),
        Value::String(item.author.clone().unwrap_or_default()),
    );
    row.insert(
        "publisher".to_string(),
        Value::String(item.publisher.clone().unwrap_or_default()),
    );
    row.insert("isbn".to_string(), Value::String(isbn));
    row.insert(
        "description".to_string(),
        Value::String(item.description.clone().unwrap_or_default()),
    );
    row.insert(
        "pubdate".to_string(),
        Value::String(item.pubdate.clone().unwrap_or_default()),
    );

    let discount_value = item
        .discount
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .and_then(|v| v.parse::<i64>().ok())
        .map(Number::from)
        .map(Value::Number)
        .unwrap_or(Value::Null);
    row.insert("discount".to_string(), discount_value);

    Ok(row)
}

pub fn upsert_naver_items(
    ch: &ClickHouseHttp,
    table: &str,
    table_columns: &HashSet<String>,
    items: &[NaverBookItem],
    version: i64,
    now: &str,
    created_log_for_new: &str,
    updated_log: &str,
) -> Result<usize> {
    let isbns = items
        .iter()
        .filter_map(|item| item.isbn.as_ref())
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .collect::<Vec<_>>();

    let existing_map = build_existing_map(ch, table, table_columns.contains("version"), &isbns)?;
    let mut rows = Vec::new();

    for item in items {
        let isbn = item.isbn.clone().unwrap_or_default().trim().to_string();
        if isbn.is_empty() {
            continue;
        }

        let (uuid, created_at, created_log) = match existing_map.get(&isbn) {
            Some(existing) if !existing.uuid.trim().is_empty() => (
                existing.uuid.clone(),
                if existing.created_at.trim().is_empty() {
                    now.to_string()
                } else {
                    existing.created_at.clone()
                },
                if existing.created_log.trim().is_empty() {
                    created_log_for_new.to_string()
                } else {
                    existing.created_log.clone()
                },
            ),
            _ => (
                Uuid::now_v7().to_string(),
                now.to_string(),
                created_log_for_new.to_string(),
            ),
        };

        let row = build_raw_naver_row(item, &uuid, version, &created_at, &created_log, now, updated_log)?;
        rows.push(filter_row_by_existing_columns(row, table_columns));
    }

    ch.insert_json_each_row(table, &rows)
        .with_context(|| format!("failed to insert {} rows into {}", rows.len(), table))?;
    Ok(rows.len())
}

pub fn manual_upsert_naver_items(
    ch: &ClickHouseHttp,
    table: &str,
    table_columns: &HashSet<String>,
    items: &[NaverBookItem],
    version: i64,
    now: &str,
    created_log: &str,
    updated_log: &str,
) -> Result<usize> {
    let isbns = items
        .iter()
        .filter_map(|item| item.isbn.as_ref())
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .collect::<Vec<_>>();

    let uuid_map = build_uuid_map(ch, table, &isbns)?;
    let mut rows = Vec::new();

    for item in items {
        let isbn = item.isbn.clone().unwrap_or_default().trim().to_string();
        if isbn.is_empty() {
            continue;
        }
        let uuid = uuid_map.get(&isbn).cloned().unwrap_or_else(|| Uuid::now_v7().to_string());
        let row = build_raw_naver_row(item, &uuid, version, now, created_log, now, updated_log)?;
        rows.push(filter_row_by_existing_columns(row, table_columns));
    }

    ch.insert_json_each_row(table, &rows)
        .with_context(|| format!("failed to insert {} rows into {}", rows.len(), table))?;
    Ok(rows.len())
}
