use crate::env::{env_i64_with_fallback, env_string, env_string_with_fallback, require_env};
use anyhow::{anyhow, bail, Context, Result};
use reqwest::blocking::Client;
use reqwest::header::CONTENT_TYPE;
use serde_json::{Map, Value};
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct ClickHouseConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: Option<String>,
    pub scheme: String,
}

impl ClickHouseConfig {
    pub fn from_env_required() -> Result<Self> {
        Ok(Self {
            host: require_env("CH_HOST")?,
            port: require_env("CH_PORT")?.parse::<u16>().context("CH_PORT must be a valid integer")?,
            user: require_env("CH_USER")?,
            password: require_env("CH_PASSWORD")?,
            database: Some(require_env("CH_DATABASE")?),
            scheme: env_string("CH_SCHEME", "http"),
        })
    }

    pub fn from_env_with_fallbacks() -> Result<Self> {
        let host = env_string_with_fallback(&["CH_HOST", "CLICKHOUSE_HOST"], "");
        if host.trim().is_empty() {
            bail!("Missing CH_HOST (or CLICKHOUSE_HOST)");
        }

        let port = env_i64_with_fallback(&["CH_PORT", "CLICKHOUSE_PORT"], 8123) as u16;
        let user = env_string_with_fallback(&["CH_USER", "CLICKHOUSE_USER"], "default");
        let password = env_string_with_fallback(&["CH_PASSWORD", "CLICKHOUSE_PASSWORD"], "");
        let database = env_string_with_fallback(&["CH_DATABASE", "CLICKHOUSE_DATABASE"], "");
        let scheme = env_string("CH_SCHEME", "http");

        Ok(Self {
            host,
            port,
            user,
            password,
            database: if database.trim().is_empty() { None } else { Some(database) },
            scheme,
        })
    }

    pub fn base_url(&self) -> String {
        match &self.database {
            Some(database) if !database.trim().is_empty() => {
                format!("{}://{}:{}/?database={}&wait_end_of_query=1", self.scheme, self.host, self.port, database)
            }
            _ => format!("{}://{}:{}/?wait_end_of_query=1", self.scheme, self.host, self.port),
        }
    }
}

#[derive(Clone)]
pub struct ClickHouseHttp {
    client: Client,
    pub cfg: ClickHouseConfig,
}

impl ClickHouseHttp {
    pub fn new(cfg: ClickHouseConfig) -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(300))
            .connect_timeout(Duration::from_secs(20))
            .build()
            .context("failed to build reqwest client for ClickHouse")?;

        Ok(Self { client, cfg })
    }

    fn send_sql(&self, sql: &str) -> Result<String> {
        let response = self
            .client
            .post(self.cfg.base_url())
            .basic_auth(&self.cfg.user, Some(&self.cfg.password))
            .header(CONTENT_TYPE, "text/plain; charset=utf-8")
            .body(sql.to_string())
            .send()
            .context("failed to send ClickHouse HTTP request")?;

        let status = response.status();
        let body = response.text().context("failed to read ClickHouse response body")?;
        if !status.is_success() {
            bail!("ClickHouse HTTP error {}: {}", status, body);
        }
        Ok(body)
    }

    pub fn command(&self, sql: &str) -> Result<()> {
        self.send_sql(trim_sql(sql)).map(|_| ())
    }

    pub fn scalar_string(&self, sql: &str) -> Result<Option<String>> {
        let value = self.query_tsv_raw(sql)?.into_iter().next();
        Ok(match value {
            Some(v) if !v.trim().is_empty() && v.trim() != "\\N" && v.trim().to_uppercase() != "NULL" => Some(v),
            _ => None,
        })
    }

    pub fn scalar_i64(&self, sql: &str) -> Result<i64> {
        match self.scalar_string(sql)? {
            Some(v) if !v.trim().is_empty() => v
                .trim()
                .parse::<i64>()
                .with_context(|| format!("failed to parse ClickHouse scalar as i64: {v}")),
            _ => Ok(0),
        }
    }

    pub fn query_tsv_raw(&self, sql: &str) -> Result<Vec<String>> {
        let body = format!("{}\nFORMAT TSVRaw", trim_sql(sql));
        let text = self.send_sql(&body)?;
        Ok(text
            .lines()
            .map(|line| line.trim_end_matches('\r').to_string())
            .filter(|line| !line.trim().is_empty())
            .collect())
    }

    pub fn query_json_rows(&self, sql: &str) -> Result<Vec<Map<String, Value>>> {
        let body = format!("{}\nFORMAT JSONEachRow", trim_sql(sql));
        let text = self.send_sql(&body)?;
        let mut rows = Vec::new();
        for line in text.lines().map(str::trim).filter(|line| !line.is_empty()) {
            let value: Value = serde_json::from_str(line)
                .with_context(|| format!("failed to parse JSONEachRow line from ClickHouse: {line}"))?;
            let object = value
                .as_object()
                .cloned()
                .ok_or_else(|| anyhow!("ClickHouse JSONEachRow row is not an object: {line}"))?;
            rows.push(object);
        }
        Ok(rows)
    }

    pub fn query_pairs(&self, sql: &str, key_field: &str, value_field: &str) -> Result<Vec<(String, u64)>> {
        let rows = self.query_json_rows(sql)?;
        rows.into_iter()
            .map(|row| {
                let key = row
                    .get(key_field)
                    .map(json_value_to_string)
                    .unwrap_or_default();
                let value = row
                    .get(value_field)
                    .map(json_value_to_u64)
                    .transpose()?
                    .unwrap_or(0);
                Ok((key, value))
            })
            .collect()
    }

    pub fn query_single_column_strings(&self, sql: &str, column: &str) -> Result<Vec<String>> {
        let rows = self.query_json_rows(sql)?;
        Ok(rows
            .into_iter()
            .filter_map(|row| row.get(column).map(json_value_to_string))
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
            .collect())
    }

    pub fn insert_json_each_row(&self, table: &str, rows: &[Map<String, Value>]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        let mut body = format!("INSERT INTO {} FORMAT JSONEachRow\n", table);
        for row in rows {
            body.push_str(&serde_json::to_string(row).context("failed to serialize row to JSONEachRow")?);
            body.push('\n');
        }

        self.send_sql(&body).map(|_| ())
    }
}

pub fn trim_sql(sql: &str) -> &str {
    sql.trim().trim_end_matches(';').trim()
}

pub fn json_value_to_string(value: &Value) -> String {
    match value {
        Value::String(v) => v.clone(),
        Value::Number(v) => v.to_string(),
        Value::Bool(v) => {
            if *v { "1".to_string() } else { "0".to_string() }
        }
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

pub fn json_value_to_u64(value: &Value) -> Result<u64> {
    match value {
        Value::Number(v) => v
            .as_u64()
            .or_else(|| v.as_i64().and_then(|i| u64::try_from(i).ok()))
            .ok_or_else(|| anyhow!("numeric value cannot be represented as u64: {v}")),
        Value::String(v) => v
            .trim()
            .parse::<u64>()
            .with_context(|| format!("failed to parse numeric string as u64: {v}")),
        Value::Null => Ok(0),
        other => bail!("expected numeric JSON value, got: {other}"),
    }
}

pub fn sql_string_literal(input: &str) -> String {
    let escaped = input.replace('\\', "\\\\").replace('\'', "\\'");
    format!("'{}'", escaped)
}

pub fn join_string_literals(values: &[String]) -> String {
    values
        .iter()
        .map(|v| sql_string_literal(v))
        .collect::<Vec<_>>()
        .join(",")
}
