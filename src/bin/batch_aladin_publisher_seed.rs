use anyhow::{bail, Context, Result};
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use regex::Regex;
use reqwest::blocking::Client;
use scraper::{Html, Selector};
use serde_json::{Map, Number, Value};
use statground_naver_book::clickhouse_http::{ClickHouseConfig, ClickHouseHttp};
use statground_naver_book::env::{env_f64, env_string, env_u32, env_usize};
use statground_naver_book::naver::NaverClient;
use statground_naver_book::raw_naver::{get_table_columns, upsert_naver_items};
use statground_naver_book::time_helpers::{format_clickhouse_datetime, now_kst, sleep_random};
use std::collections::HashSet;
use uuid::Uuid;

fn http_client(timeout_secs: u64) -> Result<Client> {
    Client::builder()
        .timeout(std::time::Duration::from_secs(timeout_secs))
        .build()
        .context("failed to build blocking HTTP client")
}

fn aladin_headers(request: reqwest::blocking::RequestBuilder, base_url: &str) -> reqwest::blocking::RequestBuilder {
    request
        .header(
            "User-Agent",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
        )
        .header("Referer", base_url)
        .header("Origin", "https://www.aladin.co.kr")
}

fn parse_cnt(html: &str) -> Option<String> {
    let document = Html::parse_document(html);
    let selector = Selector::parse("form[name=\"PageNav\"] input[name=\"cnt\"]").ok()?;
    document
        .select(&selector)
        .next()
        .and_then(|node| node.value().attr("value"))
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn parse_last_page(html: &str) -> Option<usize> {
    let re = Regex::new(r"Page_Set\('([0-9]+)'\)").ok()?;
    let document = Html::parse_document(html);
    let selector = Selector::parse("a").ok()?;

    for node in document.select(&selector) {
        let text = node.text().collect::<Vec<_>>().join("").trim().to_string();
        if text == "끝" {
            if let Some(href) = node.value().attr("href") {
                if let Some(capture) = re.captures(href) {
                    if let Ok(page) = capture[1].parse::<usize>() {
                        return Some(page);
                    }
                }
            }
        }
    }

    let mut max_page = None;
    for node in document.select(&selector) {
        if let Some(href) = node.value().attr("href") {
            if let Some(capture) = re.captures(href) {
                if let Ok(page) = capture[1].parse::<usize>() {
                    max_page = Some(max_page.map_or(page, |current| current.max(page)));
                }
            }
        }
    }
    max_page
}

fn extract_publishers_from_html(html: &str) -> Vec<String> {
    let document = Html::parse_document(html);
    let selector = Selector::parse("td.c2b_center").unwrap();
    document
        .select(&selector)
        .filter_map(|node| {
            let text = node.text().collect::<Vec<_>>().join("").trim().to_string();
            if text.is_empty() {
                None
            } else {
                Some(text)
            }
        })
        .collect()
}

fn detect_cnt_and_last_page(base_url: &str) -> Result<(String, usize)> {
    let client = http_client(30)?;
    let response = aladin_headers(client.get(base_url), base_url)
        .send()
        .with_context(|| format!("failed to GET Aladin publisher list: {base_url}"))?;
    let status = response.status();
    let body = response.text().context("failed to read Aladin response body")?;
    if !status.is_success() {
        bail!("Aladin GET failed with status {}", status);
    }

    let cnt = parse_cnt(&body).unwrap_or_else(|| "27942".to_string());
    let last_page = parse_last_page(&body)
        .ok_or_else(|| anyhow::anyhow!("Failed to detect Aladin last page (pagination structure changed or blocked)"))?;
    Ok((cnt, last_page))
}

fn aladin_fetch_page(base_url: &str, page: usize, cnt: &str, sleep_min: f64, sleep_max: f64) -> Result<(usize, Vec<String>)> {
    sleep_random(sleep_min, sleep_max);
    let client = http_client(30)?;
    let response = aladin_headers(client.post(base_url), base_url)
        .form(&[("page", page.to_string()), ("cnt", cnt.to_string())])
        .send()
        .with_context(|| format!("failed to POST Aladin page={} cnt={}", page, cnt))?;
    let status = response.status();
    let body = response.text().context("failed to read Aladin page response body")?;
    if !status.is_success() {
        bail!("Aladin POST failed for page {} with status {}", page, status);
    }
    Ok((page, extract_publishers_from_html(&body)))
}

fn ensure_aladin_cache_table(ch: &ClickHouseHttp, table: &str) -> Result<()> {
    let ddl = format!(
        r#"
CREATE TABLE IF NOT EXISTS {table}
(
  publisher String COMMENT '알라딘 출판사명(원문)',
  collected_at DateTime64(3, 'Asia/Seoul') COMMENT '수집 시각 (Asia/Seoul)',
  detected_last_page UInt32 COMMENT '수집 당시 알라딘 최대 페이지',
  run_uuid UUID COMMENT '배치 실행 UUID v7 (OLAP 전용, SSOT 아님)',
  source LowCardinality(String) COMMENT '수집 출처 (aladin)'
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(collected_at)
ORDER BY (collected_at, detected_last_page, publisher)
COMMENT '알라딘 출판사 목록 캐시/로그 (OLAP 전용, SSOT 아님). 배치 효율 목적 캐시'
"#
    );
    ch.command(&ddl)
}

fn load_cached_publishers_if_fresh(ch: &ClickHouseHttp, cache_table: &str, current_last_page: usize) -> Result<Vec<String>> {
    let cached_last_page = ch.scalar_i64(&format!("SELECT max(detected_last_page) FROM {}", cache_table))?;
    if cached_last_page != current_last_page as i64 {
        return Ok(Vec::new());
    }

    let sql = format!(
        "SELECT DISTINCT publisher FROM {} WHERE detected_last_page = {} ORDER BY publisher",
        cache_table, current_last_page
    );
    let publishers = ch.query_single_column_strings(&sql, "publisher")?;

    let mut seen = HashSet::new();
    Ok(publishers
        .into_iter()
        .filter(|publisher| seen.insert(publisher.clone()))
        .collect())
}

fn save_publishers_cache(
    ch: &ClickHouseHttp,
    cache_table: &str,
    publishers: &[String],
    detected_last_page: usize,
    run_uuid: &str,
) -> Result<()> {
    let now = format_clickhouse_datetime(now_kst());
    let mut rows = Vec::new();
    for publisher in publishers {
        let mut row = Map::new();
        row.insert("publisher".to_string(), Value::String(publisher.clone()));
        row.insert("collected_at".to_string(), Value::String(now.clone()));
        row.insert(
            "detected_last_page".to_string(),
            Value::Number(Number::from(detected_last_page as u64)),
        );
        row.insert("run_uuid".to_string(), Value::String(run_uuid.to_string()));
        row.insert("source".to_string(), Value::String("aladin".to_string()));
        rows.push(row);
    }
    ch.insert_json_each_row(cache_table, &rows)
}

fn crawl_aladin_publishers_dynamic(
    base_url: &str,
    max_workers: usize,
    sleep_min: f64,
    sleep_max: f64,
) -> Result<(Vec<String>, usize)> {
    let (cnt, last_page) = detect_cnt_and_last_page(base_url)?;
    println!("[ALADIN] detected cnt={}, last_page={}", cnt, last_page);

    let pages = (1..=last_page).collect::<Vec<_>>();
    let pool = ThreadPoolBuilder::new()
        .num_threads(max_workers.max(1))
        .build()
        .context("failed to build rayon pool for Aladin crawl")?;

    let mut results = pool.install(|| {
        pages
            .par_iter()
            .map(|page| aladin_fetch_page(base_url, *page, &cnt, sleep_min, sleep_max))
            .collect::<Vec<_>>()
    });

    results.sort_by_key(|result| result.as_ref().map(|(page, _)| *page).unwrap_or(usize::MAX));

    let mut all_publishers = Vec::new();
    for result in results {
        let (page, publishers) = result?;
        if page % 50 == 0 {
            println!("[ALADIN] fetched page={}/{}", page, last_page);
        }
        all_publishers.extend(publishers);
    }

    let mut seen = HashSet::new();
    let unique = all_publishers
        .into_iter()
        .map(|publisher| publisher.trim().to_string())
        .filter(|publisher| !publisher.is_empty())
        .filter(|publisher| seen.insert(publisher.clone()))
        .collect::<Vec<_>>();

    println!("[ALADIN] pages={} unique_publishers={}", last_page, unique.len());
    Ok((unique, last_page))
}

fn collect_all_books_for_publisher(
    ch: &ClickHouseHttp,
    naver: &NaverClient,
    raw_naver_table: &str,
    raw_naver_columns: &HashSet<String>,
    publisher_name: &str,
    display: u32,
    reqs_per_term: usize,
    sleep_min: f64,
    sleep_max: f64,
) -> Result<()> {
    sleep_random(sleep_min, sleep_max);
    let sorts = if reqs_per_term <= 1 {
        vec!["date"]
    } else {
        vec!["sim", "date"]
    };

    for sort in sorts {
        let now = now_kst();
        let now_ch = format_clickhouse_datetime(now);
        let version = now.timestamp();
        let mut start = 1_u32;
        let mut total = None;
        let mut fetched = 0_usize;

        loop {
            if start > 1000 {
                break;
            }

            let response = naver.fetch_page(publisher_name, sort, start, display)?;
            let page_total = response.total.unwrap_or(0);
            let items = response.items.unwrap_or_default();
            if total.is_none() {
                total = Some(page_total);
            }
            if items.is_empty() {
                break;
            }

            let updated_log = format!(
                "aladin_publisher_seed|publisher={}|sort={}|start={}",
                publisher_name, sort, start
            );
            let inserted = upsert_naver_items(
                ch,
                raw_naver_table,
                raw_naver_columns,
                &items,
                version,
                &now_ch,
                "github_actions_auto",
                &updated_log,
            )?;

            fetched += items.len();
            println!(
                "[NAVER] publisher='{}' sort={} start={} items={} inserted={}",
                publisher_name,
                sort,
                start,
                items.len(),
                inserted
            );

            start += display;
            if total.unwrap_or(0) > 0 && start > total.unwrap_or(0) as u32 {
                break;
            }
            sleep_random(sleep_min, sleep_max);
        }

        println!(
            "[NAVER] publisher='{}' sort={} total={} fetched={}",
            publisher_name,
            sort,
            total.unwrap_or(0),
            fetched
        );
    }

    Ok(())
}

fn main() -> Result<()> {
    let ch = ClickHouseHttp::new(ClickHouseConfig::from_env_required()?)?;
    let naver = NaverClient::from_env()?;

    let raw_naver_table = env_string("RAW_NAVER_TABLE", "raw_naver");
    let aladin_url = env_string(
        "ALADIN_PUBLISHER_LIST_URL",
        "https://www.aladin.co.kr/aladdin/PublisherList.aspx",
    );
    let aladin_max_workers = env_usize("ALADIN_MAX_WORKERS", 6);
    let aladin_sleep_min = env_f64("ALADIN_SLEEP_MIN", 0.05);
    let aladin_sleep_max = env_f64("ALADIN_SLEEP_MAX", 0.20);
    let aladin_cache_table = env_string("ALADIN_CACHE_TABLE", "raw_aladin_publisher_cache");
    let publisher_sample_n = env_usize("PUBLISHER_SAMPLE_N", 100);
    let display = env_u32("NAVER_DISPLAY", 100);
    let naver_max_workers = env_usize("NAVER_MAX_WORKERS", 10);
    let naver_sleep_min = env_f64("NAVER_SLEEP_MIN", 0.05);
    let naver_sleep_max = env_f64("NAVER_SLEEP_MAX", 0.20);
    let reqs_per_term = env_usize("REQS_PER_TERM", 1);

    let database = ch
        .cfg
        .database
        .clone()
        .context("CH_DATABASE is required")?;
    let raw_naver_columns = get_table_columns(&ch, &database, &raw_naver_table)?;
    if raw_naver_columns.is_empty() {
        bail!("Cannot read columns for {}.{}", database, raw_naver_table);
    }

    ensure_aladin_cache_table(&ch, &aladin_cache_table)?;
    let run_uuid = Uuid::now_v7().to_string();

    let (_, current_last_page) = detect_cnt_and_last_page(&aladin_url)?;
    let cached_publishers = load_cached_publishers_if_fresh(&ch, &aladin_cache_table, current_last_page)?;

    let publishers = if !cached_publishers.is_empty() {
        println!(
            "[CACHE] use cached publishers for last_page={}: {}",
            current_last_page,
            cached_publishers.len()
        );
        cached_publishers
    } else {
        let (publishers, detected_last_page) = crawl_aladin_publishers_dynamic(
            &aladin_url,
            aladin_max_workers,
            aladin_sleep_min,
            aladin_sleep_max,
        )?;
        save_publishers_cache(&ch, &aladin_cache_table, &publishers, detected_last_page, &run_uuid)?;
        println!(
            "[CACHE] saved publishers: {} (last_page={})",
            publishers.len(),
            detected_last_page
        );
        publishers
    };

    if publishers.is_empty() {
        bail!("No publishers collected from Aladin");
    }

    let sample_n = publisher_sample_n.min(publishers.len());
    let mut sampled = publishers.clone();
    use rand::seq::SliceRandom;
    sampled.shuffle(&mut rand::thread_rng());
    sampled.truncate(sample_n);
    println!("[SAMPLE] picked {} publishers", sampled.len());

    let pool = ThreadPoolBuilder::new()
        .num_threads(naver_max_workers.max(1))
        .build()
        .context("failed to build rayon pool for NAVER collection")?;

    let results = pool.install(|| {
        sampled
            .par_iter()
            .map(|publisher| {
                collect_all_books_for_publisher(
                    &ch,
                    &naver,
                    &raw_naver_table,
                    &raw_naver_columns,
                    publisher,
                    display,
                    reqs_per_term,
                    naver_sleep_min,
                    naver_sleep_max,
                )
            })
            .collect::<Vec<_>>()
    });

    let mut ok = 0_usize;
    for result in results {
        result?;
        ok += 1;
        if ok % 10 == 0 {
            println!("[DONE] completed publishers: {}/{}", ok, sampled.len());
        }
    }

    println!("[FINISH] completed publishers: {}/{}", ok, sampled.len());
    Ok(())
}
