use crate::env::{env_string, optional_env, require_env};
use anyhow::{anyhow, Context, Result};
use rand::seq::SliceRandom;
use reqwest::blocking::Client;
use serde::Deserialize;
use std::time::Duration;

#[derive(Debug, Clone, Deserialize)]
pub struct NaverApiKey {
    pub client_id: String,
    pub client_secret: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct NaverBookItem {
    pub title: Option<String>,
    pub link: Option<String>,
    pub image: Option<String>,
    pub author: Option<String>,
    pub discount: Option<String>,
    pub publisher: Option<String>,
    pub isbn: Option<String>,
    pub description: Option<String>,
    pub pubdate: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct NaverSearchResponse {
    pub total: Option<u64>,
    pub items: Option<Vec<NaverBookItem>>,
}

#[derive(Clone)]
pub struct NaverClient {
    client: Client,
    api_keys: Vec<NaverApiKey>,
    url: String,
}

impl NaverClient {
    pub fn from_env() -> Result<Self> {
        let raw = require_env("NAVER_API_KEYS")?;
        let api_keys: Vec<NaverApiKey> = serde_json::from_str(&raw)
            .context("NAVER_API_KEYS must be valid JSON like [{\"client_id\":\"...\",\"client_secret\":\"...\"}]")?;
        if api_keys.is_empty() {
            return Err(anyhow!("NAVER_API_KEYS must contain at least one API credential pair"));
        }

        let client = Client::builder()
            .timeout(Duration::from_secs(20))
            .build()
            .context("failed to build reqwest client for NAVER API")?;

        Ok(Self {
            client,
            api_keys,
            url: optional_env("NAVER_URL").unwrap_or_else(|| "https://openapi.naver.com/v1/search/book.json".to_string()),
        })
    }

    fn pick_api_key(&self) -> Result<&NaverApiKey> {
        let mut rng = rand::thread_rng();
        self.api_keys
            .choose(&mut rng)
            .ok_or_else(|| anyhow!("NAVER_API_KEYS is empty"))
    }

    pub fn fetch_page(&self, query: &str, sort: &str, start: u32, display: u32) -> Result<NaverSearchResponse> {
        let api_key = self.pick_api_key()?;
        let response = self
            .client
            .get(&self.url)
            .header("X-Naver-Client-Id", &api_key.client_id)
            .header("X-Naver-Client-Secret", &api_key.client_secret)
            .query(&[
                ("query", query.to_string()),
                ("display", display.to_string()),
                ("start", start.to_string()),
                ("sort", sort.to_string()),
            ])
            .send()
            .with_context(|| format!("failed to call NAVER Book API for query={query} sort={sort} start={start}"))?;

        if !response.status().is_success() {
            eprintln!(
                "[WARN] NAVER API returned non-success status={} query={} sort={} start={}",
                response.status(),
                query,
                sort,
                start
            );
            return Ok(NaverSearchResponse::default());
        }

        response
            .json::<NaverSearchResponse>()
            .context("failed to parse NAVER API JSON response")
    }

    pub fn fetch_items(&self, query: &str, sort: &str, start: u32, display: u32) -> Result<Vec<NaverBookItem>> {
        Ok(self.fetch_page(query, sort, start, display)?.items.unwrap_or_default())
    }
}
