use anyhow::{anyhow, Result};
use std::env;

pub fn require_env(name: &str) -> Result<String> {
    let value = env::var(name).map_err(|_| anyhow!("Missing required environment variable: {name}"))?;
    let trimmed = value.trim();
    if trimmed.is_empty() {
        Err(anyhow!("Missing required environment variable: {name}"))
    } else {
        Ok(trimmed.to_string())
    }
}

pub fn optional_env(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

pub fn env_string(name: &str, default: &str) -> String {
    optional_env(name).unwrap_or_else(|| default.to_string())
}

pub fn env_usize(name: &str, default: usize) -> usize {
    optional_env(name)
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(default)
}

pub fn env_u16(name: &str, default: u16) -> u16 {
    optional_env(name)
        .and_then(|v| v.parse::<u16>().ok())
        .unwrap_or(default)
}

pub fn env_u32(name: &str, default: u32) -> u32 {
    optional_env(name)
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(default)
}

pub fn env_i64(name: &str, default: i64) -> i64 {
    optional_env(name)
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(default)
}

pub fn env_f64(name: &str, default: f64) -> f64 {
    optional_env(name)
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(default)
}

pub fn env_string_with_fallback(names: &[&str], default: &str) -> String {
    names
        .iter()
        .find_map(|name| optional_env(name))
        .unwrap_or_else(|| default.to_string())
}

pub fn env_i64_with_fallback(names: &[&str], default: i64) -> i64 {
    names
        .iter()
        .find_map(|name| optional_env(name).and_then(|v| v.parse::<i64>().ok()))
        .unwrap_or(default)
}
