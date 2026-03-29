use crate::raw_naver::TermSampleRow;
use once_cell::sync::Lazy;
use rand::seq::SliceRandom;
use regex::Regex;
use std::collections::HashSet;

pub const FALLBACK_TERMS: &[&str] = &["통계", "데이터", "Statistics", "Data"];

static RE_HANGUL_TOKEN: Lazy<Regex> = Lazy::new(|| Regex::new(r"[가-힣]{2,}").unwrap());
static RE_EN_TOKEN: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)[a-z][a-z0-9'\-]{3,}").unwrap());
static RE_HAS_HANGUL: Lazy<Regex> = Lazy::new(|| Regex::new(r"[가-힣]").unwrap());
static KOREAN_STOPWORDS: Lazy<HashSet<&'static str>> = Lazy::new(|| {
    [
        "개정", "증보", "전면", "최신", "판", "세트", "상", "하", "권", "입문", "기초", "실전",
    ]
    .into_iter()
    .collect()
});
static ENGLISH_STOPWORDS: Lazy<HashSet<&'static str>> = Lazy::new(|| {
    [
        "book", "books", "edition", "revised", "updated", "series", "volume", "guide", "study",
        "introduction",
    ]
    .into_iter()
    .collect()
});

pub fn sanitize_keyword(input: Option<&str>) -> String {
    let candidate = input.unwrap_or_default().trim();
    if candidate.is_empty() {
        let mut rng = rand::thread_rng();
        FALLBACK_TERMS
            .choose(&mut rng)
            .copied()
            .unwrap_or("통계")
            .to_string()
    } else {
        candidate.to_string()
    }
}

pub fn contains_hangul(input: &str) -> bool {
    RE_HAS_HANGUL.is_match(input)
}

pub fn extract_keywords_from_title(title: &str) -> Vec<String> {
    let trimmed = title.trim();
    if trimmed.is_empty() {
        return Vec::new();
    }

    if contains_hangul(trimmed) {
        RE_HANGUL_TOKEN
            .find_iter(trimmed)
            .map(|m| m.as_str().trim().to_string())
            .filter(|token| token.len() >= 2)
            .filter(|token| !KOREAN_STOPWORDS.contains(token.as_str()))
            .collect()
    } else {
        RE_EN_TOKEN
            .find_iter(trimmed)
            .map(|m| m.as_str().trim().to_string())
            .filter(|token| token.len() >= 4)
            .filter(|token| !ENGLISH_STOPWORDS.contains(token.to_ascii_lowercase().as_str()))
            .collect()
    }
}

pub fn extract_authors(author_values: &[String]) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for value in author_values {
        for token in value.split('^') {
            let token = token.trim();
            if token.len() >= 2 && seen.insert(token.to_string()) {
                out.push(token.to_string());
            }
        }
    }
    out
}

pub fn extract_publishers(publisher_values: &[String]) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for value in publisher_values {
        let token = value.trim();
        if token.len() >= 2 && seen.insert(token.to_string()) {
            out.push(token.to_string());
        }
    }
    out
}

pub fn generate_keyword(samples: &[TermSampleRow]) -> String {
    let mut rng = rand::thread_rng();
    if samples.is_empty() {
        return sanitize_keyword(None);
    }

    let source = ["title", "author", "publisher"]
        .choose(&mut rng)
        .copied()
        .unwrap_or("title");

    match source {
        "author" => {
            let author_values = samples.iter().map(|row| row.author.clone()).collect::<Vec<_>>();
            let candidates = extract_authors(&author_values);
            sanitize_keyword(candidates.choose(&mut rng).map(String::as_str))
        }
        "publisher" => {
            let publisher_values = samples
                .iter()
                .map(|row| row.publisher.clone())
                .collect::<Vec<_>>();
            let candidates = extract_publishers(&publisher_values);
            sanitize_keyword(candidates.choose(&mut rng).map(String::as_str))
        }
        _ => {
            let mut keywords = Vec::new();
            for sample in samples {
                keywords.extend(extract_keywords_from_title(&sample.title));
            }
            sanitize_keyword(keywords.choose(&mut rng).map(String::as_str))
        }
    }
}

pub fn pick_unique_terms(samples: &[TermSampleRow], mode: &str, batch_size: usize) -> Vec<String> {
    let normalized_mode = mode.trim().to_lowercase();
    let mut rng = rand::thread_rng();

    let pool = match normalized_mode.as_str() {
        "author" => extract_authors(
            &samples
                .iter()
                .map(|row| row.author.clone())
                .collect::<Vec<_>>(),
        ),
        "publisher" => extract_publishers(
            &samples
                .iter()
                .map(|row| row.publisher.clone())
                .collect::<Vec<_>>(),
        ),
        _ => {
            let mut seen = HashSet::new();
            let mut out = Vec::new();
            for sample in samples {
                for token in extract_keywords_from_title(&sample.title) {
                    if seen.insert(token.clone()) {
                        out.push(token);
                    }
                }
            }
            out
        }
    };

    let mut picked = if pool.is_empty() {
        FALLBACK_TERMS.iter().map(|v| v.to_string()).collect::<Vec<_>>()
    } else {
        pool
    };
    picked.shuffle(&mut rng);
    picked.truncate(batch_size.min(picked.len()));

    picked
        .into_iter()
        .map(|term| sanitize_keyword(Some(&term)))
        .collect()
}
