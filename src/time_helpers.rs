use chrono::{DateTime, SecondsFormat, Utc};
use chrono_tz::Asia::Seoul;
use rand::Rng;
use std::thread;
use std::time::Duration;

pub fn now_kst() -> DateTime<chrono_tz::Tz> {
    Utc::now().with_timezone(&Seoul)
}

pub fn format_clickhouse_datetime(dt: DateTime<chrono_tz::Tz>) -> String {
    dt.format("%Y-%m-%d %H:%M:%S%.3f").to_string()
}

pub fn iso8601_millis(dt: DateTime<chrono_tz::Tz>) -> String {
    dt.to_rfc3339_opts(SecondsFormat::Millis, true)
}

pub fn sleep_random(min_secs: f64, max_secs: f64) {
    let duration = if max_secs <= min_secs {
        min_secs.max(0.0)
    } else {
        rand::thread_rng().gen_range(min_secs..=max_secs).max(0.0)
    };
    if duration > 0.0 {
        thread::sleep(Duration::from_secs_f64(duration));
    }
}
