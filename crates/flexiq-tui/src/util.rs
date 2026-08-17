//! Small formatting helpers. All timestamps are Unix milliseconds.

use flexiq_core::now_millis;

/// Render a Unix-ms timestamp as a compact relative age, e.g. `3s`, `5m`, `2h`,
/// `4d`. Future timestamps (scheduled ahead) render as `in Xs`.
pub fn fmt_age(ts_ms: i64) -> String {
    let delta = now_millis() - ts_ms;
    if delta < 0 {
        return format!("in {}", fmt_duration(-delta));
    }
    fmt_duration(delta)
}

/// Optional-timestamp age, or `-` when absent.
pub fn fmt_age_opt(ts_ms: Option<i64>) -> String {
    ts_ms.map(fmt_age).unwrap_or_else(|| "-".to_string())
}

fn fmt_duration(ms: i64) -> String {
    let secs = ms / 1000;
    if secs < 60 {
        format!("{secs}s")
    } else if secs < 3600 {
        format!("{}m", secs / 60)
    } else if secs < 86_400 {
        format!("{}h", secs / 3600)
    } else {
        format!("{}d", secs / 86_400)
    }
}

/// Truncate to `max` chars with an ellipsis, for table cells.
pub fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        return s.to_string();
    }
    let keep = max.saturating_sub(1);
    let mut out: String = s.chars().take(keep).collect();
    out.push('…');
    out
}
