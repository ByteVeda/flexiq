use std::str::FromStr;

use chrono::Utc;
use cron::Schedule;

use crate::error::{QueueError, Result};

/// Compute the next run time (in UNIX milliseconds) for a cron expression,
/// starting from `after_ms` (also UNIX milliseconds).
pub fn next_cron_time(cron_expr: &str, after_ms: i64) -> Result<i64> {
    let schedule = Schedule::from_str(cron_expr)
        .map_err(|e| QueueError::Config(format!("invalid cron expression '{cron_expr}': {e}")))?;

    let after_dt = chrono::DateTime::from_timestamp_millis(after_ms).unwrap_or_else(Utc::now);

    let next = schedule
        .after(&after_dt)
        .next()
        .ok_or_else(|| QueueError::Config(format!("no next run time for '{cron_expr}'")))?;

    Ok(next.timestamp_millis())
}

/// Compute the next run time for a cron expression in a specific timezone.
/// Converts to the target timezone, computes next, then converts back to UTC millis.
pub fn next_cron_time_tz(cron_expr: &str, after_ms: i64, timezone: &str) -> Result<i64> {
    use chrono_tz::Tz;

    let tz: Tz = timezone
        .parse()
        .map_err(|_| QueueError::Config(format!("invalid timezone '{timezone}'")))?;

    let schedule = Schedule::from_str(cron_expr)
        .map_err(|e| QueueError::Config(format!("invalid cron expression '{cron_expr}': {e}")))?;

    let after_utc = chrono::DateTime::from_timestamp_millis(after_ms).unwrap_or_else(Utc::now);
    let after_tz = after_utc.with_timezone(&tz);

    let next = schedule
        .after(&after_tz)
        .next()
        .ok_or_else(|| QueueError::Config(format!("no next run time for '{cron_expr}'")))?;

    Ok(next.with_timezone(&Utc).timestamp_millis())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_next_cron_time_every_minute() {
        let now = crate::job::now_millis();
        let next = next_cron_time("0 * * * * *", now).unwrap();
        // Next minute should be within 60 seconds
        assert!(next > now);
        assert!(next <= now + 60_000);
    }

    #[test]
    fn test_invalid_cron_expr() {
        let result = next_cron_time("not a cron", 0);
        assert!(result.is_err());
    }
}
