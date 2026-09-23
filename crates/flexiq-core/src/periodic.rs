use std::str::FromStr;

use chrono::Utc;
use cron::Schedule;

use crate::error::{QueueError, Result};
use crate::job::NewJob;
use crate::storage::records::PeriodicTask;

/// Retry cap of a job a periodic task fires.
const PERIODIC_DEFAULT_MAX_RETRIES: i32 = 3;

/// Timeout of a job a periodic task fires (ms).
const PERIODIC_DEFAULT_TIMEOUT_MS: i64 = 300_000;

/// The job a periodic task fires at `now`.
///
/// One builder for the scheduler's firing and an operator's manual trigger, so
/// the two cannot drift. The job inherits the *row's* namespace (#918). The
/// stored `args` blob is the whole payload, opaque to the core; `kwargs` is
/// never read, because the shells fold keyword arguments into `args`.
pub fn periodic_job(task: &PeriodicTask, now: i64, unique_key: Option<String>) -> NewJob {
    NewJob {
        queue: task.queue.clone(),
        task_name: task.task_name.clone(),
        payload: task.args.clone().unwrap_or_default(),
        priority: 0,
        scheduled_at: now,
        max_retries: PERIODIC_DEFAULT_MAX_RETRIES,
        timeout_ms: PERIODIC_DEFAULT_TIMEOUT_MS,
        unique_key,
        metadata: None,
        notes: None,
        depends_on: vec![],
        expires_at: None,
        result_ttl_ms: None,
        namespace: task.namespace.clone(),
        debounce_key: None,
    }
}

/// Next run after `after_ms` for a cron expression, in `timezone` when one is
/// given and UTC otherwise.
pub fn next_run(cron_expr: &str, timezone: Option<&str>, after_ms: i64) -> Result<i64> {
    match timezone {
        Some(tz) => next_cron_time_tz(cron_expr, after_ms, tz),
        None => next_cron_time(cron_expr, after_ms),
    }
}

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
    fn a_periodic_job_carries_the_rows_payload_and_namespace() {
        let task = PeriodicTask {
            name: "nightly".into(),
            task_name: "report".into(),
            cron_expr: "0 0 0 * * *".into(),
            args: Some(vec![9, 9]),
            kwargs: Some(vec![1]),
            queue: "reports".into(),
            enabled: true,
            last_run: None,
            next_run: 0,
            timezone: None,
            namespace: Some("billing".into()),
        };
        let job = periodic_job(&task, 42, None);
        assert_eq!(job.payload, vec![9, 9], "kwargs is never read");
        assert_eq!(job.namespace.as_deref(), Some("billing"));
        assert_eq!(
            (job.queue.as_str(), job.task_name.as_str()),
            ("reports", "report")
        );
        assert_eq!((job.scheduled_at, job.unique_key), (42, None));
    }

    #[test]
    fn next_run_honours_the_timezone_only_when_given() {
        let utc = next_run("0 0 0 * * *", None, 0).unwrap();
        let tokyo = next_run("0 0 0 * * *", Some("Asia/Tokyo"), 0).unwrap();
        assert_ne!(utc, tokyo);
        assert!(next_run("0 0 0 * * *", Some("Not/AZone"), 0).is_err());
    }

    #[test]
    fn test_invalid_cron_expr() {
        let result = next_cron_time("not a cron", 0);
        assert!(result.is_err());
    }
}
