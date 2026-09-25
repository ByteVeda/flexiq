//! Task and queue overrides, applied to the scheduler this process builds.
//!
//! An SDK worker reads the overrides an operator set — on the dashboard, over
//! the admin door or with the CLI — when it starts, and registers them with
//! the scheduler it runs. `flexiq-server` runs a scheduler too, and before
//! this module it registered nothing: every job it dispatched, over attach or
//! push, retried on the core's default backoff and passed no rate limit or
//! concurrency cap an override asked for. For a push deployment that was
//! every task it ran, because nothing else in the process knows the tasks.
//!
//! The same moment applies here as in an SDK: overrides are read when the
//! scheduler is built, so a change reaches the next start and not a scheduler
//! already running.
//!
//! Only the fields a scheduler consumes are applied. `timeout` and `priority`
//! are enqueue defaults an SDK stamps on the jobs it creates, which a
//! scheduler never sees; a task's `paused` is an enqueue-side guard in the
//! SDKs; and a queue's `paused` is already read from storage on every claim.

use flexiq_core::{QueueConfig, RateLimitConfig, RetryPolicy, StorageBackend, TaskConfig, Worker};
use serde_json::{Map, Value};

use crate::dashboard::stores::overrides::{self, Scope};

/// Register every task and queue override in `namespace` on `worker`.
///
/// A row that cannot be read, or a field in one that does not parse, is
/// logged and skipped rather than failing the start. The admin door and the
/// dashboard both validate a field before writing it, so a bad one is a row
/// something else wrote — and under attach this runs on the first executor's
/// attach, where refusing would turn one bad row into a scheduler that never
/// starts.
pub fn apply(mut worker: Worker, storage: &StorageBackend, namespace: Option<&str>) -> Worker {
    match overrides::list(Scope::Task, storage, namespace) {
        Ok(rows) => {
            for (task, fields) in rows {
                if let Some(config) = task_config(&task, &fields) {
                    log::info!("[flexiq] task override in effect for {task}");
                    worker = worker.task_config(task, config);
                }
            }
        }
        Err(error) => log::warn!(
            "[flexiq] task overrides could not be read, so none apply to this scheduler: {error}"
        ),
    }
    match overrides::list(Scope::Queue, storage, namespace) {
        Ok(rows) => {
            for (queue, fields) in rows {
                if let Some(config) = queue_config(&queue, &fields) {
                    log::info!("[flexiq] queue override in effect for {queue}");
                    worker = worker.queue_config(queue, config);
                }
            }
        }
        Err(error) => log::warn!(
            "[flexiq] queue overrides could not be read, so none apply to this scheduler: {error}"
        ),
    }
    worker
}

/// The scheduler config a task override asks for, or `None` when it sets
/// nothing a scheduler consumes.
///
/// Fields left unset keep the core default, which is what the scheduler would
/// have applied with no override at all.
pub fn task_config(task: &str, fields: &Map<String, Value>) -> Option<TaskConfig> {
    let backoff = base_delay_ms(task, fields);
    let max_retries = int_field("task", task, fields, "max_retries");
    let rate_limit = rate_field("task", task, fields);
    let max_concurrent = int_field("task", task, fields, "max_concurrent");
    if backoff.is_none()
        && max_retries.is_none()
        && rate_limit.is_none()
        && max_concurrent.is_none()
    {
        return None;
    }
    let defaults = RetryPolicy::default();
    Some(TaskConfig {
        retry_policy: RetryPolicy {
            // The budget the scheduler counts against is the one stamped on
            // the job at enqueue; this is kept only so the policy says what
            // the override said.
            max_retries: max_retries.unwrap_or(defaults.max_retries),
            base_delay_ms: backoff.unwrap_or(defaults.base_delay_ms),
            ..defaults
        },
        rate_limit,
        max_concurrent,
        ..TaskConfig::default()
    })
}

/// The scheduler config a queue override asks for, or `None` when it sets no
/// rate limit and no concurrency cap.
pub fn queue_config(queue: &str, fields: &Map<String, Value>) -> Option<QueueConfig> {
    let rate_limit = rate_field("queue", queue, fields);
    let max_concurrent = int_field("queue", queue, fields, "max_concurrent");
    if rate_limit.is_none() && max_concurrent.is_none() {
        return None;
    }
    Some(QueueConfig {
        rate_limit,
        max_concurrent,
    })
}

/// `retry_backoff`, stored in seconds as the SDKs write it, as the base delay
/// in milliseconds the core's policy takes.
fn base_delay_ms(task: &str, fields: &Map<String, Value>) -> Option<i64> {
    let value = fields
        .get("retry_backoff")
        .filter(|value| !value.is_null())?;
    match value.as_f64() {
        Some(seconds) if seconds.is_finite() && seconds >= 0.0 => {
            Some((seconds.min(i64::MAX as f64 / 1000.0) * 1000.0) as i64)
        }
        _ => {
            skipped("task", task, "retry_backoff", value);
            None
        }
    }
}

fn rate_field(kind: &str, name: &str, fields: &Map<String, Value>) -> Option<RateLimitConfig> {
    let value = fields.get("rate_limit").filter(|value| !value.is_null())?;
    match value.as_str().and_then(RateLimitConfig::parse) {
        Some(rate) => Some(rate),
        None => {
            skipped(kind, name, "rate_limit", value);
            None
        }
    }
}

fn int_field(kind: &str, name: &str, fields: &Map<String, Value>, field: &str) -> Option<i32> {
    let value = fields.get(field).filter(|value| !value.is_null())?;
    match value.as_i64().and_then(|n| i32::try_from(n).ok()) {
        Some(n) if n >= 0 => Some(n),
        _ => {
            skipped(kind, name, field, value);
            None
        }
    }
}

fn skipped(kind: &str, name: &str, field: &str, value: &Value) {
    log::warn!(
        "[flexiq] ignoring {field} = {value} in the override for {kind} {name}: it is not a \
         value the scheduler can apply"
    );
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn fields(value: Value) -> Map<String, Value> {
        value.as_object().expect("an object").clone()
    }

    #[test]
    fn a_backoff_override_sets_the_base_delay_in_milliseconds() {
        let config = task_config("send", &fields(json!({"retry_backoff": 2.5})))
            .expect("a backoff is something the scheduler applies");
        assert_eq!(config.retry_policy.base_delay_ms, 2_500);
        // The cap is not an override field, so it keeps the core default.
        assert_eq!(
            config.retry_policy.max_delay_ms,
            RetryPolicy::default().max_delay_ms
        );
    }

    #[test]
    fn a_task_rate_limit_and_cap_are_applied() {
        let config = task_config(
            "send",
            &fields(json!({"rate_limit": "100/m", "max_concurrent": 4})),
        )
        .expect("both are scheduler fields");
        assert!(config.rate_limit.is_some());
        assert_eq!(config.max_concurrent, Some(4));
        assert_eq!(
            config.retry_policy.base_delay_ms,
            RetryPolicy::default().base_delay_ms
        );
    }

    #[test]
    fn enqueue_side_fields_alone_register_nothing() {
        // Timeout, priority and a task pause are read where jobs are made,
        // not where they are dispatched.
        let row = fields(json!({"timeout": 30, "priority": 5, "paused": true, "updated_at": 1}));
        assert!(task_config("send", &row).is_none());
    }

    #[test]
    fn a_field_that_does_not_parse_is_skipped_and_the_rest_still_apply() {
        let config = task_config(
            "send",
            &fields(json!({"rate_limit": "lots", "retry_backoff": 1})),
        )
        .expect("the backoff still applies");
        assert!(config.rate_limit.is_none());
        assert_eq!(config.retry_policy.base_delay_ms, 1_000);

        assert!(task_config("send", &fields(json!({"retry_backoff": -1}))).is_none());
        assert!(task_config("send", &fields(json!({"max_concurrent": "four"}))).is_none());
    }

    #[test]
    fn a_null_field_reads_as_unset() {
        assert!(task_config("send", &fields(json!({"rate_limit": null}))).is_none());
    }

    #[test]
    fn a_queue_override_sets_its_rate_and_cap() {
        let config = queue_config(
            "emails",
            &fields(json!({"rate_limit": "10/s", "max_concurrent": 2, "paused": false})),
        )
        .expect("both are scheduler fields");
        assert!(config.rate_limit.is_some());
        assert_eq!(config.max_concurrent, Some(2));
        assert!(queue_config("emails", &fields(json!({"paused": true}))).is_none());
    }
}
