//! What the admin verbs print.
//!
//! The same two renderings as the rest of [`super`]. The JSON writers here are
//! field for field with `flexiq_server::grpc::facade::json::admin_response`;
//! `crates/flexiq-server/tests/grpc_cli.rs` asserts they agree. A response
//! carrying one `Job` — replay and trigger — is written by
//! [`super::job_envelope_json`], so it reads exactly as an enqueued one.
//!
//! Every table cell is a plain `String` here and escaped by [`super::table`];
//! anything printed outside a table goes through [`crate::safe::escape`].

use base64::Engine as _;
use serde_json::{Map, Value};

use super::{duration_to_json, insert_timestamp, int64, timestamp_to_json, BASE64};
use crate::pb::admin as pb;
use crate::time::seconds_of;

/// Seconds in a minute, for a per-minute rate.
const SECONDS_PER_MINUTE: f64 = 60.0;

/// What an unset optional reads as in a table.
const UNSET: &str = "-";

/// The columns of a queue listing.
pub const QUEUE_COLUMNS: [&str; 8] = [
    "queue",
    "paused",
    "pending",
    "running",
    "completed",
    "failed",
    "dead",
    "cancelled",
];

/// The columns of a throughput table. The two rates are computed here, from
/// the window the server echoes; the wire carries counts only.
pub const THROUGHPUT_COLUMNS: [&str; 7] = [
    "queue",
    "completed",
    "failed",
    "dead",
    "cancelled",
    "completed/min",
    "finished/min",
];

/// The columns of a dead-letter listing.
pub const DEAD_LETTER_COLUMNS: [&str; 7] =
    ["id", "job", "queue", "task", "failed", "retries", "replays"];

/// The columns of a worker listing.
pub const WORKER_COLUMNS: [&str; 8] = [
    "id",
    "status",
    "queues",
    "concurrency",
    "host",
    "pid",
    "sdk",
    "heartbeat",
];

/// The columns of a periodic-task listing.
pub const PERIODIC_COLUMNS: [&str; 8] = [
    "name", "task", "cron", "timezone", "queue", "state", "next", "last",
];

/// The columns of an override listing. A queue override has no retries,
/// backoff, timeout, priority or pause, so those cells are [`UNSET`].
pub const OVERRIDE_COLUMNS: [&str; 10] = [
    "scope",
    "name",
    "rate",
    "concurrency",
    "retries",
    "backoff",
    "timeout",
    "priority",
    "paused",
    "updated",
];

// ── Tables ───────────────────────────────────────────────────────────

/// One queue as a row of [`QUEUE_COLUMNS`].
pub fn queue_row(queue: &pb::Queue) -> Vec<String> {
    vec![
        queue.name.clone(),
        yes_no(queue.paused),
        queue.pending.to_string(),
        queue.running.to_string(),
        queue.completed.to_string(),
        queue.failed.to_string(),
        queue.dead.to_string(),
        queue.cancelled.to_string(),
    ]
}

/// The throughput table: counts as sent, then two per-minute rates over the
/// window the server says it applied — not the one asked for, which the server
/// may have defaulted.
pub fn throughput_rows(response: &pb::GetThroughputResponse) -> Vec<Vec<String>> {
    let minutes = response
        .window
        .as_ref()
        .map(|window| seconds_of(window) / SECONDS_PER_MINUTE)
        .filter(|minutes| *minutes > 0.0);
    response
        .queues
        .iter()
        .map(|counts| {
            let finished = counts.completed + counts.failed + counts.dead + counts.cancelled;
            vec![
                counts.queue.clone(),
                counts.completed.to_string(),
                counts.failed.to_string(),
                counts.dead.to_string(),
                counts.cancelled.to_string(),
                per_minute(counts.completed, minutes),
                per_minute(finished, minutes),
            ]
        })
        .collect()
}

/// The line above a throughput table naming the window. Built from parsed
/// values only, so nothing in it needs escaping.
pub fn throughput_header(response: &pb::GetThroughputResponse) -> String {
    let window = response
        .window
        .as_ref()
        .map(duration_to_json)
        .unwrap_or_else(|| UNSET.to_string());
    let since = response
        .since
        .as_ref()
        .and_then(timestamp_to_json)
        .unwrap_or_else(|| UNSET.to_string());
    format!("window {window} since {since}")
}

/// `count` per minute, to two places, or [`UNSET`] without a usable window.
fn per_minute(count: i64, minutes: Option<f64>) -> String {
    minutes
        .map(|minutes| format!("{:.2}", count as f64 / minutes))
        .unwrap_or_else(|| UNSET.to_string())
}

/// One dead letter as a row of [`DEAD_LETTER_COLUMNS`].
pub fn dead_letter_row(entry: &pb::DeadLetter) -> Vec<String> {
    vec![
        entry.id.clone(),
        entry.original_job_id.clone(),
        entry.queue.clone(),
        entry.task_name.clone(),
        instant_cell(entry.failed_at.as_ref()),
        format!("{}/{}", entry.retry_count, entry.max_retries),
        entry.replay_count.to_string(),
    ]
}

/// One worker as a row of [`WORKER_COLUMNS`].
pub fn worker_row(worker: &pb::Worker) -> Vec<String> {
    let sdk = match (worker.sdk.as_deref(), worker.sdk_version.as_deref()) {
        (Some(sdk), Some(version)) => format!("{sdk} {version}"),
        (Some(sdk), None) => sdk.to_string(),
        (None, _) => UNSET.to_string(),
    };
    vec![
        worker.worker_id.clone(),
        worker_status_name(worker.status),
        worker.queues.join(","),
        worker.concurrency.to_string(),
        optional_cell(worker.hostname.as_ref()),
        optional_cell(worker.pid.as_ref()),
        sdk,
        instant_cell(worker.last_heartbeat.as_ref()),
    ]
}

/// A worker status for a table cell: the short name, lowercased, or the
/// number when this build does not know it — the contract says to show an
/// unknown status as unknown, not to guess.
pub fn worker_status_name(status: i32) -> String {
    match pb::WorkerStatus::try_from(status) {
        Ok(known) => known
            .as_str_name()
            .trim_start_matches("WORKER_STATUS_")
            .to_ascii_lowercase(),
        Err(_) => status.to_string(),
    }
}

/// One periodic task as a row of [`PERIODIC_COLUMNS`].
pub fn periodic_row(task: &pb::PeriodicTask) -> Vec<String> {
    vec![
        task.name.clone(),
        task.task_name.clone(),
        task.cron.clone(),
        task.timezone.clone().unwrap_or_else(|| "UTC".to_string()),
        task.queue.clone(),
        if task.enabled { "enabled" } else { "paused" }.to_string(),
        instant_cell(task.next_run.as_ref()),
        instant_cell(task.last_run.as_ref()),
    ]
}

/// Every override as rows of [`OVERRIDE_COLUMNS`], tasks then queues, each by
/// name. The wire carries two maps, whose order is the decoder's, so a stable
/// one is imposed here or two runs print differently.
pub fn override_rows(response: &pb::ListOverridesResponse) -> Vec<Vec<String>> {
    let mut tasks: Vec<_> = response.tasks.iter().collect();
    tasks.sort_by(|left, right| left.0.cmp(right.0));
    let mut queues: Vec<_> = response.queues.iter().collect();
    queues.sort_by(|left, right| left.0.cmp(right.0));
    tasks
        .into_iter()
        .map(|(name, value)| task_override_row(name, value))
        .chain(
            queues
                .into_iter()
                .map(|(name, value)| queue_override_row(name, value)),
        )
        .collect()
}

/// One task override as a row of [`OVERRIDE_COLUMNS`].
pub fn task_override_row(name: &str, value: &pb::TaskOverride) -> Vec<String> {
    vec![
        "task".to_string(),
        name.to_string(),
        optional_cell(value.rate_limit.as_ref()),
        optional_cell(value.max_concurrent.as_ref()),
        optional_cell(value.max_retries.as_ref()),
        value
            .retry_backoff
            .as_ref()
            .map(duration_to_json)
            .unwrap_or_else(|| UNSET.to_string()),
        value
            .timeout
            .as_ref()
            .map(duration_to_json)
            .unwrap_or_else(|| UNSET.to_string()),
        optional_cell(value.priority.as_ref()),
        value
            .paused
            .map(yes_no)
            .unwrap_or_else(|| UNSET.to_string()),
        instant_cell(value.update_time.as_ref()),
    ]
}

/// One queue override as a row of [`OVERRIDE_COLUMNS`].
pub fn queue_override_row(name: &str, value: &pb::QueueOverride) -> Vec<String> {
    let mut row = vec![UNSET.to_string(); OVERRIDE_COLUMNS.len()];
    row[0] = "queue".to_string();
    row[1] = name.to_string();
    row[2] = optional_cell(value.rate_limit.as_ref());
    row[3] = optional_cell(value.max_concurrent.as_ref());
    row[9] = instant_cell(value.update_time.as_ref());
    row
}

/// A present value as its text, an absent one as [`UNSET`].
fn optional_cell<T: ToString>(value: Option<&T>) -> String {
    value
        .map(ToString::to_string)
        .unwrap_or_else(|| UNSET.to_string())
}

/// An instant as RFC 3339, or [`UNSET`] when absent or unrenderable.
fn instant_cell(value: Option<&prost_types::Timestamp>) -> String {
    value
        .and_then(timestamp_to_json)
        .unwrap_or_else(|| UNSET.to_string())
}

/// A boolean as an operator reads it.
fn yes_no(flag: bool) -> String {
    if flag { "yes" } else { "no" }.to_string()
}

// ── proto3 JSON ──────────────────────────────────────────────────────

/// A message with no fields.
pub fn empty_json() -> Value {
    Value::Object(Map::new())
}

/// One message holding one optional message field, written only when set.
fn wrapping<T>(key: &str, value: Option<&T>, render: fn(&T) -> Value) -> Value {
    let mut object = Map::new();
    if let Some(value) = value {
        object.insert(key.to_string(), render(value));
    }
    Value::Object(object)
}

/// One `Queue`.
pub fn queue_json(queue: &pb::Queue) -> Value {
    Value::Object(Map::from_iter([
        ("name".to_string(), queue.name.clone().into()),
        ("paused".to_string(), queue.paused.into()),
        ("pending".to_string(), int64(queue.pending)),
        ("running".to_string(), int64(queue.running)),
        ("completed".to_string(), int64(queue.completed)),
        ("failed".to_string(), int64(queue.failed)),
        ("dead".to_string(), int64(queue.dead)),
        ("cancelled".to_string(), int64(queue.cancelled)),
    ]))
}

/// `ListQueuesResponse`.
pub fn list_queues_json(response: &pb::ListQueuesResponse) -> Value {
    Value::Object(Map::from_iter([(
        "queues".to_string(),
        response.queues.iter().map(queue_json).collect::<Value>(),
    )]))
}

/// `PauseQueueResponse` and `ResumeQueueResponse`, which are the same shape.
pub fn queue_envelope_json(queue: Option<&pb::Queue>) -> Value {
    wrapping("queue", queue, queue_json)
}

/// One `QueueThroughput`.
fn queue_throughput_json(counts: &pb::QueueThroughput) -> Value {
    Value::Object(Map::from_iter([
        ("queue".to_string(), counts.queue.clone().into()),
        ("completed".to_string(), int64(counts.completed)),
        ("failed".to_string(), int64(counts.failed)),
        ("dead".to_string(), int64(counts.dead)),
        ("cancelled".to_string(), int64(counts.cancelled)),
    ]))
}

/// `GetThroughputResponse`. The counts only: the rates in the table are this
/// binary's arithmetic, not the message.
pub fn throughput_json(response: &pb::GetThroughputResponse) -> Value {
    let mut object = Map::new();
    if let Some(window) = response.window.as_ref() {
        object.insert("window".to_string(), duration_to_json(window).into());
    }
    insert_timestamp(&mut object, "since", response.since.as_ref());
    object.insert(
        "queues".to_string(),
        response
            .queues
            .iter()
            .map(queue_throughput_json)
            .collect::<Value>(),
    );
    Value::Object(object)
}

/// One `DeadLetter`.
pub fn dead_letter_json(entry: &pb::DeadLetter) -> Value {
    let mut object = Map::new();
    object.insert("id".to_string(), entry.id.clone().into());
    object.insert(
        "originalJobId".to_string(),
        entry.original_job_id.clone().into(),
    );
    object.insert("queue".to_string(), entry.queue.clone().into());
    object.insert("taskName".to_string(), entry.task_name.clone().into());
    insert_timestamp(&mut object, "failedAt", entry.failed_at.as_ref());
    object.insert("retryCount".to_string(), entry.retry_count.into());
    object.insert("maxRetries".to_string(), entry.max_retries.into());
    object.insert("priority".to_string(), entry.priority.into());
    object.insert("replayCount".to_string(), entry.replay_count.into());
    if let Some(error) = entry.error.as_ref() {
        object.insert("error".to_string(), error.clone().into());
    }
    if let Some(metadata) = entry.metadata.as_ref() {
        object.insert("metadata".to_string(), metadata.clone().into());
    }
    if let Some(payload) = entry.payload.as_ref() {
        object.insert("payload".to_string(), BASE64.encode(payload).into());
    }
    Value::Object(object)
}

/// `ListDeadLettersResponse`.
pub fn list_dead_letters_json(response: &pb::ListDeadLettersResponse) -> Value {
    Value::Object(Map::from_iter([
        (
            "deadLetters".to_string(),
            response
                .dead_letters
                .iter()
                .map(dead_letter_json)
                .collect::<Value>(),
        ),
        (
            "nextPageToken".to_string(),
            response.next_page_token.clone().into(),
        ),
    ]))
}

/// `GetDeadLetterResponse`.
pub fn dead_letter_envelope_json(entry: Option<&pb::DeadLetter>) -> Value {
    wrapping("deadLetter", entry, dead_letter_json)
}

/// `PurgeDeadLettersResponse`.
pub fn purge_json(response: &pb::PurgeDeadLettersResponse) -> Value {
    Value::Object(Map::from_iter([(
        "purged".to_string(),
        int64(response.purged),
    )]))
}

/// One `Worker`.
pub fn worker_json(worker: &pb::Worker) -> Value {
    let mut object = Map::new();
    object.insert("workerId".to_string(), worker.worker_id.clone().into());
    object.insert("queues".to_string(), worker.queues.clone().into());
    object.insert("status".to_string(), worker_status_json(worker.status));
    insert_timestamp(&mut object, "lastHeartbeat", worker.last_heartbeat.as_ref());
    object.insert("concurrency".to_string(), worker.concurrency.into());
    insert_timestamp(&mut object, "startedAt", worker.started_at.as_ref());
    if let Some(hostname) = worker.hostname.as_ref() {
        object.insert("hostname".to_string(), hostname.clone().into());
    }
    if let Some(pid) = worker.pid {
        object.insert("pid".to_string(), pid.into());
    }
    if let Some(pool_type) = worker.pool_type.as_ref() {
        object.insert("poolType".to_string(), pool_type.clone().into());
    }
    if let Some(sdk) = worker.sdk.as_ref() {
        object.insert("sdk".to_string(), sdk.clone().into());
    }
    if let Some(sdk_version) = worker.sdk_version.as_ref() {
        object.insert("sdkVersion".to_string(), sdk_version.clone().into());
    }
    Value::Object(object)
}

/// A `WorkerStatus` by name, or by number when this build does not know it.
fn worker_status_json(status: i32) -> Value {
    match pb::WorkerStatus::try_from(status) {
        Ok(known) => known.as_str_name().into(),
        Err(_) => status.into(),
    }
}

/// `ListWorkersResponse`.
pub fn list_workers_json(response: &pb::ListWorkersResponse) -> Value {
    Value::Object(Map::from_iter([(
        "workers".to_string(),
        response.workers.iter().map(worker_json).collect::<Value>(),
    )]))
}

/// `DrainWorkerResponse`: `{worker}`.
pub fn worker_envelope_json(worker: Option<&pb::Worker>) -> Value {
    wrapping("worker", worker, worker_json)
}

/// One `PeriodicTask`.
pub fn periodic_task_json(task: &pb::PeriodicTask) -> Value {
    let mut object = Map::new();
    object.insert("name".to_string(), task.name.clone().into());
    object.insert("taskName".to_string(), task.task_name.clone().into());
    object.insert("cron".to_string(), task.cron.clone().into());
    object.insert("queue".to_string(), task.queue.clone().into());
    object.insert("enabled".to_string(), task.enabled.into());
    insert_timestamp(&mut object, "nextRun", task.next_run.as_ref());
    insert_timestamp(&mut object, "lastRun", task.last_run.as_ref());
    if let Some(timezone) = task.timezone.as_ref() {
        object.insert("timezone".to_string(), timezone.clone().into());
    }
    if let Some(payload) = task.payload.as_ref() {
        object.insert("payload".to_string(), BASE64.encode(payload).into());
    }
    Value::Object(object)
}

/// `ListPeriodicTasksResponse`.
pub fn list_periodic_tasks_json(response: &pb::ListPeriodicTasksResponse) -> Value {
    Value::Object(Map::from_iter([(
        "periodicTasks".to_string(),
        response
            .periodic_tasks
            .iter()
            .map(periodic_task_json)
            .collect::<Value>(),
    )]))
}

/// The four responses carrying one optional `PeriodicTask` — get, put, pause
/// and resume are the same shape.
pub fn periodic_task_envelope_json(task: Option<&pb::PeriodicTask>) -> Value {
    wrapping("periodicTask", task, periodic_task_json)
}

/// One `TaskOverride`. An unset field is a missing key, which is what "not
/// overridden" looks like.
pub fn task_override_json(value: &pb::TaskOverride) -> Value {
    let mut object = Map::new();
    if let Some(rate_limit) = value.rate_limit.as_ref() {
        object.insert("rateLimit".to_string(), rate_limit.clone().into());
    }
    if let Some(max_concurrent) = value.max_concurrent {
        object.insert("maxConcurrent".to_string(), max_concurrent.into());
    }
    if let Some(max_retries) = value.max_retries {
        object.insert("maxRetries".to_string(), max_retries.into());
    }
    if let Some(backoff) = value.retry_backoff.as_ref() {
        object.insert("retryBackoff".to_string(), duration_to_json(backoff).into());
    }
    if let Some(timeout) = value.timeout.as_ref() {
        object.insert("timeout".to_string(), duration_to_json(timeout).into());
    }
    if let Some(priority) = value.priority {
        object.insert("priority".to_string(), priority.into());
    }
    if let Some(paused) = value.paused {
        object.insert("paused".to_string(), paused.into());
    }
    insert_timestamp(&mut object, "updateTime", value.update_time.as_ref());
    Value::Object(object)
}

/// One `QueueOverride`.
pub fn queue_override_json(value: &pb::QueueOverride) -> Value {
    let mut object = Map::new();
    if let Some(rate_limit) = value.rate_limit.as_ref() {
        object.insert("rateLimit".to_string(), rate_limit.clone().into());
    }
    if let Some(max_concurrent) = value.max_concurrent {
        object.insert("maxConcurrent".to_string(), max_concurrent.into());
    }
    insert_timestamp(&mut object, "updateTime", value.update_time.as_ref());
    Value::Object(object)
}

/// `ListOverridesResponse`. A protobuf map is a JSON object keyed by the map's
/// own keys.
pub fn list_overrides_json(response: &pb::ListOverridesResponse) -> Value {
    let tasks: Map<String, Value> = response
        .tasks
        .iter()
        .map(|(name, value)| (name.clone(), task_override_json(value)))
        .collect();
    let queues: Map<String, Value> = response
        .queues
        .iter()
        .map(|(name, value)| (name.clone(), queue_override_json(value)))
        .collect();
    Value::Object(Map::from_iter([
        ("tasks".to_string(), Value::Object(tasks)),
        ("queues".to_string(), Value::Object(queues)),
    ]))
}

/// `SetTaskOverrideResponse`.
pub fn task_override_envelope_json(value: Option<&pb::TaskOverride>) -> Value {
    wrapping("taskOverride", value, task_override_json)
}

/// `SetQueueOverrideResponse`.
pub fn queue_override_envelope_json(value: Option<&pb::QueueOverride>) -> Value {
    wrapping("queueOverride", value, queue_override_json)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use prost_types::{Duration as ProtoDuration, Timestamp};

    use super::*;

    fn minutes(count: i64) -> ProtoDuration {
        ProtoDuration {
            seconds: count * 60,
            nanos: 0,
        }
    }

    /// The rate divides by the window the server applied, so a five-minute
    /// window of 30 completions is six a minute.
    #[test]
    fn a_rate_is_per_minute_over_the_echoed_window() {
        let response = pb::GetThroughputResponse {
            window: Some(minutes(5)),
            since: None,
            queues: vec![pb::QueueThroughput {
                queue: "mail".into(),
                completed: 30,
                failed: 3,
                dead: 1,
                cancelled: 1,
            }],
        };
        let rows = throughput_rows(&response);
        assert_eq!(rows[0][5], "6.00");
        assert_eq!(rows[0][6], "7.00");
    }

    /// No window, or a zero one, is no rate — not a division by zero.
    #[test]
    fn a_missing_or_zero_window_is_no_rate() {
        for window in [None, Some(minutes(0))] {
            let response = pb::GetThroughputResponse {
                window,
                since: None,
                queues: vec![pb::QueueThroughput {
                    queue: "mail".into(),
                    completed: 1,
                    ..Default::default()
                }],
            };
            assert_eq!(throughput_rows(&response)[0][5], UNSET);
        }
    }

    #[test]
    fn the_throughput_header_names_the_window_and_its_start() {
        let response = pb::GetThroughputResponse {
            window: Some(minutes(5)),
            since: Some(Timestamp {
                seconds: 1_757_500_000,
                nanos: 0,
            }),
            queues: vec![],
        };
        assert_eq!(
            throughput_header(&response),
            "window 300s since 2025-09-10T10:26:40Z"
        );
    }

    /// Two maps on the wire, decoded in whatever order the decoder chose; the
    /// table must not print differently between two runs of the same state.
    #[test]
    fn overrides_print_tasks_then_queues_each_by_name() {
        let response = pb::ListOverridesResponse {
            tasks: HashMap::from([
                ("zeta".to_string(), pb::TaskOverride::default()),
                ("alpha".to_string(), pb::TaskOverride::default()),
            ]),
            queues: HashMap::from([("mail".to_string(), pb::QueueOverride::default())]),
        };
        let names: Vec<_> = override_rows(&response)
            .into_iter()
            .map(|row| format!("{}:{}", row[0], row[1]))
            .collect();
        assert_eq!(names, ["task:alpha", "task:zeta", "queue:mail"]);
    }

    #[test]
    fn an_unset_override_field_is_a_dash_in_a_table_and_absent_in_json() {
        let value = pb::TaskOverride {
            rate_limit: Some("10/s".into()),
            ..Default::default()
        };
        let row = task_override_row("send", &value);
        assert_eq!(row[2], "10/s");
        assert!(row[3..].iter().all(|cell| cell == UNSET), "{row:?}");
        assert_eq!(
            task_override_json(&value),
            serde_json::json!({"rateLimit": "10/s"})
        );
    }

    #[test]
    fn a_worker_status_is_short_or_its_number() {
        assert_eq!(
            worker_status_name(pb::WorkerStatus::Draining as i32),
            "draining"
        );
        assert_eq!(worker_status_name(99), "99");
        assert_eq!(worker_status_json(99), Value::from(99));
    }

    #[test]
    fn a_purge_count_is_an_int64_string() {
        let rendered = purge_json(&pb::PurgeDeadLettersResponse {
            purged: 9_007_199_254_740_993,
        });
        assert_eq!(rendered["purged"], "9007199254740993");
    }

    #[test]
    fn an_absent_envelope_field_is_an_empty_object() {
        assert_eq!(queue_envelope_json(None), empty_json());
        assert_eq!(periodic_task_envelope_json(None), empty_json());
    }

    #[test]
    fn a_periodic_task_without_a_timezone_reads_as_utc_in_a_table() {
        let task = pb::PeriodicTask {
            name: "nightly".into(),
            enabled: false,
            ..Default::default()
        };
        let row = periodic_row(&task);
        assert_eq!(row[3], "UTC");
        assert_eq!(row[5], "paused");
        assert!(periodic_task_json(&task).get("timezone").is_none());
    }
}
