//! Writing a `flexiq.admin.v1` response as proto3 JSON.
//!
//! [`super::response`]'s rules, unchanged: a field with explicit presence is
//! written only when set, an `int64` is a string, an enum is its name, bytes
//! are base64, and a message with no fields is `{}`. A `Job` is written by the
//! producer's own writer, so a replayed or triggered job reads exactly as an
//! enqueued one does.
//!
//! The drift test at the bottom holds every message here to the descriptor,
//! the same way the producer's are held.

use serde_json::{Map, Value};

use super::response::{insert_timestamp, int64, job};
use super::wkt::{bytes_to_json, duration_to_json};
use crate::grpc::pb::admin as pb;

/// A message with no fields.
pub fn empty<T>(_: &T) -> Value {
    Value::Object(Map::new())
}

// ── Queues ───────────────────────────────────────────────────────────

/// One `Queue`.
fn queue(queue: &pb::Queue) -> Value {
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

/// A singular message field, written only when the message holds it.
fn insert_message<T>(
    object: &mut Map<String, Value>,
    key: &str,
    value: Option<&T>,
    render: fn(&T) -> Value,
) {
    if let Some(value) = value {
        object.insert(key.to_string(), render(value));
    }
}

/// One message holding one optional message field.
fn wrapping<T>(key: &str, value: Option<&T>, render: fn(&T) -> Value) -> Value {
    let mut object = Map::new();
    insert_message(&mut object, key, value, render);
    Value::Object(object)
}

/// `ListQueuesResponse`.
pub fn list_queues(response: &pb::ListQueuesResponse) -> Value {
    Value::Object(Map::from_iter([(
        "queues".to_string(),
        response.queues.iter().map(queue).collect::<Value>(),
    )]))
}

/// `PauseQueueResponse`.
pub fn pause_queue(response: &pb::PauseQueueResponse) -> Value {
    wrapping("queue", response.queue.as_ref(), queue)
}

/// `ResumeQueueResponse`.
pub fn resume_queue(response: &pb::ResumeQueueResponse) -> Value {
    wrapping("queue", response.queue.as_ref(), queue)
}

/// One `QueueThroughput`.
fn queue_throughput(counts: &pb::QueueThroughput) -> Value {
    Value::Object(Map::from_iter([
        ("queue".to_string(), counts.queue.clone().into()),
        ("completed".to_string(), int64(counts.completed)),
        ("failed".to_string(), int64(counts.failed)),
        ("dead".to_string(), int64(counts.dead)),
        ("cancelled".to_string(), int64(counts.cancelled)),
    ]))
}

/// `GetThroughputResponse`.
pub fn get_throughput(response: &pb::GetThroughputResponse) -> Value {
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
            .map(queue_throughput)
            .collect::<Value>(),
    );
    Value::Object(object)
}

// ── Dead letters ─────────────────────────────────────────────────────

/// One `DeadLetter`.
fn dead_letter(entry: &pb::DeadLetter) -> Value {
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
        object.insert("payload".to_string(), bytes_to_json(payload).into());
    }
    Value::Object(object)
}

/// `ListDeadLettersResponse`.
pub fn list_dead_letters(response: &pb::ListDeadLettersResponse) -> Value {
    Value::Object(Map::from_iter([
        (
            "deadLetters".to_string(),
            response
                .dead_letters
                .iter()
                .map(dead_letter)
                .collect::<Value>(),
        ),
        (
            "nextPageToken".to_string(),
            response.next_page_token.clone().into(),
        ),
    ]))
}

/// `GetDeadLetterResponse`.
pub fn get_dead_letter(response: &pb::GetDeadLetterResponse) -> Value {
    wrapping("deadLetter", response.dead_letter.as_ref(), dead_letter)
}

/// `ReplayDeadLetterResponse`.
pub fn replay_dead_letter(response: &pb::ReplayDeadLetterResponse) -> Value {
    wrapping("job", response.job.as_ref(), job)
}

/// `PurgeDeadLettersResponse`.
pub fn purge_dead_letters(response: &pb::PurgeDeadLettersResponse) -> Value {
    Value::Object(Map::from_iter([(
        "purged".to_string(),
        int64(response.purged),
    )]))
}

// ── Workers ──────────────────────────────────────────────────────────

/// One `Worker`.
fn worker(worker: &pb::Worker) -> Value {
    let mut object = Map::new();
    object.insert("workerId".to_string(), worker.worker_id.clone().into());
    object.insert("queues".to_string(), worker.queues.clone().into());
    object.insert("status".to_string(), worker_status(worker.status));
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

/// A `WorkerStatus` by name, or by number when this build does not know it —
/// the contract tells a reader to show an unknown one as unknown, which it can
/// only do if it is told the number.
fn worker_status(status: i32) -> Value {
    match pb::WorkerStatus::try_from(status) {
        Ok(known) => known.as_str_name().into(),
        Err(_) => status.into(),
    }
}

/// `ListWorkersResponse`.
pub fn list_workers(response: &pb::ListWorkersResponse) -> Value {
    Value::Object(Map::from_iter([(
        "workers".to_string(),
        response.workers.iter().map(worker).collect::<Value>(),
    )]))
}

/// `DrainWorkerResponse`.
pub fn drain_worker(response: &pb::DrainWorkerResponse) -> Value {
    wrapping("worker", response.worker.as_ref(), worker)
}

// ── Periodic tasks ───────────────────────────────────────────────────

/// One `PeriodicTask`.
fn periodic_task(task: &pb::PeriodicTask) -> Value {
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
        object.insert("payload".to_string(), bytes_to_json(payload).into());
    }
    Value::Object(object)
}

/// `ListPeriodicTasksResponse`.
pub fn list_periodic_tasks(response: &pb::ListPeriodicTasksResponse) -> Value {
    Value::Object(Map::from_iter([(
        "periodicTasks".to_string(),
        response
            .periodic_tasks
            .iter()
            .map(periodic_task)
            .collect::<Value>(),
    )]))
}

/// `GetPeriodicTaskResponse`.
pub fn get_periodic_task(response: &pb::GetPeriodicTaskResponse) -> Value {
    wrapping(
        "periodicTask",
        response.periodic_task.as_ref(),
        periodic_task,
    )
}

/// `PutPeriodicTaskResponse`.
pub fn put_periodic_task(response: &pb::PutPeriodicTaskResponse) -> Value {
    wrapping(
        "periodicTask",
        response.periodic_task.as_ref(),
        periodic_task,
    )
}

/// `PausePeriodicTaskResponse`.
pub fn pause_periodic_task(response: &pb::PausePeriodicTaskResponse) -> Value {
    wrapping(
        "periodicTask",
        response.periodic_task.as_ref(),
        periodic_task,
    )
}

/// `ResumePeriodicTaskResponse`.
pub fn resume_periodic_task(response: &pb::ResumePeriodicTaskResponse) -> Value {
    wrapping(
        "periodicTask",
        response.periodic_task.as_ref(),
        periodic_task,
    )
}

/// `TriggerPeriodicTaskResponse`.
pub fn trigger_periodic_task(response: &pb::TriggerPeriodicTaskResponse) -> Value {
    wrapping("job", response.job.as_ref(), job)
}

// ── Overrides ────────────────────────────────────────────────────────

/// One `TaskOverride`. Every field but the two durations and `updateTime` has
/// explicit presence, and those three are messages — so an unset field is a
/// missing key throughout, which is what "not overridden" looks like.
fn task_override(value: &pb::TaskOverride) -> Value {
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
fn queue_override(value: &pb::QueueOverride) -> Value {
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
pub fn list_overrides(response: &pb::ListOverridesResponse) -> Value {
    let tasks: Map<String, Value> = response
        .tasks
        .iter()
        .map(|(name, value)| (name.clone(), task_override(value)))
        .collect();
    let queues: Map<String, Value> = response
        .queues
        .iter()
        .map(|(name, value)| (name.clone(), queue_override(value)))
        .collect();
    Value::Object(Map::from_iter([
        ("tasks".to_string(), Value::Object(tasks)),
        ("queues".to_string(), Value::Object(queues)),
    ]))
}

/// `SetTaskOverrideResponse`.
pub fn set_task_override(response: &pb::SetTaskOverrideResponse) -> Value {
    wrapping(
        "taskOverride",
        response.task_override.as_ref(),
        task_override,
    )
}

/// `SetQueueOverrideResponse`.
pub fn set_queue_override(response: &pb::SetQueueOverrideResponse) -> Value {
    wrapping(
        "queueOverride",
        response.queue_override.as_ref(),
        queue_override,
    )
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::grpc::facade::descriptor::ADMIN_PACKAGE;
    use crate::grpc::facade::json::response::tests::{assert_package_names, populated_job};
    use crate::grpc::producer::convert::{duration, timestamp};

    fn assert_names(message: &str, rendered: &Value) {
        assert_package_names(ADMIN_PACKAGE, message, rendered);
    }

    fn populated_queue() -> pb::Queue {
        pb::Queue {
            name: "emails".to_string(),
            paused: true,
            pending: 1,
            running: 2,
            completed: 3,
            failed: 4,
            dead: 5,
            cancelled: 6,
        }
    }

    fn populated_dead_letter() -> pb::DeadLetter {
        pb::DeadLetter {
            id: "dl-1".to_string(),
            original_job_id: "job-1".to_string(),
            queue: "emails".to_string(),
            task_name: "send".to_string(),
            failed_at: Some(timestamp(1_756_900_000_000)),
            retry_count: 3,
            max_retries: 3,
            priority: 1,
            replay_count: 2,
            error: Some("boom".to_string()),
            metadata: Some("{}".to_string()),
            payload: Some(vec![0x02]),
        }
    }

    fn populated_worker() -> pb::Worker {
        pb::Worker {
            worker_id: "w-1".to_string(),
            queues: vec!["emails".to_string()],
            status: pb::WorkerStatus::Active as i32,
            last_heartbeat: Some(timestamp(1_756_900_000_000)),
            concurrency: 4,
            started_at: Some(timestamp(1_756_899_000_000)),
            hostname: Some("host".to_string()),
            pid: Some(42),
            pool_type: Some("thread".to_string()),
            sdk: Some("rust".to_string()),
            sdk_version: Some("2.0.0".to_string()),
        }
    }

    fn populated_periodic_task() -> pb::PeriodicTask {
        pb::PeriodicTask {
            name: "nightly".to_string(),
            task_name: "report".to_string(),
            cron: "0 0 3 * * *".to_string(),
            queue: "default".to_string(),
            enabled: true,
            next_run: Some(timestamp(1_756_900_000_000)),
            last_run: Some(timestamp(1_756_800_000_000)),
            timezone: Some("UTC".to_string()),
            payload: Some(vec![0x02]),
        }
    }

    fn populated_task_override() -> pb::TaskOverride {
        pb::TaskOverride {
            rate_limit: Some("100/m".to_string()),
            max_concurrent: Some(2),
            max_retries: Some(3),
            retry_backoff: Some(duration(1_500)),
            timeout: Some(duration(30_000)),
            priority: Some(5),
            paused: Some(false),
            update_time: Some(timestamp(1_756_900_000_000)),
        }
    }

    fn populated_queue_override() -> pb::QueueOverride {
        pb::QueueOverride {
            rate_limit: Some("10/s".to_string()),
            max_concurrent: Some(4),
            update_time: Some(timestamp(1_756_900_000_000)),
        }
    }

    /// Every message this module renders, each fully populated, against the
    /// JSON names the contract gives it.
    #[test]
    fn every_admin_message_carries_every_field_the_contract_names() {
        let queue_value = populated_queue();
        assert_names("Queue", &queue(&queue_value));
        assert_names(
            "ListQueuesResponse",
            &list_queues(&pb::ListQueuesResponse {
                queues: vec![queue_value.clone()],
            }),
        );
        assert_names(
            "PauseQueueResponse",
            &pause_queue(&pb::PauseQueueResponse {
                queue: Some(queue_value.clone()),
            }),
        );
        assert_names(
            "ResumeQueueResponse",
            &resume_queue(&pb::ResumeQueueResponse {
                queue: Some(queue_value),
            }),
        );

        let counts = pb::QueueThroughput {
            queue: "emails".to_string(),
            completed: 1,
            failed: 2,
            dead: 3,
            cancelled: 4,
        };
        assert_names("QueueThroughput", &queue_throughput(&counts));
        assert_names(
            "GetThroughputResponse",
            &get_throughput(&pb::GetThroughputResponse {
                window: Some(duration(300_000)),
                since: Some(timestamp(1_756_900_000_000)),
                queues: vec![counts],
            }),
        );

        let entry = populated_dead_letter();
        assert_names("DeadLetter", &dead_letter(&entry));
        assert_names(
            "ListDeadLettersResponse",
            &list_dead_letters(&pb::ListDeadLettersResponse {
                dead_letters: vec![entry.clone()],
                next_page_token: "cursor".to_string(),
            }),
        );
        assert_names(
            "GetDeadLetterResponse",
            &get_dead_letter(&pb::GetDeadLetterResponse {
                dead_letter: Some(entry),
            }),
        );
        assert_names(
            "ReplayDeadLetterResponse",
            &replay_dead_letter(&pb::ReplayDeadLetterResponse {
                job: Some(populated_job()),
            }),
        );
        assert_names(
            "DeleteDeadLetterResponse",
            &empty(&pb::DeleteDeadLetterResponse {}),
        );
        assert_names(
            "PurgeDeadLettersResponse",
            &purge_dead_letters(&pb::PurgeDeadLettersResponse { purged: 7 }),
        );

        let worker_value = populated_worker();
        assert_names("Worker", &worker(&worker_value));
        assert_names(
            "ListWorkersResponse",
            &list_workers(&pb::ListWorkersResponse {
                workers: vec![worker_value.clone()],
            }),
        );
        assert_names(
            "DrainWorkerResponse",
            &drain_worker(&pb::DrainWorkerResponse {
                worker: Some(worker_value),
            }),
        );

        let task = populated_periodic_task();
        assert_names("PeriodicTask", &periodic_task(&task));
        assert_names(
            "ListPeriodicTasksResponse",
            &list_periodic_tasks(&pb::ListPeriodicTasksResponse {
                periodic_tasks: vec![task.clone()],
            }),
        );
        assert_names(
            "GetPeriodicTaskResponse",
            &get_periodic_task(&pb::GetPeriodicTaskResponse {
                periodic_task: Some(task.clone()),
            }),
        );
        assert_names(
            "PutPeriodicTaskResponse",
            &put_periodic_task(&pb::PutPeriodicTaskResponse {
                periodic_task: Some(task.clone()),
            }),
        );
        assert_names(
            "PausePeriodicTaskResponse",
            &pause_periodic_task(&pb::PausePeriodicTaskResponse {
                periodic_task: Some(task.clone()),
            }),
        );
        assert_names(
            "ResumePeriodicTaskResponse",
            &resume_periodic_task(&pb::ResumePeriodicTaskResponse {
                periodic_task: Some(task),
            }),
        );
        assert_names(
            "DeletePeriodicTaskResponse",
            &empty(&pb::DeletePeriodicTaskResponse {}),
        );
        assert_names(
            "TriggerPeriodicTaskResponse",
            &trigger_periodic_task(&pb::TriggerPeriodicTaskResponse {
                job: Some(populated_job()),
            }),
        );

        let task_value = populated_task_override();
        let queue_value = populated_queue_override();
        assert_names("TaskOverride", &task_override(&task_value));
        assert_names("QueueOverride", &queue_override(&queue_value));
        assert_names(
            "ListOverridesResponse",
            &list_overrides(&pb::ListOverridesResponse {
                tasks: HashMap::from([("send".to_string(), task_value.clone())]),
                queues: HashMap::from([("emails".to_string(), queue_value.clone())]),
            }),
        );
        assert_names(
            "SetTaskOverrideResponse",
            &set_task_override(&pb::SetTaskOverrideResponse {
                task_override: Some(task_value),
            }),
        );
        assert_names(
            "SetQueueOverrideResponse",
            &set_queue_override(&pb::SetQueueOverrideResponse {
                queue_override: Some(queue_value),
            }),
        );
        assert_names(
            "ClearTaskOverrideResponse",
            &empty(&pb::ClearTaskOverrideResponse {}),
        );
        assert_names(
            "ClearQueueOverrideResponse",
            &empty(&pb::ClearQueueOverrideResponse {}),
        );
    }

    #[test]
    fn a_map_is_an_object_keyed_by_the_maps_own_keys() {
        let rendered = list_overrides(&pb::ListOverridesResponse {
            tasks: HashMap::from([("send".to_string(), populated_task_override())]),
            queues: HashMap::new(),
        });
        assert_eq!(rendered["tasks"]["send"]["timeout"], Value::from("30s"));
        assert_eq!(
            rendered["tasks"]["send"]["retryBackoff"],
            Value::from("1.500s")
        );
        assert_eq!(rendered["queues"], Value::Object(Map::new()));
    }

    #[test]
    fn an_unset_override_field_is_a_missing_key() {
        let rendered = task_override(&pb::TaskOverride::default());
        assert_eq!(rendered, Value::Object(Map::new()));
    }

    #[test]
    fn a_worker_status_is_its_name_and_an_unknown_one_is_its_number() {
        let mut value = populated_worker();
        assert_eq!(
            worker(&value)["status"],
            Value::from("WORKER_STATUS_ACTIVE")
        );
        value.status = 99;
        assert_eq!(worker(&value)["status"], Value::from(99));
    }

    #[test]
    fn a_purge_count_is_a_string() {
        let rendered = purge_dead_letters(&pb::PurgeDeadLettersResponse {
            purged: 9_007_199_254_740_993,
        });
        assert_eq!(rendered["purged"], Value::from("9007199254740993"));
    }
}
