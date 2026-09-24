//! From mapped arguments to stored jobs.
//!
//! Everything about the job except its payload and its deduplication key
//! comes from the definition. That is the whole of the scope bound: a request
//! can choose what the task receives, never which task, queue or namespace
//! receives it.

use flexiq_core::{now_millis, EventHub, Job, NewJob, Storage, StorageBackend};

use crate::trigger::definition::Trigger;

/// Longest delivery id accepted as a deduplication key. Provider ids are
/// tens of characters; anything near this is a body field that was never an
/// id, and keying on it would put a sender-controlled blob in an index.
pub const MAX_KEY_LEN: usize = 256;

/// One job a request asks for.
#[derive(Debug, Clone)]
pub struct Planned {
    /// The payload envelope.
    pub payload: Vec<u8>,
    /// The delivery id, when the trigger keys on one.
    pub delivery: Option<String>,
}

/// What one enqueue came to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Enqueued {
    /// The job's id — the existing one's, when the enqueue deduplicated.
    pub id: String,
    /// Whether an earlier delivery already enqueued it.
    pub deduplicated: bool,
}

/// The job `planned` becomes under `trigger`, in `namespace`.
pub fn new_job(trigger: &Trigger, namespace: &str, planned: Planned) -> NewJob {
    NewJob {
        queue: trigger.queue.clone(),
        task_name: trigger.task.clone(),
        payload: planned.payload,
        priority: trigger.priority,
        scheduled_at: now_millis(),
        max_retries: trigger.max_retries,
        timeout_ms: trigger.timeout_ms,
        // Prefixed with the trigger, so two triggers whose senders happen to
        // reuse an id space cannot swallow each other's deliveries.
        unique_key: planned
            .delivery
            .map(|delivery| format!("trigger:{}:{delivery}", trigger.name)),
        // Which trigger enqueued a job is the first question when one looks
        // wrong, and nothing else on the row would answer it.
        metadata: Some(serde_json::json!({ "trigger": trigger.name }).to_string()),
        notes: None,
        depends_on: Vec::new(),
        expires_at: None,
        result_ttl_ms: None,
        namespace: Some(namespace.to_string()),
        debounce_key: None,
    }
}

/// Store `jobs`, in order, reporting each.
///
/// Not one transaction: a batch here comes from an event platform that
/// redelivers on failure, and with a keyed trigger the redelivery skips what
/// already landed. A keyless one would duplicate — which is why the
/// object-store triggers key on the event id by default.
///
/// Each job this call inserts is announced on `events` as it lands, so one
/// that fails part-way still announces the jobs before it.
pub fn submit(
    storage: &StorageBackend,
    jobs: Vec<NewJob>,
    events: Option<&EventHub>,
) -> flexiq_core::Result<Vec<Enqueued>> {
    jobs.into_iter()
        .map(|job| {
            let (stored, deduplicated): (Job, bool) = if job.unique_key.is_some() {
                storage.enqueue_unique_reporting(job)?
            } else {
                (storage.enqueue(job)?, false)
            };
            if !deduplicated {
                crate::events::enqueued(events, &stored);
            }
            Ok(Enqueued {
                id: stored.id,
                deduplicated,
            })
        })
        .collect()
}
