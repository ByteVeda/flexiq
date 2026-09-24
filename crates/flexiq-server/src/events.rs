//! Job events the server's own doors emit.
//!
//! The scheduler emits every transition from `job.started` on. What only a
//! door sees is the write itself: an enqueue, and a cancel that lands before
//! the job ever ran. A running job's cancel is the scheduler's to report, once
//! the attempt actually ends.
//!
//! Workflow node jobs (`SubmitWorkflow`) get no door `job.enqueued`:
//! `submit_workflow` writes them and returns only ids. The scheduler still
//! emits from `job.started` on.
//!
//! Every helper here takes the hub as an `Option` so a door calls it
//! unconditionally; with no `FLEXIQ_EVENTS_FILE` it does nothing.

use std::sync::Arc;

use flexiq_core::{EventHub, EventType, Job, JobEvent};

/// The hub the doors share, when events are configured.
pub type Events = Option<Arc<EventHub>>;

/// Emit `job.enqueued` for a job a door just inserted.
///
/// Only for a row this call wrote: a unique or debounced enqueue that answered
/// with an existing job must not call it, or one job would be announced twice
/// under two different enqueues.
pub fn enqueued(hub: Option<&EventHub>, job: &Job) {
    let Some(hub) = hub else {
        return;
    };
    let mut event = door_event(EventType::JobEnqueued, job);
    event.attempt = Some(0);
    // The row is already in hand, so a payload costs a copy and no read.
    if hub.wants_payload() {
        event.payload = Some(job.payload.clone());
    }
    hub.emit(event);
}

/// Emit `job.cancelled` for a pending job a door just cancelled.
///
/// Only when storage reported the cancel took: a running job's cancel is a
/// request the task has yet to honour, and the scheduler emits it when it does.
pub fn cancelled(hub: Option<&EventHub>, job: &Job) {
    let Some(hub) = hub else {
        return;
    };
    let mut event = door_event(EventType::JobCancelled, job);
    // A pending job can be a retry waiting its turn, so the attempt is the
    // row's, not zero.
    event.attempt = Some(job.retry_count);
    hub.emit(event);
}

/// The hub's metrics in Prometheus text, or nothing when events are off, so a
/// deployment without them exposes no empty `flexiq_events_*` families.
pub fn render_metrics(hub: Option<&EventHub>) -> String {
    hub.map(EventHub::render_prometheus).unwrap_or_default()
}

fn door_event(event_type: EventType, job: &Job) -> JobEvent {
    JobEvent::new(
        event_type,
        job.id.clone(),
        job.namespace.clone(),
        job.queue.clone(),
        job.task_name.clone(),
    )
}
