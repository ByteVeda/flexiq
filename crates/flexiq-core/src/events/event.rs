//! The event record and its CloudEvents encoding.

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use chrono::{SecondsFormat, TimeZone, Utc};
use serde_json::{json, Map, Value};

use crate::scheduler::retention::DEFAULT_NAMESPACE;

/// Prefix every CloudEvents `type` carries. Reverse-DNS, as the spec asks, so
/// a consumer reading several producers' events can route on it.
pub const CLOUDEVENTS_TYPE_PREFIX: &str = "org.byteveda.flexiq.";

/// CloudEvents `source` when the configuration names none.
pub const DEFAULT_SOURCE: &str = "/flexiq";

/// A job lifecycle transition an egress sink can be sent.
///
/// The names are the cross-SDK in-process taxonomy's, so a filter written for
/// a webhook subscription reads the same here. `job.started` is the one
/// addition: in process it has no event, because the task body starting *is*
/// the signal.
///
/// `#[non_exhaustive]`: closing a lifecycle coverage gap adds a type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum EventType {
    /// A job was written to the queue by one of the server's producer doors.
    JobEnqueued,
    /// A job was claimed and handed to a worker.
    JobStarted,
    /// A job finished successfully.
    JobCompleted,
    /// An attempt failed. Always followed by `job.retrying` or `job.dead`.
    JobFailed,
    /// A failed job was rescheduled for another attempt.
    JobRetrying,
    /// A job was dead-lettered: out of retries, not retryable, or shed.
    JobDead,
    /// A job was cancelled, before or during its run.
    JobCancelled,
    /// An attempt ended in a durable step sleep; the job is pending again.
    JobSleeping,
}

impl EventType {
    /// Every event type, in lifecycle order. A slice, so a new type does not
    /// change its type.
    pub const ALL: &[EventType] = &[
        Self::JobEnqueued,
        Self::JobStarted,
        Self::JobCompleted,
        Self::JobFailed,
        Self::JobRetrying,
        Self::JobDead,
        Self::JobCancelled,
        Self::JobSleeping,
    ];

    /// The short name filters and ids use, e.g. `job.completed`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::JobEnqueued => "job.enqueued",
            Self::JobStarted => "job.started",
            Self::JobCompleted => "job.completed",
            Self::JobFailed => "job.failed",
            Self::JobRetrying => "job.retrying",
            Self::JobDead => "job.dead",
            Self::JobCancelled => "job.cancelled",
            Self::JobSleeping => "job.sleeping",
        }
    }

    /// Parse a short name. `None` for anything this build does not emit.
    pub fn parse(name: &str) -> Option<Self> {
        Self::ALL.iter().copied().find(|t| t.as_str() == name)
    }
}

/// One job lifecycle transition, as the scheduler or a producer door saw it.
///
/// Carries metadata only, plus the payload where the emitter already held it
/// (enqueue and dispatch). Whether a sink sends that payload is the sink's
/// `include_payload`, never the emitter's.
///
/// `#[non_exhaustive]` so a new attribute is not a breaking change: build one
/// with [`JobEvent::new`], then set the public fields.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct JobEvent {
    /// What happened.
    pub event_type: EventType,
    /// The job it happened to.
    pub job_id: String,
    /// Tenant namespace; `None` is the default namespace.
    pub namespace: Option<String>,
    /// Queue the job belongs to. Empty only when the job vanished before its
    /// queue could be read.
    pub queue: String,
    /// Task the job runs.
    pub task_name: String,
    /// `retry_count` of the attempt this event belongs to, when known.
    pub attempt: Option<i32>,
    /// Epoch of the execution claim the attempt ran under, when known.
    pub epoch: Option<i64>,
    /// When the transition was observed, Unix milliseconds.
    pub time_ms: i64,
    /// Error message of a failed attempt.
    pub error: Option<String>,
    /// Whether a failure was an execution timeout.
    pub timed_out: Option<bool>,
    /// Deadline a sleeping job was rescheduled to, Unix milliseconds.
    pub wake_at_ms: Option<i64>,
    /// Execution time the worker measured, nanoseconds.
    pub wall_time_ns: Option<i64>,
    /// Why a job was dead-lettered without running out of retries.
    pub reason: Option<String>,
    /// The job's payload bytes, where the emitter held them.
    pub payload: Option<Vec<u8>>,
}

impl JobEvent {
    /// An event with every optional field empty and the time set to now.
    pub fn new(
        event_type: EventType,
        job_id: impl Into<String>,
        namespace: Option<String>,
        queue: impl Into<String>,
        task_name: impl Into<String>,
    ) -> Self {
        Self {
            event_type,
            job_id: job_id.into(),
            namespace,
            queue: queue.into(),
            task_name: task_name.into(),
            attempt: None,
            epoch: None,
            time_ms: crate::job::now_millis(),
            error: None,
            timed_out: None,
            wake_at_ms: None,
            wall_time_ns: None,
            reason: None,
            payload: None,
        }
    }

    /// The namespace as filters and consumers see it: `default` for `None`.
    pub fn namespace_label(&self) -> &str {
        self.namespace.as_deref().unwrap_or(DEFAULT_NAMESPACE)
    }

    /// The dedupe key: `<job_id>:<attempt>:<epoch>:<type>`, `-` for an unknown
    /// part.
    ///
    /// Deterministic, so every redelivery of one transition carries the same
    /// id. Each part is there because a job can repeat the rest: the attempt
    /// separates retries, the epoch separates two claims of one attempt after
    /// an operator requeue, and the type separates the `job.failed` and
    /// `job.retrying` one failure produces.
    pub fn id(&self) -> String {
        let attempt = self
            .attempt
            .map_or_else(|| "-".to_string(), |a| a.to_string());
        let epoch = self
            .epoch
            .map_or_else(|| "-".to_string(), |e| e.to_string());
        format!(
            "{}:{attempt}:{epoch}:{}",
            self.job_id,
            self.event_type.as_str()
        )
    }

    /// A copy with the payload left out, built field by field so the payload
    /// bytes are never cloned just to be thrown away.
    pub(crate) fn without_payload(&self) -> Self {
        Self {
            event_type: self.event_type,
            job_id: self.job_id.clone(),
            namespace: self.namespace.clone(),
            queue: self.queue.clone(),
            task_name: self.task_name.clone(),
            attempt: self.attempt,
            epoch: self.epoch,
            time_ms: self.time_ms,
            error: self.error.clone(),
            timed_out: self.timed_out,
            wake_at_ms: self.wake_at_ms,
            wall_time_ns: self.wall_time_ns,
            reason: self.reason.clone(),
            payload: None,
        }
    }

    /// This event as a CloudEvents 1.0 JSON object (structured mode).
    ///
    /// The payload goes in only when `include_payload` is set *and* the
    /// emitter held one, base64 as `data.payload_base64`: the bytes are the
    /// wire envelope, not JSON.
    pub fn to_cloudevent(&self, source: &str, include_payload: bool) -> Value {
        let mut data = Map::new();
        data.insert("job_id".into(), json!(self.job_id));
        data.insert("namespace".into(), json!(self.namespace_label()));
        data.insert("queue".into(), json!(self.queue));
        data.insert("task".into(), json!(self.task_name));
        insert_some(&mut data, "attempt", self.attempt);
        insert_some(&mut data, "error", self.error.as_deref());
        insert_some(&mut data, "timed_out", self.timed_out);
        insert_some(&mut data, "wake_at_ms", self.wake_at_ms);
        insert_some(&mut data, "wall_time_ns", self.wall_time_ns);
        insert_some(&mut data, "reason", self.reason.as_deref());
        if include_payload {
            if let Some(payload) = &self.payload {
                data.insert("payload_base64".into(), json!(STANDARD.encode(payload)));
            }
        }
        json!({
            "specversion": "1.0",
            "id": self.id(),
            "source": source,
            "type": format!("{CLOUDEVENTS_TYPE_PREFIX}{}", self.event_type.as_str()),
            "subject": self.job_id,
            "time": rfc3339(self.time_ms),
            "datacontenttype": "application/json",
            "flexiqnamespace": self.namespace_label(),
            "flexiqqueue": self.queue,
            "flexiqtask": self.task_name,
            "data": Value::Object(data),
        })
    }
}

fn insert_some<T: serde::Serialize>(map: &mut Map<String, Value>, key: &str, value: Option<T>) {
    if let Some(value) = value {
        map.insert(key.into(), json!(value));
    }
}

/// Unix milliseconds as RFC 3339 UTC. An out-of-range value falls back to the
/// epoch rather than failing the event: the time is metadata, the id is not.
fn rfc3339(ms: i64) -> String {
    Utc.timestamp_millis_opt(ms)
        .single()
        .unwrap_or_default()
        .to_rfc3339_opts(SecondsFormat::Millis, true)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event() -> JobEvent {
        let mut e = JobEvent::new(EventType::JobRetrying, "j1", None, "emails", "send");
        e.attempt = Some(2);
        e.epoch = Some(7);
        e.time_ms = 1_700_000_000_123;
        e.error = Some("boom".into());
        e.payload = Some(vec![2, 0xa0]);
        e
    }

    #[test]
    fn every_type_round_trips_through_its_name() {
        for &t in EventType::ALL {
            assert_eq!(EventType::parse(t.as_str()), Some(t));
        }
        assert_eq!(EventType::parse("job.exploded"), None);
    }

    #[test]
    fn id_names_attempt_epoch_and_type() {
        assert_eq!(event().id(), "j1:2:7:job.retrying");
        let bare = JobEvent::new(EventType::JobEnqueued, "j1", None, "q", "t");
        assert_eq!(bare.id(), "j1:-:-:job.enqueued");
    }

    #[test]
    fn cloudevent_carries_required_attributes_and_no_payload_by_default() {
        let ce = event().to_cloudevent("/flexiq", false);
        assert_eq!(ce["specversion"], "1.0");
        assert_eq!(ce["id"], "j1:2:7:job.retrying");
        assert_eq!(ce["type"], "org.byteveda.flexiq.job.retrying");
        assert_eq!(ce["source"], "/flexiq");
        assert_eq!(ce["time"], "2023-11-14T22:13:20.123Z");
        assert_eq!(ce["flexiqnamespace"], "default");
        assert_eq!(ce["data"]["error"], "boom");
        assert!(ce["data"].get("payload_base64").is_none());
        assert!(ce["data"].get("wake_at_ms").is_none());
    }

    #[test]
    fn payload_is_base64_only_when_opted_in() {
        let ce = event().to_cloudevent("/flexiq", true);
        assert_eq!(ce["data"]["payload_base64"], "AqA=");
    }
}
