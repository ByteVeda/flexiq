//! The boundary between the core's integers and the wire's well-known types.
//!
//! Everything inside `flexiq-core` measures time in Unix milliseconds — a plain
//! `i64` on `Job`, `NewJob` and `DebounceOptions`. The wire uses
//! `google.protobuf.Timestamp` and `google.protobuf.Duration`, because a
//! duration expressed as a bare integer is a field whose unit lives in a
//! comment, and comments are not part of a contract.
//!
//! That conversion lives here and nowhere else. Spread across the handlers it
//! would be twenty chances to divide by the wrong thousand.

use flexiq_core::job::{Job, JobStatus, NewJob};
use flexiq_core::storage::records::DebounceOptions;
use prost_types::{Duration as ProtoDuration, Timestamp};

use crate::grpc::pb;
use crate::grpc::status::WireError;

/// The queue an enqueue lands in when the request names none.
///
/// Substituted here rather than left empty: it is the name every SDK already
/// defaults to, and a queue literally called `""` is not addressable anyway.
const DEFAULT_QUEUE: &str = "default";

/// Milliseconds per second, as the two integer widths the conversions need.
const MILLIS_PER_SEC: i64 = 1_000;
const NANOS_PER_MILLI: i32 = 1_000_000;

// ── Time ─────────────────────────────────────────────────────────────

/// Unix milliseconds as a `Timestamp`.
pub fn timestamp(millis: i64) -> Timestamp {
    // `div_euclid`/`rem_euclid` rather than `/` and `%`: a pre-epoch instant is
    // negative, and truncating division would put the nanos field negative,
    // which is not a valid Timestamp.
    Timestamp {
        seconds: millis.div_euclid(MILLIS_PER_SEC),
        nanos: (millis.rem_euclid(MILLIS_PER_SEC) as i32) * NANOS_PER_MILLI,
    }
}

/// A `Timestamp` as Unix milliseconds, truncating sub-millisecond precision.
///
/// Truncation is the documented behaviour, not an accident: storage has no
/// finer resolution, so a nanosecond a client sends has nowhere to be kept.
pub fn millis_from_timestamp(value: &Timestamp) -> i64 {
    value
        .seconds
        .saturating_mul(MILLIS_PER_SEC)
        .saturating_add(i64::from(value.nanos / NANOS_PER_MILLI))
}

/// Milliseconds as a `Duration`.
pub fn duration(millis: i64) -> ProtoDuration {
    ProtoDuration {
        seconds: millis.div_euclid(MILLIS_PER_SEC),
        nanos: (millis.rem_euclid(MILLIS_PER_SEC) as i32) * NANOS_PER_MILLI,
    }
}

/// A `Duration` as milliseconds, truncating sub-millisecond precision.
pub fn millis_from_duration(value: &ProtoDuration) -> i64 {
    value
        .seconds
        .saturating_mul(MILLIS_PER_SEC)
        .saturating_add(i64::from(value.nanos / NANOS_PER_MILLI))
}

// ── Status ───────────────────────────────────────────────────────────

/// The core's discriminant as the wire's enum.
///
/// The two are offset by one: zero is spent on `_UNSPECIFIED` here, and the
/// core starts at `Pending = 0`. The match is exhaustive with no wildcard, so
/// adding a variant on either side fails to compile rather than quietly mapping
/// to something wrong.
pub fn status_to_wire(status: JobStatus) -> pb::JobStatus {
    match status {
        JobStatus::Pending => pb::JobStatus::Pending,
        JobStatus::Running => pb::JobStatus::Running,
        JobStatus::Complete => pb::JobStatus::Complete,
        JobStatus::Failed => pb::JobStatus::Failed,
        JobStatus::Dead => pb::JobStatus::Dead,
        JobStatus::Cancelled => pb::JobStatus::Cancelled,
    }
}

/// The wire's enum as the core's discriminant.
///
/// `Unspecified` has no counterpart, which is what makes it usable as "no
/// filter" on a listing rather than as a status in its own right.
pub fn status_from_wire(status: pb::JobStatus) -> Option<JobStatus> {
    match status {
        pb::JobStatus::Unspecified => None,
        pb::JobStatus::Pending => Some(JobStatus::Pending),
        pb::JobStatus::Running => Some(JobStatus::Running),
        pb::JobStatus::Complete => Some(JobStatus::Complete),
        pb::JobStatus::Failed => Some(JobStatus::Failed),
        pb::JobStatus::Dead => Some(JobStatus::Dead),
        pb::JobStatus::Cancelled => Some(JobStatus::Cancelled),
    }
}

// ── Job ──────────────────────────────────────────────────────────────

/// What a response is allowed to carry of a job's blobs.
///
/// Absent and empty are different answers for both fields, so "the caller did
/// not ask" cannot be expressed by sending an empty value — it is expressed by
/// sending no value, which is what makes them `optional bytes`.
#[derive(Debug, Clone, Copy, Default)]
pub struct Blobs {
    /// Send `Job.payload`.
    pub payload: bool,
    /// Send `Job.result`.
    pub result: bool,
}

impl Blobs {
    /// Neither blob — what a listing sends, and what a cancel answers with.
    pub const NONE: Self = Self {
        payload: false,
        result: false,
    };
}

/// A core job as the wire's `Job`.
pub fn job_to_wire(job: Job, blobs: Blobs) -> pb::Job {
    pb::Job {
        id: job.id,
        queue: job.queue,
        task_name: job.task_name,
        status: status_to_wire(job.status) as i32,
        priority: job.priority,
        created_at: Some(timestamp(job.created_at)),
        scheduled_at: Some(timestamp(job.scheduled_at)),
        retry_count: job.retry_count,
        max_retries: job.max_retries,
        timeout: Some(duration(job.timeout_ms)),
        cancel_requested: job.cancel_requested,
        has_deps: job.has_deps,
        namespace: job.namespace.unwrap_or_default(),
        started_at: job.started_at.map(timestamp),
        completed_at: job.completed_at.map(timestamp),
        payload: blobs.payload.then_some(job.payload),
        result: if blobs.result { job.result } else { None },
        error: job.error,
        progress: job.progress,
        metadata: job.metadata,
        notes: job.notes,
        unique_key: job.unique_key,
        expires_at: job.expires_at.map(timestamp),
        result_ttl: job.result_ttl_ms.map(duration),
        debounce_key: job.debounce_key,
    }
}

// ── Enqueue ──────────────────────────────────────────────────────────

/// The default a job takes when the request sets no timeout.
///
/// The same five minutes the SDKs default to, so a job enqueued over the wire
/// and one enqueued in-process behave alike.
const DEFAULT_TIMEOUT_MS: i64 = 300_000;

/// Build a `NewJob` from one request.
///
/// `namespace` is the server's own and is never read from the request: nothing
/// a client sends can change which namespace it writes into.
pub fn new_job(
    task_name: String,
    payload: Vec<u8>,
    options: Option<pb::EnqueueOptions>,
    namespace: &str,
    now_millis: i64,
) -> Result<NewJob, WireError> {
    let options = options.unwrap_or_default();
    if task_name.is_empty() {
        return Err(WireError::invalid_request("task_name must not be empty"));
    }

    let queue = if options.queue.is_empty() {
        DEFAULT_QUEUE.to_string()
    } else {
        options.queue
    };

    let debounce_key = options.debounce.as_ref().map(|d| d.key.clone());

    Ok(NewJob {
        queue,
        task_name,
        payload,
        priority: options.priority,
        scheduled_at: options
            .scheduled_at
            .as_ref()
            .map_or(now_millis, millis_from_timestamp),
        max_retries: options.max_retries,
        timeout_ms: options
            .timeout
            .as_ref()
            .map_or(DEFAULT_TIMEOUT_MS, millis_from_duration),
        unique_key: options.unique_key,
        metadata: options.metadata,
        notes: options.notes,
        depends_on: options.depends_on,
        expires_at: options.expires_at.as_ref().map(millis_from_timestamp),
        result_ttl_ms: options.result_ttl.as_ref().map(millis_from_duration),
        namespace: Some(namespace.to_string()),
        debounce_key,
    })
}

/// The debounce block of a request, when it has one.
///
/// The core validates the three fields against each other on the way in; what
/// this adds is that a `window` or `max_wait` a client left unset is refused
/// here rather than silently read as zero, which the core would then reject
/// with a message about a field the client never sent.
pub fn debounce_options(
    options: &pb::EnqueueOptions,
) -> Result<Option<DebounceOptions>, WireError> {
    let Some(debounce) = options.debounce.as_ref() else {
        return Ok(None);
    };
    if debounce.key.is_empty() {
        return Err(WireError::invalid_request("debounce.key must not be empty"));
    }
    let Some(window) = debounce.window.as_ref() else {
        return Err(WireError::invalid_request("debounce.window is required"));
    };
    let Some(max_wait) = debounce.max_wait.as_ref() else {
        return Err(WireError::invalid_request("debounce.max_wait is required"));
    };

    Ok(Some(DebounceOptions {
        window_ms: millis_from_duration(window),
        max_wait_ms: millis_from_duration(max_wait),
        replace_payload: debounce.replace_payload,
        max_pending: debounce.max_pending,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn milliseconds_round_trip_through_both_well_known_types() {
        for millis in [0_i64, 1, 999, 1_000, 1_500, 1_700_000_000_123] {
            assert_eq!(millis_from_timestamp(&timestamp(millis)), millis);
            assert_eq!(millis_from_duration(&duration(millis)), millis);
        }
    }

    #[test]
    fn a_pre_epoch_instant_keeps_its_nanos_non_negative() {
        // Truncating division would produce nanos = -500_000_000 here, which is
        // not a valid Timestamp and which readers render an hour out.
        let value = timestamp(-1_500);
        assert_eq!(value.seconds, -2);
        assert_eq!(value.nanos, 500_000_000);
        assert_eq!(millis_from_timestamp(&value), -1_500);
    }

    #[test]
    fn sub_millisecond_precision_is_truncated_not_rounded() {
        let value = Timestamp {
            seconds: 1,
            nanos: 999_999,
        };
        assert_eq!(millis_from_timestamp(&value), 1_000);
    }

    #[test]
    fn the_status_enums_are_offset_by_exactly_one() {
        for status in [
            JobStatus::Pending,
            JobStatus::Running,
            JobStatus::Complete,
            JobStatus::Failed,
            JobStatus::Dead,
            JobStatus::Cancelled,
        ] {
            let wire = status_to_wire(status);
            assert_eq!(wire as i32, status as i32 + 1);
            assert_eq!(status_from_wire(wire), Some(status));
        }
        assert_eq!(status_from_wire(pb::JobStatus::Unspecified), None);
    }

    fn sample_job() -> Job {
        let mut job = NewJob {
            queue: "q".into(),
            task_name: "t".into(),
            payload: vec![1, 2, 3],
            priority: 0,
            scheduled_at: 10,
            max_retries: 3,
            timeout_ms: 1_000,
            unique_key: None,
            metadata: None,
            notes: None,
            depends_on: vec![],
            expires_at: None,
            result_ttl_ms: None,
            namespace: Some("ns".into()),
            debounce_key: None,
        }
        .into_job();
        job.result = Some(Vec::new());
        job
    }

    #[test]
    fn a_blob_not_asked_for_is_absent_and_not_empty() {
        let wire = job_to_wire(sample_job(), Blobs::NONE);
        assert_eq!(wire.payload, None);
        assert_eq!(wire.result, None);

        // An empty result the job really returned is present-and-empty, which
        // is a different answer from "not requested".
        let wire = job_to_wire(
            sample_job(),
            Blobs {
                payload: true,
                result: true,
            },
        );
        assert_eq!(wire.payload, Some(vec![1, 2, 3]));
        assert_eq!(wire.result, Some(Vec::new()));
    }

    #[test]
    fn an_empty_queue_becomes_the_default_one() {
        let job = new_job("t".into(), vec![], None, "ns", 42).expect("valid");
        assert_eq!(job.queue, DEFAULT_QUEUE);
        assert_eq!(job.scheduled_at, 42);
        assert_eq!(job.timeout_ms, DEFAULT_TIMEOUT_MS);
        assert_eq!(job.namespace.as_deref(), Some("ns"));
    }

    #[test]
    fn an_empty_task_name_is_refused() {
        // `NewJob` is not Debug, so unwrap the error side by hand.
        let Err(error) = new_job(String::new(), vec![], None, "ns", 0) else {
            panic!("an empty task_name must be refused");
        };
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn a_debounce_block_missing_a_window_is_refused_by_name() {
        let options = pb::EnqueueOptions {
            debounce: Some(pb::Debounce {
                key: "k".into(),
                window: None,
                max_wait: Some(duration(10)),
                replace_payload: false,
                max_pending: None,
            }),
            ..Default::default()
        };
        let error = debounce_options(&options).expect_err("refused");
        assert!(error.message().contains("window"));
    }
}
