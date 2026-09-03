//! `flexiq.executor.v1` frames ↔ the worker frame protocol's messages.
//!
//! The only place the two vocabularies meet. Everything else in this module
//! talks in [`ExecutorMessage`]/[`SchedulerMessage`], which is what keeps the
//! gRPC door a transport rather than a second protocol.
//!
//! All four directions live here — a door needs two, and a Rust executor
//! attaching over gRPC needs the other two — so a field can only be forgotten
//! in one direction if the round-trip test below stops naming its frame.
//!
//! # Two give-ups, both stated rather than discovered
//!
//! **The matches cannot be exhaustive.** Both message enums are
//! `#[non_exhaustive]`, so a match outside `flexiq-core` must carry a wildcard
//! and a frame added later compiles here silently. The wildcards therefore log
//! at `error!` rather than returning quietly, and `every_frame_round_trips`
//! names all fourteen so a new one is a test to extend rather than a field to
//! lose.
//!
//! **`hello` loses its token.** The frame protocol carries a shared secret in
//! the handshake; this door's credential is the bearer token the auth layer
//! already checked, and a second credential inside the body would be a second
//! thing to keep in step. The conversion drops it in both directions.

use flexiq_core::step::StepFailure;
use flexiq_core::storage::records::StepKind;
use flexiq_core::worker::protocol::{ExecutorMessage, SchedulerMessage};
use flexiq_core::Lease;
use prost_types::{Duration, Timestamp};

use crate::grpc::pb::executor as pb;

// ── Units ─────────────────────────────────────────────────────────
//
// The frame protocol counts in integers because storage does. The wire uses
// `Duration` and `Timestamp` because §9 of the proto design says every wire
// time does, and the conversion has to live in one module or it lives in
// twenty. Both are exact: milliseconds and nanoseconds both divide evenly into
// what these two messages hold.

/// A positive millisecond budget as a `Duration`. Non-positive means "no
/// limit", which the wire spells as an absent field.
fn duration_from_millis(millis: i64) -> Option<Duration> {
    (millis > 0).then(|| Duration {
        seconds: millis / 1_000,
        nanos: ((millis % 1_000) * 1_000_000) as i32,
    })
}

/// A nanosecond measurement as a `Duration`. Zero is absent, and reads back as
/// zero.
fn duration_from_nanos(nanos: i64) -> Option<Duration> {
    (nanos > 0).then_some(Duration {
        seconds: nanos / 1_000_000_000,
        nanos: (nanos % 1_000_000_000) as i32,
    })
}

fn millis_from_duration(duration: Option<Duration>) -> i64 {
    duration.map_or(0, |d| d.seconds * 1_000 + i64::from(d.nanos) / 1_000_000)
}

fn nanos_from_duration(duration: Option<Duration>) -> i64 {
    duration.map_or(0, |d| d.seconds * 1_000_000_000 + i64::from(d.nanos))
}

/// Unix milliseconds as a `Timestamp`.
///
/// `div_euclid`, not `/`: a `Timestamp`'s nanos must be non-negative, and a
/// truncating division would produce -1 second and +something nanos for any
/// instant before the epoch.
fn timestamp_from_millis(millis: i64) -> Timestamp {
    Timestamp {
        seconds: millis.div_euclid(1_000),
        nanos: (millis.rem_euclid(1_000) * 1_000_000) as i32,
    }
}

fn millis_from_timestamp(timestamp: Timestamp) -> i64 {
    timestamp.seconds * 1_000 + i64::from(timestamp.nanos) / 1_000_000
}

fn lease_to_wire(lease: Option<Lease>) -> Option<Vec<u8>> {
    lease.map(|lease| lease.as_bytes().to_vec())
}

fn lease_from_wire(lease: Option<Vec<u8>>) -> Option<Lease> {
    lease.and_then(|bytes| Lease::from_wire(&bytes))
}

// ── Executor → scheduler ──────────────────────────────────────────

/// The frame an `AttachRequest` carries, or `None` when it carries none this
/// build knows — which is skipped, never fatal.
pub fn to_executor_message(request: pb::AttachRequest) -> Option<(ExecutorMessage, Vec<u8>)> {
    use pb::attach_request::Frame;

    let frame = request.frame.or_else(|| {
        log::debug!("grpc: an executor frame carried no arm this build knows; skipping");
        None
    })?;
    Some(match frame {
        Frame::Hello(hello) => (
            ExecutorMessage::hello(
                hello.executor_id,
                hello.sdk,
                hello.version,
                hello.tasks,
                hello.slots,
            )
            .protocol_version(hello.protocol_version)
            .capabilities(hello.capabilities)
            .build(),
            Vec::new(),
        ),
        Frame::Success(success) => {
            // Presence here, a declared length there. An absent `result` means
            // the task returned nothing; an empty one means it returned an
            // empty value, and the frame protocol draws the same distinction.
            let result_len = success.result.as_ref().map(Vec::len);
            let payload = success.result.unwrap_or_default();
            (
                ExecutorMessage::Success {
                    job_id: success.job_id,
                    result_len,
                    task_name: success.task_name,
                    wall_time_ns: nanos_from_duration(success.wall_time),
                    lease: lease_from_wire(success.lease),
                },
                payload,
            )
        }
        Frame::Failure(failure) => (
            ExecutorMessage::Failure {
                job_id: failure.job_id,
                error: failure.error,
                retry_count: failure.retry_count,
                max_retries: failure.max_retries,
                task_name: failure.task_name,
                wall_time_ns: nanos_from_duration(failure.wall_time),
                should_retry: failure.should_retry,
                timed_out: failure.timed_out,
                lease: lease_from_wire(failure.lease),
            },
            Vec::new(),
        ),
        Frame::Cancelled(cancelled) => (
            ExecutorMessage::Cancelled {
                job_id: cancelled.job_id,
                task_name: cancelled.task_name,
                wall_time_ns: nanos_from_duration(cancelled.wall_time),
                lease: lease_from_wire(cancelled.lease),
            },
            Vec::new(),
        ),
        Frame::Slept(slept) => (
            ExecutorMessage::Slept {
                job_id: slept.job_id,
                task_name: slept.task_name,
                wake_at: slept.wake_at.map(millis_from_timestamp).unwrap_or_default(),
                wall_time_ns: nanos_from_duration(slept.wall_time),
                lease: lease_from_wire(slept.lease),
            },
            Vec::new(),
        ),
        Frame::Progress(progress) => (
            ExecutorMessage::Progress {
                job_id: progress.job_id,
                progress: progress.progress,
                lease: lease_from_wire(progress.lease),
            },
            Vec::new(),
        ),
        Frame::TaskLog(log) => {
            let extra_len = log.extra.as_ref().map(Vec::len);
            let payload = log.extra.unwrap_or_default();
            (
                ExecutorMessage::TaskLog {
                    job_id: log.job_id,
                    task_name: log.task_name,
                    level: log.level,
                    message: log.message,
                    extra_len,
                    lease: lease_from_wire(log.lease),
                },
                payload,
            )
        }
        Frame::StepCommit(commit) => {
            let payload = commit.payload;
            (
                ExecutorMessage::StepCommit {
                    job_id: commit.job_id,
                    seq: commit.seq,
                    step_key: commit.step_key,
                    kind: step_kind_from_wire(commit.kind)?,
                    wake_at: commit.wake_at.map(millis_from_timestamp),
                    payload_len: payload.len(),
                    lease: lease_from_wire(commit.lease),
                },
                payload,
            )
        }
    })
}

/// The `AttachRequest` for one executor frame, or `None` for a frame this build
/// cannot render.
pub fn from_executor_message(
    message: ExecutorMessage,
    payload: Vec<u8>,
) -> Option<pb::AttachRequest> {
    use pb::attach_request::Frame;

    let frame = match message {
        ExecutorMessage::Hello {
            executor_id,
            sdk,
            version,
            tasks,
            slots,
            protocol_version,
            capabilities,
            ..
        } => Frame::Hello(pb::HelloFrame {
            executor_id,
            sdk,
            version,
            tasks,
            slots,
            protocol_version,
            capabilities,
        }),
        ExecutorMessage::Success {
            job_id,
            result_len,
            task_name,
            wall_time_ns,
            lease,
        } => Frame::Success(pb::SuccessFrame {
            job_id,
            task_name,
            result: result_len.map(|_| payload),
            wall_time: duration_from_nanos(wall_time_ns),
            lease: lease_to_wire(lease),
        }),
        ExecutorMessage::Failure {
            job_id,
            error,
            retry_count,
            max_retries,
            task_name,
            wall_time_ns,
            should_retry,
            timed_out,
            lease,
        } => Frame::Failure(pb::FailureFrame {
            job_id,
            task_name,
            error,
            retry_count,
            max_retries,
            wall_time: duration_from_nanos(wall_time_ns),
            should_retry,
            timed_out,
            lease: lease_to_wire(lease),
        }),
        ExecutorMessage::Cancelled {
            job_id,
            task_name,
            wall_time_ns,
            lease,
        } => Frame::Cancelled(pb::CancelledFrame {
            job_id,
            task_name,
            wall_time: duration_from_nanos(wall_time_ns),
            lease: lease_to_wire(lease),
        }),
        ExecutorMessage::Slept {
            job_id,
            task_name,
            wake_at,
            wall_time_ns,
            lease,
        } => Frame::Slept(pb::SleptFrame {
            job_id,
            task_name,
            wake_at: Some(timestamp_from_millis(wake_at)),
            wall_time: duration_from_nanos(wall_time_ns),
            lease: lease_to_wire(lease),
        }),
        ExecutorMessage::Progress {
            job_id,
            progress,
            lease,
        } => Frame::Progress(pb::ProgressFrame {
            job_id,
            progress,
            lease: lease_to_wire(lease),
        }),
        ExecutorMessage::TaskLog {
            job_id,
            task_name,
            level,
            message,
            extra_len,
            lease,
        } => Frame::TaskLog(pb::TaskLogFrame {
            job_id,
            task_name,
            level,
            message,
            extra: extra_len.map(|_| payload),
            lease: lease_to_wire(lease),
        }),
        ExecutorMessage::StepCommit {
            job_id,
            seq,
            step_key,
            kind,
            wake_at,
            lease,
            ..
        } => Frame::StepCommit(pb::StepCommitFrame {
            job_id,
            seq,
            step_key,
            kind: step_kind_to_wire(kind).into(),
            payload,
            wake_at: wake_at.map(timestamp_from_millis),
            lease: lease_to_wire(lease),
        }),
        // `Heartbeat` is not on this stream by design — it is a unary RPC — and
        // anything else is a frame added to the protocol after this module.
        other => {
            log::error!("grpc: no executor wire frame for {other:?}; dropping it");
            return None;
        }
    };
    Some(pb::AttachRequest { frame: Some(frame) })
}

// ── Scheduler → executor ──────────────────────────────────────────

/// The `AttachResponse` for one scheduler frame, or `None` for a frame this
/// build cannot render.
pub fn from_scheduler_message(
    message: SchedulerMessage,
    payload: Vec<u8>,
) -> Option<pb::AttachResponse> {
    use pb::attach_response::Frame;

    let frame = match message {
        SchedulerMessage::HelloAck {
            scheduler_id,
            protocol_version,
            capabilities,
        } => Frame::HelloAck(pb::HelloAckFrame {
            scheduler_id,
            protocol_version,
            capabilities,
        }),
        SchedulerMessage::Job {
            id,
            task_name,
            retry_count,
            max_retries,
            queue,
            timeout_ms,
            namespace,
            disabled_middleware,
            metadata,
            lease,
            ..
        } => Frame::Job(pb::JobFrame {
            id,
            task_name,
            payload,
            retry_count,
            max_retries,
            queue,
            timeout: duration_from_millis(timeout_ms),
            namespace,
            disabled_middleware,
            metadata,
            lease: lease_to_wire(lease),
        }),
        SchedulerMessage::JobSteps { job_id, .. } => Frame::JobSteps(pb::JobStepsFrame {
            job_id,
            snapshot: payload,
        }),
        SchedulerMessage::StepAck {
            job_id,
            seq,
            ok,
            already,
            wake_at,
            error,
            failure,
        } => Frame::StepAck(pb::StepAckFrame {
            job_id,
            seq,
            ok,
            already,
            wake_at: wake_at.map(timestamp_from_millis),
            error,
            failure: step_failure_to_wire(failure).into(),
        }),
        SchedulerMessage::Cancel { job_id } => Frame::Cancel(pb::CancelFrame { job_id }),
        SchedulerMessage::Shutdown => Frame::Shutdown(pb::ShutdownFrame {}),
        other => {
            log::error!("grpc: no scheduler wire frame for {other:?}; dropping it");
            return None;
        }
    };
    Some(pb::AttachResponse { frame: Some(frame) })
}

/// The frame an `AttachResponse` carries, or `None` when it carries none this
/// build knows — which is skipped, never fatal.
pub fn to_scheduler_message(response: pb::AttachResponse) -> Option<(SchedulerMessage, Vec<u8>)> {
    use pb::attach_response::Frame;

    let frame = response.frame.or_else(|| {
        log::debug!("grpc: a scheduler frame carried no arm this build knows; skipping");
        None
    })?;
    Some(match frame {
        Frame::HelloAck(ack) => (
            SchedulerMessage::HelloAck {
                scheduler_id: ack.scheduler_id,
                protocol_version: ack.protocol_version,
                capabilities: ack.capabilities,
            },
            Vec::new(),
        ),
        Frame::Job(job) => {
            let payload = job.payload;
            (
                SchedulerMessage::Job {
                    id: job.id,
                    task_name: job.task_name,
                    payload_len: payload.len(),
                    retry_count: job.retry_count,
                    max_retries: job.max_retries,
                    queue: job.queue,
                    timeout_ms: millis_from_duration(job.timeout),
                    namespace: job.namespace,
                    disabled_middleware: job.disabled_middleware,
                    metadata: job.metadata,
                    lease: lease_from_wire(job.lease),
                },
                payload,
            )
        }
        Frame::JobSteps(steps) => {
            let payload = steps.snapshot;
            (
                SchedulerMessage::JobSteps {
                    job_id: steps.job_id,
                    payload_len: payload.len(),
                },
                payload,
            )
        }
        Frame::StepAck(ack) => (
            SchedulerMessage::StepAck {
                job_id: ack.job_id,
                seq: ack.seq,
                ok: ack.ok,
                already: ack.already,
                wake_at: ack.wake_at.map(millis_from_timestamp),
                error: ack.error,
                failure: step_failure_from_wire(ack.failure),
            },
            Vec::new(),
        ),
        Frame::Cancel(cancel) => (
            SchedulerMessage::Cancel {
                job_id: cancel.job_id,
            },
            Vec::new(),
        ),
        Frame::Shutdown(_) => (SchedulerMessage::Shutdown, Vec::new()),
    })
}

// ── Enums ─────────────────────────────────────────────────────────

fn step_kind_to_wire(kind: StepKind) -> pb::StepKind {
    match kind {
        StepKind::Run => pb::StepKind::Run,
        StepKind::Sleep => pb::StepKind::Sleep,
    }
}

/// `None` for `UNSPECIFIED` and for a value this build does not know.
///
/// A commit whose kind cannot be read is dropped rather than guessed: `Run` and
/// `Sleep` write different rows and end the attempt differently, so defaulting
/// either way would be worse than skipping the frame.
fn step_kind_from_wire(kind: i32) -> Option<StepKind> {
    match pb::StepKind::try_from(kind) {
        Ok(pb::StepKind::Run) => Some(StepKind::Run),
        Ok(pb::StepKind::Sleep) => Some(StepKind::Sleep),
        Ok(pb::StepKind::Unspecified) | Err(_) => {
            log::error!("grpc: a step commit named no step kind this build knows; skipping it");
            None
        }
    }
}

fn step_failure_to_wire(failure: Option<StepFailure>) -> pb::StepFailure {
    match failure {
        None => pb::StepFailure::Unspecified,
        Some(StepFailure::Retryable) => pb::StepFailure::Retryable,
        Some(StepFailure::Permanent) => pb::StepFailure::Permanent,
        Some(StepFailure::Superseded) => pb::StepFailure::Superseded,
    }
}

/// An unknown classification reads as none at all, which the executor treats as
/// a refusal it was given no advice about — never as permission to retry.
fn step_failure_from_wire(failure: i32) -> Option<StepFailure> {
    match pb::StepFailure::try_from(failure) {
        Ok(pb::StepFailure::Retryable) => Some(StepFailure::Retryable),
        Ok(pb::StepFailure::Permanent) => Some(StepFailure::Permanent),
        Ok(pb::StepFailure::Superseded) => Some(StepFailure::Superseded),
        Ok(pb::StepFailure::Unspecified) | Err(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip_executor(message: ExecutorMessage, payload: &[u8]) -> (ExecutorMessage, Vec<u8>) {
        let wire = from_executor_message(message, payload.to_vec()).expect("a wire frame");
        to_executor_message(wire).expect("a message back")
    }

    fn round_trip_scheduler(
        message: SchedulerMessage,
        payload: &[u8],
    ) -> (SchedulerMessage, Vec<u8>) {
        let wire = from_scheduler_message(message, payload.to_vec()).expect("a wire frame");
        to_scheduler_message(wire).expect("a message back")
    }

    #[test]
    fn every_frame_round_trips() {
        // Fourteen frames, named one by one. The enums are `#[non_exhaustive]`,
        // so nothing here can be a compiler check — a frame added to the
        // protocol has to be added to this list, and the wildcard arms log
        // loudly in the meantime.
        let (back, payload) = round_trip_executor(
            ExecutorMessage::hello("exec-1", "rust", "1.2.3", vec!["a".into(), "b".into()], 4)
                .capabilities(vec!["steps".into(), "lease".into()])
                .build(),
            b"",
        );
        assert!(payload.is_empty());
        let ExecutorMessage::Hello {
            executor_id,
            tasks,
            slots,
            capabilities,
            ..
        } = back
        else {
            panic!("expected hello");
        };
        assert_eq!(executor_id, "exec-1");
        assert_eq!(tasks, vec!["a".to_string(), "b".to_string()]);
        assert_eq!(slots, 4);
        assert_eq!(capabilities, vec!["steps".to_string(), "lease".to_string()]);

        let lease = Lease::from_epoch(987_654_321);
        let (back, payload) = round_trip_executor(
            ExecutorMessage::Success {
                job_id: "job-1".into(),
                result_len: Some(5),
                task_name: "charge".into(),
                wall_time_ns: 1_500_000_123,
                lease: Some(lease.clone()),
            },
            b"hello",
        );
        assert_eq!(payload, b"hello");
        let ExecutorMessage::Success {
            result_len,
            wall_time_ns,
            lease: back_lease,
            ..
        } = back
        else {
            panic!("expected success");
        };
        assert_eq!(result_len, Some(5));
        assert_eq!(wall_time_ns, 1_500_000_123);
        assert_eq!(back_lease, Some(lease.clone()));

        let (back, _) = round_trip_executor(
            ExecutorMessage::Failure {
                job_id: "job-1".into(),
                error: "boom".into(),
                retry_count: 2,
                max_retries: 5,
                task_name: "charge".into(),
                wall_time_ns: 7,
                should_retry: true,
                timed_out: true,
                lease: None,
            },
            b"",
        );
        assert!(matches!(
            back,
            ExecutorMessage::Failure {
                retry_count: 2,
                max_retries: 5,
                should_retry: true,
                timed_out: true,
                ..
            }
        ));

        let (back, _) = round_trip_executor(
            ExecutorMessage::Cancelled {
                job_id: "job-1".into(),
                task_name: "charge".into(),
                wall_time_ns: 9,
                lease: Some(lease.clone()),
            },
            b"",
        );
        assert!(matches!(
            back,
            ExecutorMessage::Cancelled {
                wall_time_ns: 9,
                ..
            }
        ));

        let (back, _) = round_trip_executor(
            ExecutorMessage::Slept {
                job_id: "job-1".into(),
                task_name: "charge".into(),
                wake_at: 1_764_000_000_123,
                wall_time_ns: 11,
                lease: None,
            },
            b"",
        );
        assert!(matches!(
            back,
            ExecutorMessage::Slept {
                wake_at: 1_764_000_000_123,
                ..
            }
        ));

        let (back, _) = round_trip_executor(
            ExecutorMessage::Progress {
                job_id: "job-1".into(),
                progress: 42,
                lease: Some(lease.clone()),
            },
            b"",
        );
        assert!(matches!(
            back,
            ExecutorMessage::Progress { progress: 42, .. }
        ));

        let (back, payload) = round_trip_executor(
            ExecutorMessage::TaskLog {
                job_id: "job-1".into(),
                task_name: "charge".into(),
                level: "result".into(),
                message: String::new(),
                extra_len: Some(2),
                lease: None,
            },
            b"{}",
        );
        assert_eq!(payload, b"{}");
        assert!(matches!(
            back,
            ExecutorMessage::TaskLog {
                extra_len: Some(2),
                ..
            }
        ));

        let (back, payload) = round_trip_executor(
            ExecutorMessage::StepCommit {
                job_id: "job-1".into(),
                seq: 3,
                step_key: "charge#0".into(),
                kind: StepKind::Sleep,
                wake_at: Some(1_764_000_000_500),
                payload_len: 4,
                lease: Some(lease.clone()),
            },
            b"body",
        );
        assert_eq!(payload, b"body");
        assert!(matches!(
            back,
            ExecutorMessage::StepCommit {
                seq: 3,
                kind: StepKind::Sleep,
                payload_len: 4,
                wake_at: Some(1_764_000_000_500),
                ..
            }
        ));

        let (back, _) = round_trip_scheduler(
            SchedulerMessage::HelloAck {
                scheduler_id: "sched-1".into(),
                protocol_version: 1,
                capabilities: vec!["side_channel".into()],
            },
            b"",
        );
        assert!(matches!(back, SchedulerMessage::HelloAck { .. }));

        let (back, payload) = round_trip_scheduler(
            SchedulerMessage::Job {
                id: "job-1".into(),
                task_name: "charge".into(),
                payload_len: 7,
                retry_count: 1,
                max_retries: 3,
                queue: "default".into(),
                timeout_ms: 30_000,
                namespace: Some("tenant".into()),
                disabled_middleware: vec!["otel".into()],
                metadata: Some("{\"a\":1}".into()),
                lease: Some(lease.clone()),
            },
            b"payload",
        );
        assert_eq!(payload, b"payload");
        let SchedulerMessage::Job {
            payload_len,
            timeout_ms,
            namespace,
            disabled_middleware,
            metadata,
            lease: back_lease,
            ..
        } = back
        else {
            panic!("expected a job");
        };
        assert_eq!(payload_len, 7);
        assert_eq!(timeout_ms, 30_000);
        assert_eq!(namespace.as_deref(), Some("tenant"));
        assert_eq!(disabled_middleware, vec!["otel".to_string()]);
        assert_eq!(metadata.as_deref(), Some("{\"a\":1}"));
        assert_eq!(back_lease, Some(lease));

        let (back, payload) = round_trip_scheduler(
            SchedulerMessage::JobSteps {
                job_id: "job-1".into(),
                payload_len: 3,
            },
            b"abc",
        );
        assert_eq!(payload, b"abc");
        assert!(matches!(
            back,
            SchedulerMessage::JobSteps { payload_len: 3, .. }
        ));

        let (back, _) = round_trip_scheduler(
            SchedulerMessage::StepAck {
                job_id: "job-1".into(),
                seq: 2,
                ok: false,
                already: true,
                wake_at: Some(1_764_000_000_007),
                error: Some("nope".into()),
                failure: Some(StepFailure::Superseded),
            },
            b"",
        );
        assert!(matches!(
            back,
            SchedulerMessage::StepAck {
                seq: 2,
                ok: false,
                already: true,
                wake_at: Some(1_764_000_000_007),
                failure: Some(StepFailure::Superseded),
                ..
            }
        ));

        let (back, _) = round_trip_scheduler(
            SchedulerMessage::Cancel {
                job_id: "job-1".into(),
            },
            b"",
        );
        assert!(matches!(back, SchedulerMessage::Cancel { .. }));

        let (back, _) = round_trip_scheduler(SchedulerMessage::Shutdown, b"");
        assert!(matches!(back, SchedulerMessage::Shutdown));
    }

    #[test]
    fn a_returned_nothing_and_a_returned_empty_value_stay_different() {
        // The distinction the frame protocol draws in `result_len` and the wire
        // draws in field presence. Collapsing them turns "the task returned
        // None" into "the task returned b''" for every gRPC executor.
        let (nothing, _) = round_trip_executor(
            ExecutorMessage::Success {
                job_id: "job-1".into(),
                result_len: None,
                task_name: "t".into(),
                wall_time_ns: 1,
                lease: None,
            },
            b"",
        );
        let (empty, _) = round_trip_executor(
            ExecutorMessage::Success {
                job_id: "job-1".into(),
                result_len: Some(0),
                task_name: "t".into(),
                wall_time_ns: 1,
                lease: None,
            },
            b"",
        );
        assert!(matches!(
            nothing,
            ExecutorMessage::Success {
                result_len: None,
                ..
            }
        ));
        assert!(matches!(
            empty,
            ExecutorMessage::Success {
                result_len: Some(0),
                ..
            }
        ));
    }

    #[test]
    fn a_frame_with_no_arm_is_skipped_rather_than_fatal() {
        assert!(to_executor_message(pb::AttachRequest { frame: None }).is_none());
        assert!(to_scheduler_message(pb::AttachResponse { frame: None }).is_none());
    }

    #[test]
    fn a_non_positive_timeout_is_no_timeout() {
        // The frame protocol spells "no limit" as a non-positive integer and
        // the wire spells it as an absent field. Both directions have to agree
        // or every gRPC dispatch would carry a timeout of zero milliseconds.
        for timeout_ms in [0, -1] {
            let (back, _) = round_trip_scheduler(
                SchedulerMessage::Job {
                    id: "job-1".into(),
                    task_name: "charge".into(),
                    payload_len: 0,
                    retry_count: 0,
                    max_retries: 0,
                    queue: "default".into(),
                    timeout_ms,
                    namespace: None,
                    disabled_middleware: Vec::new(),
                    metadata: None,
                    lease: None,
                },
                b"",
            );
            assert!(
                matches!(back, SchedulerMessage::Job { timeout_ms: 0, .. }),
                "a non-positive timeout must read back as no timeout"
            );
        }
    }

    #[test]
    fn a_lease_that_is_not_a_token_resolves_to_no_lease() {
        // Refused, not accepted: a peer echoing bytes the scheduler never
        // minted is not the current dispatch.
        let mut wire = from_executor_message(
            ExecutorMessage::Progress {
                job_id: "job-1".into(),
                progress: 1,
                lease: Some(Lease::from_epoch(1)),
            },
            Vec::new(),
        )
        .expect("a wire frame");
        let Some(pb::attach_request::Frame::Progress(progress)) = wire.frame.as_mut() else {
            panic!("expected progress");
        };
        progress.lease = Some(vec![0xff, 0xfe]);

        let (back, _) = to_executor_message(wire).expect("a message back");
        assert!(matches!(
            back,
            ExecutorMessage::Progress { lease: None, .. }
        ));
    }

    #[test]
    fn a_step_commit_naming_no_kind_is_skipped() {
        let mut wire = from_executor_message(
            ExecutorMessage::StepCommit {
                job_id: "job-1".into(),
                seq: 0,
                step_key: "k".into(),
                kind: StepKind::Run,
                wake_at: None,
                payload_len: 0,
                lease: None,
            },
            Vec::new(),
        )
        .expect("a wire frame");
        let Some(pb::attach_request::Frame::StepCommit(commit)) = wire.frame.as_mut() else {
            panic!("expected a step commit");
        };
        commit.kind = pb::StepKind::Unspecified.into();

        assert!(
            to_executor_message(wire).is_none(),
            "a commit whose kind cannot be read is dropped, never guessed"
        );
    }

    #[test]
    fn a_heartbeat_has_no_place_on_the_dispatch_stream() {
        // It is a unary RPC. Rendering one here would be the design's stated
        // failure — heartbeats riding the stream they are meant to report on.
        assert!(
            from_executor_message(ExecutorMessage::Heartbeat { free_slots: 3 }, Vec::new())
                .is_none()
        );
    }
}
