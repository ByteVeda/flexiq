//! Wire protocol shared by every out-of-process worker transport.
//!
//! A frame is a JSON header line followed by exactly the number of raw payload
//! bytes it declares:
//!
//! ```text
//! {"type":"job","id":"018f…","task_name":"resize","payload_len":7,…}\n
//! <7 raw bytes>
//! ```
//!
//! The blob stays raw instead of base64 inside the header so the bytes on the
//! wire *are* the wire-envelope bytes of `BINDING_CONTRACT.md`. Headers stay
//! JSON so every SDK can write an executor with its standard library alone.
//! The same format serves a pipe (prefork's stdio children) and a socket.
//!
//! Because a header declares its own payload length, a peer can skip a frame
//! type it has never heard of and stay aligned — see
//! [`FrameReader::read_or_skip`], which is how a live session survives the far
//! side being upgraded first. That only holds while the length stays findable
//! without knowing the frame's shape: **a frame type added from here on must
//! declare its payload length in a field named `payload_len`**. The two frames
//! that predate the rule (`success`, `task_log`) are aliased into it.

use std::io::{BufRead, BufWriter, Read, Write};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use super::auth::Secret;
use crate::job::{Job, JobStatus};
use crate::scheduler::JobResult;

/// Frame format version. Both sides announce it in the handshake; a mismatch
/// is rejected rather than silently downgraded.
///
/// Optional additions do *not* bump this. A version bump forces scheduler and
/// executors to upgrade in lockstep, which is exactly the coupling an attached
/// deployment exists to remove; anything a peer can do without is negotiated
/// through [`SchedulerMessage::HelloAck`]'s capability list instead.
pub const PROTOCOL_VERSION: u32 = 1;

/// Capability: the scheduler applies [`ExecutorMessage::Progress`] and
/// [`ExecutorMessage::TaskLog`] frames to storage on the executor's behalf.
///
/// An executor emits neither frame unless the `hello_ack` advertised this, so
/// a new executor attached to an older scheduler sends nothing it cannot
/// understand — it degrades to dropping progress and logs, as it did before
/// the side-channel existed.
pub const CAP_SIDE_CHANNEL: &str = "side_channel";

/// Header cap, bounding a peer that never sends a newline.
pub const MAX_HEADER_BYTES: u64 = 64 * 1024;

/// Payload cap. A header declares its own length, so without this a corrupt
/// length field would allocate unboundedly.
pub const MAX_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;

/// Errors raised while framing or parsing a message.
#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    /// The underlying stream failed.
    #[error("protocol I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Peer closed cleanly *between* frames — an orderly shutdown, not
    /// corruption. A close mid-frame surfaces as [`ProtocolError::Io`].
    #[error("peer closed the connection")]
    Eof,

    /// A header line hit [`MAX_HEADER_BYTES`] without a newline.
    #[error("frame header exceeds {MAX_HEADER_BYTES} bytes")]
    HeaderTooLarge,

    /// A header declared a payload over [`MAX_PAYLOAD_BYTES`].
    #[error("frame payload of {len} bytes exceeds the {MAX_PAYLOAD_BYTES} byte limit")]
    PayloadTooLarge {
        /// Length the header declared.
        len: usize,
    },

    /// The header line was not valid JSON for the expected frame type.
    #[error("malformed frame header: {0}")]
    Json(#[from] serde_json::Error),

    /// Header length disagreed with the payload handed to the writer. Always a
    /// caller bug — the reader would desync on it.
    #[error("frame declared {declared} payload bytes but {actual} were supplied")]
    PayloadLengthMismatch {
        /// Length the header declared.
        declared: usize,
        /// Length actually supplied.
        actual: usize,
    },

    /// The peer speaks a different version of this protocol.
    #[error("protocol version mismatch: we speak {ours}, peer speaks {theirs}")]
    VersionMismatch {
        /// Version this build speaks.
        ours: u32,
        /// Version the peer announced.
        theirs: u32,
    },

    /// A valid frame arrived where the state machine expected another kind.
    #[error("expected a {expected} frame")]
    UnexpectedFrame {
        /// Frame the reader was waiting for.
        expected: &'static str,
    },
}

/// A message the scheduler sends to an executor.
///
/// `#[non_exhaustive]`: the contract says the wire is designed to grow, and a
/// reader that cannot name a frame already skips it rather than failing. A
/// match outside this crate therefore has to carry a `_` arm anyway — saying so
/// in the type is what keeps the next frame type a *minor* release instead of a
/// major one.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[non_exhaustive]
pub enum SchedulerMessage {
    /// Answer to [`ExecutorMessage::Hello`], completing the handshake.
    HelloAck {
        /// Identity of the scheduler that accepted the attach.
        scheduler_id: String,
        /// Version the scheduler speaks, so a rejected peer can log both.
        protocol_version: u32,
        /// Optional behaviours this scheduler supports, e.g.
        /// [`CAP_SIDE_CHANNEL`]. Absent on a scheduler built before the list
        /// existed, which is why it defaults to empty rather than being
        /// required: an executor that sees no capability sends no new frames.
        #[serde(default)]
        capabilities: Vec<String>,
    },
    /// Run a job. The task payload follows the header as raw bytes.
    Job {
        /// Job id, echoed back on the result frame.
        id: String,
        /// Task to run.
        task_name: String,
        /// Length of the payload blob that follows.
        payload_len: usize,
        /// Retries already attempted.
        retry_count: i32,
        /// The job's retry cap.
        max_retries: i32,
        /// Queue the job came from.
        queue: String,
        /// Execution timeout in milliseconds; `<= 0` means none.
        timeout_ms: i64,
        /// Namespace the job belongs to.
        namespace: Option<String>,
        /// Middleware the operator has disabled for this task, resolved by the
        /// scheduler at dispatch time.
        ///
        /// An executor has no storage to read the toggle list from, so it
        /// rides the dispatch instead. That changes the semantics from
        /// "re-read on every invocation" to "attached to every dispatch",
        /// which is observably identical: a toggle flipped in the dashboard
        /// still takes effect on the next job, with nothing to restart.
        #[serde(default)]
        disabled_middleware: Vec<String>,
        /// The job's metadata blob, as stored. Middleware reads it, and an
        /// executor cannot fetch the row itself.
        #[serde(default)]
        metadata: Option<String>,
    },
    /// Cooperative-cancel request, so the executor observes a cancel without
    /// either side polling storage.
    Cancel {
        /// Job to cancel.
        job_id: String,
    },
    /// Stop accepting work and exit once in-flight jobs finish.
    Shutdown,
}

/// A message an executor sends to the scheduler.
///
/// `#[non_exhaustive]` for the same reason as [`SchedulerMessage`].
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[non_exhaustive]
pub enum ExecutorMessage {
    /// First frame on every connection: who is attaching and what it can run.
    ///
    /// The variant is `#[non_exhaustive]` on top of the enum: the handshake is
    /// where the protocol actually grows — `token` and `capabilities` both
    /// arrived after it shipped — and a field added to a variant an external
    /// crate can write as a literal is a major release on its own. Build one
    /// with [`ExecutorMessage::hello`].
    #[non_exhaustive]
    Hello {
        /// Stable identity of this executor.
        executor_id: String,
        /// SDK the executor is built on, e.g. `"python"`.
        sdk: String,
        /// SDK version string, for logs and inventory.
        version: String,
        /// Tasks this executor has handlers for. Nothing else is sent to it.
        tasks: Vec<String>,
        /// How many jobs it can run concurrently.
        slots: u32,
        /// Version the executor speaks.
        protocol_version: u32,
        /// Shared secret, when the scheduler is configured to require one.
        ///
        /// Omitted from the wire when absent so a transport that needs no
        /// credential — a pipe to a prefork child, a Unix socket behind
        /// filesystem permissions — sends the same frame it always did.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        token: Option<Secret>,
    },
    /// Liveness signal carrying current free capacity.
    Heartbeat {
        /// Slots free right now.
        free_slots: u32,
    },
    /// Progress for a running job, to be applied to storage by the scheduler.
    ///
    /// Fire-and-forget: there is nothing to answer, and a task that only wanted
    /// to report progress must never block on the scheduler to do it. Sent only
    /// when the `hello_ack` advertised [`CAP_SIDE_CHANNEL`].
    Progress {
        /// Job being reported on. The scheduler drops a frame naming a job the
        /// sending executor is not running.
        job_id: String,
        /// Completion percentage, 0-100.
        progress: i32,
    },
    /// One structured log line for a running job, applied by the scheduler.
    ///
    /// A published partial is this frame at level `result`, so `log` and
    /// `publish` share one frame type — the same collapse
    /// [`crate::storage::Storage::write_task_log`] already makes. Sent only
    /// when the `hello_ack` advertised [`CAP_SIDE_CHANNEL`].
    TaskLog {
        /// Job being logged against.
        job_id: String,
        /// Task that is running.
        task_name: String,
        /// Log level, e.g. `"info"`, or `"result"` for a published partial.
        level: String,
        /// Human-readable message. Empty for a published partial, whose value
        /// lives in `extra`.
        message: String,
        /// Length of the pre-encoded JSON `extra` blob that follows, or `None`
        /// when there is none.
        ///
        /// Carried as the frame's payload rather than inside the header: a
        /// published partial can be arbitrarily large, and a header is capped
        /// at [`MAX_HEADER_BYTES`].
        extra_len: Option<usize>,
    },
    /// The task completed. Its serialized result, if any, follows the header.
    Success {
        /// Job that finished.
        job_id: String,
        /// Length of the result blob, or `None` when the task returned nothing.
        /// `Some(0)` is an empty result, not a missing one.
        result_len: Option<usize>,
        /// Task that ran.
        task_name: String,
        /// Wall-clock execution time in nanoseconds.
        wall_time_ns: i64,
    },
    /// The task raised or timed out.
    Failure {
        /// Job that failed.
        job_id: String,
        /// Error message (canonical JSON `TaskError` when structured).
        error: String,
        /// Retries already attempted before this failure.
        retry_count: i32,
        /// The job's retry cap.
        max_retries: i32,
        /// Task that ran.
        task_name: String,
        /// Wall-clock execution time in nanoseconds.
        wall_time_ns: i64,
        /// Whether the failure is retryable. The executor decides — only it can
        /// see the exception; the core never inspects one.
        should_retry: bool,
        /// True when the failure was an execution timeout.
        timed_out: bool,
    },
    /// The task observed its cancel request and stopped.
    Cancelled {
        /// Job that was cancelled.
        job_id: String,
        /// Task that ran.
        task_name: String,
        /// Wall-clock execution time in nanoseconds.
        wall_time_ns: i64,
    },
    /// The attempt ended in a `step.sleep`, and the job is already `Pending` at
    /// its deadline. Ends the attempt like a cancel, without being a failure.
    Slept {
        /// Job that is sleeping.
        job_id: String,
        /// Task that ran.
        task_name: String,
        /// Deadline the job was rescheduled to, in Unix milliseconds. The one
        /// storage settled on, which on a replay is not the one the executor
        /// proposed.
        wake_at: i64,
        /// Wall-clock time the attempt ran before it slept, in nanoseconds.
        wall_time_ns: i64,
    },
}

/// Builder for the [`ExecutorMessage::Hello`] frame, from
/// [`ExecutorMessage::hello`].
///
/// Exists because the variant is `#[non_exhaustive]`. Its whole point is that
/// a field added to the handshake leaves every caller here compiling, so the
/// setters take the optional half only and the required half stays on
/// [`ExecutorMessage::hello`].
#[derive(Debug, Clone)]
pub struct HelloBuilder {
    executor_id: String,
    sdk: String,
    version: String,
    tasks: Vec<String>,
    slots: u32,
    protocol_version: u32,
    token: Option<Secret>,
}

impl HelloBuilder {
    /// Announce a version other than [`PROTOCOL_VERSION`]. Only a test that
    /// exercises the mismatch rejection has a reason to.
    pub fn protocol_version(mut self, protocol_version: u32) -> Self {
        self.protocol_version = protocol_version;
        self
    }

    /// Attach the shared secret, when the scheduler requires one. `None` sends
    /// the frame without the key at all, which is what a transport secured by
    /// something other than a token does.
    pub fn token(mut self, token: Option<Secret>) -> Self {
        self.token = token;
        self
    }

    /// Finish the frame.
    pub fn build(self) -> ExecutorMessage {
        ExecutorMessage::Hello {
            executor_id: self.executor_id,
            sdk: self.sdk,
            version: self.version,
            tasks: self.tasks,
            slots: self.slots,
            protocol_version: self.protocol_version,
            token: self.token,
        }
    }
}

/// A frame header that may declare a trailing binary blob. Implemented by both
/// message enums so the reader and writer stay generic over direction.
pub trait Frame: Serialize + DeserializeOwned {
    /// Bytes of payload that follow this header; zero for frames carrying none.
    fn payload_len(&self) -> usize;

    /// Whether `tag` names a frame type this build can parse.
    ///
    /// Tells a frame from a *newer* peer, which is safe to skip, from a corrupt
    /// one of a type we do know — a real disagreement that must not be papered
    /// over, because skipping it would silently lose a dispatch or a result.
    fn is_known_type(tag: &str) -> bool;
}

impl Frame for SchedulerMessage {
    fn payload_len(&self) -> usize {
        match self {
            Self::Job { payload_len, .. } => *payload_len,
            _ => 0,
        }
    }

    fn is_known_type(tag: &str) -> bool {
        matches!(tag, "hello_ack" | "job" | "cancel" | "shutdown")
    }
}

impl Frame for ExecutorMessage {
    fn payload_len(&self) -> usize {
        match self {
            Self::Success { result_len, .. } => result_len.unwrap_or(0),
            Self::TaskLog { extra_len, .. } => extra_len.unwrap_or(0),
            _ => 0,
        }
    }

    fn is_known_type(tag: &str) -> bool {
        matches!(
            tag,
            "hello" | "heartbeat" | "progress" | "task_log" | "success" | "failure" | "cancelled"
        )
    }
}

/// One frame off the wire, which the reader may not be able to name.
///
/// Returned by [`FrameReader::read_or_skip`], the tolerant read a live session
/// uses. The handshake keeps the strict [`FrameReader::read`]: a peer that
/// cannot say hello has nothing to offer, so there is nothing to be tolerant of.
#[derive(Debug)]
pub enum Incoming<F> {
    /// A frame this build understands, with its payload.
    Known(F, Vec<u8>),
    /// A frame type this build does not know. Its payload has already been
    /// consumed, so the stream is still aligned on a frame boundary.
    Unknown {
        /// The `type` tag the peer sent, for the log line that reports it.
        frame_type: String,
    },
}

/// The two things a frame this build cannot name must still yield: what it is,
/// and how many payload bytes follow it.
///
/// The length is what makes an unknown frame skippable rather than fatal, so
/// **any frame type added from now on must declare its payload length as
/// `payload_len`** — a peer released before it has nothing else to go on. The
/// two names that predate the rule, `result_len` (`success`) and `extra_len`
/// (`task_log`), are aliased so a later frame modelled on either still skips
/// cleanly rather than desyncing the wire.
#[derive(Debug, Deserialize)]
struct FramePreamble {
    #[serde(rename = "type")]
    frame_type: String,
    #[serde(default, alias = "result_len", alias = "extra_len")]
    payload_len: Option<usize>,
}

impl From<&Job> for SchedulerMessage {
    fn from(job: &Job) -> Self {
        Self::job_with(job, Vec::new())
    }
}

/// A dispatched job, as an executor sees it: the job itself plus the toggle
/// list the scheduler resolved for it.
///
/// The disable list is not a [`Job`] column — it is dashboard state resolved
/// per dispatch — so it travels beside the job rather than inside it.
#[derive(Debug)]
pub struct Dispatch {
    /// The job to run.
    pub job: Job,
    /// Middleware disabled for this task, as resolved by the scheduler.
    pub disabled_middleware: Vec<String>,
}

impl SchedulerMessage {
    /// Build a dispatch frame for `job`, carrying the middleware the scheduler
    /// resolved as disabled for its task.
    pub fn job_with(job: &Job, disabled_middleware: Vec<String>) -> Self {
        Self::Job {
            id: job.id.clone(),
            task_name: job.task_name.clone(),
            payload_len: job.payload.len(),
            retry_count: job.retry_count,
            max_retries: job.max_retries,
            queue: job.queue.clone(),
            timeout_ms: job.timeout_ms,
            namespace: job.namespace.clone(),
            disabled_middleware,
            metadata: job.metadata.clone(),
        }
    }

    /// Rebuild what a dispatch frame describes. `None` for control frames
    /// (`hello_ack`, `cancel`, `shutdown`).
    ///
    /// The inverse of [`SchedulerMessage::job_with`]. A frame carries only what
    /// running a task needs, so the columns an executor never reads — timing,
    /// dedup key, archived result — take their defaults rather than being put
    /// on the wire. `status` is [`JobStatus::Running`] because that is what the
    /// job is by the time a frame describing it has been dispatched.
    ///
    /// Those defaults are not purely internal: the Node and Python SDKs build
    /// their handler-visible job objects straight from this `Job`, so a task
    /// reading `created_at`, `scheduled_at`, `priority`, `unique_key` or
    /// `notes` sees zeros and nulls on an attached executor where an in-process
    /// worker would show the stored values. Carrying them would mean widening
    /// the frame for fields no dispatch decision uses, so the difference is
    /// documented rather than papered over — see the `detached` module in each
    /// SDK for the other side of the same trade. `metadata` is the exception:
    /// middleware reads it, and an executor cannot fetch the row, so it rides
    /// the frame.
    pub fn into_dispatch(self, payload: Vec<u8>) -> Option<Dispatch> {
        match self {
            Self::HelloAck { .. } | Self::Cancel { .. } | Self::Shutdown => None,
            Self::Job {
                id,
                task_name,
                retry_count,
                max_retries,
                queue,
                timeout_ms,
                namespace,
                disabled_middleware,
                metadata,
                payload_len: _,
            } => Some(Dispatch {
                job: Job {
                    id,
                    queue,
                    task_name,
                    payload,
                    status: JobStatus::Running,
                    priority: 0,
                    created_at: 0,
                    scheduled_at: 0,
                    started_at: None,
                    completed_at: None,
                    retry_count,
                    max_retries,
                    result: None,
                    error: None,
                    timeout_ms,
                    unique_key: None,
                    progress: None,
                    metadata,
                    notes: None,
                    cancel_requested: false,
                    expires_at: None,
                    result_ttl_ms: None,
                    namespace,
                    has_deps: false,
                    // Not on the wire: the frame carries only what an executor
                    // runs with, and debouncing is settled before dispatch.
                    debounce_key: None,
                },
                disabled_middleware,
            }),
        }
    }
}

impl ExecutorMessage {
    /// Start a [`ExecutorMessage::Hello`] frame.
    ///
    /// The variant is `#[non_exhaustive]`, so this is how an executor outside
    /// this crate writes its handshake. Required here is what a correct
    /// executor cannot omit — who it is, and what it can run. The rest has a
    /// right answer: [`PROTOCOL_VERSION`] and no token, overridable with
    /// [`HelloBuilder::protocol_version`] and [`HelloBuilder::token`].
    ///
    /// ```
    /// # use flexiq_core::worker::protocol::ExecutorMessage;
    /// let hello = ExecutorMessage::hello("exec-1", "python", "1.0.0", vec!["resize".into()], 4)
    ///     .build();
    /// ```
    pub fn hello(
        executor_id: impl Into<String>,
        sdk: impl Into<String>,
        version: impl Into<String>,
        tasks: Vec<String>,
        slots: u32,
    ) -> HelloBuilder {
        HelloBuilder {
            executor_id: executor_id.into(),
            sdk: sdk.into(),
            version: version.into(),
            tasks,
            slots,
            protocol_version: PROTOCOL_VERSION,
            token: None,
        }
    }

    /// Build the result frame and payload for a finished job.
    ///
    /// The inverse of [`ExecutorMessage::into_job_result`]. A success carries
    /// its serialized result as the frame's blob, so the payload is returned
    /// alongside the header rather than inside it.
    pub fn from_job_result(result: JobResult) -> (Self, Vec<u8>) {
        match result {
            JobResult::Success {
                job_id,
                result,
                task_name,
                wall_time_ns,
            } => {
                // `Some(vec![])` is an empty result and `None` is no result, so
                // the length is read off the `Option` itself — deriving it from
                // the flattened payload would collapse the two.
                let result_len = result.as_ref().map(Vec::len);
                let payload = result.unwrap_or_default();
                (
                    Self::Success {
                        job_id,
                        result_len,
                        task_name,
                        wall_time_ns,
                    },
                    payload,
                )
            }
            JobResult::Failure {
                job_id,
                error,
                retry_count,
                max_retries,
                task_name,
                wall_time_ns,
                should_retry,
                timed_out,
            } => (
                Self::Failure {
                    job_id,
                    error,
                    retry_count,
                    max_retries,
                    task_name,
                    wall_time_ns,
                    should_retry,
                    timed_out,
                },
                Vec::new(),
            ),
            JobResult::Cancelled {
                job_id,
                task_name,
                wall_time_ns,
            } => (
                Self::Cancelled {
                    job_id,
                    task_name,
                    wall_time_ns,
                },
                Vec::new(),
            ),
            JobResult::Slept {
                job_id,
                task_name,
                wake_at,
                wall_time_ns,
            } => (
                Self::Slept {
                    job_id,
                    task_name,
                    wake_at,
                    wall_time_ns,
                },
                Vec::new(),
            ),
        }
    }

    /// Build a `task_log` frame and its payload.
    ///
    /// `extra` is pre-encoded JSON, exactly as
    /// [`crate::storage::Storage::write_task_log`] takes it, and rides as the
    /// frame's blob rather than in the header.
    pub fn task_log(
        job_id: impl Into<String>,
        task_name: impl Into<String>,
        level: impl Into<String>,
        message: impl Into<String>,
        extra: Option<&str>,
    ) -> (Self, Vec<u8>) {
        let payload = extra.map(|extra| extra.as_bytes().to_vec());
        (
            Self::TaskLog {
                job_id: job_id.into(),
                task_name: task_name.into(),
                level: level.into(),
                message: message.into(),
                // Read off the `Option`, not the flattened payload: an empty
                // `extra` and no `extra` are different, exactly as they are for
                // a success result.
                extra_len: payload.as_ref().map(Vec::len),
            },
            payload.unwrap_or_default(),
        )
    }

    /// Convert a result frame plus its payload into a [`JobResult`]. `None` for
    /// non-result frames (`hello`, `heartbeat`, and the side-channel frames).
    ///
    /// Side-channel frames answering `None` is what keeps them out of the
    /// exactly-once accounting: a progress report must never be mistaken for
    /// the job's one outcome.
    pub fn into_job_result(self, payload: Vec<u8>) -> Option<JobResult> {
        match self {
            Self::Hello { .. }
            | Self::Heartbeat { .. }
            | Self::Progress { .. }
            | Self::TaskLog { .. } => None,
            Self::Success {
                job_id,
                result_len,
                task_name,
                wall_time_ns,
            } => Some(JobResult::Success {
                job_id,
                result: result_len.map(|_| payload),
                task_name,
                wall_time_ns,
            }),
            Self::Failure {
                job_id,
                error,
                retry_count,
                max_retries,
                task_name,
                wall_time_ns,
                should_retry,
                timed_out,
            } => Some(JobResult::Failure {
                job_id,
                error,
                retry_count,
                max_retries,
                task_name,
                wall_time_ns,
                should_retry,
                timed_out,
            }),
            Self::Cancelled {
                job_id,
                task_name,
                wall_time_ns,
            } => Some(JobResult::Cancelled {
                job_id,
                task_name,
                wall_time_ns,
            }),
            Self::Slept {
                job_id,
                task_name,
                wake_at,
                wall_time_ns,
            } => Some(JobResult::Slept {
                job_id,
                task_name,
                wake_at,
                wall_time_ns,
            }),
        }
    }
}

/// Writes frames onto any byte sink — a child's stdin, a socket, a test buffer.
pub struct FrameWriter<W: Write> {
    inner: BufWriter<W>,
}

impl<W: Write> FrameWriter<W> {
    /// Wrap a sink. Each frame is flushed as written, so a peer never waits on
    /// a partially buffered header.
    pub fn new(sink: W) -> Self {
        Self {
            inner: BufWriter::new(sink),
        }
    }

    /// Write one frame and its payload. A length disagreement would desync the
    /// reader, so it is rejected before anything reaches the wire.
    pub fn write<F: Frame>(&mut self, frame: &F, payload: &[u8]) -> Result<(), ProtocolError> {
        let declared = frame.payload_len();
        if declared != payload.len() {
            return Err(ProtocolError::PayloadLengthMismatch {
                declared,
                actual: payload.len(),
            });
        }
        let header = serde_json::to_vec(frame)?;
        self.inner.write_all(&header)?;
        self.inner.write_all(b"\n")?;
        self.inner.write_all(payload)?;
        self.inner.flush()?;
        Ok(())
    }

    /// Write a payload-free frame.
    pub fn write_header<F: Frame>(&mut self, frame: &F) -> Result<(), ProtocolError> {
        self.write(frame, &[])
    }

    /// Dispatch a job, sending its payload as the frame's blob.
    pub fn write_job(&mut self, job: &Job) -> Result<(), ProtocolError> {
        self.write_job_with(job, Vec::new())
    }

    /// Dispatch a job along with the middleware disabled for its task.
    pub fn write_job_with(
        &mut self,
        job: &Job,
        disabled_middleware: Vec<String>,
    ) -> Result<(), ProtocolError> {
        self.write(
            &SchedulerMessage::job_with(job, disabled_middleware),
            &job.payload,
        )
    }

    /// Ask the peer to cancel a running job.
    pub fn write_cancel(&mut self, job_id: &str) -> Result<(), ProtocolError> {
        self.write_header(&SchedulerMessage::Cancel {
            job_id: job_id.to_string(),
        })
    }

    /// Ask the peer to drain and exit.
    pub fn write_shutdown(&mut self) -> Result<(), ProtocolError> {
        self.write_header(&SchedulerMessage::Shutdown)
    }
}

/// Reads frames from any buffered byte source.
pub struct FrameReader<R: BufRead> {
    inner: R,
}

impl<R: BufRead> FrameReader<R> {
    /// Wrap a buffered source.
    pub fn new(source: R) -> Self {
        Self { inner: source }
    }

    /// Read one frame and its payload. Blocks until a whole frame arrives.
    pub fn read<F: Frame>(&mut self) -> Result<(F, Vec<u8>), ProtocolError> {
        let header = self.read_header_line()?;
        let frame: F = serde_json::from_slice(&header)?;
        let payload = self.read_payload(frame.payload_len())?;
        Ok((frame, payload))
    }

    /// Read one frame, skipping past a type this build does not know.
    ///
    /// Scheduler and executor are released independently — that decoupling is
    /// the whole reason an attach exists — so a frame type added on the far side
    /// must degrade to "ignored". [`FrameReader::read`] would surface it as a
    /// parse error, and the only thing a reader loop can do with one is drop the
    /// connection, abandoning every job in flight to the reaper.
    ///
    /// Skipping is possible because a header declares its own payload length,
    /// which the preamble reads without knowing the frame's shape. A *known*
    /// type that fails to parse is still an error: that is a disagreement about
    /// a frame we both claim to speak, not a newer peer.
    pub fn read_or_skip<F: Frame>(&mut self) -> Result<Incoming<F>, ProtocolError> {
        let header = self.read_header_line()?;
        // Typed first, so a known frame pays nothing for the tolerance: the
        // preamble is parsed only once this build has failed to name the frame.
        let typed = match serde_json::from_slice::<F>(&header) {
            Ok(frame) => {
                let payload = self.read_payload(frame.payload_len())?;
                return Ok(Incoming::Known(frame, payload));
            }
            Err(error) => error,
        };

        match serde_json::from_slice::<FramePreamble>(&header) {
            // Not even a type tag: report the typed failure, which names what
            // was expected. Nothing here says how far to skip, either.
            Err(_) => Err(typed.into()),
            Ok(preamble) if F::is_known_type(&preamble.frame_type) => Err(typed.into()),
            Ok(preamble) => {
                self.read_payload(preamble.payload_len.unwrap_or(0))?;
                Ok(Incoming::Unknown {
                    frame_type: preamble.frame_type,
                })
            }
        }
    }

    /// Read exactly `len` payload bytes, capped so a corrupt length field cannot
    /// allocate without bound.
    fn read_payload(&mut self, len: usize) -> Result<Vec<u8>, ProtocolError> {
        if len > MAX_PAYLOAD_BYTES {
            return Err(ProtocolError::PayloadTooLarge { len });
        }
        let mut payload = vec![0u8; len];
        if len > 0 {
            self.inner.read_exact(&mut payload)?;
        }
        Ok(payload)
    }

    /// Read through the header's newline, capped so a peer that never sends one
    /// cannot grow the buffer without bound.
    fn read_header_line(&mut self) -> Result<Vec<u8>, ProtocolError> {
        let mut header = Vec::new();
        let read = (&mut self.inner)
            .take(MAX_HEADER_BYTES)
            .read_until(b'\n', &mut header)?;
        if read == 0 {
            return Err(ProtocolError::Eof);
        }
        if !header.ends_with(b"\n") {
            // Only a header that actually reached the cap is oversized. Fewer
            // bytes with no newline means the peer closed mid-header — a
            // truncated frame, which the `Eof` contract says surfaces as I/O.
            if read as u64 == MAX_HEADER_BYTES {
                return Err(ProtocolError::HeaderTooLarge);
            }
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "peer closed mid-header",
            )
            .into());
        }
        Ok(header)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::job::JobStatus;

    fn round_trip<F: Frame>(frame: &F, payload: &[u8]) -> (F, Vec<u8>) {
        let mut buf = Vec::new();
        FrameWriter::new(&mut buf)
            .write(frame, payload)
            .expect("write");
        FrameReader::new(buf.as_slice()).read::<F>().expect("read")
    }

    fn sample_job(payload: &[u8]) -> Job {
        Job {
            id: "job-1".into(),
            queue: "default".into(),
            task_name: "resize".into(),
            payload: payload.to_vec(),
            status: JobStatus::Running,
            priority: 0,
            retry_count: 1,
            max_retries: 3,
            scheduled_at: 0,
            created_at: 0,
            started_at: None,
            completed_at: None,
            error: None,
            result: None,
            timeout_ms: 30_000,
            unique_key: None,
            progress: None,
            metadata: None,
            notes: None,
            cancel_requested: false,
            expires_at: None,
            result_ttl_ms: None,
            namespace: Some("tenant-a".into()),
            has_deps: false,
            debounce_key: None,
        }
    }

    #[test]
    fn job_frame_carries_payload_bytes_verbatim() {
        // The CBOR envelope for f(1, "a") — the BINDING_CONTRACT test vector.
        let payload = [0x02, 0x82, 0x82, 0x01, 0x61, 0x61, 0xa0];
        let job = sample_job(&payload);

        let mut buf = Vec::new();
        FrameWriter::new(&mut buf).write_job(&job).expect("write");
        assert!(buf.ends_with(&payload), "payload must be written raw");

        let (frame, read_payload) = FrameReader::new(buf.as_slice())
            .read::<SchedulerMessage>()
            .expect("read");
        assert_eq!(read_payload, payload);
        match frame {
            SchedulerMessage::Job {
                id,
                task_name,
                payload_len,
                retry_count,
                max_retries,
                queue,
                timeout_ms,
                namespace,
                disabled_middleware,
                metadata,
            } => {
                assert_eq!(id, "job-1");
                assert_eq!(task_name, "resize");
                assert_eq!(payload_len, payload.len());
                assert_eq!(retry_count, 1);
                assert_eq!(max_retries, 3);
                assert_eq!(queue, "default");
                assert_eq!(timeout_ms, 30_000);
                assert_eq!(namespace.as_deref(), Some("tenant-a"));
                assert!(disabled_middleware.is_empty());
                assert_eq!(metadata, None);
            }
            other => panic!("expected a job frame, got {other:?}"),
        }
    }

    #[test]
    fn a_job_survives_a_round_trip_through_a_frame() {
        let payload = [0x02, 0x82, 0x82, 0x01, 0x61, 0x61, 0xa0];
        let mut original = sample_job(&payload);
        original.metadata = Some(r#"{"trace_id":"abc"}"#.into());

        let (frame, read_payload) = round_trip(&SchedulerMessage::from(&original), &payload);
        let rebuilt = frame.into_dispatch(read_payload).expect("a job frame").job;

        // Everything a task body can observe. The columns left out of the frame
        // are storage bookkeeping the executor never reads.
        assert_eq!(rebuilt.id, original.id);
        assert_eq!(rebuilt.queue, original.queue);
        assert_eq!(rebuilt.task_name, original.task_name);
        assert_eq!(rebuilt.payload, original.payload);
        assert_eq!(rebuilt.retry_count, original.retry_count);
        assert_eq!(rebuilt.max_retries, original.max_retries);
        assert_eq!(rebuilt.timeout_ms, original.timeout_ms);
        assert_eq!(rebuilt.namespace, original.namespace);
        assert_eq!(rebuilt.status, JobStatus::Running);
        // Middleware reads it, and an executor cannot fetch the row itself.
        assert_eq!(rebuilt.metadata, original.metadata);
    }

    #[test]
    fn a_dispatch_carries_the_toggles_the_scheduler_resolved() {
        let job = sample_job(b"x");
        let disabled = vec!["tracing".to_string(), "app.mw.Audit".to_string()];

        let mut buf = Vec::new();
        FrameWriter::new(&mut buf)
            .write_job_with(&job, disabled.clone())
            .expect("write");
        let (frame, payload) = FrameReader::new(buf.as_slice())
            .read::<SchedulerMessage>()
            .expect("read");

        let dispatch = frame.into_dispatch(payload).expect("a job frame");
        assert_eq!(dispatch.disabled_middleware, disabled);
        assert_eq!(dispatch.job.task_name, "resize");
    }

    #[test]
    fn control_frames_describe_no_job() {
        assert!(SchedulerMessage::Shutdown.into_dispatch(vec![]).is_none());
        assert!(SchedulerMessage::Cancel {
            job_id: "job-1".into()
        }
        .into_dispatch(vec![])
        .is_none());
        assert!(SchedulerMessage::HelloAck {
            scheduler_id: "s".into(),
            protocol_version: PROTOCOL_VERSION,
            capabilities: Vec::new(),
        }
        .into_dispatch(vec![])
        .is_none());
    }

    #[test]
    fn a_result_survives_a_round_trip_through_a_frame() {
        for (label, original) in [
            (
                "a result",
                JobResult::Success {
                    job_id: "job-1".into(),
                    result: Some(b"out".to_vec()),
                    task_name: "resize".into(),
                    wall_time_ns: 42,
                },
            ),
            (
                "an empty result",
                JobResult::Success {
                    job_id: "job-1".into(),
                    result: Some(Vec::new()),
                    task_name: "resize".into(),
                    wall_time_ns: 42,
                },
            ),
            (
                "no result",
                JobResult::Success {
                    job_id: "job-1".into(),
                    result: None,
                    task_name: "resize".into(),
                    wall_time_ns: 42,
                },
            ),
        ] {
            let JobResult::Success { result: before, .. } = &original else {
                unreachable!("the table holds successes only")
            };
            let expected = before.clone();

            let (frame, payload) = ExecutorMessage::from_job_result(original);
            let (frame, payload) = round_trip(&frame, &payload);

            match frame.into_job_result(payload) {
                Some(JobResult::Success { result, .. }) => {
                    assert_eq!(result, expected, "{label} must survive the round trip")
                }
                _ => panic!("expected a success for {label}"),
            }
        }
    }

    #[test]
    fn a_failure_round_trips_with_its_verdict_intact() {
        let (frame, payload) = ExecutorMessage::from_job_result(JobResult::Failure {
            job_id: "job-1".into(),
            error: "boom".into(),
            retry_count: 2,
            max_retries: 5,
            task_name: "resize".into(),
            wall_time_ns: 7,
            should_retry: false,
            timed_out: true,
        });
        let (frame, payload) = round_trip(&frame, &payload);

        match frame.into_job_result(payload) {
            Some(JobResult::Failure {
                error,
                retry_count,
                max_retries,
                should_retry,
                timed_out,
                ..
            }) => {
                assert_eq!(error, "boom");
                assert_eq!(retry_count, 2);
                assert_eq!(max_retries, 5);
                assert!(!should_retry, "only the executor can judge retryability");
                assert!(timed_out);
            }
            _ => panic!("expected a failure"),
        }
    }

    #[test]
    fn a_cancellation_round_trips() {
        let (frame, payload) = ExecutorMessage::from_job_result(JobResult::Cancelled {
            job_id: "job-1".into(),
            task_name: "resize".into(),
            wall_time_ns: 9,
        });
        let (frame, payload) = round_trip(&frame, &payload);
        assert!(matches!(
            frame.into_job_result(payload),
            Some(JobResult::Cancelled { job_id, .. }) if job_id == "job-1"
        ));
    }

    #[test]
    fn a_sleep_round_trips_with_its_deadline() {
        // The deadline is the one thing this frame carries that a cancel does
        // not: an attached executor learns it from storage's answer, not from
        // the duration it asked for.
        let (frame, payload) = ExecutorMessage::from_job_result(JobResult::Slept {
            job_id: "job-1".into(),
            task_name: "resize".into(),
            wake_at: 1_760_000_000_000,
            wall_time_ns: 9,
        });
        let (frame, payload) = round_trip(&frame, &payload);
        match frame.into_job_result(payload) {
            Some(JobResult::Slept {
                job_id,
                task_name,
                wake_at,
                wall_time_ns,
            }) => {
                assert_eq!(job_id, "job-1");
                assert_eq!(task_name, "resize");
                assert_eq!(wake_at, 1_760_000_000_000);
                assert_eq!(wall_time_ns, 9);
            }
            _ => panic!("expected a sleep"),
        }
    }

    #[test]
    fn handshake_frames_round_trip() {
        let (hello, payload) = round_trip(
            &ExecutorMessage::Hello {
                executor_id: "exec-1".into(),
                sdk: "python".into(),
                version: "0.21.0".into(),
                tasks: vec!["resize".into(), "thumbnail".into()],
                slots: 4,
                protocol_version: PROTOCOL_VERSION,
                token: None,
            },
            &[],
        );
        assert!(payload.is_empty());
        match hello {
            ExecutorMessage::Hello {
                tasks,
                slots,
                protocol_version,
                ..
            } => {
                assert_eq!(tasks, ["resize", "thumbnail"]);
                assert_eq!(slots, 4);
                assert_eq!(protocol_version, PROTOCOL_VERSION);
            }
            other => panic!("expected hello, got {other:?}"),
        }

        let (ack, _) = round_trip(
            &SchedulerMessage::HelloAck {
                scheduler_id: "scheduler-1".into(),
                protocol_version: PROTOCOL_VERSION,
                capabilities: vec![CAP_SIDE_CHANNEL.to_string()],
            },
            &[],
        );
        match ack {
            SchedulerMessage::HelloAck { capabilities, .. } => {
                assert_eq!(capabilities, [CAP_SIDE_CHANNEL])
            }
            other => panic!("expected hello_ack, got {other:?}"),
        }
    }

    #[test]
    fn frames_from_a_peer_that_predates_the_side_channel_still_parse() {
        // The exact bytes a scheduler and an executor built before these fields
        // existed put on the wire. Optional additions are what let one side
        // upgrade without the other, so this is the compatibility contract.
        let legacy_ack = br#"{"type":"hello_ack","scheduler_id":"s","protocol_version":1}
"#;
        match FrameReader::new(&legacy_ack[..])
            .read::<SchedulerMessage>()
            .expect("a legacy hello_ack must still parse")
            .0
        {
            SchedulerMessage::HelloAck { capabilities, .. } => assert!(
                capabilities.is_empty(),
                "an ack with no list advertises nothing, so no new frame is ever sent"
            ),
            other => panic!("expected hello_ack, got {other:?}"),
        }

        let legacy_job = br#"{"type":"job","id":"j","task_name":"t","payload_len":0,"retry_count":0,"max_retries":3,"queue":"default","timeout_ms":0,"namespace":null}
"#;
        let (frame, payload) = FrameReader::new(&legacy_job[..])
            .read::<SchedulerMessage>()
            .expect("a legacy job frame must still parse");
        let dispatch = frame.into_dispatch(payload).expect("a job frame");
        assert!(dispatch.disabled_middleware.is_empty());
        assert_eq!(dispatch.job.metadata, None);
    }

    #[test]
    fn side_channel_frames_round_trip_and_are_never_results() {
        let (progress, payload) = round_trip(
            &ExecutorMessage::Progress {
                job_id: "job-1".into(),
                progress: 42,
            },
            &[],
        );
        assert!(matches!(
            &progress,
            ExecutorMessage::Progress { job_id, progress } if job_id == "job-1" && *progress == 42
        ));
        assert!(
            progress.into_job_result(payload).is_none(),
            "progress must never consume a job's one outcome"
        );

        // A published partial: level `result`, no message, value in `extra`.
        let (frame, payload) =
            ExecutorMessage::task_log("job-1", "resize", "result", "", Some(r#"{"step":3}"#));
        let (frame, payload) = round_trip(&frame, &payload);
        match &frame {
            ExecutorMessage::TaskLog {
                job_id,
                task_name,
                level,
                message,
                extra_len,
            } => {
                assert_eq!(job_id, "job-1");
                assert_eq!(task_name, "resize");
                assert_eq!(level, "result");
                assert!(message.is_empty());
                assert_eq!(*extra_len, Some(payload.len()));
                assert_eq!(payload, br#"{"step":3}"#);
            }
            other => panic!("expected a task_log frame, got {other:?}"),
        }
        assert!(frame.into_job_result(payload).is_none());
    }

    #[test]
    fn a_log_without_extra_is_distinct_from_one_with_empty_extra() {
        for (label, extra, expected) in [
            ("no extra", None, None),
            ("empty extra", Some(""), Some(0)),
            ("some extra", Some("{}"), Some(2)),
        ] {
            let (frame, payload) = ExecutorMessage::task_log("j", "t", "info", "hi", extra);
            let (frame, _) = round_trip(&frame, &payload);
            match frame {
                ExecutorMessage::TaskLog { extra_len, .. } => {
                    assert_eq!(extra_len, expected, "{label} must survive the round trip")
                }
                other => panic!("expected a task_log frame for {label}, got {other:?}"),
            }
        }
    }

    fn hello_with_token(token: Option<&str>) -> ExecutorMessage {
        ExecutorMessage::Hello {
            executor_id: "exec-1".into(),
            sdk: "test".into(),
            version: "0.0.0".into(),
            tasks: vec!["resize".into()],
            slots: 1,
            protocol_version: PROTOCOL_VERSION,
            token: token.map(Secret::new),
        }
    }

    #[test]
    fn a_hello_without_a_token_stays_off_the_wire_and_parses_back() {
        let mut buf = Vec::new();
        FrameWriter::new(&mut buf)
            .write_header(&hello_with_token(None))
            .expect("write");
        let header = String::from_utf8(buf.clone()).expect("utf-8 header");
        assert!(
            !header.contains("token"),
            "an absent token must not appear as a null field: {header}"
        );

        // The same shape an executor built before this field existed sends.
        let legacy = br#"{"type":"hello","executor_id":"exec-1","sdk":"test","version":"0.0.0","tasks":[],"slots":1,"protocol_version":1}
"#;
        assert!(matches!(
            FrameReader::new(&legacy[..])
                .read::<ExecutorMessage>()
                .expect("legacy hello must still parse")
                .0,
            ExecutorMessage::Hello { token: None, .. }
        ));
    }

    #[test]
    fn a_hello_token_round_trips_but_never_prints() {
        let (frame, _) = round_trip(&hello_with_token(Some("s3cret-value")), &[]);
        match &frame {
            ExecutorMessage::Hello { token, .. } => {
                let presented = token.as_ref().expect("the token must survive the wire");
                assert!(presented.matches(&Secret::new("s3cret-value")));
            }
            other => panic!("expected hello, got {other:?}"),
        }
        assert!(
            !format!("{frame:?}").contains("s3cret"),
            "a debug dump of a frame must not carry token material"
        );
    }

    #[test]
    fn control_frames_round_trip() {
        let mut buf = Vec::new();
        let mut writer = FrameWriter::new(&mut buf);
        writer.write_cancel("job-1").expect("cancel");
        writer.write_shutdown().expect("shutdown");
        drop(writer);

        let mut reader = FrameReader::new(buf.as_slice());
        assert!(matches!(
            reader.read::<SchedulerMessage>().expect("read cancel").0,
            SchedulerMessage::Cancel { job_id } if job_id == "job-1"
        ));
        assert!(matches!(
            reader.read::<SchedulerMessage>().expect("read shutdown").0,
            SchedulerMessage::Shutdown
        ));
        assert!(matches!(
            reader.read::<SchedulerMessage>(),
            Err(ProtocolError::Eof)
        ));
    }

    #[test]
    fn empty_result_is_distinct_from_no_result() {
        let empty = ExecutorMessage::Success {
            job_id: "job-1".into(),
            result_len: Some(0),
            task_name: "t".into(),
            wall_time_ns: 5,
        };
        let (frame, payload) = round_trip(&empty, &[]);
        match frame.into_job_result(payload) {
            Some(JobResult::Success { result, .. }) => assert_eq!(result, Some(vec![])),
            _ => panic!("expected a success result"),
        }

        let none = ExecutorMessage::Success {
            job_id: "job-1".into(),
            result_len: None,
            task_name: "t".into(),
            wall_time_ns: 5,
        };
        let (frame, payload) = round_trip(&none, &[]);
        match frame.into_job_result(payload) {
            Some(JobResult::Success { result, .. }) => assert_eq!(result, None),
            _ => panic!("expected a success result"),
        }
    }

    #[test]
    fn result_frames_map_onto_job_results() {
        let (failure, payload) = round_trip(
            &ExecutorMessage::Failure {
                job_id: "job-1".into(),
                error: r#"{"errtype":"ValueError","message":"boom","traceback":[]}"#.into(),
                retry_count: 2,
                max_retries: 3,
                task_name: "t".into(),
                wall_time_ns: 42,
                should_retry: false,
                timed_out: true,
            },
            &[],
        );
        match failure.into_job_result(payload) {
            Some(JobResult::Failure {
                should_retry,
                timed_out,
                retry_count,
                ..
            }) => {
                assert!(!should_retry);
                assert!(timed_out);
                assert_eq!(retry_count, 2);
            }
            _ => panic!("expected a failure result"),
        }

        let (cancelled, payload) = round_trip(
            &ExecutorMessage::Cancelled {
                job_id: "job-1".into(),
                task_name: "t".into(),
                wall_time_ns: 7,
            },
            &[],
        );
        assert!(matches!(
            cancelled.into_job_result(payload),
            Some(JobResult::Cancelled { .. })
        ));
    }

    #[test]
    fn non_result_frames_produce_no_job_result() {
        let heartbeat = ExecutorMessage::Heartbeat { free_slots: 3 };
        assert!(heartbeat.into_job_result(vec![]).is_none());
    }

    #[test]
    fn declared_length_must_match_the_payload() {
        let job = sample_job(b"1234");
        let frame = SchedulerMessage::from(&job);
        let mut buf = Vec::new();
        let err = FrameWriter::new(&mut buf)
            .write(&frame, b"12")
            .expect_err("length mismatch must be rejected");
        assert!(matches!(
            err,
            ProtocolError::PayloadLengthMismatch {
                declared: 4,
                actual: 2
            }
        ));
        assert!(buf.is_empty(), "nothing may reach the wire on a mismatch");
    }

    #[test]
    fn oversized_header_is_rejected() {
        let mut buf = vec![b'{'; MAX_HEADER_BYTES as usize + 1];
        buf.push(b'\n');
        assert!(matches!(
            FrameReader::new(buf.as_slice()).read::<SchedulerMessage>(),
            Err(ProtocolError::HeaderTooLarge)
        ));
    }

    #[test]
    fn oversized_payload_is_rejected_before_allocating() {
        let header = format!(
            r#"{{"type":"job","id":"j","task_name":"t","payload_len":{},"retry_count":0,"max_retries":0,"queue":"q","timeout_ms":0,"namespace":null,"disabled_middleware":[],"metadata":null}}"#,
            MAX_PAYLOAD_BYTES + 1
        );
        let mut buf = header.into_bytes();
        buf.push(b'\n');
        assert!(matches!(
            FrameReader::new(buf.as_slice()).read::<SchedulerMessage>(),
            Err(ProtocolError::PayloadTooLarge { .. })
        ));
    }

    #[test]
    fn truncated_payload_is_an_error_not_a_clean_eof() {
        let job = sample_job(b"1234");
        let mut buf = Vec::new();
        FrameWriter::new(&mut buf).write_job(&job).expect("write");
        buf.truncate(buf.len() - 2);

        let err = FrameReader::new(buf.as_slice())
            .read::<SchedulerMessage>()
            .expect_err("truncated frame must not read");
        assert!(matches!(err, ProtocolError::Io(_)), "got {err:?}");
    }

    #[test]
    fn truncated_header_is_an_io_error_not_an_oversized_header() {
        // A short header with no newline is a mid-frame disconnect, not a peer
        // that blew the size cap — the two must not report the same way.
        let buf = br#"{"type":"job","id":"j""#;
        let err = FrameReader::new(&buf[..])
            .read::<SchedulerMessage>()
            .expect_err("truncated header must not read");
        assert!(matches!(err, ProtocolError::Io(_)), "got {err:?}");
    }

    #[test]
    fn malformed_header_is_reported_as_json_error() {
        let buf = b"not json\n";
        assert!(matches!(
            FrameReader::new(&buf[..]).read::<ExecutorMessage>(),
            Err(ProtocolError::Json(_))
        ));
    }

    /// The `type` tag a frame actually serializes to.
    fn wire_type<F: Frame>(frame: &F) -> String {
        let value: serde_json::Value = serde_json::to_value(frame).expect("serialize");
        value["type"].as_str().expect("a type tag").to_string()
    }

    #[test]
    fn every_frame_reports_its_own_wire_type_as_known() {
        // `is_known_type` is what separates "a peer newer than us" from "a frame
        // we should have been able to read". A tag missing from it would make a
        // real desync look like forward compatibility and skip a job's result.
        for frame in [
            SchedulerMessage::HelloAck {
                scheduler_id: "s".into(),
                protocol_version: PROTOCOL_VERSION,
                capabilities: vec![],
            },
            SchedulerMessage::from(&sample_job(b"")),
            SchedulerMessage::Cancel {
                job_id: "job-1".into(),
            },
            SchedulerMessage::Shutdown,
        ] {
            let tag = wire_type(&frame);
            assert!(
                SchedulerMessage::is_known_type(&tag),
                "'{tag}' serializes but is not listed as known"
            );
        }

        for frame in [
            hello_with_token(None),
            ExecutorMessage::Heartbeat { free_slots: 1 },
            ExecutorMessage::Progress {
                job_id: "job-1".into(),
                progress: 10,
            },
            ExecutorMessage::task_log("job-1", "t", "info", "m", None).0,
            ExecutorMessage::Success {
                job_id: "job-1".into(),
                result_len: None,
                task_name: "t".into(),
                wall_time_ns: 1,
            },
            ExecutorMessage::Failure {
                job_id: "job-1".into(),
                error: "e".into(),
                retry_count: 0,
                max_retries: 3,
                task_name: "t".into(),
                wall_time_ns: 1,
                should_retry: true,
                timed_out: false,
            },
            ExecutorMessage::Cancelled {
                job_id: "job-1".into(),
                task_name: "t".into(),
                wall_time_ns: 1,
            },
        ] {
            let tag = wire_type(&frame);
            assert!(
                ExecutorMessage::is_known_type(&tag),
                "'{tag}' serializes but is not listed as known"
            );
        }
    }

    #[test]
    fn an_unknown_frame_is_skipped_and_the_next_one_still_reads() {
        // The forward-compatibility contract: a peer released after this build
        // can send frames it does not know, and the stream stays aligned.
        let mut buf = Vec::new();
        buf.extend_from_slice(b"{\"type\":\"telemetry\",\"payload_len\":5}\nabcde");
        FrameWriter::new(&mut buf)
            .write_header(&ExecutorMessage::Heartbeat { free_slots: 2 })
            .expect("write");

        let mut reader = FrameReader::new(buf.as_slice());
        match reader.read_or_skip::<ExecutorMessage>().expect("skip") {
            Incoming::Unknown { frame_type } => assert_eq!(frame_type, "telemetry"),
            Incoming::Known(frame, _) => panic!("unexpectedly parsed {frame:?}"),
        }
        assert!(matches!(
            reader.read_or_skip::<ExecutorMessage>().expect("read"),
            Incoming::Known(ExecutorMessage::Heartbeat { free_slots: 2 }, _)
        ));
    }

    #[test]
    fn an_unknown_frame_declaring_a_legacy_length_field_is_still_skipped() {
        // `success` and `task_log` predate the `payload_len` rule, so a later
        // frame modelled on either would name its blob `result_len`/`extra_len`.
        // Honouring both is what keeps that copy-paste from desyncing the wire.
        let mut buf = Vec::new();
        buf.extend_from_slice(b"{\"type\":\"partial\",\"extra_len\":2}\nhi");
        FrameWriter::new(&mut buf)
            .write_header(&ExecutorMessage::Heartbeat { free_slots: 1 })
            .expect("write");

        let mut reader = FrameReader::new(buf.as_slice());
        assert!(matches!(
            reader.read_or_skip::<ExecutorMessage>().expect("skip"),
            Incoming::Unknown { .. }
        ));
        assert!(matches!(
            reader.read_or_skip::<ExecutorMessage>().expect("read"),
            Incoming::Known(ExecutorMessage::Heartbeat { free_slots: 1 }, _)
        ));
    }

    #[test]
    fn a_known_frame_type_that_will_not_parse_is_still_an_error() {
        // Skipping this would silently drop a dispatch. Only an *unrecognised*
        // type is forward compatibility; a broken `job` is a disagreement.
        let buf = b"{\"type\":\"job\",\"id\":\"job-1\"}\n";
        assert!(matches!(
            FrameReader::new(&buf[..]).read_or_skip::<SchedulerMessage>(),
            Err(ProtocolError::Json(_))
        ));
    }

    #[test]
    fn a_header_without_a_type_tag_is_an_error_rather_than_a_skip() {
        // Nothing here says how far to skip, so pretending otherwise would
        // desync the stream on the very next read.
        let buf = b"{\"payload_len\":4}\n";
        assert!(matches!(
            FrameReader::new(&buf[..]).read_or_skip::<ExecutorMessage>(),
            Err(ProtocolError::Json(_))
        ));
    }
}
