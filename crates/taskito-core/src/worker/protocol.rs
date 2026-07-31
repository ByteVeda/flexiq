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

use std::io::{BufRead, BufWriter, Read, Write};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use super::auth::Secret;
use crate::job::Job;
use crate::scheduler::JobResult;

/// Frame format version. Both sides announce it in the handshake; a mismatch
/// is rejected rather than silently downgraded.
pub const PROTOCOL_VERSION: u32 = 1;

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
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SchedulerMessage {
    /// Answer to [`ExecutorMessage::Hello`], completing the handshake.
    HelloAck {
        /// Identity of the scheduler that accepted the attach.
        scheduler_id: String,
        /// Version the scheduler speaks, so a rejected peer can log both.
        protocol_version: u32,
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
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ExecutorMessage {
    /// First frame on every connection: who is attaching and what it can run.
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
}

/// A frame header that may declare a trailing binary blob. Implemented by both
/// message enums so the reader and writer stay generic over direction.
pub trait Frame: Serialize + DeserializeOwned {
    /// Bytes of payload that follow this header; zero for frames carrying none.
    fn payload_len(&self) -> usize;
}

impl Frame for SchedulerMessage {
    fn payload_len(&self) -> usize {
        match self {
            Self::Job { payload_len, .. } => *payload_len,
            _ => 0,
        }
    }
}

impl Frame for ExecutorMessage {
    fn payload_len(&self) -> usize {
        match self {
            Self::Success { result_len, .. } => result_len.unwrap_or(0),
            _ => 0,
        }
    }
}

impl From<&Job> for SchedulerMessage {
    fn from(job: &Job) -> Self {
        Self::Job {
            id: job.id.clone(),
            task_name: job.task_name.clone(),
            payload_len: job.payload.len(),
            retry_count: job.retry_count,
            max_retries: job.max_retries,
            queue: job.queue.clone(),
            timeout_ms: job.timeout_ms,
            namespace: job.namespace.clone(),
        }
    }
}

impl ExecutorMessage {
    /// Convert a result frame plus its payload into a [`JobResult`]. `None` for
    /// non-result frames (`hello`, `heartbeat`).
    pub fn into_job_result(self, payload: Vec<u8>) -> Option<JobResult> {
        match self {
            Self::Hello { .. } | Self::Heartbeat { .. } => None,
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
        self.write(&SchedulerMessage::from(job), &job.payload)
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

        let len = frame.payload_len();
        if len > MAX_PAYLOAD_BYTES {
            return Err(ProtocolError::PayloadTooLarge { len });
        }
        let mut payload = vec![0u8; len];
        if len > 0 {
            self.inner.read_exact(&mut payload)?;
        }
        Ok((frame, payload))
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
            } => {
                assert_eq!(id, "job-1");
                assert_eq!(task_name, "resize");
                assert_eq!(payload_len, payload.len());
                assert_eq!(retry_count, 1);
                assert_eq!(max_retries, 3);
                assert_eq!(queue, "default");
                assert_eq!(timeout_ms, 30_000);
                assert_eq!(namespace.as_deref(), Some("tenant-a"));
            }
            other => panic!("expected a job frame, got {other:?}"),
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
            },
            &[],
        );
        assert!(matches!(ack, SchedulerMessage::HelloAck { .. }));
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
            r#"{{"type":"job","id":"j","task_name":"t","payload_len":{},"retry_count":0,"max_retries":0,"queue":"q","timeout_ms":0,"namespace":null}}"#,
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
}
