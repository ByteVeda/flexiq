//! A [`Transport`] for a peer that speaks frames rather than bytes.
//!
//! [`Transport`] is byte-oriented, because the two things that first
//! implemented it were sockets. A door built on a message protocol — gRPC, a
//! queue, anything that hands over a decoded message instead of a stream —
//! has nothing to put behind `split()`.
//!
//! [`FrameTransport`] is that missing half. It is an ordinary `Transport` from
//! [`RemoteDispatcher`](super::remote::RemoteDispatcher)'s side, so the
//! handshake, the reader thread, placement, the lease fence, the side channel
//! and the drain are all the ones that already exist; and on the far side it is
//! a [`FrameEndpoint`] that takes and returns
//! [`ExecutorMessage`]/[`SchedulerMessage`] values. The frame codec sits
//! between them, in this crate, which is what keeps the transport for a wire
//! format out of the core: a gRPC door converts protobuf to these two types and
//! knows nothing else about the protocol.
//!
//! # Why the bytes are encoded at all
//!
//! A frame that arrives already decoded is encoded here and decoded again by
//! the dispatcher's reader. The alternative is a frame-level `Transport` trait,
//! which rewrites `remote.rs`, `executor.rs` and the prefork pool to save a
//! memcpy on a path that is about to cross a network anyway. The seam is worth
//! more than the copy.
//!
//! # The two directions are not symmetric
//!
//! Frames toward the scheduler are unbounded: the dispatcher's reader thread
//! drains them continuously, and whatever is upstream of the endpoint has its
//! own flow control. Frames toward the executor are **bounded**, so a peer that
//! stops reading fails a dispatch write instead of growing a buffer until the
//! process dies — the same failure `Connection::set_write_timeout` exists to
//! produce on a socket.

use std::io::{self, BufReader};
use std::sync::{Arc, Mutex};

use super::protocol::{ExecutorMessage, FrameReader, FrameWriter, ProtocolError, SchedulerMessage};
use super::transport::{
    Channel, ChannelReader, ChannelWriter, Connection, ReadHalf, Transport, WriteHalf,
};

/// Unread dispatch bytes a stalled peer may accumulate before a write blocks.
///
/// Large enough that an ordinary job frame is written in one call, small enough
/// that a fleet of stalled executors cannot exhaust memory. A payload larger
/// than this is still delivered: writes are partial and the endpoint drains as
/// they land, so the cap bounds what is *unread*, never what may be sent.
const DISPATCH_BUFFER_BYTES: usize = 1024 * 1024;

/// Recover a guard from a poisoned lock rather than cascading the panic — the
/// state behind it is a frame codec over a buffer, so reading it is safe.
fn recover<T>(poisoned: std::sync::PoisonError<T>) -> T {
    poisoned.into_inner()
}

/// The dispatcher's side of a frame-speaking connection.
pub struct FrameTransport {
    /// Frames from the executor. The dispatcher reads this.
    to_scheduler: Arc<Channel>,
    /// Frames to the executor. The dispatcher writes this.
    to_executor: Arc<Channel>,
    reader: ChannelReader,
    writer: ChannelWriter,
    peer: String,
    authenticated: bool,
}

impl FrameTransport {
    /// Build both ends of one connection.
    ///
    /// `peer` is the label logs carry and must never contain a credential.
    /// `authenticated` says whether the door already established the peer's
    /// authority — see [`Transport::is_authenticated`], which is the whole
    /// reason a caller would pass `true`.
    pub fn new(peer: impl Into<String>, authenticated: bool) -> (Self, FrameEndpoint) {
        let to_scheduler = Arc::new(Channel::unbounded());
        let to_executor = Arc::new(Channel::bounded(DISPATCH_BUFFER_BYTES));

        // Both writers are opened now rather than at `split`: a reader treats
        // "a writer existed and is gone" as EOF, and opening lazily would race
        // the first frame either side sends.
        let transport = Self {
            reader: to_scheduler.reader(),
            writer: to_executor.writer(),
            to_scheduler: to_scheduler.clone(),
            to_executor: to_executor.clone(),
            peer: peer.into(),
            authenticated,
        };
        let endpoint = FrameEndpoint {
            writer: Mutex::new(FrameWriter::new(to_scheduler.writer())),
            reader: Mutex::new(FrameReader::new(BufReader::new(to_executor.reader()))),
            to_scheduler,
            to_executor,
        };
        (transport, endpoint)
    }
}

impl Transport for FrameTransport {
    fn split(self: Box<Self>) -> io::Result<(ReadHalf, WriteHalf, Connection)> {
        let this = *self;
        let read_control = this.to_scheduler.clone();
        let write_control = this.to_executor.clone();
        let inbound = this.to_scheduler;
        let outbound = this.to_executor;
        Ok((
            Box::new(BufReader::new(this.reader)),
            Box::new(this.writer),
            Connection::new(
                move |timeout| {
                    read_control.set_read_timeout(timeout);
                    Ok(())
                },
                move |timeout| {
                    write_control.set_write_timeout(timeout);
                    Ok(())
                },
                // Both directions, and directly rather than by dropping halves:
                // the endpoint has to learn the connection is over so it can end
                // whatever it is bridging to, and waiting for the last `Arc` to
                // fall is a hang waiting to happen.
                move || {
                    inbound.close_reader();
                    outbound.close_reader();
                },
            ),
        ))
    }

    fn peer(&self) -> String {
        self.peer.clone()
    }

    fn is_authenticated(&self) -> bool {
        self.authenticated
    }
}

/// The far side of a [`FrameTransport`]: frames in, frames out.
///
/// Both methods take `&self` so a door can pump the two directions from
/// separate tasks. Each direction is serialized by its own lock, and only one
/// caller per direction ever makes sense — [`recv`](Self::recv) blocks.
pub struct FrameEndpoint {
    writer: Mutex<FrameWriter<ChannelWriter>>,
    reader: Mutex<FrameReader<BufReader<ChannelReader>>>,
    to_scheduler: Arc<Channel>,
    to_executor: Arc<Channel>,
}

impl FrameEndpoint {
    /// Hand the scheduler a frame the executor sent.
    ///
    /// `payload` must be exactly the length the frame declares; a mismatch is
    /// [`ProtocolError::PayloadLengthMismatch`] rather than a truncated stream.
    pub fn send(&self, frame: &ExecutorMessage, payload: &[u8]) -> Result<(), ProtocolError> {
        self.writer
            .lock()
            .unwrap_or_else(recover)
            .write(frame, payload)
    }

    /// The next frame the scheduler wrote, or `None` once the connection ends.
    ///
    /// Blocks until one arrives. A clean end is `Ok(None)`, which is what a
    /// caller turns into "the stream is over"; anything else is an error worth
    /// reporting, because the dispatcher only ever writes frames it can encode.
    pub fn recv(&self) -> Result<Option<(SchedulerMessage, Vec<u8>)>, ProtocolError> {
        match self
            .reader
            .lock()
            .unwrap_or_else(recover)
            .read::<SchedulerMessage>()
        {
            Ok(frame) => Ok(Some(frame)),
            Err(ProtocolError::Eof) => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// End the connection from this side, in both directions.
    ///
    /// Idempotent. The dispatcher's reader sees EOF and abandons whatever the
    /// executor was running, exactly as it does when a socket goes away.
    pub fn close(&self) {
        self.to_scheduler.close_reader();
        self.to_executor.close_reader();
    }
}

/// A dropped endpoint is a door that went away, and the dispatcher has to find
/// out now rather than when its next dispatch write fails.
impl Drop for FrameEndpoint {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::worker::protocol::PROTOCOL_VERSION;

    fn split(
        transport: FrameTransport,
    ) -> (FrameReader<ReadHalf>, FrameWriter<WriteHalf>, Connection) {
        let (read, write, connection) = Box::new(transport).split().expect("split");
        (FrameReader::new(read), FrameWriter::new(write), connection)
    }

    #[test]
    fn frames_cross_in_both_directions() {
        let (transport, endpoint) = FrameTransport::new("grpc:test", false);
        let (mut reader, mut writer, _connection) = split(transport);

        endpoint
            .send(
                &ExecutorMessage::hello("exec-1", "rust", "0.1", vec!["a".into()], 2).build(),
                &[],
            )
            .expect("send hello");
        let (frame, payload) = reader.read::<ExecutorMessage>().expect("read hello");
        assert!(matches!(frame, ExecutorMessage::Hello { .. }));
        assert!(payload.is_empty());

        writer
            .write(
                &SchedulerMessage::HelloAck {
                    scheduler_id: "sched-1".into(),
                    protocol_version: PROTOCOL_VERSION,
                    capabilities: vec![],
                },
                &[],
            )
            .expect("write ack");
        let (frame, _) = endpoint.recv().expect("recv").expect("a frame");
        assert!(matches!(frame, SchedulerMessage::HelloAck { .. }));
    }

    #[test]
    fn a_payload_survives_the_round_trip() {
        let (transport, endpoint) = FrameTransport::new("grpc:test", false);
        let (mut reader, mut writer, _connection) = split(transport);

        let blob = vec![7u8; 3 * DISPATCH_BUFFER_BYTES];
        // Larger than the dispatch buffer on purpose: the cap bounds unread
        // bytes, and a frame that exceeds it must still be delivered whole.
        let pump = std::thread::spawn(move || {
            writer
                .write(
                    &SchedulerMessage::JobSteps {
                        job_id: "job-1".into(),
                        payload_len: 3 * DISPATCH_BUFFER_BYTES,
                    },
                    &vec![7u8; 3 * DISPATCH_BUFFER_BYTES],
                )
                .expect("write snapshot");
            writer
        });
        let (frame, payload) = endpoint.recv().expect("recv").expect("a frame");
        assert!(matches!(frame, SchedulerMessage::JobSteps { .. }));
        assert_eq!(payload, blob);
        let _writer = pump.join().expect("pump thread");

        endpoint
            .send(
                &ExecutorMessage::Success {
                    job_id: "job-1".into(),
                    result_len: Some(4),
                    task_name: "t".into(),
                    wall_time_ns: 1,
                    lease: None,
                },
                b"done",
            )
            .expect("send success");
        let (_, result) = reader.read::<ExecutorMessage>().expect("read success");
        assert_eq!(result, b"done");
    }

    #[test]
    fn a_stalled_peer_times_a_dispatch_write_out_rather_than_buffering() {
        let (transport, endpoint) = FrameTransport::new("grpc:test", false);
        let (_reader, mut writer, connection) = split(transport);
        connection
            .set_write_timeout(Some(Duration::from_millis(50)))
            .expect("set write timeout");

        // Nothing reads `endpoint`, so the bounded direction fills and the next
        // write has to fail instead of parking the dispatch thread for good.
        let error = loop {
            match writer.write(
                &SchedulerMessage::Cancel {
                    job_id: "x".repeat(1024),
                },
                &[],
            ) {
                Ok(()) => continue,
                Err(error) => break error,
            }
        };
        assert!(
            matches!(&error, ProtocolError::Io(io) if io.kind() == io::ErrorKind::WouldBlock),
            "expected a timed-out write, got {error}"
        );
        drop(endpoint);
    }

    #[test]
    fn closing_the_endpoint_ends_the_connection() {
        let (transport, endpoint) = FrameTransport::new("grpc:test", false);
        let (mut reader, _writer, _connection) = split(transport);

        endpoint.close();
        assert!(matches!(
            reader.read::<ExecutorMessage>(),
            Err(ProtocolError::Eof)
        ));
        assert!(endpoint.recv().expect("recv after close").is_none());
    }

    #[test]
    fn a_send_to_a_scheduler_that_is_gone_fails_rather_than_buffering() {
        // The scheduler-bound direction is unbounded, so nothing back-pressures
        // it; without a reader check a `send` after the connection ended would
        // answer `Ok` for a frame that reached no one, and each one would grow a
        // buffer nobody drains. A heartbeat is the caller that would report
        // that success straight back to an executor.
        let (transport, endpoint) = FrameTransport::new("grpc:test", false);
        let (_reader, _writer, connection) = split(transport);

        endpoint
            .send(&ExecutorMessage::Heartbeat { free_slots: 2 }, &[])
            .expect("a live connection takes it");

        connection.close();
        let error = endpoint
            .send(&ExecutorMessage::Heartbeat { free_slots: 1 }, &[])
            .expect_err("a send to a closed connection must fail");
        assert!(
            matches!(&error, ProtocolError::Io(io) if io.kind() == io::ErrorKind::BrokenPipe),
            "expected a broken pipe, got {error}"
        );
    }

    #[test]
    fn a_send_after_the_reader_half_is_dropped_fails_too() {
        // Same rule by the other route: the dispatcher's read half going away
        // is the scheduler going away, whether or not anyone called `close`.
        let (transport, endpoint) = FrameTransport::new("grpc:test", false);
        let (reader, _writer, _connection) = split(transport);
        drop(reader);

        assert!(
            endpoint
                .send(&ExecutorMessage::Heartbeat { free_slots: 1 }, &[])
                .is_err(),
            "a send with no reader behind it must not report success"
        );
    }

    #[test]
    fn closing_the_connection_ends_the_endpoint() {
        let (transport, endpoint) = FrameTransport::new("grpc:test", false);
        let (_reader, _writer, connection) = split(transport);

        connection.close();
        assert!(endpoint.recv().expect("recv after close").is_none());
    }

    #[test]
    fn authentication_is_the_transports_to_declare() {
        let (plain, _plain_endpoint) = FrameTransport::new("grpc:test", false);
        let (vouched, _vouched_endpoint) = FrameTransport::new("grpc:test", true);
        assert!(!plain.is_authenticated());
        assert!(vouched.is_authenticated());
    }

    #[test]
    fn a_write_after_the_reader_is_gone_is_a_broken_pipe() {
        let (transport, endpoint) = FrameTransport::new("grpc:test", false);
        let (reader, mut writer, _connection) = split(transport);
        drop(endpoint);
        drop(reader);

        // Fill past the cap so the write has to consult the reader's state.
        let mut last = Ok(());
        for _ in 0..(DISPATCH_BUFFER_BYTES / 512 + 8) {
            last = writer.write(
                &SchedulerMessage::Cancel {
                    job_id: "x".repeat(512),
                },
                &[],
            );
            if last.is_err() {
                break;
            }
        }
        let error = last.expect_err("a write to a gone peer must fail");
        assert!(
            matches!(&error, ProtocolError::Io(io) if io.kind() == io::ErrorKind::BrokenPipe),
            "expected a broken pipe, got {error}"
        );
    }

    #[test]
    fn an_endpoint_write_never_blocks_on_a_slow_scheduler() {
        let (transport, endpoint) = FrameTransport::new("grpc:test", false);
        let (_reader, _writer, _connection) = split(transport);

        // The scheduler-bound direction is unbounded on purpose: the reader
        // thread drains it continuously and the door upstream has its own flow
        // control, so an executor reporting progress must never park here.
        for index in 0..2000 {
            endpoint
                .send(
                    &ExecutorMessage::Progress {
                        job_id: format!("job-{index}"),
                        progress: 1,
                        lease: None,
                    },
                    &[],
                )
                .expect("send progress");
        }
    }

    #[test]
    fn the_read_timeout_bounds_a_silent_peer() {
        let (transport, endpoint) = FrameTransport::new("grpc:test", false);
        let (mut reader, _writer, connection) = split(transport);
        connection
            .set_read_timeout(Some(Duration::from_millis(30)))
            .expect("set read timeout");

        let error = reader
            .read::<ExecutorMessage>()
            .expect_err("a silent peer must time out");
        assert!(
            matches!(&error, ProtocolError::Io(io) if io.kind() == io::ErrorKind::WouldBlock),
            "expected a timed-out read, got {error}"
        );
        drop(endpoint);
    }

    #[test]
    fn a_declared_length_that_does_not_match_its_payload_is_refused() {
        let (_transport, endpoint) = FrameTransport::new("grpc:test", false);
        let error = endpoint
            .send(
                &ExecutorMessage::Success {
                    job_id: "job-1".into(),
                    result_len: Some(9),
                    task_name: "t".into(),
                    wall_time_ns: 1,
                    lease: None,
                },
                b"short",
            )
            .expect_err("a mismatch must not reach the stream");
        assert!(matches!(
            error,
            ProtocolError::PayloadLengthMismatch {
                declared: 9,
                actual: 5
            }
        ));
    }

    #[test]
    fn the_peer_label_is_the_transports_own() {
        let (transport, _endpoint) = FrameTransport::new("grpc:10.0.0.4:44100", true);
        assert_eq!(transport.peer(), "grpc:10.0.0.4:44100");
    }

    #[test]
    fn a_dropped_writer_half_ends_the_endpoint() {
        let (transport, endpoint) = FrameTransport::new("grpc:test", false);
        let (_reader, writer, _connection) = split(transport);
        drop(writer);
        assert!(endpoint
            .recv()
            .expect("recv after the writer went")
            .is_none());
    }

    #[test]
    fn bytes_written_before_a_close_are_not_kept() {
        // A forced close is a shutdown, not a flush: whatever had not been read
        // is gone, exactly as it is when a socket is torn down under a peer.
        let (transport, endpoint) = FrameTransport::new("grpc:test", false);
        let (_reader, mut writer, connection) = split(transport);
        writer
            .write(&SchedulerMessage::Shutdown, &[])
            .expect("write shutdown");
        connection.close();
        assert!(endpoint.recv().expect("recv after close").is_none());
    }
}
