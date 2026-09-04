//! `flexiq.executor.v1.ExecutorService`: the executor door.
//!
//! Everything here is plumbing between a gRPC stream and a
//! [`FrameTransport`](flexiq_core::worker::FrameTransport). The scheduler side
//! is [`RemoteDispatcher::attach`], unchanged and unaware — an executor that
//! dialled in over gRPC is placed by the same rules as one on a socket, appears
//! in the same registry, and is compared against it by the same registry
//! divergence check. That is the whole design: a fourth transport, not a second
//! dispatcher.
//!
//! # Three threads and a stream
//!
//! * **Inbound** (async task): `AttachRequest` → [`ExecutorMessage`] →
//!   the endpoint. Never blocks; the endpoint's scheduler-bound direction is
//!   unbounded and the dispatcher's reader drains it continuously.
//! * **Outbound** (OS thread): the endpoint → [`SchedulerMessage`] →
//!   `AttachResponse`. A thread rather than a task because `recv` blocks, the
//!   same shape as the dispatcher's own reader thread.
//! * **Lifecycle** (async task): runs the handshake off the runtime, starts the
//!   scheduler on the first attach, then either rotates the stream on its timer
//!   or waits for the client to end it.
//!
//! # The stream ends on a timer
//!
//! A gRPC stream cannot be load balanced once it has started, and a
//! stream-per-executor that never ends pins every executor to whichever replica
//! it first reached. So it is bounded — and bounding it is only safe because
//! [`RemoteDispatcher::detach`] stops matching work *before* it closes anything
//! and waits for every slot to come back. Closing first and letting the reaper
//! notice is the failure this door exists not to have.

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use flexiq_core::worker::protocol::ExecutorMessage;
use flexiq_core::worker::{AttachError, FrameTransport};
use flexiq_core::RemoteDispatcher;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;
use tonic::metadata::MetadataValue;
use tonic::{Request, Response, Status, Streaming};

use crate::grpc::executor::frames;
use crate::grpc::executor::session::SessionRegistry;
use crate::grpc::limits::EXECUTOR_MAX_MESSAGE_BYTES;
use crate::grpc::pb::executor as pb;
use crate::grpc::pb::executor::executor_service_server::{ExecutorService, ExecutorServiceServer};
use crate::runtime::scheduler::SchedulerSupervisor;

/// Metadata key carrying the session token. `-bin` because the value is bytes.
pub const SESSION_METADATA: &str = "flexiq-attach-session-bin";

/// Dispatch frames a stream may have queued before the writer feels it.
///
/// Small: this is the second buffer in a chain that already has a bounded one
/// inside the transport, and its whole job is to keep backpressure reaching the
/// dispatch write rather than absorbing it.
const OUTBOUND_FRAMES: usize = 16;

/// How far either side of [`Rotation::max_age`] a stream's lifetime may fall.
///
/// Without it a fleet that started together rotates together, and the gap that
/// a rotation opens — brief, but real — would land on every executor at once.
const JITTER: f64 = 0.1;

/// When a stream's lifetime runs out.
#[derive(Debug, Clone, Copy)]
pub struct Rotation {
    max_age: Option<Duration>,
}

impl Rotation {
    /// Rotate after `max_age`. Zero, or `None`, leaves streams unbounded.
    pub fn new(max_age: Option<Duration>) -> Self {
        Self {
            max_age: max_age.filter(|age| !age.is_zero()),
        }
    }

    /// This stream's lifetime: the configured age, jittered.
    fn deadline(&self) -> Option<Duration> {
        self.max_age.map(|age| {
            let spread = 1.0 + (rand::random::<f64>() * 2.0 - 1.0) * JITTER;
            age.mul_f64(spread)
        })
    }
}

/// The executor door's state.
#[derive(Clone)]
pub struct ExecutorDoor {
    dispatcher: RemoteDispatcher,
    supervisor: Arc<SchedulerSupervisor>,
    sessions: Arc<SessionRegistry>,
    rotation: Rotation,
}

impl std::fmt::Debug for ExecutorDoor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecutorDoor")
            .field("rotation", &self.rotation)
            .finish_non_exhaustive()
    }
}

impl ExecutorDoor {
    /// Serve executors into `dispatcher`, starting `supervisor` on the first
    /// one to attach.
    pub fn new(
        dispatcher: RemoteDispatcher,
        supervisor: Arc<SchedulerSupervisor>,
        rotation: Rotation,
    ) -> Self {
        Self {
            dispatcher,
            supervisor,
            sessions: Arc::new(SessionRegistry::default()),
            rotation,
        }
    }

    /// The registered service, capped at the executor door's message size.
    ///
    /// Its own cap, and larger than the producer's: a payload limit and a
    /// message limit are different numbers, and setting this to the payload
    /// limit would make the gRPC transport refuse work a socket accepts.
    pub fn into_service(self) -> ExecutorServiceServer<Self> {
        ExecutorServiceServer::new(self)
            .max_decoding_message_size(EXECUTOR_MAX_MESSAGE_BYTES)
            .max_encoding_message_size(EXECUTOR_MAX_MESSAGE_BYTES)
    }

    /// What the attached executors advertise, for the `/metrics` gauges.
    ///
    /// The door is where the dispatcher reaches this listener at all — nothing
    /// else on it holds one — so the gauges the dashboard publishes are only
    /// reachable here.
    pub fn capacity(&self) -> flexiq_core::Capacity {
        self.dispatcher.capacity()
    }

    /// The live sessions, for tests and for a leak check.
    pub fn sessions(&self) -> &Arc<SessionRegistry> {
        &self.sessions
    }
}

/// What a client should read into a refused attach.
///
/// The frame protocol says nothing at all to a refused peer, on purpose: it is
/// reachable by anyone who can open a socket. This door is not — the auth layer
/// checked a scoped token before the RPC was entered — so naming the reason is
/// diagnostics rather than an oracle.
fn refusal(error: &AttachError) -> Status {
    match error {
        AttachError::DuplicateId(id) => Status::already_exists(format!(
            "executor {id} is already attached; wait for the previous stream to end"
        )),
        AttachError::ShuttingDown => {
            Status::unavailable("the scheduler is shutting down and accepts no new executors")
        }
        // A version mismatch is not retryable by waiting: the peer has to be
        // the build that speaks this protocol.
        AttachError::Protocol(protocol) => Status::failed_precondition(protocol.to_string()),
        // Unreachable: this transport vouches for its peer, so the frame
        // credential is never asked for. Mapped anyway rather than defaulted.
        AttachError::Unauthorized(_) => Status::permission_denied("the attach was refused"),
        AttachError::Transport(io) => Status::unavailable(io.to_string()),
    }
}

#[tonic::async_trait]
impl ExecutorService for ExecutorDoor {
    type AttachStream = Pin<Box<dyn Stream<Item = Result<pb::AttachResponse, Status>> + Send>>;

    async fn attach(
        &self,
        request: Request<Streaming<pb::AttachRequest>>,
    ) -> Result<Response<Self::AttachStream>, Status> {
        let peer = request
            .remote_addr()
            .map_or_else(|| "grpc:unknown".to_string(), |addr| format!("grpc:{addr}"));
        let mut inbound = request.into_inner();

        // `true`: the auth layer checked a scoped token before this RPC was
        // entered, so the handshake must not also demand the frame credential
        // — which this door has no way to present.
        let (transport, endpoint) = FrameTransport::new(peer.clone(), true);
        let endpoint = Arc::new(endpoint);
        let session = self.sessions.open(Arc::clone(&endpoint));

        let (tx, rx) = mpsc::channel::<Result<pb::AttachResponse, Status>>(OUTBOUND_FRAMES);
        let refusals = tx.clone();

        std::thread::Builder::new()
            .name("flexiq-grpc-executor-out".to_string())
            .spawn({
                let endpoint = Arc::clone(&endpoint);
                let peer = peer.clone();
                move || loop {
                    match endpoint.recv() {
                        Ok(Some((frame, payload))) => {
                            // A frame with no wire form is skipped rather than
                            // ending the stream, the same answer the frame
                            // protocol gives an unknown frame type.
                            let Some(response) = frames::from_scheduler_message(frame, payload)
                            else {
                                continue;
                            };
                            if tx.blocking_send(Ok(response)).is_err() {
                                break;
                            }
                        }
                        Ok(None) => break,
                        Err(error) => {
                            log::warn!("[flexiq] executor stream {peer} failed to read: {error}");
                            break;
                        }
                    }
                }
            })
            .map_err(|error| {
                self.sessions.close(&session);
                Status::resource_exhausted(format!("could not start the executor stream: {error}"))
            })?;

        let (done_tx, done_rx) = oneshot::channel::<()>();
        tokio::spawn({
            let endpoint = Arc::clone(&endpoint);
            let peer = peer.clone();
            async move {
                // `_done` is never sent on: dropping it is the signal, so the
                // lifecycle task learns the stream ended however it ended.
                let _done = done_tx;
                loop {
                    match inbound.message().await {
                        Ok(Some(request)) => {
                            let Some((frame, payload)) = frames::to_executor_message(request)
                            else {
                                continue;
                            };
                            if let Err(error) = endpoint.send(&frame, &payload) {
                                log::warn!(
                                    "[flexiq] executor stream {peer} could not be written: {error}"
                                );
                                break;
                            }
                        }
                        Ok(None) => break,
                        Err(status) => {
                            log::debug!("[flexiq] executor stream {peer} ended: {status}");
                            break;
                        }
                    }
                }
                // The dispatcher's reader sees EOF and abandons whatever this
                // executor held, exactly as it does when a socket goes away.
                endpoint.close();
            }
        });

        tokio::spawn({
            let dispatcher = self.dispatcher.clone();
            let supervisor = Arc::clone(&self.supervisor);
            let sessions = Arc::clone(&self.sessions);
            let session = session.clone();
            let rotation = self.rotation;
            let endpoint = Arc::clone(&endpoint);
            async move {
                // Off the runtime: the handshake blocks reading `hello`, and
                // `hello` is delivered by the inbound task above.
                let attached = {
                    let dispatcher = dispatcher.clone();
                    tokio::task::spawn_blocking(move || {
                        let executor_id = dispatcher.attach(Box::new(transport))?;
                        if let Err(error) = supervisor.ensure_started() {
                            log::error!(
                                "[flexiq] executor {executor_id} attached but the scheduler \
                                 failed to start: {error}"
                            );
                        }
                        Ok::<_, AttachError>(executor_id)
                    })
                    .await
                };

                let executor_id = match attached {
                    Ok(Ok(executor_id)) => executor_id,
                    Ok(Err(error)) => {
                        log::warn!("[flexiq] attach from {peer} rejected: {error}");
                        // Best effort, and after the handshake's own frames:
                        // a version mismatch is acked before it is refused, so
                        // both ends can log both numbers.
                        let _ = refusals.send(Err(refusal(&error))).await;
                        endpoint.close();
                        sessions.close(&session);
                        return;
                    }
                    Err(join) => {
                        log::error!("[flexiq] the attach from {peer} panicked: {join}");
                        endpoint.close();
                        sessions.close(&session);
                        return;
                    }
                };

                // The outbound pump is the response stream's only other sender,
                // and the stream ends when the last one is dropped. Holding
                // this clone past the handshake would keep a finished stream
                // open until the client hung up — and a graceful listener
                // shutdown waits for exactly that, so the process would never
                // exit while an executor was attached.
                drop(refusals);

                let mut done = done_rx;
                match rotation.deadline() {
                    Some(max_age) => {
                        tokio::select! {
                            () = tokio::time::sleep(max_age) => {
                                log::info!(
                                    "[flexiq] rotating executor {executor_id}'s stream after \
                                     {max_age:?}; it will reconnect"
                                );
                                // The drain budget is the rotation period: a
                                // job that outlives a whole period is
                                // pathological, not a case to configure for.
                                dispatcher.detach(&executor_id, max_age).await;
                            }
                            _ = &mut done => {}
                        }
                    }
                    None => {
                        let _ = done.await;
                    }
                }
                sessions.close(&session);
            }
        });

        let stream = Box::pin(ReceiverStream::new(rx)) as Self::AttachStream;
        let mut response = Response::new(stream);
        response
            .metadata_mut()
            .insert_bin(SESSION_METADATA, MetadataValue::from_bytes(&session));
        Ok(response)
    }

    async fn heartbeat(
        &self,
        request: Request<pb::HeartbeatRequest>,
    ) -> Result<Response<pb::HeartbeatResponse>, Status> {
        let request = request.into_inner();
        let Some(endpoint) = self.sessions.get(&request.session) else {
            return Err(Status::not_found(
                "no attached stream for this session; reattach and use the session \
                 the Attach response returned",
            ));
        };

        // Injected as the frame it already is, rather than applied here. The
        // RPC moved the *delivery* off the dispatch stream; it did not add a
        // second way for capacity to change.
        endpoint
            .send(
                &ExecutorMessage::Heartbeat {
                    free_slots: request.free_slots,
                },
                &[],
            )
            .map_err(|error| {
                Status::unavailable(format!("the attached stream is not writable: {error}"))
            })?;

        Ok(Response::new(pb::HeartbeatResponse {}))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rotation_is_off_when_no_age_is_configured() {
        assert!(Rotation::new(None).deadline().is_none());
        // Zero is how an operator turns it off, and it must not become a
        // stream that ends immediately.
        assert!(Rotation::new(Some(Duration::ZERO)).deadline().is_none());
    }

    #[test]
    fn rotation_is_jittered_around_the_configured_age() {
        let rotation = Rotation::new(Some(Duration::from_secs(1_000)));
        let mut seen = std::collections::HashSet::new();
        for _ in 0..64 {
            let deadline = rotation.deadline().expect("configured");
            assert!(
                deadline >= Duration::from_secs(900) && deadline <= Duration::from_secs(1_100),
                "{deadline:?} is outside the jitter window"
            );
            seen.insert(deadline);
        }
        assert!(
            seen.len() > 1,
            "a fleet that started together must not rotate together"
        );
    }

    #[test]
    fn a_refusal_says_which_kind_it_was() {
        // The socket handshake is deliberately mute; this door is not reachable
        // without a scoped token, so naming the reason is diagnostics rather
        // than an oracle.
        assert_eq!(
            refusal(&AttachError::DuplicateId("exec-1".into())).code(),
            tonic::Code::AlreadyExists
        );
        assert_eq!(
            refusal(&AttachError::ShuttingDown).code(),
            tonic::Code::Unavailable
        );
        assert_eq!(
            refusal(&AttachError::Protocol(
                flexiq_core::ProtocolError::VersionMismatch { ours: 1, theirs: 2 }
            ))
            .code(),
            tonic::Code::FailedPrecondition
        );
    }
}
