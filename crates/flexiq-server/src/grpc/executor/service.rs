//! `flexiq.executor.v1.ExecutorService`: the executor door.
//!
//! Everything here is plumbing between a gRPC stream and a
//! [`flexiq_core::worker::FrameTransport`]. The scheduler side
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
//! * **Outbound** (OS thread): the endpoint → [`flexiq_core::SchedulerMessage`] →
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
///
/// Two shapes, because a deployment has one dispatch path and the door has to
/// serve whichever it is:
///
/// * **Attach** — executors dial in and hold a stream. Every RPC is served.
/// * **Settle-only** — the scheduler dials *out* to a push target, and the
///   door exists solely so that target can report on work that outlived the
///   request it arrived on. There is nothing to attach *to*, so `Attach` and
///   `Heartbeat` refuse.
///
/// Not one type per shape: a client reads one service descriptor, and a door
/// that served a different set of RPCs depending on configuration would make
/// "is this RPC available" a deployment question rather than a version one.
/// Refusing by precondition keeps the surface constant and the answer honest.
#[derive(Clone)]
pub struct ExecutorDoor {
    /// `None` on a push deployment: nothing attaches.
    dispatcher: Option<RemoteDispatcher>,
    /// `None` on an attach deployment: nothing was dialled out to.
    #[cfg(feature = "http-target")]
    target: Option<Arc<flexiq_core::HttpDispatchTarget>>,
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
            dispatcher: Some(dispatcher),
            #[cfg(feature = "http-target")]
            target: None,
            supervisor,
            sessions: Arc::new(SessionRegistry::default()),
            rotation,
        }
    }

    /// Serve only the reporting RPCs, for a push deployment where nothing
    /// attaches.
    ///
    /// The door is the one inbound surface a push target has. Without it a job
    /// that answered `202 Accepted` would have nowhere to report, which is why
    /// turning settle callbacks on requires the gRPC listener.
    #[cfg(feature = "http-target")]
    pub fn settle_only(
        target: Arc<flexiq_core::HttpDispatchTarget>,
        supervisor: Arc<SchedulerSupervisor>,
    ) -> Self {
        Self {
            dispatcher: None,
            target: Some(target),
            supervisor,
            sessions: Arc::new(SessionRegistry::default()),
            // Nothing holds a stream, so there is nothing to rotate.
            rotation: Rotation::new(None),
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
    /// `None` on a settle-only door: it has no attached capacity to report,
    /// and a zero would read as "attached, with no slots" rather than "nothing
    /// attaches here".
    pub fn capacity(&self) -> Option<flexiq_core::Capacity> {
        self.dispatcher.as_ref().map(RemoteDispatcher::capacity)
    }

    /// Dispatches this replica accepted over push and has not settled.
    ///
    /// `None` on an attach door, which accepts none. **Per replica, not per
    /// cluster**: the waiting attempts live in this process, and a cluster
    /// total would mean a storage count on every scrape for a number an
    /// operator reads per incident.
    #[cfg(feature = "http-target")]
    pub fn awaiting_settle(&self) -> Option<usize> {
        self.target.as_ref().map(|target| target.awaiting_settle())
    }

    /// Without the push path there is no dispatch to accept, so there is
    /// nothing to count. Present so the metrics route reads the same either
    /// way rather than growing a `cfg` of its own.
    #[cfg(not(feature = "http-target"))]
    pub fn awaiting_settle(&self) -> Option<usize> {
        None
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
        // A settle-only door has nothing to attach *to*. Refused before a
        // stream is opened, so a client is told on its first RPC rather than
        // holding a connection that will never carry a job.
        let dispatcher = self.dispatcher.clone().ok_or_else(nothing_attaches)?;

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
        if self.dispatcher.is_none() {
            return Err(nothing_attaches());
        }

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

    async fn settle(
        &self,
        request: Request<pb::SettleRequest>,
    ) -> Result<Response<pb::SettleResponse>, Status> {
        #[cfg(feature = "http-target")]
        {
            let target = self.target.as_ref().ok_or_else(settle_disabled)?;
            let (job_id, lease, outcome) = frames::settle_request(request.into_inner())?;
            // Blocking: the fence is a storage write, and this runtime also
            // carries the dispatch requests. The same reason the dashboard's
            // reads go through `blocking`.
            let target = Arc::clone(target);
            crate::grpc::blocking::run(move || {
                target
                    .settle(&job_id, &lease, outcome)
                    .map_err(settle_refusal)
            })
            .await?;
            return Ok(Response::new(pb::SettleResponse {}));
        }
        #[cfg(not(feature = "http-target"))]
        {
            let _ = request;
            Err(settle_disabled())
        }
    }

    async fn extend_lease(
        &self,
        request: Request<pb::ExtendLeaseRequest>,
    ) -> Result<Response<pb::ExtendLeaseResponse>, Status> {
        #[cfg(feature = "http-target")]
        {
            let target = self.target.as_ref().ok_or_else(settle_disabled)?;
            let request = request.into_inner();
            let lease = frames::lease_from_bytes(&request.lease)?;
            let extend_by = frames::extension_from_wire(request.extend_by)?;
            let job_id = request.job_id;

            let target = Arc::clone(target);
            let deadline = crate::grpc::blocking::run(move || {
                target
                    .extend_lease(&job_id, &lease, extend_by)
                    .map_err(settle_refusal)
            })
            .await?;
            return Ok(Response::new(pb::ExtendLeaseResponse {
                deadline: Some(frames::timestamp_from_millis(deadline)),
            }));
        }
        #[cfg(not(feature = "http-target"))]
        {
            let _ = request;
            Err(settle_disabled())
        }
    }

    async fn report_progress(
        &self,
        request: Request<pb::ReportProgressRequest>,
    ) -> Result<Response<pb::ReportProgressResponse>, Status> {
        #[cfg(feature = "http-target")]
        {
            let target = self.target.as_ref().ok_or_else(settle_disabled)?;
            let frame = request
                .into_inner()
                .progress
                .ok_or_else(|| Status::invalid_argument("a progress report carries no frame"))?;
            let lease = frames::lease_from_bytes(frame.lease.as_deref().unwrap_or_default())?;

            let target = Arc::clone(target);
            // Fire and forget, like the frame it mirrors: an empty response
            // means the frame was taken, not that a row was written. A task
            // that only wanted to report progress must never block on us.
            crate::grpc::blocking::run(move || {
                target
                    .report_progress(&frame.job_id, &lease, frame.progress)
                    .map_err(settle_refusal)
            })
            .await?;
            return Ok(Response::new(pb::ReportProgressResponse {}));
        }
        #[cfg(not(feature = "http-target"))]
        {
            let _ = request;
            Err(settle_disabled())
        }
    }

    async fn write_task_log(
        &self,
        request: Request<pb::WriteTaskLogRequest>,
    ) -> Result<Response<pb::WriteTaskLogResponse>, Status> {
        #[cfg(feature = "http-target")]
        {
            let target = self.target.as_ref().ok_or_else(settle_disabled)?;
            let frame = request
                .into_inner()
                .task_log
                .ok_or_else(|| Status::invalid_argument("a task log carries no frame"))?;
            let lease = frames::lease_from_bytes(frame.lease.as_deref().unwrap_or_default())?;

            let target = Arc::clone(target);
            crate::grpc::blocking::run(move || {
                // `extra` is pre-encoded JSON that is not guaranteed UTF-8 on
                // the wire. The scheduler's existing rule is to drop an
                // unreadable blob and keep the line, which is what `None` does
                // here — losing the whole frame over it would be worse.
                let extra = frame
                    .extra
                    .as_deref()
                    .and_then(|bytes| std::str::from_utf8(bytes).ok());
                target
                    .write_task_log(
                        &frame.job_id,
                        &lease,
                        &frame.task_name,
                        &frame.level,
                        &frame.message,
                        extra,
                    )
                    .map_err(settle_refusal)
            })
            .await?;
            return Ok(Response::new(pb::WriteTaskLogResponse {}));
        }
        #[cfg(not(feature = "http-target"))]
        {
            let _ = request;
            Err(settle_disabled())
        }
    }
}

/// The answer the four reporting RPCs give on a deployment that does not
/// accept `202`.
///
/// `FAILED_PRECONDITION` rather than `UNIMPLEMENTED`: the RPC exists and this
/// build serves it, but the deployment has not turned settle callbacks on, so
/// there is no accepted dispatch for one to name. `UNIMPLEMENTED` would read as
/// "upgrade the server", which is the wrong thing to go and do.
/// What `Attach` and `Heartbeat` answer on a settle-only door.
///
/// `FAILED_PRECONDITION` rather than `UNIMPLEMENTED`, for the reason
/// [`settle_disabled`] gives: the build serves the RPC, the deployment has no
/// use for it. `UNIMPLEMENTED` would send an operator to upgrade a server that
/// is already the right version.
fn nothing_attaches() -> Status {
    Status::failed_precondition(
        "this deployment dispatches by pushing to a target, so nothing attaches here; \
         the executor door serves only the reporting RPCs",
    )
}

fn settle_disabled() -> Status {
    Status::failed_precondition(
        "settle callbacks are not enabled on this deployment; \
         set FLEXIQ_PUSH_TARGET_SETTLE=grpc on the scheduler that dispatches",
    )
}

/// Map a refusal from the dispatch target onto a status code.
///
/// Every one of these is `FAILED_PRECONDITION`, never `ABORTED`: `ABORTED` sits
/// in the retry-with-backoff class, and a report that lost its fence must not
/// be resent — resending it is the double execution the fence exists to refuse.
/// The messages differ because the operator actions differ.
#[cfg(feature = "http-target")]
fn settle_refusal(refused: flexiq_core::SettleRefused) -> Status {
    use flexiq_core::SettleRefused;
    match refused {
        // Not a stale lease: the caller may be perfectly current and simply
        // have reached the wrong replica. Said plainly, because the fix is an
        // operator's routing and not the target's code.
        SettleRefused::NotHere => Status::failed_precondition(
            "no accepted dispatch for this job on this replica; a settle must reach the \
             scheduler that dispatched the job, so run one scheduler replica or route \
             these calls to it",
        ),
        SettleRefused::Fenced => Status::failed_precondition(
            "this dispatch was already settled, or the lease names an attempt that has been \
             superseded; do not retry",
        ),
        SettleRefused::Unsupported => settle_disabled(),
        SettleRefused::Storage(error) => {
            log::warn!("[flexiq] a settle could not be fenced: {error}");
            // Deliberately not `UNAVAILABLE`, which is retryable: we do not
            // know whether the marker was consumed, and a resend under that
            // doubt is the one thing the fence must not permit.
            Status::failed_precondition("the settle fence could not be evaluated; do not retry")
        }
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

    /// Both refusals a settle-only door gives are `FAILED_PRECONDITION`, and
    /// neither is `UNIMPLEMENTED`: the build serves every RPC, and it is the
    /// deployment that has no use for some of them. `UNIMPLEMENTED` would send
    /// an operator to upgrade a server that is already the right version.
    #[test]
    fn a_settle_only_door_refuses_by_precondition() {
        assert_eq!(nothing_attaches().code(), tonic::Code::FailedPrecondition);
        assert_eq!(settle_disabled().code(), tonic::Code::FailedPrecondition);
        // Each names what to do about it, and they are different things.
        assert!(nothing_attaches().message().contains("nothing attaches"));
        assert!(settle_disabled()
            .message()
            .contains("FLEXIQ_PUSH_TARGET_SETTLE"));
    }

    /// A report that lost its fence must never be resent, so none of these may
    /// be `ABORTED` — which sits in the retry-with-backoff class. Resending one
    /// is the double execution the fence exists to refuse.
    #[cfg(feature = "http-target")]
    #[test]
    fn no_settle_refusal_is_retryable() {
        use flexiq_core::SettleRefused;
        for refused in [
            SettleRefused::NotHere,
            SettleRefused::Fenced,
            SettleRefused::Unsupported,
            SettleRefused::Storage("the database is unhappy".to_string()),
        ] {
            let status = settle_refusal(refused);
            assert_eq!(
                status.code(),
                tonic::Code::FailedPrecondition,
                "unexpected code for {status:?}"
            );
        }

        // And the storage arm says nothing about the storage: the detail is
        // logged, never handed to a peer.
        let leaked = settle_refusal(SettleRefused::Storage("host=db user=root".to_string()));
        assert!(!leaked.message().contains("host=db"));
    }

    /// "Not on this replica" and "already settled" are different problems with
    /// different fixes, and an operator told the wrong one goes looking for a
    /// race that never happened.
    #[cfg(feature = "http-target")]
    #[test]
    fn a_misrouted_settle_is_not_reported_as_a_stale_one() {
        use flexiq_core::SettleRefused;
        let elsewhere = settle_refusal(SettleRefused::NotHere);
        let stale = settle_refusal(SettleRefused::Fenced);
        assert_ne!(elsewhere.message(), stale.message());
        assert!(elsewhere.message().contains("replica"));
        assert!(stale.message().contains("do not retry"));
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
