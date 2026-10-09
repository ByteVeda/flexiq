//! `StreamIsActive`: KEDA's push variant of `IsActive`.
//!
//! A stream answers once with the current state, then again only when it
//! flips. Two wakes, as `WatchJobs` has:
//!
//! - [`Wakes`] — enqueues and retries *this* process announces on its event
//!   hub, so a job sent through the producer door activates at once.
//! - a re-read every reconcile interval, for what the hub never hears: jobs
//!   an SDK wrote straight to the database, and the queue draining.
//!
//! Either way a wake costs one depth read, however many events woke it, and
//! only an idle stream wakes on arrivals. Streams are bounded per credential,
//! as watches are.

use std::sync::Arc;
use std::time::Duration;

use flexiq_core::{EventHub, EventTap, EventType, JobEvent};
use tokio::sync::{broadcast, mpsc};
use tokio::time::{Instant, Interval, MissedTickBehavior};
use tokio_stream::wrappers::ReceiverStream;
use tonic::Status;

use super::{service, Answers, Scoped};
use crate::config::watch::WatchConfig;
use crate::grpc::pb::externalscaler as pb;
use crate::grpc::producer::watch::quota::{Quota, Slot};
use crate::grpc::status::WireError;
use crate::runtime::shutdown::Shutdown;

/// Wakes held for streams that have not read them yet. Overrun only costs a
/// spare read: a lagged stream re-reads the depth like any other wake.
const WAKES: usize = 256;

/// Answers queued between the task and the transport. Each is one bool, sent
/// only on a flip, so a handful is plenty.
const CHANNEL: usize = 4;

/// One announced arrival: where a job became due.
#[derive(Debug)]
struct Arrival {
    namespace: Option<String>,
    queue: String,
}

/// An [`EventTap`] that passes on every enqueue and retry.
pub struct Wakes {
    arrivals: broadcast::Sender<Arc<Arrival>>,
}

impl EventTap for Wakes {
    fn observe(&self, event: &JobEvent) {
        if !matches!(
            event.event_type,
            EventType::JobEnqueued | EventType::JobRetrying
        ) {
            return;
        }
        // An error only means no stream is open, so no one to wake.
        let _ = self.arrivals.send(Arc::new(Arrival {
            namespace: event.namespace.clone(),
            queue: event.queue.clone(),
        }));
    }
}

/// Every stream's shared state.
pub struct Streams {
    wakes: Arc<Wakes>,
    quota: Quota,
    /// How often a stream re-reads its depth; zero turns the re-read off.
    interval: Duration,
    /// How long a send waits on a client that is not reading.
    stall: Duration,
    shutdown: Shutdown,
}

impl std::fmt::Debug for Streams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Streams").finish_non_exhaustive()
    }
}

impl Streams {
    /// Tap `hub`, and hold streams to the same bounds `WatchJobs` streams are:
    /// its reconcile interval, stall and per-credential cap.
    pub fn start(hub: &EventHub, config: &WatchConfig, shutdown: Shutdown) -> Arc<Self> {
        let (arrivals, _) = broadcast::channel(WAKES);
        let wakes = Arc::new(Wakes { arrivals });
        hub.add_tap(Arc::clone(&wakes) as Arc<dyn EventTap>);
        Arc::new(Self {
            wakes,
            quota: Quota::new(config.max_per_credential),
            interval: config.reconcile_interval,
            stall: config.stall,
            shutdown,
        })
    }

    /// Take a slot for the caller, read the current state and start the stream.
    pub async fn open(&self, scoped: Scoped) -> Result<Answers<pb::IsActiveResponse>, Status> {
        let slot = self
            .quota
            .acquire(scoped.principal.credential())
            .ok_or_else(|| Status::from(WireError::watch_limit(self.quota.cap())))?;
        // Subscribed before the first read, so an arrival during it still wakes.
        let arrivals = self.wakes.arrivals.subscribe();
        // Read here, not in the task, so a failing first read is the call's status.
        let first = service::is_active(&scoped).await?;
        let (tx, rx) = mpsc::channel(CHANNEL);
        // Room is certain: the channel is new and its receiver is in hand.
        tx.try_send(Ok(first))
            .map_err(|_| Status::internal("a new scaler stream had no room"))?;
        let task = Task {
            scoped,
            arrivals,
            tx,
            last: first.result,
            stall: self.stall,
            shutdown: self.shutdown.clone(),
            _slot: slot,
        };
        tokio::spawn(task.run(self.interval));
        Ok(Box::pin(ReceiverStream::new(rx)))
    }
}

/// Why a stream stopped.
enum Stop {
    /// The client went away, or stopped reading for longer than the stall.
    Gone,
    /// End the stream with this status.
    End(Status),
}

/// One stream's feeding task.
struct Task {
    scoped: Scoped,
    arrivals: broadcast::Receiver<Arc<Arrival>>,
    tx: mpsc::Sender<Result<pb::IsActiveResponse, Status>>,
    /// The state the client last heard.
    last: bool,
    stall: Duration,
    shutdown: Shutdown,
    _slot: Slot,
}

impl Task {
    async fn run(mut self, interval: Duration) {
        let mut ticks = ticker(interval);
        if let Stop::End(status) = self.follow(ticks.as_mut()).await {
            // Best effort: a client that stopped reading never hears why.
            let _ = tokio::time::timeout(self.stall, self.tx.send(Err(status))).await;
        }
    }

    async fn follow(&mut self, mut ticks: Option<&mut Interval>) -> Stop {
        loop {
            let tick = async {
                match ticks.as_mut() {
                    Some(ticks) => {
                        ticks.tick().await;
                    }
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                () = self.shutdown.wait() => return Stop::End(WireError::shutting_down().into()),
                () = self.tx.closed() => return Stop::Gone,
                // An arrival cannot idle an active queue, so only an idle
                // stream listens; a busy queue then costs no read per enqueue.
                () = wait_for_arrival(&mut self.arrivals, &self.scoped), if !self.last => {}
                () = tick => {}
            }
            // One read for the whole burst: whatever else queued meanwhile is
            // answered by the read below. Bounded, so a sustained enqueue rate
            // cannot keep the task draining.
            for _ in 0..WAKES {
                if matches!(
                    self.arrivals.try_recv(),
                    Err(broadcast::error::TryRecvError::Empty
                        | broadcast::error::TryRecvError::Closed)
                ) {
                    break;
                }
            }
            let active = match service::is_active(&self.scoped).await {
                Ok(answer) => answer.result,
                Err(status) => {
                    // The next wake tries again; the client keeps the last state.
                    log::warn!("grpc: scaler stream read failed: {}", status.message());
                    continue;
                }
            };
            if active == self.last {
                continue;
            }
            self.last = active;
            let answer = Ok(pb::IsActiveResponse { result: active });
            match tokio::time::timeout(self.stall, self.tx.send(answer)).await {
                Ok(Ok(())) => {}
                Ok(Err(_)) | Err(_) => return Stop::Gone,
            }
        }
    }
}

/// The re-read's clock, first firing one interval from now: the stream has
/// just read. `None` when the re-read is off.
fn ticker(interval: Duration) -> Option<Interval> {
    if interval.is_zero() {
        return None;
    }
    let mut ticks = tokio::time::interval_at(Instant::now() + interval, interval);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
    Some(ticks)
}

/// Resolve on an arrival in the stream's namespace and queue, or on a lag,
/// which may have skipped one.
async fn wait_for_arrival(arrivals: &mut broadcast::Receiver<Arc<Arrival>>, scoped: &Scoped) {
    loop {
        match arrivals.recv().await {
            Ok(arrival) if concerns(&arrival, scoped) => return,
            Ok(_) => {}
            Err(broadcast::error::RecvError::Lagged(_)) => return,
            // The sender lives in the tap the hub holds, so this is shutdown;
            // the shutdown branch ends the stream.
            Err(broadcast::error::RecvError::Closed) => std::future::pending().await,
        }
    }
}

fn concerns(arrival: &Arrival, scoped: &Scoped) -> bool {
    arrival.lands_in(scoped.principal.namespace(), scoped.query.queue.as_deref())
}

impl Arrival {
    /// Whether it lands in `namespace` and, unless `None` (every queue), `queue`.
    fn lands_in(&self, namespace: &str, queue: Option<&str>) -> bool {
        self.namespace.as_deref() == Some(namespace)
            && queue.is_none_or(|queue| queue == self.queue)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(kind: EventType, namespace: &str, queue: &str) -> JobEvent {
        JobEvent::new(kind, "j", Some(namespace.into()), queue, "t")
    }

    #[test]
    fn only_enqueues_and_retries_wake() {
        let (arrivals, mut rx) = broadcast::channel(8);
        let wakes = Wakes { arrivals };
        wakes.observe(&event(EventType::JobStarted, "ns", "q"));
        wakes.observe(&event(EventType::JobCompleted, "ns", "q"));
        wakes.observe(&event(EventType::JobEnqueued, "ns", "q"));
        wakes.observe(&event(EventType::JobRetrying, "ns", "r"));
        assert_eq!(rx.try_recv().expect("the enqueue").queue, "q");
        assert_eq!(rx.try_recv().expect("the retry").queue, "r");
        assert!(rx.try_recv().is_err(), "nothing else was passed on");
    }

    #[test]
    fn an_arrival_concerns_its_namespace_and_queue() {
        let arrival = Arrival {
            namespace: Some("ns".into()),
            queue: "q".into(),
        };
        assert!(arrival.lands_in("ns", Some("q")));
        assert!(arrival.lands_in("ns", None), "a whole-namespace stream");
        assert!(!arrival.lands_in("ns", Some("r")));
        assert!(!arrival.lands_in("other", None));
        let unscoped = Arrival {
            namespace: None,
            queue: "q".into(),
        };
        assert!(!unscoped.lands_in("ns", None));
    }
}
