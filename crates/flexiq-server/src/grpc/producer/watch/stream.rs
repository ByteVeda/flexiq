//! One `WatchJobs` stream: the task that feeds it, and the stream tonic reads.
//!
//! The task writes into a small bounded channel and never waits on a client
//! longer than the stall bound. Its final status travels beside the channel,
//! not through it, so a stream that ended because the channel was full can
//! still say why once the client drains what was queued.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};
use std::time::Duration;

use flexiq_core::{Storage, StorageBackend};
use tokio::sync::{mpsc, watch};
use tokio_stream::Stream;
use tonic::Status;

use super::cursor;
use super::feed::{Seq, WatchFeed};
use super::quota::Slot;
use super::reconcile::{Reading, Reconciler, Round};
use super::transition::{self, Observed, Rank};
use crate::grpc::blocking;
use crate::grpc::pb::{self, watch_jobs_response::Item};
use crate::grpc::status::WireError;
use crate::runtime::shutdown::Shutdown;

/// Items queued between the task and the transport. Small on purpose: the
/// feed is the buffer, and this only smooths the hand-off.
const CHANNEL: usize = 32;

/// The response stream tonic reads: queued items, then the final status.
pub struct Outlet {
    items: mpsc::Receiver<pb::WatchJobsResponse>,
    end: Arc<Mutex<Option<Status>>>,
    done: bool,
}

impl Stream for Outlet {
    type Item = Result<pb::WatchJobsResponse, Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }
        match self.items.poll_recv(cx) {
            Poll::Ready(Some(item)) => Poll::Ready(Some(Ok(item))),
            Poll::Ready(None) => {
                self.done = true;
                let end = self
                    .end
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .take();
                Poll::Ready(end.map(Err))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

/// What the stream task shares with every stream.
#[derive(Clone)]
pub struct Shared {
    /// Where snapshots are read.
    pub storage: StorageBackend,
    /// This process's transitions.
    pub feed: Arc<WatchFeed>,
    /// Other processes' transitions, for id watches.
    pub reconciler: Arc<Reconciler>,
    /// Ends every stream `UNAVAILABLE`.
    pub shutdown: Shutdown,
    /// How long a send waits on a client that is not reading.
    pub stall: Duration,
}

/// Why the task stopped sending.
enum Stop {
    /// The client went away; there is no one to tell.
    Gone,
    /// End the stream with this status, or OK for `None`.
    End(Option<Status>),
}

/// The task's half of one stream.
pub struct Session {
    items: mpsc::Sender<pb::WatchJobsResponse>,
    end: Arc<Mutex<Option<Status>>>,
    stall: Duration,
    _slot: Slot,
}

/// A stream and the session that feeds it.
pub fn open(slot: Slot, stall: Duration) -> (Session, Outlet) {
    let (tx, rx) = mpsc::channel(CHANNEL);
    let end = Arc::new(Mutex::new(None));
    (
        Session {
            items: tx,
            end: Arc::clone(&end),
            stall,
            _slot: slot,
        },
        Outlet {
            items: rx,
            end,
            done: false,
        },
    )
}

impl Session {
    /// Queue one item, waiting at most the stall bound for room.
    async fn send(&self, item: Item, cursor: String) -> Result<(), Stop> {
        self.respond(pb::WatchJobsResponse {
            item: Some(item),
            cursor,
        })
        .await
    }

    /// Queue a response carrying only a cursor: where a queue watch starts,
    /// so a client that loses the stream before any transition resumes
    /// without a gap.
    async fn checkpoint(&self, cursor: String) -> Result<(), Stop> {
        self.respond(pb::WatchJobsResponse { item: None, cursor })
            .await
    }

    async fn respond(&self, response: pb::WatchJobsResponse) -> Result<(), Stop> {
        match tokio::time::timeout(self.stall, self.items.send(response)).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) => Err(Stop::Gone),
            Err(_) => Err(Stop::End(Some(WireError::watch_overflow().into()))),
        }
    }

    /// Record how the stream ends; dropping the session then closes it.
    fn finish(self, stop: Stop) {
        if let Stop::End(Some(status)) = stop {
            *self.end.lock().unwrap_or_else(PoisonError::into_inner) = Some(status);
        }
    }
}

/// What woke a waiting stream.
enum Wake {
    Feed,
    Round,
    Stop(Stop),
}

/// Wait for something to do.
async fn wake(
    session: &Session,
    shutdown: &Shutdown,
    head: &mut watch::Receiver<Seq>,
    rounds: Option<&mut watch::Receiver<Arc<Round>>>,
) -> Wake {
    let round = async {
        match rounds {
            Some(rounds) => rounds.changed().await,
            None => std::future::pending().await,
        }
    };
    tokio::select! {
        () = shutdown.wait() => Wake::Stop(Stop::End(Some(WireError::shutting_down().into()))),
        () = session.items.closed() => Wake::Stop(Stop::Gone),
        // The sender lives as long as the listener; an error here is shutdown.
        changed = head.changed() => match changed {
            Ok(()) => Wake::Feed,
            Err(_) => Wake::Stop(Stop::End(Some(WireError::shutting_down().into()))),
        },
        changed = round => match changed {
            Ok(()) => Wake::Round,
            Err(_) => Wake::Stop(Stop::End(Some(WireError::shutting_down().into()))),
        },
    }
}

/// Watch `ids`: snapshot each, then follow them until every one is finished.
pub async fn watch_ids(ctx: Shared, session: Session, namespace: Arc<str>, ids: Vec<String>) {
    let stop = follow_ids(&ctx, &session, &namespace, ids).await;
    session.finish(stop);
}

async fn follow_ids(
    ctx: &Shared,
    session: &Session,
    namespace: &Arc<str>,
    ids: Vec<String>,
) -> Stop {
    let registration = ctx.reconciler.register(Arc::clone(namespace), ids.clone());
    let mut rounds = ctx.reconciler.subscribe();
    rounds.borrow_and_update();
    // Subscribed before the snapshot, so nothing emitted during the read is
    // missed; what lands in between is ordered against the snapshot by rank.
    let mut head = ctx.feed.subscribe();
    let mut position = *head.borrow_and_update();

    let snapshot = {
        let ids = ids.clone();
        let scope = Arc::clone(namespace);
        blocking::on_storage(&ctx.storage, move |storage| {
            let ids: Vec<&str> = ids.iter().map(String::as_str).collect();
            storage.get_jobs_by_ids(&ids, Some(&scope))
        })
        .await
    };
    let rows = match snapshot {
        Ok(rows) => rows,
        Err(status) => return Stop::End(Some(status)),
    };
    let boundary = ctx.feed.head();

    let mut rows: HashMap<String, _> = rows.into_iter().map(|job| (job.id.clone(), job)).collect();
    let mut watching: HashMap<String, Rank> = HashMap::with_capacity(ids.len());
    for id in ids {
        let Some(job) = rows.remove(&id) else {
            if let Err(stop) = session.send(Item::NotFoundJobId(id), String::new()).await {
                return stop;
            }
            continue;
        };
        let observed = transition::from_row(&job);
        if !observed.terminal() {
            watching.insert(id, observed.rank);
        }
        if let Err(stop) = send_transition(session, observed, String::new()).await {
            return stop;
        }
    }

    loop {
        if watching.is_empty() {
            return Stop::End(None);
        }
        registration.update(watching.keys().cloned().collect());
        match wake(session, &ctx.shutdown, &mut head, Some(&mut rounds)).await {
            Wake::Stop(stop) => return stop,
            Wake::Feed => {
                let events = match ctx.feed.read_after(position) {
                    Ok(events) => events,
                    Err(_) => return Stop::End(Some(WireError::watch_overflow().into())),
                };
                for (seq, event) in events {
                    position = seq;
                    if event.namespace.as_deref() != Some(&**namespace) {
                        continue;
                    }
                    let Some(last) = watching.get(&event.job_id).copied() else {
                        continue;
                    };
                    let Some(observed) = transition::from_event(&event) else {
                        continue;
                    };
                    // Emitted while the snapshot was being read: it may be
                    // older than what the snapshot already reported.
                    if seq <= boundary && observed.rank <= last {
                        continue;
                    }
                    if let Err(stop) = advance(session, &mut watching, observed).await {
                        return stop;
                    }
                }
            }
            Wake::Round => {
                let round = Arc::clone(&rounds.borrow_and_update());
                let ids: Vec<String> = watching.keys().cloned().collect();
                for id in ids {
                    let last = watching[&id];
                    match round.reading(namespace, &id) {
                        Reading::Unasked => {}
                        Reading::Gone => {
                            watching.remove(&id);
                            if let Err(stop) =
                                session.send(Item::NotFoundJobId(id), String::new()).await
                            {
                                return stop;
                            }
                        }
                        Reading::Row(job) => {
                            let observed = transition::from_row(job);
                            // Only forward: a row read before a live event
                            // this stream already sent must not undo it.
                            if observed.rank > last {
                                if let Err(stop) = advance(session, &mut watching, observed).await {
                                    return stop;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Send `observed` and record where its job now stands.
async fn advance(
    session: &Session,
    watching: &mut HashMap<String, Rank>,
    observed: Observed,
) -> Result<(), Stop> {
    let id = observed.job_id().to_string();
    if observed.terminal() {
        watching.remove(&id);
    } else {
        watching.insert(id, observed.rank);
    }
    send_transition(session, observed, String::new()).await
}

async fn send_transition(
    session: &Session,
    observed: Observed,
    cursor: String,
) -> Result<(), Stop> {
    session
        .send(Item::Transition(observed.transition), cursor)
        .await
}

/// Watch every job in `queue`, after position `from`, until the client leaves.
pub async fn watch_queue(
    ctx: Shared,
    session: Session,
    namespace: Arc<str>,
    queue: String,
    from: Seq,
) {
    let stop = follow_queue(&ctx, &session, &namespace, &queue, from).await;
    session.finish(stop);
}

async fn follow_queue(
    ctx: &Shared,
    session: &Session,
    namespace: &str,
    queue: &str,
    from: Seq,
) -> Stop {
    let mut head = ctx.feed.subscribe();
    let mut position = from;
    let instance = ctx.feed.instance();
    // Without it, a stream lost before its first transition leaves the client
    // no cursor, and reopening from the newer head skips the gap.
    if let Err(stop) = session.checkpoint(cursor::encode(instance, from)).await {
        return stop;
    }
    loop {
        let events = match ctx.feed.read_after(position) {
            Ok(events) => events,
            Err(_) => return Stop::End(Some(WireError::watch_overflow().into())),
        };
        for (seq, event) in events {
            position = seq;
            if event.namespace.as_deref() != Some(namespace) || event.queue != queue {
                continue;
            }
            let Some(observed) = transition::from_event(&event) else {
                continue;
            };
            if let Err(stop) =
                send_transition(session, observed, cursor::encode(instance, seq)).await
            {
                return stop;
            }
        }
        if let Wake::Stop(stop) = wake(session, &ctx.shutdown, &mut head, None).await {
            return stop;
        }
    }
}

#[cfg(test)]
mod tests {
    use flexiq_core::storage::sqlite::SqliteStorage;
    use flexiq_core::{EventTap, EventType, JobEvent};
    use tokio_stream::StreamExt;

    use super::*;
    use crate::grpc::producer::watch::quota::Quota;
    use crate::grpc::status::reason;

    fn shared(buffer: usize, stall: Duration) -> Shared {
        Shared {
            storage: StorageBackend::Sqlite(SqliteStorage::new(":memory:").expect("in-memory")),
            feed: Arc::new(WatchFeed::new(buffer)),
            reconciler: Arc::new(Reconciler::new()),
            shutdown: Shutdown::default(),
            stall,
        }
    }

    fn slot() -> Slot {
        Quota::new(0).acquire(&Arc::from("tok")).expect("unbounded")
    }

    fn enqueued(feed: &WatchFeed, id: &str) {
        feed.observe(&JobEvent::new(
            EventType::JobEnqueued,
            id,
            Some("ns".into()),
            "q",
            "t",
        ));
    }

    /// Read `outlet` to its end: every item's job id, then the final status's
    /// reason, if any.
    async fn drain(mut outlet: Outlet) -> (Vec<String>, Option<String>) {
        let mut ids = Vec::new();
        while let Some(item) = outlet.next().await {
            match item {
                Ok(response) => match response.item {
                    Some(Item::Transition(t)) => ids.push(t.job_id),
                    // A queue watch's opening checkpoint.
                    None => assert!(!response.cursor.is_empty()),
                    other => panic!("unexpected item {other:?}"),
                },
                Err(status) => {
                    let reason = tonic_types::StatusExt::get_error_details(&status)
                        .error_info()
                        .map(|info| info.reason.clone());
                    return (ids, reason);
                }
            }
        }
        (ids, None)
    }

    #[tokio::test]
    async fn a_queue_watch_behind_the_window_ends_with_overflow() {
        let ctx = shared(2, Duration::from_secs(5));
        for id in ["a", "b", "c"] {
            enqueued(&ctx.feed, id);
        }
        let (session, outlet) = open(slot(), ctx.stall);
        watch_queue(ctx, session, "ns".into(), "q".into(), 0).await;
        let (ids, reason) = drain(outlet).await;
        assert!(ids.is_empty());
        assert_eq!(reason.as_deref(), Some(reason::WATCH_OVERFLOW));
    }

    #[tokio::test]
    async fn a_client_that_stops_reading_is_ended_after_the_stall() {
        let ctx = shared(1_000, Duration::from_millis(50));
        for i in 0..(CHANNEL + 5) {
            enqueued(&ctx.feed, &i.to_string());
        }
        let (session, outlet) = open(slot(), ctx.stall);
        // Nobody reads while the task runs, so its sends back up.
        watch_queue(ctx, session, "ns".into(), "q".into(), 0).await;
        let (ids, reason) = drain(outlet).await;
        // The channel held the opening checkpoint and then transitions.
        assert_eq!(ids.len(), CHANNEL - 1, "what was queued is still delivered");
        assert_eq!(reason.as_deref(), Some(reason::WATCH_OVERFLOW));
    }

    #[tokio::test]
    async fn a_queue_watch_sees_only_its_namespace_and_queue_and_ends_on_shutdown() {
        let ctx = shared(16, Duration::from_secs(5));
        let feed = Arc::clone(&ctx.feed);
        let shutdown = ctx.shutdown.clone();
        let (session, outlet) = open(slot(), ctx.stall);
        let task = tokio::spawn(watch_queue(ctx, session, "ns".into(), "q".into(), 0));
        enqueued(&feed, "mine");
        feed.observe(&JobEvent::new(
            EventType::JobEnqueued,
            "other-ns",
            Some("x".into()),
            "q",
            "t",
        ));
        feed.observe(&JobEvent::new(
            EventType::JobEnqueued,
            "other-q",
            Some("ns".into()),
            "r",
            "t",
        ));
        enqueued(&feed, "mine-too");
        let mut outlet = outlet;
        let opening = outlet.next().await.expect("an item").expect("not an error");
        assert_eq!(opening.item, None, "a queue watch opens with a checkpoint");
        assert_eq!(opening.cursor, cursor::encode(feed.instance(), 0));
        for expected in ["mine", "mine-too"] {
            let item = outlet.next().await.expect("an item").expect("not an error");
            match item.item {
                Some(Item::Transition(t)) => assert_eq!(t.job_id, expected),
                other => panic!("unexpected item {other:?}"),
            }
            assert!(!item.cursor.is_empty(), "a queue watch hands out cursors");
        }
        shutdown.trigger();
        task.await.expect("the task must not panic");
        let (ids, reason) = drain(outlet).await;
        assert!(ids.is_empty());
        assert_eq!(reason.as_deref(), Some(reason::SHUTTING_DOWN));
    }
}
