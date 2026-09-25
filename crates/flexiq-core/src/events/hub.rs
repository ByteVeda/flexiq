//! [`EventHub`]: fan-out of job events to bounded, per-sink delivery threads.

use std::fmt;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError, RwLock, TryLockError};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use log::warn;

use super::config::{EventsConfig, EventsConfigError, Filter, SinkConfig};
use super::event::JobEvent;
use super::sink::{build_backend, DeliveryResult, SinkBackend};
use crate::resilience::retry::full_jitter;

/// First retry's backoff ceiling; doubles per attempt.
const BACKOFF_BASE_MS: u64 = 100;
/// Largest backoff ceiling, however many attempts have failed.
const BACKOFF_CAP_MS: u64 = 10_000;

/// Routes job events to the configured sinks.
///
/// Each sink gets its own bounded buffer and OS thread, so a slow or wedged
/// destination can only lose its own events: it never blocks [`emit`], the
/// host runtime, or another sink.
///
/// [`emit`]: EventHub::emit
pub struct EventHub {
    source: String,
    sinks: Vec<Sink>,
    wants_payload: bool,
    lifecycle: Arc<Lifecycle>,
    shut_down: AtomicBool,
}

/// One sink's counters at one moment.
///
/// `#[non_exhaustive]`: a new drop reason adds a counter.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct SinkStats {
    /// The sink's configured name.
    pub name: String,
    /// The sink's `kind`, e.g. `http`.
    pub kind: &'static str,
    /// Events the destination accepted.
    pub delivered: u64,
    /// Events dropped because the sink's buffer was full.
    pub dropped_buffer_full: u64,
    /// Events the destination refused outright.
    pub dropped_rejected: u64,
    /// Events dropped after every delivery attempt failed.
    pub dropped_failed: u64,
    /// Events dropped because the hub was shutting down.
    pub dropped_shutdown: u64,
    /// Events accepted but not yet delivered or dropped. Approximate while
    /// events are in motion: each counter is read separately, not as one
    /// snapshot.
    pub queued: u64,
}

impl EventHub {
    /// Validate `config`, build every sink's backend and start its thread.
    ///
    /// Fails on a kind this build lacks ([`EventsConfigError::NotCompiled`]),
    /// so a config that cannot deliver is refused at boot, not at first event.
    pub fn start(config: EventsConfig) -> Result<Self, EventsConfigError> {
        config.validate()?;
        let mut sinks = Vec::with_capacity(config.sinks.len());
        for sink in config.sinks {
            let backend = build_backend(&sink, &config.source)?;
            sinks.push((sink, backend));
        }
        Self::with_backends(config.source, sinks)
    }

    /// Parse a configuration document and [`start`](Self::start) it.
    pub fn from_json(json: &str) -> Result<Self, EventsConfigError> {
        Self::start(EventsConfig::parse(json)?)
    }

    /// Start a hub over already-built backends.
    pub(crate) fn with_backends(
        source: String,
        backends: Vec<(SinkConfig, Box<dyn SinkBackend>)>,
    ) -> Result<Self, EventsConfigError> {
        let lifecycle = Arc::new(Lifecycle::default());
        // Built up in place, so a spawn failure part-way closes the sinks
        // already started when `hub` drops.
        let mut hub = Self {
            source,
            sinks: Vec::with_capacity(backends.len()),
            wants_payload: false,
            lifecycle: Arc::clone(&lifecycle),
            shut_down: AtomicBool::new(false),
        };
        for (config, backend) in backends {
            if config.include_payload() {
                warn!("events sink '{}' sends job payloads", config.name());
                hub.wants_payload = true;
            }
            hub.sinks.push(Sink::spawn(&config, backend, &lifecycle)?);
        }
        Ok(hub)
    }

    /// The CloudEvents `source` this hub's sinks stamp.
    pub fn source(&self) -> &str {
        &self.source
    }

    /// Hand an event to every sink whose filter admits it.
    ///
    /// Never blocks and never does I/O: a sink whose buffer is full drops the
    /// event and counts it. Sinks that do not send payloads get one shared,
    /// payload-free copy, so the bytes never reach their thread.
    pub fn emit(&self, event: JobEvent) {
        let full = Arc::new(event);
        let mut stripped: Option<Arc<JobEvent>> = None;
        for sink in &self.sinks {
            if !sink.filter.admits(&full) {
                continue;
            }
            let event = if sink.include_payload || full.payload.is_none() {
                Arc::clone(&full)
            } else {
                Arc::clone(stripped.get_or_insert_with(|| Arc::new(full.without_payload())))
            };
            sink.offer(event);
        }
    }

    /// Whether any sink sends payloads, so an emitter knows whether reading
    /// them is worth it.
    pub fn wants_payload(&self) -> bool {
        self.wants_payload
    }

    /// Every sink's counters, in configuration order.
    pub fn stats(&self) -> Vec<SinkStats> {
        self.sinks.iter().map(Sink::stats).collect()
    }

    /// The hub's metrics in Prometheus text exposition format.
    ///
    /// Every drop reason is rendered for every sink, zero included, so a rate
    /// query has a series from boot rather than from the first drop.
    pub fn render_prometheus(&self) -> String {
        let stats = self.stats();
        let mut body = String::from(
            "# HELP flexiq_events_delivered_total Events a sink delivered.\n\
             # TYPE flexiq_events_delivered_total counter\n",
        );
        for s in &stats {
            body.push_str(&format!(
                "flexiq_events_delivered_total{{sink=\"{}\"}} {}\n",
                escape_label(&s.name),
                s.delivered
            ));
        }
        body.push_str(
            "# HELP flexiq_events_dropped_total Events a sink dropped, by reason.\n\
             # TYPE flexiq_events_dropped_total counter\n",
        );
        for s in &stats {
            for (reason, count) in [
                ("buffer_full", s.dropped_buffer_full),
                ("rejected", s.dropped_rejected),
                ("failed", s.dropped_failed),
                ("shutdown", s.dropped_shutdown),
            ] {
                body.push_str(&format!(
                    "flexiq_events_dropped_total{{sink=\"{}\",reason=\"{reason}\"}} {count}\n",
                    escape_label(&s.name)
                ));
            }
        }
        body.push_str(
            "# HELP flexiq_events_queued Events a sink accepted but has not yet delivered or dropped.\n\
             # TYPE flexiq_events_queued gauge\n",
        );
        for s in &stats {
            body.push_str(&format!(
                "flexiq_events_queued{{sink=\"{}\"}} {}\n",
                escape_label(&s.name),
                s.queued
            ));
        }
        body
    }

    /// Stop accepting events and deliver what is buffered within `budget`.
    ///
    /// No attempt starts and no backoff sleeps past the deadline. Whatever is
    /// still buffered or in flight then is counted as dropped for `shutdown`,
    /// and a sink thread still blocked in its destination is left behind
    /// rather than waited on. Idempotent: a second call, even one racing the
    /// first, returns at once without waiting for the first call's drain.
    pub fn shutdown(&self, budget: Duration) {
        if self.shut_down.swap(true, Ordering::SeqCst) {
            return;
        }
        let deadline = Instant::now() + budget;
        // Close first: a thread that sees the deadline drains a finite buffer.
        self.close();
        self.lifecycle.set_deadline(deadline);
        self.lifecycle.wait_idle(deadline);
        for sink in &self.sinks {
            sink.abandon();
        }
    }

    fn close(&self) {
        for sink in &self.sinks {
            *sink.sender.write().unwrap_or_else(PoisonError::into_inner) = None;
        }
    }
}

impl Drop for EventHub {
    /// Closes the channels without waiting: each sink thread delivers what it
    /// already holds, then exits. Without a prior [`shutdown`](Self::shutdown)
    /// no deadline is set, so nothing bounds how long a thread keeps retrying
    /// that remainder.
    fn drop(&mut self) {
        self.close();
    }
}

impl fmt::Debug for EventHub {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EventHub")
            .field("source", &self.source)
            .field(
                "sinks",
                &self.sinks.iter().map(|s| &s.name).collect::<Vec<_>>(),
            )
            .finish_non_exhaustive()
    }
}

/// The hub's half of one sink.
struct Sink {
    name: String,
    kind: &'static str,
    filter: Filter,
    include_payload: bool,
    /// `None` once closed. Behind a lock only so shutdown can close it through
    /// `&self`; `emit` never waits on it.
    sender: RwLock<Option<SyncSender<Arc<JobEvent>>>>,
    counters: Arc<Counters>,
    thread: Mutex<Option<JoinHandle<()>>>,
}

impl Sink {
    fn spawn(
        config: &SinkConfig,
        backend: Box<dyn SinkBackend>,
        lifecycle: &Arc<Lifecycle>,
    ) -> Result<Self, EventsConfigError> {
        let delivery = config.delivery();
        // `max(1)`: a zero-capacity channel is a rendezvous, which would make
        // every `try_send` fail while the thread is busy.
        let (sender, receiver) = sync_channel(delivery.buffer.max(1));
        let counters = Arc::new(Counters::default());
        let worker = Worker {
            name: config.name().to_string(),
            backend,
            receiver,
            max_batch: delivery.max_batch.max(1),
            max_attempts: delivery.max_attempts.max(1),
            counters: Arc::clone(&counters),
            lifecycle: Arc::clone(lifecycle),
            _running: Running::enter(lifecycle),
        };
        // A spawn failure drops the closure, and with it the `Running` guard.
        let thread = std::thread::Builder::new()
            .name("flexiq-events".into())
            .spawn(move || worker.run())
            .map_err(|e| EventsConfigError::Sink {
                sink: config.name().to_string(),
                message: format!("could not start the delivery thread: {e}"),
            })?;
        Ok(Self {
            name: config.name().to_string(),
            kind: config.kind(),
            filter: config.filter().clone(),
            include_payload: config.include_payload(),
            sender: RwLock::new(Some(sender)),
            counters,
            thread: Mutex::new(Some(thread)),
        })
    }

    fn offer(&self, event: Arc<JobEvent>) {
        let counters = &self.counters;
        // `try_read`: the only writer is close, so a held lock means shutdown.
        let guard = match self.sender.try_read() {
            Ok(guard) => guard,
            Err(TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
            Err(TryLockError::WouldBlock) => {
                counters.shutdown.fetch_add(1, Ordering::Relaxed);
                return;
            }
        };
        let Some(sender) = guard.as_ref() else {
            counters.shutdown.fetch_add(1, Ordering::Relaxed);
            return;
        };
        // Counted before the send, so the thread can never settle an event
        // `queued` does not yet include.
        counters.queued.fetch_add(1, Ordering::Relaxed);
        match sender.try_send(event) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                counters.queued.fetch_sub(1, Ordering::Relaxed);
                counters.buffer_full.fetch_add(1, Ordering::Relaxed);
            }
            // The thread is gone (a backend panic is caught, so this is a
            // panic in the hub's own loop); nothing will drain it.
            Err(TrySendError::Disconnected(_)) => {
                counters.queued.fetch_sub(1, Ordering::Relaxed);
                counters.shutdown.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Past the deadline: count what the thread still holds, and join it only
    /// if it has already finished.
    fn abandon(&self) {
        let abandoned = self.counters.abandon();
        if abandoned > 0 {
            warn!(
                "events sink '{}' dropped {abandoned} event(s): shutdown deadline reached",
                self.name
            );
        }
        let handle = lock(&self.thread).take();
        if let Some(handle) = handle {
            if handle.is_finished() && handle.join().is_err() {
                warn!("events sink '{}' delivery thread panicked", self.name);
            }
        }
    }

    fn stats(&self) -> SinkStats {
        let c = &self.counters;
        SinkStats {
            name: self.name.clone(),
            kind: self.kind,
            delivered: c.delivered.load(Ordering::Relaxed),
            dropped_buffer_full: c.buffer_full.load(Ordering::Relaxed),
            dropped_rejected: c.rejected.load(Ordering::Relaxed),
            dropped_failed: c.failed.load(Ordering::Relaxed),
            dropped_shutdown: c.shutdown.load(Ordering::Relaxed),
            queued: c.queued.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum Outcome {
    Delivered,
    Rejected,
    Failed,
    Shutdown,
}

impl Outcome {
    fn reason(self) -> &'static str {
        match self {
            Self::Delivered => "delivered",
            Self::Rejected => "rejected",
            Self::Failed => "failed",
            Self::Shutdown => "shutdown",
        }
    }
}

#[derive(Default)]
struct Counters {
    delivered: AtomicU64,
    buffer_full: AtomicU64,
    rejected: AtomicU64,
    failed: AtomicU64,
    shutdown: AtomicU64,
    queued: AtomicU64,
    /// Set once shutdown has counted this sink's remainder. Held while
    /// settling too, so an event is counted by the thread or by shutdown,
    /// never both.
    abandoned: Mutex<bool>,
}

impl Counters {
    /// Move `n` events from `queued` to `outcome`. False, counting nothing,
    /// once shutdown has already counted them.
    fn settle(&self, outcome: Outcome, n: u64) -> bool {
        let abandoned = lock(&self.abandoned);
        if *abandoned {
            return false;
        }
        self.queued.fetch_sub(n, Ordering::Relaxed);
        let counter = match outcome {
            Outcome::Delivered => &self.delivered,
            Outcome::Rejected => &self.rejected,
            Outcome::Failed => &self.failed,
            Outcome::Shutdown => &self.shutdown,
        };
        counter.fetch_add(n, Ordering::Relaxed);
        true
    }

    /// Count everything still queued as dropped for shutdown; returns how many.
    fn abandon(&self) -> u64 {
        let mut abandoned = lock(&self.abandoned);
        if *abandoned {
            return 0;
        }
        *abandoned = true;
        let n = self.queued.swap(0, Ordering::Relaxed);
        self.shutdown.fetch_add(n, Ordering::Relaxed);
        n
    }

    fn is_abandoned(&self) -> bool {
        *lock(&self.abandoned)
    }
}

/// The sink thread's half: drains the buffer into the backend.
struct Worker {
    name: String,
    backend: Box<dyn SinkBackend>,
    receiver: Receiver<Arc<JobEvent>>,
    max_batch: usize,
    max_attempts: u32,
    counters: Arc<Counters>,
    lifecycle: Arc<Lifecycle>,
    _running: Running,
}

impl Worker {
    fn run(mut self) {
        // Blocks for the first event; ends once the hub closes the channel
        // and the buffer is empty.
        while let Ok(first) = self.receiver.recv() {
            let mut batch = Vec::with_capacity(self.max_batch);
            batch.push(first);
            while batch.len() < self.max_batch {
                match self.receiver.try_recv() {
                    Ok(event) => batch.push(event),
                    Err(_) => break,
                }
            }
            if !self.send(&batch) || self.counters.is_abandoned() {
                self.drain_at_shutdown();
                return;
            }
        }
    }

    /// Deliver one batch, retrying transient failures. False when the
    /// shutdown deadline stopped it.
    fn send(&mut self, batch: &[Arc<JobEvent>]) -> bool {
        let n = batch.len() as u64;
        for attempt in 1..=self.max_attempts {
            if self.lifecycle.past_deadline() {
                self.drop_batch(Outcome::Shutdown, n, "shutdown deadline reached");
                return false;
            }
            match self.attempt(batch) {
                DeliveryResult::Delivered => {
                    self.counters.settle(Outcome::Delivered, n);
                    return true;
                }
                DeliveryResult::Reject(why) => {
                    self.drop_batch(Outcome::Rejected, n, &why);
                    return true;
                }
                DeliveryResult::Retry(why) if attempt == self.max_attempts => {
                    let detail = format!("{attempt} attempt(s), last: {why}");
                    self.drop_batch(Outcome::Failed, n, &detail);
                    return true;
                }
                DeliveryResult::Retry(why) => {
                    if !self.lifecycle.pause(backoff(attempt)) {
                        let detail = format!("shutdown deadline reached while retrying: {why}");
                        self.drop_batch(Outcome::Shutdown, n, &detail);
                        return false;
                    }
                }
            }
        }
        true
    }

    /// One delivery attempt, with a panicking backend read as a rejection.
    ///
    /// Caught so one bad batch cannot kill the sink: an unwinding thread would
    /// drop the receiver, freeze `queued` and misfile every later event as a
    /// shutdown drop. Rejected, not retried, because a panic on a batch is most
    /// likely to repeat on it. The panic message is not logged: the backend
    /// may have formatted an event or a credential into it.
    fn attempt(&mut self, batch: &[Arc<JobEvent>]) -> DeliveryResult {
        let backend = &mut self.backend;
        catch_unwind(AssertUnwindSafe(|| backend.deliver(batch)))
            .unwrap_or_else(|_| DeliveryResult::Reject("the sink backend panicked".into()))
    }

    /// Count everything left in the (closed) buffer as dropped for shutdown.
    fn drain_at_shutdown(&mut self) {
        let left = self.receiver.try_iter().count() as u64;
        if left > 0 {
            self.drop_batch(Outcome::Shutdown, left, "shutdown deadline reached");
        }
    }

    /// Count a dropped batch and log it once. Names the sink and the reason,
    /// never an event's contents.
    fn drop_batch(&self, outcome: Outcome, n: u64, detail: &str) {
        if self.counters.settle(outcome, n) {
            warn!(
                "events sink '{}' dropped {n} event(s) as {}: {detail}",
                self.name,
                outcome.reason()
            );
        }
    }
}

/// Capped exponential backoff with full jitter: uniform in
/// `[0, min(cap, base * 2^(attempt-1))]`.
fn backoff(attempt: u32) -> Duration {
    let ceiling = BACKOFF_BASE_MS
        .saturating_mul(1u64 << attempt.saturating_sub(1).min(32))
        .min(BACKOFF_CAP_MS);
    let ms = full_jitter(i64::try_from(ceiling).unwrap_or(i64::MAX));
    Duration::from_millis(u64::try_from(ms).unwrap_or(0))
}

/// Shutdown coordination shared by the hub and every sink thread.
#[derive(Default)]
struct Lifecycle {
    state: Mutex<LifecycleState>,
    changed: Condvar,
}

#[derive(Default)]
struct LifecycleState {
    deadline: Option<Instant>,
    running: usize,
}

impl Lifecycle {
    fn set_deadline(&self, deadline: Instant) {
        lock(&self.state).deadline = Some(deadline);
        self.changed.notify_all();
    }

    fn past_deadline(&self) -> bool {
        lock(&self.state)
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
    }

    /// Sleep for `pause`, cut short by the deadline. False when it was.
    fn pause(&self, pause: Duration) -> bool {
        let wake = Instant::now() + pause;
        let mut state = lock(&self.state);
        loop {
            let now = Instant::now();
            let until = match state.deadline {
                Some(deadline) if deadline < wake => deadline,
                _ => wake,
            };
            if now >= until {
                return state.deadline.is_none_or(|deadline| now < deadline);
            }
            state = self
                .changed
                .wait_timeout(state, until - now)
                .map_or_else(|e| e.into_inner().0, |(guard, _)| guard);
        }
    }

    /// Wait until every sink thread has exited or `deadline` passes.
    fn wait_idle(&self, deadline: Instant) {
        let mut state = lock(&self.state);
        while state.running > 0 {
            let now = Instant::now();
            if now >= deadline {
                return;
            }
            state = self
                .changed
                .wait_timeout(state, deadline - now)
                .map_or_else(|e| e.into_inner().0, |(guard, _)| guard);
        }
    }
}

/// Counts a sink thread as running for as long as it lives, panics included.
struct Running(Arc<Lifecycle>);

impl Running {
    fn enter(lifecycle: &Arc<Lifecycle>) -> Self {
        lock(&lifecycle.state).running += 1;
        Self(Arc::clone(lifecycle))
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        lock(&self.0.state).running -= 1;
        self.0.changed.notify_all();
    }
}

/// A poisoned lock only means another thread panicked holding it; the
/// counters and flags behind these locks are still coherent.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Escape a Prometheus label value: backslash, quote and newline.
fn escape_label(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

/// The recording fake below, for tests outside this module that need a hub.
#[cfg(test)]
pub(crate) mod test_support {
    pub(crate) use super::tests::Attempts;
    use super::tests::{fake, hub, sink};
    use super::*;

    /// A started hub over one sink that records every delivered event.
    /// `extra` is appended to the sink's config, e.g. `,"include_payload":true`.
    pub(crate) fn recording_hub(extra: &str) -> (Arc<EventHub>, Attempts) {
        let (backend, attempts) = fake(vec![], DeliveryResult::Delivered);
        (Arc::new(hub(vec![(sink("rec", extra), backend)])), attempts)
    }

    /// Shut `hub` down, then everything it delivered, in delivery order.
    pub(crate) fn delivered(hub: &EventHub, attempts: &Attempts) -> Vec<JobEvent> {
        hub.shutdown(Duration::from_secs(10));
        lock(attempts)
            .iter()
            .flatten()
            .map(|event| JobEvent::clone(event))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::mpsc;

    use super::*;
    use crate::events::event::EventType;

    const WAIT: Duration = Duration::from_secs(10);

    /// Opens once, then stays open.
    #[derive(Clone, Default)]
    struct Gate(Arc<(Mutex<bool>, Condvar)>);

    impl Gate {
        fn open(&self) {
            *lock(&self.0 .0) = true;
            self.0 .1.notify_all();
        }
        fn wait(&self) {
            let mut open = lock(&self.0 .0);
            while !*open {
                open = self.0 .1.wait(open).unwrap_or_else(PoisonError::into_inner);
            }
        }
    }

    /// Records every attempt's batch and answers from a script, then with
    /// `fallback`. With a gate, every attempt waits for it to open.
    pub(super) struct Fake {
        attempts: Arc<Mutex<Vec<Vec<Arc<JobEvent>>>>>,
        script: VecDeque<DeliveryResult>,
        fallback: DeliveryResult,
        gate: Option<(Gate, mpsc::Sender<()>)>,
    }

    impl SinkBackend for Fake {
        fn deliver(&mut self, batch: &[Arc<JobEvent>]) -> DeliveryResult {
            if let Some((gate, entered)) = &self.gate {
                let _ = entered.send(());
                gate.wait();
            }
            lock(&self.attempts).push(batch.to_vec());
            self.script.pop_front().unwrap_or(self.fallback.clone())
        }
    }

    pub(crate) type Attempts = Arc<Mutex<Vec<Vec<Arc<JobEvent>>>>>;

    pub(super) fn fake(
        script: Vec<DeliveryResult>,
        fallback: DeliveryResult,
    ) -> (Box<Fake>, Attempts) {
        let attempts = Attempts::default();
        let backend = Fake {
            attempts: Arc::clone(&attempts),
            script: script.into(),
            fallback,
            gate: None,
        };
        (Box::new(backend), attempts)
    }

    fn gated() -> (Box<Fake>, Attempts, Gate, mpsc::Receiver<()>) {
        let (mut backend, attempts) = fake(vec![], DeliveryResult::Delivered);
        let gate = Gate::default();
        let (entered, entries) = mpsc::channel();
        backend.gate = Some((gate.clone(), entered));
        (backend, attempts, gate, entries)
    }

    /// A Redis Streams sink config: the kind needs no URL to parse.
    pub(super) fn sink(name: &str, extra: &str) -> SinkConfig {
        let doc = format!(
            r#"{{"sinks":[{{"kind":"redis_streams","name":"{name}","url_env":"U","stream":"s"{extra}}}]}}"#
        );
        EventsConfig::parse(&doc).unwrap().sinks.remove(0)
    }

    pub(super) fn hub(sinks: Vec<(SinkConfig, Box<dyn SinkBackend>)>) -> EventHub {
        EventHub::with_backends("/flexiq".into(), sinks).unwrap()
    }

    fn event(event_type: EventType, job_id: &str, queue: &str) -> JobEvent {
        JobEvent::new(event_type, job_id, None, queue, "task")
    }

    fn ids(attempts: &Attempts) -> Vec<String> {
        lock(attempts)
            .iter()
            .flatten()
            .map(|e| e.job_id.clone())
            .collect()
    }

    #[test]
    fn filter_routes_each_event_to_the_admitting_sinks_only() {
        let (dead_only, dead_seen) = fake(vec![], DeliveryResult::Delivered);
        let (q2_only, q2_seen) = fake(vec![], DeliveryResult::Delivered);
        let hub = hub(vec![
            (
                sink("dead", r#","filter":{"types":["job.dead"]}"#),
                dead_only,
            ),
            (sink("q2", r#","filter":{"queues":["q2"]}"#), q2_only),
        ]);
        hub.emit(event(EventType::JobDead, "a", "q1"));
        hub.emit(event(EventType::JobCompleted, "b", "q2"));
        hub.emit(event(EventType::JobDead, "c", "q2"));
        hub.emit(event(EventType::JobStarted, "d", "q1"));
        hub.shutdown(WAIT);
        assert_eq!(ids(&dead_seen), ["a", "c"]);
        assert_eq!(ids(&q2_seen), ["b", "c"]);
        let stats = hub.stats();
        assert_eq!((stats[0].delivered, stats[1].delivered), (2, 2));
        assert_eq!(stats[0].kind, "redis_streams");
    }

    #[test]
    fn a_full_buffer_drops_and_counts_without_blocking() {
        let (backend, attempts, gate, entries) = gated();
        let hub = hub(vec![(sink("s", r#","delivery":{"buffer":2}"#), backend)]);
        hub.emit(event(EventType::JobStarted, "0", "q"));
        entries.recv_timeout(WAIT).unwrap();
        let started = Instant::now();
        for i in 1..=5 {
            hub.emit(event(EventType::JobStarted, &i.to_string(), "q"));
        }
        assert!(started.elapsed() < Duration::from_secs(1));
        let stats = &hub.stats()[0];
        assert_eq!((stats.dropped_buffer_full, stats.queued), (3, 3));
        gate.open();
        hub.shutdown(WAIT);
        assert_eq!(ids(&attempts), ["0", "1", "2"]);
        let stats = &hub.stats()[0];
        assert_eq!((stats.delivered, stats.queued), (3, 0));
    }

    #[test]
    fn a_batch_drains_up_to_max_batch() {
        let (backend, attempts, gate, entries) = gated();
        let hub = hub(vec![(sink("s", r#","delivery":{"max_batch":3}"#), backend)]);
        hub.emit(event(EventType::JobStarted, "0", "q"));
        entries.recv_timeout(WAIT).unwrap();
        for i in 1..=4 {
            hub.emit(event(EventType::JobStarted, &i.to_string(), "q"));
        }
        gate.open();
        hub.shutdown(WAIT);
        let sizes: Vec<usize> = lock(&attempts).iter().map(Vec::len).collect();
        assert_eq!(sizes, [1, 3, 1]);
        assert_eq!(hub.stats()[0].delivered, 5);
    }

    #[test]
    fn retry_then_success_counts_one_delivery() {
        let (backend, attempts) = fake(
            vec![DeliveryResult::Retry("503".into())],
            DeliveryResult::Delivered,
        );
        let hub = hub(vec![(sink("s", ""), backend)]);
        hub.emit(event(EventType::JobDead, "a", "q"));
        hub.shutdown(WAIT);
        assert_eq!(lock(&attempts).len(), 2);
        let stats = &hub.stats()[0];
        assert_eq!((stats.delivered, stats.dropped_failed), (1, 0));
    }

    #[test]
    fn reject_drops_the_batch_without_retrying() {
        let (backend, attempts) = fake(vec![], DeliveryResult::Reject("400".into()));
        let hub = hub(vec![(sink("s", ""), backend)]);
        hub.emit(event(EventType::JobDead, "a", "q"));
        hub.shutdown(WAIT);
        assert_eq!(lock(&attempts).len(), 1);
        let stats = &hub.stats()[0];
        assert_eq!(
            (stats.delivered, stats.dropped_rejected, stats.queued),
            (0, 1, 0)
        );
    }

    /// Panics on its first attempt, delivers after.
    struct PanicsOnce {
        panicked: bool,
        delivered: Attempts,
    }

    impl SinkBackend for PanicsOnce {
        fn deliver(&mut self, batch: &[Arc<JobEvent>]) -> DeliveryResult {
            if !self.panicked {
                self.panicked = true;
                panic!("backend blew up");
            }
            lock(&self.delivered).push(batch.to_vec());
            DeliveryResult::Delivered
        }
    }

    #[test]
    fn a_panicking_backend_rejects_the_batch_and_the_sink_lives_on() {
        let delivered = Attempts::default();
        let backend = PanicsOnce {
            panicked: false,
            delivered: Arc::clone(&delivered),
        };
        let hub = hub(vec![(sink("s", ""), Box::new(backend))]);
        hub.emit(event(EventType::JobDead, "a", "q"));
        hub.emit(event(EventType::JobDead, "b", "q"));
        hub.shutdown(WAIT);
        assert_eq!(ids(&delivered), ["b"]);
        let stats = &hub.stats()[0];
        assert_eq!(
            (
                stats.delivered,
                stats.dropped_rejected,
                stats.dropped_shutdown,
                stats.queued
            ),
            (1, 1, 0, 0)
        );
    }

    #[test]
    fn exhausted_retries_count_failed() {
        let (backend, attempts) = fake(vec![], DeliveryResult::Retry("503".into()));
        let hub = hub(vec![(
            sink("s", r#","delivery":{"max_attempts":3}"#),
            backend,
        )]);
        hub.emit(event(EventType::JobDead, "a", "q"));
        hub.shutdown(WAIT);
        assert_eq!(lock(&attempts).len(), 3);
        let stats = &hub.stats()[0];
        assert_eq!((stats.dropped_failed, stats.queued), (1, 0));
    }

    #[test]
    fn a_payload_free_sink_never_sees_the_payload() {
        let (plain_a, seen_a) = fake(vec![], DeliveryResult::Delivered);
        let (plain_b, seen_b) = fake(vec![], DeliveryResult::Delivered);
        let (with_payload, seen_p) = fake(vec![], DeliveryResult::Delivered);
        let hub = hub(vec![
            (sink("a", ""), plain_a),
            (sink("b", ""), plain_b),
            (sink("p", r#","include_payload":true"#), with_payload),
        ]);
        assert!(hub.wants_payload());
        let mut e = event(EventType::JobEnqueued, "j", "q");
        e.payload = Some(vec![1, 2, 3]);
        hub.emit(e);
        hub.shutdown(WAIT);
        let a = Arc::clone(&lock(&seen_a)[0][0]);
        let b = Arc::clone(&lock(&seen_b)[0][0]);
        assert_eq!(a.payload, None);
        assert!(Arc::ptr_eq(&a, &b), "payload-free sinks share one copy");
        // Even an encoder that asked for the payload could not find one.
        assert!(a.to_cloudevent("/flexiq", true)["data"]
            .get("payload_base64")
            .is_none());
        assert_eq!(lock(&seen_p)[0][0].payload, Some(vec![1, 2, 3]));
    }

    #[test]
    fn wants_payload_is_false_without_an_opted_in_sink() {
        let (backend, _) = fake(vec![], DeliveryResult::Delivered);
        assert!(!hub(vec![(sink("s", ""), backend)]).wants_payload());
    }

    #[test]
    fn shutdown_delivers_everything_within_budget() {
        let (backend, attempts) = fake(vec![], DeliveryResult::Delivered);
        let hub = hub(vec![(sink("s", ""), backend)]);
        for i in 0..5 {
            hub.emit(event(EventType::JobStarted, &i.to_string(), "q"));
        }
        hub.shutdown(WAIT);
        assert_eq!(ids(&attempts).len(), 5);
        assert_eq!(hub.stats()[0].delivered, 5);
    }

    #[test]
    fn the_deadline_cuts_a_backoff_short_and_counts_the_rest() {
        let (backend, _) = fake(vec![], DeliveryResult::Retry("503".into()));
        let hub = hub(vec![(
            sink("s", r#","delivery":{"max_attempts":1000}"#),
            backend,
        )]);
        for i in 0..3 {
            hub.emit(event(EventType::JobStarted, &i.to_string(), "q"));
        }
        let started = Instant::now();
        hub.shutdown(Duration::from_millis(150));
        assert!(started.elapsed() < Duration::from_secs(2));
        let stats = &hub.stats()[0];
        assert_eq!(
            (
                stats.delivered,
                stats.dropped_failed,
                stats.dropped_shutdown,
                stats.queued
            ),
            (0, 0, 3, 0)
        );
    }

    #[test]
    fn a_wedged_sink_is_abandoned_at_the_deadline() {
        let (backend, _, gate, entries) = gated();
        let hub = hub(vec![(sink("s", ""), backend)]);
        hub.emit(event(EventType::JobStarted, "0", "q"));
        entries.recv_timeout(WAIT).unwrap();
        hub.emit(event(EventType::JobStarted, "1", "q"));
        let started = Instant::now();
        hub.shutdown(Duration::from_millis(100));
        assert!(started.elapsed() < Duration::from_secs(2));
        let stats = &hub.stats()[0];
        assert_eq!((stats.dropped_shutdown, stats.queued), (2, 0));
        // The late delivery settles nothing: shutdown already counted it.
        gate.open();
        hub.lifecycle.wait_idle(Instant::now() + WAIT);
        let stats = &hub.stats()[0];
        assert_eq!((stats.delivered, stats.dropped_shutdown), (0, 2));
    }

    #[test]
    fn emit_after_shutdown_counts_a_shutdown_drop() {
        let (backend, attempts) = fake(vec![], DeliveryResult::Delivered);
        let hub = hub(vec![(sink("s", ""), backend)]);
        hub.shutdown(WAIT);
        hub.shutdown(WAIT);
        hub.emit(event(EventType::JobStarted, "late", "q"));
        assert!(lock(&attempts).is_empty());
        let stats = &hub.stats()[0];
        assert_eq!((stats.dropped_shutdown, stats.queued), (1, 0));
    }

    #[test]
    fn prometheus_renders_every_series_with_escaped_labels() {
        let (backend, _) = fake(vec![], DeliveryResult::Delivered);
        let hub = hub(vec![(sink(r#"we\"b"#, ""), backend)]);
        hub.emit(event(EventType::JobStarted, "a", "q"));
        hub.shutdown(WAIT);
        let expected = "\
# HELP flexiq_events_delivered_total Events a sink delivered.
# TYPE flexiq_events_delivered_total counter
flexiq_events_delivered_total{sink=\"we\\\"b\"} 1
# HELP flexiq_events_dropped_total Events a sink dropped, by reason.
# TYPE flexiq_events_dropped_total counter
flexiq_events_dropped_total{sink=\"we\\\"b\",reason=\"buffer_full\"} 0
flexiq_events_dropped_total{sink=\"we\\\"b\",reason=\"rejected\"} 0
flexiq_events_dropped_total{sink=\"we\\\"b\",reason=\"failed\"} 0
flexiq_events_dropped_total{sink=\"we\\\"b\",reason=\"shutdown\"} 0
# HELP flexiq_events_queued Events a sink accepted but has not yet delivered or dropped.
# TYPE flexiq_events_queued gauge
flexiq_events_queued{sink=\"we\\\"b\"} 0
";
        assert_eq!(hub.render_prometheus(), expected);
    }

    #[test]
    fn the_hub_can_be_shared_across_threads() {
        fn shareable<T: Send + Sync>() {}
        shareable::<EventHub>();
    }

    #[test]
    fn label_escaping_covers_backslash_quote_and_newline() {
        assert_eq!(escape_label("a\\b\"c\nd"), "a\\\\b\\\"c\\nd");
    }

    #[test]
    fn backoff_never_exceeds_its_ceiling() {
        for attempt in 1..=40 {
            let ceiling = (BACKOFF_BASE_MS << (attempt - 1).min(20)).min(BACKOFF_CAP_MS);
            assert!(backoff(attempt) <= Duration::from_millis(ceiling));
        }
    }

    #[cfg(not(feature = "events-http"))]
    #[test]
    fn an_http_sink_needs_the_events_http_feature() {
        let doc = r#"{"sinks":[{"kind":"http","name":"a","url":"https://e.example.com/","allow":["e.example.com"]}]}"#;
        let error = EventHub::from_json(doc).unwrap_err();
        assert!(
            matches!(
                error,
                EventsConfigError::NotCompiled {
                    kind: "http",
                    feature: "events-http",
                    ..
                }
            ),
            "{error}"
        );
    }

    #[cfg(not(feature = "redis"))]
    #[test]
    fn a_redis_sink_needs_the_redis_feature() {
        let doc = r#"{"sinks":[{"kind":"redis_streams","name":"r","url_env":"U","stream":"s"}]}"#;
        let error = EventHub::from_json(doc).unwrap_err();
        assert!(
            matches!(
                error,
                EventsConfigError::NotCompiled {
                    kind: "redis_streams",
                    feature: "redis",
                    ..
                }
            ),
            "{error}"
        );
    }

    #[cfg(not(feature = "events-kafka"))]
    #[test]
    fn a_kafka_sink_needs_the_events_kafka_feature() {
        let doc = r#"{"sinks":[{"kind":"kafka","name":"k","brokers":["b:9092"],"topic":"t"}]}"#;
        let error = EventHub::from_json(doc).unwrap_err();
        assert!(
            matches!(
                error,
                EventsConfigError::NotCompiled {
                    kind: "kafka",
                    feature: "events-kafka",
                    ..
                }
            ),
            "{error}"
        );
    }

    #[test]
    fn start_revalidates_a_hand_built_config() {
        let config = EventsConfig {
            source: "/flexiq".into(),
            sinks: vec![],
        };
        assert!(matches!(
            EventHub::start(config),
            Err(EventsConfigError::NoSinks)
        ));
    }
}
