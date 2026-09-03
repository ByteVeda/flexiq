//! Prefork worker pool — dispatches jobs to child Python processes via IPC.
//!
//! Each child is an independent Python interpreter with its own GIL,
//! enabling true parallelism for CPU-bound tasks. The parent process
//! runs the Rust scheduler and dispatches serialized jobs over stdin
//! pipes; children send results back over stdout pipes.
//!
//! Architecture:
//! - One dispatch thread: receives `Job` from scheduler, sends to children via stdin
//! - N reader threads: one per child, reads results from stdout, sends to `result_tx`
//! - One watchdog thread: enforces per-job timeouts by `SIGKILL`-ing children
//!   whose deadlines pass
//! - One cancel-router thread: forwards cooperative-cancel requests from
//!   `notify_cancel` to the child currently running the named job
//! - Child processes: run `python -m flexiq.prefork <app_path>`

mod child;
mod dispatch;
mod slot;
mod watchdog;

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use crossbeam_channel::{Receiver, Sender, TrySendError};

use flexiq_core::job::Job;
use flexiq_core::lease::{Lease, LeaseBook};
use flexiq_core::scheduler::JobResult;
use flexiq_core::step::classify_step_failure;
use flexiq_core::worker::protocol::{encode_step_snapshot, ExecutorMessage, SchedulerMessage};
use flexiq_core::worker::{ExecutorSideChannel, ExecutorSteps, StepRelay, WorkerDispatcher};
use flexiq_core::QueueError;

use child::{spawn_child, ChildProcess, ChildReader, ChildWriter};
use slot::{ActiveJob, SlotState};

/// How long graceful shutdown will wait for each child to drain before
/// sending `SIGKILL`.
const SHUTDOWN_DRAIN: Duration = Duration::from_secs(30);

/// Bounded capacity for the cancel side-channel. Cancel requests are tiny
/// and always make progress on the router thread, so this buffer absorbs
/// realistic bursts (workflow-cascade cancels, retried clicks) without ever
/// back-pressuring the caller.
const CANCEL_CHANNEL_CAPACITY: usize = 1024;

/// Per-child writer collection shared between the dispatch thread, the
/// cancel router, and the restart path. Each slot mirrors the `processes`
/// vector — `None` while a child is being respawned, `Some(writer)` while
/// the child is live.
type WriterPool = Arc<Vec<Mutex<Option<ChildWriter>>>>;
type ProcessPool = Arc<Vec<Mutex<Option<ChildProcess>>>>;
type InFlightCounters = Arc<Vec<AtomicU32>>;

/// Where a child's progress and task logs are relayed, once this pool is
/// attached to a scheduler.
///
/// Shared rather than owned because `run()` — and so the reader threads — can
/// start before the attach completes: the handle only exists after the
/// handshake, and installing it is what a detached child's frames wait for.
type SideChannelSlot = Arc<Mutex<Option<ExecutorSideChannel>>>;

/// Where a child's step commits are relayed, once this pool is attached.
///
/// Installed at the same moment as the side channel and for the same reason:
/// the handle only exists after the handshake.
type StepsSlot = Arc<Mutex<Option<ExecutorSteps>>>;

/// What this pool needs to carry durable steps for its children.
///
/// Inert for an in-process worker's pool: its children hold real storage and
/// open their own sessions, so there is nothing here to relay.
#[derive(Clone)]
struct StepRelayState {
    /// Whether the scheduler this pool attached to advertised a step store.
    /// Known before the first child spawns, because the attach handshake
    /// precedes the pool — which is what lets a child's `hello_ack` carry it.
    supported: bool,
    /// The channel to that scheduler.
    handle: StepsSlot,
    /// Whether each child claimed `CAP_STEPS` in its own `hello`, so a snapshot
    /// is read only for a child that will use one.
    claimed: Arc<Vec<AtomicBool>>,
}

impl StepRelayState {
    fn new(supported: bool, num_workers: usize, handle: StepsSlot) -> Self {
        Self {
            supported,
            handle,
            claimed: Arc::new((0..num_workers).map(|_| AtomicBool::new(false)).collect()),
        }
    }

    /// Whether child `idx` claimed `CAP_STEPS` in its own `hello`.
    fn claimed(&self, idx: usize) -> bool {
        self.claimed[idx].load(Ordering::Relaxed)
    }

    /// Whether steps travel between this pool and child `idx` at all.
    fn active_for(&self, idx: usize) -> bool {
        self.supported && self.claimed(idx)
    }

    fn handle(&self) -> Option<ExecutorSteps> {
        self.handle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

/// Multi-process worker pool that dispatches jobs to child Python processes.
pub struct PreforkPool {
    num_workers: usize,
    app_path: String,
    python: String,
    /// Worker id this pool's children fence their durable-step writes on, or
    /// `None` when this process holds no execution claim. See [`Self::new`].
    claim_owner: Option<String>,
    shutdown: AtomicBool,
    /// Side-channel for cooperative cancellation. The dispatch loop installs
    /// the sender when `run()` starts and clears it on shutdown so
    /// `notify_cancel` becomes a no-op once the pool is no longer running.
    cancel_tx: Mutex<Option<Sender<String>>>,
    side_channel: SideChannelSlot,
    /// Whether this pool relays durable steps for its children. False for an
    /// in-process worker's pool — see [`Self::new`].
    relay_steps: bool,
    steps: StepsSlot,
    /// The scheduler's lease book, when this pool runs beside one.
    ///
    /// A child is as capable of outliving its dispatch as an attached executor
    /// is: an operator requeues a job the child is wedged on, the pool hands
    /// the next attempt to a sibling, and the first child then finishes and
    /// reports. The book is what tells the two apart. `None` under an attached
    /// executor, whose dispatches belong to a scheduler in another process.
    leases: Mutex<Option<Arc<LeaseBook>>>,
}

impl PreforkPool {
    /// Build a pool of `num_workers` children running `app_path`.
    ///
    /// `claim_owner` is the worker id this process claims execution under, and
    /// it is what a child's durable-step writes are fenced on. `None` means
    /// this process holds no claim of its own — an attached executor, which
    /// relays a scheduler's work without ever owning it.
    ///
    /// `relay_steps` is for exactly that case: an executor attached to a
    /// scheduler that advertised a step store carries its children's steps the
    /// second hop, so they commit through the claim the *scheduler* holds. An
    /// in-process pool passes `false` — its children reach storage themselves
    /// and have nothing to relay.
    pub fn new(
        num_workers: usize,
        app_path: String,
        claim_owner: Option<String>,
        relay_steps: bool,
    ) -> Self {
        let python = std::env::var("FLEXIQ_PYTHON").unwrap_or_else(|_| "python".to_string());

        Self {
            num_workers,
            app_path,
            python,
            claim_owner,
            shutdown: AtomicBool::new(false),
            cancel_tx: Mutex::new(None),
            side_channel: Arc::new(Mutex::new(None)),
            relay_steps,
            steps: Arc::new(Mutex::new(None)),
            leases: Mutex::new(None),
        }
    }

    /// The lease book, once a worker has installed one.
    fn lease_book(&self) -> Option<Arc<LeaseBook>> {
        self.leases
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    /// Relay children's progress and task logs to `side_channel`.
    ///
    /// Called once the attach has completed, which is the earliest the handle
    /// exists. Until then — and always for an in-process worker, whose children
    /// hold real storage and write for themselves — a child's side-channel
    /// frames are dropped.
    pub fn set_side_channel(&self, side_channel: ExecutorSideChannel) {
        *self
            .side_channel
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(side_channel);
    }

    /// Relay children's durable steps to `steps`.
    ///
    /// Called once the attach has completed, beside
    /// [`set_side_channel`](Self::set_side_channel). Unlike the side channel,
    /// a missing handle is never degraded past: a job whose steps cannot be
    /// carried is failed retryably rather than dispatched, because an empty
    /// snapshot re-runs every step the job already paid for.
    pub fn set_steps(&self, steps: ExecutorSteps) {
        *self
            .steps
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(steps);
    }
}

#[async_trait]
impl WorkerDispatcher for PreforkPool {
    async fn run(
        &self,
        mut job_rx: tokio::sync::mpsc::Receiver<Job>,
        result_tx: Sender<JobResult>,
    ) {
        let num_workers = self.num_workers;

        let slots: SlotState = slot::new_slots(num_workers);
        let in_flight: InFlightCounters =
            Arc::new((0..num_workers).map(|_| AtomicU32::new(0)).collect());
        let processes: ProcessPool = Arc::new((0..num_workers).map(|_| Mutex::new(None)).collect());
        let writers: WriterPool = Arc::new((0..num_workers).map(|_| Mutex::new(None)).collect());
        let steps = StepRelayState::new(self.relay_steps, num_workers, self.steps.clone());
        let mut reader_handles: Vec<JoinHandle<()>> = Vec::new();

        for idx in 0..num_workers {
            if let Some(handle) = start_child(
                idx,
                &self.python,
                &self.app_path,
                self.claim_owner.as_deref(),
                &writers,
                &processes,
                &slots,
                &in_flight,
                &result_tx,
                &self.side_channel,
                &steps,
                self.lease_book(),
            ) {
                reader_handles.push(handle);
            }
        }

        let live_children = count_live_writers(&writers);
        if live_children == 0 {
            log::error!("[flexiq] no prefork children started, aborting");
            return;
        }
        log::info!("[flexiq] prefork pool running with {live_children} children");

        let (cancel_tx, cancel_rx) = crossbeam_channel::bounded::<String>(CANCEL_CHANNEL_CAPACITY);
        self.set_cancel_sender(Some(cancel_tx));
        let cancel_router = spawn_cancel_router(slots.clone(), writers.clone(), cancel_rx);

        let watchdog_shutdown = Arc::new(AtomicBool::new(false));
        let watchdog_handle = watchdog::spawn(
            slots.clone(),
            processes.clone(),
            in_flight.clone(),
            result_tx.clone(),
            watchdog_shutdown.clone(),
        );

        let mut restart_count: u64 = 0;
        while let Some(job) = job_rx.recv().await {
            if self.shutdown.load(Ordering::Relaxed) {
                break;
            }

            for idx in 0..num_workers {
                if !is_child_dead(&processes, idx) {
                    continue;
                }
                log::warn!("[flexiq] prefork child {idx} died, restarting");
                restart_count += 1;
                if let Some(handle) = start_child(
                    idx,
                    &self.python,
                    &self.app_path,
                    self.claim_owner.as_deref(),
                    &writers,
                    &processes,
                    &slots,
                    &in_flight,
                    &result_tx,
                    &self.side_channel,
                    &steps,
                    self.lease_book(),
                ) {
                    reader_handles.push(handle);
                    log::info!(
                        "[flexiq] prefork child {idx} restarted (total restarts: {restart_count})"
                    );
                }
            }

            let counts: Vec<u32> = in_flight
                .iter()
                .map(|c| c.load(Ordering::Relaxed))
                .collect();
            let idx = dispatch::least_loaded(&counts);

            // Read before the dispatch, so the frame carries the lease this
            // job is *currently* dispatched under.
            let lease = self.lease_book().and_then(|book| book.current(&job.id));

            dispatch_job(
                idx,
                job,
                lease,
                &writers,
                &slots,
                &in_flight,
                &self.side_channel,
                &steps,
                &result_tx,
            );
        }

        // Stop accepting new cancel requests so the router can drain and exit
        // cleanly while writers are still alive.
        self.set_cancel_sender(None);

        // Stop the watchdog before sending shutdown so it doesn't race with
        // children draining their final results.
        watchdog_shutdown.store(true, Ordering::SeqCst);

        for idx in 0..num_workers {
            if let Ok(mut guard) = writers[idx].lock() {
                if let Some(w) = guard.as_mut() {
                    // Best-effort: the child may already be gone.
                    let _ = w.write_shutdown();
                    log::info!("[flexiq] sent shutdown to prefork child {idx}");
                }
            }
        }

        for idx in 0..num_workers {
            if let Ok(mut guard) = processes[idx].lock() {
                if let Some(process) = guard.as_mut() {
                    process.wait_or_kill(SHUTDOWN_DRAIN);
                    log::info!("[flexiq] prefork child {idx} exited");
                }
            }
        }

        // Drop writers so the cancel router observes `Disconnected` on its
        // receiver and exits — otherwise the router thread would leak.
        for slot in writers.iter() {
            if let Ok(mut guard) = slot.lock() {
                *guard = None;
            }
        }

        for handle in reader_handles {
            let _ = handle.join();
        }
        let _ = watchdog_handle.join();
        let _ = cancel_router.join();
    }

    fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
    }

    fn set_lease_book(&self, leases: Arc<LeaseBook>) {
        *self
            .leases
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(leases);
    }

    fn notify_cancel(&self, job_id: &str) {
        let Ok(guard) = self.cancel_tx.lock() else {
            return;
        };
        let Some(tx) = guard.as_ref() else {
            return;
        };
        match tx.try_send(job_id.to_string()) {
            Ok(()) | Err(TrySendError::Disconnected(_)) => {}
            Err(TrySendError::Full(_)) => {
                log::warn!("[flexiq] prefork cancel channel full, dropping cancel for {job_id}");
            }
        }
    }
}

impl PreforkPool {
    fn set_cancel_sender(&self, tx: Option<Sender<String>>) {
        if let Ok(mut guard) = self.cancel_tx.lock() {
            *guard = tx;
        }
    }
}

/// Count children with a live writer (i.e. successfully spawned and not
/// torn down). Used at startup to fail fast if every spawn attempt failed.
fn count_live_writers(writers: &WriterPool) -> usize {
    writers
        .iter()
        .filter(|slot| slot.lock().map(|g| g.is_some()).unwrap_or(false))
        .count()
}

/// Whether the child at `idx` has exited (or never spawned successfully).
fn is_child_dead(processes: &ProcessPool, idx: usize) -> bool {
    match processes[idx].lock() {
        Ok(mut guard) => match guard.as_mut() {
            Some(p) => !p.is_alive(),
            None => true,
        },
        Err(_) => false,
    }
}

/// Push a job to child `idx`. The slot is registered before sending so a
/// fast child cannot publish a result the reader can't pair with a slot
/// entry; on send failure the slot is rolled back so neither the reader
/// nor the watchdog will fire for this aborted dispatch.
#[allow(clippy::too_many_arguments)]
fn dispatch_job(
    idx: usize,
    job: Job,
    lease: Option<Lease>,
    writers: &WriterPool,
    slots: &SlotState,
    in_flight: &InFlightCounters,
    side_channel: &SideChannelSlot,
    steps: &StepRelayState,
    result_tx: &Sender<JobResult>,
) {
    // Read before the slot is taken, so a snapshot this pool cannot produce
    // costs nothing to roll back. A job whose committed steps could not be read
    // must **not** be dispatched: the child would open its session on an empty
    // snapshot and re-run every step the job already paid for.
    let snapshot = match step_snapshot(steps, idx, &job.id) {
        Ok(snapshot) => snapshot,
        Err(error) => {
            log::error!(
                "[flexiq] not dispatching job {}: its committed steps could not be read ({error})",
                job.id
            );
            let _ = result_tx.send(JobResult::Failure {
                job_id: job.id.clone(),
                error: format!(
                    "the steps job {} already committed could not be read, so it was not \
                     dispatched: {error}",
                    job.id
                ),
                retry_count: job.retry_count,
                max_retries: job.max_retries,
                task_name: job.task_name.clone(),
                wall_time_ns: 0,
                // Nothing ran and nothing was written, so the next attempt is
                // free to try again — somewhere the snapshot resolves.
                should_retry: true,
                timed_out: false,
            });
            return;
        }
    };

    let active = ActiveJob {
        job_id: job.id.clone(),
        task_name: job.task_name.clone(),
        retry_count: job.retry_count,
        max_retries: job.max_retries,
        timeout_ms: job.timeout_ms,
        started_at: Instant::now(),
        deadline: deadline_from_timeout(job.timeout_ms),
    };
    slot::set(slots, idx, active);

    // Carried on to the child, which is where the task body — and so the
    // middleware chain — actually runs. Empty under an in-process worker,
    // whose children read the toggle list from storage themselves.
    //
    // Cloned out of the slot before it is used, as `relay_side_channel` does:
    // the relay takes locks of its own, and this module holds exactly one at a
    // time.
    let relay = side_channel
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    let disabled = relay
        .map(|relay| relay.disabled_middleware(&job.id))
        .unwrap_or_default();

    let send_result = match writers[idx].lock() {
        Ok(mut guard) => match guard.as_mut() {
            // Both frames under the one lock, and the snapshot first: the child
            // pairs `job_steps` with the `job` frame that follows it, exactly
            // as an attached executor pairs them on the socket.
            Some(writer) => write_dispatch(writer, &job, disabled, snapshot, lease),
            None => {
                drop(guard);
                let _ = slot::take(slots, idx);
                log::error!(
                    "[flexiq] no live writer for child {idx}, dropping job {}; will be reaped",
                    job.id
                );
                return;
            }
        },
        Err(_) => {
            let _ = slot::take(slots, idx);
            log::error!("[flexiq] writer mutex poisoned for child {idx}");
            return;
        }
    };

    match send_result {
        Ok(()) => {
            in_flight[idx].fetch_add(1, Ordering::Relaxed);
        }
        Err(e) => {
            let _ = slot::take(slots, idx);
            log::error!("[flexiq] failed to send job {} to child {idx}: {e}", job.id);
        }
    }
}

/// The encoded snapshot a dispatch to child `idx` carries, if any.
///
/// `None` is an empty snapshot — a job with nothing committed, or a hop that
/// carries no steps at all — and no frame is written for it. An error is a
/// snapshot that exists but could not be *delivered*, which the caller must
/// treat as a reason not to dispatch.
fn step_snapshot(
    steps: &StepRelayState,
    idx: usize,
    job_id: &str,
) -> Result<Option<Vec<u8>>, String> {
    if !steps.supported {
        // This pool relays no steps at all, so there is nothing to withhold: a
        // step attempted on such a child refuses, naming the scheduler.
        return Ok(None);
    }
    // Advertised, so a step-using job may arrive — and there is no honest empty
    // answer to give one. In practice unreachable: the handle is installed on
    // the thread that spawned this pool, before a scheduler can dispatch.
    let Some(handle) = steps.handle() else {
        return Err("the attach has not installed its step channel yet".to_string());
    };
    let recorded = handle.snapshot(job_id).map_err(|error| error.to_string())?;
    deliverable(steps, idx, recorded.len())?;
    Ok((!recorded.is_empty()).then(|| encode_step_snapshot(&recorded)))
}

/// Whether a dispatch carrying `recorded` committed steps can go to child `idx`.
///
/// The count is read *first*, because a job with nothing committed is safe on
/// any child and only a job carrying steps is not.
///
/// An executor announces `CAP_STEPS` before it has spawned a single child, so a
/// child from an older install — `FLEXIQ_PYTHON` can point at one — reaches here
/// having claimed nothing. Withholding the snapshot from it would hand it an
/// *empty* one, and an empty snapshot re-runs every step the job already paid
/// for. Refusing the dispatch is the only fail-closed answer.
fn deliverable(steps: &StepRelayState, idx: usize, recorded: usize) -> Result<(), String> {
    if recorded == 0 || steps.claimed(idx) {
        return Ok(());
    }
    Err(format!(
        "the task runner for this job did not claim the step capability, so it cannot replay \
         the {recorded} step(s) already committed"
    ))
}

/// Write one dispatch: the step snapshot, then the job itself.
fn write_dispatch(
    writer: &mut ChildWriter,
    job: &Job,
    disabled: Vec<String>,
    snapshot: Option<Vec<u8>>,
    lease: Option<Lease>,
) -> Result<(), flexiq_core::worker::protocol::ProtocolError> {
    if let Some(snapshot) = snapshot {
        writer.write(
            &SchedulerMessage::JobSteps {
                job_id: job.id.clone(),
                payload_len: snapshot.len(),
            },
            &snapshot,
        )?;
    }
    writer.write_job_leased(job, disabled, lease)
}

/// Spawn child `idx` and its reader thread, plumbing the writer + process into
/// the shared state. Returns the reader thread handle on success, `None` on
/// spawn failure (already logged).
#[allow(clippy::too_many_arguments)]
fn start_child(
    idx: usize,
    python: &str,
    app_path: &str,
    claim_owner: Option<&str>,
    writers: &WriterPool,
    processes: &ProcessPool,
    slots: &SlotState,
    in_flight: &InFlightCounters,
    result_tx: &Sender<JobResult>,
    side_channel: &SideChannelSlot,
    steps: &StepRelayState,
    leases: Option<Arc<LeaseBook>>,
) -> Option<JoinHandle<()>> {
    match spawn_child(python, app_path, claim_owner, steps.supported) {
        Ok(child) => {
            log::info!("[flexiq] prefork child {idx} ready");
            // Re-read on every respawn: a restarted child is a new interpreter,
            // and a stale `true` would send it a snapshot it never asked for.
            steps.claimed[idx].store(child.steps, Ordering::Relaxed);
            // Likewise re-read on every respawn: `FLEXIQ_PYTHON` can point a
            // restarted child at a different flexiq install than the one that
            // answered last time.
            let leases = leases.filter(|_| child.leases);
            if let Ok(mut guard) = writers[idx].lock() {
                *guard = Some(child.writer);
            }
            if let Ok(mut guard) = processes[idx].lock() {
                *guard = Some(child.process);
            }
            // Reset the slot for the new child — the killed/dead one's job (if
            // any) was already completed by the watchdog or shutdown path.
            let _ = slot::take(slots, idx);
            in_flight[idx].store(0, Ordering::Relaxed);

            Some(spawn_reader_thread(
                idx,
                child.reader,
                slots.clone(),
                in_flight.clone(),
                result_tx.clone(),
                side_channel.clone(),
                writers.clone(),
                steps.clone(),
                leases,
            ))
        }
        Err(e) => {
            log::error!("[flexiq] failed to spawn prefork child {idx}: {e}");
            None
        }
    }
}

/// Reader thread: forwards child results to the scheduler.
///
/// The slot acts as the ownership token — the reader emits a result *only* if
/// it can `take()` the slot entry first. If the watchdog has already taken
/// the slot (deadline expired), the reader silently drops the message because
/// the watchdog has already synthesised the timeout failure.
#[allow(clippy::too_many_arguments)]
fn spawn_reader_thread(
    idx: usize,
    mut reader: ChildReader,
    slots: SlotState,
    in_flight: InFlightCounters,
    result_tx: Sender<JobResult>,
    side_channel: SideChannelSlot,
    writers: WriterPool,
    steps: StepRelayState,
    leases: Option<Arc<LeaseBook>>,
) -> JoinHandle<()> {
    thread::Builder::new()
        .name(format!("flexiq-prefork-reader-{idx}"))
        .spawn(move || loop {
            match reader.read::<ExecutorMessage>() {
                Ok((msg, payload)) => {
                    // Before anything is relayed or applied: a child that
                    // outlived its dispatch is answering for a job the pool has
                    // since handed to a sibling, and only the frame still says
                    // which dispatch it came from.
                    if !frame_is_current(leases.as_deref(), &msg) {
                        refuse_stale(idx, msg, &slots, &in_flight, &writers);
                        continue;
                    }
                    // A detached child cannot reach storage, so its progress and
                    // logs arrive here to be passed on. Handled before the
                    // result path: they are not outcomes, and taking the slot —
                    // which is what completing a job does — would strand it.
                    let Some(msg) = relay_side_channel(&side_channel, msg, &payload) else {
                        continue;
                    };
                    let Some(msg) = relay_step_commit(idx, &steps, &slots, &writers, msg, &payload)
                    else {
                        continue;
                    };
                    let Some(job_result) = msg.into_job_result(payload) else {
                        continue;
                    };
                    if slot::take(&slots, idx).is_none() {
                        // Watchdog already completed this job; drop the
                        // (now-redundant) child message.
                        continue;
                    }
                    in_flight[idx].fetch_sub(1, Ordering::Relaxed);
                    if result_tx.send(job_result).is_err() {
                        break;
                    }
                }
                Err(e) => {
                    log::warn!("[flexiq] prefork child {idx} reader error: {e}");
                    break;
                }
            }
        })
        .expect("failed to spawn prefork reader thread")
}

/// Whether a frame from a child belongs to the dispatch that is current.
///
/// The pool's half of [`flexiq_core::lease`]'s rule, and the same three
/// absences the scheduler's own check treats as `true`: no book, no entry for
/// the job, or no lease on a frame from a child whose dispatch carried none.
fn frame_is_current(leases: Option<&LeaseBook>, msg: &ExecutorMessage) -> bool {
    // `None` is either pool without a book *or* a child that never claimed
    // `CAP_LEASE` — `start_child` collapses the two, because both mean "no
    // lease was ever handed out here".
    let Some(book) = leases else {
        return true;
    };
    let Some((job_id, lease)) = msg.leased_job() else {
        return true;
    };
    let Some(current) = book.current(job_id) else {
        return true;
    };
    lease == Some(&current)
}

/// Answer a frame whose dispatch is no longer current.
///
/// `error!`, not `warn!`: the frame is dropped, but behind it is a second
/// execution of a job that had already been handed to a sibling child.
///
/// What "answer" means depends on the frame. A result is dropped and the slot
/// released — the child's attempt is over either way, and holding the slot
/// would strand a live worker. A step commit is *refused*, because the child is
/// blocked on the ack and would otherwise wait out its whole backstop. A
/// progress report or log line is simply dropped.
fn refuse_stale(
    idx: usize,
    msg: ExecutorMessage,
    slots: &SlotState,
    in_flight: &InFlightCounters,
    writers: &WriterPool,
) {
    if let Some((job_id, _)) = msg.leased_job() {
        log::error!(
            "[flexiq] prefork child {idx} answered for job {job_id} under a lease that is no \
             longer current; refusing it — the job was re-dispatched while that attempt was \
             still running"
        );
    }
    match &msg {
        ExecutorMessage::StepCommit { job_id, seq, .. } => {
            let ack = refusal(job_id, *seq, QueueError::ClaimLost(job_id.clone()));
            answer_child(writers, idx, ack);
        }
        ExecutorMessage::Success { .. }
        | ExecutorMessage::Failure { .. }
        | ExecutorMessage::Cancelled { .. }
        | ExecutorMessage::Slept { .. } => release_slot(slots, in_flight, idx),
        _ => {}
    }
}

/// Free child `idx`'s slot, if it still holds one.
///
/// Taking the slot is the ownership token for a job's single outcome, so this
/// is also what keeps a stale result from being counted twice — the watchdog
/// may already have taken it.
fn release_slot(slots: &SlotState, in_flight: &InFlightCounters, idx: usize) {
    if slot::take(slots, idx).is_some() {
        in_flight[idx].fetch_sub(1, Ordering::Relaxed);
    }
}

/// Pass a child's progress or task log on to the scheduler.
///
/// Returns the frame untouched when it is not one of those, so the caller can
/// go on to treat it as a result. A frame with nowhere to go — an in-process
/// worker's pool, or an attach whose scheduler advertised no side-channel — is
/// dropped, which is what the child's own degraded path would have done.
fn relay_side_channel(
    side_channel: &SideChannelSlot,
    msg: ExecutorMessage,
    payload: &[u8],
) -> Option<ExecutorMessage> {
    let (ExecutorMessage::Progress { .. } | ExecutorMessage::TaskLog { .. }) = &msg else {
        return Some(msg);
    };

    let relay = side_channel
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()?;

    match msg {
        ExecutorMessage::Progress {
            job_id, progress, ..
        } => {
            relay.report_progress(&job_id, progress);
        }
        ExecutorMessage::TaskLog {
            job_id,
            task_name,
            level,
            message,
            extra_len,
            ..
        } => {
            // Absent and empty are different, so the blob is taken from the
            // declared length rather than from whether the payload is empty.
            let extra = extra_len.map(|_| String::from_utf8_lossy(payload).into_owned());
            relay.write_task_log(&job_id, &task_name, &level, &message, extra.as_deref());
        }
        _ => unreachable!("the guard above admits only side-channel frames"),
    }
    None
}

/// Carry one child's step commit to the scheduler and its ack back.
///
/// Returns the frame untouched when it is not a commit, so the caller can go on
/// to treat it as a result.
///
/// Run inline on the reader thread, deliberately. Least-loaded dispatch gives a
/// child at most one job in flight, so nothing of that child's is queued behind
/// this answer — and the child is blocked on it, so there is nothing else for
/// this thread to read until it is written. **Every path answers exactly once**:
/// a child parked on an ack that never comes waits out its whole backstop.
fn relay_step_commit(
    idx: usize,
    steps: &StepRelayState,
    slots: &SlotState,
    writers: &WriterPool,
    msg: ExecutorMessage,
    payload: &[u8],
) -> Option<ExecutorMessage> {
    let ExecutorMessage::StepCommit {
        job_id,
        seq,
        step_key,
        kind,
        wake_at,
        payload_len: _,
        lease: _,
    } = msg
    else {
        return Some(msg);
    };

    // The child names which job and which step; whether the write is *allowed*
    // is settled here and above. A commit for a job this child is not running
    // has no dispatch behind it, so there is no attempt for the scheduler to
    // fence it on.
    let running = slot::peek(slots, idx).filter(|active| active.job_id == job_id);
    let ack = match (running, steps.handle()) {
        (Some(active), Some(handle)) if steps.active_for(idx) => handle.relay_commit(StepRelay {
            job_id: &job_id,
            timeout_ms: active.timeout_ms,
            seq,
            step_key: &step_key,
            kind,
            wake_at,
            result: payload,
        }),
        (None, _) => {
            log::warn!(
                "[flexiq] prefork child {idx} committed step '{step_key}' of job {job_id}, which \
                 it is not running; refusing it"
            );
            // `ClaimLost`, as the scheduler answers the same case: this attempt
            // is not the one holding the job, so it ends without a result
            // rather than failing a run proceeding correctly elsewhere.
            refusal(&job_id, seq, QueueError::ClaimLost(job_id.clone()))
        }
        _ => {
            log::warn!(
                "[flexiq] prefork child {idx} committed step '{step_key}' of job {job_id}, but \
                 this pool relays no steps; refusing it"
            );
            refusal(
                &job_id,
                seq,
                QueueError::Other(format!(
                    "step '{step_key}' of job {job_id} cannot be committed: this worker relays \
                     no durable steps"
                )),
            )
        }
    };

    answer_child(writers, idx, ack);
    None
}

/// Write one answer back to child `idx`.
///
/// Every step commit gets exactly one of these: a child parked on an ack that
/// never comes waits out its whole backstop, so a failure to write is logged
/// rather than propagated — its own bounded wait then ends the attempt
/// retryably, which is right, because a commit whose answer was lost may or may
/// not have landed.
fn answer_child(writers: &WriterPool, idx: usize, ack: SchedulerMessage) {
    match writers[idx].lock() {
        Ok(mut guard) => match guard.as_mut() {
            Some(writer) => {
                if let Err(error) = writer.write_header(&ack) {
                    log::warn!(
                        "[flexiq] could not answer a step commit from prefork child {idx}: {error}"
                    );
                }
            }
            None => log::warn!("[flexiq] no live writer to answer prefork child {idx}'s step"),
        },
        Err(_) => log::error!("[flexiq] writer mutex poisoned for child {idx}"),
    }
}

/// A refusal this pool produced, carrying what the child should do about it.
///
/// The classification is made here because only this side saw the error — the
/// same split `step_pump` makes one hop up.
fn refusal(job_id: &str, seq: i32, error: QueueError) -> SchedulerMessage {
    let failure = classify_step_failure(&error);
    SchedulerMessage::StepAck {
        job_id: job_id.to_string(),
        seq,
        ok: false,
        already: false,
        wake_at: None,
        error: Some(error.to_string()),
        failure: Some(failure),
    }
}

/// Cancel router: forwards cooperative-cancel requests to the child
/// currently running the named job.
///
/// The router never owns the slot — it only consults `find_by_job_id` to
/// route the message. Result/timeout completion still owns the slot
/// `take()`. If the job is no longer running (already completed, never
/// dispatched, or just finished between `notify_cancel` and the router
/// pick-up), the request is dropped silently — the storage cancel flag
/// set by `Storage::request_cancel` already handles those cases.
fn spawn_cancel_router(
    slots: SlotState,
    writers: WriterPool,
    cancel_rx: Receiver<String>,
) -> JoinHandle<()> {
    thread::Builder::new()
        .name("flexiq-prefork-cancel-router".into())
        .spawn(move || {
            for job_id in cancel_rx.iter() {
                let Some(idx) = slot::find_by_job_id(&slots, &job_id) else {
                    continue;
                };
                let Ok(mut guard) = writers[idx].lock() else {
                    continue;
                };
                let Some(writer) = guard.as_mut() else {
                    continue;
                };
                if let Err(e) = writer.write_cancel(&job_id) {
                    log::warn!(
                        "[flexiq] failed to forward cancel for {job_id} to child {idx}: {e}"
                    );
                }
            }
        })
        .expect("failed to spawn prefork cancel-router thread")
}

/// Convert a per-task timeout in milliseconds to an absolute `Instant` deadline.
/// Returns `None` for `timeout_ms <= 0` (no timeout configured) so the watchdog
/// skips the slot.
fn deadline_from_timeout(timeout_ms: i64) -> Option<Instant> {
    if timeout_ms <= 0 {
        None
    } else {
        Instant::now().checked_add(Duration::from_millis(timeout_ms as u64))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A relay state with no channel installed, which is all these need: the
    /// decisions under test are made before the handle is ever consulted.
    fn state(supported: bool, claimed: bool) -> StepRelayState {
        let steps = StepRelayState::new(supported, 1, Arc::new(Mutex::new(None)));
        steps.claimed[0].store(claimed, Ordering::Relaxed);
        steps
    }

    #[test]
    fn a_pool_that_relays_no_steps_carries_no_snapshot() {
        // Nothing to withhold: a step attempted on such a child refuses on its
        // own, naming the scheduler that offers no step store.
        let steps = state(false, true);
        assert!(matches!(step_snapshot(&steps, 0, "job-1"), Ok(None)));
    }

    #[test]
    fn a_child_that_claimed_nothing_is_not_handed_a_job_with_steps() {
        // Refusing is the only fail-closed answer: the alternative is an empty
        // snapshot, which re-runs every step the job already paid for.
        let steps = state(true, false);
        assert!(
            deliverable(&steps, 0, 2).is_err(),
            "a snapshot that cannot be replayed must stop the dispatch, not travel empty"
        );
    }

    #[test]
    fn a_child_that_claimed_nothing_still_takes_a_job_with_no_steps() {
        // The count is what decides, not the capability: a job with nothing
        // committed has nothing to lose, and refusing it would strand every
        // ordinary job on a fleet mid-upgrade.
        let steps = state(true, false);
        assert!(deliverable(&steps, 0, 0).is_ok());
    }

    #[test]
    fn a_child_that_claimed_the_capability_takes_the_snapshot() {
        let steps = state(true, true);
        assert!(deliverable(&steps, 0, 2).is_ok());
    }

    #[test]
    fn an_advertised_pool_with_no_channel_yet_refuses_to_dispatch() {
        // Unreachable in practice — the handle is installed on the thread that
        // spawned this pool — but the honest answer is still a refusal, because
        // there is no empty snapshot that is safe to send.
        let steps = state(true, true);
        assert!(step_snapshot(&steps, 0, "job-1").is_err());
    }
}
