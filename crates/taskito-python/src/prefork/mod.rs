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
//! - Child processes: run `python -m taskito.prefork <app_path>`

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

use taskito_core::job::Job;
use taskito_core::scheduler::JobResult;
use taskito_core::worker::protocol::ExecutorMessage;
use taskito_core::worker::{ExecutorSideChannel, WorkerDispatcher};

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

/// Multi-process worker pool that dispatches jobs to child Python processes.
pub struct PreforkPool {
    num_workers: usize,
    app_path: String,
    python: String,
    shutdown: AtomicBool,
    /// Side-channel for cooperative cancellation. The dispatch loop installs
    /// the sender when `run()` starts and clears it on shutdown so
    /// `notify_cancel` becomes a no-op once the pool is no longer running.
    cancel_tx: Mutex<Option<Sender<String>>>,
    side_channel: SideChannelSlot,
}

impl PreforkPool {
    pub fn new(num_workers: usize, app_path: String) -> Self {
        let python = std::env::var("FLEXIQ_PYTHON").unwrap_or_else(|_| "python".to_string());

        Self {
            num_workers,
            app_path,
            python,
            shutdown: AtomicBool::new(false),
            cancel_tx: Mutex::new(None),
            side_channel: Arc::new(Mutex::new(None)),
        }
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
        let mut reader_handles: Vec<JoinHandle<()>> = Vec::new();

        for idx in 0..num_workers {
            if let Some(handle) = start_child(
                idx,
                &self.python,
                &self.app_path,
                &writers,
                &processes,
                &slots,
                &in_flight,
                &result_tx,
                &self.side_channel,
            ) {
                reader_handles.push(handle);
            }
        }

        let live_children = count_live_writers(&writers);
        if live_children == 0 {
            log::error!("[taskito] no prefork children started, aborting");
            return;
        }
        log::info!("[taskito] prefork pool running with {live_children} children");

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
                log::warn!("[taskito] prefork child {idx} died, restarting");
                restart_count += 1;
                if let Some(handle) = start_child(
                    idx,
                    &self.python,
                    &self.app_path,
                    &writers,
                    &processes,
                    &slots,
                    &in_flight,
                    &result_tx,
                    &self.side_channel,
                ) {
                    reader_handles.push(handle);
                    log::info!(
                        "[taskito] prefork child {idx} restarted (total restarts: {restart_count})"
                    );
                }
            }

            let counts: Vec<u32> = in_flight
                .iter()
                .map(|c| c.load(Ordering::Relaxed))
                .collect();
            let idx = dispatch::least_loaded(&counts);

            dispatch_job(idx, job, &writers, &slots, &in_flight, &self.side_channel);
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
                    log::info!("[taskito] sent shutdown to prefork child {idx}");
                }
            }
        }

        for idx in 0..num_workers {
            if let Ok(mut guard) = processes[idx].lock() {
                if let Some(process) = guard.as_mut() {
                    process.wait_or_kill(SHUTDOWN_DRAIN);
                    log::info!("[taskito] prefork child {idx} exited");
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
                log::warn!("[taskito] prefork cancel channel full, dropping cancel for {job_id}");
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
fn dispatch_job(
    idx: usize,
    job: Job,
    writers: &WriterPool,
    slots: &SlotState,
    in_flight: &InFlightCounters,
    side_channel: &SideChannelSlot,
) {
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
            Some(writer) => writer.write_job_with(&job, disabled),
            None => {
                drop(guard);
                let _ = slot::take(slots, idx);
                log::error!(
                    "[taskito] no live writer for child {idx}, dropping job {}; will be reaped",
                    job.id
                );
                return;
            }
        },
        Err(_) => {
            let _ = slot::take(slots, idx);
            log::error!("[taskito] writer mutex poisoned for child {idx}");
            return;
        }
    };

    match send_result {
        Ok(()) => {
            in_flight[idx].fetch_add(1, Ordering::Relaxed);
        }
        Err(e) => {
            let _ = slot::take(slots, idx);
            log::error!(
                "[taskito] failed to send job {} to child {idx}: {e}",
                job.id
            );
        }
    }
}

/// Spawn child `idx` and its reader thread, plumbing the writer + process into
/// the shared state. Returns the reader thread handle on success, `None` on
/// spawn failure (already logged).
#[allow(clippy::too_many_arguments)]
fn start_child(
    idx: usize,
    python: &str,
    app_path: &str,
    writers: &WriterPool,
    processes: &ProcessPool,
    slots: &SlotState,
    in_flight: &InFlightCounters,
    result_tx: &Sender<JobResult>,
    side_channel: &SideChannelSlot,
) -> Option<JoinHandle<()>> {
    match spawn_child(python, app_path) {
        Ok((writer, reader, process)) => {
            log::info!("[taskito] prefork child {idx} ready");
            if let Ok(mut guard) = writers[idx].lock() {
                *guard = Some(writer);
            }
            if let Ok(mut guard) = processes[idx].lock() {
                *guard = Some(process);
            }
            // Reset the slot for the new child — the killed/dead one's job (if
            // any) was already completed by the watchdog or shutdown path.
            let _ = slot::take(slots, idx);
            in_flight[idx].store(0, Ordering::Relaxed);

            Some(spawn_reader_thread(
                idx,
                reader,
                slots.clone(),
                in_flight.clone(),
                result_tx.clone(),
                side_channel.clone(),
            ))
        }
        Err(e) => {
            log::error!("[taskito] failed to spawn prefork child {idx}: {e}");
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
fn spawn_reader_thread(
    idx: usize,
    mut reader: ChildReader,
    slots: SlotState,
    in_flight: InFlightCounters,
    result_tx: Sender<JobResult>,
    side_channel: SideChannelSlot,
) -> JoinHandle<()> {
    thread::Builder::new()
        .name(format!("taskito-prefork-reader-{idx}"))
        .spawn(move || loop {
            match reader.read::<ExecutorMessage>() {
                Ok((msg, payload)) => {
                    // A detached child cannot reach storage, so its progress and
                    // logs arrive here to be passed on. Handled before the
                    // result path: they are not outcomes, and taking the slot —
                    // which is what completing a job does — would strand it.
                    let Some(msg) = relay_side_channel(&side_channel, msg, &payload) else {
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
                    log::warn!("[taskito] prefork child {idx} reader error: {e}");
                    break;
                }
            }
        })
        .expect("failed to spawn prefork reader thread")
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
        ExecutorMessage::Progress { job_id, progress } => {
            relay.report_progress(&job_id, progress);
        }
        ExecutorMessage::TaskLog {
            job_id,
            task_name,
            level,
            message,
            extra_len,
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
        .name("taskito-prefork-cancel-router".into())
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
                        "[taskito] failed to forward cancel for {job_id} to child {idx}: {e}"
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
