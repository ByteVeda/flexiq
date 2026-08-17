//! Per-job timeout enforcer for the prefork pool.
//!
//! A single thread wakes every `TICK` and, for each child slot whose deadline
//! has passed, atomically takes ownership of the job, kills the child process,
//! decrements the in-flight counter, and emits a synthesised
//! `JobResult::Failure { timed_out: true }` — matching the shape produced by
//! the scheduler's `reap_stale_jobs` maintenance task so downstream middleware
//! and event hooks treat both paths identically.

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crossbeam_channel::Sender;

use flexiq_core::scheduler::JobResult;

use super::child::ChildProcess;
use super::slot::{self, SlotState};

/// Watchdog poll interval. 250ms balances kill latency against parent CPU use:
/// a 5s task timeout is enforced within ~5.25s, well inside the issue's
/// "small watchdog tick budget".
pub const TICK: Duration = Duration::from_millis(250);

/// Spawn the timeout-watchdog thread.
///
/// The thread exits when `shutdown` is set. Children killed for exceeding
/// their deadline are left for the dispatcher's existing dead-child respawn
/// logic to bring back online on the next dispatched job.
pub fn spawn(
    slots: SlotState,
    processes: Arc<Vec<Mutex<Option<ChildProcess>>>>,
    in_flight: Arc<Vec<AtomicU32>>,
    result_tx: Sender<JobResult>,
    shutdown: Arc<AtomicBool>,
) -> JoinHandle<()> {
    thread::Builder::new()
        .name("flexiq-prefork-watchdog".into())
        .spawn(move || run(slots, processes, in_flight, result_tx, shutdown))
        .expect("failed to spawn prefork watchdog thread")
}

fn run(
    slots: SlotState,
    processes: Arc<Vec<Mutex<Option<ChildProcess>>>>,
    in_flight: Arc<Vec<AtomicU32>>,
    result_tx: Sender<JobResult>,
    shutdown: Arc<AtomicBool>,
) {
    while !shutdown.load(Ordering::Relaxed) {
        thread::sleep(TICK);
        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        let now = Instant::now();
        for idx in 0..slots.len() {
            let Some(job) = slot::take_if_expired(&slots, idx, now) else {
                continue;
            };

            log::warn!(
                "[flexiq] prefork child {idx} exceeded {timeout}ms timeout on job {job_id}, killing",
                timeout = job.timeout_ms,
                job_id = job.job_id,
            );

            if let Ok(mut guard) = processes[idx].lock() {
                if let Some(process) = guard.as_mut() {
                    process.kill_and_reap();
                }
            }
            in_flight[idx].fetch_sub(1, Ordering::Relaxed);

            let result = JobResult::Failure {
                job_id: job.job_id,
                error: format!("job timed out after {}ms", job.timeout_ms),
                retry_count: job.retry_count,
                max_retries: job.max_retries,
                task_name: job.task_name,
                wall_time_ns: job.started_at.elapsed().as_nanos() as i64,
                should_retry: true,
                timed_out: true,
            };
            if result_tx.send(result).is_err() {
                // Scheduler already shut down; nothing more to do.
                return;
            }
        }
    }
}
