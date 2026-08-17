//! Per-child active-job tracking, shared between dispatcher, reader, and watchdog.
//!
//! Each child runs at most one job at a time, so each slot holds an
//! `Option<ActiveJob>`. The slot is the single source of truth for "this child
//! has a job in flight": whichever thread successfully `take()`s a `Some`
//! becomes responsible for emitting exactly one `JobResult` and decrementing
//! the in-flight counter — preventing double-completion races between the
//! reader (child finished normally) and the watchdog (child exceeded its
//! deadline).

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Instant;

/// Recover a slot guard from a poisoned mutex instead of panicking.
///
/// A panic in one reader/watchdog thread must not cascade into every later slot
/// operation (which would crash the whole prefork pool and abandon in-flight
/// jobs). The slot holds a plain `Option<ActiveJob>`, so reading through a
/// poisoned lock is safe. Mirrors the recovery used in the scheduler poller.
fn recover_poison<T>(poisoned: PoisonError<T>) -> T {
    poisoned.into_inner()
}

/// Metadata about a job currently being executed by a child process.
#[derive(Clone)]
pub struct ActiveJob {
    pub job_id: String,
    pub task_name: String,
    pub retry_count: i32,
    pub max_retries: i32,
    pub timeout_ms: i64,
    pub started_at: Instant,
    /// Absolute deadline. `None` => no timeout configured.
    pub deadline: Option<Instant>,
}

/// One mutex-protected `Option<ActiveJob>` per child, shared across threads.
pub type SlotState = Arc<Vec<Mutex<Option<ActiveJob>>>>;

/// Build an empty slot vector for `n` children.
pub fn new_slots(n: usize) -> SlotState {
    Arc::new((0..n).map(|_| Mutex::new(None)).collect())
}

/// Atomically install `job` in slot `idx`, returning any previous occupant
/// (which would only happen on a programming error — children are sequential).
pub fn set(slots: &SlotState, idx: usize, job: ActiveJob) -> Option<ActiveJob> {
    slots[idx]
        .lock()
        .unwrap_or_else(recover_poison)
        .replace(job)
}

/// Atomically take whatever is in slot `idx`, leaving it empty.
pub fn take(slots: &SlotState, idx: usize) -> Option<ActiveJob> {
    slots[idx].lock().unwrap_or_else(recover_poison).take()
}

/// Atomically take the slot only if its deadline has passed at `now`.
///
/// Re-checking the deadline *while holding the lock* is what makes this
/// race-free: if a result arrived between the watchdog's scan and its
/// take, the reader will have cleared the slot first and we return `None`.
pub fn take_if_expired(slots: &SlotState, idx: usize, now: Instant) -> Option<ActiveJob> {
    let mut guard = slots[idx].lock().unwrap_or_else(recover_poison);
    let expired = guard
        .as_ref()
        .and_then(|j| j.deadline)
        .is_some_and(|d| now >= d);
    if expired {
        guard.take()
    } else {
        None
    }
}

/// Find the slot index whose active job has the given `job_id`, if any.
///
/// Used by the cancel router to locate the child currently running a job.
/// Each slot lock is held only for the duration of a single comparison, so
/// the scan never serialises with dispatch or completion.
pub fn find_by_job_id(slots: &SlotState, job_id: &str) -> Option<usize> {
    for (idx, slot) in slots.iter().enumerate() {
        let guard = slot.lock().unwrap_or_else(recover_poison);
        if guard.as_ref().is_some_and(|j| j.job_id == job_id) {
            return Some(idx);
        }
    }
    None
}
