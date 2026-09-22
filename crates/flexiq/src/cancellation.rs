//! Cooperative cancel for a running task.
//!
//! `request_cancel` only flags a running job; the body has to look. The pool
//! hears of a cancel through `notify_cancel`, which the worker's cancel relay
//! calls within about a second of the flag being set — so a check here is a
//! lock and a set lookup, never a database read, and a body may call it in a
//! tight loop.
//!
//! Held in a thread-local for the same reason the step session is: a
//! macro-expanded body is the caller's own function and cannot grow an
//! argument it did not declare.

use std::cell::RefCell;
use std::sync::Arc;

use flexiq_core::worker::CancelSignals;

use crate::{Abort, Outcome};

thread_local! {
    /// The job this thread is running, and where its cancel would land.
    static CURRENT: RefCell<Option<(String, Arc<CancelSignals>)>> = const { RefCell::new(None) };
}

/// Point this thread at `job_id`'s cancel for the length of one dispatch.
pub(crate) fn install(job_id: &str, signals: Arc<CancelSignals>) {
    CURRENT.with(|cell| *cell.borrow_mut() = Some((job_id.to_string(), signals)));
}

/// Forget the dispatch, so a reused blocking thread starts clean.
pub(crate) fn clear() {
    CURRENT.with(|cell| *cell.borrow_mut() = None);
}

/// Whether the task running on this thread has been asked to cancel.
///
/// `false` outside a running task.
pub fn cancel_requested() -> bool {
    CURRENT.with(|cell| {
        cell.borrow()
            .as_ref()
            .is_some_and(|(job_id, signals)| signals.is_cancelled(job_id))
    })
}

/// End the task here if it has been asked to cancel.
///
/// The job settles `Cancelled` and is not retried. Place it between units of
/// work that are safe to stop after — a check is cheap, but nothing stops the
/// body at any other point.
///
/// ```ignore
/// #[flexiq::task]
/// fn reindex(ids: Vec<String>) -> flexiq::Outcome<()> {
///     for id in ids {
///         flexiq::check_cancelled()?;
///         reindex_one(&id)?;
///     }
///     Ok(())
/// }
/// ```
pub fn check_cancelled() -> Outcome<()> {
    if cancel_requested() {
        return Err(Abort::Cancelled);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outside_a_task_nothing_is_cancelled() {
        clear();
        assert!(!cancel_requested());
        assert!(check_cancelled().is_ok());
    }

    #[test]
    fn a_signal_for_the_running_job_ends_it() {
        let signals = Arc::new(CancelSignals::detached());
        install("job-1", Arc::clone(&signals));
        assert!(check_cancelled().is_ok());

        signals.signal("job-1");
        assert!(matches!(check_cancelled(), Err(Abort::Cancelled)));
        clear();
    }

    #[test]
    fn a_signal_for_another_job_is_not_this_ones() {
        let signals = Arc::new(CancelSignals::detached());
        install("job-1", Arc::clone(&signals));
        signals.signal("job-2");
        assert!(!cancel_requested());
        clear();
    }
}
