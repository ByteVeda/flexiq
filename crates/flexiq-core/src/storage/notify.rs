//! Storage-side notification hook for push-based dispatch.
//!
//! Entirely behind the `push-dispatch` feature. When off, this module is not
//! compiled and no backend carries any notification state — the default build
//! is byte-for-byte unchanged.
//!
//! A backend implements [`StorageNotifier`] to signal the scheduler that a
//! ready job (`scheduled_at <= now`) has been enqueued, so the scheduler can
//! dispatch it immediately instead of waiting for the next poll. Delayed jobs
//! (`scheduled_at > now`) deliberately do NOT notify — the scheduler relies on
//! its fallback timer to pick those up at the right time.

#![cfg(feature = "push-dispatch")]

/// Emit a "job ready" signal for `queue`. Implementations must be cheap and
/// must never panic or propagate errors into the enqueue path — a failed
/// notification only costs the dispatch-latency improvement, never
/// correctness (the fallback poll still finds the job).
pub trait StorageNotifier: Send + Sync {
    fn notify_job_ready(&self, queue: &str, scheduled_at: i64);
}
