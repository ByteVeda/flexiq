//! Storage-side notification hook for push-based dispatch.
//!
//! Entirely behind the `push-dispatch` feature. When off, this module is not
//! compiled and no backend carries any notification state — the default build
//! is byte-for-byte unchanged.
//!
//! A backend implements [`StorageNotifier`] to signal the scheduler that a job
//! has been enqueued: a ready one (`scheduled_at <= now`) so the scheduler can
//! dispatch it immediately instead of waiting for the next poll, a delayed one
//! so it arms a timer for `scheduled_at` instead of waiting out its fallback.
//!
//! Signals carry the job's namespace and queue. Redis publishes on that pair's
//! pub/sub channel, reaching every scheduler serving it; SQLite (one in-process
//! handle) wakes regardless of queue. Postgres implements no notifier: its
//! listener is a stub that ticks on a timer and never reads a `NOTIFY`.

#![cfg(feature = "push-dispatch")]

/// Announce a job due at `scheduled_at` (ms) on `queue` in `namespace` (the
/// job's real ones, never placeholders — Redis routes on both). Implementations must be cheap and
/// must never panic or propagate errors into the enqueue path — a failed
/// notification only costs the dispatch-latency improvement, never
/// correctness (the fallback poll still finds the job).
pub trait StorageNotifier: Send + Sync {
    fn notify_job_ready(&self, namespace: Option<&str>, queue: &str, scheduled_at: i64);
}
