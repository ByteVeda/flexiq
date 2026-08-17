//! Redis storage job operations.
//!
//! Submodules each hold a partial `impl RedisStorage` block grouped by
//! concern. Shared helpers live in `helpers.rs`; the `dequeue_score`
//! function used by enqueue/retry paths is here.

mod dequeue;
mod enqueue;
mod errors;
mod helpers;
mod maintenance;
mod query;
mod state;

/// Compute dequeue score: higher priority → lower score → dequeued first.
/// Within same priority, earlier scheduled_at wins.
pub(super) fn dequeue_score(priority: i32, scheduled_at: i64) -> f64 {
    let p = (priority as i64).clamp(0, 999);
    ((1000i64 - p) * 10_000_000_000_000i64 + scheduled_at) as f64
}
