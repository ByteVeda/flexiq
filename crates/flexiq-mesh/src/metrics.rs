//! Free-running counters for what the mesh did.
//!
//! Plain atomics, never reset, gathered into a single [`MetricsSnapshot`] so a
//! scrape passes one struct around rather than eight counters. The loads are
//! independent and `Relaxed`, so the fields may describe slightly different
//! moments — a snapshot is a convenient bundle, not a point-in-time view.
//!
//! They answer whether stealing is earning its keep, with one caveat:
//! `steals_initiated` counts every attempt, including ones lost to a
//! connection, write, timeout or decode failure, and a target is chosen from
//! gossiped load that may already be stale. A large gap from `steals_succeeded`
//! therefore means "attempts are not paying off", not specifically "peers are
//! empty".

use std::sync::atomic::{AtomicU64, Ordering};

/// Counters for mesh operations, exposed for observability.
pub struct MeshMetrics {
    /// Prefetch batches handed to the local deque. Rises with how often the
    /// scheduler tops the buffer up, not with how much work it fetched.
    pub prefetch_count: AtomicU64,
    /// Jobs those batches actually buffered. Falls short of what was offered
    /// whenever `local_buffer_capacity` was already reached.
    pub prefetch_jobs: AtomicU64,
    /// Jobs taken off the hot end to run here. The mesh's useful output — if
    /// this stays flat while the steal counters climb, jobs are only moving.
    pub local_pops: AtomicU64,
    /// Steal attempts started, counted before the connection is made, so
    /// failures to connect, write, read or decode are all in here.
    pub steals_initiated: AtomicU64,
    /// Attempts that came back with at least one job. A widening gap from
    /// `steals_initiated` means attempts are not paying off.
    pub steals_succeeded: AtomicU64,
    /// Jobs received from peers. Rises when this node is the idle one.
    pub jobs_stolen_in: AtomicU64,
    /// Jobs surrendered to thieves off the cold end. Rises when this node is
    /// the buffered-up one others are draining.
    pub jobs_stolen_out: AtomicU64,
    /// Ring rebuilds — a peer joining, or leaving by any route. Counts changes
    /// to the ring's contents, not writes attempted against it, so a load
    /// refresh from a peer that is still alive does not move it. A fast climb
    /// means placements keep moving, which is affinity thrashing.
    pub ring_recalculations: AtomicU64,
}

impl Default for MeshMetrics {
    fn default() -> Self {
        Self {
            prefetch_count: AtomicU64::new(0),
            prefetch_jobs: AtomicU64::new(0),
            local_pops: AtomicU64::new(0),
            steals_initiated: AtomicU64::new(0),
            steals_succeeded: AtomicU64::new(0),
            jobs_stolen_in: AtomicU64::new(0),
            jobs_stolen_out: AtomicU64::new(0),
            ring_recalculations: AtomicU64::new(0),
        }
    }
}

impl MeshMetrics {
    /// Read every counter into one [`MetricsSnapshot`] a scrape can carry
    /// around.
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            prefetch_count: self.prefetch_count.load(Ordering::Relaxed),
            prefetch_jobs: self.prefetch_jobs.load(Ordering::Relaxed),
            local_pops: self.local_pops.load(Ordering::Relaxed),
            steals_initiated: self.steals_initiated.load(Ordering::Relaxed),
            steals_succeeded: self.steals_succeeded.load(Ordering::Relaxed),
            jobs_stolen_in: self.jobs_stolen_in.load(Ordering::Relaxed),
            jobs_stolen_out: self.jobs_stolen_out.load(Ordering::Relaxed),
            ring_recalculations: self.ring_recalculations.load(Ordering::Relaxed),
        }
    }
}

/// The counters as plain numbers, for serializing into an SDK's metrics
/// output. Cumulative since process start, so a rate needs two of these.
#[derive(Debug, Clone, serde::Serialize)]
pub struct MetricsSnapshot {
    /// [`MeshMetrics::prefetch_count`] at scrape time.
    pub prefetch_count: u64,
    /// [`MeshMetrics::prefetch_jobs`] at scrape time.
    pub prefetch_jobs: u64,
    /// [`MeshMetrics::local_pops`] at scrape time.
    pub local_pops: u64,
    /// [`MeshMetrics::steals_initiated`] at scrape time.
    pub steals_initiated: u64,
    /// [`MeshMetrics::steals_succeeded`] at scrape time.
    pub steals_succeeded: u64,
    /// [`MeshMetrics::jobs_stolen_in`] at scrape time.
    pub jobs_stolen_in: u64,
    /// [`MeshMetrics::jobs_stolen_out`] at scrape time.
    pub jobs_stolen_out: u64,
    /// [`MeshMetrics::ring_recalculations`] at scrape time.
    pub ring_recalculations: u64,
}
