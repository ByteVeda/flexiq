use std::sync::atomic::{AtomicU64, Ordering};

/// Counters for mesh operations, exposed for observability.
pub struct MeshMetrics {
    pub prefetch_count: AtomicU64,
    pub prefetch_jobs: AtomicU64,
    pub local_pops: AtomicU64,
    pub steals_initiated: AtomicU64,
    pub steals_succeeded: AtomicU64,
    pub jobs_stolen_in: AtomicU64,
    pub jobs_stolen_out: AtomicU64,
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

#[derive(Debug, Clone, serde::Serialize)]
pub struct MetricsSnapshot {
    pub prefetch_count: u64,
    pub prefetch_jobs: u64,
    pub local_pops: u64,
    pub steals_initiated: u64,
    pub steals_succeeded: u64,
    pub jobs_stolen_in: u64,
    pub jobs_stolen_out: u64,
    pub ring_recalculations: u64,
}
