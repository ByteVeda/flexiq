//! The queue depth an autoscaler reads.
//!
//! Two doors answer KEDA: the dashboard's `GET /api/scaler` (the metrics-api
//! scaler) and the gRPC `externalscaler.ExternalScaler` service (#850). Both
//! count through [`depth`], so the two can never report different numbers for
//! one queue.

use flexiq_core::{Storage, StorageBackend};

/// Queue depth KEDA scales against when a scaled object names no target.
pub const TARGET_QUEUE_DEPTH: i64 = 10;

/// The two counts an autoscaler decides on.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Depth {
    /// Jobs waiting to run — the metric value.
    pub pending: i64,
    /// Jobs executing now, which a scale-to-zero must not strand.
    pub running: i64,
}

/// Pending and running jobs in `namespace`, for one queue or (`None`) all of
/// them. Blocking: call it from the blocking pool. Reads live counts only —
/// KEDA polls this every few seconds, so it must not scan the archive.
pub fn depth(
    storage: &StorageBackend,
    namespace: Option<&str>,
    queue: Option<&str>,
) -> flexiq_core::Result<Depth> {
    let live = storage.live_counts(queue, namespace)?;
    Ok(Depth {
        pending: live.pending,
        running: live.running,
    })
}
