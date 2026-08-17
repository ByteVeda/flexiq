//! Shared drain loop for the batched retention purges.
//!
//! Every purge deletes in bounded batches rather than one statement: an
//! unbounded `DELETE` over millions of rows holds SQLite's single writer lock
//! for the whole sweep, and on Postgres pins `xmin` in one long transaction —
//! blocking autovacuum cluster-wide while the dead tuples pile up.

use log::warn;

use crate::error::Result;

/// Max rows deleted per transaction across every batched purge. Bounds the
/// writer-lock hold time and keeps the `IN (...)` bind count under SQLite's
/// 999-parameter limit.
pub(crate) const PURGE_BATCH: i64 = 500;

/// Max ids per child-delete statement. The `job_dependencies` delete binds the
/// ids twice (`job_id OR depends_on_job_id`), so 450 keeps it under SQLite's
/// 999-parameter limit.
pub(crate) const DELETE_ID_CHUNK: usize = 450;

/// Max batches one sweep may drain. A table written faster than it purges would
/// otherwise keep a single sweep running indefinitely; the cap makes the sweep
/// yield and the next tick resume where this one stopped.
const MAX_BATCHES_PER_SWEEP: u32 = 200;

/// Run `delete_batch` until it reports a short batch (nothing left to match) or
/// the sweep cap trips.
///
/// `delete_batch` must open its own transaction and delete at most
/// `PURGE_BATCH` rows — that is what keeps the lock held for one batch rather
/// than the whole sweep. It returns the rows it deleted.
pub(crate) fn drain_batches<F>(mut delete_batch: F) -> Result<u64>
where
    F: FnMut() -> Result<u64>,
{
    let mut total = 0u64;
    for _ in 0..MAX_BATCHES_PER_SWEEP {
        let removed = delete_batch()?;
        total += removed;
        if removed < PURGE_BATCH as u64 {
            return Ok(total);
        }
    }
    warn!("purge sweep stopped at the {MAX_BATCHES_PER_SWEEP}-batch cap after {total} rows; the backlog resumes next tick");
    Ok(total)
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::*;
    use crate::error::QueueError;

    #[test]
    fn drains_until_a_short_batch() {
        let calls = Cell::new(0u32);
        let total = drain_batches(|| {
            calls.set(calls.get() + 1);
            // Two full batches, then a short one ends the sweep.
            Ok(if calls.get() < 3 {
                PURGE_BATCH as u64
            } else {
                7
            })
        })
        .unwrap();

        assert_eq!(calls.get(), 3, "must stop on the first short batch");
        assert_eq!(total, PURGE_BATCH as u64 * 2 + 7);
    }

    #[test]
    fn a_single_short_batch_ends_the_sweep() {
        let calls = Cell::new(0u32);
        let total = drain_batches(|| {
            calls.set(calls.get() + 1);
            Ok(0)
        })
        .unwrap();

        assert_eq!(calls.get(), 1, "an empty table must cost exactly one query");
        assert_eq!(total, 0);
    }

    #[test]
    fn stops_at_the_sweep_cap() {
        let calls = Cell::new(0u32);
        let total = drain_batches(|| {
            calls.set(calls.get() + 1);
            Ok(PURGE_BATCH as u64)
        })
        .unwrap();

        assert_eq!(calls.get(), MAX_BATCHES_PER_SWEEP);
        assert_eq!(total, PURGE_BATCH as u64 * MAX_BATCHES_PER_SWEEP as u64);
    }

    #[test]
    fn propagates_a_batch_error() {
        let calls = Cell::new(0u32);
        let result = drain_batches(|| {
            calls.set(calls.get() + 1);
            Err(QueueError::Other("boom".to_string()))
        });

        assert!(result.is_err(), "a failing batch must abort the sweep");
        assert_eq!(calls.get(), 1, "must not retry a failing batch");
    }
}
