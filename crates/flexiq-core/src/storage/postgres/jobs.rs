use diesel::pg::PgConnection;
use diesel::prelude::*;

use super::super::models::*;
use super::super::schema::{
    archived_jobs, execution_claims, job_dependencies, job_errors, jobs, replay_history, task_logs,
    task_metrics,
};
use super::PostgresStorage;
use crate::error::{QueueError, Result};
use crate::job::{now_millis, Job, JobStatus, NewJob};
use crate::storage::QueueStats;

crate::storage::diesel_common::impl_diesel_job_ops!(PostgresStorage, PgConnection);

impl PostgresStorage {
    /// Run a read-then-write unit of work in a transaction. Postgres serializes
    /// row-level writes without the SQLite lock-upgrade deadlock, so a regular
    /// transaction suffices; this mirrors [`SqliteStorage::write_transaction`]
    /// so the shared job-ops macro can call one name on both backends.
    pub(crate) fn write_transaction<T, F>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&mut PgConnection) -> std::result::Result<T, QueueError>,
    {
        let mut pooled = self.conn()?;
        let conn: &mut PgConnection = &mut pooled;
        conn.transaction(f)
    }

    /// Pending rows a debounced enqueue may slide, oldest first (narrow — no
    /// payload/result blobs; the winner is re-read in full), locked
    /// `FOR UPDATE` so a concurrent debounce write blocks instead of sliding
    /// the same row twice.
    ///
    /// `FOR UPDATE` alone would not be enough. It can only lock rows that
    /// already exist, so two first-of-a-burst enqueues for the same key would
    /// both find nothing and both insert — the burst case debounce exists to
    /// collapse. The advisory lock closes that window, making "one pending job
    /// per debounce key" an invariant of the write transaction on Postgres the
    /// way `BEGIN IMMEDIATE` does on SQLite. It is transaction-scoped, not
    /// session-scoped, so it releases on commit or rollback and is safe behind
    /// the connection pool.
    ///
    /// `SKIP LOCKED` is deliberately absent, unlike the dequeue scan: skipping
    /// a contended row would insert a duplicate instead of coalescing onto it.
    fn lock_debounce_candidates(
        conn: &mut PgConnection,
        namespace: Option<&str>,
        debounce_key: &str,
    ) -> diesel::result::QueryResult<Vec<NarrowJobRow>> {
        diesel::sql_query("SELECT pg_advisory_xact_lock($1)")
            .bind::<diesel::sql_types::BigInt, _>(debounce_lock_id(namespace, debounce_key))
            .execute(conn)?;

        let base = jobs::table
            .filter(jobs::debounce_key.eq(debounce_key))
            .filter(jobs::status.eq(JobStatus::Pending as i32));

        // Locking is not available on a boxed query in Diesel, so the namespace
        // filter is branched into two concrete statements — same shape as
        // `scan_dequeue_candidates`.
        match namespace {
            Some(ns) => base
                .filter(jobs::namespace.eq(ns))
                .order((jobs::created_at.asc(), jobs::id.asc()))
                .limit(crate::storage::DEBOUNCE_CANDIDATE_SCAN)
                .select(NarrowJobRow::as_select())
                .for_update()
                .load(conn),
            None => base
                .filter(jobs::namespace.is_null())
                .order((jobs::created_at.asc(), jobs::id.asc()))
                .limit(crate::storage::DEBOUNCE_CANDIDATE_SCAN)
                .select(NarrowJobRow::as_select())
                .for_update()
                .load(conn),
        }
    }

    /// Load up to `limit` ready candidate rows (narrow — no payload/result
    /// blobs) for a dequeue, locking each with `FOR UPDATE SKIP LOCKED`.
    ///
    /// SKIP LOCKED is what lets many Postgres workers dequeue concurrently
    /// without contending: each worker's scan skips rows another worker has
    /// already locked in its open transaction, so they claim disjoint sets
    /// instead of all racing on the same head rows. Runs inside the caller's
    /// `write_transaction`, which holds the locks until the claim commits.
    ///
    /// Locking is not available on a boxed query in Diesel, so the namespace
    /// filter is branched into two concrete (un-boxed) statements rather than
    /// built dynamically.
    fn scan_dequeue_candidates(
        conn: &mut PgConnection,
        queue_name: &str,
        now: i64,
        namespace: Option<&str>,
        limit: i64,
        order: crate::storage::DispatchOrder,
    ) -> diesel::result::QueryResult<Vec<NarrowJobRow>> {
        use crate::storage::DispatchOrder;
        let base = jobs::table
            .filter(jobs::queue.eq(queue_name))
            .filter(jobs::status.eq(JobStatus::Pending as i32))
            .filter(jobs::scheduled_at.le(now));

        // `SKIP LOCKED` needs a concrete (un-boxed) statement, so the namespace
        // filter and the order direction are each branched rather than built
        // dynamically. Priority always dominates; the (scheduled_at, id)
        // tie-break flips with the dispatch order (UUIDv7 `id` = deterministic
        // time-ordered final key).
        match (namespace, order) {
            (Some(ns), DispatchOrder::Fifo) => base
                .filter(jobs::namespace.eq(ns))
                .order((
                    jobs::priority.desc(),
                    jobs::scheduled_at.asc(),
                    jobs::id.asc(),
                ))
                .limit(limit)
                .select(NarrowJobRow::as_select())
                .for_update()
                .skip_locked()
                .load(conn),
            (Some(ns), DispatchOrder::Lifo) => base
                .filter(jobs::namespace.eq(ns))
                .order((
                    jobs::priority.desc(),
                    jobs::scheduled_at.desc(),
                    jobs::id.desc(),
                ))
                .limit(limit)
                .select(NarrowJobRow::as_select())
                .for_update()
                .skip_locked()
                .load(conn),
            (None, DispatchOrder::Fifo) => base
                .filter(jobs::namespace.is_null())
                .order((
                    jobs::priority.desc(),
                    jobs::scheduled_at.asc(),
                    jobs::id.asc(),
                ))
                .limit(limit)
                .select(NarrowJobRow::as_select())
                .for_update()
                .skip_locked()
                .load(conn),
            (None, DispatchOrder::Lifo) => base
                .filter(jobs::namespace.is_null())
                .order((
                    jobs::priority.desc(),
                    jobs::scheduled_at.desc(),
                    jobs::id.desc(),
                ))
                .limit(limit)
                .select(NarrowJobRow::as_select())
                .for_update()
                .skip_locked()
                .load(conn),
        }
    }
}

/// Advisory-lock id a debounce write serializes on, derived from the key it
/// coalesces on.
///
/// FNV-1a, hand-rolled rather than `DefaultHasher`: every process writing to
/// this database has to derive the same id from the same key, and
/// `DefaultHasher`'s algorithm is explicitly not stable across Rust releases —
/// a rolling upgrade would silently split the lock in two. The domain prefix
/// keeps the id clear of any advisory lock the host application takes, and the
/// `\x1f` separator keeps `(ns="a", key="xb")` from colliding with
/// `(ns="ax", key="b")`. Collisions only cost extra serialization, never
/// correctness.
///
/// The id deliberately omits `PostgresStorage::schema`, even though advisory
/// locks are database-wide: two instances isolated by schema then serialize on
/// a shared `(namespace, debounce_key)` pair. Each still scans only its own
/// schema, so that is contention and nothing more — not worth threading the
/// schema through a scan signature the SQLite twin has no use for.
fn debounce_lock_id(namespace: Option<&str>, debounce_key: &str) -> i64 {
    const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

    let mut hash = FNV_OFFSET_BASIS;
    for byte in b"flexiq:debounce:"
        .iter()
        .chain(namespace.unwrap_or("").as_bytes())
        .chain(b"\x1f")
        .chain(debounce_key.as_bytes())
    {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash as i64
}

#[cfg(test)]
mod tests {
    use super::debounce_lock_id;

    /// The id is a cross-process contract, not an implementation detail: two
    /// builds that disagree stop excluding each other. Pinning one vector keeps
    /// a future "just use DefaultHasher" refactor from passing silently.
    ///
    /// The vector moved once, in 1.0.0, when the salt was renamed with the rest
    /// of the project. A 0.23.x and a 1.0.0 process therefore do not exclude
    /// each other on the same key — one more reason the upgrade requires a
    /// drained queue rather than a rolling restart.
    #[test]
    fn debounce_lock_id_is_stable_and_key_scoped() {
        assert_eq!(debounce_lock_id(None, "report:user-7"), 3655878828092512785);
        assert_eq!(
            debounce_lock_id(None, "report:user-7"),
            debounce_lock_id(None, "report:user-7")
        );
        assert_ne!(
            debounce_lock_id(None, "report:user-7"),
            debounce_lock_id(Some("tenant-a"), "report:user-7")
        );
        // The separator is what keeps these apart.
        assert_ne!(
            debounce_lock_id(Some("a"), "xb"),
            debounce_lock_id(Some("ax"), "b")
        );
    }
}
