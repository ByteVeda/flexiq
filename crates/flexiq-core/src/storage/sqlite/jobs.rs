use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;

use super::super::models::*;
use super::super::schema::{
    archived_jobs, execution_claims, job_dependencies, job_errors, job_steps, jobs, replay_history,
    task_logs, task_metrics,
};
use super::SqliteStorage;
use crate::error::{QueueError, Result};
use crate::job::{now_millis, Job, JobStatus, NewJob};
use crate::storage::QueueStats;

crate::storage::diesel_common::impl_diesel_job_ops!(SqliteStorage, SqliteConnection);

impl SqliteStorage {
    /// Run a read-then-write unit of work under a write-priority transaction.
    ///
    /// Any sequence that reads a row and then writes it (dequeue/claim, enqueue
    /// with dependency validation, unique-key dedup, DLQ/archive moves, rate-limit
    /// token consume) must use this. Under SQLite a plain deferred transaction
    /// starts as a reader and must later upgrade to a writer; two such
    /// transactions racing each other deadlock and surface as `SQLITE_BUSY`
    /// ("database is locked") without honoring `busy_timeout`. `BEGIN IMMEDIATE`
    /// takes the write lock up front so the busy-timeout backoff applies and the
    /// operations serialize cleanly. The Postgres twin is a plain transaction
    /// (MVCC has no deferred-upgrade hazard), so the name is uniform across
    /// backends and callers never pick the wrong transaction kind.
    pub(crate) fn write_transaction<T, F>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&mut SqliteConnection) -> std::result::Result<T, QueueError>,
    {
        let mut conn = self.conn()?;
        conn.immediate_transaction(f)
    }

    /// Pending rows a debounced enqueue may slide, oldest first (narrow — no
    /// payload/result blobs; the winner is re-read in full).
    ///
    /// No row-locking clause and none needed: the caller's `BEGIN IMMEDIATE`
    /// write transaction already holds the database-wide write lock, so no
    /// second writer can insert a competing row for this key between the scan
    /// and the write. That is what makes "one pending job per debounce key" an
    /// invariant of the transaction rather than of an index — see
    /// `m0010_debounce` for why the index is deliberately not unique.
    fn lock_debounce_candidates(
        conn: &mut SqliteConnection,
        namespace: Option<&str>,
        debounce_key: &str,
    ) -> diesel::result::QueryResult<Vec<NarrowJobRow>> {
        let mut query = jobs::table
            .filter(jobs::debounce_key.eq(debounce_key))
            .filter(jobs::status.eq(JobStatus::Pending as i32))
            .into_boxed();
        query = match namespace {
            Some(ns) => query.filter(jobs::namespace.eq(ns)),
            None => query.filter(jobs::namespace.is_null()),
        };
        query
            .order((jobs::created_at.asc(), jobs::id.asc()))
            .limit(crate::storage::DEBOUNCE_CANDIDATE_SCAN)
            .select(NarrowJobRow::as_select())
            .load(conn)
    }

    /// Load up to `limit` ready candidate rows (narrow — no payload/result
    /// blobs) for a dequeue. SQLite needs no row-level locking clause: the
    /// `BEGIN IMMEDIATE` write transaction already serializes writers, so two
    /// dequeues never scan the same uncommitted candidates concurrently.
    fn scan_dequeue_candidates(
        conn: &mut SqliteConnection,
        queue_name: &str,
        now: i64,
        namespace: Option<&str>,
        limit: i64,
        order: crate::storage::DispatchOrder,
    ) -> diesel::result::QueryResult<Vec<NarrowJobRow>> {
        let mut query = jobs::table
            .filter(jobs::queue.eq(queue_name))
            .filter(jobs::status.eq(JobStatus::Pending as i32))
            .filter(jobs::scheduled_at.le(now))
            .limit(limit)
            .into_boxed();
        query = match namespace {
            Some(ns) => query.filter(jobs::namespace.eq(ns)),
            None => query.filter(jobs::namespace.is_null()),
        };
        // Priority always wins; the (scheduled_at, id) tie-break flips with the
        // dispatch order. `id` is a UUIDv7, so it is a deterministic time-ordered
        // final key — without it, same-`scheduled_at` ties are engine-order.
        query = match order {
            crate::storage::DispatchOrder::Fifo => query.order((
                jobs::priority.desc(),
                jobs::scheduled_at.asc(),
                jobs::id.asc(),
            )),
            crate::storage::DispatchOrder::Lifo => query.order((
                jobs::priority.desc(),
                jobs::scheduled_at.desc(),
                jobs::id.desc(),
            )),
        };
        query.select(NarrowJobRow::as_select()).load(conn)
    }
}
