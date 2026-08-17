use diesel::prelude::*;

use super::super::models::NarrowArchivedJobRow;
use super::super::schema::archived_jobs;
use super::PostgresStorage;
use crate::error::Result;
use crate::job::{Job, JobStatus};

crate::storage::diesel_common::impl_diesel_archival_ops!(PostgresStorage);

impl PostgresStorage {
    /// Move `Complete`/`Dead`/`Cancelled` jobs completed before `cutoff_ms`
    /// (Unix milliseconds) from `jobs` into `archived_jobs`. Returns the count
    /// archived. `Failed` jobs never appear here — `fail()` archives them
    /// immediately.
    pub fn archive_old_jobs(&self, cutoff_ms: i64) -> Result<u64> {
        let mut conn = self.conn()?;

        let archivable_statuses = format!(
            "{}, {}, {}",
            JobStatus::Complete as i32,
            JobStatus::Dead as i32,
            JobStatus::Cancelled as i32,
        );

        conn.transaction(|conn| {
            // Explicit column lists: `jobs` has a `has_deps` column that
            // `archived_jobs` lacks, so `SELECT *` would misalign columns.
            let affected = diesel::sql_query(format!(
                "INSERT INTO archived_jobs \
                 (id, queue, task_name, payload, status, priority, created_at, scheduled_at, \
                  started_at, completed_at, retry_count, max_retries, result, error, timeout_ms, \
                  unique_key, progress, metadata, notes, cancel_requested, expires_at, \
                  result_ttl_ms, namespace) \
                 SELECT id, queue, task_name, payload, status, priority, created_at, scheduled_at, \
                  started_at, completed_at, retry_count, max_retries, result, error, timeout_ms, \
                  unique_key, progress, metadata, notes, cancel_requested, expires_at, \
                  result_ttl_ms, namespace FROM jobs \
                 WHERE status IN ({archivable_statuses}) AND completed_at IS NOT NULL AND completed_at < $1 \
                 ON CONFLICT (id) DO NOTHING",
            ))
            .bind::<diesel::sql_types::BigInt, _>(cutoff_ms)
            .execute(conn)?;

            diesel::sql_query(format!(
                "DELETE FROM jobs WHERE status IN ({archivable_statuses}) AND completed_at IS NOT NULL AND completed_at < $1",
            ))
            .bind::<diesel::sql_types::BigInt, _>(cutoff_ms)
            .execute(conn)?;

            Ok(affected as u64)
        })
    }
}
