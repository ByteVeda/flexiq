use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;

use super::super::models::*;
use super::super::schema::{execution_claims, job_steps, jobs};
use super::SqliteStorage;
use crate::error::{QueueError, Result};
use crate::job::{now_millis, JobStatus};
use crate::storage::records::NewJobStep;

crate::storage::diesel_common::impl_diesel_step_ops!(SqliteStorage, SqliteConnection);

impl SqliteStorage {
    /// The job row a step write fences against.
    ///
    /// No row lock and none needed: `write_transaction` is `BEGIN IMMEDIATE`,
    /// so the caller already holds the database-wide write lock and no
    /// concurrent `retry` or archive can commit between this read and the
    /// insert that follows it. The Postgres twin takes the row `FOR UPDATE`.
    fn lock_job_for_step_fence(
        conn: &mut SqliteConnection,
        job_id: &str,
    ) -> diesel::result::QueryResult<Option<(i32, i32, Option<String>)>> {
        jobs::table
            .find(job_id)
            .select((jobs::status, jobs::retry_count, jobs::namespace))
            .first(conn)
            .optional()
    }
}
