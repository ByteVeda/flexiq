use diesel::pg::PgConnection;
use diesel::prelude::*;

use super::super::models::*;
use super::super::schema::{execution_claims, job_steps, jobs};
use super::PostgresStorage;
use crate::error::{QueueError, Result};
use crate::job::{now_millis, JobStatus};
use crate::storage::records::NewJobStep;

crate::storage::diesel_common::impl_diesel_step_ops!(PostgresStorage, PgConnection);

impl PostgresStorage {
    /// The job row a step write fences against, taken `FOR UPDATE`.
    ///
    /// Postgres runs the step transaction at READ COMMITTED with no
    /// database-wide writer lock, so an unlocked read lets a concurrent `retry`
    /// or terminal archive commit between the fence and the step insert — and
    /// the row then lands in a sequence that has moved on, or under a job that
    /// no longer exists. `retry` updates this row and the archive deletes it,
    /// so both serialize behind the same lock. The SQLite twin needs none:
    /// `BEGIN IMMEDIATE` already serializes writers.
    fn lock_job_for_step_fence(
        conn: &mut PgConnection,
        job_id: &str,
    ) -> diesel::result::QueryResult<Option<(i32, i32, Option<String>)>> {
        jobs::table
            .find(job_id)
            .select((jobs::status, jobs::retry_count, jobs::namespace))
            .for_update()
            .first(conn)
            .optional()
    }
}
