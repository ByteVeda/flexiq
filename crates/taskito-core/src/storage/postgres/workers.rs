use diesel::prelude::*;

use super::super::models::*;
use super::super::schema::{execution_claims, workers};
use super::PostgresStorage;
use crate::error::Result;
use crate::job::now_millis;
use crate::storage::records::WorkerRegistration;

crate::storage::diesel_common::impl_diesel_worker_ops!(PostgresStorage);

impl PostgresStorage {
    /// Register a new worker or update an existing one.
    pub fn register_worker(&self, registration: &WorkerRegistration<'_>) -> Result<()> {
        let mut conn = self.conn()?;
        let row = NewWorkerRow::joining(registration, now_millis());

        diesel::insert_into(workers::table)
            .values(&row)
            .on_conflict(workers::worker_id)
            .do_update()
            .set(&row)
            .execute(&mut conn)?;

        Ok(())
    }
}
