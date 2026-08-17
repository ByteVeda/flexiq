/// Generates shared worker operation methods for Diesel-backed storage backends.
///
/// `register_worker` differs between SQLite (`replace_into`) and Postgres
/// (`insert_into...on_conflict.do_update`), so it remains backend-specific.
macro_rules! impl_diesel_worker_ops {
    ($storage_type:ty) => {
        impl $storage_type {
            /// Update the heartbeat timestamp for a worker.
            pub fn heartbeat(&self, worker_id: &str, resource_health: Option<&str>) -> Result<()> {
                let mut conn = self.conn()?;
                let now = now_millis();

                diesel::update(workers::table)
                    .filter(workers::worker_id.eq(worker_id))
                    .set((
                        workers::last_heartbeat.eq(now),
                        workers::resource_health.eq(resource_health),
                    ))
                    .execute(&mut conn)?;

                Ok(())
            }

            /// Update the status of a worker.
            pub fn update_worker_status(
                &self,
                worker_id: &str,
                status: $crate::storage::records::WorkerStatus,
            ) -> Result<()> {
                let mut conn = self.conn()?;

                diesel::update(workers::table)
                    .filter(workers::worker_id.eq(worker_id))
                    .set(workers::status.eq(status.as_str()))
                    .execute(&mut conn)?;

                Ok(())
            }

            /// List all workers with their heartbeat status.
            pub fn list_workers(&self) -> Result<Vec<$crate::storage::records::WorkerInfo>> {
                let mut conn = self.conn()?;

                let rows = workers::table
                    .select(WorkerRow::as_select())
                    .load::<WorkerRow>(&mut conn)?;

                Ok(rows.into_iter().map(Into::into).collect())
            }

            /// Ids of workers whose heartbeat is at or after `cutoff_ms`.
            ///
            /// Pushes both the liveness filter and the projection into the query
            /// — a caller that only needs live ids must not load every worker's
            /// `resource_health` blob just to discard it.
            pub fn list_live_worker_ids(&self, cutoff_ms: i64) -> Result<Vec<String>> {
                let mut conn = self.conn()?;

                let ids = workers::table
                    .filter(workers::last_heartbeat.ge(cutoff_ms))
                    .select(workers::worker_id)
                    .load(&mut conn)?;

                Ok(ids)
            }

            /// Remove workers that haven't sent a heartbeat within the threshold.
            /// Returns the IDs of the reaped workers.
            ///
            /// The delete re-applies the `last_heartbeat < cutoff` predicate so
            /// a worker that heartbeats between the scan and the delete is not
            /// reaped erroneously. The returned id list reflects the scan
            /// snapshot and may slightly over-report the actual reaped set when
            /// such a race occurs — callers use the list for event emission and
            /// orphan rescue, both of which tolerate the false positive.
            pub fn reap_dead_workers(&self) -> Result<Vec<String>> {
                let mut conn = self.conn()?;
                let cutoff = $crate::storage::dead_worker_cutoff(now_millis());

                let dead_ids: Vec<String> = workers::table
                    .filter(workers::last_heartbeat.lt(cutoff))
                    .select(workers::worker_id)
                    .load(&mut conn)?;

                if !dead_ids.is_empty() {
                    diesel::delete(
                        workers::table
                            .filter(workers::worker_id.eq_any(&dead_ids))
                            .filter(workers::last_heartbeat.lt(cutoff)),
                    )
                    .execute(&mut conn)?;
                }

                Ok(dead_ids)
            }

            /// Unregister a worker (called on shutdown).
            pub fn unregister_worker(&self, worker_id: &str) -> Result<()> {
                let mut conn = self.conn()?;

                diesel::delete(workers::table.filter(workers::worker_id.eq(worker_id)))
                    .execute(&mut conn)?;

                Ok(())
            }

            /// List all job IDs currently claimed by a worker.
            pub fn list_claims_by_worker(&self, worker_id: &str) -> Result<Vec<String>> {
                let mut conn = self.conn()?;

                let job_ids: Vec<String> = execution_claims::table
                    .filter(execution_claims::worker_id.eq(worker_id))
                    .select(execution_claims::job_id)
                    .load(&mut conn)?;

                Ok(job_ids)
            }
        }
    };
}

pub(crate) use impl_diesel_worker_ops;
