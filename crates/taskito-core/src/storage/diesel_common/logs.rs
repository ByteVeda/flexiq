/// Generates shared log operation methods for Diesel-backed storage backends.
macro_rules! impl_diesel_log_ops {
    ($storage_type:ty) => {
        impl $storage_type {
            /// Write a structured log entry for a task, tagged with the
            /// namespace the writer is scoped to.
            pub fn write_task_log(
                &self,
                job_id: &str,
                task_name: &str,
                level: &str,
                message: &str,
                extra: Option<&str>,
                namespace: Option<&str>,
            ) -> Result<()> {
                let mut conn = self.conn()?;
                let id = uuid::Uuid::now_v7().to_string();
                let now = now_millis();

                let row = NewTaskLogRow {
                    id: &id,
                    job_id,
                    task_name,
                    level,
                    message,
                    extra,
                    logged_at: now,
                    namespace,
                };

                diesel::insert_into(task_logs::table)
                    .values(&row)
                    .execute(&mut conn)?;

                Ok(())
            }

            /// Get logs for a specific job.
            pub fn get_task_logs(
                &self,
                job_id: &str,
            ) -> Result<Vec<$crate::storage::records::TaskLogEntry>> {
                let mut conn = self.conn()?;
                let rows = task_logs::table
                    .filter(task_logs::job_id.eq(job_id))
                    .order(task_logs::logged_at.asc())
                    .select(TaskLogRow::as_select())
                    .load::<TaskLogRow>(&mut conn)?;
                Ok(rows.into_iter().map(Into::into).collect())
            }

            /// Logs for a job with id strictly after `after_id` (cursor scan).
            pub fn get_task_logs_after(
                &self,
                job_id: &str,
                after_id: Option<&str>,
            ) -> Result<Vec<$crate::storage::records::TaskLogEntry>> {
                let mut conn = self.conn()?;
                let mut query = task_logs::table
                    .filter(task_logs::job_id.eq(job_id))
                    .into_boxed();
                if let Some(cursor) = after_id {
                    query = query.filter(task_logs::id.gt(cursor.to_string()));
                }
                // UUIDv7 ids sort by creation time, so id order == time order.
                let rows = query
                    .order(task_logs::id.asc())
                    .select(TaskLogRow::as_select())
                    .load::<TaskLogRow>(&mut conn)?;
                Ok(rows.into_iter().map(Into::into).collect())
            }

            /// Query logs by task name, level, etc. `namespace` of `None`
            /// returns every namespace, matching `list_jobs`.
            pub fn query_task_logs(
                &self,
                task_name: Option<&str>,
                level: Option<&str>,
                since_ms: i64,
                limit: i64,
                namespace: Option<&str>,
            ) -> Result<Vec<$crate::storage::records::TaskLogEntry>> {
                let mut conn = self.conn()?;

                let mut query = task_logs::table
                    .filter(task_logs::logged_at.ge(since_ms))
                    .into_boxed();

                if let Some(n) = task_name {
                    query = query.filter(task_logs::task_name.eq(n));
                }
                if let Some(l) = level {
                    query = query.filter(task_logs::level.eq(l));
                }
                if let Some(ns) = namespace {
                    query = query.filter(task_logs::namespace.eq(ns));
                }

                let rows = query
                    .order(task_logs::logged_at.desc())
                    .limit(limit)
                    .select(TaskLogRow::as_select())
                    .load::<TaskLogRow>(&mut conn)?;

                Ok(rows.into_iter().map(Into::into).collect())
            }

            /// Purge old log records.
            ///
            /// Deletes in bounded batches, each its own txn — see
            /// `diesel_common::purge`. This is the highest-volume purge (N rows
            /// per job), so it is the one an unbounded DELETE hurts most.
            pub fn purge_task_logs(&self, older_than_ms: i64) -> Result<u64> {
                $crate::storage::diesel_common::purge::drain_batches(|| {
                    self.write_transaction(|conn| {
                        let ids: Vec<String> = task_logs::table
                            .filter(task_logs::logged_at.lt(older_than_ms))
                            .select(task_logs::id)
                            .limit($crate::storage::diesel_common::purge::PURGE_BATCH)
                            .load(conn)?;
                        let affected =
                            diesel::delete(task_logs::table.filter(task_logs::id.eq_any(&ids)))
                                .execute(conn)?;
                        Ok(affected as u64)
                    })
                })
            }
        }
    };
}

pub(crate) use impl_diesel_log_ops;
