//! Shared read-only retention dry-run counts for the Diesel backends.
//!
//! Each count mirrors the predicate of its purge counterpart in
//! `diesel_common::{jobs, dead_letter, logs, metrics}` exactly, so the dry-run
//! reports the number of rows an actual purge would remove. Counts are cheap
//! indexed aggregates — no rows are loaded or deleted.

/// Generates the `count_expired_rows` inherent method for a Diesel-backed
/// storage backend. The invocation site must bring the five history-table
/// schemas (`archived_jobs`, `dead_letter`, `task_logs`, `task_metrics`,
/// `job_errors`) and `diesel::prelude::*` into scope.
macro_rules! impl_diesel_retention_ops {
    ($storage_type:ty) => {
        impl $storage_type {
            /// Count the rows each retention purge would delete under `cutoffs`,
            /// without deleting anything. Mirrors the purge predicates: the two
            /// blob-carrying tables (`archived_jobs`/`dead_letter`) always count
            /// per-entry-TTL-expired rows against `now` and add the global
            /// window when its cutoff is set; the side tables count only when a
            /// cutoff is set.
            pub fn count_expired_rows(
                &self,
                cutoffs: &$crate::storage::RetentionCutoffs,
                now: i64,
            ) -> Result<$crate::storage::RetentionCounts> {
                let mut conn = self.conn()?;

                // archived_jobs — global (no per-entry TTL, past the cutoff) plus
                // per-entry (`completed_at + result_ttl_ms < now`). Disjoint on
                // `result_ttl_ms IS NULL`, so the two counts never overlap.
                let archived_global = match cutoffs.archived_jobs {
                    Some(cutoff) => archived_jobs::table
                        .filter(archived_jobs::result_ttl_ms.is_null())
                        .filter(archived_jobs::completed_at.lt(cutoff))
                        .count()
                        .get_result::<i64>(&mut conn)? as u64,
                    None => 0,
                };
                let archived_per_entry = archived_jobs::table
                    .filter(archived_jobs::result_ttl_ms.is_not_null())
                    .filter(archived_jobs::completed_at.is_not_null())
                    .filter(
                        (archived_jobs::completed_at.assume_not_null()
                            + archived_jobs::result_ttl_ms.assume_not_null())
                        .lt(now),
                    )
                    .count()
                    .get_result::<i64>(&mut conn)? as u64;

                // dead_letter — same global/per-entry split (`failed_at` is not
                // nullable, so no `assume_not_null` on it).
                let dead_global = match cutoffs.dead_letter {
                    Some(cutoff) => dead_letter::table
                        .filter(dead_letter::result_ttl_ms.is_null())
                        .filter(dead_letter::failed_at.lt(cutoff))
                        .count()
                        .get_result::<i64>(&mut conn)? as u64,
                    None => 0,
                };
                let dead_per_entry = dead_letter::table
                    .filter(dead_letter::result_ttl_ms.is_not_null())
                    .filter(
                        (dead_letter::failed_at + dead_letter::result_ttl_ms.assume_not_null())
                            .lt(now),
                    )
                    .count()
                    .get_result::<i64>(&mut conn)? as u64;

                // Side tables — a single-column age comparison, only when the
                // table has a window.
                let task_logs = match cutoffs.task_logs {
                    Some(cutoff) => task_logs::table
                        .filter(task_logs::logged_at.lt(cutoff))
                        .count()
                        .get_result::<i64>(&mut conn)? as u64,
                    None => 0,
                };
                let task_metrics = match cutoffs.task_metrics {
                    Some(cutoff) => task_metrics::table
                        .filter(task_metrics::recorded_at.lt(cutoff))
                        .count()
                        .get_result::<i64>(&mut conn)? as u64,
                    None => 0,
                };
                let job_errors = match cutoffs.job_errors {
                    Some(cutoff) => job_errors::table
                        .filter(job_errors::failed_at.lt(cutoff))
                        .count()
                        .get_result::<i64>(&mut conn)? as u64,
                    None => 0,
                };

                Ok($crate::storage::RetentionCounts {
                    archived_jobs: archived_global + archived_per_entry,
                    dead_letter: dead_global + dead_per_entry,
                    task_logs,
                    task_metrics,
                    job_errors,
                })
            }
        }
    };
}

pub(crate) use impl_diesel_retention_ops;
