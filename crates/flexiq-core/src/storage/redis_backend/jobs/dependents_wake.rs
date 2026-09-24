//! Push-dispatch wake for the dependents a completion may have unblocked.
//!
//! A dependent skipped by a drain while its parent ran has no enqueue left to
//! announce it, and the scheduler serving its queue may not be the one that
//! finished the parent — so the completion publishes on the dependents' own
//! `(namespace, queue)` channels, or they wait out the fallback poll.

use std::collections::BTreeMap;

use crate::error::Result;
use crate::job::{Job, JobStatus};
use crate::storage::redis_backend::{map_err, RedisConnection, RedisStorage};

impl RedisStorage {
    /// [`archive_job_immediately`](Self::archive_job_immediately) for a
    /// completion, also returning the job's dependents. The `SMEMBERS` rides the
    /// archive's own transaction, so a job without dependents pays no extra
    /// round trip for the wake.
    pub(in crate::storage::redis_backend) fn archive_reading_dependents(
        &self,
        conn: &mut RedisConnection,
        job: &Job,
        old_status: JobStatus,
    ) -> Result<Vec<String>> {
        let job_json = serde_json::to_string(job)?;
        let pipe = &mut redis::pipe();
        pipe.atomic();
        self.push_archive_ops(pipe, job, old_status, &job_json);
        pipe.smembers(self.key(&["job", &job.id, "dependents"]));
        let (dependents,): (Vec<String>,) = pipe.query(conn).map_err(map_err)?;
        Ok(dependents)
    }

    /// Wake the schedulers serving each distinct `(namespace, queue)` holding a
    /// pending dependent in `ids`, once per pair with its earliest
    /// `scheduled_at`. Best-effort: the completion has already committed, so a
    /// failed read or a refused `PUBLISH` only costs latency and is logged.
    pub(in crate::storage::redis_backend) fn wake_dependents(
        &self,
        conn: &mut RedisConnection,
        ids: &[String],
    ) {
        if ids.is_empty() {
            return;
        }
        let keys: Vec<String> = ids.iter().map(|id| self.key(&["job", id])).collect();
        let docs: Vec<Option<String>> = match redis::cmd("MGET").arg(&keys).query(conn) {
            Ok(docs) => docs,
            Err(e) => {
                log::warn!("push-dispatch: reading dependents to wake failed: {e}");
                return;
            }
        };
        let mut earliest: BTreeMap<(Option<String>, String), i64> = BTreeMap::new();
        for doc in docs.iter().flatten() {
            let job: Job = match serde_json::from_str(doc) {
                Ok(job) => job,
                Err(e) => {
                    log::warn!("push-dispatch: undecodable dependent skipped for wake: {e}");
                    continue;
                }
            };
            if job.status != JobStatus::Pending {
                continue;
            }
            earliest
                .entry((job.namespace, job.queue))
                .and_modify(|at| *at = (*at).min(job.scheduled_at))
                .or_insert(job.scheduled_at);
        }
        if earliest.is_empty() {
            return;
        }
        let mut pipe = redis::pipe();
        for ((namespace, queue), scheduled_at) in &earliest {
            self.fold_notify(&mut pipe, namespace.as_deref(), queue, *scheduled_at);
        }
        if let Err(e) = self.exec_enqueue_pipe(&pipe, conn) {
            log::warn!("push-dispatch: dependent wake PUBLISH failed: {e}");
        }
    }
}
