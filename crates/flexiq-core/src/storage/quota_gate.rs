//! Namespace quotas on the backend-agnostic handle (#841).
//!
//! `StorageBackend`'s enqueue forwarders are the one place every producer door
//! meets — shells, the gRPC producer, triggers, replays, periodic fires — so
//! depth and rate quotas are admitted here and nowhere else.

use std::collections::BTreeMap;

use super::{Storage, StorageBackend};
use crate::error::{QueueError, Result};
use crate::job::{now_millis, Job, JobStatus, NewJob};
use crate::quota::admission::admit;
use crate::quota::{quota_key, read_quota, NamespaceQuota, QuotaCache};
use crate::scheduler::shed::QUOTA_SHED_METADATA;

impl StorageBackend {
    /// This handle's quota cache, shared by every clone.
    pub(crate) fn quota_cache(&self) -> &QuotaCache {
        match self {
            StorageBackend::Sqlite(s) => s.quota_cache(),
            #[cfg(feature = "postgres")]
            StorageBackend::Postgres(s) => s.quota_cache(),
            #[cfg(feature = "redis")]
            StorageBackend::Redis(s) => s.quota_cache(),
        }
    }

    /// `namespace`'s quota, read fresh — `None` when it has none. An
    /// unreadable document is an error, never "unlimited".
    pub fn namespace_quota(&self, namespace: Option<&str>) -> Result<Option<NamespaceQuota>> {
        read_quota(self, namespace)
    }

    /// Replace `namespace`'s quota. Validated first, so nothing that would
    /// fail every enqueue closed is ever stored. Other processes see it
    /// within [`QUOTA_CACHE_TTL`](crate::quota::QUOTA_CACHE_TTL).
    pub fn set_namespace_quota(
        &self,
        namespace: Option<&str>,
        quota: &NamespaceQuota,
    ) -> Result<()> {
        self.set_setting(&quota_key(namespace), &quota.to_json()?)?;
        self.quota_cache().invalidate(namespace);
        Ok(())
    }

    /// Remove `namespace`'s quota, making it unlimited. `false` when it had none.
    pub fn clear_namespace_quota(&self, namespace: Option<&str>) -> Result<bool> {
        let removed = self.delete_setting(&quota_key(namespace))?;
        self.quota_cache().invalidate(namespace);
        Ok(removed)
    }

    /// Run one enqueue call through its namespaces' depth and rate quotas.
    /// Admitted jobs go to `enqueue`; jobs a `drop` quota sheds are
    /// dead-lettered without ever going live and come back as `Dead` through
    /// `shed`. Results keep call order. A `reject` quota fails the whole call
    /// before anything is written.
    ///
    /// Only a call that mixes admitted and shed jobs — one batch spanning
    /// namespaces — writes twice, and no backend offers one transaction over
    /// both. The admitted enqueue goes first and decides the call's outcome:
    /// once any write has committed, a shed record that fails is logged, not
    /// returned. The shed jobs are discarded either way; answering with an
    /// error would tell the caller its committed jobs failed, and a retry
    /// would enqueue them twice.
    pub(super) fn with_quota<T>(
        &self,
        new_jobs: Vec<NewJob>,
        enqueue: impl FnOnce(Vec<NewJob>) -> Result<Vec<T>>,
        shed: impl Fn(Job) -> T,
    ) -> Result<Vec<T>> {
        let verdicts = admit(self, self.quota_cache(), &new_jobs)?;
        if verdicts.iter().all(Option::is_none) {
            return enqueue(new_jobs);
        }

        let mut results: Vec<Option<T>> = Vec::with_capacity(new_jobs.len());
        let mut admitted = Vec::new();
        let mut by_reason: BTreeMap<String, Vec<(usize, Job)>> = BTreeMap::new();
        for (i, (new_job, verdict)) in new_jobs.into_iter().zip(verdicts).enumerate() {
            results.push(None);
            match verdict {
                None => admitted.push((i, new_job)),
                Some(reason) => {
                    let job = shed_copy(new_job, &reason);
                    by_reason.entry(reason).or_default().push((i, job));
                }
            }
        }

        let mut committed = false;
        if !admitted.is_empty() {
            let (positions, jobs): (Vec<usize>, Vec<NewJob>) = admitted.into_iter().unzip();
            for (i, result) in positions.into_iter().zip(enqueue(jobs)?) {
                results[i] = Some(result);
            }
            committed = true;
        }

        for (reason, jobs) in by_reason {
            let dead: Vec<Job> = jobs.iter().map(|(_, job)| job.clone()).collect();
            match self.shed_new_jobs(&dead, &reason, Some(QUOTA_SHED_METADATA)) {
                Ok(()) => committed = true,
                Err(error) if committed => log::error!(
                    "recording {} quota-shed job(s) failed after the rest of the call \
                     committed; they are dropped without a dead-letter entry: {error}",
                    dead.len()
                ),
                Err(error) => return Err(error),
            }
            for (i, job) in jobs {
                results[i] = Some(shed(job));
            }
        }
        // Every slot was filled by exactly one of the two branches above.
        Ok(results.into_iter().flatten().collect())
    }
}

/// The one result of a single-job [`StorageBackend::with_quota`] call.
pub(super) fn single<T>(mut results: Vec<T>) -> Result<T> {
    results
        .pop()
        .ok_or_else(|| QueueError::Other("enqueue returned no job".to_string()))
}

/// `new_job` as a shed enqueue records it: `Dead` with `reason`, holding no
/// unique or debounce key, since it never went live to hold one.
fn shed_copy(new_job: NewJob, reason: &str) -> Job {
    let mut job = new_job.into_job();
    job.status = JobStatus::Dead;
    job.error = Some(reason.to_string());
    job.completed_at = Some(now_millis());
    job.unique_key = None;
    job.debounce_key = None;
    job
}
