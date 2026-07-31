//! How a dispatcher learns that a running job was cancelled.
//!
//! An in-process worker reads the storage flag `Storage::request_cancel` sets.
//! An attached executor has no storage — it is the whole point of #546 that it
//! carries no database credentials — and learns instead from the `cancel` frame
//! the scheduler sends, which arrives as [`WorkerDispatcher::notify_cancel`].
//!
//! Both sources answer one question, so both dispatchers ask it here rather
//! than growing two cancel paths each.

use std::collections::HashSet;
use std::sync::{Mutex, PoisonError};

use crate::storage::{Storage, StorageBackend};

/// The cancel sources available to one dispatcher.
pub struct CancelSignals {
    /// Present only for a dispatcher running inside a worker.
    storage: Option<StorageBackend>,
    /// Ids delivered out of band. Kept until the job reports, so a cancel that
    /// races a job's start still fires rather than being missed.
    signalled: Mutex<HashSet<String>>,
}

impl CancelSignals {
    /// Read cancels from storage, and from `notify_cancel` when it is called.
    pub fn from_storage(storage: StorageBackend) -> Self {
        Self {
            storage: Some(storage),
            signalled: Mutex::new(HashSet::new()),
        }
    }

    /// Read cancels only from `notify_cancel` — the attached-executor case.
    pub fn detached() -> Self {
        Self {
            storage: None,
            signalled: Mutex::new(HashSet::new()),
        }
    }

    /// Record a cancel request for `job_id`.
    pub fn signal(&self, job_id: &str) {
        self.lock().insert(job_id.to_string());
    }

    /// Whether `job_id` has been cancelled.
    ///
    /// The out-of-band set is checked first: it needs no I/O, and for a
    /// detached executor it is the only answer there is.
    pub fn is_cancelled(&self, job_id: &str) -> bool {
        if self.lock().contains(job_id) {
            return true;
        }
        self.storage
            .as_ref()
            .is_some_and(|storage| storage.is_cancel_requested(job_id).unwrap_or(false))
    }

    /// Drop the record for a finished job, so the set cannot grow for the life
    /// of the process.
    pub fn forget(&self, job_id: &str) {
        self.lock().remove(job_id);
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashSet<String>> {
        // The state behind the lock is a plain set, so reading it stays safe
        // even if a holder panicked.
        self.signalled
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_detached_signal_is_observed_without_storage() {
        let signals = CancelSignals::detached();
        assert!(!signals.is_cancelled("job-1"));

        signals.signal("job-1");
        assert!(signals.is_cancelled("job-1"));
    }

    #[test]
    fn forgetting_a_job_clears_its_signal() {
        // Ids are held until the job reports, so they have to be released or
        // the set grows for the life of the process.
        let signals = CancelSignals::detached();
        signals.signal("job-1");
        signals.forget("job-1");
        assert!(!signals.is_cancelled("job-1"));
    }

    #[test]
    fn an_unknown_job_is_not_cancelled() {
        let signals = CancelSignals::detached();
        signals.signal("job-1");
        assert!(!signals.is_cancelled("job-2"));
    }

    #[test]
    fn a_storage_flag_is_honoured_too() {
        use crate::job::{now_millis, NewJob};
        use crate::storage::sqlite::SqliteStorage;

        let storage = StorageBackend::Sqlite(SqliteStorage::in_memory().expect("in-memory"));
        let job = storage
            .enqueue(NewJob {
                queue: "default".to_string(),
                task_name: "resize".to_string(),
                payload: Vec::new(),
                priority: 0,
                scheduled_at: now_millis(),
                max_retries: 0,
                timeout_ms: 0,
                unique_key: None,
                metadata: None,
                notes: None,
                depends_on: vec![],
                expires_at: None,
                result_ttl_ms: None,
                namespace: None,
            })
            .expect("enqueue");

        // `request_cancel` only flags a *running* job — a pending one is
        // cancelled outright — so the job has to be dequeued first.
        let running = storage
            .dequeue("default", now_millis() + 1_000, None)
            .expect("dequeue")
            .expect("the enqueued job");
        assert_eq!(running.id, job.id);

        let signals = CancelSignals::from_storage(storage.clone());
        assert!(!signals.is_cancelled(&job.id));

        assert!(storage.request_cancel(&job.id).expect("request cancel"));
        assert!(signals.is_cancelled(&job.id));
    }
}
