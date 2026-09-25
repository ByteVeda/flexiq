use crate::error::Result;
use crate::job::Job;
use crate::storage::{Storage, StorageBackend};

/// Dead letter queue manager. In-crate only: consumed solely by `Scheduler`;
/// bindings drive the DLQ through `Storage` methods directly.
pub(crate) struct DeadLetterQueue {
    storage: StorageBackend,
}

impl DeadLetterQueue {
    /// Build a DLQ manager over `storage`.
    pub(crate) fn new(storage: StorageBackend) -> Self {
        Self { storage }
    }

    /// Move a *failed* job to the dead letter queue, returning the dependents
    /// the cascade cancelled so the caller can report them. The shed paths
    /// call `Storage::shed_to_dlq_reporting` directly so their entries are
    /// flagged.
    pub(crate) fn move_to_dlq_reporting(
        &self,
        job: &Job,
        error: &str,
        metadata: Option<&str>,
    ) -> Result<Vec<Job>> {
        self.storage.move_to_dlq_reporting(job, error, metadata)
    }
}
