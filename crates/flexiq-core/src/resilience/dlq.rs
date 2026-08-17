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

    /// Move a failed job to the dead letter queue.
    pub(crate) fn move_to_dlq(&self, job: &Job, error: &str, metadata: Option<&str>) -> Result<()> {
        self.storage.move_to_dlq(job, error, metadata)
    }
}
