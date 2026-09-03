//! Running a blocking `Storage` call from an async gRPC handler.
//!
//! Every `Storage` method is synchronous — the Diesel pools and the Redis
//! client both are — so calling one straight from a tonic handler would park a
//! runtime worker thread for the length of a database round trip. The dashboard
//! has the same problem and the same answer; this is that answer for the gRPC
//! side, where the error type on the other end is a [`Status`] rather than an
//! HTTP response.

use flexiq_core::StorageBackend;
use flexiq_workflows::WorkflowStorageBackend;
use tonic::Status;

use super::status::{self, WireError};

/// Run `work` on the blocking pool and map its failure onto the wire.
///
/// The handle is cloned rather than shared: `StorageBackend` clones a
/// connection pool handle, not a connection.
pub async fn on_storage<T, F>(storage: &StorageBackend, work: F) -> Result<T, Status>
where
    F: FnOnce(&StorageBackend) -> flexiq_core::Result<T> + Send + 'static,
    T: Send + 'static,
{
    let storage = storage.clone();
    match tokio::task::spawn_blocking(move || work(&storage)).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(status::from_queue_error(&error)),
        // The closure panicked, or the runtime is shutting down. Neither is
        // something the caller can act on, and the panic message is ours.
        Err(error) => {
            log::error!("grpc: storage task failed to run: {error}");
            Err(WireError::internal().into())
        }
    }
}

/// Run a read-only `work` against workflow storage on the blocking pool.
pub async fn on_workflows<T, F>(workflows: &WorkflowStorageBackend, work: F) -> Result<T, Status>
where
    F: FnOnce(&WorkflowStorageBackend) -> flexiq_core::Result<T> + Send + 'static,
    T: Send + 'static,
{
    let workflows = workflows.clone();
    match tokio::task::spawn_blocking(move || work(&workflows)).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(status::from_queue_error(&error)),
        Err(error) => {
            log::error!("grpc: workflow storage task failed to run: {error}");
            Err(WireError::internal().into())
        }
    }
}

/// Run `work` on the blocking pool with both storage handles at once.
///
/// `SubmitWorkflow` needs both in the same call — a job's storage handle to
/// pre-enqueue nodes, workflow storage to write the run and its nodes — and
/// running them as one blocking task is what `flexiq_workflows::lifecycle::submit_workflow`
/// already assumes of its two `&` parameters.
///
/// Generic over the closure's error type — `SubmitWorkflow` distinguishes a
/// caller mistake from a storage failure (`SubmitWorkflowError`), which a
/// `QueueError`-only signature could not carry — so the caller supplies its
/// own `Into<WireError>` rather than this function assuming `QueueError`.
pub async fn on_storage_and_workflows<T, E, F>(
    storage: &StorageBackend,
    workflows: &WorkflowStorageBackend,
    work: F,
) -> Result<T, Status>
where
    F: FnOnce(&StorageBackend, &WorkflowStorageBackend) -> std::result::Result<T, E>
        + Send
        + 'static,
    E: Into<WireError> + Send + 'static,
    T: Send + 'static,
{
    let storage = storage.clone();
    let workflows = workflows.clone();
    match tokio::task::spawn_blocking(move || work(&storage, &workflows)).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(error.into().into()),
        Err(error) => {
            log::error!("grpc: storage task failed to run: {error}");
            Err(WireError::internal().into())
        }
    }
}
