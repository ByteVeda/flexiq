//! Running a blocking `Storage` call from an async gRPC handler.
//!
//! Every `Storage` method is synchronous — the Diesel pools and the Redis
//! client both are — so calling one straight from a tonic handler would park a
//! runtime worker thread for the length of a database round trip. The dashboard
//! has the same problem and the same answer; this is that answer for the gRPC
//! side, where the error type on the other end is a [`Status`] rather than an
//! HTTP response.

use flexiq_core::StorageBackend;
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
