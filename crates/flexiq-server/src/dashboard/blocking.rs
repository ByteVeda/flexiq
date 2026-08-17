//! Run blocking storage work off the async runtime.
//!
//! Every `Storage` call is blocking — Diesel pools and the Redis client are
//! synchronous. Calling one directly from an axum handler would park a runtime
//! worker thread for the duration of the query, so a handful of slow listings
//! would stall the whole dashboard.

use anyhow::anyhow;
use flexiq_core::StorageBackend;
use flexiq_workflows::WorkflowStorageBackend;

use crate::dashboard::error::{ApiError, ApiResult};
use crate::dashboard::state::SharedState;

/// Run `work` against a cloned storage handle on the blocking pool.
pub async fn on_storage<T, F>(state: &SharedState, work: F) -> ApiResult<T>
where
    F: FnOnce(&StorageBackend) -> flexiq_core::Result<T> + Send + 'static,
    T: Send + 'static,
{
    let storage = state.storage.clone();
    join(tokio::task::spawn_blocking(move || work(&storage)).await)?.map_err(ApiError::from)
}

/// Like [`on_storage`], for work that can fail with an [`ApiError`] of its own
/// — validation that has to read storage before it can decide.
pub async fn on_storage_api<T, F>(state: &SharedState, work: F) -> ApiResult<T>
where
    F: FnOnce(&StorageBackend) -> ApiResult<T> + Send + 'static,
    T: Send + 'static,
{
    let storage = state.storage.clone();
    join(tokio::task::spawn_blocking(move || work(&storage)).await)?
}

/// Run `work` against a cloned workflow-storage handle on the blocking pool.
pub async fn on_workflows<T, F>(state: &SharedState, work: F) -> ApiResult<T>
where
    F: FnOnce(&WorkflowStorageBackend) -> ApiResult<T> + Send + 'static,
    T: Send + 'static,
{
    let workflows = state.workflows.clone();
    join(tokio::task::spawn_blocking(move || work(&workflows)).await)?
}

/// A panicking blocking task is a bug, not a client error — surface it as 500
/// rather than letting the `JoinError` bubble as an opaque runtime failure.
fn join<T>(result: Result<T, tokio::task::JoinError>) -> ApiResult<T> {
    result.map_err(|error| ApiError::Internal(anyhow!("storage task failed: {error}")))
}
