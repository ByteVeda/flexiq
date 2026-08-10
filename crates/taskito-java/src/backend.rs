//! Construct a [`QueueHandle`] from caller options (mirrors the Node shell).

use taskito_core::{NewJob, SqliteStorage, StorageBackend};

use crate::convert::OpenOptions;
use crate::error::BindingError;

const DEFAULT_SQLITE_POOL: u32 = 8;
#[cfg(feature = "postgres")]
const DEFAULT_POSTGRES_POOL: u32 = 10;
#[cfg(feature = "postgres")]
const DEFAULT_POSTGRES_SCHEMA: &str = "taskito";

/// An open queue: the storage backend plus the default namespace applied to
/// enqueues that do not specify their own.
pub struct QueueHandle {
    pub storage: StorageBackend,
    pub namespace: Option<String>,
    /// Workflow storage, built lazily on first workflow call (its constructor
    /// runs the workflow-table migrations). Shares this queue's connection pool.
    #[cfg(feature = "workflows")]
    pub workflow_storage: std::sync::OnceLock<taskito_workflows::WorkflowStorageBackend>,
    /// Serializes the lazy init above: the constructor runs DDL migrations, and
    /// concurrent `CREATE TABLE/INDEX IF NOT EXISTS` from separate sessions can
    /// race in Postgres's catalog, failing one thread's first workflow call.
    #[cfg(feature = "workflows")]
    workflow_init: std::sync::Mutex<()>,
}

#[cfg(feature = "workflows")]
impl QueueHandle {
    /// Return the workflow storage, initializing it (and its migrations) once.
    pub fn workflow_store(
        &self,
    ) -> Result<taskito_workflows::WorkflowStorageBackend, BindingError> {
        if let Some(wf) = self.workflow_storage.get() {
            return Ok(wf.clone());
        }
        // A poisoned lock only means another thread panicked mid-init; the
        // OnceLock still tells the truth, so recover and retry the init.
        let _init = self
            .workflow_init
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(wf) = self.workflow_storage.get() {
            return Ok(wf.clone()); // another thread won the race while we waited
        }
        let wf = build_workflow_storage(&self.storage, self.namespace.clone())?;
        let _ = self.workflow_storage.set(wf.clone());
        Ok(wf)
    }
}

/// Construct workflow storage matching this queue's core backend.
#[cfg(feature = "workflows")]
fn build_workflow_storage(
    storage: &StorageBackend,
    namespace: Option<String>,
) -> Result<taskito_workflows::WorkflowStorageBackend, BindingError> {
    use taskito_workflows::{WorkflowSqliteStorage, WorkflowStorageBackend};
    let wf = match storage {
        StorageBackend::Sqlite(s) => {
            WorkflowStorageBackend::Sqlite(WorkflowSqliteStorage::new(s.clone(), namespace)?)
        }
        #[cfg(feature = "postgres")]
        StorageBackend::Postgres(s) => WorkflowStorageBackend::Postgres(
            taskito_workflows::WorkflowPostgresStorage::new(s.clone(), namespace)?,
        ),
        #[cfg(feature = "redis")]
        StorageBackend::Redis(s) => WorkflowStorageBackend::Redis(
            taskito_workflows::WorkflowRedisStorage::new(s.clone(), namespace)?,
        ),
    };
    Ok(wf)
}

/// Enqueue a batch, routing any job with a `unique_key` through the dedup path
/// (matching single `enqueue`) instead of the raw batch insert. Ids come back in
/// input order, a duplicate key resolving to the existing job's id.
pub fn enqueue_batch_dedup(
    storage: &StorageBackend,
    jobs: Vec<NewJob>,
) -> Result<Vec<String>, BindingError> {
    let created = taskito_core::storage::enqueue_batch_dedup(storage, jobs)?;
    Ok(created.into_iter().map(|job| job.id).collect())
}

/// Drop ephemeral topic subscriptions whose owning worker is no longer in the
/// worker registry. Durable rows and rows owned by a live worker are untouched.
/// Callers prune dead workers first — a stale registry row keeps its
/// subscriptions alive. Returns the count removed.
pub fn reap_ephemeral_subscriptions(storage: &StorageBackend) -> Result<u64, BindingError> {
    Ok(taskito_core::storage::sweep_ephemeral_subscriptions(
        storage, None,
    )?)
}

/// Error for a backend that is unknown or whose cargo feature is not compiled in.
fn unknown_backend(name: &str) -> BindingError {
    BindingError::new(format!(
        "backend '{name}' is not available (unknown, or this build omits its cargo feature)"
    ))
}

/// Reject an explicit zero pool size — r2d2 panics when `max_size == 0`, which
/// would take down the whole JVM.
fn resolve_pool_size(pool_size: Option<u32>, default: u32) -> Result<u32, BindingError> {
    match pool_size {
        Some(0) => Err(BindingError::new("poolSize must be greater than 0")),
        Some(n) => Ok(n),
        None => Ok(default),
    }
}

/// Open the storage backend named by `options.backend` (default `"sqlite"`).
/// Returns an error if a requested backend was not compiled into this library.
pub fn open(options: OpenOptions) -> Result<QueueHandle, BindingError> {
    let storage = match options.backend.as_deref().unwrap_or("sqlite") {
        "sqlite" => {
            let pool = resolve_pool_size(options.pool_size, DEFAULT_SQLITE_POOL)?;
            StorageBackend::Sqlite(SqliteStorage::with_pool_size(&options.dsn, pool)?)
        }
        #[cfg(feature = "postgres")]
        "postgres" => {
            let schema = options.schema.as_deref().unwrap_or(DEFAULT_POSTGRES_SCHEMA);
            let pool = resolve_pool_size(options.pool_size, DEFAULT_POSTGRES_POOL)?;
            StorageBackend::Postgres(taskito_core::PostgresStorage::with_schema_and_pool_size(
                &options.dsn,
                schema,
                pool,
            )?)
        }
        #[cfg(feature = "redis")]
        "redis" => {
            let storage = match options.prefix.as_deref() {
                Some(prefix) => taskito_core::RedisStorage::with_prefix(&options.dsn, prefix),
                None => taskito_core::RedisStorage::new(&options.dsn),
            }?;
            StorageBackend::Redis(storage)
        }
        other => return Err(unknown_backend(other)),
    };
    // Every process that joins a deployment passes through this one open, so
    // the contract floor is checked here rather than in the SDK.
    taskito_core::ensure_contract_supported(&storage)?;
    Ok(QueueHandle {
        storage,
        namespace: options.namespace,
        #[cfg(feature = "workflows")]
        workflow_storage: std::sync::OnceLock::new(),
        #[cfg(feature = "workflows")]
        workflow_init: std::sync::Mutex::new(()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use taskito_core::Storage;

    fn new_job(unique_key: Option<&str>) -> NewJob {
        NewJob {
            queue: "default".into(),
            task_name: "task".into(),
            payload: Vec::new(),
            priority: 0,
            scheduled_at: 0,
            max_retries: 0,
            timeout_ms: 0,
            unique_key: unique_key.map(str::to_owned),
            metadata: None,
            notes: None,
            depends_on: Vec::new(),
            expires_at: None,
            result_ttl_ms: None,
            namespace: None,
        }
    }

    /// A batch containing an active duplicate `unique_key` must dedup that job
    /// to the existing id instead of failing the whole batch on the unique index.
    #[test]
    fn batch_dedup_resolves_duplicate_unique_keys() {
        let storage = StorageBackend::Sqlite(SqliteStorage::in_memory().expect("storage"));
        let existing = storage
            .enqueue_unique(new_job(Some("k1")))
            .expect("seed enqueue");
        let ids = enqueue_batch_dedup(
            &storage,
            vec![new_job(Some("k1")), new_job(Some("k2")), new_job(None)],
        )
        .expect("batch enqueue");
        assert_eq!(ids.len(), 3);
        assert_eq!(
            ids[0], existing.id,
            "duplicate key resolves to existing job"
        );
        assert_ne!(ids[1], ids[0]);
        assert_ne!(ids[2], ids[0]);
    }
}
