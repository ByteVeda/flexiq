//! Open a `StorageBackend` (+ matching workflow store) from `TASKITO_DSN`.
//!
//! Same dispatch as `crates/taskito-tui/src/backend.rs`, plus the explicit
//! `TASKITO_BACKEND` override the deployment contract exposes: a DSN whose
//! scheme is ambiguous (a bare SQLite path, a proxy URL) can still name its
//! backend. Postgres/Redis arms are `#[cfg]`-gated and a DSN for a backend that
//! was not compiled in fails with the feature to rebuild with.

use anyhow::{bail, Context, Result};

use taskito_core::{SqliteStorage, StorageBackend};
use taskito_workflows::{WorkflowSqliteStorage, WorkflowStorageBackend};

/// Core storage and its workflow-aware wrapper, sharing one connection pool.
pub struct Backend {
    /// Job storage the scheduler and dashboard read.
    pub storage: StorageBackend,
    /// Workflow storage backing the workflow views.
    pub workflows: WorkflowStorageBackend,
}

/// Open the backend named by `dsn`. `hint` is `TASKITO_BACKEND` when set;
/// otherwise the URL scheme decides.
pub fn open(dsn: &str, hint: Option<&str>) -> Result<Backend> {
    match hint.map(str::to_ascii_lowercase).as_deref() {
        Some("sqlite") => open_sqlite(dsn),
        Some("postgres") | Some("postgresql") => open_postgres(dsn),
        Some("redis") => open_redis(dsn),
        Some(other) => bail!("TASKITO_BACKEND must be sqlite, postgres, or redis, got '{other}'"),
        None => open_by_scheme(dsn),
    }
}

fn open_by_scheme(dsn: &str) -> Result<Backend> {
    let lower = dsn.to_ascii_lowercase();
    if lower.starts_with("postgres://") || lower.starts_with("postgresql://") {
        open_postgres(dsn)
    } else if lower.starts_with("redis://") || lower.starts_with("rediss://") {
        open_redis(dsn)
    } else {
        open_sqlite(dsn)
    }
}

fn open_sqlite(dsn: &str) -> Result<Backend> {
    // Accept `sqlite:///abs/path`, `sqlite://:memory:`, a bare path, or `:memory:`.
    let path = dsn.strip_prefix("sqlite://").unwrap_or(dsn);
    if path.is_empty() {
        bail!("empty SQLite path; pass a file path, sqlite:///path, or :memory:");
    }
    let storage = SqliteStorage::new(path)
        .with_context(|| format!("failed to open SQLite database at '{path}'"))?;
    let workflows = WorkflowSqliteStorage::new(storage.clone())
        .context("failed to initialise workflow tables")?;
    Ok(Backend {
        storage: StorageBackend::Sqlite(storage),
        workflows: WorkflowStorageBackend::Sqlite(workflows),
    })
}

#[cfg(feature = "postgres")]
fn open_postgres(dsn: &str) -> Result<Backend> {
    use taskito_core::PostgresStorage;
    use taskito_workflows::WorkflowPostgresStorage;

    // Don't interpolate the DSN — it may embed credentials.
    let storage = PostgresStorage::new(dsn).context("failed to connect to Postgres")?;
    let workflows = WorkflowPostgresStorage::new(storage.clone())
        .context("failed to initialise workflow tables")?;
    Ok(Backend {
        storage: StorageBackend::Postgres(storage),
        workflows: WorkflowStorageBackend::Postgres(workflows),
    })
}

#[cfg(not(feature = "postgres"))]
fn open_postgres(_dsn: &str) -> Result<Backend> {
    bail!("Postgres backend not compiled in — rebuild with `--features postgres`.")
}

#[cfg(feature = "redis")]
fn open_redis(dsn: &str) -> Result<Backend> {
    use taskito_core::RedisStorage;
    use taskito_workflows::WorkflowRedisStorage;

    // Don't interpolate the DSN — it may embed credentials.
    let storage = RedisStorage::new(dsn).context("failed to connect to Redis")?;
    let workflows = WorkflowRedisStorage::new(storage.clone())
        .context("failed to initialise workflow store")?;
    Ok(Backend {
        storage: StorageBackend::Redis(storage),
        workflows: WorkflowStorageBackend::Redis(workflows),
    })
}

#[cfg(not(feature = "redis"))]
fn open_redis(_dsn: &str) -> Result<Backend> {
    bail!("Redis backend not compiled in — rebuild with `--features redis`.")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_unknown_backend_hint_is_rejected() {
        let error = open(":memory:", Some("mysql")).err().expect("must reject");
        assert!(error.to_string().contains("TASKITO_BACKEND"));
    }

    #[test]
    fn an_empty_sqlite_path_is_rejected() {
        let error = open("sqlite://", None).err().expect("must reject");
        assert!(error.to_string().contains("empty SQLite path"));
    }

    #[test]
    fn a_memory_dsn_opens() {
        open(":memory:", None).expect("in-memory SQLite opens");
    }
}
