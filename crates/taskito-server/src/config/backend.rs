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

/// Pool sizes the migrating constructors pick by default, restated for the
/// unmigrated ones (which take them explicitly).
const SQLITE_POOL: u32 = 8;
#[cfg(feature = "postgres")]
const PG_POOL: u32 = 10;
#[cfg(feature = "postgres")]
const PG_SCHEMA: &str = "taskito";

/// Core storage and its workflow-aware wrapper, sharing one connection pool.
pub struct Backend {
    /// Job storage the scheduler and dashboard read.
    pub storage: StorageBackend,
    /// Workflow storage backing the workflow views.
    pub workflows: WorkflowStorageBackend,
}

/// Open the backend named by `dsn`. `hint` is `TASKITO_BACKEND` when set;
/// otherwise the URL scheme decides. `namespace` is `TASKITO_NAMESPACE`: it
/// scopes the workflow store the same way it already scopes the scheduler and
/// every dashboard view.
pub fn open(
    dsn: &str,
    hint: Option<&str>,
    namespace: Option<String>,
    auto_migrate: bool,
) -> Result<Backend> {
    let backend = match hint.map(str::to_ascii_lowercase).as_deref() {
        Some("sqlite") => open_sqlite(dsn, namespace, auto_migrate),
        Some("postgres") | Some("postgresql") => open_postgres(dsn, namespace, auto_migrate),
        Some("redis") => open_redis(dsn, namespace),
        Some(other) => bail!("TASKITO_BACKEND must be sqlite, postgres, or redis, got '{other}'"),
        None => open_by_scheme(dsn, namespace, auto_migrate),
    }?;
    // The server joins the deployment like any SDK process, so it is held to
    // the same contract floor rather than scheduling over data it cannot read.
    // Reading the floor needs the schema, which a gated deployment applies out
    // of band — say so rather than leaking a bare "no such table".
    taskito_core::ensure_contract_supported(&backend.storage).map_err(|error| {
        if auto_migrate {
            anyhow::Error::new(error)
        } else {
            anyhow::Error::new(error).context(
                "TASKITO_AUTO_MIGRATE is off, so this server applies no schema — \
                 run an SDK's `taskito migrate` against this database first",
            )
        }
    })?;
    Ok(backend)
}

fn open_by_scheme(dsn: &str, namespace: Option<String>, auto_migrate: bool) -> Result<Backend> {
    let lower = dsn.to_ascii_lowercase();
    if lower.starts_with("postgres://") || lower.starts_with("postgresql://") {
        open_postgres(dsn, namespace, auto_migrate)
    } else if lower.starts_with("redis://") || lower.starts_with("rediss://") {
        open_redis(dsn, namespace)
    } else {
        open_sqlite(dsn, namespace, auto_migrate)
    }
}

fn open_sqlite(dsn: &str, namespace: Option<String>, auto_migrate: bool) -> Result<Backend> {
    // Accept `sqlite:///abs/path`, `sqlite://:memory:`, a bare path, or `:memory:`.
    let path = dsn.strip_prefix("sqlite://").unwrap_or(dsn);
    if path.is_empty() {
        bail!("empty SQLite path; pass a file path, sqlite:///path, or :memory:");
    }
    let storage = if auto_migrate {
        SqliteStorage::new(path)
    } else {
        SqliteStorage::unmigrated(path, SQLITE_POOL)
    }
    .with_context(|| format!("failed to open SQLite database at '{path}'"))?;
    let workflows = if auto_migrate {
        WorkflowSqliteStorage::new(storage.clone(), namespace)
    } else {
        WorkflowSqliteStorage::unmigrated(storage.clone(), namespace)
    }
    .context("failed to initialise workflow tables")?;
    Ok(Backend {
        storage: StorageBackend::Sqlite(storage),
        workflows: WorkflowStorageBackend::Sqlite(workflows),
    })
}

#[cfg(feature = "postgres")]
fn open_postgres(dsn: &str, namespace: Option<String>, auto_migrate: bool) -> Result<Backend> {
    use taskito_core::PostgresStorage;
    use taskito_workflows::WorkflowPostgresStorage;

    // Don't interpolate the DSN — it may embed credentials.
    let storage = if auto_migrate {
        PostgresStorage::new(dsn)
    } else {
        PostgresStorage::unmigrated(dsn, PG_SCHEMA, PG_POOL)
    }
    .context("failed to connect to Postgres")?;
    let workflows = if auto_migrate {
        WorkflowPostgresStorage::new(storage.clone(), namespace)
    } else {
        WorkflowPostgresStorage::unmigrated(storage.clone(), namespace)
    }
    .context("failed to initialise workflow tables")?;
    Ok(Backend {
        storage: StorageBackend::Postgres(storage),
        workflows: WorkflowStorageBackend::Postgres(workflows),
    })
}

#[cfg(not(feature = "postgres"))]
fn open_postgres(_dsn: &str, _namespace: Option<String>, _auto_migrate: bool) -> Result<Backend> {
    bail!("Postgres backend not compiled in — rebuild with `--features postgres`.")
}

#[cfg(feature = "redis")]
fn open_redis(dsn: &str, namespace: Option<String>) -> Result<Backend> {
    use taskito_core::RedisStorage;
    use taskito_workflows::WorkflowRedisStorage;

    // Don't interpolate the DSN — it may embed credentials.
    let storage = RedisStorage::new(dsn).context("failed to connect to Redis")?;
    let workflows = WorkflowRedisStorage::new(storage.clone(), namespace)
        .context("failed to initialise workflow store")?;
    Ok(Backend {
        storage: StorageBackend::Redis(storage),
        workflows: WorkflowStorageBackend::Redis(workflows),
    })
}

#[cfg(not(feature = "redis"))]
fn open_redis(_dsn: &str, _namespace: Option<String>) -> Result<Backend> {
    bail!("Redis backend not compiled in — rebuild with `--features redis`.")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_unknown_backend_hint_is_rejected() {
        let error = open(":memory:", Some("mysql"), None, true)
            .err()
            .expect("must reject");
        assert!(error.to_string().contains("TASKITO_BACKEND"));
    }

    #[test]
    fn an_empty_sqlite_path_is_rejected() {
        let error = open("sqlite://", None, None, true)
            .err()
            .expect("must reject");
        assert!(error.to_string().contains("empty SQLite path"));
    }

    #[test]
    fn a_memory_dsn_opens() {
        open(":memory:", None, None, true).expect("in-memory SQLite opens");
    }

    #[test]
    fn a_gated_open_applies_no_ddl_and_says_so_when_the_schema_is_absent() {
        // TASKITO_AUTO_MIGRATE=off is for a deployment whose credentials do not
        // permit DDL: the schema must already be there, applied out of band by
        // an SDK's `migrate`. Against an empty database that is an operator
        // error, and the message has to name it.
        let dir = std::env::temp_dir().join(format!(
            "taskito-server-gated-{}",
            taskito_core::now_millis()
        ));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let path = dir.join("q.db");
        let dsn = path.to_str().expect("utf-8 path");

        let error = open(dsn, None, None, false).err().expect("must refuse");
        assert!(
            error.to_string().contains("TASKITO_AUTO_MIGRATE"),
            "unexpected message: {error}"
        );

        // Applied out of band, the same open succeeds and still ran no DDL of
        // its own — the migrating open is what created the schema.
        open(dsn, None, None, true).expect("migrating open");
        open(dsn, None, None, false).expect("gated open over a migrated database");

        std::fs::remove_dir_all(&dir).ok();
    }
}
