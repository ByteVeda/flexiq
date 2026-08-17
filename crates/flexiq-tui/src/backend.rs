//! Open a `StorageBackend` (+ matching workflow store) from a connection URL.
//!
//! Mirrors the backend dispatch in the Python binding
//! (`crates/flexiq-python/src/py_queue/mod.rs`), but sniffs the backend from
//! the URL scheme instead of a separate argument. Postgres/Redis arms are
//! `#[cfg]`-gated; a URL for a disabled backend fails with a clear message
//! telling the user which feature to rebuild with.
//!
//! Deliberately **not** held to the contract floor the SDKs and the server
//! check on open: the TUI inspects a deployment rather than joining it, and a
//! floor raised past this build is exactly the situation an operator needs to
//! be able to look at.

use anyhow::{bail, Context, Result};

use flexiq_core::{SqliteStorage, StorageBackend};
use flexiq_workflows::{WorkflowSqliteStorage, WorkflowStorageBackend};

/// A connected pair of core storage and its workflow-aware wrapper, both sharing
/// the same underlying connection pool.
pub struct Backend {
    pub storage: StorageBackend,
    pub workflows: WorkflowStorageBackend,
}

/// Open the backend named by `url`. Scheme decides the backend:
/// `postgres://`/`postgresql://`, `redis://`/`rediss://`, otherwise SQLite
/// (with an optional `sqlite://` prefix, a bare file path, or `:memory:`).
pub fn open(url: &str) -> Result<Backend> {
    let lower = url.to_ascii_lowercase();
    if lower.starts_with("postgres://") || lower.starts_with("postgresql://") {
        open_postgres(url)
    } else if lower.starts_with("redis://") || lower.starts_with("rediss://") {
        open_redis(url)
    } else {
        open_sqlite(url)
    }
}

fn open_sqlite(url: &str) -> Result<Backend> {
    // Accept `sqlite:///abs/path`, `sqlite://:memory:`, a bare path, or `:memory:`.
    let path = url.strip_prefix("sqlite://").unwrap_or(url);
    if path.is_empty() {
        bail!("empty SQLite path; pass a file path, sqlite:///path, or :memory:");
    }
    // NB: opening runs schema migrations and a one-time archive sweep — this is
    // not a passive read-only open. See `--help` and the crate docs.
    let storage = SqliteStorage::new(path)
        .with_context(|| format!("failed to open SQLite database at '{path}'"))?;
    let workflows = WorkflowSqliteStorage::new(storage.clone(), None)
        .context("failed to initialise workflow tables")?;
    Ok(Backend {
        storage: StorageBackend::Sqlite(storage),
        workflows: WorkflowStorageBackend::Sqlite(workflows),
    })
}

#[cfg(feature = "postgres")]
fn open_postgres(url: &str) -> Result<Backend> {
    use flexiq_core::PostgresStorage;
    use flexiq_workflows::WorkflowPostgresStorage;

    // Don't interpolate the URL — it may embed credentials.
    let storage = PostgresStorage::new(url).context("failed to connect to Postgres")?;
    let workflows = WorkflowPostgresStorage::new(storage.clone(), None)
        .context("failed to initialise workflow tables")?;
    Ok(Backend {
        storage: StorageBackend::Postgres(storage),
        workflows: WorkflowStorageBackend::Postgres(workflows),
    })
}

#[cfg(not(feature = "postgres"))]
fn open_postgres(_url: &str) -> Result<Backend> {
    bail!("Postgres backend not compiled in — rebuild with `--features postgres`.")
}

#[cfg(feature = "redis")]
fn open_redis(url: &str) -> Result<Backend> {
    use flexiq_core::RedisStorage;
    use flexiq_workflows::WorkflowRedisStorage;

    // Don't interpolate the URL — it may embed credentials.
    let storage = RedisStorage::new(url).context("failed to connect to Redis")?;
    let workflows = WorkflowRedisStorage::new(storage.clone(), None)
        .context("failed to initialise workflow store")?;
    Ok(Backend {
        storage: StorageBackend::Redis(storage),
        workflows: WorkflowStorageBackend::Redis(workflows),
    })
}

#[cfg(not(feature = "redis"))]
fn open_redis(_url: &str) -> Result<Backend> {
    bail!("Redis backend not compiled in — rebuild with `--features redis`.")
}
