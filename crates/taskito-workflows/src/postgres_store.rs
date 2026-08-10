//! PostgreSQL implementation of `WorkflowStorage`.
//!
//! Construction runs the workflow-table migrations once and caches a pool
//! handle to the underlying `PostgresStorage`. Every trait method is generated
//! by the `impl_workflow_diesel_ops!` macro in `diesel_common.rs` — the SQL
//! bodies are byte-identical to SQLite (Diesel rewrites `?` to `$N` per
//! backend), only the DDL differs (`BYTEA` for `BLOB`, `BIGINT` for `INTEGER`
//! timestamps).

use diesel::prelude::*;

use taskito_core::error::Result;
use taskito_core::storage::postgres::PostgresStorage;

use crate::diesel_common::impl_workflow_diesel_ops;

/// Workflow-aware wrapper around `PostgresStorage`.
///
/// Runs workflow table migrations on construction, then delegates all
/// `WorkflowStorage` operations through the shared diesel macro. The wrapped
/// `PostgresStorage` owns the pool; clones share it.
#[derive(Clone)]
pub struct WorkflowPostgresStorage {
    pub(crate) inner: PostgresStorage,
    pub(crate) namespace: Option<String>,
}

impl WorkflowPostgresStorage {
    /// Wrap an existing `PostgresStorage` and ensure workflow tables exist.
    ///
    /// `namespace` is the tenant every run this store creates is stamped with,
    /// and the only one it can read or mutate. `None` addresses every
    /// namespace, matching an unscoped queue.
    pub fn new(storage: PostgresStorage, namespace: Option<String>) -> Result<Self> {
        let mut conn = storage.conn()?;
        taskito_core::storage::migrate::run_postgres(
            &mut conn,
            "workflow_schema_migrations",
            &crate::migrations::all(),
        )?;
        Ok(Self {
            inner: storage,
            namespace,
        })
    }

    /// Wrap an existing `PostgresStorage` without applying workflow DDL, for a
    /// deployment that gates schema changes behind an explicit migrate step.
    /// Every workflow query fails until [`Self::migrate`] has run.
    pub fn unmigrated(storage: PostgresStorage, namespace: Option<String>) -> Result<Self> {
        Ok(Self {
            inner: storage,
            namespace,
        })
    }

    /// Apply any pending workflow schema changes, returning the versions this
    /// call applied. Idempotent: a current database applies nothing.
    pub fn migrate(&self) -> Result<Vec<String>> {
        let mut conn = self.inner.conn()?;
        taskito_core::storage::migrate::run_postgres(
            &mut conn,
            "workflow_schema_migrations",
            &crate::migrations::all(),
        )
    }

    /// Access the underlying `PostgresStorage`.
    pub fn inner(&self) -> &PostgresStorage {
        &self.inner
    }
}

impl_workflow_diesel_ops!(
    WorkflowPostgresStorage,
    PgConnection,
    crate::diesel_common::pg_rewrite
);
