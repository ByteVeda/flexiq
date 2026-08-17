//! SQLite implementation of `WorkflowStorage`.
//!
//! Construction runs the workflow-table migrations once and caches a pool
//! handle to the underlying `SqliteStorage`. Every trait method is generated
//! by the `impl_workflow_diesel_ops!` macro in `diesel_common.rs` — both
//! backends share byte-identical SQL.

use diesel::prelude::*;

use flexiq_core::error::Result;
use flexiq_core::storage::sqlite::SqliteStorage;

use crate::diesel_common::impl_workflow_diesel_ops;

/// Workflow-aware wrapper around `SqliteStorage`.
///
/// Runs workflow table migrations on construction, then delegates all
/// `WorkflowStorage` operations through the shared diesel macro.
#[derive(Clone)]
pub struct WorkflowSqliteStorage {
    pub(crate) inner: SqliteStorage,
    pub(crate) namespace: Option<String>,
}

impl WorkflowSqliteStorage {
    /// Wrap an existing `SqliteStorage` and ensure workflow tables exist.
    ///
    /// `namespace` is the tenant every run this store creates is stamped with,
    /// and the only one it can read or mutate. `None` addresses every
    /// namespace, matching an unscoped queue.
    pub fn new(storage: SqliteStorage, namespace: Option<String>) -> Result<Self> {
        let mut conn = storage.conn()?;
        flexiq_core::storage::migrate::run_sqlite(
            &mut conn,
            "workflow_schema_migrations",
            &crate::migrations::all(),
        )?;
        Ok(Self {
            inner: storage,
            namespace,
        })
    }

    /// Wrap an existing `SqliteStorage` without applying workflow DDL, for a
    /// deployment that gates schema changes behind an explicit migrate step.
    /// Every workflow query fails until [`Self::migrate`] has run.
    pub fn unmigrated(storage: SqliteStorage, namespace: Option<String>) -> Result<Self> {
        Ok(Self {
            inner: storage,
            namespace,
        })
    }

    /// Apply any pending workflow schema changes, returning the versions this
    /// call applied. Idempotent: a current database applies nothing.
    pub fn migrate(&self) -> Result<Vec<String>> {
        let mut conn = self.inner.conn()?;
        flexiq_core::storage::migrate::run_sqlite(
            &mut conn,
            "workflow_schema_migrations",
            &crate::migrations::all(),
        )
    }

    /// Access the underlying `SqliteStorage`.
    pub fn inner(&self) -> &SqliteStorage {
        &self.inner
    }
}

impl_workflow_diesel_ops!(
    WorkflowSqliteStorage,
    SqliteConnection,
    crate::diesel_common::sql_as_is
);
