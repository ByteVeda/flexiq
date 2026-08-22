mod archival;
mod circuit_breakers;
mod dashboard_settings;
mod dead_letter;
mod jobs;
mod locks;
mod logs;
mod metrics;
mod periodic;
mod pubsub;
mod queue_state;
mod rate_limits;
mod retention;
mod steps;
mod trait_impl;
mod workers;

use diesel::prelude::*;
use diesel::r2d2::{ConnectionManager, CustomizeConnection, Pool};
use diesel::sqlite::SqliteConnection;

use crate::storage::migrate::MigrationReport;

/// One `COUNT(*)` result, for the catalog probe in [`SqliteStorage::is_migrated`].
#[derive(diesel::QueryableByName)]
struct CountRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    count: i64,
}

use crate::error::Result;

type DbPool = Pool<ConnectionManager<SqliteConnection>>;

/// Sets SQLite pragmas on every new connection from the pool.
#[derive(Debug)]
struct SqlitePragmaCustomizer;

impl CustomizeConnection<SqliteConnection, diesel::r2d2::Error> for SqlitePragmaCustomizer {
    fn on_acquire(
        &self,
        conn: &mut SqliteConnection,
    ) -> std::result::Result<(), diesel::r2d2::Error> {
        diesel::sql_query("PRAGMA journal_mode = WAL")
            .execute(conn)
            .map_err(diesel::r2d2::Error::QueryError)?;
        diesel::sql_query("PRAGMA busy_timeout = 5000")
            .execute(conn)
            .map_err(diesel::r2d2::Error::QueryError)?;
        diesel::sql_query("PRAGMA journal_size_limit = 67108864")
            .execute(conn)
            .map_err(diesel::r2d2::Error::QueryError)?;
        diesel::sql_query("PRAGMA synchronous = NORMAL")
            .execute(conn)
            .map_err(diesel::r2d2::Error::QueryError)?;
        Ok(())
    }
}

/// SQLite-backed storage for the task queue, using Diesel ORM.
#[derive(Clone)]
pub struct SqliteStorage {
    pool: DbPool,
    /// In-process wake handle, set by the scheduler when push-dispatch is
    /// enabled. Enqueue of a ready job calls `notify_one()` so the scheduler
    /// dispatches immediately instead of waiting for the next poll.
    #[cfg(feature = "push-dispatch")]
    notify: std::sync::Arc<tokio::sync::Notify>,
}

impl SqliteStorage {
    /// Open (or create) a SQLite database at the given path.
    pub fn new(db_path: &str) -> Result<Self> {
        Self::with_pool_size(db_path, 8)
    }

    /// Open (or create) a SQLite database with a custom connection pool size.
    pub fn with_pool_size(db_path: &str, pool_size: u32) -> Result<Self> {
        Self::build(db_path, pool_size, true)
    }

    fn build(db_path: &str, pool_size: u32, auto_migrate: bool) -> Result<Self> {
        let manager = ConnectionManager::<SqliteConnection>::new(db_path);
        let pool = Pool::builder()
            .max_size(pool_size)
            .connection_customizer(Box::new(SqlitePragmaCustomizer))
            .build(manager)?;

        let storage = Self {
            pool,
            #[cfg(feature = "push-dispatch")]
            notify: std::sync::Arc::new(tokio::sync::Notify::new()),
        };
        if auto_migrate {
            storage.migrate()?;
        }
        Ok(storage)
    }

    /// Open without applying any DDL, for a deployment that gates schema
    /// changes behind an explicit `migrate` step.
    ///
    /// The database is left exactly as found — on a fresh file that means no
    /// tables at all, so every query fails until [`Self::migrate`] has run.
    pub fn unmigrated(db_path: &str, pool_size: u32) -> Result<Self> {
        Self::build(db_path, pool_size, false)
    }

    /// Apply any pending schema changes, plus the one-time backlog sweep the
    /// automatic path runs. Idempotent: a current database reports an empty
    /// [`MigrationReport`].
    pub fn migrate(&self) -> Result<MigrationReport> {
        let mut conn = self.conn()?;
        let applied = crate::storage::migrate::run_sqlite(
            &mut conn,
            "schema_migrations",
            &crate::storage::migrations::all(),
        )?;
        drop(conn);

        // Drain any pre-existing terminal jobs left in `jobs` by older
        // versions into `archived_jobs`. Terminal jobs now live there from the
        // moment they transition; this one-time sweep migrates the backlog.
        let archived_jobs = self.archive_old_jobs(i64::MAX)?;

        Ok(MigrationReport {
            applied,
            archived_jobs,
            ..MigrationReport::default()
        })
    }

    /// Whether the core schema has been applied — the ledger table exists.
    ///
    /// A cheap catalog read, not a DDL statement, so a gated deployment can ask
    /// without applying anything: it is how a shell tells "nothing here yet"
    /// apart from "opened without migrating an existing deployment".
    pub fn is_migrated(&self) -> Result<bool> {
        let mut conn = self.conn()?;
        let rows: Vec<CountRow> = diesel::sql_query(
            "SELECT COUNT(*) AS count FROM sqlite_master \
             WHERE type = 'table' AND name = 'schema_migrations'",
        )
        .load(&mut conn)?;
        Ok(rows.first().is_some_and(|row| row.count > 0))
    }

    /// Create an in-memory storage (useful for tests).
    pub fn in_memory() -> Result<Self> {
        let manager = ConnectionManager::<SqliteConnection>::new(":memory:");
        let pool = Pool::builder()
            .max_size(1)
            .connection_customizer(Box::new(SqlitePragmaCustomizer))
            .build(manager)?;

        let storage = Self {
            pool,
            #[cfg(feature = "push-dispatch")]
            notify: std::sync::Arc::new(tokio::sync::Notify::new()),
        };
        storage.migrate()?;
        Ok(storage)
    }

    /// Replace the in-process wake handle so it is shared with the scheduler's
    /// `WakeSource`. Only meaningful under `push-dispatch`.
    #[cfg(feature = "push-dispatch")]
    pub fn set_notify_handle(&mut self, notify: std::sync::Arc<tokio::sync::Notify>) {
        self.notify = notify;
    }

    /// The in-process wake handle. Enqueue paths call `notify_one()` on this
    /// when a ready job is inserted.
    #[cfg(feature = "push-dispatch")]
    pub fn notify_handle(&self) -> &std::sync::Arc<tokio::sync::Notify> {
        &self.notify
    }

    /// Check a pooled SQLite connection out of the r2d2 pool.
    pub fn conn(
        &self,
    ) -> Result<diesel::r2d2::PooledConnection<ConnectionManager<SqliteConnection>>> {
        Ok(self.pool.get()?)
    }
}

#[cfg(feature = "push-dispatch")]
impl crate::storage::notify::StorageNotifier for SqliteStorage {
    fn notify_job_ready(&self, _queue: &str, _scheduled_at: i64) {
        // Single-process: wake the in-memory scheduler loop directly.
        self.notify.notify_one();
    }
}

pub use crate::storage::{DeadJob, QueueStats};

#[cfg(test)]
mod tests;
