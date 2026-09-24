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

#[cfg(feature = "push-dispatch")]
#[doc(hidden)]
pub mod listener;

use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::r2d2::{ConnectionManager, Pool};

use crate::error::Result;
use crate::storage::migrate::MigrationReport;

/// One `COUNT(*)` result, for the catalog probe in [`PostgresStorage::is_migrated`].
#[derive(diesel::QueryableByName)]
struct CountRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    count: i64,
}

type PgPool = Pool<ConnectionManager<PgConnection>>;

/// Validate a PostgreSQL schema name (alphanumeric + underscores, non-empty).
fn validate_schema_name(schema: &str) -> Result<()> {
    if schema.is_empty() {
        return Err(crate::error::QueueError::Config(
            "Schema name cannot be empty".into(),
        ));
    }
    if !schema
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return Err(crate::error::QueueError::Config(
            format!("Invalid schema name '{schema}': only alphanumeric characters and underscores are allowed"),
        ));
    }
    Ok(())
}

/// Initialize OpenSSL ahead of libpq, with its `atexit` cleanup suppressed
/// (`openssl_sys::init` passes `OPENSSL_INIT_NO_ATEXIT`).
///
/// r2d2 opens connections on detached worker threads that can outlive the pool,
/// so libpq/OpenSSL may still be running on one of them when the process exits.
/// OpenSSL's default `atexit` teardown racing with threads still inside the
/// library is a documented cause of exit-time segfaults; suppressing it is the
/// standard remedy (see the OpenSSL `OPENSSL_INIT_NO_ATEXIT` docs). Only the
/// first initializer's flags count, so claiming it here — before the first
/// connection — wins the race; skipping cleanup is free because the OS reclaims
/// the memory at exit anyway. Idempotent, so repeated pool construction is cheap.
fn init_openssl_without_atexit() {
    openssl_sys::init();
}

/// Quote a SQL identifier for safe interpolation. Postgres can't bind
/// identifiers as parameters, and while `validate_schema_name` already
/// restricts the schema to `[A-Za-z0-9_]`, quoting here makes the structural
/// safety explicit rather than relying solely on the validator.
fn pg_quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// PostgreSQL-backed storage for the task queue, using Diesel ORM.
#[derive(Clone)]
pub struct PostgresStorage {
    pool: PgPool,
    schema: String,
    /// Original connection URL, retained only to open the dedicated
    /// (non-pooled) `LISTEN` connection used by push-dispatch.
    #[cfg(feature = "push-dispatch")]
    database_url: String,
}

impl PostgresStorage {
    /// Connect to a PostgreSQL database at the given URL.
    /// Tables are created in the `flexiq` schema by default.
    pub fn new(database_url: &str) -> Result<Self> {
        Self::with_schema(database_url, "flexiq")
    }

    /// Connect with a custom schema name.
    pub fn with_schema(database_url: &str, schema: &str) -> Result<Self> {
        Self::build(database_url, 10, schema)
    }

    /// Connect with a custom connection pool size and schema.
    pub fn with_pool_size(database_url: &str, pool_size: u32) -> Result<Self> {
        Self::build(database_url, pool_size, "flexiq")
    }

    /// Connect with a custom schema name and connection pool size.
    pub fn with_schema_and_pool_size(
        database_url: &str,
        schema: &str,
        pool_size: u32,
    ) -> Result<Self> {
        Self::build(database_url, pool_size, schema)
    }

    /// Connect without applying any DDL, for a deployment that gates schema
    /// changes behind an explicit `migrate` step. The database is left exactly
    /// as found, so every query fails until [`Self::migrate`] has run.
    pub fn unmigrated(database_url: &str, schema: &str, pool_size: u32) -> Result<Self> {
        Self::build_with(database_url, pool_size, schema, false)
    }

    /// Apply any pending schema changes, plus the one-time backlog sweep the
    /// automatic path runs. Idempotent: a current database reports an empty
    /// [`MigrationReport`].
    pub fn migrate(&self) -> Result<MigrationReport> {
        let mut conn = self.conn()?;

        // Postgres-only: ensure the target schema exists before any DDL runs.
        diesel::sql_query(format!(
            "CREATE SCHEMA IF NOT EXISTS {}",
            pg_quote_ident(&self.schema)
        ))
        .execute(&mut conn)?;

        let applied = crate::storage::migrate::run_postgres(
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

    /// Whether the core schema has been applied in this storage's schema — the
    /// ledger table exists.
    ///
    /// A catalog read, not DDL, so a gated deployment can ask without applying
    /// anything: it is how a shell tells "nothing here yet" apart from "opened
    /// without migrating an existing deployment".
    pub fn is_migrated(&self) -> Result<bool> {
        let mut conn = self.conn()?;
        let rows: Vec<CountRow> = diesel::sql_query(
            "SELECT COUNT(*) AS count FROM information_schema.tables \
             WHERE table_schema = current_schema() AND table_name = 'schema_migrations'",
        )
        .load(&mut conn)?;
        Ok(rows.first().is_some_and(|row| row.count > 0))
    }

    fn build(database_url: &str, pool_size: u32, schema: &str) -> Result<Self> {
        Self::build_with(database_url, pool_size, schema, true)
    }

    fn build_with(
        database_url: &str,
        pool_size: u32,
        schema: &str,
        auto_migrate: bool,
    ) -> Result<Self> {
        validate_schema_name(schema)?;
        init_openssl_without_atexit();

        let manager = ConnectionManager::<PgConnection>::new(database_url);
        let pool = Pool::builder()
            .max_size(pool_size)
            .min_idle(Some(1))
            .idle_timeout(Some(std::time::Duration::from_secs(300)))
            .max_lifetime(Some(std::time::Duration::from_secs(1800)))
            .connection_timeout(std::time::Duration::from_secs(10))
            .build(manager)?;

        let storage = Self {
            pool,
            schema: schema.to_string(),
            #[cfg(feature = "push-dispatch")]
            database_url: database_url.to_string(),
        };
        if auto_migrate {
            storage.migrate()?;
        }
        Ok(storage)
    }

    /// The connection URL, for opening the dedicated `LISTEN` connection.
    #[cfg(feature = "push-dispatch")]
    pub fn database_url(&self) -> &str {
        &self.database_url
    }

    /// Pooled connection with `search_path` set to this storage's schema.
    pub fn conn(&self) -> Result<diesel::r2d2::PooledConnection<ConnectionManager<PgConnection>>> {
        let mut conn = self.pool.get()?;
        diesel::sql_query(format!(
            "SET search_path TO {}",
            pg_quote_ident(&self.schema)
        ))
        .execute(&mut conn)
        .map_err(crate::error::QueueError::Storage)?;
        Ok(conn)
    }
}

/// Postgres `NOTIFY` channel reserved for "a ready job was enqueued" signals.
/// Nothing publishes on it yet: the stub listener never reads a notification,
/// so enqueue skips `pg_notify` rather than pay a round trip for nothing.
#[cfg(feature = "push-dispatch")]
pub const JOB_READY_CHANNEL: &str = "flexiq_job_ready";

#[cfg(all(test, feature = "push-dispatch"))]
mod notify_tests {
    use super::PostgresStorage;
    use crate::storage::notify::StorageNotifier;

    // Inherent const wins when `T: StorageNotifier`, else the blanket trait
    // const answers — a compile-time "does T implement it" probe.
    trait Fallback {
        const NOTIFIES: bool = false;
    }
    impl<T> Fallback for T {}
    struct Probe<T>(std::marker::PhantomData<T>);
    impl<T: StorageNotifier> Probe<T> {
        const NOTIFIES: bool = true;
    }

    /// The enqueue path cannot issue a `pg_notify`: Postgres has no
    /// `StorageNotifier` impl to call, while SQLite still does. Checked at
    /// compile time; the test exists so the proof shows up in the suite.
    #[test]
    fn postgres_enqueue_issues_no_notify() {
        const { assert!(!<Probe<PostgresStorage>>::NOTIFIES) };
        const { assert!(<Probe<crate::storage::sqlite::SqliteStorage>>::NOTIFIES) };
    }
}
