//! Code-first schema migrations.
//!
//! DDL is built with `sea-query` (no hand-written SQL) and rendered to a
//! dialect-correct string that is executed through the existing Diesel
//! connection — no second database driver. A per-tracking-table `version`
//! ledger records applied migrations so each runs exactly once.
//!
//! The baseline migration (`m0001`) is written idempotently (every
//! `CREATE TABLE`/`CREATE INDEX` uses `IF NOT EXISTS`, and the historical
//! `ADD COLUMN`s use Postgres `ADD COLUMN IF NOT EXISTS` / the SQLite
//! dup-column swallow). On a pre-existing database — which has no ledger row —
//! it runs once as a harmless no-op pass and is then recorded, so live
//! databases carry no cutover risk. Later migrations can be plain one-shot DDL.

use std::collections::HashSet;

use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;
use sea_query::{
    Alias, ColumnDef, OnConflict, Query, SchemaStatementBuilder, SqliteQueryBuilder, Table,
};

#[cfg(feature = "postgres")]
use diesel::pg::PgConnection;
#[cfg(feature = "postgres")]
use sea_query::PostgresQueryBuilder;

use crate::error::{QueueError, Result};
use crate::job::now_millis;

/// The SQL dialect a migration renders for.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Backend {
    /// SQLite dialect.
    Sqlite,
    /// PostgreSQL dialect.
    Postgres,
}

/// One rendered statement plus whether a SQLite "duplicate column" error should
/// be swallowed. SQLite has no `ADD COLUMN IF NOT EXISTS`, so an already-present
/// column is signalled by that error and treated as success — mirroring the
/// backend's historical `migration_alter` behaviour.
pub struct Stmt {
    sql: String,
    swallow_dup_column: bool,
}

impl Stmt {
    /// Rendered SQL. Test-only accessor so migration render tests co-located in a
    /// migration's own module (a different module than `Stmt`) can inspect output.
    #[cfg(test)]
    pub(crate) fn sql(&self) -> &str {
        &self.sql
    }
}

/// A single schema version. `up` returns the rendered statements to apply.
pub trait Migration {
    /// Stable, unique version key recorded in the ledger (e.g. `"0001_initial"`).
    fn version(&self) -> &'static str;
    /// Statements to run for this migration on the given backend, in order.
    fn up(&self, backend: Backend) -> Vec<Stmt>;
}

/// Render a schema statement (`CREATE TABLE`/`CREATE INDEX`/`DROP`) to a plain
/// DDL string. Ordinary statement — an existing object is a hard error unless
/// the statement itself is idempotent (`.if_not_exists()`).
pub fn ddl<S: SchemaStatementBuilder>(backend: Backend, stmt: &S) -> Stmt {
    Stmt {
        sql: render_schema(stmt, backend),
        swallow_dup_column: false,
    }
}

/// Render an `ALTER TABLE … ADD COLUMN` that must be idempotent. Postgres emits
/// `ADD COLUMN IF NOT EXISTS`; SQLite emits a plain `ADD COLUMN` whose
/// duplicate-column error is swallowed at execution time.
pub fn add_column(backend: Backend, table: &str, column: &mut ColumnDef) -> Stmt {
    let mut alter = Table::alter();
    alter.table(Alias::new(table));
    match backend {
        Backend::Sqlite => alter.add_column(column),
        Backend::Postgres => alter.add_column_if_not_exists(column),
    };
    Stmt {
        sql: render_schema(&alter, backend),
        swallow_dup_column: true,
    }
}

/// Escape hatch for DDL sea-query cannot model — currently only Postgres table
/// storage parameters (`ALTER TABLE … SET (…)`, which `TableAlterStatement` has
/// no method for). The SQL is a code-defined literal that never renders through
/// a dialect builder: no user input reaches here, and the caller owns dialect
/// gating (return no statements for the backends it does not apply to).
pub fn raw_ddl(sql: impl Into<String>) -> Stmt {
    Stmt {
        sql: sql.into(),
        swallow_dup_column: false,
    }
}

/// Render a data statement (the `has_deps` backfill `UPDATE`). Literals are
/// inlined by sea-query; only trusted, code-defined values reach here.
pub fn dml(backend: Backend, stmt: &sea_query::UpdateStatement) -> Stmt {
    Stmt {
        sql: match backend {
            Backend::Sqlite => stmt.to_string(SqliteQueryBuilder),
            #[cfg(feature = "postgres")]
            Backend::Postgres => stmt.to_string(PostgresQueryBuilder),
            #[cfg(not(feature = "postgres"))]
            Backend::Postgres => unreachable!("Postgres migrations require the `postgres` feature"),
        },
        swallow_dup_column: false,
    }
}

fn render_schema<S: SchemaStatementBuilder>(stmt: &S, backend: Backend) -> String {
    match backend {
        Backend::Sqlite => stmt.to_string(SqliteQueryBuilder),
        #[cfg(feature = "postgres")]
        Backend::Postgres => stmt.to_string(PostgresQueryBuilder),
        #[cfg(not(feature = "postgres"))]
        Backend::Postgres => unreachable!("Postgres migrations require the `postgres` feature"),
    }
}

/// Column name of the `version` ledger, factored out so the tracking-table DDL
/// and the applied-versions read agree.
const VERSION_COL: &str = "version";
const APPLIED_AT_COL: &str = "applied_at";

fn create_ledger_sql(table: &str, backend: Backend) -> String {
    render_schema(
        Table::create()
            .table(Alias::new(table))
            .if_not_exists()
            .col(
                ColumnDef::new(Alias::new(VERSION_COL))
                    .text()
                    .not_null()
                    .primary_key(),
            )
            .col(
                ColumnDef::new(Alias::new(APPLIED_AT_COL))
                    .big_integer()
                    .not_null(),
            ),
        backend,
    )
}

fn select_versions_sql(table: &str, backend: Backend) -> String {
    let stmt = Query::select()
        .column(Alias::new(VERSION_COL))
        .from(Alias::new(table))
        .to_owned();
    match backend {
        Backend::Sqlite => stmt.to_string(SqliteQueryBuilder),
        #[cfg(feature = "postgres")]
        Backend::Postgres => stmt.to_string(PostgresQueryBuilder),
        #[cfg(not(feature = "postgres"))]
        Backend::Postgres => unreachable!("Postgres migrations require the `postgres` feature"),
    }
}

fn record_version_sql(table: &str, version: &str, now: i64, backend: Backend) -> String {
    let stmt = Query::insert()
        .into_table(Alias::new(table))
        .columns([Alias::new(VERSION_COL), Alias::new(APPLIED_AT_COL)])
        .values_panic([version.into(), now.into()])
        // Two processes booting at once can both apply a migration and then race
        // to record it; without this the loser hits a primary-key violation and
        // fails *after* the schema already converged. The version is what matters,
        // not which racer's `applied_at` wins.
        .on_conflict(
            OnConflict::column(Alias::new(VERSION_COL))
                .do_nothing()
                .to_owned(),
        )
        .to_owned();
    match backend {
        Backend::Sqlite => stmt.to_string(SqliteQueryBuilder),
        #[cfg(feature = "postgres")]
        Backend::Postgres => stmt.to_string(PostgresQueryBuilder),
        #[cfg(not(feature = "postgres"))]
        Backend::Postgres => unreachable!("Postgres migrations require the `postgres` feature"),
    }
}

/// Row shape for reading the ledger back through Diesel.
#[derive(diesel::QueryableByName)]
struct VersionRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    version: String,
}

/// Connection operations the migrator needs, abstracted over the two Diesel
/// backends so the driver-generic runner stays one code path.
trait MigrationConn {
    fn exec(&mut self, sql: &str, swallow_dup_column: bool) -> Result<()>;
    fn load_versions(&mut self, select_sql: &str) -> Result<Vec<String>>;
}

impl MigrationConn for SqliteConnection {
    fn exec(&mut self, sql: &str, swallow_dup_column: bool) -> Result<()> {
        match diesel::sql_query(sql).execute(self) {
            Ok(_) => Ok(()),
            Err(e) if swallow_dup_column && e.to_string().contains("duplicate column") => Ok(()),
            Err(e) => Err(QueueError::Storage(e)),
        }
    }

    fn load_versions(&mut self, select_sql: &str) -> Result<Vec<String>> {
        let rows: Vec<VersionRow> = diesel::sql_query(select_sql).load(self)?;
        Ok(rows.into_iter().map(|r| r.version).collect())
    }
}

#[cfg(feature = "postgres")]
impl MigrationConn for PgConnection {
    fn exec(&mut self, sql: &str, _swallow_dup_column: bool) -> Result<()> {
        // Postgres alters use `ADD COLUMN IF NOT EXISTS`, so nothing to swallow —
        // every error here is genuine and must propagate.
        diesel::sql_query(sql).execute(self)?;
        Ok(())
    }

    fn load_versions(&mut self, select_sql: &str) -> Result<Vec<String>> {
        let rows: Vec<VersionRow> = diesel::sql_query(select_sql).load(self)?;
        Ok(rows.into_iter().map(|r| r.version).collect())
    }
}

fn run_generic<C: MigrationConn + Connection>(
    conn: &mut C,
    backend: Backend,
    tracking_table: &str,
    migrations: &[Box<dyn Migration>],
) -> Result<Vec<String>> {
    conn.exec(&create_ledger_sql(tracking_table, backend), false)?;
    let applied: HashSet<String> = conn
        .load_versions(&select_versions_sql(tracking_table, backend))?
        .into_iter()
        .collect();

    // Apply in ascending `version()` order regardless of the order build.rs
    // discovered them in — `version()` is the authoritative key (Alembic-style
    // in-file identity), not the filename. A shared key between two files is a
    // registration bug, not a valid history, so fail loudly.
    let mut ordered: Vec<&dyn Migration> = migrations.iter().map(Box::as_ref).collect();
    ordered.sort_by(|a, b| a.version().cmp(b.version()));
    if let Some(dup) = ordered
        .windows(2)
        .find(|w| w[0].version() == w[1].version())
    {
        return Err(QueueError::Config(format!(
            "duplicate migration version: {}",
            dup[0].version()
        )));
    }

    let mut newly_applied = Vec::new();
    for migration in ordered {
        if applied.contains(migration.version()) {
            continue;
        }
        // Apply the migration's statements and record its version in one
        // transaction: an interruption between the two would otherwise leave an
        // applied change untracked, so the next boot re-runs it — safe for the
        // idempotent baseline, but not for later one-shot DDL.
        conn.transaction::<_, QueueError, _>(|conn| {
            for stmt in migration.up(backend) {
                conn.exec(&stmt.sql, stmt.swallow_dup_column)?;
            }
            conn.exec(
                &record_version_sql(tracking_table, migration.version(), now_millis(), backend),
                false,
            )
        })?;
        newly_applied.push(migration.version().to_string());
    }
    Ok(newly_applied)
}

/// What one explicit migration run did.
///
/// `applied` is empty on an already-current database, which is the common case
/// and not an error. A schemaless backend reports `schemaless` and nothing else:
/// it has no DDL to run and never will, so "nothing to migrate" is the honest
/// answer rather than a failure.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MigrationReport {
    /// Core schema versions applied by this run, in the order applied.
    pub applied: Vec<String>,
    /// Workflow schema versions applied by this run, if workflow tables were
    /// migrated alongside.
    pub workflow_applied: Vec<String>,
    /// Terminal jobs the one-time backlog sweep moved into `archived_jobs`.
    pub archived_jobs: u64,
    /// The backend stores no schema, so there is nothing to migrate.
    pub schemaless: bool,
}

impl MigrationReport {
    /// The report a schemaless backend returns.
    pub fn schemaless() -> Self {
        Self {
            schemaless: true,
            ..Self::default()
        }
    }

    /// Whether this run changed anything.
    pub fn is_empty(&self) -> bool {
        self.applied.is_empty() && self.workflow_applied.is_empty() && self.archived_jobs == 0
    }
}

/// Apply pending `migrations` to a SQLite database, recording each in
/// `tracking_table`. Returns the versions this call applied, in the order it
/// applied them — empty when the database was already current.
pub fn run_sqlite(
    conn: &mut SqliteConnection,
    tracking_table: &str,
    migrations: &[Box<dyn Migration>],
) -> Result<Vec<String>> {
    run_generic(conn, Backend::Sqlite, tracking_table, migrations)
}

/// Apply pending `migrations` to a Postgres database, recording each in
/// `tracking_table`. Returns the versions this call applied, in the order it
/// applied them — empty when the database was already current.
#[cfg(feature = "postgres")]
pub fn run_postgres(
    conn: &mut PgConnection,
    tracking_table: &str,
    migrations: &[Box<dyn Migration>],
) -> Result<Vec<String>> {
    run_generic(conn, Backend::Postgres, tracking_table, migrations)
}

#[cfg(test)]
mod tests {
    use super::*;
    use diesel::Connection;
    use sea_query::{Alias, ColumnDef, Query, SqliteQueryBuilder, Table};

    fn mem() -> SqliteConnection {
        SqliteConnection::establish(":memory:").expect("open in-memory sqlite")
    }

    fn applied(conn: &mut SqliteConnection) -> Vec<String> {
        conn.load_versions(&select_versions_sql("schema_migrations", Backend::Sqlite))
            .expect("read ledger")
    }

    #[test]
    fn raw_ddl_passes_sql_through_verbatim() {
        // The escape hatch must not touch the SQL — it is a code-defined literal
        // the caller has already made dialect-correct.
        let stmt = raw_ddl("ALTER TABLE jobs SET (fillfactor = 85)");
        assert_eq!(stmt.sql, "ALTER TABLE jobs SET (fillfactor = 85)");
        assert!(!stmt.swallow_dup_column);
    }

    #[test]
    fn m0003_renders_partial_indexes_on_both_backends() {
        // The Postgres arm only renders under its feature — `render_schema`
        // is `unreachable!()` otherwise.
        let backends = [
            (Backend::Sqlite, "sqlite"),
            #[cfg(feature = "postgres")]
            (Backend::Postgres, "postgres"),
        ];
        for (backend, label) in backends {
            let sql: Vec<String> = crate::storage::migrations::all()
                .iter()
                .find(|m| m.version() == "0003_retention_indexes")
                .expect("m0003 registered")
                .up(backend)
                .iter()
                .map(|s| s.sql.clone())
                .collect();
            let joined = sql.join("\n");
            assert!(joined.contains("idx_dead_letter_ttl"), "{label}: {joined}");
            assert!(
                joined.contains("WHERE") && joined.contains("result_ttl_ms"),
                "{label}: partial predicate must render: {joined}"
            );
        }
    }

    #[test]
    fn m0010_renders_the_debounce_column_and_its_partial_index() {
        let backends = [
            (Backend::Sqlite, "sqlite"),
            #[cfg(feature = "postgres")]
            (Backend::Postgres, "postgres"),
        ];
        for (backend, label) in backends {
            let joined = crate::storage::migrations::all()
                .iter()
                .find(|m| m.version() == "0010_debounce")
                .expect("m0010 registered")
                .up(backend)
                .iter()
                .map(|s| s.sql.clone())
                .collect::<Vec<_>>()
                .join("\n");

            assert!(joined.contains("debounce_key"), "{label}: {joined}");
            assert!(
                joined.contains("idx_jobs_debounce_key"),
                "{label}: {joined}"
            );
            // The index only covers rows a debounce write can collide with;
            // without the predicate it would span every terminal job ever run.
            assert!(
                joined.contains("WHERE") && joined.contains("\"status\" = 0"),
                "{label}: partial predicate must render: {joined}"
            );
            // Uniqueness is deliberately absent — `namespace` is nullable and
            // NULLs are distinct in a unique index, so it would constrain
            // nothing in the default namespace. See the migration's doc comment.
            assert!(
                !joined.contains("UNIQUE"),
                "{label}: index must not be unique: {joined}"
            );
        }
    }

    #[test]
    fn m0014_renders_a_nullable_origin_column() {
        let backends = [
            (Backend::Sqlite, "sqlite"),
            #[cfg(feature = "postgres")]
            (Backend::Postgres, "postgres"),
        ];
        for (backend, label) in backends {
            let joined = crate::storage::migrations::all()
                .iter()
                .find(|m| m.version() == "0014_dead_letter_origin")
                .expect("m0014 registered")
                .up(backend)
                .iter()
                .map(|s| s.sql.clone())
                .collect::<Vec<_>>()
                .join("\n");

            assert!(joined.contains("origin_job_id"), "{label}: {joined}");
            // Nullable by design: NULL is what sends `retry_dead` to the blob
            // fallback for a row written before the column. A `NOT NULL DEFAULT`
            // would make a pre-migration row indistinguishable from one whose
            // run genuinely began at `original_job_id`.
            assert!(
                !joined.contains("NOT NULL"),
                "{label}: the column must stay nullable: {joined}"
            );
        }
    }

    #[test]
    fn m0015_renders_a_nullable_job_metadata_column() {
        let backends = [
            (Backend::Sqlite, "sqlite"),
            #[cfg(feature = "postgres")]
            (Backend::Postgres, "postgres"),
        ];
        for (backend, label) in backends {
            let joined = crate::storage::migrations::all()
                .iter()
                .find(|m| m.version() == "0015_dead_letter_job_metadata")
                .expect("m0015 registered")
                .up(backend)
                .iter()
                .map(|s| s.sql.clone())
                .collect::<Vec<_>>()
                .join("\n");

            assert!(joined.contains("job_metadata"), "{label}: {joined}");
            // Nullable by design: NULL is what tells the reader that `metadata`
            // is itself the job's own, which is both the no-replacement case
            // and every row written before this migration.
            assert!(
                !joined.contains("NOT NULL"),
                "{label}: the column must stay nullable: {joined}"
            );
        }
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn add_column_renders_if_not_exists_on_postgres() {
        // The Postgres `ADD COLUMN IF NOT EXISTS` branch had no coverage — SQLite
        // renders a plain `ADD COLUMN` and swallows the dup-column error instead.
        let mut col = ColumnDef::new(Alias::new("namespace"));
        col.text();
        let stmt = add_column(Backend::Postgres, "jobs", &mut col);
        assert!(
            stmt.sql.contains("ADD COLUMN IF NOT EXISTS"),
            "{}",
            stmt.sql
        );
    }

    #[test]
    fn fresh_db_applies_all_migrations_and_is_idempotent() {
        let mut conn = mem();
        let migrations = crate::storage::migrations::all();

        run_sqlite(&mut conn, "schema_migrations", &migrations).expect("first run");
        let first = applied(&mut conn);
        assert!(first.iter().any(|v| v == "0001_initial"));
        assert!(first.iter().any(|v| v == "0002_scaling_indexes"));

        // Re-running is a clean no-op: no error, no duplicate ledger rows.
        run_sqlite(&mut conn, "schema_migrations", &migrations).expect("second run");
        assert_eq!(applied(&mut conn), first);
    }

    #[test]
    fn existing_partial_db_gets_missing_columns() {
        let mut conn = mem();

        // Simulate a database created by an older version: a `jobs` table that
        // predates the `namespace` column. Built with sea-query — no raw SQL.
        let old_jobs = Table::create()
            .table(Alias::new("jobs"))
            .if_not_exists()
            .col(
                ColumnDef::new(Alias::new("id"))
                    .text()
                    .not_null()
                    .primary_key(),
            )
            .col(ColumnDef::new(Alias::new("payload")).blob().not_null())
            .col(
                ColumnDef::new(Alias::new("status"))
                    .integer()
                    .not_null()
                    .default(0),
            )
            .col(
                ColumnDef::new(Alias::new("created_at"))
                    .big_integer()
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new("scheduled_at"))
                    .big_integer()
                    .not_null(),
            )
            .to_owned();
        conn.exec(&old_jobs.to_string(SqliteQueryBuilder), false)
            .expect("seed old jobs table");

        // The baseline's CREATE TABLE IF NOT EXISTS is a no-op here, but its
        // ADD COLUMN backfills must still widen the table.
        run_sqlite(
            &mut conn,
            "schema_migrations",
            &crate::storage::migrations::all(),
        )
        .expect("upgrade existing db");

        // Prove `namespace` now exists: selecting it parses only if the column
        // is present (empty result set is fine).
        #[derive(diesel::QueryableByName)]
        struct Ns {
            #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
            #[allow(dead_code)]
            namespace: Option<String>,
        }
        let select = Query::select()
            .column(Alias::new("namespace"))
            .from(Alias::new("jobs"))
            .to_owned()
            .to_string(SqliteQueryBuilder);
        let _rows: Vec<Ns> = diesel::sql_query(select)
            .load(&mut conn)
            .expect("namespace column was added by the baseline backfill");

        assert!(applied(&mut conn).iter().any(|v| v == "0001_initial"));
    }
}
