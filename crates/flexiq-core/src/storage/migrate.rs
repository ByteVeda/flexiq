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
use sea_query::{Alias, ColumnDef, Query, SchemaStatementBuilder, SqliteQueryBuilder, Table};

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

/// The one SQLite error a statement is allowed to treat as success.
///
/// SQLite has neither `ADD COLUMN IF NOT EXISTS` nor `DROP COLUMN IF EXISTS`,
/// so it signals the already-done case with an error where Postgres renders the
/// `IF …EXISTS` clause and succeeds. Swallowing exactly one message per
/// statement kind keeps an idempotent alter idempotent on both backends without
/// widening into "ignore ALTER failures".
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Tolerate {
    /// Every error is genuine.
    Nothing,
    /// `ADD COLUMN` on a column that is already there.
    DuplicateColumn,
    /// `DROP COLUMN` on a column that is already gone.
    MissingColumn,
}

impl Tolerate {
    /// Whether this SQLite error is the already-done case this statement expects.
    fn absorbs(self, error: &str) -> bool {
        match self {
            Self::Nothing => false,
            Self::DuplicateColumn => error.contains("duplicate column"),
            Self::MissingColumn => error.contains("no such column"),
        }
    }
}

/// One rendered statement plus the SQLite error it tolerates, if any.
pub struct Stmt {
    sql: String,
    tolerate: Tolerate,
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
        tolerate: Tolerate::Nothing,
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
        tolerate: Tolerate::DuplicateColumn,
    }
}

/// Render an `ALTER TABLE … DROP COLUMN` that must be idempotent. Postgres emits
/// `DROP COLUMN IF EXISTS`; SQLite has no such clause, so sea-query renders a
/// plain `DROP COLUMN` there and the missing-column error is swallowed at
/// execution time.
///
/// Dropping a column is one-way and invisible to older readers — a build whose
/// `SELECT` still names it fails on every read of the table — so a migration
/// that calls this belongs with a [`CONTRACT_VERSION`](crate::CONTRACT_VERSION)
/// bump.
pub fn drop_column(backend: Backend, table: &str, column: &str) -> Stmt {
    let mut alter = Table::alter();
    alter
        .table(Alias::new(table))
        .drop_column_if_exists(Alias::new(column));
    Stmt {
        sql: render_schema(&alter, backend),
        tolerate: Tolerate::MissingColumn,
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
        tolerate: Tolerate::Nothing,
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
        tolerate: Tolerate::Nothing,
    }
}

/// Render an `INSERT … SELECT` — how a table rebuild carries the old rows into
/// the new shape. Column lists are code-defined; no literal and no user input
/// reaches here.
pub fn insert_select(backend: Backend, stmt: &sea_query::InsertStatement) -> Stmt {
    Stmt {
        sql: match backend {
            Backend::Sqlite => stmt.to_string(SqliteQueryBuilder),
            #[cfg(feature = "postgres")]
            Backend::Postgres => stmt.to_string(PostgresQueryBuilder),
            #[cfg(not(feature = "postgres"))]
            Backend::Postgres => unreachable!("Postgres migrations require the `postgres` feature"),
        },
        tolerate: Tolerate::Nothing,
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

/// The ledger insert, which is also the lock two booting processes take against
/// each other.
///
/// Deliberately **not** `ON CONFLICT DO NOTHING`. `run_generic` runs this first
/// inside the migration's transaction, so a second process that read the same
/// version as pending blocks on the primary key and then loses it — rolling its
/// transaction back before it has applied anything. Swallowing the conflict
/// would let both apply the same one-shot DDL, which a table rebuild like
/// `m0018` does not survive twice.
fn record_version_sql(table: &str, version: &str, now: i64, backend: Backend) -> String {
    let stmt = Query::insert()
        .into_table(Alias::new(table))
        .columns([Alias::new(VERSION_COL), Alias::new(APPLIED_AT_COL)])
        .values_panic([version.into(), now.into()])
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
    fn exec(&mut self, sql: &str, tolerate: Tolerate) -> Result<()>;
    fn load_versions(&mut self, select_sql: &str) -> Result<Vec<String>>;
}

impl MigrationConn for SqliteConnection {
    fn exec(&mut self, sql: &str, tolerate: Tolerate) -> Result<()> {
        match diesel::sql_query(sql).execute(self) {
            Ok(_) => Ok(()),
            Err(e) if tolerate.absorbs(&e.to_string()) => Ok(()),
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
    fn exec(&mut self, sql: &str, _tolerate: Tolerate) -> Result<()> {
        // Postgres alters render `IF NOT EXISTS`/`IF EXISTS`, so nothing to
        // swallow — every error here is genuine and must propagate.
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
    conn.exec(
        &create_ledger_sql(tracking_table, backend),
        Tolerate::Nothing,
    )?;
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
        // Record the version and apply the statements in one transaction: an
        // interruption between the two would otherwise leave an applied change
        // untracked, so the next boot re-runs it — safe for the idempotent
        // baseline, but not for later one-shot DDL.
        //
        // The recording goes *first* because the ledger's primary key is the
        // only thing serializing two processes booting at once. `applied` was
        // read outside this transaction, so both can see the same version as
        // pending; the loser blocks on the duplicate key here and rolls back
        // before touching the schema, instead of rebuilding a table the winner
        // has already rebuilt.
        let outcome = conn.transaction::<_, QueueError, _>(|conn| {
            conn.exec(
                &record_version_sql(tracking_table, migration.version(), now_millis(), backend),
                Tolerate::Nothing,
            )?;
            for stmt in migration.up(backend) {
                conn.exec(&stmt.sql, stmt.tolerate)?;
            }
            Ok(())
        });

        if let Err(err) = outcome {
            // A unique violation is either the race above or a migration's own
            // DDL colliding with existing data. Only the first is survivable,
            // and the ledger is what tells them apart: the winner's row is
            // committed by the time our insert is refused.
            let lost_the_race = is_unique_violation(&err)
                && conn
                    .load_versions(&select_versions_sql(tracking_table, backend))?
                    .iter()
                    .any(|v| v == migration.version());
            if !lost_the_race {
                return Err(err);
            }
            continue;
        }
        newly_applied.push(migration.version().to_string());
    }
    Ok(newly_applied)
}

/// Whether this error is a row refused by a unique index — the shape the ledger
/// takes when a second process has already recorded the version.
fn is_unique_violation(err: &QueueError) -> bool {
    matches!(
        err,
        QueueError::Storage(diesel::result::Error::DatabaseError(
            diesel::result::DatabaseErrorKind::UniqueViolation,
            _
        ))
    )
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
        assert_eq!(stmt.tolerate, Tolerate::Nothing);
    }

    #[test]
    fn drop_column_renders_the_dialect_each_backend_accepts() {
        // SQLite has no `DROP COLUMN IF EXISTS` — emitting one would be a syntax
        // error, so there the idempotence has to come from the swallow instead.
        let sqlite = drop_column(Backend::Sqlite, "workflow_nodes", "fan_in_data");
        assert!(sqlite.sql.contains("DROP COLUMN"), "{}", sqlite.sql);
        assert!(!sqlite.sql.contains("IF EXISTS"), "{}", sqlite.sql);
        assert_eq!(sqlite.tolerate, Tolerate::MissingColumn);

        #[cfg(feature = "postgres")]
        {
            let pg = drop_column(Backend::Postgres, "workflow_nodes", "fan_in_data");
            assert!(pg.sql.contains("DROP COLUMN IF EXISTS"), "{}", pg.sql);
        }
    }

    #[test]
    fn drop_column_is_a_no_op_when_the_column_is_already_gone() {
        // Re-dropping must not fail: a database that never had the column (or
        // that a partial run already widened) still has to reach the ledger row.
        let mut conn = mem();
        let table = Table::create()
            .table(Alias::new("widget"))
            .col(ColumnDef::new(Alias::new("id")).text().not_null())
            .col(ColumnDef::new(Alias::new("scrap")).text())
            .to_owned();
        conn.exec(&table.to_string(SqliteQueryBuilder), Tolerate::Nothing)
            .expect("seed table");

        let stmt = drop_column(Backend::Sqlite, "widget", "scrap");
        conn.exec(&stmt.sql, stmt.tolerate).expect("first drop");
        conn.exec(&stmt.sql, stmt.tolerate)
            .expect("second drop is tolerated");

        // The tolerance is scoped to the missing column, not to ALTER failures
        // at large: a bad table name must still surface.
        let missing_table = drop_column(Backend::Sqlite, "no_such_table", "scrap");
        assert!(conn
            .exec(&missing_table.sql, missing_table.tolerate)
            .is_err());
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

    /// Seeds `probe`, the table the two race tests below watch.
    fn seed_probe(conn: &mut SqliteConnection) {
        let probe = Table::create()
            .table(Alias::new("probe"))
            .col(ColumnDef::new(Alias::new("version")).text().not_null())
            .to_owned();
        conn.exec(&probe.to_string(SqliteQueryBuilder), Tolerate::Nothing)
            .expect("seed probe table");
    }

    fn probe_rows(conn: &mut SqliteConnection) -> Vec<String> {
        #[derive(diesel::QueryableByName)]
        struct Row {
            #[diesel(sql_type = diesel::sql_types::Text)]
            version: String,
        }
        let select = Query::select()
            .column(Alias::new("version"))
            .from(Alias::new("probe"))
            .to_owned()
            .to_string(SqliteQueryBuilder);
        let rows: Vec<Row> = diesel::sql_query(select).load(conn).expect("read probe");
        rows.into_iter().map(|r| r.version).collect()
    }

    /// Copies its own ledger row into `probe`, which only finds anything if the
    /// recording ran before the migration's own statements.
    struct CopiesItsOwnLedgerRow;

    impl Migration for CopiesItsOwnLedgerRow {
        fn version(&self) -> &'static str {
            "0001_probe"
        }
        fn up(&self, backend: Backend) -> Vec<Stmt> {
            // No predicate needed: this is the first migration of its run, so
            // the only row the ledger can hold is the one recorded for it.
            let mine = Query::select()
                .column(Alias::new(VERSION_COL))
                .from(Alias::new("schema_migrations"))
                .to_owned();
            let mut copy = Query::insert();
            copy.into_table(Alias::new("probe"))
                .columns([Alias::new("version")]);
            copy.select_from(mine).expect("one column either side");
            vec![insert_select(backend, &copy.to_owned())]
        }
    }

    /// Stands in for the other process in the race: it records a *later*
    /// version, so by the time the runner reaches that migration the ledger row
    /// is there although the `applied` set read at the start said it was pending.
    struct RecordsTheNextVersion;

    impl Migration for RecordsTheNextVersion {
        fn version(&self) -> &'static str {
            "0001_racer"
        }
        fn up(&self, backend: Backend) -> Vec<Stmt> {
            vec![raw_ddl(record_version_sql(
                "schema_migrations",
                "0002_rebuild",
                0,
                backend,
            ))]
        }
    }

    /// The kind of migration that must never run twice — `m0018` rebuilds a
    /// table, and a second pass would carry the rows through a shape that has
    /// lost a column.
    struct DestroysTheProbe;

    impl Migration for DestroysTheProbe {
        fn version(&self) -> &'static str {
            "0002_rebuild"
        }
        fn up(&self, _backend: Backend) -> Vec<Stmt> {
            vec![ddl(
                Backend::Sqlite,
                &Table::drop().table(Alias::new("probe")).to_owned(),
            )]
        }
    }

    #[test]
    fn the_ledger_row_lands_before_the_migration_runs() {
        // Ordering is what makes the ledger's primary key a lock: the loser has
        // to block on the key *before* it applies any DDL, not after.
        let mut conn = mem();
        seed_probe(&mut conn);

        let migrations: Vec<Box<dyn Migration>> = vec![Box::new(CopiesItsOwnLedgerRow)];
        run_sqlite(&mut conn, "schema_migrations", &migrations).expect("apply");

        assert_eq!(probe_rows(&mut conn), vec!["0001_probe".to_string()]);
    }

    #[test]
    fn a_version_another_process_recorded_is_not_applied_again() {
        // The race m0018 made dangerous: both processes read the version as
        // pending, and the loser must skip it rather than rebuild the table the
        // winner already rebuilt.
        let mut conn = mem();
        seed_probe(&mut conn);

        let migrations: Vec<Box<dyn Migration>> =
            vec![Box::new(RecordsTheNextVersion), Box::new(DestroysTheProbe)];
        let newly = run_sqlite(&mut conn, "schema_migrations", &migrations)
            .expect("losing the race is not an error");

        assert_eq!(newly, vec!["0001_racer".to_string()]);
        assert!(
            probe_rows(&mut conn).is_empty(),
            "the drop must not have run"
        );
        assert!(applied(&mut conn).iter().any(|v| v == "0002_rebuild"));
    }

    #[test]
    fn a_migrations_own_unique_violation_still_fails_the_run() {
        // The skip above keys on the ledger, not on the error kind alone: a
        // migration whose own DDL collides with existing data must not be
        // mistaken for a lost race and silently passed over.
        struct DuplicateIndex;
        impl Migration for DuplicateIndex {
            fn version(&self) -> &'static str {
                "0001_dup"
            }
            fn up(&self, _backend: Backend) -> Vec<Stmt> {
                vec![raw_ddl(
                    "CREATE UNIQUE INDEX idx_probe_version ON probe (version)",
                )]
            }
        }

        let mut conn = mem();
        seed_probe(&mut conn);
        for _ in 0..2 {
            conn.exec(
                &Query::insert()
                    .into_table(Alias::new("probe"))
                    .columns([Alias::new("version")])
                    .values_panic(["same".into()])
                    .to_owned()
                    .to_string(SqliteQueryBuilder),
                Tolerate::Nothing,
            )
            .expect("seed duplicate rows");
        }

        let migrations: Vec<Box<dyn Migration>> = vec![Box::new(DuplicateIndex)];
        let err = run_sqlite(&mut conn, "schema_migrations", &migrations)
            .expect_err("the index cannot be built over duplicate rows");
        assert!(is_unique_violation(&err), "{err:?}");
        assert!(
            !applied(&mut conn).iter().any(|v| v == "0001_dup"),
            "a failed migration must not be recorded"
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
        conn.exec(&old_jobs.to_string(SqliteQueryBuilder), Tolerate::Nothing)
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
