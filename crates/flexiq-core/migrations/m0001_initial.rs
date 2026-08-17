//! Baseline schema (`0001_initial`).
//!
//! A faithful, idempotent reproduction of the historical inline bring-up:
//! `CREATE TABLE IF NOT EXISTS` for every table, the historical `ADD COLUMN`
//! backfills (so a database that predates a column is upgraded in place), every
//! index, the `has_deps` data backfill, and the drop of the legacy
//! `job_payloads` side table — all built with sea-query, no hand-written SQL.
//! Because every statement is idempotent, running this once against a
//! pre-existing database is a harmless no-op pass before it is recorded.

use sea_query::{
    Alias, ColumnDef, ConditionalStatement, Expr, ExprTrait, Index, IndexOrder, Query, Table,
};

use crate::storage::migrate::{add_column, ddl, dml, Backend, Migration, Stmt};

pub struct M0001Initial;

/// Column definition shorthand.
fn col(name: &str) -> ColumnDef {
    ColumnDef::new(Alias::new(name))
}

/// Table/identifier shorthand.
fn t(name: &str) -> Alias {
    Alias::new(name)
}

impl Migration for M0001Initial {
    fn version(&self) -> &'static str {
        "0001_initial"
    }

    fn up(&self, b: Backend) -> Vec<Stmt> {
        let mut out = Vec::new();
        create_tables(b, &mut out);
        alter_columns(b, &mut out);
        create_indexes(b, &mut out);
        backfill(b, &mut out);
        drop_legacy(b, &mut out);
        out
    }
}

fn create_tables(b: Backend, out: &mut Vec<Stmt>) {
    let jobs = Table::create()
        .table(t("jobs"))
        .if_not_exists()
        .col(col("id").text().not_null().primary_key())
        .col(col("queue").text().not_null().default("default"))
        .col(col("task_name").text().not_null())
        .col(col("payload").blob().not_null())
        .col(col("status").integer().not_null().default(0))
        .col(col("priority").integer().not_null().default(0))
        .col(col("created_at").big_integer().not_null())
        .col(col("scheduled_at").big_integer().not_null())
        .col(col("started_at").big_integer())
        .col(col("completed_at").big_integer())
        .col(col("retry_count").integer().not_null().default(0))
        .col(col("max_retries").integer().not_null().default(3))
        .col(col("result").blob())
        .col(col("error").text())
        .col(
            col("timeout_ms")
                .big_integer()
                .not_null()
                .default(300_000i64),
        )
        .col(col("unique_key").text())
        .col(col("progress").integer())
        .col(col("metadata").text())
        .col(col("notes").text())
        .col(col("cancel_requested").integer().not_null().default(0))
        .col(col("expires_at").big_integer())
        .col(col("result_ttl_ms").big_integer())
        .col(col("has_deps").boolean().not_null().default(false))
        .to_owned();
    out.push(ddl(b, &jobs));

    let dead_letter = Table::create()
        .table(t("dead_letter"))
        .if_not_exists()
        .col(col("id").text().not_null().primary_key())
        .col(col("original_job_id").text().not_null())
        .col(col("queue").text().not_null())
        .col(col("task_name").text().not_null())
        .col(col("payload").blob().not_null())
        .col(col("error").text())
        .col(col("retry_count").integer().not_null())
        .col(col("failed_at").big_integer().not_null())
        .col(col("metadata").text())
        .col(col("notes").text())
        .col(col("priority").integer().not_null().default(0))
        .col(col("max_retries").integer().not_null().default(3))
        .col(
            col("timeout_ms")
                .big_integer()
                .not_null()
                .default(300_000i64),
        )
        .col(col("result_ttl_ms").big_integer())
        .to_owned();
    out.push(ddl(b, &dead_letter));

    let rate_limits = Table::create()
        .table(t("rate_limits"))
        .if_not_exists()
        .col(col("key").text().not_null().primary_key())
        .col(col("tokens").double().not_null())
        .col(col("max_tokens").double().not_null())
        .col(col("refill_rate").double().not_null())
        .col(col("last_refill").big_integer().not_null())
        .to_owned();
    out.push(ddl(b, &rate_limits));

    let periodic_tasks = Table::create()
        .table(t("periodic_tasks"))
        .if_not_exists()
        .col(col("name").text().not_null().primary_key())
        .col(col("task_name").text().not_null())
        .col(col("cron_expr").text().not_null())
        .col(col("args").blob())
        .col(col("kwargs").blob())
        .col(col("queue").text().not_null().default("default"))
        .col(col("enabled").boolean().not_null().default(true))
        .col(col("last_run").big_integer())
        .col(col("next_run").big_integer().not_null())
        .to_owned();
    out.push(ddl(b, &periodic_tasks));

    let job_errors = Table::create()
        .table(t("job_errors"))
        .if_not_exists()
        .col(col("id").text().not_null().primary_key())
        .col(col("job_id").text().not_null())
        .col(col("attempt").integer().not_null())
        .col(col("error").text().not_null())
        .col(col("failed_at").big_integer().not_null())
        .to_owned();
    out.push(ddl(b, &job_errors));

    let job_dependencies = Table::create()
        .table(t("job_dependencies"))
        .if_not_exists()
        .col(col("id").text().not_null().primary_key())
        .col(col("job_id").text().not_null())
        .col(col("depends_on_job_id").text().not_null())
        .to_owned();
    out.push(ddl(b, &job_dependencies));

    let task_metrics = Table::create()
        .table(t("task_metrics"))
        .if_not_exists()
        .col(col("id").text().not_null().primary_key())
        .col(col("task_name").text().not_null())
        .col(col("job_id").text().not_null())
        .col(col("wall_time_ns").big_integer().not_null())
        .col(col("memory_bytes").big_integer().not_null().default(0))
        .col(col("succeeded").boolean().not_null().default(true))
        .col(col("recorded_at").big_integer().not_null())
        .to_owned();
    out.push(ddl(b, &task_metrics));

    let replay_history = Table::create()
        .table(t("replay_history"))
        .if_not_exists()
        .col(col("id").text().not_null().primary_key())
        .col(col("original_job_id").text().not_null())
        .col(col("replay_job_id").text().not_null())
        .col(col("replayed_at").big_integer().not_null())
        .col(col("original_result").blob())
        .col(col("replay_result").blob())
        .col(col("original_error").text())
        .col(col("replay_error").text())
        .to_owned();
    out.push(ddl(b, &replay_history));

    let task_logs = Table::create()
        .table(t("task_logs"))
        .if_not_exists()
        .col(col("id").text().not_null().primary_key())
        .col(col("job_id").text().not_null())
        .col(col("task_name").text().not_null())
        .col(col("level").text().not_null().default("info"))
        .col(col("message").text().not_null())
        .col(col("extra").text())
        .col(col("logged_at").big_integer().not_null())
        .to_owned();
    out.push(ddl(b, &task_logs));

    let circuit_breakers = Table::create()
        .table(t("circuit_breakers"))
        .if_not_exists()
        .col(col("task_name").text().not_null().primary_key())
        .col(col("state").integer().not_null().default(0))
        .col(col("failure_count").integer().not_null().default(0))
        .col(col("last_failure_at").big_integer())
        .col(col("opened_at").big_integer())
        .col(col("half_open_at").big_integer())
        .col(col("threshold").integer().not_null().default(5))
        .col(col("window_ms").big_integer().not_null().default(60_000i64))
        .col(
            col("cooldown_ms")
                .big_integer()
                .not_null()
                .default(300_000i64),
        )
        .col(col("half_open_max_probes").integer().not_null().default(5))
        .col(
            col("half_open_success_rate")
                .double()
                .not_null()
                .default(0.8),
        )
        .col(col("half_open_probe_count").integer().not_null().default(0))
        .col(
            col("half_open_success_count")
                .integer()
                .not_null()
                .default(0),
        )
        .col(
            col("half_open_failure_count")
                .integer()
                .not_null()
                .default(0),
        )
        .to_owned();
    out.push(ddl(b, &circuit_breakers));

    let workers = Table::create()
        .table(t("workers"))
        .if_not_exists()
        .col(col("worker_id").text().not_null().primary_key())
        .col(col("last_heartbeat").big_integer().not_null())
        .col(col("queues").text().not_null().default("default"))
        .col(col("status").text().not_null().default("active"))
        .to_owned();
    out.push(ddl(b, &workers));

    let queue_state = Table::create()
        .table(t("queue_state"))
        .if_not_exists()
        .col(col("queue_name").text().not_null().primary_key())
        .col(col("paused").boolean().not_null().default(false))
        .col(col("paused_at").big_integer())
        .to_owned();
    out.push(ddl(b, &queue_state));

    let archived_jobs = Table::create()
        .table(t("archived_jobs"))
        .if_not_exists()
        .col(col("id").text().not_null().primary_key())
        .col(col("queue").text().not_null().default("default"))
        .col(col("task_name").text().not_null())
        .col(col("payload").blob().not_null())
        .col(col("status").integer().not_null().default(0))
        .col(col("priority").integer().not_null().default(0))
        .col(col("created_at").big_integer().not_null())
        .col(col("scheduled_at").big_integer().not_null())
        .col(col("started_at").big_integer())
        .col(col("completed_at").big_integer())
        .col(col("retry_count").integer().not_null().default(0))
        .col(col("max_retries").integer().not_null().default(3))
        .col(col("result").blob())
        .col(col("error").text())
        .col(
            col("timeout_ms")
                .big_integer()
                .not_null()
                .default(300_000i64),
        )
        .col(col("unique_key").text())
        .col(col("progress").integer())
        .col(col("metadata").text())
        .col(col("notes").text())
        .col(col("cancel_requested").integer().not_null().default(0))
        .col(col("expires_at").big_integer())
        .col(col("result_ttl_ms").big_integer())
        .to_owned();
    out.push(ddl(b, &archived_jobs));

    let distributed_locks = Table::create()
        .table(t("distributed_locks"))
        .if_not_exists()
        .col(col("lock_name").text().not_null().primary_key())
        .col(col("owner_id").text().not_null())
        .col(col("acquired_at").big_integer().not_null())
        .col(col("expires_at").big_integer().not_null())
        .to_owned();
    out.push(ddl(b, &distributed_locks));

    let execution_claims = Table::create()
        .table(t("execution_claims"))
        .if_not_exists()
        .col(col("job_id").text().not_null().primary_key())
        .col(col("worker_id").text().not_null())
        .col(col("claimed_at").big_integer().not_null())
        .to_owned();
    out.push(ddl(b, &execution_claims));

    let dashboard_settings = Table::create()
        .table(t("dashboard_settings"))
        .if_not_exists()
        .col(col("key").text().not_null().primary_key())
        .col(col("value").text().not_null())
        .col(col("updated_at").big_integer().not_null())
        .to_owned();
    out.push(ddl(b, &dashboard_settings));

    // Composite primary key (topic, subscription_name).
    let topic_subscriptions = Table::create()
        .table(t("topic_subscriptions"))
        .if_not_exists()
        .col(col("topic").text().not_null())
        .col(col("subscription_name").text().not_null())
        .col(col("task_name").text().not_null())
        .col(col("queue").text().not_null().default("default"))
        .col(col("active").boolean().not_null().default(true))
        .col(col("durable").boolean().not_null().default(true))
        .col(col("owner_worker_id").text())
        .col(col("created_at").big_integer().not_null())
        .col(col("priority").integer())
        .col(col("max_retries").integer())
        .col(col("timeout_ms").big_integer())
        .primary_key(Index::create().col(t("topic")).col(t("subscription_name")))
        .to_owned();
    out.push(ddl(b, &topic_subscriptions));
}

fn alter_columns(b: Backend, out: &mut Vec<Stmt>) {
    // jobs — cancel_requested / expires_at / result_ttl_ms
    out.push(add_column(
        b,
        "jobs",
        col("cancel_requested").integer().not_null().default(0),
    ));
    out.push(add_column(b, "jobs", col("expires_at").big_integer()));
    out.push(add_column(b, "jobs", col("result_ttl_ms").big_integer()));
    // dead_letter retry / timeout / ttl backfill
    out.push(add_column(
        b,
        "dead_letter",
        col("priority").integer().not_null().default(0),
    ));
    out.push(add_column(
        b,
        "dead_letter",
        col("max_retries").integer().not_null().default(3),
    ));
    out.push(add_column(
        b,
        "dead_letter",
        col("timeout_ms")
            .big_integer()
            .not_null()
            .default(300_000i64),
    ));
    out.push(add_column(
        b,
        "dead_letter",
        col("result_ttl_ms").big_integer(),
    ));
    // workers metadata backfill
    out.push(add_column(b, "workers", col("tags").text()));
    out.push(add_column(b, "workers", col("resources").text()));
    out.push(add_column(b, "workers", col("resource_health").text()));
    out.push(add_column(
        b,
        "workers",
        col("threads").integer().not_null().default(0),
    ));
    out.push(add_column(b, "workers", col("started_at").big_integer()));
    out.push(add_column(b, "workers", col("hostname").text()));
    out.push(add_column(b, "workers", col("pid").integer()));
    out.push(add_column(b, "workers", col("pool_type").text()));
    // periodic_tasks timezone
    out.push(add_column(b, "periodic_tasks", col("timezone").text()));
    // jobs namespace
    out.push(add_column(b, "jobs", col("namespace").text()));
    // sample-based half-open circuit-breaker probe counters
    out.push(add_column(
        b,
        "circuit_breakers",
        col("half_open_max_probes").integer().not_null().default(5),
    ));
    out.push(add_column(
        b,
        "circuit_breakers",
        col("half_open_success_rate")
            .double()
            .not_null()
            .default(0.8),
    ));
    out.push(add_column(
        b,
        "circuit_breakers",
        col("half_open_probe_count").integer().not_null().default(0),
    ));
    out.push(add_column(
        b,
        "circuit_breakers",
        col("half_open_success_count")
            .integer()
            .not_null()
            .default(0),
    ));
    out.push(add_column(
        b,
        "circuit_breakers",
        col("half_open_failure_count")
            .integer()
            .not_null()
            .default(0),
    ));
    // namespace backfill on dead_letter / archived_jobs
    out.push(add_column(b, "dead_letter", col("namespace").text()));
    out.push(add_column(b, "archived_jobs", col("namespace").text()));
    // structured notes
    out.push(add_column(b, "jobs", col("notes").text()));
    out.push(add_column(b, "dead_letter", col("notes").text()));
    out.push(add_column(b, "archived_jobs", col("notes").text()));
    // has_deps fast-path flag
    out.push(add_column(
        b,
        "jobs",
        col("has_deps").boolean().not_null().default(false),
    ));
    // DLQ auto-retry counter
    out.push(add_column(
        b,
        "dead_letter",
        col("dlq_retry_count").integer().not_null().default(0),
    ));
    // per-subscription delivery settings
    out.push(add_column(
        b,
        "topic_subscriptions",
        col("priority").integer(),
    ));
    out.push(add_column(
        b,
        "topic_subscriptions",
        col("max_retries").integer(),
    ));
    out.push(add_column(
        b,
        "topic_subscriptions",
        col("timeout_ms").big_integer(),
    ));
    // pub/sub delivery attribution
    out.push(add_column(b, "jobs", col("topic").text()));
    out.push(add_column(b, "jobs", col("subscription_name").text()));
    out.push(add_column(b, "dead_letter", col("topic").text()));
    out.push(add_column(
        b,
        "dead_letter",
        col("subscription_name").text(),
    ));
}

fn create_indexes(b: Backend, out: &mut Vec<Stmt>) {
    // `.to_owned()` produces an owned statement so it can be passed by shared
    // reference to `ddl` (which is generic over `&S`, not `&mut S`).
    let idx = |name: &str| Index::create().if_not_exists().name(name).to_owned();

    out.push(ddl(
        b,
        &idx("idx_jobs_dequeue")
            .table(t("jobs"))
            .col(t("queue"))
            .col(t("status"))
            .col((t("priority"), IndexOrder::Desc))
            .col(t("scheduled_at"))
            .to_owned(),
    ));
    out.push(ddl(
        b,
        &idx("idx_jobs_status")
            .table(t("jobs"))
            .col(t("status"))
            .to_owned(),
    ));
    out.push(ddl(
        b,
        &idx("idx_jobs_unique_key")
            .unique()
            .table(t("jobs"))
            .col(t("unique_key"))
            .and_where(
                Expr::col(t("unique_key"))
                    .is_not_null()
                    .and(Expr::col(t("status")).is_in([0, 1])),
            )
            .to_owned(),
    ));
    out.push(ddl(
        b,
        &idx("idx_job_errors_job_id")
            .table(t("job_errors"))
            .col(t("job_id"))
            .to_owned(),
    ));
    out.push(ddl(
        b,
        &idx("idx_job_deps_job_id")
            .table(t("job_dependencies"))
            .col(t("job_id"))
            .to_owned(),
    ));
    out.push(ddl(
        b,
        &idx("idx_job_deps_depends_on")
            .table(t("job_dependencies"))
            .col(t("depends_on_job_id"))
            .to_owned(),
    ));
    out.push(ddl(
        b,
        &idx("idx_task_metrics_task_name")
            .table(t("task_metrics"))
            .col(t("task_name"))
            .to_owned(),
    ));
    out.push(ddl(
        b,
        &idx("idx_task_metrics_recorded_at")
            .table(t("task_metrics"))
            .col(t("recorded_at"))
            .to_owned(),
    ));
    out.push(ddl(
        b,
        &idx("idx_replay_original")
            .table(t("replay_history"))
            .col(t("original_job_id"))
            .to_owned(),
    ));
    out.push(ddl(
        b,
        &idx("idx_task_logs_job_id")
            .table(t("task_logs"))
            .col(t("job_id"))
            .to_owned(),
    ));
    out.push(ddl(
        b,
        &idx("idx_task_logs_recorded")
            .table(t("task_logs"))
            .col(t("logged_at"))
            .to_owned(),
    ));
    out.push(ddl(
        b,
        &idx("idx_archived_jobs_completed")
            .table(t("archived_jobs"))
            .col(t("completed_at"))
            .to_owned(),
    ));
    out.push(ddl(
        b,
        &idx("idx_archived_jobs_status")
            .table(t("archived_jobs"))
            .col(t("status"))
            .to_owned(),
    ));
    out.push(ddl(
        b,
        &idx("idx_archived_jobs_queue_status")
            .table(t("archived_jobs"))
            .col(t("queue"))
            .col(t("status"))
            .to_owned(),
    ));
    out.push(ddl(
        b,
        &idx("idx_distributed_locks_expires")
            .table(t("distributed_locks"))
            .col(t("expires_at"))
            .to_owned(),
    ));
    out.push(ddl(
        b,
        &idx("idx_execution_claims_claimed")
            .table(t("execution_claims"))
            .col(t("claimed_at"))
            .to_owned(),
    ));
    out.push(ddl(
        b,
        &idx("idx_dead_letter_failed_at")
            .table(t("dead_letter"))
            .col(t("failed_at"))
            .to_owned(),
    ));
    out.push(ddl(
        b,
        &idx("idx_dead_letter_task")
            .table(t("dead_letter"))
            .col(t("task_name"))
            .to_owned(),
    ));
    out.push(ddl(
        b,
        &idx("idx_jobs_task_status")
            .table(t("jobs"))
            .col(t("task_name"))
            .col(t("status"))
            .to_owned(),
    ));
    out.push(ddl(
        b,
        &idx("idx_jobs_expires_at")
            .table(t("jobs"))
            .col(t("expires_at"))
            .and_where(Expr::col(t("expires_at")).is_not_null())
            .to_owned(),
    ));
    out.push(ddl(
        b,
        &idx("idx_jobs_namespace")
            .table(t("jobs"))
            .col(t("namespace"))
            .and_where(Expr::col(t("namespace")).is_not_null())
            .to_owned(),
    ));
    out.push(ddl(
        b,
        &idx("idx_archived_jobs_created_at")
            .table(t("archived_jobs"))
            .col(t("created_at"))
            .to_owned(),
    ));
    out.push(ddl(
        b,
        &idx("idx_archived_jobs_namespace")
            .table(t("archived_jobs"))
            .col(t("namespace"))
            .to_owned(),
    ));
    out.push(ddl(
        b,
        &idx("idx_topic_subs_owner")
            .table(t("topic_subscriptions"))
            .col(t("owner_worker_id"))
            .and_where(Expr::col(t("owner_worker_id")).is_not_null())
            .to_owned(),
    ));
    out.push(ddl(
        b,
        &idx("idx_jobs_subscription_status")
            .table(t("jobs"))
            .col(t("topic"))
            .col(t("subscription_name"))
            .col(t("status"))
            .and_where(Expr::col(t("subscription_name")).is_not_null())
            .to_owned(),
    ));
    out.push(ddl(
        b,
        &idx("idx_dead_letter_subscription")
            .table(t("dead_letter"))
            .col(t("topic"))
            .col(t("subscription_name"))
            .and_where(Expr::col(t("subscription_name")).is_not_null())
            .to_owned(),
    ));
}

fn backfill(b: Backend, out: &mut Vec<Stmt>) {
    // Backfill has_deps for pre-existing pending jobs whose dependency state
    // would otherwise default to false. Guarded so it only touches rows whose
    // value actually differs — a no-op after the first run and on fresh DBs.
    let in_deps = || {
        Expr::col(t("id")).in_subquery(
            Query::select()
                .column(t("job_id"))
                .from(t("job_dependencies"))
                .to_owned(),
        )
    };
    let update = Query::update()
        .table(t("jobs"))
        .value(t("has_deps"), in_deps())
        .and_where(Expr::col(t("status")).eq(0))
        .and_where(Expr::col(t("has_deps")).ne(in_deps()))
        .to_owned();
    out.push(dml(b, &update));
}

fn drop_legacy(b: Backend, out: &mut Vec<Stmt>) {
    // `job_payloads` was a 1:1 side table superseded by inline payload/result.
    let drop = Table::drop()
        .table(t("job_payloads"))
        .if_exists()
        .to_owned();
    out.push(ddl(b, &drop));
}
