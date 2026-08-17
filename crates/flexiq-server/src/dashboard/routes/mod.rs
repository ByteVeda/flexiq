//! The JSON API the dashboard SPA calls.
//!
//! Paths, methods, and response bodies match the SDK dashboards exactly — one
//! SPA build is served by all of them, so a divergence here is a broken page,
//! not a variation.

pub mod dead_letters;
pub mod executors;
pub mod jobs;
pub mod logs;
pub mod metrics;
pub mod middleware;
pub mod overrides;
pub mod queues;
pub mod resources;
pub mod retention;
pub mod settings;
pub mod topics;
pub mod webhooks;
pub mod workflows;

use axum::routing::{delete, get, post, put};
use axum::Router;

use crate::dashboard::state::SharedState;

/// Every API route, ready to be merged into the server's router.
pub fn router() -> Router<SharedState> {
    Router::new()
        // ── Jobs ────────────────────────────────────────────────────
        .route("/api/jobs", get(jobs::list))
        .route("/api/jobs/{job_id}", get(jobs::detail))
        .route("/api/jobs/{job_id}/errors", get(jobs::errors))
        .route("/api/jobs/{job_id}/logs", get(jobs::logs))
        .route(
            "/api/jobs/{job_id}/replay-history",
            get(jobs::replay_history),
        )
        .route("/api/jobs/{job_id}/dag", get(jobs::dag))
        .route("/api/jobs/{job_id}/cancel", post(jobs::cancel))
        .route("/api/jobs/{job_id}/replay", post(jobs::replay))
        // ── Dead letters ────────────────────────────────────────────
        .route("/api/dead-letters", get(dead_letters::list))
        .route("/api/dead-letters/purge", post(dead_letters::purge))
        .route("/api/dead-letters/{dead_id}", delete(dead_letters::delete))
        .route(
            "/api/dead-letters/{dead_id}/retry",
            post(dead_letters::retry),
        )
        // ── Queues and capacity ─────────────────────────────────────
        .route("/api/stats", get(queues::stats))
        .route("/api/stats/queues", get(queues::stats_by_queue))
        .route("/api/workers", get(queues::workers))
        .route("/api/circuit-breakers", get(queues::circuit_breakers))
        .route("/api/queues/paused", get(queues::paused))
        .route("/api/queues/{queue}/pause", post(queues::pause))
        .route("/api/queues/{queue}/resume", post(queues::resume))
        .route("/api/scaler", get(queues::scaler))
        .route("/api/executors", get(executors::list))
        // ── Observability ───────────────────────────────────────────
        .route("/api/metrics", get(metrics::aggregate))
        .route("/api/metrics/timeseries", get(metrics::timeseries))
        .route("/api/logs", get(logs::query))
        .route("/api/resources", get(resources::status))
        .route("/api/proxy-stats", get(resources::proxy_stats))
        .route(
            "/api/interception-stats",
            get(resources::interception_stats),
        )
        .route("/api/retention", get(retention::published))
        .route("/api/retention/dry-run", get(retention::preview))
        // ── Settings ────────────────────────────────────────────────
        .route("/api/settings", get(settings::list))
        // Keys may contain slashes, so the tail is captured whole.
        .route(
            "/api/settings/{*key}",
            get(settings::get)
                .put(settings::set)
                .delete(settings::delete),
        )
        // ── Pub/sub ─────────────────────────────────────────────────
        .route("/api/topics", get(topics::list))
        .route("/api/topics/{topic}", get(topics::detail))
        .route(
            "/api/topics/{topic}/subscriptions/{name}",
            delete(topics::unsubscribe),
        )
        .route(
            "/api/topics/{topic}/subscriptions/{name}/pause",
            post(topics::pause),
        )
        .route(
            "/api/topics/{topic}/subscriptions/{name}/resume",
            post(topics::resume),
        )
        // ── Tasks, queues, and their overrides ──────────────────────
        .route("/api/tasks", get(overrides::list_tasks))
        .route("/api/queues", get(overrides::list_queues))
        .route(
            "/api/tasks/{task_name}/override",
            get(overrides::get_task)
                .put(overrides::put_task)
                .delete(overrides::delete_task),
        )
        .route(
            "/api/queues/{queue_name}/override",
            get(overrides::get_queue)
                .put(overrides::put_queue)
                .delete(overrides::delete_queue),
        )
        // ── Middleware toggles ──────────────────────────────────────
        .route("/api/middleware", get(middleware::list))
        .route(
            "/api/tasks/{task_name}/middleware",
            get(middleware::for_task).delete(middleware::clear),
        )
        .route(
            "/api/tasks/{task_name}/middleware/{middleware_name}",
            put(middleware::set),
        )
        // ── Webhooks ────────────────────────────────────────────────
        .route("/api/event-types", get(webhooks::event_types))
        .route("/api/webhooks", get(webhooks::list).post(webhooks::create))
        .route(
            "/api/webhooks/{id}",
            get(webhooks::detail)
                .put(webhooks::update)
                .delete(webhooks::delete),
        )
        .route("/api/webhooks/{id}/test", post(webhooks::test))
        .route(
            "/api/webhooks/{id}/rotate-secret",
            post(webhooks::rotate_secret),
        )
        .route(
            "/api/webhooks/{id}/deliveries",
            get(webhooks::list_deliveries),
        )
        .route(
            "/api/webhooks/{id}/deliveries/{delivery_id}",
            get(webhooks::get_delivery),
        )
        .route(
            "/api/webhooks/{id}/deliveries/{delivery_id}/replay",
            post(webhooks::replay_delivery),
        )
        // ── Workflows ───────────────────────────────────────────────
        .route("/api/workflows/runs", get(workflows::list))
        .route("/api/workflows/runs/{run_id}", get(workflows::detail))
        .route("/api/workflows/runs/{run_id}/dag", get(workflows::dag))
        .route(
            "/api/workflows/runs/{run_id}/children",
            get(workflows::children),
        )
}
