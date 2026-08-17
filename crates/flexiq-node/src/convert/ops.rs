//! JS shapes for operational inspection: circuit breakers, replay history,
//! and the job dependency DAG.

use flexiq_core::storage::records::{CircuitBreakerState, ReplayEntry};
use napi_derive::napi;

use super::JsJob;

/// A task's circuit-breaker state (the dashboard-facing subset).
#[napi(object)]
pub struct JsCircuitBreaker {
    pub task_name: String,
    /// `closed`, `open`, or `half_open`.
    pub state: String,
    pub failure_count: i32,
    pub last_failure_at: Option<i64>,
    pub opened_at: Option<i64>,
    pub threshold: i32,
    pub window_ms: i64,
    pub cooldown_ms: i64,
}

pub fn circuit_breaker_to_js(row: CircuitBreakerState) -> JsCircuitBreaker {
    let state = match row.state {
        1 => "open",
        2 => "half_open",
        _ => "closed",
    };
    JsCircuitBreaker {
        task_name: row.task_name,
        state: state.to_string(),
        failure_count: row.failure_count,
        last_failure_at: row.last_failure_at,
        opened_at: row.opened_at,
        threshold: row.threshold,
        window_ms: row.window_ms,
        cooldown_ms: row.cooldown_ms,
    }
}

/// One replay of a job (result blobs are omitted; ids + errors suffice).
#[napi(object)]
pub struct JsReplayEntry {
    pub id: String,
    pub original_job_id: String,
    pub replay_job_id: String,
    pub replayed_at: i64,
    pub original_error: Option<String>,
    pub replay_error: Option<String>,
}

pub fn replay_to_js(row: ReplayEntry) -> JsReplayEntry {
    JsReplayEntry {
        id: row.id,
        original_job_id: row.original_job_id,
        replay_job_id: row.replay_job_id,
        replayed_at: row.replayed_at,
        original_error: row.original_error,
        replay_error: row.replay_error,
    }
}

/// A `dependency -> dependent` edge in a job DAG.
#[napi(object)]
pub struct JsDagEdge {
    pub from: String,
    pub to: String,
}

/// The dependency DAG reachable from one job.
#[napi(object)]
pub struct JsJobDag {
    pub nodes: Vec<JsJob>,
    pub edges: Vec<JsDagEdge>,
}
