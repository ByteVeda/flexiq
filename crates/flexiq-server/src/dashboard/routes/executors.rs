//! Attached-executor inventory.
//!
//! This view exists only where executors attach, so it is additive to the
//! shared dashboard contract: a server with no attach listener reports an empty
//! inventory rather than 404, and the SPA can render it whenever it grows a
//! view for it.

use axum::extract::State;
use axum::Json;
use serde_json::{json, Value};

use crate::dashboard::error::ApiResult;
use crate::dashboard::state::SharedState;

/// `GET /api/executors` — who is attached, what they run, and free capacity.
pub async fn list(State(state): State<SharedState>) -> ApiResult<Json<Value>> {
    let Some(dispatcher) = &state.dispatcher else {
        return Ok(Json(json!({
            "executors": [],
            "capacity": { "executors": 0, "total_slots": 0, "free_slots": 0 },
        })));
    };

    let capacity = dispatcher.capacity();
    let executors: Vec<Value> = dispatcher
        .executors()
        .into_iter()
        .map(|executor| {
            json!({
                "executor_id": executor.executor_id,
                "sdk": executor.sdk,
                "version": executor.version,
                "tasks": executor.tasks,
                "slots": executor.slots,
                "free_slots": executor.free_slots,
                "in_flight": executor.in_flight,
                "peer": executor.peer,
                "idle_ms": executor.idle_ms,
            })
        })
        .collect();

    Ok(Json(json!({
        "executors": executors,
        "capacity": {
            "executors": capacity.executors,
            "total_slots": capacity.total_slots,
            "free_slots": capacity.free_slots,
        },
    })))
}
