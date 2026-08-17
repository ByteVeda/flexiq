//! Worker-resource health, and the two views this process cannot produce.
//!
//! Resource health is reconstructed from what each worker publishes on its
//! heartbeat, which is exactly what an SDK dashboard does when it runs outside
//! the worker process. Proxy and interception metrics are in-process counters
//! of a language runtime; a scheduler that runs no user code has none, so it
//! reports empty rather than pretending.

use axum::extract::State;
use axum::Json;
use flexiq_core::Storage;
use serde_json::{json, Value};

use crate::dashboard::blocking::on_storage;
use crate::dashboard::error::ApiResult;
use crate::dashboard::state::SharedState;

/// `GET /api/resources` — per-resource health across live workers.
pub async fn status(State(state): State<SharedState>) -> ApiResult<Json<Value>> {
    let workers = on_storage(&state, |storage| storage.list_workers()).await?;

    // resource name → the health strings each worker reported for it.
    let mut observed: std::collections::BTreeMap<String, Vec<String>> =
        std::collections::BTreeMap::new();
    for worker in &workers {
        let Some(raw) = worker.resource_health.as_deref() else {
            continue;
        };
        let Ok(Value::Object(report)) = serde_json::from_str::<Value>(raw) else {
            continue;
        };
        for (name, health) in report {
            let health = health.as_str().unwrap_or_default().to_ascii_lowercase();
            observed.entry(name).or_default().push(health);
        }
    }

    let rows: Vec<Value> = observed
        .into_iter()
        .map(|(name, healths)| {
            json!({
                "name": name,
                // No definitions are registered in this process — only workers
                // know a resource's scope and dependencies.
                "scope": "unknown",
                "health": fold_health(&healths),
                "init_duration_ms": 0,
                "recreations": 0,
                "depends_on": Vec::<String>::new(),
            })
        })
        .collect();
    Ok(Json(Value::Array(rows)))
}

/// `GET /api/proxy-stats` — always empty here; see the module docs.
pub async fn proxy_stats() -> Json<Value> {
    Json(json!([]))
}

/// `GET /api/interception-stats` — always empty here; see the module docs.
pub async fn interception_stats() -> Json<Value> {
    Json(json!({}))
}

/// Any unhealthy wins; a mix of healthy and anything else is degraded.
fn fold_health(healths: &[String]) -> &'static str {
    if healths.is_empty() {
        "not_initialized"
    } else if healths.iter().any(|health| health == "unhealthy") {
        "unhealthy"
    } else if healths.iter().all(|health| health == "healthy") {
        "healthy"
    } else {
        "degraded"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn healths(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    #[test]
    fn health_folding_matches_the_sdk_rule() {
        assert_eq!(fold_health(&healths(&[])), "not_initialized");
        assert_eq!(fold_health(&healths(&["healthy", "healthy"])), "healthy");
        assert_eq!(
            fold_health(&healths(&["healthy", "unhealthy"])),
            "unhealthy"
        );
        assert_eq!(fold_health(&healths(&["healthy", "starting"])), "degraded");
    }
}
