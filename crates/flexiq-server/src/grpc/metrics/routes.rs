//! `GET /metrics` on the gRPC listener.
//!
//! Plain HTTP on the gRPC port, the way the JSON facade already is: a
//! Prometheus scraper speaks HTTP/1.1 and has no gRPC client, and a second
//! listener would be a second port to expose, probe and firewall for one route.
//!
//! It is credentialled by the same [`crate::grpc::auth::AuthLayer`] as
//! everything else here, requiring a token of either scope — this is one
//! tenant's state, read through a door that has no anonymous path except the
//! two health RPCs a kubelet probe cannot credential.

use std::sync::Arc;

use axum::extract::State;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use flexiq_core::{Storage, StorageBackend};

use super::registry::RpcMetrics;
use crate::grpc::blocking::on_storage;
use crate::grpc::executor::ExecutorDoor;
use crate::grpc::facade::error;

/// Everything `/metrics` reads.
#[derive(Clone)]
pub struct MetricsState {
    storage: StorageBackend,
    namespace: String,
    /// Absent when this process attaches no executors, which is why the two
    /// executor gauges are then omitted rather than reported as zero.
    door: Option<ExecutorDoor>,
    metrics: Arc<RpcMetrics>,
}

/// The `/metrics` route.
pub fn router(
    storage: StorageBackend,
    namespace: String,
    door: Option<ExecutorDoor>,
    metrics: Arc<RpcMetrics>,
) -> Router {
    Router::new()
        .route(super::METRICS_PATH, get(scrape))
        .with_state(MetricsState {
            storage,
            namespace,
            door,
            metrics,
        })
}

async fn scrape(State(state): State<MetricsState>) -> Response {
    let namespace = state.namespace.clone();
    let per_queue = match on_storage(&state.storage, move |storage| {
        storage.stats_all_queues(Some(&namespace))
    })
    .await
    {
        Ok(per_queue) => per_queue,
        Err(status) => return error::response(&status),
    };
    let workers = match on_storage(&state.storage, |storage| storage.list_workers()).await {
        Ok(workers) => workers,
        Err(status) => return error::response(&status),
    };

    let mut body = crate::metrics::storage_gauges(
        per_queue,
        workers.len(),
        state.door.as_ref().map(ExecutorDoor::capacity),
    );
    body.push_str(&state.metrics.render());

    (
        [("content-type", crate::metrics::EXPOSITION_CONTENT_TYPE)],
        body,
    )
        .into_response()
}
