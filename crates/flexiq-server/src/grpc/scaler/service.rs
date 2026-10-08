//! `IsActive`, `GetMetricSpec` and `GetMetrics`: KEDA's three polled calls.

use tonic::Status;

use super::Scoped;
use crate::grpc::blocking::on_storage;
use crate::grpc::pb::externalscaler as pb;
use crate::scaling::{self, Depth};

/// Whether the target should run at all: active while anything is pending or
/// running above the activation depth, so a scale to zero never strands a job
/// mid-flight.
pub async fn is_active(scoped: &Scoped) -> Result<pb::IsActiveResponse, Status> {
    let depth = depth(scoped).await?;
    Ok(pb::IsActiveResponse {
        result: depth.pending + depth.running > scoped.query.activation,
    })
}

/// The one metric this scaled object reads, and the depth the HPA aims for.
/// Needs no storage, but the caller was still checked against the queue.
pub fn metric_spec(scoped: &Scoped) -> pb::GetMetricSpecResponse {
    let query = &scoped.query;
    pb::GetMetricSpecResponse {
        metric_specs: vec![pb::MetricSpec {
            metric_name: query.metric_name(),
            target_size: query.target,
            target_size_float: query.target as f64,
        }],
    }
}

/// The metric's value: pending jobs, the number `/api/scaler` reports.
pub async fn metrics(scoped: &Scoped) -> Result<pb::GetMetricsResponse, Status> {
    let pending = depth(scoped).await?.pending;
    Ok(pb::GetMetricsResponse {
        metric_values: vec![pb::MetricValue {
            metric_name: scoped.query.metric_name(),
            metric_value: pending,
            metric_value_float: pending as f64,
        }],
    })
}

/// Pending and running jobs for the query, in the caller's namespace.
async fn depth(scoped: &Scoped) -> Result<Depth, Status> {
    let namespace = scoped.principal.namespace().to_string();
    let queue = scoped.query.queue.clone();
    on_storage(&scoped.storage, move |storage| {
        scaling::depth(storage, Some(&namespace), queue.as_deref())
    })
    .await
}
