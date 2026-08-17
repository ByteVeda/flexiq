//! Task metrics: per-task aggregates and the time-bucketed series.
//!
//! Aggregation happens here rather than in storage because the SDK dashboards
//! do it in their shells too — same percentiles, same rounding, so the charts
//! read identically whichever server the SPA is talking to.

use axum::extract::State;
use axum::Json;
use flexiq_core::storage::records::TaskMetric;
use flexiq_core::{now_millis, Storage};
use serde_json::{json, Map, Value};

use crate::dashboard::blocking::on_storage;
use crate::dashboard::error::ApiResult;
use crate::dashboard::query::Params;
use crate::dashboard::state::SharedState;

/// `GET /api/metrics` — per-task aggregates over the lookback window.
pub async fn aggregate(State(state): State<SharedState>, params: Params) -> ApiResult<Json<Value>> {
    let rows = fetch(&state, &params).await?;

    let mut by_task: std::collections::HashMap<String, Vec<&TaskMetric>> =
        std::collections::HashMap::new();
    for row in &rows {
        by_task.entry(row.task_name.clone()).or_default().push(row);
    }

    let mut body = Map::new();
    for (task_name, records) in by_task {
        let durations = sorted_durations_ms(&records);
        let succeeded = records.iter().filter(|record| record.succeeded).count();
        body.insert(
            task_name,
            json!({
                "count": durations.len(),
                "success_count": succeeded,
                "failure_count": durations.len() - succeeded,
                "avg_ms": mean(&durations),
                "p50_ms": percentile(&durations, 0.50),
                "p95_ms": percentile(&durations, 0.95),
                "p99_ms": percentile(&durations, 0.99),
                "min_ms": durations.first().map(|first| round2(*first)).unwrap_or(0.0),
                "max_ms": durations.last().map(|last| round2(*last)).unwrap_or(0.0),
            }),
        );
    }
    Ok(Json(Value::Object(body)))
}

/// `GET /api/metrics/timeseries` — the same data bucketed for the charts.
pub async fn timeseries(
    State(state): State<SharedState>,
    params: Params,
) -> ApiResult<Json<Value>> {
    let since_seconds = params.int("since", 3_600)?;
    let bucket_seconds = params.int("bucket", 60)?.max(1);
    let rows = fetch(&state, &params).await?;

    let bucket_ms = bucket_seconds.saturating_mul(1_000);
    let window_start = now_millis().saturating_sub(since_seconds.saturating_mul(1_000));

    // Buckets are anchored to the window start, not to the epoch, so the first
    // bucket always begins where the requested window does.
    let mut buckets: std::collections::BTreeMap<i64, Vec<&TaskMetric>> =
        std::collections::BTreeMap::new();
    for row in &rows {
        let offset = row.recorded_at - window_start;
        let key = offset.div_euclid(bucket_ms) * bucket_ms + window_start;
        buckets.entry(key).or_default().push(row);
    }

    let series: Vec<Value> = buckets
        .into_iter()
        .map(|(timestamp, records)| {
            let durations = sorted_durations_ms(&records);
            let succeeded = records.iter().filter(|record| record.succeeded).count();
            json!({
                "timestamp": timestamp,
                "count": durations.len(),
                "success": succeeded,
                "failure": durations.len() - succeeded,
                "avg_ms": mean(&durations),
                "p50_ms": percentile(&durations, 0.50),
                "p95_ms": percentile(&durations, 0.95),
                "p99_ms": percentile(&durations, 0.99),
            })
        })
        .collect();

    Ok(Json(Value::Array(series)))
}

async fn fetch(state: &SharedState, params: &Params) -> ApiResult<Vec<TaskMetric>> {
    let task_name = params.get("task").map(str::to_string);
    let since_seconds = params.int("since", 3_600)?;
    let since_ms = now_millis().saturating_sub(since_seconds.saturating_mul(1_000));
    let namespace = state.namespace.clone();
    on_storage(state, move |storage| {
        storage.get_metrics(task_name.as_deref(), since_ms, namespace.as_deref())
    })
    .await
}

/// Wall times in milliseconds, ascending — the order both percentile and
/// min/max depend on.
fn sorted_durations_ms(records: &[&TaskMetric]) -> Vec<f64> {
    let mut durations: Vec<f64> = records
        .iter()
        .map(|record| record.wall_time_ns as f64 / 1_000_000.0)
        .collect();
    durations.sort_by(|left, right| left.total_cmp(right));
    durations
}

fn mean(sorted: &[f64]) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    round2(sorted.iter().sum::<f64>() / sorted.len() as f64)
}

/// Nearest-rank percentile, matching the SDK dashboards exactly.
fn percentile(sorted: &[f64], quantile: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let index = ((sorted.len() as f64 * quantile) as usize).min(sorted.len() - 1);
    round2(sorted[index])
}

fn round2(value: f64) -> f64 {
    (value * 100.0).round() / 100.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentiles_use_the_nearest_rank() {
        let sorted = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];
        assert_eq!(percentile(&sorted, 0.50), 6.0);
        assert_eq!(percentile(&sorted, 0.95), 10.0);
        assert_eq!(percentile(&sorted, 0.99), 10.0);
    }

    #[test]
    fn empty_input_reports_zero_rather_than_nan() {
        assert_eq!(percentile(&[], 0.5), 0.0);
        assert_eq!(mean(&[]), 0.0);
    }

    #[test]
    fn averages_round_to_two_decimals() {
        assert_eq!(mean(&[1.0, 2.0]), 1.5);
        assert_eq!(mean(&[1.0, 1.0, 2.0]), 1.33);
    }
}
