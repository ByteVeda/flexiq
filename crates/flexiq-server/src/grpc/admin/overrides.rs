//! The override RPCs, over the same settings documents the dashboards edit.
//!
//! The documents keep the dashboards' JSON shape — `timeout` in whole seconds,
//! `retry_backoff` in fractional ones — because the workers that read them at
//! startup read that shape. The conversion to and from the wire's `Duration`
//! is here, and it refuses what the stored shape cannot hold rather than
//! rounding it.

use std::collections::HashMap;

use flexiq_core::RateLimitConfig;
use prost_types::Duration as ProtoDuration;
use serde_json::{json, Map, Value};
use tonic::{Response, Status};

use super::{require, Scoped};
use crate::dashboard::error::ApiError;
use crate::dashboard::stores::overrides::{self, Scope};
use crate::grpc::blocking::{on_storage, run};
use crate::grpc::pb::admin as pb;
use crate::grpc::producer::convert::timestamp;
use crate::grpc::status::{self as status, WireError};

/// Stored fields the admin door does not own and a replace must keep: a queue
/// override's `paused` mirrors the live pause, which `PauseQueue` owns.
const QUEUE_KEEPS: [&str; 1] = ["paused"];

const NANOS_PER_SEC: f64 = 1_000_000_000.0;

/// Every task and queue override in the namespace.
pub(crate) async fn list(scoped: &Scoped) -> Result<Response<pb::ListOverridesResponse>, Status> {
    let namespace = scoped.namespace_owned();
    let (tasks, queues) = on_storage(scoped.storage(), move |storage| {
        Ok((
            overrides::list(Scope::Task, storage, Some(&namespace))?,
            overrides::list(Scope::Queue, storage, Some(&namespace))?,
        ))
    })
    .await?;

    Ok(Response::new(pb::ListOverridesResponse {
        tasks: tasks
            .into_iter()
            .map(|(name, stored)| (name, task_from_stored(&stored)))
            .collect::<HashMap<_, _>>(),
        queues: queues
            .into_iter()
            .map(|(name, stored)| (name, queue_from_stored(&stored)))
            .collect::<HashMap<_, _>>(),
    }))
}

/// Replace one task's override.
pub(crate) async fn set_task(
    scoped: &Scoped,
    request: pb::SetTaskOverrideRequest,
) -> Result<Response<pb::SetTaskOverrideResponse>, Status> {
    let name = require("task_name", request.task_name)?;
    let fields = task_to_stored(request.task_override.unwrap_or_default())?;
    let stored = replace(scoped, Scope::Task, name, fields, &[]).await?;
    Ok(Response::new(pb::SetTaskOverrideResponse {
        task_override: Some(task_from_stored(&stored)),
    }))
}

/// Remove one task's override. Clearing none is not an error: "no override"
/// is the state asked for.
pub(crate) async fn clear_task(
    scoped: &Scoped,
    request: pb::ClearTaskOverrideRequest,
) -> Result<Response<pb::ClearTaskOverrideResponse>, Status> {
    let name = require("task_name", request.task_name)?;
    clear(scoped, Scope::Task, name).await?;
    Ok(Response::new(pb::ClearTaskOverrideResponse {}))
}

/// Replace one queue's override, keeping the stored `paused`.
pub(crate) async fn set_queue(
    scoped: &Scoped,
    request: pb::SetQueueOverrideRequest,
) -> Result<Response<pb::SetQueueOverrideResponse>, Status> {
    let name = require("queue", request.queue)?;
    let fields = queue_to_stored(request.queue_override.unwrap_or_default())?;
    let stored = replace(scoped, Scope::Queue, name, fields, &QUEUE_KEEPS).await?;
    Ok(Response::new(pb::SetQueueOverrideResponse {
        queue_override: Some(queue_from_stored(&stored)),
    }))
}

/// Remove one queue's override.
pub(crate) async fn clear_queue(
    scoped: &Scoped,
    request: pb::ClearQueueOverrideRequest,
) -> Result<Response<pb::ClearQueueOverrideResponse>, Status> {
    let name = require("queue", request.queue)?;
    clear(scoped, Scope::Queue, name).await?;
    Ok(Response::new(pb::ClearQueueOverrideResponse {}))
}

/// Write `fields` as the whole override and answer with what is stored — an
/// empty document when nothing is left.
async fn replace(
    scoped: &Scoped,
    scope: Scope,
    name: String,
    fields: Map<String, Value>,
    keep: &'static [&'static str],
) -> Result<Map<String, Value>, Status> {
    let storage = scoped.storage().clone();
    let namespace = scoped.namespace_owned();
    let stored = run(move || {
        overrides::replace(scope, &storage, Some(&namespace), &name, &fields, keep)
            .map_err(api_error)
    })
    .await?;
    Ok(stored.unwrap_or_default())
}

async fn clear(scoped: &Scoped, scope: Scope, name: String) -> Result<(), Status> {
    let namespace = scoped.namespace_owned();
    on_storage(scoped.storage(), move |storage| {
        overrides::clear(scope, storage, Some(&namespace), &name)
    })
    .await?;
    Ok(())
}

/// The store's refusal on the wire. A validation failure is the caller's; a
/// lost write race is the retryable conflict the core already names.
fn api_error(error: ApiError) -> Status {
    match error {
        ApiError::BadRequest(message) => WireError::invalid_request(message).into(),
        ApiError::Conflict(key) => {
            status::from_queue_error(&flexiq_core::error::QueueError::SettingConflict(key))
        }
        other => {
            log::error!("grpc: override write failed: {other:?}");
            WireError::internal().into()
        }
    }
}

// ── Wire → stored ────────────────────────────────────────────────────

fn task_to_stored(wire: pb::TaskOverride) -> Result<Map<String, Value>, WireError> {
    let mut fields = Map::new();
    if let Some(rate) = wire.rate_limit {
        fields.insert("rate_limit".into(), json!(rate_limit(rate)?));
    }
    if let Some(max) = wire.max_concurrent {
        fields.insert("max_concurrent".into(), json!(max));
    }
    if let Some(max) = wire.max_retries {
        fields.insert("max_retries".into(), json!(max));
    }
    if let Some(backoff) = wire.retry_backoff {
        fields.insert("retry_backoff".into(), json!(backoff_seconds(&backoff)?));
    }
    if let Some(timeout) = wire.timeout {
        fields.insert("timeout".into(), json!(timeout_seconds(&timeout)?));
    }
    if let Some(priority) = wire.priority {
        fields.insert("priority".into(), json!(priority));
    }
    if let Some(paused) = wire.paused {
        fields.insert("paused".into(), json!(paused));
    }
    Ok(fields)
}

fn queue_to_stored(wire: pb::QueueOverride) -> Result<Map<String, Value>, WireError> {
    let mut fields = Map::new();
    if let Some(rate) = wire.rate_limit {
        fields.insert("rate_limit".into(), json!(rate_limit(rate)?));
    }
    if let Some(max) = wire.max_concurrent {
        fields.insert("max_concurrent".into(), json!(max));
    }
    Ok(fields)
}

/// A rate the scheduler can actually build a bucket from — the store's own
/// check only asks for a `/`, which admits a rate that never releases a job.
fn rate_limit(rate: String) -> Result<String, WireError> {
    if RateLimitConfig::parse(&rate).is_none() {
        return Err(WireError::invalid_request(format!(
            "rate_limit '{rate}' is not '<count>/<s|m|h>' with a count of at least one"
        )));
    }
    Ok(rate)
}

/// A backoff as the stored fractional seconds. Never negative.
fn backoff_seconds(value: &ProtoDuration) -> Result<f64, WireError> {
    let seconds = value.seconds as f64 + f64::from(value.nanos) / NANOS_PER_SEC;
    if seconds < 0.0 {
        return Err(WireError::invalid_request(
            "retry_backoff must not be negative",
        ));
    }
    Ok(seconds)
}

/// A timeout as the stored whole seconds: at least one, and no fraction, which
/// the stored integer could only drop.
fn timeout_seconds(value: &ProtoDuration) -> Result<i64, WireError> {
    if value.nanos != 0 || value.seconds < 1 {
        return Err(WireError::invalid_request(
            "timeout must be a whole number of seconds, at least one",
        ));
    }
    Ok(value.seconds)
}

// ── Stored → wire ────────────────────────────────────────────────────

fn task_from_stored(stored: &Map<String, Value>) -> pb::TaskOverride {
    pb::TaskOverride {
        rate_limit: string(stored, "rate_limit"),
        max_concurrent: int32(stored, "max_concurrent"),
        max_retries: int32(stored, "max_retries"),
        retry_backoff: stored
            .get("retry_backoff")
            .and_then(Value::as_f64)
            .map(duration_from_seconds),
        timeout: stored
            .get("timeout")
            .and_then(Value::as_i64)
            .map(|seconds| ProtoDuration { seconds, nanos: 0 }),
        priority: int32(stored, "priority"),
        paused: stored.get("paused").and_then(Value::as_bool),
        update_time: update_time(stored),
    }
}

fn queue_from_stored(stored: &Map<String, Value>) -> pb::QueueOverride {
    pb::QueueOverride {
        rate_limit: string(stored, "rate_limit"),
        max_concurrent: int32(stored, "max_concurrent"),
        update_time: update_time(stored),
    }
}

fn string(stored: &Map<String, Value>, field: &str) -> Option<String> {
    stored.get(field).and_then(Value::as_str).map(str::to_owned)
}

/// An integer field that fits the wire's `int32`. One a dashboard stored
/// beyond that range reads as unset rather than as a wrapped value.
fn int32(stored: &Map<String, Value>, field: &str) -> Option<i32> {
    stored
        .get(field)
        .and_then(Value::as_i64)
        .and_then(|value| i32::try_from(value).ok())
}

fn duration_from_seconds(seconds: f64) -> ProtoDuration {
    let whole = seconds.trunc();
    ProtoDuration {
        seconds: whole as i64,
        nanos: ((seconds - whole) * NANOS_PER_SEC).round() as i32,
    }
}

fn update_time(stored: &Map<String, Value>) -> Option<prost_types::Timestamp> {
    let millis = overrides::updated_at(stored);
    (millis > 0).then(|| timestamp(millis))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_timeout_is_whole_seconds_of_at_least_one() {
        let whole = ProtoDuration {
            seconds: 30,
            nanos: 0,
        };
        assert_eq!(timeout_seconds(&whole).unwrap(), 30);
        for refused in [
            ProtoDuration {
                seconds: 0,
                nanos: 0,
            },
            ProtoDuration {
                seconds: 1,
                nanos: 500_000_000,
            },
            ProtoDuration {
                seconds: -5,
                nanos: 0,
            },
        ] {
            assert!(timeout_seconds(&refused).is_err(), "{refused:?}");
        }
    }

    #[test]
    fn a_backoff_round_trips_through_fractional_seconds() {
        let backoff = ProtoDuration {
            seconds: 1,
            nanos: 500_000_000,
        };
        let stored = backoff_seconds(&backoff).unwrap();
        assert_eq!(stored, 1.5);
        assert_eq!(duration_from_seconds(stored), backoff);
        assert!(backoff_seconds(&ProtoDuration {
            seconds: -1,
            nanos: 0
        })
        .is_err());
    }

    #[test]
    fn a_rate_that_never_releases_a_job_is_refused() {
        for refused in ["100", "0/s", "0.5/m", "10/d", "x/s"] {
            assert!(rate_limit(refused.to_string()).is_err(), "{refused}");
        }
        assert!(rate_limit("100/m".to_string()).is_ok());
    }

    #[test]
    fn a_task_override_round_trips_through_the_stored_shape() {
        let wire = pb::TaskOverride {
            rate_limit: Some("10/s".into()),
            max_concurrent: Some(3),
            max_retries: Some(0),
            retry_backoff: Some(ProtoDuration {
                seconds: 2,
                nanos: 0,
            }),
            timeout: Some(ProtoDuration {
                seconds: 60,
                nanos: 0,
            }),
            priority: Some(-1),
            paused: Some(true),
            update_time: None,
        };
        let stored = task_to_stored(wire.clone()).unwrap();
        assert_eq!(stored["timeout"], json!(60));
        assert_eq!(task_from_stored(&stored), wire);
    }

    #[test]
    fn an_unset_field_is_not_stored() {
        let stored = task_to_stored(pb::TaskOverride::default()).unwrap();
        assert!(stored.is_empty());
    }
}
