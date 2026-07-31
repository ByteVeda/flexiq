//! Webhook subscription CRUD, the delivery log, and the event taxonomy.

use axum::extract::{Path, State};
use axum::Json;
use serde_json::{json, Map, Value};

use crate::dashboard::blocking::on_storage;
use crate::dashboard::error::{ApiError, ApiResult};
use crate::dashboard::query::Params;
use crate::dashboard::state::SharedState;
use crate::dashboard::stores::deliveries::{self, DeliveryFilter, DeliveryRecord, DeliveryStatus};
use crate::dashboard::stores::url_safety::validate_webhook_url;
use crate::dashboard::stores::webhooks::{self, WebhookSubscription};
use crate::dashboard::webhook_sender;

/// Largest delivery page the API will return.
const MAX_PAGE_SIZE: i64 = 200;

/// The cross-SDK event taxonomy. A subscription must stay portable between
/// SDKs, so this list is a contract, not this server's own vocabulary.
const EVENT_TYPES: [&str; 29] = [
    "job.enqueued",
    "job.completed",
    "job.failed",
    "job.retrying",
    "job.dead",
    "job.cancelled",
    "worker.started",
    "worker.stopped",
    "worker.online",
    "worker.offline",
    "worker.unhealthy",
    "queue.paused",
    "queue.resumed",
    "workflow.submitted",
    "workflow.completed",
    "workflow.completed_with_failures",
    "workflow.failed",
    "workflow.cancelled",
    "workflow.gate_reached",
    "workflow.compensating",
    "workflow.compensated",
    "workflow.compensation_failed",
    "workflow.node_compensating",
    "workflow.node_compensated",
    "workflow.node_compensation_failed",
    "predicate.deferred",
    "predicate.cancelled",
    "predicate.rejected",
    "predicate.skipped",
];

/// `GET /api/event-types` — the subscribable events, sorted.
pub async fn event_types() -> Json<Value> {
    let mut types = EVENT_TYPES;
    types.sort_unstable();
    Json(json!(types))
}

/// `GET /api/webhooks`.
pub async fn list(State(state): State<SharedState>) -> ApiResult<Json<Value>> {
    let subscriptions = on_storage(&state, webhooks::list_all).await?;
    Ok(Json(Value::Array(
        subscriptions
            .iter()
            .map(|subscription| subscription.to_api_json(false))
            .collect(),
    )))
}

/// `GET /api/webhooks/{id}`.
pub async fn detail(
    State(state): State<SharedState>,
    Path(id): Path<String>,
) -> ApiResult<Json<Value>> {
    let subscription = require(&state, id).await?;
    Ok(Json(subscription.to_api_json(false)))
}

/// `POST /api/webhooks` — create one, revealing the secret exactly once.
pub async fn create(
    State(state): State<SharedState>,
    Json(body): Json<Value>,
) -> ApiResult<Json<Value>> {
    let body = object(&body)?;
    let url = required_string(body, "url")?;
    validate_webhook_url(&url).map_err(|error| ApiError::BadRequest(error.to_string()))?;

    let mut subscription = WebhookSubscription::new(url);
    apply_patch(&mut subscription, body)?;
    if body
        .get("generate_secret")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        subscription.secret = Some(webhooks::generate_secret());
    }

    let stored = subscription.clone();
    on_storage(&state, move |storage| webhooks::create(storage, &stored)).await?;
    Ok(Json(subscription.to_api_json(true)))
}

/// `PUT /api/webhooks/{id}` — patch the fields present in the body.
pub async fn update(
    State(state): State<SharedState>,
    Path(id): Path<String>,
    Json(body): Json<Value>,
) -> ApiResult<Json<Value>> {
    let patch = object(&body)?;
    let mut subscription = require(&state, id).await?;
    if let Some(url) = patch.get("url") {
        let url = url
            .as_str()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| ApiError::BadRequest("missing or empty field 'url'".into()))?;
        validate_webhook_url(url).map_err(|error| ApiError::BadRequest(error.to_string()))?;
        subscription.url = url.to_string();
    }
    apply_patch(&mut subscription, patch)?;

    let updated = on_storage(&state, move |storage| {
        webhooks::replace(storage, subscription)
    })
    .await?
    .ok_or_else(|| ApiError::NotFound("webhook not found".into()))?;
    Ok(Json(updated.to_api_json(false)))
}

/// `DELETE /api/webhooks/{id}` — drops the subscription and its delivery log.
pub async fn delete(
    State(state): State<SharedState>,
    Path(id): Path<String>,
) -> ApiResult<Json<Value>> {
    let removed = on_storage(&state, move |storage| {
        let removed = webhooks::delete(storage, &id)?;
        if removed {
            // Otherwise the log outlives its subscription and nothing can ever
            // read or delete it again.
            deliveries::delete_for(storage, &id)?;
        }
        Ok(removed)
    })
    .await?;
    if !removed {
        return Err(ApiError::NotFound("webhook not found".into()));
    }
    Ok(Json(json!({ "deleted": true })))
}

/// `POST /api/webhooks/{id}/rotate-secret` — new secret, returned once.
pub async fn rotate_secret(
    State(state): State<SharedState>,
    Path(id): Path<String>,
) -> ApiResult<Json<Value>> {
    let mut subscription = require(&state, id.clone()).await?;
    let secret = webhooks::generate_secret();
    subscription.secret = Some(secret.clone());
    on_storage(&state, move |storage| {
        webhooks::replace(storage, subscription)
    })
    .await?
    .ok_or_else(|| ApiError::NotFound("webhook not found".into()))?;
    Ok(Json(json!({ "id": id, "secret": secret })))
}

/// `POST /api/webhooks/{id}/test` — deliver a synthetic event, inline.
pub async fn test(
    State(state): State<SharedState>,
    Path(id): Path<String>,
) -> ApiResult<Json<Value>> {
    let subscription = require(&state, id.clone()).await?;
    let payload = json!({
        "event": "test.ping",
        "task_name": Value::Null,
        "subscription_id": id,
        "message": "synthetic test event from dashboard",
    });

    let outcome = webhook_sender::deliver(&subscription, &payload).await;
    Ok(Json(json!({
        "status": outcome.status,
        "delivered": outcome.delivered(),
    })))
}

/// `GET /api/webhooks/{id}/deliveries` — recent attempts, newest first.
pub async fn list_deliveries(
    State(state): State<SharedState>,
    Path(id): Path<String>,
    params: Params,
) -> ApiResult<Json<Value>> {
    require(&state, id.clone()).await?;

    let status = match params.get("status") {
        None => None,
        Some(raw) => Some(DeliveryStatus::parse(raw).ok_or_else(|| {
            ApiError::BadRequest("status must be one of: pending, delivered, failed, dead".into())
        })?),
    };
    let limit = params.int("limit", 50)?.clamp(1, MAX_PAGE_SIZE);
    let offset = params.int("offset", 0)?;
    let filter = DeliveryFilter {
        status,
        event: params.get("event").map(str::to_string),
        limit: limit as usize,
        offset: offset as usize,
    };

    let (items, total) = on_storage(&state, move |storage| {
        Ok((
            deliveries::list_for(storage, &id, &filter)?,
            deliveries::count_for(storage, &id)?,
        ))
    })
    .await?;

    Ok(Json(json!({
        "items": items,
        "limit": limit,
        "offset": offset,
        "total": total,
    })))
}

/// `GET /api/webhooks/{id}/deliveries/{delivery_id}`.
pub async fn get_delivery(
    State(state): State<SharedState>,
    Path((id, delivery_id)): Path<(String, String)>,
) -> ApiResult<Json<Value>> {
    let record = on_storage(&state, move |storage| {
        deliveries::get(storage, &id, &delivery_id)
    })
    .await?
    .ok_or_else(|| ApiError::NotFound("delivery not found".into()))?;
    Ok(Json(serde_json::to_value(record).unwrap_or(Value::Null)))
}

/// `POST /api/webhooks/{id}/deliveries/{delivery_id}/replay`.
///
/// The replay is recorded as a **new** attempt rather than overwriting the
/// original, so the audit trail keeps both.
pub async fn replay_delivery(
    State(state): State<SharedState>,
    Path((id, delivery_id)): Path<(String, String)>,
) -> ApiResult<Json<Value>> {
    let subscription = require(&state, id.clone()).await?;
    let lookup_id = id.clone();
    let record = on_storage(&state, move |storage| {
        deliveries::get(storage, &lookup_id, &delivery_id)
    })
    .await?
    .ok_or_else(|| ApiError::NotFound("delivery not found".into()))?;

    let mut payload = record.payload.clone();
    payload.insert("replay_of".into(), json!(record.id));
    let outcome = webhook_sender::deliver(&subscription, &Value::Object(payload.clone())).await;

    let (delivered, status) = (outcome.delivered(), outcome.status);
    let attempt = DeliveryRecord {
        id: String::new(),
        subscription_id: id,
        event: payload
            .get("event")
            .and_then(Value::as_str)
            .unwrap_or(&record.event)
            .to_string(),
        payload,
        task_name: record.task_name.clone(),
        job_id: record.job_id.clone(),
        status: if delivered {
            DeliveryStatus::Delivered
        } else {
            DeliveryStatus::Failed
        },
        attempts: 1,
        response_code: outcome.status,
        response_body: outcome.body,
        latency_ms: Some(outcome.latency_ms),
        error: outcome.error,
        created_at: 0,
        completed_at: None,
    };
    on_storage(&state, move |storage| {
        deliveries::record_attempt(storage, attempt)
    })
    .await?;

    Ok(Json(json!({
        "replayed_of": record.id,
        "status": status,
        "delivered": delivered,
    })))
}

/// Load a subscription or 404.
async fn require(state: &SharedState, id: String) -> ApiResult<WebhookSubscription> {
    on_storage(state, move |storage| webhooks::get(storage, &id))
        .await?
        .ok_or_else(|| ApiError::NotFound("webhook not found".into()))
}

fn object(body: &Value) -> ApiResult<&Map<String, Value>> {
    body.as_object()
        .ok_or_else(|| ApiError::BadRequest("body must be a JSON object".into()))
}

fn required_string(body: &Map<String, Value>, field: &str) -> ApiResult<String> {
    body.get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .ok_or_else(|| ApiError::BadRequest(format!("missing or empty field '{field}'")))
}

/// Apply the optional fields of a create or update body.
fn apply_patch(subscription: &mut WebhookSubscription, body: &Map<String, Value>) -> ApiResult<()> {
    if let Some(events) = body.get("events") {
        subscription.events = coerce_events(events)?;
    }
    if let Some(filter) = body.get("task_filter") {
        subscription.task_filter = coerce_task_filter(filter)?;
    }
    if let Some(headers) = body.get("headers") {
        subscription.headers = coerce_headers(headers)?;
    }
    if let Some(retries) = body.get("max_retries") {
        subscription.max_retries = non_negative_int(retries, "max_retries")?;
    }
    if let Some(timeout) = body.get("timeout_seconds") {
        subscription.timeout_seconds = positive_number(timeout, "timeout_seconds")?;
    }
    if let Some(backoff) = body.get("retry_backoff") {
        subscription.retry_backoff = positive_number(backoff, "retry_backoff")?;
    }
    if let Some(enabled) = body.get("enabled") {
        subscription.enabled = enabled
            .as_bool()
            .ok_or_else(|| ApiError::BadRequest("enabled must be a boolean".into()))?;
    }
    if let Some(description) = body.get("description") {
        subscription.description = match description {
            Value::Null => None,
            Value::String(text) => Some(text.clone()),
            _ => {
                return Err(ApiError::BadRequest(
                    "description must be a string or null".into(),
                ))
            }
        };
    }
    if let Some(secret) = body.get("secret") {
        subscription.secret = match secret {
            Value::Null => None,
            Value::String(text) => Some(text.clone()),
            _ => {
                return Err(ApiError::BadRequest(
                    "secret must be a string or null".into(),
                ))
            }
        };
    }
    Ok(())
}

fn coerce_events(value: &Value) -> ApiResult<Vec<String>> {
    let Value::Array(entries) = value else {
        if value.is_null() {
            return Ok(Vec::new());
        }
        return Err(ApiError::BadRequest(
            "events must be a list of event type strings".into(),
        ));
    };
    entries
        .iter()
        .map(|entry| {
            let name = entry
                .as_str()
                .ok_or_else(|| ApiError::BadRequest("events must contain only strings".into()))?;
            // An unknown event would silently never fire, which reads as a
            // broken webhook rather than a typo.
            if !EVENT_TYPES.contains(&name) {
                return Err(ApiError::BadRequest(format!("unknown event type '{name}'")));
            }
            Ok(name.to_string())
        })
        .collect()
}

fn coerce_task_filter(value: &Value) -> ApiResult<Option<Vec<String>>> {
    match value {
        Value::Null => Ok(None),
        Value::Array(entries) => entries
            .iter()
            .map(|entry| {
                entry
                    .as_str()
                    .filter(|name| !name.is_empty())
                    .map(str::to_string)
                    .ok_or_else(|| {
                        ApiError::BadRequest("task_filter entries must be non-empty strings".into())
                    })
            })
            .collect::<ApiResult<Vec<String>>>()
            .map(Some),
        _ => Err(ApiError::BadRequest(
            "task_filter must be a list of task names or null".into(),
        )),
    }
}

fn coerce_headers(value: &Value) -> ApiResult<std::collections::BTreeMap<String, String>> {
    match value {
        Value::Null => Ok(std::collections::BTreeMap::new()),
        Value::Object(entries) => entries
            .iter()
            .map(|(name, value)| {
                value
                    .as_str()
                    .map(|text| (name.clone(), text.to_string()))
                    .ok_or_else(|| {
                        ApiError::BadRequest("headers must map strings to strings".into())
                    })
            })
            .collect(),
        _ => Err(ApiError::BadRequest(
            "headers must be an object of string→string".into(),
        )),
    }
}

fn non_negative_int(value: &Value, field: &str) -> ApiResult<i64> {
    value
        .as_i64()
        .filter(|number| *number >= 0)
        .ok_or_else(|| ApiError::BadRequest(format!("{field} must be a non-negative integer")))
}

fn positive_number(value: &Value, field: &str) -> ApiResult<f64> {
    value
        .as_f64()
        .filter(|number| *number > 0.0 && !value.is_boolean())
        .ok_or_else(|| ApiError::BadRequest(format!("{field} must be a positive number")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_taxonomy_has_no_duplicates() {
        let mut sorted = EVENT_TYPES;
        sorted.sort_unstable();
        let unique: std::collections::BTreeSet<&str> = EVENT_TYPES.into_iter().collect();
        assert_eq!(unique.len(), EVENT_TYPES.len());
    }

    #[test]
    fn unknown_events_are_rejected() {
        assert!(coerce_events(&json!(["job.completed"])).is_ok());
        assert!(coerce_events(&json!(["job.exploded"])).is_err());
        assert!(coerce_events(&json!([1])).is_err());
        assert_eq!(
            coerce_events(&Value::Null).expect("null clears"),
            Vec::<String>::new()
        );
    }

    #[test]
    fn a_null_task_filter_means_all_tasks() {
        assert_eq!(coerce_task_filter(&Value::Null).expect("valid"), None);
        assert_eq!(
            coerce_task_filter(&json!(["send_email"])).expect("valid"),
            Some(vec!["send_email".to_string()])
        );
        assert!(coerce_task_filter(&json!([""])).is_err());
        assert!(coerce_task_filter(&json!("send_email")).is_err());
    }

    #[test]
    fn numeric_fields_reject_out_of_range_values() {
        assert!(non_negative_int(&json!(-1), "max_retries").is_err());
        assert!(non_negative_int(&json!(true), "max_retries").is_err());
        assert!(positive_number(&json!(0), "timeout_seconds").is_err());
        assert_eq!(
            positive_number(&json!(2.5), "timeout_seconds").expect("valid"),
            2.5
        );
    }

    #[test]
    fn a_patch_only_touches_the_fields_it_carries() {
        let mut subscription = WebhookSubscription::new("https://example.com/hook".into());
        subscription.max_retries = 9;
        let body = json!({ "enabled": false });
        apply_patch(&mut subscription, body.as_object().expect("object")).expect("valid patch");
        assert!(!subscription.enabled);
        assert_eq!(subscription.max_retries, 9);
    }
}
