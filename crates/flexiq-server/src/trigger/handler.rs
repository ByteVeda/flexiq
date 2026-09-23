//! One inbound request, start to finish.
//!
//! The order is the security argument, so it is fixed:
//!
//! 1. **bound the body** — before anything reads it, so a slow or huge upload
//!    costs at most the trigger's own limit;
//! 2. **verify the sender** — on the raw bytes, before any of them are
//!    interpreted;
//! 3. **parse, unwrap and map** — a request that cannot become a job is
//!    refused before it costs a token, and a subscription handshake is
//!    answered without one;
//! 4. **draw from the bucket** — only a verified, well-formed request spends
//!    the sender's budget;
//! 5. **enqueue**.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::extract::State;
use axum::http::{header, HeaderMap, HeaderValue, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::Json;
use flexiq_core::StorageBackend;
use serde_json::{json, Value};

use crate::trigger::auth::{Inbound, KeyFetcher};
use crate::trigger::definition::{Source, Trigger, HEALTH_PATH};
use crate::trigger::document::{self, DocumentError};
use crate::trigger::enqueue::{self, Enqueued, Planned, MAX_KEY_LEN};
use crate::trigger::metrics;
use crate::trigger::object_store::{self, Unwrapped};
use crate::trigger::rate;

/// What the listener serves: the definitions, indexed by path, and where
/// their jobs go.
pub struct Role {
    storage: StorageBackend,
    namespace: String,
    triggers: Arc<[Trigger]>,
    by_path: HashMap<String, usize>,
    keys: KeyFetcher,
}

impl Role {
    /// Serve `triggers` into `namespace` on `storage`, fetching the published
    /// keys a verifier needs through `keys`.
    pub fn new(
        storage: StorageBackend,
        namespace: String,
        triggers: Arc<[Trigger]>,
        keys: KeyFetcher,
    ) -> Self {
        let by_path = triggers
            .iter()
            .enumerate()
            .map(|(index, trigger)| (trigger.path.clone(), index))
            .collect();
        Self {
            storage,
            namespace,
            triggers,
            by_path,
            keys,
        }
    }

    fn lookup(&self, path: &str) -> Option<&Trigger> {
        self.by_path.get(path).map(|index| &self.triggers[*index])
    }
}

/// How one request ended. Each arm is one status, one metrics label.
#[derive(Debug)]
pub enum Outcome {
    /// Every job was stored; some may have been deduplicated.
    Enqueued(Vec<Enqueued>),
    /// A subscription handshake, answered with this body and no job.
    Handshake(Value),
    /// The body exceeded the trigger's limit, or could not be read.
    TooLarge,
    /// The sender could not prove its origin.
    Unauthorized,
    /// The body's content type is not one a trigger reads.
    UnsupportedMediaType(String),
    /// The body does not parse as its declared type.
    Malformed(String),
    /// The body parsed but the mapping could not resolve.
    Unmappable(String),
    /// The trigger's bucket is empty; retry after this many seconds.
    RateLimited(u64),
    /// Storage failed. Logged in full, answered without detail.
    Failed,
}

impl Outcome {
    /// The metrics label.
    pub fn label(&self) -> &'static str {
        match self {
            Self::Enqueued(jobs) if jobs.iter().all(|job| job.deduplicated) => "deduplicated",
            Self::Enqueued(_) => "enqueued",
            Self::Handshake(_) => "handshake",
            Self::TooLarge => "too_large",
            Self::Unauthorized => "unauthorized",
            Self::UnsupportedMediaType(_) => "unsupported_media_type",
            Self::Malformed(_) => "malformed",
            Self::Unmappable(_) => "unmappable",
            Self::RateLimited(_) => "rate_limited",
            Self::Failed => "failed",
        }
    }
}

impl IntoResponse for Outcome {
    fn into_response(self) -> Response {
        match self {
            Self::Enqueued(jobs) => {
                // `202` while anything new was queued; a pure redelivery is a
                // `200`, so a sender's log can tell the two apart.
                let status = if jobs.iter().all(|job| job.deduplicated) {
                    StatusCode::OK
                } else {
                    StatusCode::ACCEPTED
                };
                let jobs: Vec<Value> = jobs
                    .into_iter()
                    .map(|job| json!({"id": job.id, "deduplicated": job.deduplicated}))
                    .collect();
                (status, Json(json!({ "jobs": jobs }))).into_response()
            }
            Self::Handshake(body) => (StatusCode::OK, Json(body)).into_response(),
            Self::TooLarge => error(StatusCode::PAYLOAD_TOO_LARGE, "the body is too large"),
            Self::Unauthorized => error(StatusCode::UNAUTHORIZED, "unauthorized"),
            Self::UnsupportedMediaType(message) => {
                error(StatusCode::UNSUPPORTED_MEDIA_TYPE, &message)
            }
            Self::Malformed(message) => error(StatusCode::BAD_REQUEST, &message),
            Self::Unmappable(message) => error(StatusCode::UNPROCESSABLE_ENTITY, &message),
            Self::RateLimited(seconds) => {
                let mut response = error(StatusCode::TOO_MANY_REQUESTS, "rate limited");
                response
                    .headers_mut()
                    .insert(header::RETRY_AFTER, HeaderValue::from(seconds));
                response
            }
            Self::Failed => error(StatusCode::INTERNAL_SERVER_ERROR, "internal error"),
        }
    }
}

fn error(status: StatusCode, message: &str) -> Response {
    (status, Json(json!({ "error": message }))).into_response()
}

/// Every request the listener receives.
pub async fn receive(
    State(role): State<Arc<Role>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let path = uri.path();
    if path == HEALTH_PATH && method == Method::GET {
        return (StatusCode::OK, "ok").into_response();
    }
    let Some(trigger) = role.lookup(path) else {
        return error(StatusCode::NOT_FOUND, "no trigger answers on this path");
    };
    if method != Method::POST {
        let mut response = error(StatusCode::METHOD_NOT_ALLOWED, "triggers accept POST");
        response
            .headers_mut()
            .insert(header::ALLOW, HeaderValue::from_static("POST"));
        return response;
    }

    let outcome = handle(&role, trigger, uri.query().unwrap_or(""), &headers, body).await;
    metrics::record(&trigger.name, outcome.label());
    match &outcome {
        Outcome::Enqueued(_) | Outcome::RateLimited(_) => {}
        Outcome::Handshake(_) => log::info!(
            "[flexiq] trigger {} answered its subscription handshake",
            trigger.name
        ),
        refused => log::info!(
            "[flexiq] trigger {} refused a request: {}",
            trigger.name,
            refused.label()
        ),
    }
    outcome.into_response()
}

async fn handle(
    role: &Role,
    trigger: &Trigger,
    query: &str,
    headers: &HeaderMap,
    body: Body,
) -> Outcome {
    let Ok(body) = axum::body::to_bytes(body, trigger.max_body_bytes).await else {
        return Outcome::TooLarge;
    };
    let inbound = Inbound {
        headers,
        query,
        body: &body,
        now_secs: now_secs(),
    };

    if let Err(rejection) = trigger.verifier.verify(&inbound, &role.keys).await {
        // The reason is for the operator; the caller learns only `401`.
        log::warn!(
            "[flexiq] trigger {} ({}) rejected a request: {rejection}",
            trigger.name,
            trigger.verifier.kind()
        );
        return Outcome::Unauthorized;
    }

    let document = match document::parse(headers, &body) {
        Ok(document) => document,
        Err(DocumentError::UnsupportedMediaType(message)) => {
            return Outcome::UnsupportedMediaType(message)
        }
        Err(DocumentError::Malformed(message)) => return Outcome::Malformed(message),
    };

    // A handshake is answered here, before the bucket: it creates no job, and
    // a subscription that could not be confirmed under load would never start.
    let events = match events(trigger.source, document) {
        Ok(Unwrapped::Events(events)) => events,
        Ok(Unwrapped::Handshake(body)) => return Outcome::Handshake(body),
        Err(message) => return Outcome::Unmappable(message),
    };
    let planned = match events
        .iter()
        .map(|event| plan(trigger, &inbound, event))
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(planned) => planned,
        Err(message) => return Outcome::Unmappable(message),
    };
    let jobs: Vec<_> = planned
        .into_iter()
        .map(|planned| enqueue::new_job(trigger, &role.namespace, planned))
        .collect();

    let storage = role.storage.clone();
    let key = rate::bucket_key(&role.namespace, &trigger.name);
    let limit = trigger.rate.clone();
    let retry_after = rate::retry_after_secs(&limit);
    let name = trigger.name.clone();
    let stored = tokio::task::spawn_blocking(move || {
        if !rate::acquire(&storage, &key, &limit, jobs.len())? {
            return Ok(None);
        }
        enqueue::submit(&storage, jobs).map(Some)
    })
    .await;

    match stored {
        Ok(Ok(Some(jobs))) => Outcome::Enqueued(jobs),
        Ok(Ok(None)) => Outcome::RateLimited(retry_after),
        Ok(Err(error)) => {
            log::error!("[flexiq] trigger {name} could not enqueue: {error}");
            Outcome::Failed
        }
        Err(error) => {
            log::error!("[flexiq] trigger {name} enqueue task failed: {error}");
            Outcome::Failed
        }
    }
}

/// The documents a request's mapping runs over: the body itself, or one
/// unwrapped event per object.
fn events(source: Source, document: Value) -> Result<Unwrapped, String> {
    match source {
        Source::Http => Ok(Unwrapped::Events(vec![document])),
        Source::ObjectStore(provider) => object_store::unwrap(provider, &document),
    }
}

/// The job one document asks for.
fn plan(trigger: &Trigger, inbound: &Inbound<'_>, document: &Value) -> Result<Planned, String> {
    let payload = trigger
        .mapping
        .payload(inbound, document)
        .map_err(|error| error.0)?;
    let delivery = match &trigger.unique_key {
        None => None,
        Some(selector) => selector
            .resolve_text(inbound, document)
            .map_err(|error| error.0)?,
    };
    if delivery.as_ref().is_some_and(|key| key.len() > MAX_KEY_LEN) {
        return Err(format!(
            "the unique_key value is longer than {MAX_KEY_LEN} bytes"
        ));
    }
    Ok(Planned { payload, delivery })
}

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| {
            i64::try_from(elapsed.as_secs()).unwrap_or(i64::MAX)
        })
}
