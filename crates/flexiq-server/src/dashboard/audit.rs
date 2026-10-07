//! Recording what the dashboard changes (#994).
//!
//! The gate is the one place that knows who is asking, whether they were let
//! in and — once the route ran — how it ended, so it records here rather than
//! in each handler. Which requests it records is a rule, not a list: every
//! `POST`/`PUT`/`PATCH`/`DELETE` under `/api/` that matched a route and was
//! made by a caller worth naming. A state-changing route added later is
//! recorded without anyone editing this file, the same default the gRPC door
//! derives from its gate table.
//!
//! - **Who.** A live session's user, or [`Actor::anonymous`] when auth is
//!   off. A request with no session — login, setup, an expired cookie — is
//!   not recorded: there is no one to name, as on the gRPC door.
//! - **Refusals.** Opened before the CSRF and role checks, so a viewer's
//!   refused write is recorded `PERMISSION_DENIED`, attributed.
//! - **Operation.** `dashboard <METHOD> <route template>` — the template, not
//!   the path, so the operation set stays bounded.
//! - **Targets.** The route's path parameters, kinded by [`target_kind`]; a
//!   handler adds what only it learns — a minted token's id — through
//!   [`Targets`]. One record per target.
//!
//! Records go through the same off-path [`AuditSink`] as the gRPC door's: an
//! action is never refused or slowed because the audit table is.

use std::sync::{Arc, Mutex, PoisonError};

use axum::extract::{FromRequestParts, MatchedPath, RawPathParams, Request};
use axum::http::{Method, StatusCode};
use flexiq_core::scheduler::retention::DEFAULT_NAMESPACE;

use crate::audit::record::{self, outcome_of};
use crate::audit::{Actor, AuditSink, TargetKind};
use crate::dashboard::auth::gate;
use crate::dashboard::state::SharedState;

/// What a request acted on, shared between the gate and a handler. Cloning
/// shares it.
#[derive(Debug, Clone, Default)]
pub struct Targets(Arc<Mutex<Vec<(String, String)>>>);

impl Targets {
    /// The slot a request carries — only one the gate is recording.
    pub fn of(extensions: &axum::http::Extensions) -> Option<Self> {
        extensions.get::<Self>().cloned()
    }

    /// Record one more thing the request acted on.
    pub fn add(&self, kind: TargetKind, id: impl Into<String>) {
        self.push(kind.as_str().to_string(), id.into());
    }

    fn push(&self, kind: String, id: String) {
        self.lock().push((kind, id));
    }

    fn take(&self) -> Vec<(String, String)> {
        std::mem::take(&mut *self.lock())
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Vec<(String, String)>> {
        // A handler that panicked mid-push still acted on what it pushed.
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// A recorded request whose records are still owed. Settled with the
/// answer's status; dropped unsettled — a client that hung up mid-route — it
/// records `CANCELLED`, because the route may already have committed.
pub struct Pending {
    call: Option<Call>,
}

struct Call {
    sink: AuditSink,
    namespace: String,
    actor: Actor,
    operation: String,
    targets: Targets,
}

impl Pending {
    /// Record the request as answered with `status`.
    pub fn settle(mut self, status: StatusCode) {
        self.finish(outcome_of(status));
    }

    fn finish(&mut self, outcome: &str) {
        if let Some(call) = self.call.take() {
            for record in record::records(
                &call.namespace,
                &call.actor,
                &call.operation,
                call.targets.take(),
                outcome,
            ) {
                call.sink.record(record);
            }
        }
    }
}

impl Drop for Pending {
    fn drop(&mut self) {
        self.finish("CANCELLED");
    }
}

/// Start recording `request` as `actor`'s, if it is one the dashboard
/// records. Hands the request back either way, carrying a [`Targets`] slot
/// when it is recorded.
pub async fn open(
    state: &SharedState,
    request: Request,
    actor: Actor,
) -> (Request, Option<Pending>) {
    if !recorded(request.method(), request.uri().path()) {
        return (request, None);
    }
    let (mut parts, body) = request.into_parts();
    // No matched route — the SPA fallback's JSON 404 — changed nothing.
    let Some(template) = parts
        .extensions
        .get::<MatchedPath>()
        .map(|path| path.as_str().to_string())
    else {
        return (Request::from_parts(parts, body), None);
    };

    let targets = Targets::default();
    // A parameter that is not valid UTF-8 fails the route's own extractor
    // too; the request is still recorded, just without the targets.
    if let Ok(params) = RawPathParams::from_request_parts(&mut parts, state).await {
        for (name, value) in &params {
            targets.push(target_kind(&template, name), value.to_string());
        }
    }
    parts.extensions.insert(targets.clone());

    let pending = Pending {
        call: Some(Call {
            sink: state.audit.clone(),
            namespace: state
                .namespace
                .clone()
                .unwrap_or_else(|| DEFAULT_NAMESPACE.to_string()),
            actor,
            operation: format!("dashboard {} {template}", parts.method),
            targets,
        }),
    };
    (Request::from_parts(parts, body), Some(pending))
}

/// Whether a request is one the trail records: it changes state, through
/// the API.
pub fn recorded(method: &Method, path: &str) -> bool {
    path.starts_with("/api/") && gate::is_state_changing(method.as_str())
}

/// The `target_kind` a path parameter of `template` stands for. Keyed by the
/// parameter's name, and by the resource for the generic ones (`{id}`,
/// `{name}`). One this table does not know is recorded under its own name —
/// a vaguer kind, never a missing target.
pub fn target_kind(template: &str, param: &str) -> String {
    let resource = template
        .strip_prefix("/api/")
        .and_then(|rest| rest.split('/').next())
        .unwrap_or_default();
    let kind = match (param, resource) {
        ("job_id", _) => TargetKind::Job,
        ("dead_id", _) => TargetKind::DeadLetter,
        ("queue" | "queue_name", _) => TargetKind::Queue,
        ("task_name", _) => TargetKind::Task,
        ("run_id", _) => TargetKind::WorkflowRun,
        ("middleware_name", _) => TargetKind::Middleware,
        ("topic", _) => TargetKind::Topic,
        ("name", "topics") => TargetKind::Subscription,
        ("key", "settings") => TargetKind::Setting,
        ("id", "grpc-tokens") => TargetKind::Token,
        ("id", "webhooks") => TargetKind::Webhook,
        ("delivery_id", "webhooks") => TargetKind::WebhookDelivery,
        _ => return param.to_string(),
    };
    kind.as_str().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every state-changing route the dashboard serves today, as registered in
    /// `routes::router` and `auth::router`.
    const MUTATING_ROUTES: [&str; 26] = [
        "/api/jobs/{job_id}/cancel",
        "/api/jobs/{job_id}/replay",
        "/api/dead-letters/purge",
        "/api/dead-letters/{dead_id}",
        "/api/dead-letters/{dead_id}/retry",
        "/api/queues/{queue}/pause",
        "/api/queues/{queue}/resume",
        "/api/settings/{*key}",
        "/api/topics/{topic}/subscriptions/{name}",
        "/api/topics/{topic}/subscriptions/{name}/pause",
        "/api/topics/{topic}/subscriptions/{name}/resume",
        "/api/tasks/{task_name}/override",
        "/api/queues/{queue_name}/override",
        "/api/tasks/{task_name}/middleware",
        "/api/tasks/{task_name}/middleware/{middleware_name}",
        "/api/grpc-tokens",
        "/api/grpc-tokens/{id}",
        "/api/webhooks",
        "/api/webhooks/{id}",
        "/api/webhooks/{id}/test",
        "/api/webhooks/{id}/rotate-secret",
        "/api/webhooks/{id}/deliveries/{delivery_id}/replay",
        "/api/auth/logout",
        "/api/auth/change-password",
        "/api/auth/login",
        "/api/auth/setup",
    ];

    fn params(template: &str) -> Vec<&str> {
        template
            .split('/')
            .filter_map(|segment| segment.strip_prefix('{')?.strip_suffix('}'))
            .map(|name| name.trim_start_matches('*'))
            .collect()
    }

    /// A new parameter name on a mutating route must be given a kind here,
    /// or its records would carry the bare parameter name.
    #[test]
    fn every_mutating_route_parameter_has_a_known_kind() {
        let known = [
            TargetKind::Job,
            TargetKind::DeadLetter,
            TargetKind::Queue,
            TargetKind::Task,
            TargetKind::Middleware,
            TargetKind::Topic,
            TargetKind::Subscription,
            TargetKind::Setting,
            TargetKind::Token,
            TargetKind::Webhook,
            TargetKind::WebhookDelivery,
        ]
        .map(TargetKind::as_str);
        for template in MUTATING_ROUTES {
            for param in params(template) {
                let kind = target_kind(template, param);
                assert!(
                    known.contains(&kind.as_str()),
                    "{template}: `{param}` has no kind (read as `{kind}`)"
                );
            }
        }
    }

    #[test]
    fn generic_parameters_are_kinded_by_their_resource() {
        assert_eq!(target_kind("/api/grpc-tokens/{id}", "id"), "token");
        assert_eq!(target_kind("/api/webhooks/{id}", "id"), "webhook");
        assert_eq!(
            target_kind("/api/topics/{topic}/subscriptions/{name}", "name"),
            "subscription"
        );
        assert_eq!(target_kind("/api/settings/{*key}", "key"), "setting");
        assert_eq!(target_kind("/api/things/{id}", "id"), "id", "unknown");
    }

    #[test]
    fn only_api_mutations_are_recorded() {
        for method in [Method::POST, Method::PUT, Method::PATCH, Method::DELETE] {
            assert!(recorded(&method, "/api/queues/emails/pause"), "{method}");
        }
        assert!(!recorded(&Method::GET, "/api/jobs"));
        assert!(!recorded(&Method::HEAD, "/api/jobs"));
        assert!(!recorded(&Method::POST, "/health"));
        assert!(!recorded(&Method::POST, "/assets/x.js"));
    }
}
