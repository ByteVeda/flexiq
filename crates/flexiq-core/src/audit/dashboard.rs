//! Recording what a dashboard changes (#994, #1020) — the cross-SDK rule.
//!
//! Every SDK's dashboard and `flexiq-server`'s answer the same routes from
//! the same SPA build, so the records they leave must not differ either. The
//! shells hand over raw facts — method, path, status, who — and this module
//! turns them into records, so the row shape is decided once:
//!
//! - **Which requests.** A `POST`/`PUT`/`PATCH`/`DELETE` that matched one of
//!   [`ROUTES`]. Anything else — a read, an unrouted path — leaves nothing.
//! - **Operation.** `dashboard <METHOD> <route template>` — the template, not
//!   the path, so the operation set stays bounded.
//! - **Targets.** The template's path values, kinded by [`target_kind`]. One
//!   record per target.
//! - **Outcome.** The HTTP status as a `google.rpc.Code` name
//!   ([`outcome_of`]).

use super::record::{outcome_of, records, Access, Actor};
use super::target::TargetKind;
use crate::storage::records::AuditRecord;

/// Every state-changing dashboard route, as `(method, template)`. The
/// server's router is pinned to this list by a test on its side; an SDK
/// shell serving a route not listed here records nothing for it.
///
/// Literal routes come before the parameterised ones they could shadow, and
/// [`match_route`] takes the first match.
pub const ROUTES: [(&str, &str); 30] = [
    ("POST", "/api/auth/setup"),
    ("POST", "/api/auth/login"),
    ("POST", "/api/auth/logout"),
    ("POST", "/api/auth/change-password"),
    ("POST", "/api/jobs/{job_id}/cancel"),
    ("POST", "/api/jobs/{job_id}/replay"),
    ("POST", "/api/dead-letters/purge"),
    ("DELETE", "/api/dead-letters/{dead_id}"),
    ("POST", "/api/dead-letters/{dead_id}/retry"),
    ("POST", "/api/queues/{queue}/pause"),
    ("POST", "/api/queues/{queue}/resume"),
    ("PUT", "/api/settings/{*key}"),
    ("DELETE", "/api/settings/{*key}"),
    ("DELETE", "/api/topics/{topic}/subscriptions/{name}"),
    ("POST", "/api/topics/{topic}/subscriptions/{name}/pause"),
    ("POST", "/api/topics/{topic}/subscriptions/{name}/resume"),
    ("PUT", "/api/tasks/{task_name}/override"),
    ("DELETE", "/api/tasks/{task_name}/override"),
    ("PUT", "/api/queues/{queue_name}/override"),
    ("DELETE", "/api/queues/{queue_name}/override"),
    ("DELETE", "/api/tasks/{task_name}/middleware"),
    ("PUT", "/api/tasks/{task_name}/middleware/{middleware_name}"),
    ("POST", "/api/grpc-tokens"),
    ("DELETE", "/api/grpc-tokens/{id}"),
    ("POST", "/api/webhooks"),
    ("PUT", "/api/webhooks/{id}"),
    ("DELETE", "/api/webhooks/{id}"),
    ("POST", "/api/webhooks/{id}/test"),
    ("POST", "/api/webhooks/{id}/rotate-secret"),
    ("POST", "/api/webhooks/{id}/deliveries/{delivery_id}/replay"),
];

/// A request matched against [`ROUTES`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteMatch {
    /// The route template, e.g. `/api/jobs/{job_id}/cancel`.
    pub template: &'static str,
    /// Each path parameter's name and raw (still percent-encoded) value.
    pub params: Vec<(&'static str, String)>,
}

/// The state-changing route `method` and `path` hit, if any. `path` is the
/// request path without its query string, as sent — not percent-decoded,
/// matching what the server's router hands its audit gate.
pub fn match_route(method: &str, path: &str) -> Option<RouteMatch> {
    ROUTES
        .iter()
        .filter(|(m, _)| m.eq_ignore_ascii_case(method))
        .find_map(|(_, template)| match_template(template, path))
}

fn match_template(template: &'static str, path: &str) -> Option<RouteMatch> {
    let mut wanted = template.split('/');
    let mut given = path.split('/');
    let mut params = Vec::new();
    loop {
        match (wanted.next(), given.next()) {
            (None, None) => return Some(RouteMatch { template, params }),
            (Some(segment), Some(value)) => {
                let Some(name) = segment.strip_prefix('{').and_then(|s| s.strip_suffix('}')) else {
                    if segment != value {
                        return None;
                    }
                    continue;
                };
                if value.is_empty() {
                    return None;
                }
                if let Some(rest_name) = name.strip_prefix('*') {
                    // A catch-all takes the rest of the path, slashes and all.
                    let rest: Vec<&str> = std::iter::once(value).chain(given).collect();
                    params.push((rest_name, rest.join("/")));
                    return Some(RouteMatch { template, params });
                }
                params.push((name, value.to_string()));
            }
            _ => return None,
        }
    }
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

/// The operation a dashboard record names.
pub fn operation(method: &str, template: &str) -> String {
    format!("dashboard {} {template}", method.to_ascii_uppercase())
}

/// The records one answered dashboard request leaves, or none when it is
/// not a state-changing route. `status` is the HTTP status it was answered
/// with, refusals included.
pub fn action_records(
    namespace: &str,
    actor: &Actor,
    method: &str,
    path: &str,
    status: u16,
) -> Vec<AuditRecord> {
    let Some(route) = match_route(method, path) else {
        return Vec::new();
    };
    let targets = route
        .params
        .iter()
        .map(|(name, value)| (target_kind(route.template, name), value.clone()))
        .collect();
    records(
        namespace,
        actor,
        Access::Write,
        &operation(method, route.template),
        targets,
        outcome_of(status),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_route_matches_by_method_and_shape() {
        let hit = match_route("post", "/api/jobs/j%201/cancel").expect("matched");
        assert_eq!(hit.template, "/api/jobs/{job_id}/cancel");
        assert_eq!(hit.params, [("job_id", "j%201".to_string())]);

        assert_eq!(match_route("GET", "/api/jobs/j1/cancel"), None, "a read");
        assert_eq!(match_route("POST", "/api/jobs/j1"), None, "unrouted");
        assert_eq!(match_route("POST", "/api/jobs//cancel"), None, "empty id");
        assert_eq!(match_route("POST", "/api/jobs/j1/cancel/x"), None);
    }

    #[test]
    fn a_literal_route_wins_over_a_parameter() {
        let purge = match_route("POST", "/api/dead-letters/purge").expect("matched");
        assert_eq!(purge.template, "/api/dead-letters/purge");
        assert!(purge.params.is_empty());
        let delete = match_route("DELETE", "/api/dead-letters/purge").expect("matched");
        assert_eq!(delete.template, "/api/dead-letters/{dead_id}");
    }

    #[test]
    fn a_catch_all_takes_the_rest_of_the_path() {
        let hit = match_route("PUT", "/api/settings/ui/theme").expect("matched");
        assert_eq!(hit.params, [("key", "ui/theme".to_string())]);
        assert_eq!(match_route("PUT", "/api/settings/"), None);
    }

    /// Every template parses: its parameters are all named, and a path built
    /// from it matches back to it — so the list holds no unreachable route.
    #[test]
    fn every_route_matches_itself() {
        for (method, template) in ROUTES {
            let path = template
                .split('/')
                .map(|s| if s.starts_with('{') { "v" } else { s })
                .collect::<Vec<_>>()
                .join("/");
            let hit = match_route(method, &path).expect(template);
            assert_eq!(hit.template, template, "{method} {path}");
        }
    }

    /// A new parameter name must be given a kind, or its records would carry
    /// the bare parameter name.
    #[test]
    fn every_route_parameter_has_a_known_kind() {
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
        for (_, template) in ROUTES {
            for segment in template.split('/') {
                let Some(name) = segment.strip_prefix('{').and_then(|s| s.strip_suffix('}')) else {
                    continue;
                };
                let name = name.trim_start_matches('*');
                let kind = target_kind(template, name);
                assert!(
                    known.contains(&kind.as_str()),
                    "{template}: `{name}` has no kind (read as `{kind}`)"
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
    fn an_action_is_one_record_per_target() {
        let records = action_records(
            "prod",
            &Actor::user("alice"),
            "POST",
            "/api/topics/orders/subscriptions/audit/pause",
            403,
        );
        assert_eq!(records.len(), 2);
        let first = &records[0];
        assert_eq!(
            first.operation,
            "dashboard POST /api/topics/{topic}/subscriptions/{name}/pause"
        );
        assert_eq!(first.principal_kind, "user");
        assert_eq!(first.token_id, "alice");
        assert_eq!(first.outcome, "PERMISSION_DENIED");
        assert_eq!(first.access, "write");
        assert_eq!(
            (first.target_kind.as_deref(), first.target.as_deref()),
            (Some("topic"), Some("orders"))
        );
        assert_eq!(records[1].target_kind.as_deref(), Some("subscription"));

        let purge = action_records(
            "prod",
            &Actor::anonymous(),
            "POST",
            "/api/dead-letters/purge",
            200,
        );
        assert_eq!(purge.len(), 1);
        assert_eq!(purge[0].target_kind, None);
        assert_eq!(purge[0].principal_kind, "anonymous");

        assert!(action_records("prod", &Actor::anonymous(), "GET", "/api/jobs", 200).is_empty());
    }
}
