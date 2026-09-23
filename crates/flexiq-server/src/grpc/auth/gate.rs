//! What each path requires, as one table.
//!
//! The dashboard keeps the same table for the same reason (`dashboard/auth/
//! gate.rs`): one classification applied in one middleware is what stops a new
//! route from silently landing outside the check. Here the unit is a gRPC path,
//! which is always `/<package>.<Service>/<Method>`, so a whole package is
//! classified by its prefix and a new RPC in an existing package inherits its
//! answer without anyone editing this file.
//!
//! The default is [`Requirement::Authenticated`] rather than
//! [`Requirement::Public`]: an unrecognised path is one the router will answer
//! `UNIMPLEMENTED` to, and answering that without a credential tells an
//! anonymous caller which services a build carries.

use super::principal::Scope;

/// The health RPCs, named exactly rather than by prefix.
///
/// `grpc.health.v1` has had these two methods and no others since it was
/// written, so an exact list costs nothing and keeps the public set to what an
/// unauthenticated caller genuinely must reach. A prefix would hand
/// `/grpc.health.v1.Health/Anything` straight to the router, which is the
/// unauthenticated `UNIMPLEMENTED` the rule below exists to avoid.
const HEALTH: [&str; 2] = [
    "/grpc.health.v1.Health/Check",
    "/grpc.health.v1.Health/Watch",
];
/// The producer package.
const PRODUCER: &str = "/flexiq.v1.";
/// The executor package (#720). Classified now so the RPCs that land in it
/// arrive already gated, rather than relying on that PR to remember.
const EXECUTOR: &str = "/flexiq.executor.v1.";
/// The operator package (#836), split between two scopes by method.
const ADMIN: &str = "/flexiq.admin.v1.";
/// The operator package's service path.
const ADMIN_SERVICE: &str = "/flexiq.admin.v1.AdminService/";
/// Its `NO_SIDE_EFFECTS` methods, which `inspect` reaches. Every other method
/// — including one this list has not heard of — needs `admin`, so a method
/// added without updating this list fails closed. A test holds the list to the
/// descriptor's idempotency levels, both ways.
pub const INSPECT_METHODS: [&str; 8] = [
    "ListQueues",
    "GetThroughput",
    "ListDeadLetters",
    "GetDeadLetter",
    "ListWorkers",
    "ListPeriodicTasks",
    "GetPeriodicTask",
    "ListOverrides",
];
/// The JSON facade's operator namespace: `GET` is `inspect`, anything else
/// `admin`, which is the same split because the facade serves `GET` exactly
/// for the `NO_SIDE_EFFECTS` methods.
const FACADE_ADMIN: &str = "/v1/admin";
/// The JSON facade's namespace (#718), which transcodes the producer package
/// and, under [`FACADE_ADMIN`], the operator package.
///
/// It is a prefix here for the same reason a package is: a route added to the
/// facade inherits the producer scope without anyone editing this file, and it
/// **must** inherit it — a door that transcodes an RPC must not be a way to
/// call it with a credential the RPC itself would refuse.
const FACADE: &str = "/v1";

/// What a path asks of its caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Requirement {
    /// No credential. Health only.
    Public,
    /// A credential, but no particular scope.
    Authenticated,
    /// A credential carrying this scope.
    Scoped(Scope),
}

/// Whether a path belongs to the JSON facade's namespace.
///
/// `/v1` itself as well as everything under it: the root matches no binding,
/// but it is still this door's address, and a path inside the facade that a
/// non-producer credential can reach at all is one the gate table does not
/// cover. `/v1beta` is **not** in it — a bare `starts_with` would hand another
/// namespace's paths the producer scope.
fn in_facade(path: &str) -> bool {
    under(path, FACADE)
}

/// Whether `path` is `root` or a path below it — a segment match, so `/v1`
/// does not claim `/v1beta` and `/v1/admin` does not claim `/v1/administer`.
fn under(path: &str, root: &str) -> bool {
    path.strip_prefix(root)
        .is_some_and(|rest| rest.is_empty() || rest.starts_with('/'))
}

/// The scope one operator gRPC method needs.
fn admin_method(path: &str) -> Scope {
    let read_only = path
        .strip_prefix(ADMIN_SERVICE)
        .is_some_and(|method| INSPECT_METHODS.contains(&method));
    if read_only {
        Scope::Inspect
    } else {
        Scope::Admin
    }
}

/// Classify one request by its path, and — on the JSON facade's operator
/// paths, where the verb is what separates a read from a write — its method.
pub fn requirement(method: &http::Method, path: &str) -> Requirement {
    if HEALTH.contains(&path) {
        // A kubelet `grpc:` probe sends no metadata and has no way to, so
        // gating health would mean either no readiness probe or a token
        // written literally into the Deployment spec. What it publishes is one
        // bit — whether storage answers — to something that already reached
        // the port.
        Requirement::Public
    } else if path.starts_with(ADMIN) {
        Requirement::Scoped(admin_method(path))
    } else if under(path, FACADE_ADMIN) {
        // Matched before the producer's `/v1`, which would otherwise claim it.
        if method == http::Method::GET {
            Requirement::Scoped(Scope::Inspect)
        } else {
            Requirement::Scoped(Scope::Admin)
        }
    } else if path.starts_with(PRODUCER) || in_facade(path) {
        Requirement::Scoped(Scope::Produce)
    } else if path.starts_with(EXECUTOR) {
        Requirement::Scoped(Scope::Execute)
    } else {
        // Reflection lands here, as does `/metrics` and anything unrouted.
        // `/metrics` wants exactly this and not a scope: an operational read is
        // not a data door, but it is still a read of one tenant's state, and a
        // scraper is as likely to hold an `execute` credential as a `produce`
        // one.
        Requirement::Authenticated
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::grpc::facade::descriptor;

    /// A gRPC call is always a `POST`.
    fn grpc(path: &str) -> Requirement {
        requirement(&http::Method::POST, path)
    }

    /// Every operator method needs a scope, and which one is its idempotency
    /// level's: the list the gate keeps must match the descriptor both ways.
    #[test]
    fn the_inspect_methods_are_exactly_the_read_only_ones() {
        let rpcs = descriptor::rpcs(descriptor::ADMIN_PACKAGE);
        assert!(!rpcs.is_empty(), "the admin package declares no RPCs");
        for rpc in &rpcs {
            let path = format!("{ADMIN_SERVICE}{}", rpc.method);
            let want = if rpc.no_side_effects {
                Scope::Inspect
            } else {
                Scope::Admin
            };
            assert_eq!(grpc(&path), Requirement::Scoped(want), "{path}");
        }
        for method in INSPECT_METHODS {
            assert!(
                rpcs.iter()
                    .any(|rpc| rpc.method == method && rpc.no_side_effects),
                "{method} is not a read-only admin RPC"
            );
        }
    }

    /// A method nobody classified needs the stronger scope, not the weaker.
    #[test]
    fn an_unknown_admin_method_needs_admin() {
        for path in [
            "/flexiq.admin.v1.AdminService/PurgeEverything",
            "/flexiq.admin.v1.AdminService/",
            "/flexiq.admin.v1.OtherService/ListQueues",
        ] {
            assert_eq!(grpc(path), Requirement::Scoped(Scope::Admin), "{path}");
        }
    }

    /// On the facade the verb separates a read from a write, and the operator
    /// paths are not the producer's even though both sit under `/v1`.
    #[test]
    fn the_facade_admin_paths_split_by_verb() {
        for path in ["/v1/admin", "/v1/admin/queues", "/v1/admin/deadLetters/x"] {
            assert_eq!(
                requirement(&http::Method::GET, path),
                Requirement::Scoped(Scope::Inspect),
                "{path}"
            );
            for verb in [http::Method::POST, http::Method::HEAD, http::Method::DELETE] {
                assert_eq!(
                    requirement(&verb, path),
                    Requirement::Scoped(Scope::Admin),
                    "{verb} {path}"
                );
            }
        }
        // A segment match: a lookalike under `/v1` stays the producer's.
        assert_eq!(
            requirement(&http::Method::GET, "/v1/administer"),
            Requirement::Scoped(Scope::Produce)
        );
    }

    #[test]
    fn the_two_health_rpcs_are_the_only_public_paths() {
        assert_eq!(grpc("/grpc.health.v1.Health/Check"), Requirement::Public);
        assert_eq!(grpc("/grpc.health.v1.Health/Watch"), Requirement::Public);
    }

    /// The health service is public; the health *prefix* is not. A method that
    /// does not exist would otherwise reach the router with no credential and
    /// come back `UNIMPLEMENTED`.
    #[test]
    fn an_unknown_health_method_is_not_public() {
        for path in [
            "/grpc.health.v1.Health/Anything",
            "/grpc.health.v1.Health/",
            "/grpc.health.v1.Health/CheckX",
        ] {
            assert_eq!(grpc(path), Requirement::Authenticated, "path: {path}");
        }
    }

    /// A scraper reads one tenant's state, so it presents a credential — but
    /// no particular one, because a scrape is not a produce and not an execute.
    #[test]
    fn the_metrics_path_needs_a_credential_of_either_scope() {
        assert_eq!(
            grpc(crate::grpc::metrics::METRICS_PATH),
            Requirement::Authenticated
        );
    }

    #[test]
    fn each_package_carries_its_own_scope() {
        assert_eq!(
            grpc("/flexiq.v1.ProducerService/Enqueue"),
            Requirement::Scoped(Scope::Produce)
        );
        // The RPC #714 has not written yet gets the same answer as the six it
        // has: that is the property the prefix exists for.
        assert_eq!(
            grpc("/flexiq.v1.ProducerService/SubmitWorkflow"),
            Requirement::Scoped(Scope::Produce)
        );
        assert_eq!(
            grpc("/flexiq.executor.v1.ExecutorService/Dispatch"),
            Requirement::Scoped(Scope::Execute)
        );
    }

    /// The facade is the producer package by another spelling, so it asks for
    /// the same scope — including on a path no binding serves, which is
    /// refused for want of a credential before it is refused for want of a
    /// route.
    #[test]
    fn the_json_facade_carries_the_producer_scope() {
        for path in [
            "/v1",
            "/v1/",
            "/v1/jobs",
            "/v1/jobs/01924f",
            "/v1/queues/emails/stats",
            "/v1/stats",
            "/v1/whatever-lands-here-next",
        ] {
            assert_eq!(
                grpc(path),
                Requirement::Scoped(Scope::Produce),
                "path: {path}"
            );
        }
    }

    #[test]
    fn reflection_needs_a_credential() {
        for path in [
            "/grpc.reflection.v1.ServerReflection/ServerReflectionInfo",
            "/grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo",
        ] {
            assert_eq!(grpc(path), Requirement::Authenticated);
        }
    }

    /// The default must be the closed one: an unrouted path answered without a
    /// credential is an oracle for which services a build carries.
    #[test]
    fn an_unknown_path_still_needs_a_credential() {
        for path in ["/", "/nonsense", "/flexiq.v2.ProducerService/Enqueue"] {
            assert_eq!(grpc(path), Requirement::Authenticated);
        }
    }

    /// A prefix must not match a package that merely starts the same way.
    #[test]
    fn a_lookalike_package_is_not_the_producer_package() {
        assert_eq!(
            grpc("/flexiq.v1beta.ProducerService/Enqueue"),
            Requirement::Authenticated
        );
        // Same rule on the facade's side: `/v1` is a path segment, not a
        // prefix, so a future `/v1beta` namespace does not inherit its scope.
        assert_eq!(grpc("/v1beta/jobs"), Requirement::Authenticated);
        assert_eq!(grpc("/v1x"), Requirement::Authenticated);
        assert_eq!(
            grpc("/grpc.health.v1beta.Health/Check"),
            Requirement::Authenticated
        );
    }
}
