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
/// The JSON facade's namespace (#718), which transcodes the producer package
/// and only it.
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
    path.strip_prefix(FACADE)
        .is_some_and(|rest| rest.is_empty() || rest.starts_with('/'))
}

/// Classify one gRPC path.
pub fn requirement(path: &str) -> Requirement {
    if HEALTH.contains(&path) {
        // A kubelet `grpc:` probe sends no metadata and has no way to, so
        // gating health would mean either no readiness probe or a token
        // written literally into the Deployment spec. What it publishes is one
        // bit — whether storage answers — to something that already reached
        // the port.
        Requirement::Public
    } else if path.starts_with(PRODUCER) || in_facade(path) {
        Requirement::Scoped(Scope::Produce)
    } else if path.starts_with(EXECUTOR) {
        Requirement::Scoped(Scope::Execute)
    } else {
        // Reflection lands here, as does anything unrouted.
        Requirement::Authenticated
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_two_health_rpcs_are_the_only_public_paths() {
        assert_eq!(
            requirement("/grpc.health.v1.Health/Check"),
            Requirement::Public
        );
        assert_eq!(
            requirement("/grpc.health.v1.Health/Watch"),
            Requirement::Public
        );
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
            assert_eq!(
                requirement(path),
                Requirement::Authenticated,
                "path: {path}"
            );
        }
    }

    #[test]
    fn each_package_carries_its_own_scope() {
        assert_eq!(
            requirement("/flexiq.v1.ProducerService/Enqueue"),
            Requirement::Scoped(Scope::Produce)
        );
        // The RPC #714 has not written yet gets the same answer as the six it
        // has: that is the property the prefix exists for.
        assert_eq!(
            requirement("/flexiq.v1.ProducerService/SubmitWorkflow"),
            Requirement::Scoped(Scope::Produce)
        );
        assert_eq!(
            requirement("/flexiq.executor.v1.ExecutorService/Dispatch"),
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
                requirement(path),
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
            assert_eq!(requirement(path), Requirement::Authenticated);
        }
    }

    /// The default must be the closed one: an unrouted path answered without a
    /// credential is an oracle for which services a build carries.
    #[test]
    fn an_unknown_path_still_needs_a_credential() {
        for path in ["/", "/nonsense", "/flexiq.v2.ProducerService/Enqueue"] {
            assert_eq!(requirement(path), Requirement::Authenticated);
        }
    }

    /// A prefix must not match a package that merely starts the same way.
    #[test]
    fn a_lookalike_package_is_not_the_producer_package() {
        assert_eq!(
            requirement("/flexiq.v1beta.ProducerService/Enqueue"),
            Requirement::Authenticated
        );
        // Same rule on the facade's side: `/v1` is a path segment, not a
        // prefix, so a future `/v1beta` namespace does not inherit its scope.
        assert_eq!(requirement("/v1beta/jobs"), Requirement::Authenticated);
        assert_eq!(requirement("/v1x"), Requirement::Authenticated);
        assert_eq!(
            requirement("/grpc.health.v1beta.Health/Check"),
            Requirement::Authenticated
        );
    }
}
