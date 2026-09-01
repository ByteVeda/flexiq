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

/// Health, which is the one thing an unauthenticated caller must reach.
const HEALTH: &str = "/grpc.health.v1.Health/";
/// The producer package.
const PRODUCER: &str = "/flexiq.v1.";
/// The executor package (#720). Classified now so the RPCs that land in it
/// arrive already gated, rather than relying on that PR to remember.
const EXECUTOR: &str = "/flexiq.executor.v1.";

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

/// Classify one gRPC path.
pub fn requirement(path: &str) -> Requirement {
    if path.starts_with(HEALTH) {
        // A kubelet `grpc:` probe sends no metadata and has no way to, so
        // gating health would mean either no readiness probe or a token
        // written literally into the Deployment spec. What it publishes is one
        // bit — whether storage answers — to something that already reached
        // the port.
        Requirement::Public
    } else if path.starts_with(PRODUCER) {
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
    fn health_is_the_only_public_path() {
        assert_eq!(
            requirement("/grpc.health.v1.Health/Check"),
            Requirement::Public
        );
        assert_eq!(
            requirement("/grpc.health.v1.Health/Watch"),
            Requirement::Public
        );
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
        assert_eq!(
            requirement("/grpc.health.v1beta.Health/Check"),
            Requirement::Authenticated
        );
    }
}
