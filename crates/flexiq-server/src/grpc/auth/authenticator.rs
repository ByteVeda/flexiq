//! The seam: one question asked of request metadata, answered by whatever
//! credential scheme the deployment configured.
//!
//! The question is asked of a [`MetadataMap`] and of nothing else. That is the
//! whole reason the wire carries no namespace field (design doc D10): a check
//! that reads a decoded request message is a check written once per RPC, and
//! the first RPC that forgets it is a cross-tenant read. Metadata is available
//! before the router has chosen a handler, so the check happens in one place
//! for every RPC there will ever be.

use tonic::metadata::MetadataMap;
use tonic::Status;

use super::principal::{Principal, ScopeSet};

/// Turns a request's metadata into the caller it belongs to.
///
/// Implementations answer only "who is this"; whether that caller may reach the
/// path it asked for is [`super::gate`]'s question, so a new credential scheme
/// cannot accidentally redefine what a scope means.
pub trait Authenticator: Send + Sync + 'static {
    /// Identify the caller behind `metadata`, or refuse the request.
    ///
    /// The refusal must not distinguish a missing credential from a wrong one:
    /// telling them apart is an oracle for whether a guessed token exists.
    fn authenticate(&self, metadata: &MetadataMap) -> Result<Principal, Status>;
}

/// The authenticator for a door with no credential configured.
///
/// Reachable only from loopback or a Unix socket — `config::grpc` refuses any
/// other bind without a token — so the boundary is the network stack or the
/// filesystem mode rather than something a caller presents. Every caller is the
/// process's own principal.
///
/// It exists rather than the layer being skipped for the unauthenticated case
/// because the `Principal` is what carries the namespace into the handlers. A
/// door that served no principal would need a second path through every one of
/// them, and a second path is a path that can drift.
#[derive(Debug, Clone)]
pub struct Anonymous {
    principal: Principal,
}

impl Anonymous {
    /// Grant every caller `namespace` with every scope.
    pub fn new(namespace: impl Into<std::sync::Arc<str>>) -> Self {
        Self {
            principal: Principal::new(namespace, ScopeSet::ALL),
        }
    }
}

impl Authenticator for Anonymous {
    fn authenticate(&self, _metadata: &MetadataMap) -> Result<Principal, Status> {
        Ok(self.principal.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::grpc::auth::principal::Scope;

    #[test]
    fn an_unconfigured_door_grants_the_process_namespace() {
        let principal = Anonymous::new("prod")
            .authenticate(&MetadataMap::new())
            .expect("no credential is required");
        assert_eq!(&**principal.namespace(), "prod");
        assert!(principal.grants(Scope::Produce));
    }
}
