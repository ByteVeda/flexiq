//! `FLEXIQ_GRPC_TOKEN`: one secret, shared by every client, granting
//! everything.
//!
//! It is not the answer and this module does not pretend otherwise. A shared
//! secret cannot be revoked for one client, carries no scope of its own and
//! leaves no audit trail; #717 replaces it with a token store. What it is good
//! for is closing the gap the door currently has — a producer service reachable
//! with no credential at all — while the store is built, and it does that
//! behind [`Authenticator`], so the replacement is a second implementation
//! rather than a rewrite.

use flexiq_core::Secret;
use tonic::metadata::MetadataMap;
use tonic::Status;

use super::authenticator::Authenticator;
use super::principal::{Principal, ScopeSet};
use crate::grpc::status::WireError;

/// The metadata key the credential arrives under.
///
/// gRPC's own name for a bearer credential, so a generic client's
/// call-credentials plumbing sets it without being told, and so a proxy in
/// front of the door strips or rewrites the header it already knows about.
const AUTHORIZATION: &str = "authorization";

/// The scheme, matched case-insensitively as RFC 7235 requires.
const BEARER: &str = "bearer ";

/// Authenticates against one configured secret.
#[derive(Debug, Clone)]
pub struct SharedSecret {
    expected: Secret,
    principal: Principal,
}

impl SharedSecret {
    /// Accept `expected`, granting `namespace` with every scope.
    ///
    /// Every scope, because a shared secret has no way to express less: there
    /// is one token and every client presents it, so a narrower grant would be
    /// a narrower grant for all of them. #717 is where a credential can say
    /// what it is for.
    pub fn new(expected: Secret, namespace: impl Into<std::sync::Arc<str>>) -> Self {
        Self {
            expected,
            principal: Principal::new(namespace, ScopeSet::ALL),
        }
    }

    /// The token in `metadata`, if it is present and well-formed.
    fn presented(metadata: &MetadataMap) -> Option<Secret> {
        let raw = metadata.get(AUTHORIZATION)?.to_str().ok()?;
        // `split_at` on a fixed ASCII prefix length is safe once the prefix has
        // matched, and matching it case-insensitively costs one allocation on
        // a string that is at most a header.
        if raw.len() < BEARER.len() || !raw[..BEARER.len()].eq_ignore_ascii_case(BEARER) {
            return None;
        }
        Some(Secret::new(&raw[BEARER.len()..]))
    }
}

impl Authenticator for SharedSecret {
    fn authenticate(&self, metadata: &MetadataMap) -> Result<Principal, Status> {
        // One `else` for every way this can fail — no header, the wrong
        // scheme, a value that is not UTF-8, the wrong token. A branch per
        // cause would let a caller tell "I sent nothing" from "I guessed
        // wrong", which is exactly the signal a guessing client wants.
        match Self::presented(metadata) {
            Some(presented) if self.expected.matches(&presented) => Ok(self.principal.clone()),
            _ => Err(WireError::unauthenticated().into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::grpc::auth::principal::Scope;
    use crate::grpc::status::reason;
    use tonic::Code;

    const TOKEN: &str = "0123456789abcdef";

    fn authenticator() -> SharedSecret {
        SharedSecret::new(Secret::new(TOKEN), "prod")
    }

    fn with_authorization(value: &str) -> MetadataMap {
        let mut metadata = MetadataMap::new();
        metadata.insert(AUTHORIZATION, value.parse().expect("an ASCII header value"));
        metadata
    }

    #[test]
    fn the_configured_token_authenticates() {
        let principal = authenticator()
            .authenticate(&with_authorization(&format!("Bearer {TOKEN}")))
            .expect("the right token must be accepted");
        assert_eq!(&**principal.namespace(), "prod");
        assert!(principal.grants(Scope::Produce));
        assert!(principal.grants(Scope::Execute));
    }

    #[test]
    fn the_scheme_is_case_insensitive() {
        for scheme in ["Bearer", "bearer", "BEARER"] {
            assert!(authenticator()
                .authenticate(&with_authorization(&format!("{scheme} {TOKEN}")))
                .is_ok());
        }
    }

    /// The acceptance criterion: nothing distinguishes the ways of failing.
    #[test]
    fn every_failure_is_the_same_answer() {
        let authenticator = authenticator();
        let refusals = [
            // No credential at all.
            authenticator.authenticate(&MetadataMap::new()),
            // A credential, wrong.
            authenticator.authenticate(&with_authorization(&format!("Bearer {TOKEN}x"))),
            // The right token under the wrong scheme.
            authenticator.authenticate(&with_authorization(&format!("Basic {TOKEN}"))),
            // The right token with no scheme.
            authenticator.authenticate(&with_authorization(TOKEN)),
            // A prefix of the right token, which a naive compare might accept.
            authenticator.authenticate(&with_authorization("Bearer 0123456789abcde")),
            // The scheme and nothing after it.
            authenticator.authenticate(&with_authorization("Bearer ")),
        ];

        for refusal in refusals {
            let status = refusal.expect_err("must refuse");
            assert_eq!(status.code(), Code::Unauthenticated);
            assert_eq!(
                status.message(),
                WireError::unauthenticated().message(),
                "the message must not say which way the credential was wrong"
            );
            assert_eq!(
                WireError::unauthenticated().reason(),
                reason::UNAUTHENTICATED
            );
        }
    }
}
