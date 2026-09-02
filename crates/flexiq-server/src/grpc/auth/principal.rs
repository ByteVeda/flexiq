//! Who the caller is, once a credential has been believed.
//!
//! A [`Principal`] carries a namespace and a set of scopes, both taken from the
//! token that was presented — never from a request message (design doc D10,
//! §5.1). It is the only thing a handler learns about its caller, which is what
//! keeps "which namespace is this call scoped to" a question with one answer
//! and one place to read it.

use std::sync::Arc;

// A scope is a property of a token, and tokens are minted by builds with no
// `grpc` feature, so the types live outside this gate. They are re-exported
// here because the gate and the layer have always named them through this
// module, and where a scope is *defined* is not their concern.
pub use crate::tokens::scope::{Scope, ScopeSet};

/// An authenticated caller: one namespace, and what it may do in it.
#[derive(Debug, Clone)]
pub struct Principal {
    namespace: Arc<str>,
    scopes: ScopeSet,
}

impl Principal {
    /// A principal scoped to `namespace` with exactly `scopes`.
    pub fn new(namespace: impl Into<Arc<str>>, scopes: ScopeSet) -> Self {
        Self {
            namespace: namespace.into(),
            scopes,
        }
    }

    /// The namespace every `Storage` call made for this caller is scoped to.
    ///
    /// Never empty and never `None`: the role refuses to start without a
    /// namespace precisely so that this cannot be the ambiguous value (D11).
    pub fn namespace(&self) -> &Arc<str> {
        &self.namespace
    }

    /// Whether this caller may call `scope`'s package.
    pub fn grants(&self, scope: Scope) -> bool {
        self.scopes.contains(scope)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_principal_grants_every_scope_its_token_carried() {
        let principal = Principal::new("prod", ScopeSet::ALL);
        assert!(principal.grants(Scope::Produce));
        assert!(principal.grants(Scope::Execute));
        assert_eq!(&**principal.namespace(), "prod");
    }

    #[test]
    fn a_narrower_set_grants_only_what_it_lists() {
        let principal = Principal::new("prod", ScopeSet::of(&[Scope::Produce]));
        assert!(principal.grants(Scope::Produce));
        assert!(!principal.grants(Scope::Execute));
    }

    #[test]
    fn an_empty_set_grants_nothing() {
        let principal = Principal::new("prod", ScopeSet::of(&[]));
        assert!(!principal.grants(Scope::Produce));
        assert!(!principal.grants(Scope::Execute));
    }
}
