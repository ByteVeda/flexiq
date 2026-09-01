//! Who the caller is, once a credential has been believed.
//!
//! A [`Principal`] carries a namespace and a set of scopes from this, the first
//! commit that authenticates anything, even though the shared secret behind it
//! grants both scopes and the only namespace this process serves. The fields
//! are not doing work yet; they are what makes #717's token store an
//! implementation of [`Authenticator`](super::Authenticator) rather than a
//! rewrite of everything that reads one.

use std::sync::Arc;

/// What a credential may do, at the granularity the wire contract draws it:
/// one scope per proto package (design doc D1).
///
/// A package is the right unit because the audiences differ — a producer
/// submits work and an executor runs it — and because a scope that named an RPC
/// would have to grow every time the service does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    /// `flexiq.v1` — submit, read and cancel work.
    Produce,
    /// `flexiq.executor.v1` — claim work and report on it.
    Execute,
}

impl Scope {
    /// This scope's bit in a [`ScopeSet`].
    const fn bit(self) -> u8 {
        match self {
            Self::Produce => 1 << 0,
            Self::Execute => 1 << 1,
        }
    }

    /// The scope's name, for a log line or a token definition.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Produce => "produce",
            Self::Execute => "execute",
        }
    }
}

/// A set of [`Scope`]s.
///
/// A bitset rather than a `Vec`: a principal is built once per request and read
/// once per request, so the allocation would buy nothing, and `Copy` keeps
/// [`Principal`] cheap to clone into a request's extensions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScopeSet(u8);

impl ScopeSet {
    /// Every scope. What a shared secret grants, having no way to say less.
    pub const ALL: Self = Self(Scope::Produce.bit() | Scope::Execute.bit());

    /// Exactly the scopes listed.
    pub fn of(scopes: &[Scope]) -> Self {
        Self(scopes.iter().fold(0, |bits, scope| bits | scope.bit()))
    }

    /// Whether this set grants `scope`.
    pub fn contains(self, scope: Scope) -> bool {
        self.0 & scope.bit() != 0
    }
}

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
    fn a_shared_secret_grants_both_packages() {
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
