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
pub use crate::tokens::grant::{Access, Grants};
pub use crate::tokens::scope::{Scope, ScopeSet};

/// An authenticated caller: one credential, one namespace, and what it may do
/// in it.
#[derive(Debug, Clone)]
pub struct Principal {
    credential: Arc<str>,
    namespace: Arc<str>,
    /// Behind an `Arc` because a principal is cloned into every request's
    /// extensions and narrowed grants are a `Vec`.
    scopes: Arc<Grants>,
    /// The scope the path needed and what these grants reach behind it, set
    /// by [`Self::behind`]. `None` until then, which reaches nothing.
    door: Option<(Scope, Access)>,
}

impl Principal {
    /// A principal presenting `credential`, scoped to `namespace` with exactly
    /// `scopes`.
    pub fn new(
        credential: impl Into<Arc<str>>,
        namespace: impl Into<Arc<str>>,
        scopes: impl Into<Grants>,
    ) -> Self {
        Self {
            credential: credential.into(),
            namespace: namespace.into(),
            scopes: Arc::new(scopes.into()),
            door: None,
        }
    }

    /// The public id of the credential presented — never its secret. What a
    /// per-caller limit counts against.
    pub fn credential(&self) -> &Arc<str> {
        &self.credential
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
        self.scopes.opens(scope)
    }

    /// This principal, as seen from behind `scope`'s door: what it may reach
    /// there is fixed now, by the layer, so every handler reads one answer.
    pub fn behind(mut self, scope: Scope) -> Self {
        self.door = Some((scope, self.scopes.access(scope)));
        self
    }

    /// The door the layer let this caller through, if the path had one.
    pub fn door(&self) -> Option<Scope> {
        self.door.as_ref().map(|(scope, _)| *scope)
    }

    /// Whether this caller may touch the queue and task a call names, `None`
    /// meaning every queue (or task). A principal no door was fixed for reaches
    /// nothing: a handler asking has no business being reachable without one.
    pub fn reaches(&self, queue: Option<&str>, task: Option<&str>) -> bool {
        self.door
            .as_ref()
            .is_some_and(|(_, access)| access.reaches(queue, task))
    }

    /// Whether this caller reaches every queue and every task behind its door —
    /// what a method that checks neither needs.
    pub fn reaches_everything(&self) -> bool {
        self.door
            .as_ref()
            .is_some_and(|(_, access)| access.is_whole())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_principal_grants_every_scope_its_token_carried() {
        let principal = Principal::new("tok", "prod", ScopeSet::ALL);
        assert!(principal.grants(Scope::Produce));
        assert!(principal.grants(Scope::Execute));
        assert_eq!(&**principal.namespace(), "prod");
        assert_eq!(&**principal.credential(), "tok");
    }

    #[test]
    fn a_narrower_set_grants_only_what_it_lists() {
        let principal = Principal::new("tok", "prod", ScopeSet::of(&[Scope::Produce]));
        assert!(principal.grants(Scope::Produce));
        assert!(!principal.grants(Scope::Execute));
    }

    #[test]
    fn a_principal_reaches_nothing_until_a_door_is_fixed() {
        let principal = Principal::new("tok", "prod", ScopeSet::ALL);
        assert!(!principal.reaches(Some("emails"), Some("send")));
        assert!(!principal.reaches_everything());
        let behind = principal.behind(Scope::Produce);
        assert_eq!(behind.door(), Some(Scope::Produce));
        assert!(behind.reaches_everything());
    }

    #[test]
    fn a_narrowed_principal_reaches_only_its_queues() {
        let grants = Grants::parse_all(["produce:queue=emails"]).expect("valid");
        let principal = Principal::new("tok", "prod", grants).behind(Scope::Produce);
        assert!(principal.reaches(Some("emails"), Some("send")));
        assert!(!principal.reaches(Some("billing"), Some("send")));
        assert!(!principal.reaches(None, None));
        assert!(!principal.reaches_everything());
    }

    #[test]
    fn an_empty_set_grants_nothing() {
        let principal = Principal::new("tok", "prod", ScopeSet::of(&[]));
        assert!(!principal.grants(Scope::Produce));
        assert!(!principal.grants(Scope::Execute));
    }
}
