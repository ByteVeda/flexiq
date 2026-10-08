//! What a credential may do, at the granularity the wire contract draws it.
//!
//! One scope per proto package (design doc D1), except the operator package,
//! which has three: `inspect` for its read-only methods, `tokens` for its
//! credential methods, and `admin` for the rest.
//! A package is the right unit because the audiences differ — a producer
//! submits work and an executor runs it — and because a scope that named an RPC
//! would have to grow every time the service does. The operator split is drawn
//! by each method's idempotency level for the same reason (#836), and so is the
//! producer's: `read` reaches its read-only methods, `produce` all of them.
//!
//! This lives outside `grpc/` because a scope is a property of a *token*, and
//! tokens are minted, listed and revoked by builds compiled without the `grpc`
//! feature. `grpc::auth::principal` re-exports both types, so the gate and the
//! layer name them where they always did.

use std::fmt;

/// A door a credential may open.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Scope {
    /// `flexiq.v1` — submit, read and cancel work.
    Produce,
    /// `flexiq.v1`, read-only methods — look at jobs, queue stats and workflow
    /// runs, never submit or cancel. A `produce` grant covers it too.
    Read,
    /// `flexiq.executor.v1` — claim work and report on it.
    Execute,
    /// `flexiq.admin.v1`, read-only methods — look at queues, dead letters,
    /// workers, schedules and overrides.
    Inspect,
    /// `flexiq.admin.v1`, every other method — pause, replay, delete, purge,
    /// schedule and override.
    Admin,
    /// `flexiq.admin.v1`, the token methods — mint, read and revoke API
    /// tokens, reads included. Never implied by `admin`: a credential that can
    /// mint credentials is a different grant from one that can pause a queue.
    Tokens,
}

impl Scope {
    /// Every scope there is, in the order a listing shows them.
    pub const ALL: [Self; 6] = [
        Self::Produce,
        Self::Read,
        Self::Execute,
        Self::Inspect,
        Self::Admin,
        Self::Tokens,
    ];

    /// This scope's bit in a [`ScopeSet`].
    const fn bit(self) -> u8 {
        match self {
            Self::Produce => 1 << 0,
            Self::Execute => 1 << 1,
            Self::Inspect => 1 << 2,
            Self::Admin => 1 << 3,
            Self::Read => 1 << 4,
            Self::Tokens => 1 << 5,
        }
    }

    /// Whether a grant of `granted` opens what this scope guards.
    ///
    /// `produce` has always reached the producer's reads, so it still does:
    /// narrowing it would take reads away from every token minted before `read`
    /// existed. That is the only implication; neither `inspect` nor `tokens`
    /// is implied by `admin`.
    pub fn is_granted_by(self, granted: Self) -> bool {
        granted == self || (self == Self::Read && granted == Self::Produce)
    }

    /// The scope's name, for a log line, a token definition or the wire.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Produce => "produce",
            Self::Read => "read",
            Self::Execute => "execute",
            Self::Inspect => "inspect",
            Self::Admin => "admin",
            Self::Tokens => "tokens",
        }
    }

    /// The scope `name` spells, or `None` if it spells no scope this build has.
    pub fn parse(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|scope| scope.as_str() == name)
    }

    /// Every scope's name, for an error that has to say what was allowed.
    pub fn names() -> String {
        Self::ALL
            .iter()
            .map(|scope| scope.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    }
}

impl fmt::Display for Scope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A set of [`Scope`]s.
///
/// A bitset rather than a `Vec`: a principal is built once per request and read
/// once per request, so the allocation would buy nothing, and `Copy` keeps a
/// principal cheap to clone into a request's extensions.
///
/// It holds whole scopes only. A token's stored form is its
/// [`Grants`](super::grant::Grants), which keeps one of these beside any
/// narrowed grants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ScopeSet(u8);

impl ScopeSet {
    /// Every scope this build knows.
    pub const ALL: Self = Self(
        Scope::Produce.bit()
            | Scope::Read.bit()
            | Scope::Execute.bit()
            | Scope::Inspect.bit()
            | Scope::Admin.bit()
            | Scope::Tokens.bit(),
    );

    /// No scopes at all. A credential carrying this opens nothing.
    pub const NONE: Self = Self(0);

    /// Exactly the scopes listed.
    pub fn of(scopes: &[Scope]) -> Self {
        Self(scopes.iter().fold(0, |bits, scope| bits | scope.bit()))
    }

    /// Whether this set lists `scope` itself.
    pub fn contains(self, scope: Scope) -> bool {
        self.0 & scope.bit() != 0
    }

    /// Whether some scope in this set opens what `scope` guards — `contains`,
    /// plus the one implication [`Scope::is_granted_by`] draws.
    pub fn opens(self, scope: Scope) -> bool {
        self.iter().any(|granted| scope.is_granted_by(granted))
    }

    /// Whether this set grants nothing.
    pub fn is_empty(self) -> bool {
        self.0 == 0
    }

    /// Add `scope` to the set.
    pub fn insert(&mut self, scope: Scope) {
        self.0 |= scope.bit();
    }

    /// The scopes in the set, in [`Scope::ALL`] order.
    pub fn iter(self) -> impl Iterator<Item = Scope> {
        Scope::ALL
            .into_iter()
            .filter(move |scope| self.contains(*scope))
    }

    /// The set's names, which is how it is stored and displayed.
    pub fn names(self) -> Vec<&'static str> {
        self.iter().map(Scope::as_str).collect()
    }
}

impl fmt::Display for ScopeSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.names().join(","))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_set_grants_only_what_it_lists() {
        let set = ScopeSet::of(&[Scope::Produce]);
        assert!(set.contains(Scope::Produce));
        assert!(!set.contains(Scope::Execute));
    }

    #[test]
    fn the_empty_set_grants_nothing() {
        assert!(ScopeSet::NONE.is_empty());
        for scope in Scope::ALL {
            assert!(!ScopeSet::NONE.contains(scope));
        }
    }

    #[test]
    fn every_scope_round_trips_through_its_name() {
        for scope in Scope::ALL {
            assert_eq!(Scope::parse(scope.as_str()), Some(scope));
        }
        assert_eq!(Scope::parse("teleport"), None);
        assert_eq!(Scope::parse("Admin"), None);
        assert_eq!(Scope::parse(""), None);
    }

    #[test]
    fn all_holds_every_scope_this_build_knows() {
        for scope in Scope::ALL {
            assert!(ScopeSet::ALL.contains(scope));
        }
    }

    /// Reading the operator package and changing it are two grants, and
    /// neither is implied by the producer's.
    #[test]
    fn the_operator_scopes_are_not_a_hierarchy() {
        let read = ScopeSet::of(&[Scope::Inspect]);
        assert!(!read.contains(Scope::Admin));
        let write = ScopeSet::of(&[Scope::Admin]);
        assert!(!write.contains(Scope::Inspect));
        let produce = ScopeSet::of(&[Scope::Produce]);
        assert!(!produce.contains(Scope::Inspect) && !produce.contains(Scope::Admin));
        assert!(!write.opens(Scope::Inspect), "admin does not imply inspect");
    }

    /// Every token minted before `read` existed reached the producer's reads
    /// through `produce`; it still must. The reverse would be a write grant.
    #[test]
    fn produce_opens_the_reads_and_read_opens_nothing_else() {
        let produce = ScopeSet::of(&[Scope::Produce]);
        assert!(produce.opens(Scope::Read));
        assert!(
            !produce.contains(Scope::Read),
            "the implication is not a listing"
        );
        let read = ScopeSet::of(&[Scope::Read]);
        assert!(read.opens(Scope::Read));
        for scope in [
            Scope::Produce,
            Scope::Execute,
            Scope::Inspect,
            Scope::Admin,
            Scope::Tokens,
        ] {
            assert!(!read.opens(scope), "read must not open {scope}");
        }
    }

    /// Minting credentials is its own grant: no other scope opens it, and it
    /// opens nothing else.
    #[test]
    fn tokens_is_implied_by_nothing_and_implies_nothing() {
        for scope in Scope::ALL.into_iter().filter(|s| *s != Scope::Tokens) {
            assert!(
                !ScopeSet::of(&[scope]).opens(Scope::Tokens),
                "{scope} must not open tokens"
            );
            assert!(
                !ScopeSet::of(&[Scope::Tokens]).opens(scope),
                "tokens must not open {scope}"
            );
        }
        assert_eq!(Scope::parse("tokens"), Some(Scope::Tokens));
    }
}
