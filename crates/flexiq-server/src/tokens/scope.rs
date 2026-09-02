//! What a credential may do, at the granularity the wire contract draws it.
//!
//! One scope per proto package (design doc D1). A package is the right unit
//! because the audiences differ — a producer submits work and an executor runs
//! it — and because a scope that named an RPC would have to grow every time the
//! service does.
//!
//! This lives outside `grpc/` because a scope is a property of a *token*, and
//! tokens are minted, listed and revoked by builds compiled without the `grpc`
//! feature. `grpc::auth::principal` re-exports both types, so the gate and the
//! layer name them where they always did.

use std::fmt;

use serde::de::{SeqAccess, Visitor};
use serde::ser::SerializeSeq;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A door a credential may open.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Scope {
    /// `flexiq.v1` — submit, read and cancel work.
    Produce,
    /// `flexiq.executor.v1` — claim work and report on it.
    Execute,
}

impl Scope {
    /// Every scope there is, in the order a listing shows them.
    pub const ALL: [Self; 2] = [Self::Produce, Self::Execute];

    /// This scope's bit in a [`ScopeSet`].
    const fn bit(self) -> u8 {
        match self {
            Self::Produce => 1 << 0,
            Self::Execute => 1 << 1,
        }
    }

    /// The scope's name, for a log line, a token definition or the wire.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Produce => "produce",
            Self::Execute => "execute",
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
/// It serialises as an array of names rather than as the bits, because the
/// stored form is read by builds that may know a different set of scopes than
/// the one that wrote it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ScopeSet(u8);

impl ScopeSet {
    /// Every scope this build knows.
    pub const ALL: Self = Self(Scope::Produce.bit() | Scope::Execute.bit());

    /// No scopes at all. A credential carrying this opens nothing.
    pub const NONE: Self = Self(0);

    /// Exactly the scopes listed.
    pub fn of(scopes: &[Scope]) -> Self {
        Self(scopes.iter().fold(0, |bits, scope| bits | scope.bit()))
    }

    /// Whether this set grants `scope`.
    pub fn contains(self, scope: Scope) -> bool {
        self.0 & scope.bit() != 0
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

impl Serialize for ScopeSet {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let names = self.names();
        let mut seq = serializer.serialize_seq(Some(names.len()))?;
        for name in names {
            seq.serialize_element(name)?;
        }
        seq.end()
    }
}

impl<'de> Deserialize<'de> for ScopeSet {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_seq(NameVisitor)
    }
}

/// Reads the stored array of names.
struct NameVisitor;

impl<'de> Visitor<'de> for NameVisitor {
    type Value = ScopeSet;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "an array of scope names")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<ScopeSet, A::Error> {
        let mut set = ScopeSet::NONE;
        while let Some(name) = seq.next_element::<String>()? {
            // A name this build does not know is dropped, not refused. Dropping
            // it can only *narrow* what the credential opens, and a stored row
            // written by a newer build must not lock an older one out of the
            // scopes they agree on. The log is how the operator learns the two
            // disagree.
            match Scope::parse(&name) {
                Some(scope) => set.insert(scope),
                None => log::warn!(
                    "gRPC token carries scope '{name}', which this build does not \
                     know; ignoring it"
                ),
            }
        }
        Ok(set)
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
        assert_eq!(Scope::parse("admin"), None);
        assert_eq!(Scope::parse(""), None);
    }

    #[test]
    fn a_set_round_trips_through_json() {
        let set = ScopeSet::of(&[Scope::Execute, Scope::Produce]);
        let encoded = serde_json::to_string(&set).expect("encode");
        assert_eq!(encoded, r#"["produce","execute"]"#);
        let decoded: ScopeSet = serde_json::from_str(&encoded).expect("decode");
        assert_eq!(decoded, set);
    }

    /// A row written by a build that knows more scopes than this one must still
    /// grant the scopes both builds understand.
    #[test]
    fn an_unknown_scope_name_narrows_rather_than_failing() {
        let decoded: ScopeSet =
            serde_json::from_str(r#"["produce","teleport"]"#).expect("unknown names are ignored");
        assert_eq!(decoded, ScopeSet::of(&[Scope::Produce]));
    }

    #[test]
    fn all_holds_every_scope_this_build_knows() {
        for scope in Scope::ALL {
            assert!(ScopeSet::ALL.contains(scope));
        }
    }
}
