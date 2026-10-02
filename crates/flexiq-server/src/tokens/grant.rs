//! Grants: a scope, optionally narrowed to some queues and some tasks (#839).
//!
//! A scope names a door. A grant says which rooms behind it: `produce` opens
//! every queue in the token's namespace, `produce:queue=emails` only one, and
//! `produce:queue=emails-*,task=send_receipt` one task on the queues whose names
//! start `emails-`. A token holds several grants, and a call is allowed when any
//! one of them covers it.
//!
//! **Stored as the spelled form, in the row's existing `scopes` array.** An
//! unnarrowed grant spells exactly the scope name older builds wrote, so every
//! existing row reads back unchanged. A narrowed one is a name an older build
//! does not know, and older builds drop names they do not know — so a
//! downgrade can only take doors away from a narrowed token, never open one
//! wider than it was minted for.

use std::fmt;

use serde::de::{SeqAccess, Visitor};
use serde::ser::SerializeSeq;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::scope::{Scope, ScopeSet};

/// The longest queue or task pattern a grant may carry.
const MAX_PATTERN_LEN: usize = 200;

/// Which names one qualifier of a grant reaches.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Pattern {
    /// Every name.
    Any,
    /// Exactly this name.
    Exact(String),
    /// Every name starting with this prefix.
    Prefix(String),
}

impl Pattern {
    /// Parse `raw`: a name, a prefix ending in `*`, or `*` alone.
    ///
    /// `*` anywhere but the end is refused rather than read literally: a queue
    /// named `a*b` is legal, but a pattern that means it would read to every
    /// operator as a glob that it is not.
    pub fn parse(raw: &str) -> Result<Self, String> {
        if raw.is_empty() {
            return Err("a pattern must not be empty — use `*` for every name".to_string());
        }
        if raw.chars().count() > MAX_PATTERN_LEN {
            return Err(format!(
                "a pattern must be at most {MAX_PATTERN_LEN} characters"
            ));
        }
        if let Some(bad) = raw
            .chars()
            .find(|c| c.is_control() || c.is_whitespace() || *c == ',' || *c == '=')
        {
            return Err(format!(
                "pattern '{raw}' contains {bad:?}, which a grant cannot carry"
            ));
        }
        let (stem, wildcard) = match raw.strip_suffix('*') {
            Some(stem) => (stem, true),
            None => (raw, false),
        };
        if stem.contains('*') {
            return Err(format!(
                "pattern '{raw}' has a `*` before its end; only a trailing `*` \
                 (a prefix) is supported"
            ));
        }
        Ok(match (stem.is_empty(), wildcard) {
            (true, _) => Self::Any,
            (false, true) => Self::Prefix(stem.to_string()),
            (false, false) => Self::Exact(stem.to_string()),
        })
    }

    /// Whether `name` is one this pattern reaches.
    pub fn matches(&self, name: &str) -> bool {
        match self {
            Self::Any => true,
            Self::Exact(exact) => name == exact,
            Self::Prefix(prefix) => name.starts_with(prefix.as_str()),
        }
    }

    /// Whether this pattern reaches every name.
    pub fn is_any(&self) -> bool {
        matches!(self, Self::Any)
    }

    /// Whether this pattern reaches the name a filter carries — and, with no
    /// filter, whether it reaches every name. A narrowed grant asked about
    /// "all of them" must answer no.
    fn reaches(&self, filter: Option<&str>) -> bool {
        filter.map_or_else(|| self.is_any(), |name| self.matches(name))
    }
}

impl fmt::Display for Pattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Any => f.write_str("*"),
            Self::Exact(exact) => f.write_str(exact),
            Self::Prefix(prefix) => write!(f, "{prefix}*"),
        }
    }
}

/// A scope, and the queues and tasks it reaches.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Grant {
    /// The door.
    pub scope: Scope,
    /// The queues behind it.
    pub queue: Pattern,
    /// The tasks behind it.
    pub task: Pattern,
}

impl Grant {
    /// A grant of `scope` over every queue and every task.
    pub fn whole(scope: Scope) -> Self {
        Self {
            scope,
            queue: Pattern::Any,
            task: Pattern::Any,
        }
    }

    /// Parse the spelled form: `scope` or `scope:queue=<pattern>,task=<pattern>`,
    /// either qualifier optional, neither repeated.
    ///
    /// Only [`NARROWABLE`] scopes take qualifiers. The operator doors have no
    /// method that checks a queue or a task against a grant, so a narrowed
    /// grant on one of them would be a restriction nothing enforces.
    pub fn parse(spelled: &str) -> Result<Self, String> {
        let (name, qualifiers) = match spelled.split_once(':') {
            Some((name, qualifiers)) => (name, Some(qualifiers)),
            None => (spelled, None),
        };
        let scope = Scope::parse(name)
            .ok_or_else(|| format!("unknown scope '{name}'. Available: {}", Scope::names()))?;
        let mut grant = Self::whole(scope);
        let Some(qualifiers) = qualifiers else {
            return Ok(grant);
        };
        if !narrowable(scope) {
            return Err(format!(
                "scope '{scope}' cannot be narrowed to queues or tasks; only {} can",
                NARROWABLE.map(Scope::as_str).join(", ")
            ));
        }
        let (mut queue, mut task) = (None, None);
        for qualifier in qualifiers.split(',') {
            let (key, value) = qualifier.split_once('=').ok_or_else(|| {
                format!("'{qualifier}' in '{spelled}' is not `queue=<pattern>` or `task=<pattern>`")
            })?;
            let slot = match key {
                "queue" => &mut queue,
                "task" => &mut task,
                other => {
                    return Err(format!(
                        "'{other}' in '{spelled}' is not a qualifier; use `queue` or `task`"
                    ))
                }
            };
            if slot.is_some() {
                return Err(format!("'{key}' appears twice in '{spelled}'"));
            }
            *slot = Some(Pattern::parse(value)?);
        }
        grant.queue = queue.unwrap_or(Pattern::Any);
        grant.task = task.unwrap_or(Pattern::Any);
        Ok(grant)
    }

    /// Whether this grant is narrower than its whole scope.
    pub fn is_narrowed(&self) -> bool {
        !self.queue.is_any() || !self.task.is_any()
    }

    /// Whether this grant reaches the queue and task a call names. `None` means
    /// the call reaches *every* queue (or task), which only an unnarrowed
    /// qualifier covers.
    pub fn reaches(&self, queue: Option<&str>, task: Option<&str>) -> bool {
        self.queue.reaches(queue) && self.task.reaches(task)
    }
}

impl fmt::Display for Grant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.scope.as_str())?;
        let mut separator = ':';
        for (key, pattern) in [("queue", &self.queue), ("task", &self.task)] {
            if !pattern.is_any() {
                write!(f, "{separator}{key}={pattern}")?;
                separator = ',';
            }
        }
        Ok(())
    }
}

/// The scopes a grant may narrow. `execute` is checked at attach, against the
/// tasks an executor declares, and again on every dispatch (#988).
pub const NARROWABLE: [Scope; 3] = [Scope::Produce, Scope::Read, Scope::Execute];

/// Whether `scope` may carry a queue or task qualifier.
fn narrowable(scope: Scope) -> bool {
    NARROWABLE.contains(&scope)
}

/// Everything one token may do.
///
/// Whole scopes are kept as a bitset beside the narrowed grants, because they
/// are the common case and the answer to "does this open the door at all"
/// should not need a scan.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Grants {
    whole: ScopeSet,
    narrowed: Vec<Grant>,
}

impl Grants {
    /// Parse every spelled grant, refusing the first that does not parse. The
    /// mint path uses this; reading a stored row does not (see [`Deserialize`]).
    pub fn parse_all<'a>(spelled: impl IntoIterator<Item = &'a str>) -> Result<Self, String> {
        let mut grants = Self::default();
        for one in spelled {
            grants.insert(Grant::parse(one)?);
        }
        Ok(grants)
    }

    /// Add one grant. A narrowed grant a whole one already covers is still
    /// kept, so the row says what the operator asked for.
    pub fn insert(&mut self, grant: Grant) {
        if !grant.is_narrowed() {
            self.whole.insert(grant.scope);
        } else if !self.narrowed.contains(&grant) {
            self.narrowed.push(grant);
        }
    }

    /// Whether these grants open nothing.
    pub fn is_empty(&self) -> bool {
        self.whole.is_empty() && self.narrowed.is_empty()
    }

    /// Whether some grant opens `scope`'s door at all, narrowed or not.
    pub fn opens(&self, scope: Scope) -> bool {
        self.whole.opens(scope)
            || self
                .narrowed
                .iter()
                .any(|grant| scope.is_granted_by(grant.scope))
    }

    /// What these grants allow behind `scope`'s door.
    pub fn access(&self, scope: Scope) -> Access {
        if self.whole.opens(scope) {
            return Access::Whole;
        }
        Access::Narrowed(
            self.narrowed
                .iter()
                .filter(|grant| scope.is_granted_by(grant.scope))
                .cloned()
                .collect(),
        )
    }

    /// Every grant, whole scopes first in [`Scope::ALL`] order.
    pub fn iter(&self) -> impl Iterator<Item = Grant> + '_ {
        self.whole
            .iter()
            .map(Grant::whole)
            .chain(self.narrowed.iter().cloned())
    }

    /// Every grant's spelled form — how the row stores it and the API shows it.
    pub fn spelled(&self) -> Vec<String> {
        self.iter().map(|grant| grant.to_string()).collect()
    }
}

impl From<ScopeSet> for Grants {
    fn from(whole: ScopeSet) -> Self {
        Self {
            whole,
            narrowed: Vec::new(),
        }
    }
}

impl From<Grant> for Grants {
    fn from(grant: Grant) -> Self {
        let mut grants = Self::default();
        grants.insert(grant);
        grants
    }
}

impl fmt::Display for Grants {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // A space, not a comma: a narrowed grant carries commas of its own.
        f.write_str(&self.spelled().join(" "))
    }
}

impl Serialize for Grants {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let spelled = self.spelled();
        let mut seq = serializer.serialize_seq(Some(spelled.len()))?;
        for one in &spelled {
            seq.serialize_element(one)?;
        }
        seq.end()
    }
}

impl<'de> Deserialize<'de> for Grants {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_seq(GrantVisitor)
    }
}

/// Reads the stored array of spelled grants.
struct GrantVisitor;

impl<'de> Visitor<'de> for GrantVisitor {
    type Value = Grants;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "an array of grants")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Grants, A::Error> {
        let mut grants = Grants::default();
        while let Some(spelled) = seq.next_element::<String>()? {
            // Dropped, not refused: dropping a grant can only *narrow* what the
            // credential opens, and a row written by a newer build must not
            // lock an older one out of the grants they agree on. It also means
            // a pattern this build cannot read denies rather than widens.
            match Grant::parse(&spelled) {
                Ok(grant) => grants.insert(grant),
                Err(error) => log::warn!(
                    "gRPC token carries grant '{spelled}', which this build cannot \
                     read ({error}); ignoring it"
                ),
            }
        }
        Ok(grants)
    }
}

/// What one caller may reach behind the door a path needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Access {
    /// Every queue and every task.
    Whole,
    /// Only what one of these grants reaches. Empty reaches nothing.
    Narrowed(Vec<Grant>),
}

impl Access {
    /// Whether the call may touch the queue and task it names. `None` asks
    /// about every queue (or task) at once.
    pub fn reaches(&self, queue: Option<&str>, task: Option<&str>) -> bool {
        match self {
            Self::Whole => true,
            Self::Narrowed(grants) => grants.iter().any(|grant| grant.reaches(queue, task)),
        }
    }

    /// Whether `task` is reachable on *some* queue — what an executor declaring
    /// the tasks it serves can be checked against, since it names no queue.
    pub fn reaches_task_somewhere(&self, task: &str) -> bool {
        match self {
            Self::Whole => true,
            Self::Narrowed(grants) => grants.iter().any(|grant| grant.task.matches(task)),
        }
    }

    /// Whether this access reaches every queue and every task.
    pub fn is_whole(&self) -> bool {
        matches!(self, Self::Whole)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn grant(spelled: &str) -> Grant {
        Grant::parse(spelled).unwrap_or_else(|error| panic!("{spelled}: {error}"))
    }

    #[test]
    fn a_pattern_is_a_name_a_prefix_or_everything() {
        assert_eq!(Pattern::parse("*"), Ok(Pattern::Any));
        assert_eq!(
            Pattern::parse("emails"),
            Ok(Pattern::Exact("emails".into()))
        );
        assert_eq!(
            Pattern::parse("emails-*"),
            Ok(Pattern::Prefix("emails-".into()))
        );
        let prefix = Pattern::Prefix("emails-".into());
        assert!(prefix.matches("emails-eu") && prefix.matches("emails-"));
        assert!(!prefix.matches("emails") && !prefix.matches("billing"));
        let exact = Pattern::Exact("emails".into());
        assert!(exact.matches("emails") && !exact.matches("emails-eu"));
    }

    #[test]
    fn a_pattern_that_is_not_one_is_refused() {
        for raw in [
            "",
            "a*b",
            "**",
            "*a",
            "a b",
            "a,b",
            "a=b",
            "tab\t",
            &"x".repeat(201),
        ] {
            assert!(Pattern::parse(raw).is_err(), "{raw:?}");
        }
    }

    #[test]
    fn a_grant_round_trips_through_its_spelling() {
        for spelled in [
            "produce",
            "read",
            "admin",
            "produce:queue=emails",
            "produce:task=send_receipt",
            "produce:queue=emails-*,task=send_receipt",
            "read:queue=emails",
            "produce:queue=a:b",
        ] {
            assert_eq!(grant(spelled).to_string(), spelled);
        }
        // Qualifiers are written in one order, whatever order they were read in,
        // and a `*` qualifier is the whole scope.
        assert_eq!(
            grant("produce:task=t,queue=q").to_string(),
            "produce:queue=q,task=t"
        );
        assert_eq!(grant("produce:queue=*"), Grant::whole(Scope::Produce));
    }

    #[test]
    fn a_grant_that_does_not_parse_is_refused() {
        for spelled in [
            "",
            "teleport",
            "Produce",
            "produce:",
            "produce:queue",
            "produce:queue=",
            "produce:colour=red",
            "produce:queue=a,queue=b",
            "produce:queue=a*b",
            "inspect:queue=emails",
            "admin:queue=emails",
        ] {
            assert!(Grant::parse(spelled).is_err(), "{spelled:?}");
        }
    }

    /// Only the doors whose handlers check a queue and a task can be narrowed;
    /// anywhere else the qualifier would be a restriction nothing enforces.
    #[test]
    fn narrowing_a_door_that_cannot_enforce_it_says_which_can() {
        let error = Grant::parse("inspect:task=x").expect_err("refused");
        assert!(
            error.contains("produce") && error.contains("read") && error.contains("execute"),
            "{error}"
        );
    }

    #[test]
    fn an_execute_grant_narrows_to_queues_and_tasks() {
        let narrowed = grant("execute:queue=emails,task=send_*");
        assert_eq!(narrowed.scope, Scope::Execute);
        assert!(narrowed.reaches(Some("emails"), Some("send_receipt")));
        assert!(!narrowed.reaches(Some("billing"), Some("send_receipt")));
        assert_eq!(narrowed.to_string(), "execute:queue=emails,task=send_*");
    }

    /// An executor names tasks, never queues: a task is declarable when any
    /// grant reaches it on some queue, and the queue is left to dispatch.
    #[test]
    fn a_task_is_reachable_somewhere_when_any_grant_names_it() {
        let grants = Grants::parse_all(["execute:queue=emails,task=send_*", "execute:task=charge"])
            .expect("valid");
        let access = grants.access(Scope::Execute);
        assert!(access.reaches_task_somewhere("send_receipt"));
        assert!(access.reaches_task_somewhere("charge"));
        assert!(!access.reaches_task_somewhere("refund"));
        assert!(Access::Whole.reaches_task_somewhere("refund"));
        assert!(!Access::Narrowed(Vec::new()).reaches_task_somewhere("refund"));
        // Queue-only: every task, on its queues.
        let queue_only = Grants::parse_all(["execute:queue=emails"]).expect("valid");
        assert!(queue_only
            .access(Scope::Execute)
            .reaches_task_somewhere("refund"));
    }

    #[test]
    fn a_narrowed_grant_reaches_only_what_it_names() {
        let narrowed = grant("produce:queue=emails-*,task=send_receipt");
        assert!(narrowed.reaches(Some("emails-eu"), Some("send_receipt")));
        assert!(!narrowed.reaches(Some("billing"), Some("send_receipt")));
        assert!(!narrowed.reaches(Some("emails-eu"), Some("charge")));
        // "Every queue" is not a queue it names.
        assert!(!narrowed.reaches(None, Some("send_receipt")));
        assert!(!narrowed.reaches(Some("emails-eu"), None));
        let queue_only = grant("produce:queue=emails");
        assert!(queue_only.reaches(Some("emails"), None));
    }

    /// A row minted before grants existed is an array of scope names; it must
    /// read back as exactly those scopes, whole.
    #[test]
    fn a_stored_row_of_scope_names_reads_back_whole() {
        let grants: Grants = serde_json::from_str(r#"["produce","execute"]"#).expect("decode");
        assert_eq!(
            grants,
            Grants::from(ScopeSet::of(&[Scope::Produce, Scope::Execute]))
        );
        assert_eq!(
            serde_json::to_string(&grants).expect("encode"),
            r#"["produce","execute"]"#
        );
        assert_eq!(grants.access(Scope::Produce), Access::Whole);
    }

    #[test]
    fn grants_round_trip_through_json() {
        let grants =
            Grants::parse_all(["read", "produce:queue=emails,task=send_receipt"]).expect("valid");
        let encoded = serde_json::to_string(&grants).expect("encode");
        assert_eq!(
            encoded,
            r#"["read","produce:queue=emails,task=send_receipt"]"#
        );
        let decoded: Grants = serde_json::from_str(&encoded).expect("decode");
        assert_eq!(decoded, grants);
    }

    /// A grant this build cannot read — a newer build's, or a mangled row —
    /// denies what it would have granted and leaves the rest.
    #[test]
    fn an_unreadable_grant_narrows_rather_than_failing() {
        let decoded: Grants =
            serde_json::from_str(r#"["read","teleport","produce:queue=a*b","admin:task=x"]"#)
                .expect("unreadable grants are ignored");
        assert_eq!(decoded, Grants::from(ScopeSet::of(&[Scope::Read])));
    }

    #[test]
    fn parse_all_refuses_the_first_bad_grant() {
        assert!(Grants::parse_all(["produce", "teleport"]).is_err());
        assert!(Grants::parse_all(Vec::<&str>::new())
            .expect("empty parses")
            .is_empty());
    }

    #[test]
    fn access_is_the_union_of_the_grants_behind_a_door() {
        let grants = Grants::parse_all([
            "produce:queue=emails",
            "produce:queue=sms,task=send_code",
            "read:queue=billing",
            "execute",
        ])
        .expect("valid");
        assert!(grants.opens(Scope::Produce) && grants.opens(Scope::Read));
        assert!(!grants.opens(Scope::Admin));

        let produce = grants.access(Scope::Produce);
        assert!(!produce.is_whole());
        assert!(produce.reaches(Some("emails"), Some("anything")));
        assert!(produce.reaches(Some("sms"), Some("send_code")));
        assert!(!produce.reaches(Some("sms"), Some("other")));
        assert!(!produce.reaches(Some("billing"), Some("charge")));

        // A narrowed `produce` reaches the same reads it would whole, and the
        // `read` grants add to it.
        let read = grants.access(Scope::Read);
        assert!(read.reaches(Some("emails"), None));
        assert!(read.reaches(Some("billing"), None));
        assert!(!read.reaches(None, None));

        assert_eq!(grants.access(Scope::Execute), Access::Whole);
        assert_eq!(grants.access(Scope::Admin), Access::Narrowed(Vec::new()));
        assert!(!grants
            .access(Scope::Admin)
            .reaches(Some("emails"), Some("t")));
    }

    #[test]
    fn a_whole_grant_beside_a_narrowed_one_is_whole() {
        let grants = Grants::parse_all(["produce:queue=emails", "produce"]).expect("valid");
        assert_eq!(grants.access(Scope::Produce), Access::Whole);
        assert_eq!(grants.access(Scope::Read), Access::Whole);
    }
}
