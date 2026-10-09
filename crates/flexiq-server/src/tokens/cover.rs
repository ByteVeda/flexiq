//! Whether one set of grants covers another: the no-escalation rule a token
//! minting a token is held to.
//!
//! A grant covers another when it is the same scope and every queue and task
//! the other reaches, it reaches too. The scope must be the *same* one, not
//! merely one that opens the other's door: `produce` opens the reads `read`
//! guards, but an implication kept for old rows is not a grant to hand on.
//!
//! Each requested grant must be covered by **one** held grant. Two narrowed
//! grants whose union happens to reach what a wider one would are not combined:
//! the rule stays checkable by eye, and refusing a union is the safe direction.

use super::grant::{Grant, Grants, Pattern};

impl Pattern {
    /// Whether every name `asked` reaches, this pattern reaches too.
    ///
    /// `*` covers everything; a prefix covers a longer prefix and any name it
    /// starts; a name covers only itself.
    pub fn covers(&self, asked: &Self) -> bool {
        match (self, asked) {
            (Self::Any, _) => true,
            (_, Self::Any) => false,
            (Self::Prefix(held), Self::Prefix(asked) | Self::Exact(asked)) => {
                asked.starts_with(held.as_str())
            }
            (Self::Exact(held), Self::Exact(asked)) => held == asked,
            (Self::Exact(_), Self::Prefix(_)) => false,
        }
    }
}

impl Grant {
    /// Whether this grant reaches everything `asked` does, on the same scope.
    pub fn covers(&self, asked: &Self) -> bool {
        self.scope == asked.scope
            && self.queue.covers(&asked.queue)
            && self.task.covers(&asked.task)
    }
}

impl Grants {
    /// Whether one of these grants covers `asked` on its own.
    pub fn covers(&self, asked: &Grant) -> bool {
        self.iter().any(|held| held.covers(asked))
    }

    /// The first of `asked` that none of these grants covers, or `None` when
    /// every one is — which is when a holder of these may hand `asked` on.
    pub fn first_uncovered(&self, asked: &Self) -> Option<Grant> {
        asked.iter().find(|grant| !self.covers(grant))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pattern(raw: &str) -> Pattern {
        Pattern::parse(raw).unwrap_or_else(|error| panic!("{raw}: {error}"))
    }

    fn grants(spelled: &[&str]) -> Grants {
        Grants::parse_all(spelled.iter().copied()).expect("valid grants")
    }

    #[test]
    fn a_pattern_covers_only_what_it_reaches() {
        // (held, asked, covers)
        let table = [
            ("*", "*", true),
            ("*", "emails", true),
            ("*", "emails-*", true),
            ("emails-*", "*", false),
            ("emails", "*", false),
            ("emails-*", "emails-*", true),
            ("emails-*", "emails-eu-*", true),
            ("emails-*", "emails-eu", true),
            ("emails-*", "emails-", true),
            ("emails-eu-*", "emails-*", false),
            ("emails-*", "emails", false),
            ("emails-*", "billing", false),
            ("emails-*", "billing-*", false),
            ("emails", "emails", true),
            ("emails", "emails-eu", false),
            ("emails", "emails*", false),
            ("emails", "billing", false),
            ("e*", "emails", true),
        ];
        for (held, asked, want) in table {
            assert_eq!(
                pattern(held).covers(&pattern(asked)),
                want,
                "{held} covers {asked}"
            );
        }
    }

    #[test]
    fn a_grant_covers_the_same_scope_on_a_narrower_reach() {
        // (held, asked, covers)
        let table = [
            ("produce", "produce", true),
            ("produce", "produce:queue=emails", true),
            ("produce", "produce:queue=emails-*,task=send", true),
            ("produce:queue=emails-*", "produce:queue=emails-eu", true),
            (
                "produce:queue=emails-*",
                "produce:queue=emails-eu,task=x",
                true,
            ),
            ("produce:queue=emails-*", "produce", false),
            ("produce:queue=emails-*", "produce:task=send", false),
            ("produce:queue=emails", "produce:queue=emails-*", false),
            ("produce:task=send", "produce:queue=emails,task=send", true),
            ("produce:task=send", "produce:queue=emails", false),
            ("execute:queue=a,task=b", "execute:queue=a,task=b", true),
            ("execute:queue=a,task=b", "execute:queue=a,task=c", false),
            // Another scope never covers, implied or not.
            ("produce", "read", false),
            ("produce", "read:queue=emails", false),
            ("admin", "inspect", false),
            ("admin", "tokens", false),
            ("inspect", "admin", false),
            ("tokens", "tokens", true),
            ("tokens", "admin", false),
        ];
        for (held, asked, want) in table {
            let held = Grant::parse(held).expect("held parses");
            let asked = Grant::parse(asked).expect("asked parses");
            assert_eq!(held.covers(&asked), want, "{held} covers {asked}");
        }
    }

    #[test]
    fn every_requested_grant_needs_a_covering_one() {
        let held = grants(&["produce:queue=emails-*", "read", "tokens"]);
        assert_eq!(
            held.first_uncovered(&grants(&["produce:queue=emails-eu", "read", "tokens"])),
            None
        );
        assert_eq!(
            held.first_uncovered(&grants(&["read", "produce"])),
            Some(Grant::whole(crate::tokens::Scope::Produce))
        );
        assert!(held
            .first_uncovered(&grants(&["execute"]))
            .is_some_and(|grant| grant.to_string() == "execute"));
        assert!(held
            .first_uncovered(&grants(&["produce:queue=billing"]))
            .is_some_and(|grant| grant.to_string() == "produce:queue=billing"));
    }

    /// Two narrowed grants are not merged into the wider one they would make.
    #[test]
    fn a_union_of_narrow_grants_does_not_cover_a_wider_one() {
        let held = grants(&["produce:queue=a,task=t", "produce:queue=b,task=t"]);
        assert!(!held.covers(&Grant::parse("produce:task=t").expect("parses")));
        assert!(held.covers(&Grant::parse("produce:queue=a,task=t").expect("parses")));
    }

    #[test]
    fn nothing_requested_is_covered_by_anything() {
        assert_eq!(Grants::default().first_uncovered(&Grants::default()), None);
        assert!(Grants::default()
            .first_uncovered(&grants(&["read"]))
            .is_some());
    }
}
