//! Settings keys for task and queue runtime overrides (cross-SDK).
//!
//! An override is a JSON document in the settings KV, written by a dashboard or
//! the admin door and read by a worker at startup. The KV is one keyspace for
//! the whole database, so the key has to carry the namespace or two tenants
//! with a task of the same name share one override (#836).
//!
//! - default namespace: `overrides:task:<name>` / `overrides:queue:<name>` —
//!   the layout every release before #836 wrote, so existing rows keep
//!   meaning what they meant;
//! - namespace `N`: `overrides:ns:<len>:<N>:task:<name>` / `…:queue:<name>`.
//!
//! `<len>` is `N`'s length in bytes, so a `:` inside `N` cannot make two
//! namespaces share a prefix. The `ns:` segment keeps the two layouts disjoint:
//! a default-namespace listing strips `overrides:task:` and everything left is
//! a name, whatever that name contains. Every shell builds these keys itself,
//! and each is tested against the vectors this module's tests pin.

/// Which subject an override applies to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverrideScope {
    /// One task.
    Task,
    /// One queue.
    Queue,
}

impl OverrideScope {
    fn segment(self) -> &'static str {
        match self {
            Self::Task => "task",
            Self::Queue => "queue",
        }
    }
}

/// The key prefix every override in `scope` and `namespace` shares. Strip it
/// from a key to get the subject's name.
pub fn override_prefix(scope: OverrideScope, namespace: Option<&str>) -> String {
    match namespace {
        None => format!("overrides:{}:", scope.segment()),
        Some(ns) => format!("overrides:ns:{}:{ns}:{}:", ns.len(), scope.segment()),
    }
}

/// The settings key of one subject's override.
pub fn override_key(scope: OverrideScope, namespace: Option<&str>, name: &str) -> String {
    format!("{}{name}", override_prefix(scope, namespace))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The vectors every shell's own key builder is tested against.
    #[test]
    fn keys_match_the_cross_sdk_vectors() {
        use OverrideScope::{Queue, Task};
        assert_eq!(override_key(Task, None, "send"), "overrides:task:send");
        assert_eq!(
            override_key(Queue, None, "emails"),
            "overrides:queue:emails"
        );
        assert_eq!(
            override_key(Task, Some("billing"), "send"),
            "overrides:ns:7:billing:task:send"
        );
        assert_eq!(
            override_key(Queue, Some("a:b"), "emails"),
            "overrides:ns:3:a:b:queue:emails"
        );
    }

    /// No namespaced key falls under the default prefix, whatever the names.
    #[test]
    fn the_layouts_are_disjoint() {
        for scope in [OverrideScope::Task, OverrideScope::Queue] {
            let default = override_prefix(scope, None);
            for ns in ["task", "queue", "", "x:task:"] {
                let key = override_key(scope, Some(ns), "n");
                assert!(!key.starts_with(&default), "{key} under {default}");
            }
        }
        assert_ne!(
            override_prefix(OverrideScope::Task, Some("")),
            override_prefix(OverrideScope::Task, None)
        );
    }
}
