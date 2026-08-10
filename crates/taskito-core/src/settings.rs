//! Reserved settings-key prefixes — the settings namespaces the runtime owns.
//!
//! The settings KV doubles as the store for auth state, webhook subscriptions,
//! and the retention windows a cleanup leader publishes. None of those belong on
//! a dashboard's generic key/value surface: reading them leaks credentials and
//! writing them spoofs a published policy. The canonical list lives here so that
//! every binding hides the same keys instead of re-deriving it — a prefix missed
//! in one shell reopens the hole for all of them.

/// Key prefixes a shell's generic settings API must treat as absent — never
/// listed, read, written, or deleted through it. The runtime's own readers and
/// writers use the [`Storage`](crate::storage::Storage) settings methods, which
/// stay unrestricted.
pub const RESERVED_SETTING_PREFIXES: &[&str] = &[
    "auth:",                // dashboard sessions, OAuth state, API tokens
    "contract:",            // the contract floor a process is checked against
    "middleware:disabled:", // per-task middleware disable lists
    "retention:",           // the windows a cleanup leader publishes
    "taskito.webhooks",     // webhook store
    "webhook:",             // webhook store
    "webhooks:",            // webhook subscriptions and delivery log
];

/// Whether `key` falls under a reserved prefix.
pub fn is_reserved_setting_key(key: &str) -> bool {
    RESERVED_SETTING_PREFIXES
        .iter()
        .any(|prefix| key.starts_with(prefix))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scheduler::retention::retention_setting_key;

    #[test]
    fn runtime_owned_namespaces_are_reserved() {
        assert!(is_reserved_setting_key(&retention_setting_key(None)));
        assert!(is_reserved_setting_key(&retention_setting_key(Some(
            "billing"
        ))));
        assert!(is_reserved_setting_key("auth:session:abc"));
        assert!(is_reserved_setting_key(
            crate::contract::CONTRACT_FLOOR_SETTING
        ));
        assert!(is_reserved_setting_key("webhooks:subscriptions"));
    }

    #[test]
    fn ordinary_keys_are_not() {
        assert!(!is_reserved_setting_key("dashboard.theme"));
        assert!(!is_reserved_setting_key("retentio"));
        assert!(!is_reserved_setting_key("authors"));
    }
}
