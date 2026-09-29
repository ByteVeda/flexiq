//! Settings keys of quota documents, and the rate-limit bucket keys they drive.
//!
//! - default namespace: `quota:default`;
//! - namespace `N`: `quota:ns:<len>:<N>`.
//!
//! `<len>` is `N`'s length in bytes, the scheme [`crate::overrides`] uses, so a
//! `:` inside `N` cannot make two namespaces share a key.

/// Prefix every quota key shares.
pub const QUOTA_SETTING_PREFIX: &str = "quota:";

const DEFAULT_KEY: &str = "quota:default";
const NAMESPACED_PREFIX: &str = "quota:ns:";

/// The settings key of `namespace`'s quota.
pub fn quota_key(namespace: Option<&str>) -> String {
    match namespace {
        None => DEFAULT_KEY.to_string(),
        Some(ns) => format!("{NAMESPACED_PREFIX}{}:{ns}", ns.len()),
    }
}

/// The namespace a quota key belongs to — the inverse of [`quota_key`].
/// `None` for a key that is not a well-formed quota key; `Some(None)` for the
/// default namespace.
pub fn namespace_of_quota_key(key: &str) -> Option<Option<String>> {
    if key == DEFAULT_KEY {
        return Some(None);
    }
    let rest = key.strip_prefix(NAMESPACED_PREFIX)?;
    let (len, ns) = rest.split_once(':')?;
    let len: usize = len.parse().ok()?;
    (ns.len() == len).then(|| Some(ns.to_string()))
}

/// The token bucket `namespace`'s `enqueue_rate` draws from. Length-prefixed
/// like the settings key, so two namespaces never share a bucket.
pub(crate) fn enqueue_rate_key(namespace: Option<&str>) -> String {
    match namespace {
        None => "quota::-::enqueue".to_string(),
        Some(ns) => format!("quota::{}:{ns}::enqueue", ns.len()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_match_the_cross_sdk_vectors() {
        assert_eq!(quota_key(None), "quota:default");
        assert_eq!(quota_key(Some("billing")), "quota:ns:7:billing");
        assert_eq!(quota_key(Some("a:b")), "quota:ns:3:a:b");
        assert_eq!(quota_key(Some("")), "quota:ns:0:");
    }

    #[test]
    fn a_key_parses_back_to_its_namespace() {
        for ns in [
            None,
            Some("billing"),
            Some("a:b"),
            Some(""),
            Some("default"),
        ] {
            assert_eq!(
                namespace_of_quota_key(&quota_key(ns)),
                Some(ns.map(String::from))
            );
        }
        assert_eq!(namespace_of_quota_key("quota:ns:9:billing"), None);
        assert_eq!(namespace_of_quota_key("quota:other"), None);
        assert_eq!(namespace_of_quota_key("overrides:task:x"), None);
    }

    #[test]
    fn every_quota_key_is_reserved() {
        for ns in [None, Some("billing")] {
            assert!(crate::settings::is_reserved_setting_key(&quota_key(ns)));
        }
    }

    #[test]
    fn rate_buckets_are_per_namespace() {
        assert_ne!(enqueue_rate_key(None), enqueue_rate_key(Some("-")));
        assert_ne!(enqueue_rate_key(Some("a")), enqueue_rate_key(Some("b")));
    }
}
