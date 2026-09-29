//! Per-namespace quotas (#841): the limits one tenant is held to.
//!
//! A quota is one JSON document per namespace in the settings KV, so it lives
//! where every other runtime policy lives and the admin door can read and write
//! it without a schema of its own. Every field is optional and an absent field
//! is unlimited; an absent document is a namespace with no quota at all.
//!
//! - default namespace: `quota:default`;
//! - namespace `N`: `quota:ns:<len>:<N>`.
//!
//! `<len>` is `N`'s length in bytes, the scheme [`crate::overrides`] uses, so a
//! `:` inside `N` cannot make two namespaces share a key. `quota:` is a
//! [reserved prefix](crate::settings::RESERVED_SETTING_PREFIXES): a tenant's
//! generic settings surface must never be able to raise its own limit.

use serde::{Deserialize, Serialize};

use crate::error::{QueueError, Result};
use crate::resilience::rate_limiter::RateLimitConfig;

/// Prefix every quota key shares.
pub const QUOTA_SETTING_PREFIX: &str = "quota:";

const DEFAULT_KEY: &str = "quota:default";
const NAMESPACED_PREFIX: &str = "quota:ns:";

/// What an enqueue over a namespace's depth or rate quota does.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum QuotaOverflow {
    /// Refuse the enqueue with an error the producer sees.
    #[default]
    Reject,
    /// Accept the call but dead-letter the jobs as shed; they never go live.
    Drop,
}

impl QuotaOverflow {
    /// Parse the wire name (`reject` / `drop`).
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "reject" => Some(Self::Reject),
            "drop" => Some(Self::Drop),
            _ => None,
        }
    }

    /// The wire name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Reject => "reject",
            Self::Drop => "drop",
        }
    }
}

/// The limits one namespace is held to. `None` is unlimited.
///
/// Unknown fields are ignored rather than refused: during a rolling upgrade an
/// older process reads a document a newer one wrote, and refusing it would fail
/// every enqueue closed over a field the older process cannot enforce anyway.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct NamespaceQuota {
    /// Maximum pending jobs (delayed ones included).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_pending: Option<i64>,
    /// What an enqueue over `max_pending` or `enqueue_rate` does.
    #[serde(default)]
    pub on_excess: QuotaOverflow,
    /// Enqueues per interval, as a rate string (`"500/s"`, `"10000/h"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enqueue_rate: Option<String>,
    /// Maximum jobs running at once, gated at dispatch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_running: Option<i64>,
    /// Row ceiling on the archive; the retention leader trims oldest-first.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_archived_rows: Option<i64>,
    /// Row ceiling on the dead-letter queue; trimmed oldest-first.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_dead_rows: Option<i64>,
}

impl NamespaceQuota {
    /// Decode and validate a stored document.
    pub fn from_json(raw: &str) -> Result<Self> {
        let quota: Self = serde_json::from_str(raw)
            .map_err(|e| QueueError::Config(format!("invalid namespace quota: {e}")))?;
        quota.validate()?;
        Ok(quota)
    }

    /// Encode for storage. Validates first, so nothing unreadable is written.
    pub fn to_json(&self) -> Result<String> {
        self.validate()?;
        Ok(serde_json::to_string(self)?)
    }

    /// Refuse a negative cap or an unparseable rate. Zero is a real limit — a
    /// frozen tenant — not a synonym for unlimited.
    pub fn validate(&self) -> Result<()> {
        for (field, value) in [
            ("max_pending", self.max_pending),
            ("max_running", self.max_running),
            ("max_archived_rows", self.max_archived_rows),
            ("max_dead_rows", self.max_dead_rows),
        ] {
            if value.is_some_and(|v| v < 0) {
                return Err(QueueError::Config(format!(
                    "namespace quota {field} must not be negative"
                )));
            }
        }
        if let Some(rate) = &self.enqueue_rate {
            if RateLimitConfig::parse(rate).is_none() {
                return Err(QueueError::Config(format!(
                    "namespace quota enqueue_rate {rate:?} is not a rate like \"100/s\""
                )));
            }
        }
        Ok(())
    }

    /// The parsed enqueue rate, if one is set. `validate` already ran on every
    /// decoded document, so a set rate always parses.
    pub fn enqueue_rate_config(&self) -> Option<RateLimitConfig> {
        self.enqueue_rate
            .as_deref()
            .and_then(RateLimitConfig::parse)
    }

    /// Whether any limit is set at all.
    pub fn is_unlimited(&self) -> bool {
        self.max_pending.is_none()
            && self.enqueue_rate.is_none()
            && self.max_running.is_none()
            && self.max_archived_rows.is_none()
            && self.max_dead_rows.is_none()
    }
}

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
    fn an_empty_document_is_unlimited() {
        let quota = NamespaceQuota::from_json("{}").unwrap();
        assert!(quota.is_unlimited());
        assert_eq!(quota.on_excess, QuotaOverflow::Reject);
    }

    #[test]
    fn a_document_round_trips() {
        let quota = NamespaceQuota {
            max_pending: Some(10),
            on_excess: QuotaOverflow::Drop,
            enqueue_rate: Some("5/s".into()),
            max_running: Some(0),
            max_archived_rows: Some(100),
            max_dead_rows: None,
        };
        let json = quota.to_json().unwrap();
        assert_eq!(NamespaceQuota::from_json(&json).unwrap(), quota);
        assert!(json.contains(r#""on_excess":"drop""#), "{json}");
        assert!(!json.contains("max_dead_rows"), "{json}");
    }

    #[test]
    fn unknown_fields_are_ignored() {
        let quota = NamespaceQuota::from_json(r#"{"max_pending":3,"max_bytes":9}"#).unwrap();
        assert_eq!(quota.max_pending, Some(3));
    }

    #[test]
    fn invalid_documents_are_refused() {
        for raw in [
            r#"{"max_pending":-1}"#,
            r#"{"max_running":-5}"#,
            r#"{"enqueue_rate":"0/s"}"#,
            r#"{"enqueue_rate":"lots"}"#,
            r#"{"on_excess":"defer"}"#,
            r#"{"max_pending":"ten"}"#,
            "not json",
        ] {
            assert!(NamespaceQuota::from_json(raw).is_err(), "{raw} accepted");
        }
    }

    #[test]
    fn overflow_wire_names_round_trip() {
        for mode in [QuotaOverflow::Reject, QuotaOverflow::Drop] {
            assert_eq!(QuotaOverflow::parse(mode.as_str()), Some(mode));
        }
        assert_eq!(QuotaOverflow::parse("defer"), None);
    }
}
