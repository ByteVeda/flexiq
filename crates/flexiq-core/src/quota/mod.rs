//! Per-namespace quotas (#841): the limits one tenant is held to.
//!
//! A quota is one JSON document per namespace in the settings KV, so it lives
//! where every other runtime policy lives and the admin door can read and write
//! it without a schema of its own. Every field is optional and an absent field
//! is unlimited; an absent document is a namespace with no quota at all.
//!
//! `quota:` is a [reserved prefix](crate::settings::RESERVED_SETTING_PREFIXES):
//! a tenant's generic settings surface must never be able to raise its own
//! limit. Enforcement is the core's — depth and rate at enqueue
//! (`admission`), concurrency at dispatch, row ceilings in the retention
//! sweep — so no shell enforces a quota its own way.

pub(crate) mod admission;
mod cache;
mod document;
mod key;

pub use cache::{read_quota, QuotaCache, QUOTA_CACHE_TTL};
pub use document::{NamespaceQuota, QuotaOverflow};
pub use key::{namespace_of_quota_key, quota_key, QUOTA_SETTING_PREFIX};
