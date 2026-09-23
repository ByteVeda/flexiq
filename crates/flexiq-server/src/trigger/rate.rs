//! The per-trigger bucket every accepted request draws from.
//!
//! The bucket lives in storage — `Storage::try_acquire_token`, the one the
//! scheduler's task rate limits use — so the bound holds across every replica
//! serving the same trigger rather than multiplying by the replica count.
//!
//! It is drawn from **after** a request has proved its origin. A bucket
//! charged before the signature check would let anyone on the internet spend
//! a real sender's budget with forged requests, and turn the limit into the
//! denial of service it exists to prevent.

use flexiq_core::{RateLimitConfig, Storage, StorageBackend};

/// The storage key of one trigger's bucket. Namespaced, because two tenants'
/// servers can share one database and each may define a trigger of the same
/// name.
pub fn bucket_key(namespace: &str, trigger: &str) -> String {
    format!("trigger:{namespace}:{trigger}")
}

/// Seconds until the bucket holds another token, for `Retry-After`.
///
/// The bucket does not report its fill, so this is the refill interval of one
/// token — exact for a drained bucket, which is the only kind that refuses.
pub fn retry_after_secs(rate: &RateLimitConfig) -> u64 {
    let interval = (1.0 / rate.refill_rate).ceil();
    // `refill_rate` is at least 1/3600 by `RateLimitConfig::parse`, so this is
    // finite and at most an hour; the clamp only guards the cast.
    interval.clamp(1.0, 3600.0) as u64
}

/// Draw `count` tokens, stopping at the first refusal.
///
/// One per job, so an event batch costs what its jobs cost. Tokens drawn
/// before a refusal stay spent: the request is answered `429` and redelivered,
/// and the redelivery pays again — the limit errs toward admitting less.
pub fn acquire(
    storage: &StorageBackend,
    key: &str,
    rate: &RateLimitConfig,
    count: usize,
) -> flexiq_core::Result<bool> {
    for _ in 0..count {
        if !storage.try_acquire_token(key, rate.max_tokens, rate.refill_rate)? {
            return Ok(false);
        }
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use flexiq_core::SqliteStorage;

    use super::*;

    fn rate(spec: &str) -> RateLimitConfig {
        RateLimitConfig::parse(spec).expect("a valid rate")
    }

    #[test]
    fn retry_after_is_one_refill_interval() {
        assert_eq!(retry_after_secs(&rate("10/s")), 1);
        assert_eq!(retry_after_secs(&rate("30/m")), 2);
        assert_eq!(retry_after_secs(&rate("1/h")), 3600);
    }

    #[test]
    fn keys_are_scoped_by_namespace() {
        assert_ne!(bucket_key("a", "orders"), bucket_key("b", "orders"));
    }

    #[test]
    fn a_drained_bucket_refuses() {
        let storage = StorageBackend::Sqlite(SqliteStorage::in_memory().expect("storage"));
        let limit = rate("2/h");
        assert!(acquire(&storage, "k", &limit, 2).expect("acquired"));
        assert!(!acquire(&storage, "k", &limit, 1).expect("refused"));
    }
}
