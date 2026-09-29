//! Enqueue admission against namespace depth and rate quotas.
//!
//! Called from the `StorageBackend` enqueue forwarders, which every producer
//! door funnels through, so no shell can enforce a quota differently or skip
//! it. Each namespace's share of a call is admitted or refused as a unit: a
//! batch never half-lands because it straddled the cap.
//!
//! Depth is a count taken before the insert, not inside it, so concurrent
//! producers can overshoot `max_pending` by at most their own in-flight
//! batches — the trade `max_pending` on a queue already makes. Depth is
//! checked before the rate bucket so a refusal on depth spends no tokens.
//!
//! Admission runs before dedup and debounce coalescing: an enqueue that would
//! have landed on an existing job still counts, and is still refused, while
//! its namespace is over quota.

use std::collections::BTreeMap;

use super::cache::QuotaCache;
use super::document::{NamespaceQuota, QuotaOverflow};
use super::key::enqueue_rate_key;
use crate::error::{QueueError, Result};
use crate::job::{JobStatus, NewJob};
use crate::scheduler::retention::DEFAULT_NAMESPACE;
use crate::scheduler::shed::QUOTA_REASON_PREFIX;
use crate::storage::Storage;

/// One verdict per job of an enqueue call, in call order: `None` admits it,
/// `Some(reason)` sheds it with that dead-letter reason. A `reject` quota
/// answers with an error instead, refusing the whole call.
pub(crate) fn admit<S: Storage>(
    storage: &S,
    cache: &QuotaCache,
    jobs: &[NewJob],
) -> Result<Vec<Option<String>>> {
    let mut verdicts = vec![None; jobs.len()];
    let mut by_namespace: BTreeMap<Option<&str>, Vec<usize>> = BTreeMap::new();
    for (i, job) in jobs.iter().enumerate() {
        by_namespace
            .entry(job.namespace.as_deref())
            .or_default()
            .push(i);
    }

    for (namespace, positions) in by_namespace {
        let Some(quota) = cache.get(storage, namespace)? else {
            continue;
        };
        let Some(excess) = excess(storage, &quota, namespace, jobs, &positions)? else {
            continue;
        };
        match quota.on_excess {
            QuotaOverflow::Reject => return Err(excess.into_error(namespace)),
            QuotaOverflow::Drop => {
                let reason = excess.shed_reason(namespace);
                for i in positions {
                    verdicts[i] = Some(reason.clone());
                }
            }
        }
    }
    Ok(verdicts)
}

/// Which limit a namespace's share of a call would breach.
enum Excess {
    Depth {
        queue: String,
        pending: i64,
        cap: i64,
    },
    Rate {
        rate: String,
    },
}

impl Excess {
    /// The error a `reject` quota answers with. Depth reuses `QueueFull`, whose
    /// `Display` every SDK already parses; rate reuses `RateLimitExceeded`.
    fn into_error(self, namespace: Option<&str>) -> QueueError {
        match self {
            Self::Depth {
                queue,
                pending,
                cap,
            } => QueueError::QueueFull {
                queue,
                pending,
                cap,
            },
            Self::Rate { rate } => QueueError::RateLimitExceeded(format!(
                "namespace '{}' enqueue_rate {rate}",
                namespace.unwrap_or(DEFAULT_NAMESPACE)
            )),
        }
    }

    /// The dead-letter reason a `drop` quota records.
    fn shed_reason(&self, namespace: Option<&str>) -> String {
        let namespace = namespace.unwrap_or(DEFAULT_NAMESPACE);
        match self {
            Self::Depth { pending, cap, .. } => format!(
                "{QUOTA_REASON_PREFIX} namespace '{namespace}' is at max_pending {cap} ({pending} pending), and its on_excess is drop"
            ),
            Self::Rate { rate } => format!(
                "{QUOTA_REASON_PREFIX} namespace '{namespace}' is over enqueue_rate {rate}, and its on_excess is drop"
            ),
        }
    }
}

/// The limit `positions` would breach, if any. Spends rate tokens only when
/// the depth check passed, and only for an admitted share.
fn excess<S: Storage>(
    storage: &S,
    quota: &NamespaceQuota,
    namespace: Option<&str>,
    jobs: &[NewJob],
    positions: &[usize],
) -> Result<Option<Excess>> {
    let incoming = positions.len() as i64;
    if let Some(cap) = quota.max_pending {
        let pending = storage.count_by_namespace(namespace, JobStatus::Pending)?;
        if pending.saturating_add(incoming) > cap {
            return Ok(Some(Excess::Depth {
                queue: jobs[positions[0]].queue.clone(),
                pending,
                cap,
            }));
        }
    }
    if let (Some(rate), Some(config)) = (&quota.enqueue_rate, quota.enqueue_rate_config()) {
        let count = u32::try_from(positions.len()).unwrap_or(u32::MAX);
        let admitted = storage.try_acquire_tokens(
            &enqueue_rate_key(namespace),
            count,
            config.max_tokens,
            config.refill_rate,
        )?;
        if !admitted {
            return Ok(Some(Excess::Rate { rate: rate.clone() }));
        }
    }
    Ok(None)
}
