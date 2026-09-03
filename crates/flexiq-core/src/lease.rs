//! The lease on one dispatch of one job.
//!
//! `ExecutorMessage::Success` and its siblings name a job and nothing else. An
//! executor that stalls past the reaper's patience has its job requeued and
//! handed to someone else — and then finishes, and writes its result over the
//! new owner's. Nothing in the frame can tell the two apart.
//!
//! A lease is the identity of the execution claim a job was dispatched under.
//! It is minted by *storage*, in the same statement that wins the claim, and it
//! is opaque to the executor: the value is a random non-negative `i64` rendered
//! base64url, so there is no structure in it to read and nothing in it to
//! guess. The executor's only job is to hand it back.
//!
//! # Why the claim, and not `(owner, attempt)`
//!
//! `Storage::authorize_attempt` already fences a result on the claim's owner
//! and the job's `retry_count`, and between them those cover a reclaim (the
//! owner moves) and a reap (the retry bumps the attempt). They do not cover
//! [`Storage::requeue_stuck`](crate::storage::Storage::requeue_stuck) — the
//! dashboard's requeue button — which returns the job to `Pending` and deletes
//! the claim without touching `retry_count`. The next dispatch is then
//! *indistinguishable* from the stalled one: same job, same owner, same
//! attempt. The epoch is what separates them, because a claim is never won
//! twice under the same one.
//!
//! # Where it is checked
//!
//! Twice, against the same value:
//!
//! - **In memory, at the dispatcher.** [`LeaseBook`] holds the lease of each
//!   job's *current* dispatch. A frame whose lease **disagrees** with the
//!   book's entry is refused before it can become a
//!   [`JobResult`](crate::scheduler::JobResult).
//! - **Durably, at the fence.** `authorize_attempt` compares the epoch to the
//!   `execution_claims` row, so a result that outlives the book, or that
//!   reaches a different process, is still superseded.
//!
//! The dispatcher's check is a *disagreement* test, not a membership one, and
//! deliberately: a job with no entry is one that was never dispatched under a
//! lease, or one whose dispatch has already settled. Refusing those would fail
//! every result from a pool that holds no book — and the second case is exactly
//! where the fence still has an answer, because the scheduler retires the
//! dispatch record rather than forgetting it.

use std::collections::HashMap;
use std::sync::Mutex;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use serde::{Deserialize, Serialize};

/// Mint the epoch a fresh execution claim is recorded under.
///
/// Random rather than a counter or a timestamp: the comparison is equality, so
/// nothing needs ordering, and two claims of one job id inside the same
/// millisecond must not be able to collide. Non-negative so every backend can
/// hold it — the Redis claim value encodes it as a decimal suffix its Lua
/// pattern matches with `%d+`.
pub fn mint_claim_epoch() -> i64 {
    (rand::random::<u64>() >> 1) as i64
}

/// Whether a claim's epoch and a result's epoch may belong to the same
/// dispatch.
///
/// Compared only when both sides have one. A `None` on the claim is a row
/// written before the column existed; a `None` on the caller is a dispatch made
/// without a lease — a worker pool that was never handed a
/// [`LeaseBook`], or a peer that did not negotiate the capability. Neither is
/// evidence that the dispatch is stale, and treating an absent epoch as a
/// mismatch would fail every one of those results instead.
///
/// The gap this leaves is stated rather than hidden: an executor that carries
/// no lease is fenced on `(owner, attempt)` alone, exactly as it was before
/// this existed.
pub fn epochs_agree(claim: Option<i64>, result: Option<i64>) -> bool {
    match (claim, result) {
        (Some(claim), Some(result)) => claim == result,
        _ => true,
    }
}

/// A lease on one dispatch of one job, as it travels the wire.
///
/// Opaque by construction: the wire form is base64url of the claim epoch's
/// eight big-endian bytes, and the epoch is random, so a holder can read
/// nothing from it and predict no other one. An executor treats it as bytes to
/// echo back.
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Lease(String);

impl Lease {
    /// Render the lease for the claim recorded under `epoch`.
    pub fn from_epoch(epoch: i64) -> Self {
        Self(URL_SAFE_NO_PAD.encode(epoch.to_be_bytes()))
    }

    /// The epoch this lease names, or `None` when the value did not come from
    /// [`Lease::from_epoch`].
    ///
    /// A peer can only ever echo a lease back, so a value that does not decode
    /// is a broken or hostile sender — it resolves to no epoch, which every
    /// caller reads as "this is not the current dispatch".
    pub fn epoch(&self) -> Option<i64> {
        let bytes = URL_SAFE_NO_PAD.decode(&self.0).ok()?;
        let bytes: [u8; 8] = bytes.try_into().ok()?;
        Some(i64::from_be_bytes(bytes))
    }

    /// The token as bytes, for a transport that carries it as an opaque `bytes`
    /// field instead of the JSON string the frame protocol uses.
    ///
    /// The same token either way. A wire that re-derived it — carrying the
    /// epoch's eight bytes, say — would be a second encoding of one value, free
    /// to disagree with the first.
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }

    /// Read a lease back off such a transport.
    ///
    /// `None` when the bytes are not a token this scheduler could have minted,
    /// which every caller already reads the way it reads a value that does not
    /// decode: not the current dispatch, so the frame is refused.
    pub fn from_wire(bytes: &[u8]) -> Option<Self> {
        std::str::from_utf8(bytes)
            .ok()
            .map(|token| Self(token.to_string()))
    }
}

/// Redacted, like [`Secret`](crate::worker::Secret): a lease is what authorizes
/// a completion, so a log line carrying one hands the next reader the ability
/// to settle someone else's job.
impl std::fmt::Debug for Lease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Lease(<redacted>)")
    }
}

/// Which lease each job's *current* dispatch was handed.
///
/// Written by the scheduler as it dispatches and read by the worker pool as it
/// builds the job frame, because those are two objects that share no channel
/// but do share a process — the same shape
/// [`WorkerDispatcher::set_claim_owner`](crate::worker::WorkerDispatcher::set_claim_owner)
/// already uses to tell a pool the owner it may not name for itself.
///
/// A pool that never receives one dispatches without a lease and is fenced on
/// `(owner, attempt)` alone, exactly as before this existed.
#[derive(Debug, Default)]
pub struct LeaseBook {
    current: Mutex<HashMap<String, Lease>>,
}

impl LeaseBook {
    /// Record `lease` as the lease of `job_id`'s current dispatch, replacing
    /// any earlier one — which is precisely what makes the earlier dispatch's
    /// copy stale.
    pub fn issue(&self, job_id: &str, lease: Lease) {
        self.lock().insert(job_id.to_string(), lease);
    }

    /// The lease of `job_id`'s current dispatch, if it has one.
    pub fn current(&self, job_id: &str) -> Option<Lease> {
        self.lock().get(job_id).cloned()
    }

    /// Whether `lease` is the one `job_id` is currently dispatched under.
    ///
    /// Strict: `false` for a job with no entry. A dispatcher deciding whether
    /// to *refuse* a frame asks the looser question — see the module docs — but
    /// this is the one worth asserting on.
    pub fn is_current(&self, job_id: &str, lease: &Lease) -> bool {
        self.lock().get(job_id).is_some_and(|held| held == lease)
    }

    /// Forget `job_id`'s dispatch once it has been settled.
    ///
    /// Guarded by the lease being retired, so a straggler retiring *its* copy
    /// cannot evict the entry a newer dispatch just wrote.
    pub fn retire(&self, job_id: &str, lease: &Lease) {
        let mut current = self.lock();
        if current.get(job_id).is_some_and(|held| held == lease) {
            current.remove(job_id);
        }
    }

    /// Forget `job_id` outright — the rollback path, where the dispatch that
    /// wrote the entry never reached a pool.
    pub fn forget(&self, job_id: &str) {
        self.lock().remove(job_id);
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Lease>> {
        self.current
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_lease_round_trips_through_its_wire_form() {
        for epoch in [0, 1, i64::MAX, mint_claim_epoch()] {
            let lease = Lease::from_epoch(epoch);
            assert_eq!(lease.epoch(), Some(epoch), "epoch {epoch} did not survive");
        }
    }

    #[test]
    fn a_lease_that_did_not_come_from_an_epoch_resolves_to_none() {
        // Every caller reads `None` as "not the current dispatch", so a sender
        // cannot buy authority by inventing a value.
        assert_eq!(Lease("not base64".to_string()).epoch(), None);
        assert_eq!(Lease(URL_SAFE_NO_PAD.encode([0u8; 4])).epoch(), None);
    }

    #[test]
    fn a_lease_survives_a_transport_that_carries_it_as_bytes() {
        // The bytes are the token, not a re-encoding of the epoch: a wire that
        // derived its own form would be a second encoding of one value.
        let lease = Lease::from_epoch(mint_claim_epoch());
        let back = Lease::from_wire(lease.as_bytes()).expect("the token is its own bytes");
        assert_eq!(back, lease);
        assert_eq!(back.epoch(), lease.epoch());
    }

    #[test]
    fn bytes_that_are_not_a_token_resolve_to_no_lease() {
        // Refused rather than accepted-and-ignored: a peer echoing garbage is
        // not the current dispatch, which is what the caller reads `None` as.
        assert!(Lease::from_wire(&[0xff, 0xfe]).is_none());
        assert_eq!(
            Lease::from_wire(b"not a lease").and_then(|lease| lease.epoch()),
            None
        );
    }

    #[test]
    fn a_lease_never_prints_itself() {
        let printed = format!("{:?}", Lease::from_epoch(42));
        assert!(
            !printed.contains("Kg"),
            "{printed} leaked the encoded epoch"
        );
    }

    #[test]
    fn the_book_answers_only_for_the_current_dispatch() {
        let book = LeaseBook::default();
        let first = Lease::from_epoch(1);
        let second = Lease::from_epoch(2);

        book.issue("job-1", first.clone());
        assert!(book.is_current("job-1", &first));

        // The redispatch is what makes the first one stale.
        book.issue("job-1", second.clone());
        assert!(!book.is_current("job-1", &first));
        assert!(book.is_current("job-1", &second));

        // A straggler retiring its own copy must not evict the live entry.
        book.retire("job-1", &first);
        assert!(book.is_current("job-1", &second));

        book.retire("job-1", &second);
        assert!(!book.is_current("job-1", &second));
        assert_eq!(book.current("job-1"), None);
    }

    #[test]
    fn a_job_with_no_entry_is_not_current() {
        let book = LeaseBook::default();
        assert!(!book.is_current("job-1", &Lease::from_epoch(1)));
    }
}
