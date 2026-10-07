//! Folding repeated reads into one record (#993).
//!
//! Reads are dominated by polling: a client checking one job every two
//! seconds would leave thirty records a minute that all say the same thing.
//! What the trail is asked about a read is *which* things a token looked at
//! and *when*, so a read that repeats one already recorded — same token, same
//! RPC, same target, same outcome — inside the window is not recorded again.
//!
//! The outcome is part of the key: a refused look and a granted one are
//! different facts. The table is per process, so replicas each record their
//! first sight of a read; the trail can over-count across replicas, never
//! under-count within one.

use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};

use flexiq_core::AuditRecord;

/// Distinct reads remembered at once. Past this the expired ones are swept,
/// and if the table is still full a read is recorded rather than suppressed:
/// a trail that runs long is better than one with a hole in it.
pub const CAPACITY: usize = 65_536;

/// What makes two reads the same read.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Key {
    namespace: String,
    token_id: String,
    operation: String,
    target_kind: Option<String>,
    target: Option<String>,
    outcome: String,
}

impl Key {
    fn of(record: &AuditRecord) -> Self {
        Self {
            namespace: record.namespace.clone(),
            token_id: record.token_id.clone(),
            operation: record.operation.clone(),
            target_kind: record.target_kind.clone(),
            target: record.target.clone(),
            outcome: record.outcome.clone(),
        }
    }
}

/// Which reads have been recorded within the window.
#[derive(Debug)]
pub struct ReadDedup {
    window: Duration,
    capacity: usize,
    seen: Mutex<HashMap<Key, Instant>>,
}

impl ReadDedup {
    /// Fold repeats within `window`. A zero window records every read.
    pub fn new(window: Duration) -> Self {
        Self::with_capacity(window, CAPACITY)
    }

    fn with_capacity(window: Duration, capacity: usize) -> Self {
        Self {
            window,
            capacity,
            seen: Mutex::new(HashMap::new()),
        }
    }

    /// Whether `record` should be stored: it is not a repeat of one stored
    /// within the window.
    pub fn admit(&self, record: &AuditRecord) -> bool {
        self.admit_at(record, Instant::now())
    }

    fn admit_at(&self, record: &AuditRecord, now: Instant) -> bool {
        if self.window.is_zero() {
            return true;
        }
        // A poisoned table only lost a timestamp mid-write; stepping over it
        // at worst records a read twice.
        let mut seen = self.seen.lock().unwrap_or_else(PoisonError::into_inner);
        let key = Key::of(record);
        if let Some(at) = seen.get(&key) {
            if now.saturating_duration_since(*at) < self.window {
                return false;
            }
        }
        if seen.len() >= self.capacity && !seen.contains_key(&key) {
            let window = self.window;
            seen.retain(|_, at| now.saturating_duration_since(*at) < window);
            if seen.len() >= self.capacity {
                return true;
            }
        }
        seen.insert(key, now);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINUTE: Duration = Duration::from_secs(60);

    fn read(token: &str, target: &str, outcome: &str) -> AuditRecord {
        AuditRecord {
            id: uuid::Uuid::now_v7().to_string(),
            namespace: "prod".to_string(),
            at_ms: 1,
            token_id: token.to_string(),
            principal: "ci".to_string(),
            operation: "flexiq.v1.ProducerService/GetJob".to_string(),
            target_kind: Some("job".to_string()),
            target: Some(target.to_string()),
            outcome: outcome.to_string(),
        }
    }

    #[test]
    fn a_repeat_within_the_window_is_folded() {
        let dedup = ReadDedup::new(MINUTE);
        let now = Instant::now();
        assert!(dedup.admit_at(&read("tok", "j1", "OK"), now));
        assert!(!dedup.admit_at(&read("tok", "j1", "OK"), now + MINUTE / 2));
    }

    #[test]
    fn a_repeat_after_the_window_is_recorded_again() {
        let dedup = ReadDedup::new(MINUTE);
        let now = Instant::now();
        assert!(dedup.admit_at(&read("tok", "j1", "OK"), now));
        assert!(dedup.admit_at(&read("tok", "j1", "OK"), now + MINUTE));
    }

    #[test]
    fn another_token_target_or_outcome_is_another_read() {
        let dedup = ReadDedup::new(MINUTE);
        let now = Instant::now();
        assert!(dedup.admit_at(&read("tok", "j1", "OK"), now));
        assert!(dedup.admit_at(&read("other", "j1", "OK"), now));
        assert!(dedup.admit_at(&read("tok", "j2", "OK"), now));
        assert!(dedup.admit_at(&read("tok", "j1", "NOT_FOUND"), now));
    }

    #[test]
    fn a_zero_window_records_every_read() {
        let dedup = ReadDedup::new(Duration::ZERO);
        let now = Instant::now();
        assert!(dedup.admit_at(&read("tok", "j1", "OK"), now));
        assert!(dedup.admit_at(&read("tok", "j1", "OK"), now));
    }

    #[test]
    fn a_full_table_sweeps_expired_reads_first() {
        let dedup = ReadDedup::with_capacity(MINUTE, 2);
        let now = Instant::now();
        assert!(dedup.admit_at(&read("tok", "j1", "OK"), now));
        assert!(dedup.admit_at(&read("tok", "j2", "OK"), now));
        let later = now + MINUTE;
        assert!(dedup.admit_at(&read("tok", "j3", "OK"), later));
        assert!(
            !dedup.admit_at(&read("tok", "j3", "OK"), later),
            "the swept table remembers the read it made room for"
        );
    }

    #[test]
    fn a_table_full_of_live_reads_records_rather_than_forgets() {
        let dedup = ReadDedup::with_capacity(MINUTE, 1);
        let now = Instant::now();
        assert!(dedup.admit_at(&read("tok", "j1", "OK"), now));
        assert!(dedup.admit_at(&read("tok", "j2", "OK"), now));
        assert!(
            dedup.admit_at(&read("tok", "j2", "OK"), now),
            "an untracked read is recorded every time"
        );
        assert!(
            !dedup.admit_at(&read("tok", "j1", "OK"), now),
            "the tracked one is still folded"
        );
    }
}
