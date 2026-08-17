use std::collections::HashMap;
use std::time::Instant;

use super::message::MemberId;

/// Tracks outstanding probes and suspicion timers for failure detection.
pub struct FailureDetector {
    /// Pending direct pings awaiting ACK: seq → (target, sent_at).
    pending_pings: HashMap<u64, (MemberId, Instant)>,
    /// Pending indirect probes: seq → (target, sent_at).
    pending_ping_reqs: HashMap<u64, (MemberId, Instant)>,
    /// Members currently under suspicion: member_id → suspect_since.
    suspects: HashMap<MemberId, Instant>,
    /// Ping timeout before escalating to indirect probes.
    ping_timeout_ms: u64,
    /// Suspicion timeout = multiplier * log(N+1) * protocol_period.
    suspicion_multiplier: u32,
    protocol_period_ms: u64,
}

impl FailureDetector {
    pub fn new(ping_timeout_ms: u64, suspicion_multiplier: u32, protocol_period_ms: u64) -> Self {
        Self {
            pending_pings: HashMap::new(),
            pending_ping_reqs: HashMap::new(),
            suspects: HashMap::new(),
            ping_timeout_ms,
            suspicion_multiplier,
            protocol_period_ms,
        }
    }

    /// Record that a direct ping was sent.
    pub fn ping_sent(&mut self, seq: u64, target: MemberId) {
        self.pending_pings.insert(seq, (target, Instant::now()));
    }

    /// Record that indirect probes were sent.
    pub fn ping_req_sent(&mut self, seq: u64, target: MemberId) {
        self.pending_ping_reqs.insert(seq, (target, Instant::now()));
    }

    /// Process an ACK. Returns the target member ID if this resolves a pending probe.
    pub fn ack_received(&mut self, seq: u64) -> Option<MemberId> {
        if let Some((target, _)) = self.pending_pings.remove(&seq) {
            self.suspects.remove(&target);
            return Some(target);
        }
        if let Some((target, _)) = self.pending_ping_reqs.remove(&seq) {
            self.suspects.remove(&target);
            return Some(target);
        }
        None
    }

    /// Check for timed-out direct pings. Returns members that need indirect probing.
    pub fn check_ping_timeouts(&mut self) -> Vec<MemberId> {
        let timeout = std::time::Duration::from_millis(self.ping_timeout_ms);
        let now = Instant::now();
        let mut timed_out = Vec::new();

        self.pending_pings.retain(|_, (target, sent_at)| {
            if now.duration_since(*sent_at) > timeout {
                timed_out.push(target.clone());
                false
            } else {
                true
            }
        });

        timed_out
    }

    /// Mark a member as suspect. Returns true if newly suspected.
    pub fn suspect(&mut self, member_id: &str) -> bool {
        if self.suspects.contains_key(member_id) {
            return false;
        }
        self.suspects.insert(member_id.to_string(), Instant::now());
        true
    }

    /// Check for expired suspicions. Returns members that should be declared dead.
    pub fn check_suspicion_timeouts(&mut self, member_count: usize) -> Vec<MemberId> {
        let timeout = self.suspicion_timeout(member_count);
        let now = Instant::now();
        let mut dead = Vec::new();

        self.suspects.retain(|member_id, suspect_since| {
            if now.duration_since(*suspect_since) > timeout {
                dead.push(member_id.clone());
                false
            } else {
                true
            }
        });

        dead
    }

    /// Suspicion timeout scales with log(N+1) for consistency guarantees.
    pub fn suspicion_timeout(&self, member_count: usize) -> std::time::Duration {
        let log_n = ((member_count + 1) as f64).ln().max(1.0);
        let ms = self.suspicion_multiplier as f64 * log_n * self.protocol_period_ms as f64;
        std::time::Duration::from_millis(ms as u64)
    }

    /// Clear a suspect (e.g., on receiving a refutation with higher incarnation).
    pub fn clear_suspect(&mut self, member_id: &str) {
        self.suspects.remove(member_id);
    }

    /// Number of currently suspected members.
    pub fn suspect_count(&self) -> usize {
        self.suspects.len()
    }

    /// Clean up stale pending probes older than 2x protocol period.
    pub fn gc_stale_probes(&mut self) {
        let stale = std::time::Duration::from_millis(self.protocol_period_ms * 2);
        let now = Instant::now();
        self.pending_pings
            .retain(|_, (_, sent)| now.duration_since(*sent) < stale);
        self.pending_ping_reqs
            .retain(|_, (_, sent)| now.duration_since(*sent) < stale);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ping_ack_resolves() {
        let mut fd = FailureDetector::new(200, 4, 500);
        fd.ping_sent(1, "peer-a".to_string());
        let resolved = fd.ack_received(1);
        assert_eq!(resolved, Some("peer-a".to_string()));
    }

    #[test]
    fn unknown_ack_ignored() {
        let mut fd = FailureDetector::new(200, 4, 500);
        assert!(fd.ack_received(999).is_none());
    }

    #[test]
    fn suspicion_timeout_scales_with_members() {
        let fd = FailureDetector::new(200, 4, 500);
        let t1 = fd.suspicion_timeout(1);
        let t10 = fd.suspicion_timeout(10);
        let t100 = fd.suspicion_timeout(100);
        assert!(t10 > t1, "more members = longer suspicion window");
        assert!(t100 > t10);
    }

    #[test]
    fn suspect_then_clear() {
        let mut fd = FailureDetector::new(200, 4, 500);
        assert!(fd.suspect("peer-a"));
        assert!(!fd.suspect("peer-a")); // already suspected
        assert_eq!(fd.suspect_count(), 1);
        fd.clear_suspect("peer-a");
        assert_eq!(fd.suspect_count(), 0);
    }

    #[test]
    fn ack_clears_suspicion() {
        let mut fd = FailureDetector::new(200, 4, 500);
        fd.ping_sent(1, "peer-a".to_string());
        fd.suspect("peer-a");
        fd.ack_received(1);
        assert_eq!(fd.suspect_count(), 0);
    }
}
