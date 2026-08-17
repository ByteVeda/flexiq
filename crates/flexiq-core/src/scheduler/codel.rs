//! Controlled Delay (CoDel) load shedding for the dispatcher.
//!
//! Classic CoDel (RFC 8289), adapted from packet queues to task dispatch. A
//! job's *sojourn* is how long it waited past its eligibility (`now -
//! scheduled_at`). While the sojourn stays above `target` for a full `interval`,
//! the controller enters a dropping state and sheds jobs at an increasing rate
//! (`interval / sqrt(count)`), backing off the moment the sojourn recovers.
//!
//! Unlike a plain deadline this does **not** drop during a transient spike —
//! only sustained overload — which is the whole point of using CoDel over a
//! fixed max-age cutoff. Opt-in per queue; when unset the dispatcher never calls
//! in here.

/// Per-queue CoDel tuning. `target_ms` is the acceptable steady-state sojourn;
/// `interval_ms` is the window the sojourn must stay above target before
/// shedding begins.
#[derive(Debug, Clone, Copy)]
pub struct CodelConfig {
    /// Acceptable steady-state sojourn in milliseconds.
    pub target_ms: i64,
    /// Milliseconds the sojourn must stay above target before shedding.
    pub interval_ms: i64,
}

/// Mutable controller state for one queue. Advanced once per candidate job in
/// dequeue order; persists across dispatch ticks.
#[derive(Debug, Default)]
pub struct CodelState {
    /// Time at which shedding may begin, armed when the sojourn first exceeds
    /// target and cleared when it recovers. `0` = not currently above target.
    first_above_time: i64,
    /// Scheduled time of the next drop while shedding.
    drop_next: i64,
    /// Drops in the current overload episode; drives the control law.
    count: u32,
    /// Whether the controller is currently shedding.
    dropping: bool,
}

impl CodelState {
    /// Decide whether the candidate job with the given `sojourn_ms` should be
    /// shed. Mutates the controller; call once per job in dequeue order.
    pub fn should_drop(&mut self, sojourn_ms: i64, now: i64, cfg: &CodelConfig) -> bool {
        if sojourn_ms < cfg.target_ms {
            // Healthy: leave the dropping state and disarm.
            self.first_above_time = 0;
            self.dropping = false;
            return false;
        }
        // At or above target.
        if self.first_above_time == 0 {
            // Just crossed above target — arm the interval before we may drop.
            self.first_above_time = now.saturating_add(cfg.interval_ms);
            return false;
        }
        if now < self.first_above_time {
            // Above target, but not yet for a full interval — hold.
            return false;
        }
        // Sustained above target for at least one interval: shed.
        if !self.dropping {
            self.dropping = true;
            // A fresh episode restarts the rate at 1, but a quick relapse (soon
            // after the last drop) resumes near the prior rate instead of ramping
            // from scratch — the standard CoDel re-entry heuristic.
            self.count =
                if self.count > 2 && now.saturating_sub(self.drop_next) < 8 * cfg.interval_ms {
                    self.count - 2
                } else {
                    1
                };
            self.drop_next = self.control_law(now, cfg);
            return true;
        }
        if now >= self.drop_next {
            self.count = self.count.saturating_add(1);
            self.drop_next = self.control_law(now, cfg);
            return true;
        }
        false
    }

    /// Next drop time: spaced by `interval / sqrt(count)`, so the drop rate rises
    /// as an episode persists and the sojourn refuses to fall.
    fn control_law(&self, now: i64, cfg: &CodelConfig) -> i64 {
        let spacing = (cfg.interval_ms as f64 / (self.count.max(1) as f64).sqrt()) as i64;
        now.saturating_add(spacing.max(1))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CFG: CodelConfig = CodelConfig {
        target_ms: 100,
        interval_ms: 1000,
    };

    #[test]
    fn below_target_never_drops() {
        let mut s = CodelState::default();
        for i in 0..100 {
            let now = i * 50;
            assert!(!s.should_drop(10, now, &CFG), "healthy sojourn must pass");
        }
    }

    #[test]
    fn transient_spike_does_not_drop() {
        let mut s = CodelState::default();
        // Above target, but the episode never reaches a full interval before it
        // recovers — a plain deadline would have dropped, CoDel must not.
        assert!(!s.should_drop(500, 0, &CFG)); // arms first_above_time = 1000
        assert!(!s.should_drop(500, 500, &CFG)); // now < 1000: hold
        assert!(!s.should_drop(10, 800, &CFG)); // recovered before the interval
        assert!(!s.should_drop(500, 900, &CFG)); // re-arms, still no drop
    }

    #[test]
    fn sustained_overload_starts_dropping() {
        let mut s = CodelState::default();
        assert!(!s.should_drop(500, 0, &CFG)); // arm at now+interval = 1000
        assert!(!s.should_drop(500, 999, &CFG)); // still under the interval
        assert!(s.should_drop(500, 1000, &CFG)); // first drop at the interval edge
    }

    #[test]
    fn recovery_exits_dropping_state() {
        let mut s = CodelState::default();
        assert!(!s.should_drop(500, 0, &CFG));
        assert!(s.should_drop(500, 1000, &CFG)); // dropping
                                                 // Sojourn recovers: must immediately stop dropping and re-arm cleanly.
        assert!(!s.should_drop(10, 1100, &CFG));
        assert!(!s.should_drop(500, 1200, &CFG)); // re-arms, no immediate drop
        assert!(!s.should_drop(500, 2100, &CFG)); // still under the new interval
        assert!(s.should_drop(500, 2200, &CFG)); // drops again after a full interval
    }

    #[test]
    fn drop_rate_is_bounded_within_one_tick() {
        // Many candidates evaluated at the same `now` while shedding: the control
        // law spaces drops in time, so a single instant sheds at most one.
        let mut s = CodelState::default();
        assert!(!s.should_drop(500, 0, &CFG));
        assert!(s.should_drop(500, 1000, &CFG));
        for _ in 0..10 {
            assert!(
                !s.should_drop(500, 1000, &CFG),
                "no second drop at the same instant"
            );
        }
    }
}
