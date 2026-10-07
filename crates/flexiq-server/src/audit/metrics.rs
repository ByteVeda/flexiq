//! `flexiq_audit_records_total{outcome}`.
//!
//! Process-wide, like the trigger counters: the sink that counts and the
//! `/metrics` route that publishes never share a handle. `outcome` is one of
//! a fixed set, so the family is bounded.

use std::sync::atomic::{AtomicU64, Ordering};

/// Records appended to the audit table.
static WRITTEN: AtomicU64 = AtomicU64::new(0);
/// Records that found the buffer full and went to the log instead.
static OVERFLOWED: AtomicU64 = AtomicU64::new(0);
/// Records whose append failed and went to the log instead.
static FAILED: AtomicU64 = AtomicU64::new(0);

/// Count records the table took.
pub(super) fn written(count: usize) {
    WRITTEN.fetch_add(count as u64, Ordering::Relaxed);
}

/// Count one record that found the buffer full.
pub(super) fn overflowed() {
    OVERFLOWED.fetch_add(1, Ordering::Relaxed);
}

/// Count records whose append failed.
pub(super) fn failed(count: usize) {
    FAILED.fetch_add(count as u64, Ordering::Relaxed);
}

/// Overflows so far, for a test to measure one against.
#[cfg(test)]
pub(super) fn overflowed_total() -> u64 {
    OVERFLOWED.load(Ordering::Relaxed)
}

/// The counter family. Rendered even at zero: a record the table did not
/// take is the series to alert on, and an absent series cannot be alerted on.
pub fn render() -> String {
    let mut body = String::from(
        "# HELP flexiq_audit_records_total Audit records, by where they ended up: \
         `written` to the audit table, or `overflow` / `write_failed` to the log.\n\
         # TYPE flexiq_audit_records_total counter\n",
    );
    for (outcome, counter) in [
        ("written", &WRITTEN),
        ("overflow", &OVERFLOWED),
        ("write_failed", &FAILED),
    ] {
        body.push_str(&format!(
            "flexiq_audit_records_total{{outcome=\"{outcome}\"}} {}\n",
            counter.load(Ordering::Relaxed)
        ));
    }
    body
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_outcome_renders_even_at_zero() {
        let body = render();
        for outcome in ["written", "overflow", "write_failed"] {
            assert!(
                body.contains(&format!(
                    "flexiq_audit_records_total{{outcome=\"{outcome}\"}}"
                )),
                "{outcome} missing from:\n{body}"
            );
        }
    }
}
