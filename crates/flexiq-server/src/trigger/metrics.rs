//! `flexiq_trigger_requests_total{trigger, outcome}`.
//!
//! Process-wide, because the listener that counts and the `/metrics` routes
//! that publish are different roles in the same process — the trigger port is
//! public, so it serves no metrics of its own. The dashboard's and the gRPC
//! door's `/metrics` append [`render`] to what they already publish.
//!
//! Labels are bounded: `trigger` comes from the definitions file, never from
//! a request, and `outcome` is one of a fixed set.

use std::collections::BTreeMap;
use std::sync::{Mutex, OnceLock};

use crate::metrics::escape_label;

type Counts = BTreeMap<(String, &'static str), u64>;

fn counts() -> &'static Mutex<Counts> {
    static COUNTS: OnceLock<Mutex<Counts>> = OnceLock::new();
    COUNTS.get_or_init(Default::default)
}

/// Count one request to `trigger` that ended in `outcome`.
pub fn record(trigger: &str, outcome: &'static str) {
    // A poisoned lock only means another thread panicked mid-increment; the
    // map is still a map, and dropping the count would hide the traffic.
    let mut counts = counts()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    *counts.entry((trigger.to_string(), outcome)).or_default() += 1;
}

/// The counter family, or nothing when no trigger has been called.
pub fn render() -> String {
    let counts = counts()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if counts.is_empty() {
        return String::new();
    }
    let mut body = String::from(
        "# HELP flexiq_trigger_requests_total Requests to each trigger, by outcome.\n\
         # TYPE flexiq_trigger_requests_total counter\n",
    );
    for ((trigger, outcome), count) in counts.iter() {
        body.push_str(&format!(
            "flexiq_trigger_requests_total{{trigger=\"{}\",outcome=\"{outcome}\"}} {count}\n",
            escape_label(trigger)
        ));
    }
    body
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_accumulate_per_trigger_and_outcome() {
        // The registry is process-wide, so this test owns its own label.
        record("metrics-test", "enqueued");
        record("metrics-test", "enqueued");
        record("metrics-test", "unauthorized");
        let body = render();
        assert!(body.contains("# TYPE flexiq_trigger_requests_total counter"));
        assert!(
            body.contains(
                "flexiq_trigger_requests_total{trigger=\"metrics-test\",outcome=\"enqueued\"} 2"
            ),
            "{body}"
        );
        assert!(
            body.contains(
                "flexiq_trigger_requests_total{trigger=\"metrics-test\",outcome=\"unauthorized\"} 1"
            ),
            "{body}"
        );
    }
}
