//! Load shedding: the vocabulary shared by every path that throws a job away
//! rather than running it.
//!
//! Shedding and failing must stay distinguishable. A shed job is dead-lettered
//! like a failed one — same terminal status, same DLQ row, so it is visible in
//! the dashboard and countable in metrics — but its error carries a reserved
//! prefix, and the DLQ auto-retry sweep skips anything wearing one. Resurrecting
//! a job the scheduler deliberately dropped would defeat the shed.

/// Reason prefix on a job CoDel shed for sitting too long under overload.
pub(crate) const CODEL_REASON_PREFIX: &str = "codel:";

/// Reason prefix on a job a rate limit shed because its task asked for
/// [`OnExcess::Drop`].
pub(crate) const RATE_LIMIT_REASON_PREFIX: &str = "rate_limit:";

/// Dead-letter metadata marking a rate-limit shed, so a DLQ reader can count
/// shed work without parsing the reason string.
pub(crate) const RATE_LIMIT_SHED_METADATA: &str = r#"{"shed":"rate_limit"}"#;

/// What the dispatcher does with a job a rate limit turns away.
///
/// The default defers — the job keeps its place and runs once tokens are
/// available. Dropping suits traffic whose value expires with the moment
/// (metrics samples, cache warms): a job that cannot run now is worth less than
/// the backlog it would create.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum OnExcess {
    /// Reschedule the job and retry it on a later poll cycle.
    #[default]
    Defer,
    /// Dead-letter the job immediately with a reserved `rate_limit:` reason.
    Drop,
}

impl OnExcess {
    /// Parse the wire spelling the SDK shells send; `None` for anything else.
    pub fn parse(spec: &str) -> Option<Self> {
        match spec {
            "defer" => Some(Self::Defer),
            "drop" => Some(Self::Drop),
            _ => None,
        }
    }

    /// The wire spelling, the inverse of [`OnExcess::parse`].
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Defer => "defer",
            Self::Drop => "drop",
        }
    }
}

/// Whether a dead-letter reason marks a job the scheduler shed on purpose.
pub(crate) fn is_shed_reason(error: Option<&str>) -> bool {
    error.is_some_and(|e| {
        e.starts_with(CODEL_REASON_PREFIX) || e.starts_with(RATE_LIMIT_REASON_PREFIX)
    })
}

/// The dead-letter reason for a job dropped by a saturated rate limiter.
/// `scope` names the limiter that rejected it, e.g. `task 'ingest'`.
pub(crate) fn rate_limit_shed_reason(scope: &str) -> String {
    format!("{RATE_LIMIT_REASON_PREFIX} {scope} is over its dispatch rate limit, and its on_excess is drop")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_round_trips_every_variant() {
        for variant in [OnExcess::Defer, OnExcess::Drop] {
            assert_eq!(OnExcess::parse(variant.as_str()), Some(variant));
        }
        assert_eq!(OnExcess::parse("DROP"), None, "the wire spelling is exact");
        assert_eq!(OnExcess::parse(""), None);
    }

    #[test]
    fn shed_reasons_are_recognized_and_failures_are_not() {
        assert!(is_shed_reason(Some(&rate_limit_shed_reason("task 'x'"))));
        assert!(is_shed_reason(Some("codel: sojourn 900ms exceeded target")));
        assert!(!is_shed_reason(Some("ConnectionError: refused")));
        assert!(!is_shed_reason(None));
    }
}
