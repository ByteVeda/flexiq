//! How long the audit trail keeps a record.
//!
//! The trail was the gRPC door's alone (#840), so its window was named
//! `FLEXIQ_GRPC_AUDIT_RETENTION_DAYS`. The dashboard and the token command
//! line record into it too now (#994), and a dashboard-only deployment prunes
//! it as well, so the window has a name of its own. The old name is still
//! read: renaming a variable under a running deployment would quietly drop it
//! back to the default.

use std::time::Duration;

use anyhow::{bail, Result};

use super::{value, Env};

/// How many days an audit record is kept.
pub const RETENTION_DAYS_VAR: &str = "FLEXIQ_AUDIT_RETENTION_DAYS";

/// The name the window had while only the gRPC door recorded. Still honoured.
pub const LEGACY_RETENTION_DAYS_VAR: &str = "FLEXIQ_GRPC_AUDIT_RETENTION_DAYS";

/// How many days a read record is kept (#1018). Defaults to the write window.
pub const READS_RETENTION_DAYS_VAR: &str = "FLEXIQ_AUDIT_READS_RETENTION_DAYS";

/// Ninety days: long enough to answer "what did this token do" about a
/// credential revoked last quarter, short enough that the table stays small.
pub const DEFAULT_RETENTION_DAYS: u64 = 90;

/// How long the trail keeps each kind of record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuditRetention {
    /// Every record — writes, and reads too once this has passed.
    pub writes: Duration,
    /// Read records. Never longer than `writes`.
    pub reads: Duration,
}

/// The default windows: ninety days for both.
pub fn default_retention() -> AuditRetention {
    let window = days(DEFAULT_RETENTION_DAYS);
    AuditRetention {
        writes: window,
        reads: window,
    }
}

/// Read both windows in whole days.
///
/// Zero is refused rather than read as "off": the trail is not optional, and
/// "keep nothing" is not a window. Neither is "forever" — a table no one
/// prunes is the growth the window exists to stop. A read window longer than
/// the write window is refused: the prune drops every record at the write
/// window, so the longer setting would be one the server ignores.
pub fn retention(env: &Env) -> Result<AuditRetention> {
    let writes = writes_window(env)?;
    let reads = match whole_days(env, READS_RETENTION_DAYS_VAR)? {
        Some(reads) if reads > writes => bail!(
            "{READS_RETENTION_DAYS_VAR} ({}) is longer than the audit retention ({}); \
             reads cannot outlive the writes they relate to",
            reads.as_secs() / 86_400,
            writes.as_secs() / 86_400
        ),
        Some(reads) => reads,
        None => writes,
    };
    Ok(AuditRetention { writes, reads })
}

/// The write window, from either name. Both names set to different windows
/// is refused: whichever one won, the other would be a setting the operator
/// believes in and the server ignores.
fn writes_window(env: &Env) -> Result<Duration> {
    let current = whole_days(env, RETENTION_DAYS_VAR)?;
    let legacy = whole_days(env, LEGACY_RETENTION_DAYS_VAR)?;
    match (current, legacy) {
        (Some(current), Some(legacy)) if current != legacy => bail!(
            "{RETENTION_DAYS_VAR} ({}) and {LEGACY_RETENTION_DAYS_VAR} ({}) disagree; \
             set only {RETENTION_DAYS_VAR}",
            current.as_secs() / 86_400,
            legacy.as_secs() / 86_400
        ),
        (Some(window), _) | (None, Some(window)) => Ok(window),
        (None, None) => Ok(days(DEFAULT_RETENTION_DAYS)),
    }
}

fn whole_days(env: &Env, key: &str) -> Result<Option<Duration>> {
    let Some(raw) = value(env, key) else {
        return Ok(None);
    };
    match raw.parse::<u64>() {
        Ok(count) if count > 0 => Ok(Some(days(count))),
        _ => bail!("{key} must be a whole number of days, at least 1, got '{raw}'"),
    }
}

fn days(count: u64) -> Duration {
    Duration::from_secs(count.saturating_mul(86_400))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn windows(pairs: &[(&str, &str)]) -> Result<AuditRetention> {
        let env = pairs
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect();
        retention(&env)
    }

    /// The write window alone.
    fn parse(pairs: &[(&str, &str)]) -> Result<Duration> {
        windows(pairs).map(|w| w.writes)
    }

    const DAY: u64 = 86_400;

    #[test]
    fn reads_default_to_the_write_window() {
        let w = windows(&[(RETENTION_DAYS_VAR, "30")]).expect("valid");
        assert_eq!(w.reads, w.writes);
        assert_eq!(default_retention().reads.as_secs(), 90 * DAY);
    }

    #[test]
    fn reads_may_be_kept_for_less_but_never_more() {
        let w = windows(&[(RETENTION_DAYS_VAR, "30"), (READS_RETENTION_DAYS_VAR, "7")])
            .expect("shorter");
        assert_eq!((w.writes.as_secs(), w.reads.as_secs()), (30 * DAY, 7 * DAY));
        // Against the default write window too.
        let error = windows(&[(READS_RETENTION_DAYS_VAR, "91")])
            .expect_err("longer")
            .to_string();
        assert!(error.contains("cannot outlive"), "{error}");
        for refused in ["0", "-1", "7d"] {
            let error = windows(&[(READS_RETENTION_DAYS_VAR, refused)])
                .expect_err(refused)
                .to_string();
            assert!(error.contains(READS_RETENTION_DAYS_VAR), "{error}");
        }
    }

    #[test]
    fn the_window_defaults_to_ninety_days_and_reads_whole_days() {
        assert_eq!(parse(&[]).expect("valid").as_secs(), 90 * DAY);
        assert_eq!(
            parse(&[(RETENTION_DAYS_VAR, "7")])
                .expect("valid")
                .as_secs(),
            7 * DAY
        );
        for refused in ["0", "-1", "7d"] {
            let error = parse(&[(RETENTION_DAYS_VAR, refused)])
                .expect_err(refused)
                .to_string();
            assert!(error.contains(RETENTION_DAYS_VAR), "{error}");
        }
    }

    #[test]
    fn the_old_name_is_still_honoured() {
        assert_eq!(
            parse(&[(LEGACY_RETENTION_DAYS_VAR, "30")])
                .expect("valid")
                .as_secs(),
            30 * DAY
        );
        let error = parse(&[(LEGACY_RETENTION_DAYS_VAR, "0")])
            .expect_err("zero")
            .to_string();
        assert!(error.contains(LEGACY_RETENTION_DAYS_VAR), "{error}");
    }

    #[test]
    fn both_names_must_agree() {
        assert_eq!(
            parse(&[
                (RETENTION_DAYS_VAR, "14"),
                (LEGACY_RETENTION_DAYS_VAR, "14")
            ])
            .expect("agreeing")
            .as_secs(),
            14 * DAY
        );
        let error = parse(&[
            (RETENTION_DAYS_VAR, "14"),
            (LEGACY_RETENTION_DAYS_VAR, "30"),
        ])
        .expect_err("disagreeing")
        .to_string();
        assert!(error.contains("disagree"), "{error}");
    }
}
