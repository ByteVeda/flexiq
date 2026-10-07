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

/// Ninety days: long enough to answer "what did this token do" about a
/// credential revoked last quarter, short enough that the table stays small.
pub const DEFAULT_RETENTION_DAYS: u64 = 90;

/// The default window.
pub fn default_retention() -> Duration {
    days(DEFAULT_RETENTION_DAYS)
}

/// Read the window in whole days, from either name.
///
/// Zero is refused rather than read as "off": the trail is not optional, and
/// "keep nothing" is not a window. Neither is "forever" — a table no one
/// prunes is the growth the window exists to stop. Both names set to
/// different windows is refused too: whichever one won, the other would be a
/// setting the operator believes in and the server ignores.
pub fn retention(env: &Env) -> Result<Duration> {
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
        (None, None) => Ok(default_retention()),
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

    fn parse(pairs: &[(&str, &str)]) -> Result<Duration> {
        let env = pairs
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect();
        retention(&env)
    }

    const DAY: u64 = 86_400;

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
