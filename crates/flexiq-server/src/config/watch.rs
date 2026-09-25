//! The bounds on `WatchJobs` streams: how many one credential may hold, how
//! far a stream may fall behind, and how often watched ids are re-read.

use std::time::Duration;

use anyhow::{bail, Context, Result};

use crate::config::{value, Env};

/// Concurrent watches one credential may hold. Zero leaves it unbounded.
pub const MAX_PER_CREDENTIAL_VAR: &str = "FLEXIQ_GRPC_WATCH_MAX_PER_TOKEN";

/// Seconds between re-reads of every watched id. Zero turns the re-read off.
pub const RECONCILE_VAR: &str = "FLEXIQ_GRPC_WATCH_RECONCILE";

/// Transitions this process keeps for streams to catch up on and resume from.
pub const BUFFER_VAR: &str = "FLEXIQ_GRPC_WATCH_BUFFER";

/// Seconds a stream may wait on a client that is not reading.
pub const STALL_VAR: &str = "FLEXIQ_GRPC_WATCH_STALL";

/// A stream is a task, a buffer and a slot in the reconcile read, so one
/// credential gets a handful, not an unbounded number.
const DEFAULT_MAX_PER_CREDENTIAL: usize = 16;

/// How late a transition handled by another process may reach a watch. One
/// batched read per tick, whatever the number of watchers.
const DEFAULT_RECONCILE: Duration = Duration::from_secs(5);

/// Enough for a burst of several thousand transitions; each entry is one
/// payload-free event.
const DEFAULT_BUFFER: usize = 4_096;

/// Long enough for a GC pause or a slow link, short enough that a client that
/// has stopped reading does not hold its place for long.
const DEFAULT_STALL: Duration = Duration::from_secs(30);

/// The bounds every `WatchJobs` stream on this listener is held to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchConfig {
    /// Concurrent watches per credential. Zero leaves it unbounded.
    pub max_per_credential: usize,
    /// How often every watched id is re-read. Zero turns the re-read off, and
    /// with it every transition another process handles.
    pub reconcile_interval: Duration,
    /// Transitions kept in memory. At least one.
    pub buffer: usize,
    /// How long a stream waits on a client that is not reading. Positive.
    pub stall: Duration,
}

impl Default for WatchConfig {
    fn default() -> Self {
        Self {
            max_per_credential: DEFAULT_MAX_PER_CREDENTIAL,
            reconcile_interval: DEFAULT_RECONCILE,
            buffer: DEFAULT_BUFFER,
            stall: DEFAULT_STALL,
        }
    }
}

/// Read the watch bounds, each at its default when unset.
pub fn from_env(env: &Env) -> Result<WatchConfig> {
    let defaults = WatchConfig::default();
    let max_per_credential = whole(env, MAX_PER_CREDENTIAL_VAR, defaults.max_per_credential)?;
    let reconcile_interval = Duration::from_secs(whole(
        env,
        RECONCILE_VAR,
        defaults.reconcile_interval.as_secs(),
    )?);
    let buffer = whole(env, BUFFER_VAR, defaults.buffer)?;
    if buffer == 0 {
        bail!("{BUFFER_VAR} must be at least 1: a stream needs somewhere to read from");
    }
    let stall = Duration::from_secs(whole(env, STALL_VAR, defaults.stall.as_secs())?);
    if stall.is_zero() {
        bail!(
            "{STALL_VAR} must be at least 1 second: zero would end a stream the first time \
             its client was a moment slow"
        );
    }
    Ok(WatchConfig {
        max_per_credential,
        reconcile_interval,
        buffer,
        stall,
    })
}

fn whole<T: std::str::FromStr>(env: &Env, key: &str, default: T) -> Result<T>
where
    T::Err: std::error::Error + Send + Sync + 'static,
{
    match value(env, key) {
        None => Ok(default),
        Some(raw) => raw
            .parse()
            .with_context(|| format!("{key} must be a whole number, got '{raw}'")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(pairs: &[(&str, &str)]) -> Env {
        pairs
            .iter()
            .map(|(key, val)| (key.to_string(), val.to_string()))
            .collect()
    }

    #[test]
    fn nothing_set_takes_every_default() {
        assert_eq!(from_env(&env(&[])).unwrap(), WatchConfig::default());
    }

    #[test]
    fn every_bound_reads_its_variable() {
        let config = from_env(&env(&[
            (MAX_PER_CREDENTIAL_VAR, "0"),
            (RECONCILE_VAR, "0"),
            (BUFFER_VAR, "10"),
            (STALL_VAR, "2"),
        ]))
        .unwrap();
        assert_eq!(
            config,
            WatchConfig {
                max_per_credential: 0,
                reconcile_interval: Duration::ZERO,
                buffer: 10,
                stall: Duration::from_secs(2),
            }
        );
    }

    #[test]
    fn an_empty_buffer_and_a_zero_stall_are_refused() {
        for (key, raw) in [(BUFFER_VAR, "0"), (STALL_VAR, "0")] {
            let error = from_env(&env(&[(key, raw)])).unwrap_err();
            assert!(error.to_string().contains(key), "{error}");
        }
    }

    #[test]
    fn a_non_number_names_its_variable() {
        let error = from_env(&env(&[(RECONCILE_VAR, "5s")])).unwrap_err();
        assert!(error.to_string().contains(RECONCILE_VAR), "{error}");
    }
}
