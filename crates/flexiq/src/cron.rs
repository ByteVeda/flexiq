//! Periodic tasks.
//!
//! Registration is three storage calls and no new machinery: the scheduler a
//! worker already runs checks for due periodics on its own tick, mints the job
//! and advances `next_run`. This module only writes the row.
//!
//! # One process should own a periodic
//!
//! Firing is per-`Scheduler` and **not** leader-elected, unlike the dead-worker
//! reaper beside it. The dedup key a fired job carries is
//! `periodic:{name}:{now}` computed from each process's own clock, so two
//! workers that both registered the same periodic can both find it due inside
//! the same millisecond and each mint a job the other's key does not match.
//!
//! Every shell inherits this; none of them says so. Register periodics from one
//! process, or accept that a fleet may double-fire.

use flexiq_core::periodic::{next_cron_time, next_cron_time_tz};
use flexiq_core::{now_millis, NewPeriodicTask, Result, Storage};

use crate::Task;

/// A task's cron schedule, as declared.
#[derive(Debug, Clone)]
pub struct PeriodicSpec {
    /// A six-field cron expression — seconds first.
    pub cron: &'static str,
    /// An IANA timezone name. `None` is UTC.
    pub timezone: Option<&'static str>,
}

/// Write `T`'s schedule, or refresh it if it is already there.
///
/// `register_periodic` upserts on the name, so calling this on every worker
/// start is correct rather than merely tolerated.
pub(crate) fn register<T: Task>(storage: &impl Storage, spec: &PeriodicSpec) -> Result<()> {
    let now = now_millis();
    let next_run = match spec.timezone {
        Some(tz) => next_cron_time_tz(spec.cron, now, tz),
        None => next_cron_time(spec.cron, now),
    }?;

    let row = NewPeriodicTask {
        name: T::NAME.to_string(),
        task_name: T::NAME.to_string(),
        cron_expr: spec.cron.to_string(),
        // The envelope for a call with no arguments. Storing `None` would leave
        // the fired job with an empty payload, which is not a call any shell
        // could decode — including this one, if the task ever grows a
        // parameter.
        args: Some(crate::encode::encode_args(&[])),
        kwargs: None,
        queue: T::defaults().queue,
        enabled: true,
        next_run,
        timezone: spec.timezone.map(str::to_string),
    };
    storage.register_periodic(&row)
}
