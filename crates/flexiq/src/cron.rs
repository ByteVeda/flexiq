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

/// Write `T`'s schedule into `namespace`.
///
/// A worker calls this at every start, and it reads nothing first. A schedule
/// that lives in code owns its cron expression, its timezone and its queue; a
/// deadline and a pause belong to the scheduler and to an operator, and
/// [`Storage::declare_periodic`] is the write that says so. Deciding here
/// instead would mean reading the row and writing it back, which is a lost
/// update — between the two, another worker's scheduler can advance `next_run`
/// or an operator can pause the task, and the write would undo it (#919).
///
/// So a restart never resets a deadline the scheduler advanced and never
/// resumes a task somebody paused, whether or not the declaration changed.
///
/// `namespace` is the worker's own: a schedule is identified by
/// `(namespace, name)` (#918), so two tenants sharing a database can each
/// declare a task called `nightly` without either one seeing or overwriting the
/// other's row.
pub(crate) fn register<T: Task>(
    storage: &impl Storage,
    spec: &PeriodicSpec,
    namespace: Option<&str>,
) -> Result<()> {
    let now = now_millis();
    // Offered, not imposed: the backend applies it only if the stored deadline
    // was computed from a schedule this declaration no longer asks for.
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
        // A first registration is live; on a row that already exists this is
        // not written at all, so a pause survives.
        enabled: true,
        next_run,
        timezone: spec.timezone.map(str::to_string),
        namespace: namespace.map(str::to_string),
    };
    storage.declare_periodic(&row)
}
