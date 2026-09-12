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

/// Write `T`'s schedule into `namespace`, if it is not already the one on
/// record.
///
/// A worker calls this at every start. An unchanged declaration is a no-op, so
/// restarting a worker touches nothing an operator or another worker's
/// scheduler may have changed in the meantime.
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
    let existing = storage
        .list_periodic(namespace)?
        .into_iter()
        .find(|task| task.name == T::NAME);

    // An unchanged declaration writes nothing at all.
    //
    // Recomputing `next_run` on every start loses a firing whenever a restart
    // lands after a deadline passed but before the scheduler reached it, and
    // writing `enabled: true` would resume a task an operator had paused. Both
    // were real; neither is fixed by reading the row first and writing it back,
    // because `register_periodic` is not conditional — between the read and the
    // write another worker's scheduler can advance `next_run`, or an operator
    // can pause the task, and the write would undo it.
    //
    // Not writing is the only way to avoid that from here. Closing the window
    // on the changed-declaration path below needs a conditional upsert in the
    // `Storage` contract and all three backends; filed separately.
    if let Some(task) = &existing {
        if task.cron_expr == spec.cron
            && task.timezone.as_deref() == spec.timezone
            && task.queue == T::defaults().queue
        {
            return Ok(());
        }
    }

    let now = now_millis();
    let next_run = match spec.timezone {
        Some(tz) => next_cron_time_tz(spec.cron, now, tz),
        None => next_cron_time(spec.cron, now),
    }?;

    // A pause is an operator's decision and outlives a schedule change.
    let enabled = existing.as_ref().is_none_or(|task| task.enabled);

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
        enabled,
        next_run,
        timezone: spec.timezone.map(str::to_string),
        namespace: namespace.map(str::to_string),
    };
    storage.register_periodic(&row)
}
