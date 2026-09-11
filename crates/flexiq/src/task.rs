//! What the attribute macro implements.

use flexiq_core::{Job, TaskConfig};

use crate::{EnqueueOptions, Outcome};

/// A registered task: its name, its dispatch policy, its enqueue defaults, and
/// how to run one encoded job.
///
/// Implemented by `#[flexiq::task]`. Implementing it by hand is supported and is
/// what this crate's own tests do — the macro is ergonomics, not plumbing, and
/// a task with an unusual shape can skip it.
pub trait Task: Send + Sync + 'static {
    /// The name a job carries, and the name a producer in any language
    /// enqueues.
    const NAME: &'static str;

    /// Dispatch policy: retries, rate limits, breaker, concurrency caps.
    ///
    /// Everything here is read by the scheduler, never by the job row. The
    /// split is core's: [`TaskConfig`] carries no `queue`, `priority` or
    /// `timeout`, because those are properties of a job rather than of a task.
    fn config() -> TaskConfig;

    /// The enqueue defaults this task was declared with, seeded into every
    /// [`crate::TaskCall`] before a caller's per-call overrides.
    fn defaults() -> EnqueueOptions;

    /// Decode the job's payload, run the body, encode the result.
    fn run_encoded(job: &Job) -> Outcome<Option<Vec<u8>>>;

    /// This task's cron schedule, when it was declared with one.
    ///
    /// A worker registers every scheduled task it knows about at startup. The
    /// default is `None`, so a task without `cron` costs nothing.
    fn periodic() -> Option<crate::PeriodicSpec> {
        None
    }
}

/// The durable-step handle a running task holds.
///
/// Obtained from [`current_step`] rather than passed in, because a
/// macro-expanded body is the caller's own function and cannot grow an argument
/// it did not declare. The handle itself carries nothing: the session it
/// reaches belongs to the dispatch running on this thread, so two handles
/// inside one task are the same session, and one taken in another task is a
/// different session.
pub struct StepHandle {
    _private: (),
}

/// The durable-step handle for the task running on this thread.
///
/// Outside a running task, every method on the result fails with a message
/// saying so rather than panicking — a panic here would take down a pool thread
/// over a mistake in a caller's own code.
///
/// ```ignore
/// #[flexiq::task]
/// fn checkout(order: String) -> flexiq::Outcome<()> {
///     let mut step = flexiq::current_step();
///     let receipt: String = step.run("charge", || Ok(charge(&order)))?;
///     step.sleep_ms("settle", 60_000)?;
///     let _ = receipt;
///     Ok(())
/// }
/// ```
pub fn current_step() -> StepHandle {
    StepHandle { _private: () }
}
