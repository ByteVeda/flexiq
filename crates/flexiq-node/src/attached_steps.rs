//! Durable inline steps for a job running on an attached executor.
//!
//! The twin of the worker path in [`crate::steps`], for the deployment where
//! this process holds no database and no execution claim. Everything that
//! *decides* — step identity, the sequence check, the caps, the divergence rule
//! — still runs here, because that is the core's session and it runs wherever
//! its store does. Only the write leaves: the snapshot a replay answers from
//! rode in on the dispatch, and each new step crosses to the scheduler, which
//! applies it under the claim it holds.
//!
//! **No owner crosses the wire, and none is invented here.** An owner an
//! executor fills in is an owner it can forge, and a forged one writes straight
//! into the live attempt's sequence.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use flexiq_core::error::QueueError;
use flexiq_core::job::Job;
use flexiq_core::step::StepLimits;
use napi::bindgen_prelude::{spawn_blocking, Result};
use napi_derive::napi;

use crate::error::join_to_napi_err;
use crate::executor::JsExecutor;
use crate::steps::{step_error, JsStepSession};

/// The jobs an attached executor is currently running.
///
/// A step session is opened by job id, and the core opens one against the job
/// itself — its namespace, its `retry_count`, and the metadata the run key is
/// derived from. Only the dispatcher ever holds one, so it records each job here
/// for as long as the attempt lasts.
///
/// The payload is *not* kept: it is moved into the JS invocation before a job
/// lands here, so an entry costs a handful of strings rather than a copy of
/// however many megabytes the task was called with.
#[derive(Default)]
pub struct RunningJobs {
    jobs: Mutex<HashMap<String, Job>>,
}

impl RunningJobs {
    /// Record `job` for the length of one attempt. The returned guard removes
    /// it however the attempt ends, including a panic on the way out.
    pub fn enter(self: &Arc<Self>, job: &Job) -> RunningJob {
        self.locked().insert(job.id.clone(), job.clone());
        RunningJob {
            registry: Arc::clone(self),
            job_id: job.id.clone(),
        }
    }

    /// The dispatch a step session would be opened against, if it is still
    /// running here.
    fn get(&self, job_id: &str) -> Option<Job> {
        self.locked().get(job_id).cloned()
    }

    fn locked(&self) -> std::sync::MutexGuard<'_, HashMap<String, Job>> {
        self.jobs.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// One job's entry in [`RunningJobs`], removed when the attempt ends.
///
/// Held by the dispatcher rather than by the session: a step opened after the
/// attempt reported would commit into an attempt the scheduler has already
/// reaped, and dropping the entry is what turns that into a refusal.
pub struct RunningJob {
    registry: Arc<RunningJobs>,
    job_id: String,
}

impl Drop for RunningJob {
    fn drop(&mut self) {
        self.registry.locked().remove(&self.job_id);
    }
}

#[napi]
impl JsExecutor {
    /// Whether durable steps work across this attach.
    ///
    /// False against a scheduler whose storage has no step store, or one built
    /// before steps existed. Not a gate — `openStepSession` refuses on its own —
    /// but it lets the shell warn once at attach rather than at the first
    /// `step.run`.
    #[napi]
    pub fn supports_steps(&self) -> bool {
        self.steps.is_supported()
    }

    /// Open the durable-step session for one attempt of `jobId`.
    ///
    /// On the **executor**, not the queue this process built: an executor's app
    /// module constructs a queue with no storage behind it, and the job belongs
    /// to the scheduler either way.
    ///
    /// Both arguments are checked against the dispatch rather than trusted, for
    /// the same reason the worker twin re-reads the job row: a session opened
    /// for a job this executor is not running, or for an attempt it has already
    /// reported, would commit into a sequence that is not its own.
    ///
    /// A scheduler that never advertised the step capability is refused by the
    /// core's own `open_session`, retryably — a fleet mid-rollout may place the
    /// next attempt somewhere that can commit. Restating that rule here is how
    /// a shell's copy of it drifts.
    #[napi]
    pub async fn open_step_session(&self, job_id: String, attempt: i32) -> Result<JsStepSession> {
        let steps = self.steps.clone();
        let running = Arc::clone(&self.running);

        // No storage read happens here — the snapshot arrived with the dispatch
        // — but it is cloned out from under a lock the reader thread also takes,
        // and a snapshot is as large as the results it carries.
        spawn_blocking(move || {
            let job = running.get(&job_id).ok_or_else(|| {
                // Reported as a lost claim, which is what it is: this executor
                // is not running that attempt, so anything it wrote would be
                // refused by the scheduler's own fence.
                step_error(QueueError::ClaimLost(format!(
                    "job {job_id} is not running on this executor"
                )))
            })?;
            if job.retry_count != attempt {
                return Err(step_error(QueueError::ClaimLost(format!(
                    "job {job_id} was dispatched on attempt {} and this one is {attempt}",
                    job.retry_count
                ))));
            }

            // The defaults, as the worker twin uses: §4.2's answer to a result
            // that will not fit is to store it elsewhere and memoize the handle,
            // so there is nothing for a shell knob to do. The caps that *hold*
            // are the scheduler's, inside the write's own transaction.
            let session = steps
                .open_session(&job, StepLimits::default())
                .map_err(step_error)?;
            Ok(JsStepSession::new(session.boxed()))
        })
        .await
        .map_err(join_to_napi_err)?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flexiq_core::worker::SchedulerMessage;

    /// A job as its dispatch frame describes it.
    ///
    /// Built through the frame's own inverse rather than as a `Job` literal, so
    /// a column added to a job never has to be mirrored here.
    fn dispatched(id: &str, attempt: i32) -> Job {
        let frame = format!(
            r#"{{"type":"job","id":"{id}","task_name":"t","payload_len":0,
                "retry_count":{attempt},"max_retries":3,"queue":"default",
                "timeout_ms":0,"namespace":null,"disabled_middleware":[],"metadata":null}}"#
        );
        serde_json::from_str::<SchedulerMessage>(&frame)
            .expect("the dispatch frame should parse")
            .into_dispatch(Vec::new())
            .expect("the frame should be a dispatch")
            .job
    }

    #[test]
    fn records_a_dispatch_for_as_long_as_the_attempt_runs() {
        let registry = Arc::new(RunningJobs::default());
        let guard = registry.enter(&dispatched("job-1", 2));

        let found = registry.get("job-1").expect("the dispatch should be found");
        assert_eq!(found.retry_count, 2);

        // The entry is what makes a step session possible, so releasing it is
        // what refuses one opened after the attempt reported — and it is also
        // the only thing keeping the map from growing for the life of the
        // process.
        drop(guard);
        assert!(registry.get("job-1").is_none());
    }

    #[test]
    fn keeps_concurrent_attempts_apart() {
        let registry = Arc::new(RunningJobs::default());
        let first = registry.enter(&dispatched("job-1", 0));
        let second = registry.enter(&dispatched("job-2", 0));

        drop(first);

        // An executor runs many jobs at once; one finishing must not take
        // another's session with it.
        assert!(registry.get("job-1").is_none());
        assert!(registry.get("job-2").is_some());
        drop(second);
    }
}
