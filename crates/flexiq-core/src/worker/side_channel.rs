//! The storage an attached executor does not have.
//!
//! An executor exists so the app image needs no database credentials, which
//! leaves five job-scoped operations with nowhere to go: progress, task logs,
//! published partials, the dashboard's middleware toggles, and job metadata.
//! The scheduler *does* hold the connection, so the executor asks it instead of
//! reaching for a database of its own.
//!
//! This module is the scheduler's half of that arrangement: the narrow surface
//! [`RemoteDispatcher`](super::remote::RemoteDispatcher) needs, plus the
//! storage-backed implementation a real deployment installs. It is a trait
//! rather than an `Arc<dyn Storage>` so the dispatcher can be tested against a
//! fake, and so the settings key a toggle list lives under is spelled in one
//! place instead of at every call site.
//!
//! Durable steps are the sixth operation and the odd one out: every other entry
//! here is fire-and-forget, and a step commit is the one thing whose answer the
//! task is waiting on. "The write may or may not have landed" means "the step
//! may or may not re-run", so those three methods return a `Result` the
//! scheduler acknowledges rather than a failure it logs and drops.

use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};

use crate::error::{QueueError, Result};
use crate::step::StepLimits;
use crate::storage::records::{JobStep, NewJobStep, SleepOutcome, StepCommit};
use crate::storage::{Storage, StorageBackend};

/// How long a resolved toggle list is reused before it is re-read.
///
/// Matches the SDK worker caches (`_MW_CHAIN_TTL` in the Python shell): a
/// dashboard toggle is rare and a dispatch is hot, so a bounded staleness
/// window is worth far more than a settings read per job.
const DISABLE_CACHE_TTL: Duration = Duration::from_secs(5);

/// What the scheduler does on an executor's behalf, because the executor has no
/// storage of its own.
///
/// Every method is infallible by design: a task that only wanted to report
/// progress must not fail because the database was briefly unhappy, so a
/// failure is logged and dropped rather than propagated to the job.
pub trait SideChannel: Send + Sync + 'static {
    /// Record a running job's progress (0-100).
    fn update_progress(&self, job_id: &str, progress: i32, namespace: Option<&str>);

    /// Append one structured log line. `extra` is pre-encoded JSON; a published
    /// partial arrives here as level `result`.
    fn write_task_log(
        &self,
        job_id: &str,
        task_name: &str,
        level: &str,
        message: &str,
        extra: Option<&str>,
        namespace: Option<&str>,
    );

    /// Middleware the operator has disabled for `task_name`, for attaching to a
    /// dispatch frame.
    fn disabled_middleware(&self, task_name: &str) -> Vec<String>;

    // ── Durable inline steps ──────────────────────────────────────
    //
    // Fallible, unlike everything above: a step commit is what a running task
    // is blocked on, and an unacknowledged one is indistinguishable from one
    // that never happened. Defaulted so a channel that predates steps — or a
    // test fake — keeps compiling and simply never advertises `CAP_STEPS`.

    /// Whether durable steps work across this channel. `false` withholds
    /// [`CAP_STEPS`](super::protocol::CAP_STEPS), and every step is then
    /// refused rather than run un-memoized.
    fn supports_steps(&self) -> bool {
        false
    }

    /// The steps a job has already committed, read once at dispatch so the
    /// snapshot can ride the job frame.
    fn job_steps(&self, job_id: &str, namespace: Option<&str>) -> Result<Vec<JobStep>> {
        let _ = (job_id, namespace);
        Err(steps_unsupported())
    }

    /// Commit one step under the fence the *scheduler* holds.
    ///
    /// `owner`, `attempt` and `epoch` come from the dispatch this scheduler
    /// recorded, never off the frame: an owner an executor supplies is an owner
    /// it can forge, and a forged one writes into the live attempt's sequence.
    fn record_step(
        &self,
        step: &NewJobStep<'_>,
        owner: &str,
        attempt: i32,
        epoch: Option<i64>,
        namespace: Option<&str>,
    ) -> Result<StepCommit> {
        let _ = (step, owner, attempt, epoch, namespace);
        Err(steps_unsupported())
    }

    /// Commit a `step.sleep`: the row, the claim release and the reschedule as
    /// one fenced operation. `wake_at` is a candidate; the answer carries the
    /// deadline the job was actually rescheduled to.
    fn sleep_job(
        &self,
        step: &NewJobStep<'_>,
        owner: &str,
        attempt: i32,
        epoch: Option<i64>,
        wake_at: i64,
        namespace: Option<&str>,
    ) -> Result<SleepOutcome> {
        let _ = (step, owner, attempt, epoch, wake_at, namespace);
        Err(steps_unsupported())
    }
}

/// What a channel with no step store answers. Permanent by
/// [`classify_step_failure`](crate::step::classify_step_failure): retrying
/// reaches the same scheduler, which will still have no store.
fn steps_unsupported() -> QueueError {
    QueueError::Config("this scheduler does not implement the step store".to_string())
}

/// The [`SideChannel`] a real deployment installs: straight through to storage.
pub struct StorageSideChannel {
    storage: StorageBackend,
    /// Resolved toggle lists, with the instant each was read. Bounded by the
    /// number of distinct task names, which is the app's own vocabulary.
    disables: Mutex<HashMap<String, (Vec<String>, Instant)>>,
    /// Caps every step commit crossing this channel is held to.
    ///
    /// The scheduler's, not the executor's, and deliberately: the caps are the
    /// operator's policy, and the operator runs the scheduler. An executor
    /// checks its own copy first only so the error can name the value the
    /// caller passed; this is the check that holds.
    step_limits: StepLimits,
}

impl StorageSideChannel {
    /// Wrap the scheduler's storage, holding steps to the default caps.
    pub fn new(storage: StorageBackend) -> Self {
        Self {
            storage,
            disables: Mutex::new(HashMap::new()),
            step_limits: StepLimits::default(),
        }
    }

    /// Hold step commits to `limits` rather than the defaults.
    pub fn with_step_limits(mut self, limits: StepLimits) -> Self {
        self.step_limits = limits;
        self
    }

    /// The settings key holding `task_name`'s disable list.
    ///
    /// Kept identical to the key every dashboard writes
    /// (`middleware:disabled:<task_name>`), which is already reserved in
    /// [`crate::settings::RESERVED_SETTING_PREFIXES`] so a generic settings API
    /// can neither read nor forge one.
    fn disable_key(task_name: &str) -> String {
        format!("middleware:disabled:{task_name}")
    }

    /// Read and decode the disable list, treating every failure as "nothing
    /// disabled" — the same non-fatal stance the SDK workers take, because a
    /// settings blip must not silently change which middleware runs.
    fn read_disabled(&self, task_name: &str) -> Vec<String> {
        let raw = match self.storage.get_setting(&Self::disable_key(task_name)) {
            Ok(Some(raw)) => raw,
            Ok(None) => return Vec::new(),
            Err(error) => {
                log::warn!(
                    "[flexiq] could not read the middleware disable list for '{task_name}': \
                     {error}; dispatching with none disabled"
                );
                return Vec::new();
            }
        };
        serde_json::from_str::<Vec<String>>(&raw).unwrap_or_else(|error| {
            log::warn!(
                "[flexiq] the middleware disable list for '{task_name}' is not a JSON array of \
                 names ({error}); dispatching with none disabled"
            );
            Vec::new()
        })
    }
}

/// Recover a guard from a poisoned lock instead of cascading the panic. The
/// state behind it is a cache, which stays safe to read.
fn recover<T>(poisoned: PoisonError<T>) -> T {
    poisoned.into_inner()
}

impl SideChannel for StorageSideChannel {
    fn update_progress(&self, job_id: &str, progress: i32, namespace: Option<&str>) {
        // Checked here rather than left to storage: the in-process SDK paths
        // reject or clamp before they ever call storage, so an attached
        // executor's value is the one that would otherwise reach it unchecked
        // and come back as a generic write failure.
        if !(0..=100).contains(&progress) {
            log::warn!(
                "[flexiq] executor reported progress {progress} for job {job_id}, which is \
                 outside 0-100; dropping it"
            );
            return;
        }
        if let Err(error) = self.storage.update_progress(job_id, progress, namespace) {
            log::warn!("[flexiq] could not record progress for job {job_id}: {error}");
        }
    }

    fn write_task_log(
        &self,
        job_id: &str,
        task_name: &str,
        level: &str,
        message: &str,
        extra: Option<&str>,
        namespace: Option<&str>,
    ) {
        let written = self
            .storage
            .write_task_log(job_id, task_name, level, message, extra, namespace);
        if let Err(error) = written {
            log::warn!("[flexiq] could not write a task log for job {job_id}: {error}");
        }
    }

    fn disabled_middleware(&self, task_name: &str) -> Vec<String> {
        {
            let cache = self.disables.lock().unwrap_or_else(recover);
            if let Some((disabled, read_at)) = cache.get(task_name) {
                if read_at.elapsed() < DISABLE_CACHE_TTL {
                    return disabled.clone();
                }
            }
        }

        // Read outside the lock: a slow settings backend would otherwise stall
        // every other task's dispatch behind this one.
        let disabled = self.read_disabled(task_name);
        self.disables
            .lock()
            .unwrap_or_else(recover)
            .insert(task_name.to_string(), (disabled.clone(), Instant::now()));
        disabled
    }

    /// Capable *and* permitted. An attached executor holds no storage, so it
    /// has no floor of its own to read — this is where the deployment's floor
    /// reaches it, by deciding whether `CAP_STEPS` is offered at all.
    ///
    /// Read once per attach rather than per job: the answer is a deployment's,
    /// and an operator who raises the dial mid-flight gets it on the next
    /// attach, which is also when a fleet finishes rolling.
    ///
    /// A floor that will not parse is `false`. The signature has nowhere to put
    /// an error, and of the two ways to be wrong here, withholding steps from a
    /// healthy deployment is the recoverable one.
    fn supports_steps(&self) -> bool {
        if !self.storage.supports_steps() {
            return false;
        }
        match crate::contract::ensure_steps_allowed(&self.storage) {
            Ok(()) => true,
            Err(error) => {
                log::warn!(
                    "attach: not offering durable steps to executors — {error}; \
                     jobs that call step.run will be refused"
                );
                false
            }
        }
    }

    fn job_steps(&self, job_id: &str, namespace: Option<&str>) -> Result<Vec<JobStep>> {
        self.storage.get_job_steps(job_id, namespace)
    }

    fn record_step(
        &self,
        step: &NewJobStep<'_>,
        owner: &str,
        attempt: i32,
        epoch: Option<i64>,
        namespace: Option<&str>,
    ) -> Result<StepCommit> {
        self.storage
            .record_step_result(step, owner, attempt, epoch, &self.step_limits, namespace)
    }

    fn sleep_job(
        &self,
        step: &NewJobStep<'_>,
        owner: &str,
        attempt: i32,
        epoch: Option<i64>,
        wake_at: i64,
        namespace: Option<&str>,
    ) -> Result<SleepOutcome> {
        self.storage.sleep_job(
            step,
            owner,
            attempt,
            epoch,
            wake_at,
            &self.step_limits,
            namespace,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::job::{now_millis, NewJob};
    use crate::storage::sqlite::SqliteStorage;

    fn channel() -> StorageSideChannel {
        StorageSideChannel::new(StorageBackend::Sqlite(
            SqliteStorage::in_memory().expect("in-memory storage"),
        ))
    }

    fn enqueued(channel: &StorageSideChannel) -> String {
        channel
            .storage
            .enqueue(NewJob {
                queue: "default".to_string(),
                task_name: "resize".to_string(),
                payload: vec![1, 2, 3],
                priority: 0,
                scheduled_at: now_millis(),
                max_retries: 3,
                timeout_ms: 300_000,
                unique_key: None,
                metadata: None,
                notes: None,
                depends_on: vec![],
                expires_at: None,
                result_ttl_ms: None,
                namespace: None,
                debounce_key: None,
            })
            .expect("enqueue")
            .id
    }

    #[test]
    fn progress_outside_the_documented_range_is_dropped() {
        // An attached executor is the one caller that reaches storage without
        // passing an SDK boundary that already rejects or clamps, so the check
        // has to happen before the write rather than inside it.
        let channel = channel();
        let job_id = enqueued(&channel);

        channel.update_progress(&job_id, 40, None);
        channel.update_progress(&job_id, 101, None);
        channel.update_progress(&job_id, -1, None);

        let job = channel
            .storage
            .get_job(&job_id, None)
            .expect("get")
            .expect("job");
        assert_eq!(
            job.progress,
            Some(40),
            "an out-of-range report must not disturb the last good value"
        );
    }

    #[test]
    fn an_unset_toggle_list_is_empty_rather_than_an_error() {
        assert!(channel().disabled_middleware("resize").is_empty());
    }

    #[test]
    fn a_stored_toggle_list_is_decoded_and_then_cached() {
        let channel = channel();
        channel
            .storage
            .set_setting(
                &StorageSideChannel::disable_key("resize"),
                r#"["tracing","app.mw.Audit"]"#,
            )
            .expect("set");

        assert_eq!(
            channel.disabled_middleware("resize"),
            ["tracing", "app.mw.Audit"]
        );

        // Within the TTL the cached list is reused, so a dispatch costs no
        // settings read.
        channel
            .storage
            .set_setting(&StorageSideChannel::disable_key("resize"), r#"[]"#)
            .expect("set");
        assert_eq!(
            channel.disabled_middleware("resize"),
            ["tracing", "app.mw.Audit"]
        );
    }

    #[test]
    fn a_malformed_toggle_list_disables_nothing() {
        // Failing open matters more than failing loud here: the alternative is
        // silently running a chain the operator thinks they turned off, or
        // failing every job for a bad settings row.
        let channel = channel();
        channel
            .storage
            .set_setting(&StorageSideChannel::disable_key("resize"), "not json")
            .expect("set");
        assert!(channel.disabled_middleware("resize").is_empty());
    }

    #[test]
    fn the_toggle_key_matches_what_a_dashboard_writes() {
        assert_eq!(
            StorageSideChannel::disable_key("resize"),
            "middleware:disabled:resize"
        );
        assert!(crate::settings::is_reserved_setting_key(
            &StorageSideChannel::disable_key("resize")
        ));
    }

    /// A job dequeued and claimed by `owner`, ready for a step commit.
    fn claimed(channel: &StorageSideChannel, owner: &str) -> String {
        let job_id = enqueued(channel);
        channel
            .storage
            .dequeue("default", now_millis() + 1_000, None)
            .expect("dequeue");
        assert!(channel
            .storage
            .claim_execution(&job_id, owner)
            .expect("claim")
            .is_some());
        job_id
    }

    #[test]
    fn a_step_commits_under_the_owner_the_scheduler_supplies() {
        let channel = channel();
        let job_id = claimed(&channel, "scheduler-1");

        let step = NewJobStep {
            job_id: &job_id,
            seq: 0,
            step_key: "charge#0",
            kind: crate::storage::records::StepKind::Run,
            result: Some(b"receipt"),
        };
        assert_eq!(
            channel
                .record_step(&step, "scheduler-1", 0, None, None)
                .expect("commit"),
            StepCommit::Committed
        );

        // And a retransmission after a lost ack is a success, not a second row.
        assert_eq!(
            channel
                .record_step(&step, "scheduler-1", 0, None, None)
                .expect("recommit"),
            StepCommit::AlreadyCommitted
        );
        assert_eq!(channel.job_steps(&job_id, None).expect("read").len(), 1);
    }

    #[test]
    fn a_commit_naming_another_owner_loses_the_fence() {
        // The reason a step frame carries no owner: this is what an executor
        // would be able to forge if it did.
        let channel = channel();
        let job_id = claimed(&channel, "scheduler-1");

        let refused = channel.record_step(
            &NewJobStep {
                job_id: &job_id,
                seq: 0,
                step_key: "charge#0",
                kind: crate::storage::records::StepKind::Run,
                result: Some(b"receipt"),
            },
            "an-impostor",
            0,
            None,
            None,
        );
        assert!(
            matches!(refused, Err(QueueError::ClaimLost(_))),
            "{refused:?}"
        );
        assert!(channel.job_steps(&job_id, None).expect("read").is_empty());
    }

    #[test]
    fn a_channel_with_no_step_store_refuses_rather_than_reporting_none() {
        // "No steps recorded" is the one answer that re-runs a charge, so the
        // default is an error and the capability is simply withheld.
        struct Bare;
        impl SideChannel for Bare {
            fn update_progress(&self, _: &str, _: i32, _: Option<&str>) {}
            fn write_task_log(
                &self,
                _: &str,
                _: &str,
                _: &str,
                _: &str,
                _: Option<&str>,
                _: Option<&str>,
            ) {
            }
            fn disabled_middleware(&self, _: &str) -> Vec<String> {
                Vec::new()
            }
        }

        assert!(!Bare.supports_steps());
        assert!(Bare.job_steps("job-1", None).is_err());
        assert_eq!(
            crate::step::classify_step_failure(&steps_unsupported()),
            crate::step::StepFailure::Permanent
        );
    }

    /// D12 reaches an attached executor here, and nowhere else: it holds no
    /// storage, so the only statement it ever gets about the deployment's floor
    /// is whether the scheduler offered `CAP_STEPS` at all.
    #[test]
    fn the_capability_is_withheld_below_the_contract_floor() {
        let channel = channel();
        assert!(
            channel.supports_steps(),
            "a storage this build created seeds the floor, so steps start on"
        );

        crate::contract::set_min_contract(&channel.storage, crate::MIN_CONTRACT_VERSION)
            .expect("lower the floor");
        assert!(
            !channel.supports_steps(),
            "an un-raised deployment may hold a worker that cannot read job_steps"
        );

        crate::contract::set_min_contract(&channel.storage, crate::STEPS_CONTRACT_LEVEL)
            .expect("raise the floor");
        assert!(channel.supports_steps());
    }

    /// Fails closed: the signature has no room for an error, and withholding
    /// steps from a healthy deployment is the recoverable way to be wrong.
    #[test]
    fn an_unreadable_floor_withholds_the_capability() {
        let channel = channel();
        channel
            .storage
            .set_setting(crate::contract::CONTRACT_FLOOR_SETTING, "not-a-level")
            .expect("write");

        assert!(!channel.supports_steps());
    }
}
