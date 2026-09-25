//! The scheduler's lifecycle-event emit sites, kept here so the dispatch and
//! settle paths each gain a single call.
//!
//! Every helper is a no-op without a hub. With one, a transition costs a
//! constant number of allocations and no storage read: the queue, attempt and
//! epoch of a settled job come from the dispatch record its result was settled
//! against, carried out of the settle rather than looked up again.

use crate::events::{EventHub, EventType, JobEvent};
use crate::job::Job;
use crate::storage::DeadJob;

use super::{DispatchRecord, JobResult, ResultOutcome, Scheduler};

/// The attempt a failure reports for itself — the retries already spent —
/// used when no dispatch record names one (a reaper-recovered orphan).
pub(super) fn failure_attempt(result: &JobResult) -> Option<i32> {
    match result {
        JobResult::Failure { retry_count, .. } => Some(*retry_count),
        _ => None,
    }
}

impl Scheduler {
    /// `job.started` for a job about to be handed to the pool, or `None`
    /// without a hub. Built before the send moves the job, so the caller emits
    /// it only once the send succeeded.
    pub(super) fn started_event(&self, job: &Job, epoch: Option<i64>) -> Option<JobEvent> {
        let hub = self.events.as_ref()?;
        let mut event = self.job_event(EventType::JobStarted, &job.id, &job.queue, &job.task_name);
        event.attempt = Some(job.retry_count);
        event.epoch = epoch;
        if hub.wants_payload() {
            event.payload = Some(job.payload.clone());
        }
        Some(event)
    }

    /// Hand an event built by [`Self::started_event`] to the hub.
    pub(super) fn emit_event(&self, event: Option<JobEvent>) {
        if let (Some(hub), Some(event)) = (&self.events, event) {
            hub.emit(event);
        }
    }

    /// `job.dead` for a job the scheduler dead-lettered without running it, with
    /// the shed reason it wrote to the DLQ.
    pub(super) fn emit_shed(&self, job: &Job, reason: &str) {
        let Some(hub) = &self.events else {
            return;
        };
        let mut event = self.job_event(EventType::JobDead, &job.id, &job.queue, &job.task_name);
        event.attempt = Some(job.retry_count);
        event.reason = Some(reason.to_string());
        if hub.wants_payload() {
            event.payload = Some(job.payload.clone());
        }
        hub.emit(event);
    }

    /// `job.cancelled` for each job storage archived without running it: an
    /// expiry, or a dependent cascade-cancelled behind a dead-lettered parent.
    ///
    /// The rows are the archived ones storage handed back, so each event keeps
    /// the row's own namespace — the expiry sweep is unscoped — and takes its
    /// payload by move rather than by copy.
    pub(super) fn emit_cancelled_rows(&self, jobs: Vec<Job>, reason: &str) {
        let Some(hub) = &self.events else {
            return;
        };
        let with_payload = hub.wants_payload();
        for job in jobs {
            let mut event = JobEvent::new(
                EventType::JobCancelled,
                job.id,
                job.namespace,
                job.queue,
                job.task_name,
            );
            event.attempt = Some(job.retry_count);
            event.reason = Some(reason.to_string());
            if with_payload {
                event.payload = Some(job.payload);
            }
            hub.emit(event);
        }
    }

    /// `job.enqueued` for a job a periodic schedule just inserted. Only for an
    /// insert: a unique-key hit is a job some earlier firing already announced.
    pub(super) fn emit_periodic_enqueued(&self, job: Job) {
        let Some(hub) = &self.events else {
            return;
        };
        let mut event = JobEvent::new(
            EventType::JobEnqueued,
            job.id,
            job.namespace,
            job.queue,
            job.task_name,
        );
        event.attempt = Some(0);
        if hub.wants_payload() {
            event.payload = Some(job.payload);
        }
        hub.emit(event);
    }

    /// `job.enqueued` for the fresh job an auto-retry made of a dead-letter
    /// entry. No payload: the retry listing is blob-free by design, and reading
    /// the job back for its bytes would be a storage read per event.
    pub(super) fn emit_dlq_retried(&self, new_id: String, entry: &DeadJob) {
        let Some(hub) = &self.events else {
            return;
        };
        let mut event = JobEvent::new(
            EventType::JobEnqueued,
            new_id,
            entry.namespace.clone(),
            entry.queue.clone(),
            entry.task_name.clone(),
        );
        event.attempt = Some(0);
        hub.emit(event);
    }

    /// The events one settled outcome stands for. `Superseded` emits nothing:
    /// the job is proceeding elsewhere, and that attempt's events are its own.
    ///
    /// `record` is the dispatch the result was settled against, never a fresh
    /// lookup: a retried or slept job may already be re-dispatched, and the new
    /// record would stamp this attempt's events with the next one's id. With no
    /// record (a job this scheduler never dispatched) the queue is the
    /// outcome's own — empty for a `Success`, which carries none — and the
    /// attempt is `fallback_attempt`.
    pub(super) fn emit_outcome(
        &self,
        outcome: &ResultOutcome,
        record: Option<&DispatchRecord>,
        fallback_attempt: Option<i32>,
    ) {
        let Some(hub) = &self.events else {
            return;
        };
        let (event_type, job_id, task_name, queue, wall_time_ns) = match outcome {
            ResultOutcome::Success {
                job_id,
                task_name,
                wall_time_ns,
            } => (
                EventType::JobCompleted,
                job_id,
                task_name,
                "",
                *wall_time_ns,
            ),
            ResultOutcome::Retry {
                job_id,
                task_name,
                queue,
                wall_time_ns,
                ..
            }
            | ResultOutcome::DeadLettered {
                job_id,
                task_name,
                queue,
                wall_time_ns,
                ..
            } => (
                EventType::JobFailed,
                job_id,
                task_name,
                queue.as_str(),
                *wall_time_ns,
            ),
            ResultOutcome::Cancelled {
                job_id,
                task_name,
                queue,
                wall_time_ns,
            } => (
                EventType::JobCancelled,
                job_id,
                task_name,
                queue.as_str(),
                *wall_time_ns,
            ),
            ResultOutcome::Slept {
                job_id,
                task_name,
                queue,
                wall_time_ns,
                ..
            } => (
                EventType::JobSleeping,
                job_id,
                task_name,
                queue.as_str(),
                *wall_time_ns,
            ),
            ResultOutcome::Superseded { .. } => return,
        };

        let mut event = match record {
            Some(record) => {
                let mut event = self.job_event(event_type, job_id, &record.queue, task_name);
                event.attempt = Some(record.attempt);
                event.epoch = record.epoch;
                event
            }
            None => {
                let mut event = self.job_event(event_type, job_id, queue, task_name);
                event.attempt = fallback_attempt;
                event
            }
        };
        // 0 is the outcome's "not measured", which an event says by omission.
        event.wall_time_ns = (wall_time_ns != 0).then_some(wall_time_ns);

        match outcome {
            ResultOutcome::Retry {
                error, timed_out, ..
            } => emit_failure(hub, event, error, *timed_out, EventType::JobRetrying),
            ResultOutcome::DeadLettered {
                error, timed_out, ..
            } => emit_failure(hub, event, error, *timed_out, EventType::JobDead),
            ResultOutcome::Slept { wake_at, .. } => {
                event.wake_at_ms = Some(*wake_at);
                hub.emit(event);
            }
            _ => hub.emit(event),
        }
    }

    /// An event in this scheduler's namespace with every optional field empty.
    fn job_event(&self, event_type: EventType, job_id: &str, queue: &str, task: &str) -> JobEvent {
        JobEvent::new(event_type, job_id, self.namespace.clone(), queue, task)
    }
}

/// `job.failed`, then the transition it led to (`job.retrying` or `job.dead`).
/// Both carry the error, so a sink filtering on the second alone still sees why.
/// Both also name the attempt that failed: `job.retrying` is that attempt
/// being rescheduled, not the next attempt, which has its own `job.started`.
fn emit_failure(
    hub: &EventHub,
    mut failed: JobEvent,
    error: &str,
    timed_out: bool,
    then: EventType,
) {
    failed.error = Some(error.to_string());
    failed.timed_out = Some(timed_out);
    let mut next = failed.clone();
    next.event_type = then;
    hub.emit(failed);
    hub.emit(next);
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::events::reason;
    use crate::events::test_support::{delivered, recording_hub, Attempts};
    use crate::job::{now_millis, JobStatus, NewJob};
    use crate::resilience::rate_limiter::RateLimitConfig;
    use crate::resilience::retry::RetryPolicy;
    use crate::scheduler::result_handler::Settled;
    use crate::scheduler::{shed, SchedulerConfig, TaskConfig};
    use crate::storage::records::NewPeriodicTask;
    use crate::storage::sqlite::SqliteStorage;
    use crate::storage::{Storage, StorageBackend};

    type Channel = (
        tokio::sync::mpsc::Sender<Job>,
        tokio::sync::mpsc::Receiver<Job>,
    );

    fn scheduler() -> Scheduler {
        Scheduler::new(
            StorageBackend::Sqlite(SqliteStorage::in_memory().unwrap()),
            vec!["default".to_string()],
            SchedulerConfig::default(),
            None,
        )
    }

    fn with_hub(mut scheduler: Scheduler, extra: &str) -> (Scheduler, Arc<EventHub>, Attempts) {
        let (hub, attempts) = recording_hub(extra);
        scheduler.set_events(Arc::clone(&hub));
        (scheduler, hub, attempts)
    }

    fn new_job(task_name: &str, max_retries: i32) -> NewJob {
        NewJob {
            queue: "default".to_string(),
            task_name: task_name.to_string(),
            payload: vec![1, 2, 3],
            priority: 0,
            scheduled_at: now_millis(),
            max_retries,
            timeout_ms: 300_000,
            unique_key: None,
            metadata: None,
            notes: None,
            depends_on: vec![],
            expires_at: None,
            result_ttl_ms: None,
            namespace: None,
            debounce_key: None,
        }
    }

    /// Enqueue and dispatch one job through the real poller path.
    fn dispatch(scheduler: &Scheduler, channel: &Channel, task: &str, max_retries: i32) -> Job {
        let job = scheduler
            .storage
            .enqueue(new_job(task, max_retries))
            .unwrap();
        assert!(
            scheduler.try_dispatch(&channel.0).unwrap(),
            "{task} dispatched"
        );
        job
    }

    fn channel() -> Channel {
        tokio::sync::mpsc::channel(16)
    }

    fn types(events: &[JobEvent]) -> Vec<&'static str> {
        events.iter().map(|e| e.event_type.as_str()).collect()
    }

    fn failure(job: &Job, retry_count: i32, max_retries: i32) -> JobResult {
        JobResult::Failure {
            job_id: job.id.clone(),
            error: "boom".to_string(),
            retry_count,
            max_retries,
            task_name: job.task_name.clone(),
            wall_time_ns: 5,
            should_retry: true,
            timed_out: false,
        }
    }

    #[test]
    fn a_success_emits_started_then_completed_under_one_claim() {
        let (scheduler, hub, rec) = with_hub(scheduler(), "");
        let ch = channel();
        let job = dispatch(&scheduler, &ch, "ok", 3);
        scheduler
            .handle_result(JobResult::Success {
                job_id: job.id.clone(),
                result: None,
                task_name: "ok".to_string(),
                wall_time_ns: 7,
            })
            .unwrap();

        let events = delivered(&hub, &rec);
        assert_eq!(types(&events), ["job.started", "job.completed"]);
        let epoch = events[0].epoch;
        assert!(epoch.is_some(), "the claim's epoch rides the event");
        for event in &events {
            assert_eq!(event.queue, "default");
            assert_eq!(event.attempt, Some(0));
            assert_eq!(event.epoch, epoch);
            assert_eq!(event.task_name, "ok");
        }
        assert_eq!(events[1].wall_time_ns, Some(7));
        assert!(events[0].payload.is_none(), "no payload unless a sink asks");
    }

    #[test]
    fn a_payload_sink_gets_the_payload_on_started() {
        let (scheduler, hub, rec) = with_hub(scheduler(), r#","include_payload":true"#);
        let ch = channel();
        dispatch(&scheduler, &ch, "ok", 3);
        let events = delivered(&hub, &rec);
        assert_eq!(events[0].payload.as_deref(), Some(&[1u8, 2, 3][..]));
    }

    #[test]
    fn a_retryable_failure_emits_failed_then_retrying() {
        let (scheduler, hub, rec) = with_hub(scheduler(), "");
        let ch = channel();
        let job = dispatch(&scheduler, &ch, "flaky", 3);
        scheduler.handle_result(failure(&job, 0, 3)).unwrap();

        let events = delivered(&hub, &rec);
        assert_eq!(
            types(&events),
            ["job.started", "job.failed", "job.retrying"]
        );
        for event in &events[1..] {
            assert_eq!(event.error.as_deref(), Some("boom"));
            assert_eq!(event.timed_out, Some(false));
            assert_eq!(event.attempt, Some(0));
        }
        assert_ne!(events[1].id(), events[2].id());
    }

    #[test]
    fn a_final_failure_emits_failed_then_dead() {
        let (scheduler, hub, rec) = with_hub(scheduler(), "");
        let ch = channel();
        let job = dispatch(&scheduler, &ch, "doomed", 0);
        scheduler.handle_result(failure(&job, 0, 0)).unwrap();

        let events = delivered(&hub, &rec);
        assert_eq!(types(&events), ["job.started", "job.failed", "job.dead"]);
        assert_eq!(events[2].error.as_deref(), Some("boom"));
        assert_eq!(events[2].queue, "default");
    }

    #[test]
    fn a_failure_with_no_dispatch_record_falls_back_to_its_own_attempt() {
        // A reaper-recovered orphan: this scheduler never dispatched it.
        let (scheduler, hub, rec) = with_hub(scheduler(), "");
        let job = scheduler.storage.enqueue(new_job("orphan", 0)).unwrap();
        scheduler
            .storage
            .dequeue("default", now_millis() + 1000, None)
            .unwrap();
        scheduler.handle_result(failure(&job, 2, 0)).unwrap();

        let events = delivered(&hub, &rec);
        assert_eq!(types(&events), ["job.failed", "job.dead"]);
        assert_eq!(events[0].attempt, Some(2));
        assert_eq!(events[0].epoch, None);
        assert_eq!(events[0].queue, "default");
    }

    #[test]
    fn a_superseded_result_emits_nothing() {
        let (scheduler, hub, rec) = with_hub(scheduler(), "");
        let ch = channel();
        let job = dispatch(&scheduler, &ch, "stolen", 3);
        assert!(scheduler
            .storage
            .reclaim_execution(&job.id, scheduler.claim_owner(), "rescuer")
            .unwrap()
            .is_some());
        let outcome = scheduler
            .handle_result(JobResult::Success {
                job_id: job.id.clone(),
                result: None,
                task_name: "stolen".to_string(),
                wall_time_ns: 1,
            })
            .unwrap();
        assert!(matches!(outcome, ResultOutcome::Superseded { .. }));

        assert_eq!(types(&delivered(&hub, &rec)), ["job.started"]);
    }

    #[test]
    fn a_retry_redispatched_before_its_emit_keeps_its_own_attempt_and_epoch() {
        // A 0 ms retry delay makes the job due at once, so the poller can
        // claim attempt 1 between the settle and the emit.
        let mut scheduler = scheduler();
        scheduler.register_task(
            "flaky".to_string(),
            TaskConfig {
                retry_policy: RetryPolicy {
                    custom_delays_ms: Some(vec![0]),
                    ..RetryPolicy::default()
                },
                ..TaskConfig::default()
            },
        );
        let (scheduler, hub, rec) = with_hub(scheduler, "");
        let ch = channel();
        let job = dispatch(&scheduler, &ch, "flaky", 3);

        let Settled {
            outcome, record, ..
        } = scheduler.settle_result(failure(&job, 0, 3)).unwrap();
        assert!(matches!(outcome, ResultOutcome::Retry { .. }));
        assert!(
            scheduler.try_dispatch(&ch.0).unwrap(),
            "attempt 1 dispatched"
        );
        scheduler.emit_outcome(&outcome, record.as_ref(), None);

        let events = delivered(&hub, &rec);
        assert_eq!(
            types(&events),
            ["job.started", "job.started", "job.failed", "job.retrying"]
        );
        let (first, second) = (&events[0], &events[1]);
        assert_eq!(second.attempt, Some(1));
        assert_ne!(second.epoch, first.epoch, "the redispatch is a new claim");
        for settled in &events[2..] {
            assert_eq!(settled.attempt, Some(0), "{}", settled.id());
            assert_eq!(settled.epoch, first.epoch, "{}", settled.id());
        }
    }

    #[test]
    fn a_sleep_keeps_its_own_dispatch_when_the_record_is_replaced() {
        let (scheduler, hub, rec) = with_hub(scheduler(), "");
        let ch = channel();
        let job = dispatch(&scheduler, &ch, "napper", 3);
        let Settled {
            outcome, record, ..
        } = scheduler
            .settle_result(JobResult::Slept {
                job_id: job.id.clone(),
                task_name: "napper".to_string(),
                wake_at: now_millis(),
                wall_time_ns: 1,
            })
            .unwrap();
        // What a wake-up dispatch would write before the emit runs.
        scheduler.track_in_flight(&job.id, "napper", "other", 0, Some(i64::MAX));
        scheduler.emit_outcome(&outcome, record.as_ref(), None);

        let events = delivered(&hub, &rec);
        assert_eq!(types(&events), ["job.started", "job.sleeping"]);
        assert_eq!(events[1].epoch, events[0].epoch);
        assert_eq!(events[1].queue, "default");
    }

    #[test]
    fn a_sleep_emits_sleeping_with_its_deadline() {
        let (scheduler, hub, rec) = with_hub(scheduler(), "");
        let ch = channel();
        let job = dispatch(&scheduler, &ch, "napper", 3);
        let wake_at = now_millis() + 60_000;
        scheduler
            .handle_result(JobResult::Slept {
                job_id: job.id.clone(),
                task_name: "napper".to_string(),
                wake_at,
                wall_time_ns: 3,
            })
            .unwrap();

        let events = delivered(&hub, &rec);
        assert_eq!(types(&events), ["job.started", "job.sleeping"]);
        assert_eq!(events[1].wake_at_ms, Some(wake_at));
        assert_eq!(events[1].attempt, Some(0));
        assert_eq!(events[1].epoch, events[0].epoch);
    }

    #[test]
    fn a_rate_limit_shed_emits_dead_with_its_reason() {
        let mut scheduler = scheduler();
        scheduler.register_task(
            "shed_task".to_string(),
            TaskConfig {
                rate_limit: Some(RateLimitConfig {
                    max_tokens: 1.0,
                    refill_rate: 0.0,
                }),
                on_excess: shed::OnExcess::Drop,
                ..TaskConfig::default()
            },
        );
        let (scheduler, hub, rec) = with_hub(scheduler, "");
        let ch = channel();
        dispatch(&scheduler, &ch, "shed_task", 3);
        dispatch(&scheduler, &ch, "shed_task", 3);

        let events = delivered(&hub, &rec);
        assert_eq!(types(&events), ["job.started", "job.dead"]);
        let reason = events[1].reason.as_deref().unwrap();
        assert!(
            reason.starts_with(shed::RATE_LIMIT_REASON_PREFIX),
            "{reason}"
        );
        assert_eq!(events[1].attempt, Some(0));
    }

    #[test]
    fn a_mixed_batch_emits_one_terminal_event_per_result() {
        let (scheduler, hub, rec) = with_hub(scheduler(), "");
        let ch = channel();
        let ok = dispatch(&scheduler, &ch, "ok", 3);
        let flaky = dispatch(&scheduler, &ch, "flaky", 3);
        let stop = dispatch(&scheduler, &ch, "stop", 3);
        let outcomes = scheduler.handle_results(vec![
            JobResult::Success {
                job_id: ok.id.clone(),
                result: None,
                task_name: "ok".to_string(),
                wall_time_ns: 1,
            },
            failure(&flaky, 0, 3),
            JobResult::Cancelled {
                job_id: stop.id.clone(),
                task_name: "stop".to_string(),
                wall_time_ns: 1,
            },
        ]);
        assert!(outcomes.iter().all(Result::is_ok));

        let events = delivered(&hub, &rec);
        let settled: Vec<(String, &str)> = events
            .iter()
            .filter(|e| e.event_type != EventType::JobStarted)
            .map(|e| (e.job_id.clone(), e.event_type.as_str()))
            .collect();
        assert_eq!(settled.len(), 4, "{settled:?}");
        for expected in [
            (ok.id.clone(), "job.completed"),
            (flaky.id.clone(), "job.failed"),
            (flaky.id.clone(), "job.retrying"),
            (stop.id.clone(), "job.cancelled"),
        ] {
            assert!(settled.contains(&expected), "{expected:?} in {settled:?}");
        }
        let completed = events
            .iter()
            .find(|e| e.event_type == EventType::JobCompleted)
            .unwrap();
        assert_eq!(completed.queue, "default", "the batch path names the queue");
    }

    /// A job due now that expired a second ago, in `namespace`.
    fn expired_job(task: &str, namespace: Option<&str>) -> NewJob {
        NewJob {
            expires_at: Some(now_millis() - 1_000),
            namespace: namespace.map(str::to_string),
            ..new_job(task, 3)
        }
    }

    fn dependent_of(parent: &Job, task: &str) -> NewJob {
        NewJob {
            depends_on: vec![parent.id.clone()],
            ..new_job(task, 3)
        }
    }

    /// Every `job.cancelled` among `events`, as `(job_id, reason)`.
    fn cancelled(events: &[JobEvent]) -> Vec<(String, Option<String>)> {
        events
            .iter()
            .filter(|e| e.event_type == EventType::JobCancelled)
            .map(|e| (e.job_id.clone(), e.reason.clone()))
            .collect()
    }

    #[test]
    fn the_expiry_sweep_emits_cancelled_in_each_jobs_own_namespace() {
        let (scheduler, hub, rec) = with_hub(scheduler(), "");
        let plain = scheduler
            .storage
            .enqueue(expired_job("stale", None))
            .unwrap();
        let tenant = scheduler
            .storage
            .enqueue(expired_job("stale", Some("tenant-a")))
            .unwrap();
        scheduler.reap_stale().unwrap();

        let mut events = delivered(&hub, &rec);
        events.sort_by(|a, b| a.namespace.cmp(&b.namespace));
        assert_eq!(types(&events), ["job.cancelled", "job.cancelled"]);
        let expected = [(&plain, None), (&tenant, Some("tenant-a".to_string()))];
        for (event, (job, namespace)) in events.iter().zip(expected) {
            assert_eq!(event.job_id, job.id);
            assert_eq!(event.namespace, namespace, "the row's, not the scheduler's");
            assert_eq!(event.reason.as_deref(), Some(reason::EXPIRED));
            assert_eq!(event.attempt, Some(0));
            assert_eq!(event.epoch, None);
            assert_eq!(event.queue, "default");
            assert_eq!(event.task_name, "stale");
            assert!(event.payload.is_none(), "no payload unless a sink asks");
        }
    }

    #[test]
    fn a_payload_sink_gets_the_expired_rows_payload() {
        let (scheduler, hub, rec) = with_hub(scheduler(), r#","include_payload":true"#);
        scheduler
            .storage
            .enqueue(expired_job("stale", None))
            .unwrap();
        scheduler.reap_stale().unwrap();

        let events = delivered(&hub, &rec);
        assert_eq!(events[0].payload.as_deref(), Some(&[1u8, 2, 3][..]));
    }

    #[test]
    fn an_expiry_found_at_dispatch_emits_cancelled() {
        let (scheduler, hub, rec) = with_hub(scheduler(), "");
        let job = scheduler
            .storage
            .enqueue(expired_job("stale", None))
            .unwrap();
        assert!(!scheduler.try_dispatch(&channel().0).unwrap());

        let events = delivered(&hub, &rec);
        assert_eq!(
            cancelled(&events),
            [(job.id, Some(reason::EXPIRED_BEFORE_EXECUTION.to_string()))]
        );
        assert_eq!(events[0].attempt, Some(0));
    }

    #[test]
    fn an_expiry_found_by_a_batch_dispatch_emits_cancelled() {
        let mut scheduler = Scheduler::new(
            StorageBackend::Sqlite(SqliteStorage::in_memory().unwrap()),
            vec!["default".to_string()],
            SchedulerConfig {
                batch_size: Some(4),
                ..SchedulerConfig::default()
            },
            None,
        );
        let (hub, rec) = recording_hub("");
        scheduler.set_events(Arc::clone(&hub));
        let job = scheduler
            .storage
            .enqueue(expired_job("stale", None))
            .unwrap();
        assert!(!scheduler.try_dispatch_batch(&channel().0).unwrap());

        assert_eq!(
            cancelled(&delivered(&hub, &rec)),
            [(job.id, Some(reason::EXPIRED_BEFORE_EXECUTION.to_string()))]
        );
    }

    /// A parent that will dead-letter on its first failure, with a child
    /// waiting on it and a grandchild waiting on the child.
    fn doomed_chain(scheduler: &Scheduler, ch: &Channel) -> (Job, Vec<String>) {
        let parent = dispatch(scheduler, ch, "doomed", 0);
        let child = scheduler
            .storage
            .enqueue(dependent_of(&parent, "child"))
            .unwrap();
        let grandchild = scheduler
            .storage
            .enqueue(dependent_of(&child, "grandchild"))
            .unwrap();
        (parent, vec![child.id, grandchild.id])
    }

    /// The `job.dead` comes first, then one `job.cancelled` per dependent.
    fn assert_cascade_after_dead(events: &[JobEvent], dependents: &[String]) {
        let dead = events
            .iter()
            .position(|e| e.event_type == EventType::JobDead)
            .expect("the parent's job.dead");
        let tail = &events[dead + 1..];
        let mut ids: Vec<String> = cancelled(tail).into_iter().map(|(id, _)| id).collect();
        ids.sort();
        let mut expected = dependents.to_vec();
        expected.sort();
        assert_eq!(ids, expected, "one cancel per dependent, after the parent");
        assert_eq!(cancelled(events).len(), dependents.len(), "each only once");
        for event in tail {
            assert_eq!(event.event_type, EventType::JobCancelled);
            assert_eq!(event.reason.as_deref(), Some(reason::DEPENDENCY_FAILED));
            assert_eq!(event.attempt, Some(0));
            assert_eq!(event.epoch, None);
        }
    }

    #[test]
    fn a_dead_lettered_parent_emits_a_cancel_per_dependent() {
        let (scheduler, hub, rec) = with_hub(scheduler(), "");
        let ch = channel();
        let (parent, dependents) = doomed_chain(&scheduler, &ch);
        scheduler.handle_result(failure(&parent, 0, 0)).unwrap();

        let events = delivered(&hub, &rec);
        assert_eq!(
            types(&events[..3]),
            ["job.started", "job.failed", "job.dead"]
        );
        assert_cascade_after_dead(&events, &dependents);
    }

    #[test]
    fn a_dead_lettered_parent_in_a_batch_emits_a_cancel_per_dependent() {
        let (scheduler, hub, rec) = with_hub(scheduler(), "");
        let ch = channel();
        let (parent, dependents) = doomed_chain(&scheduler, &ch);
        let outcomes = scheduler.handle_results(vec![failure(&parent, 0, 0)]);
        assert!(outcomes.iter().all(Result::is_ok));

        assert_cascade_after_dead(&delivered(&hub, &rec), &dependents);
    }

    #[test]
    fn a_rate_limit_shed_parent_cascades() {
        let mut scheduler = scheduler();
        scheduler.register_task(
            "shed_task".to_string(),
            TaskConfig {
                rate_limit: Some(RateLimitConfig {
                    max_tokens: 1.0,
                    refill_rate: 0.0,
                }),
                on_excess: shed::OnExcess::Drop,
                ..TaskConfig::default()
            },
        );
        let (scheduler, hub, rec) = with_hub(scheduler, "");
        let ch = channel();
        dispatch(&scheduler, &ch, "shed_task", 3);
        let parent = scheduler.storage.enqueue(new_job("shed_task", 3)).unwrap();
        let child = scheduler
            .storage
            .enqueue(dependent_of(&parent, "child"))
            .unwrap();
        assert!(
            scheduler.try_dispatch(&ch.0).unwrap(),
            "the shed is progress"
        );

        let events = delivered(&hub, &rec);
        assert_eq!(types(&events), ["job.started", "job.dead", "job.cancelled"]);
        assert_eq!(events[1].job_id, parent.id);
        assert_cascade_after_dead(&events, &[child.id]);
    }

    #[test]
    fn a_periodic_firing_emits_enqueued_once_per_slot() {
        let (scheduler, hub, rec) = with_hub(scheduler(), "");
        scheduler
            .storage
            .register_periodic(&NewPeriodicTask {
                name: "nightly".to_string(),
                task_name: "periodic_task".to_string(),
                cron_expr: "* * * * * *".to_string(),
                args: Some(vec![9]),
                kwargs: None,
                queue: "default".to_string(),
                enabled: true,
                next_run: now_millis() - 1_000,
                timezone: None,
                namespace: Some("tenant-a".to_string()),
            })
            .unwrap();
        scheduler.check_periodic().unwrap();

        // A second firing of the same slot, as a racing scheduler would make:
        // the unique key answers with the first job, which is announced once.
        let task = scheduler
            .storage
            .list_periodic(Some("tenant-a"))
            .unwrap()
            .remove(0);
        let jobs = scheduler
            .storage
            .list_jobs(
                Some(JobStatus::Pending as i32),
                None,
                Some("periodic_task"),
                10,
                0,
                None,
            )
            .unwrap();
        assert_eq!(jobs.len(), 1);
        scheduler
            .fire_periodic(&task, jobs[0].scheduled_at)
            .unwrap();
        assert_eq!(
            scheduler
                .storage
                .list_jobs(
                    Some(JobStatus::Pending as i32),
                    None,
                    Some("periodic_task"),
                    10,
                    0,
                    None
                )
                .unwrap()
                .len(),
            1,
            "the second firing deduplicated"
        );

        let events = delivered(&hub, &rec);
        assert_eq!(types(&events), ["job.enqueued"]);
        let event = &events[0];
        assert_eq!(event.job_id, jobs[0].id);
        assert_eq!(event.namespace.as_deref(), Some("tenant-a"));
        assert_eq!(event.task_name, "periodic_task");
        assert_eq!(event.queue, "default");
        assert_eq!(event.attempt, Some(0));
        assert_eq!(event.epoch, None);
        assert!(event.payload.is_none(), "no payload unless a sink asks");
    }

    #[test]
    fn a_dlq_auto_retry_emits_enqueued_with_the_new_id() {
        let scheduler = Scheduler::new(
            StorageBackend::Sqlite(SqliteStorage::in_memory().unwrap()),
            vec!["default".to_string()],
            SchedulerConfig {
                dlq_auto_retry_delay_ms: Some(0),
                dlq_auto_retry_max: 3,
                ..SchedulerConfig::default()
            },
            None,
        );
        let (scheduler, hub, rec) = with_hub(scheduler, "");
        let failed = scheduler.storage.enqueue(new_job("flaky", 0)).unwrap();
        scheduler
            .storage
            .move_to_dlq(&failed, "ConnectionError: refused", None)
            .unwrap();
        scheduler.auto_retry_dlq().unwrap();

        let fresh = scheduler
            .storage
            .list_jobs(
                Some(JobStatus::Pending as i32),
                None,
                Some("flaky"),
                10,
                0,
                None,
            )
            .unwrap();
        assert_eq!(fresh.len(), 1);
        assert_ne!(fresh[0].id, failed.id, "the retry is a fresh job");

        let events = delivered(&hub, &rec);
        assert_eq!(types(&events), ["job.enqueued"]);
        assert_eq!(events[0].job_id, fresh[0].id);
        assert_eq!(events[0].queue, "default");
        assert_eq!(events[0].task_name, "flaky");
        assert_eq!(events[0].namespace, None);
        assert_eq!(events[0].attempt, Some(0));
    }

    #[test]
    fn no_hub_emits_nothing_and_settles_as_before() {
        let scheduler = scheduler();
        let ch = channel();
        let ok = dispatch(&scheduler, &ch, "ok", 3);
        let flaky = dispatch(&scheduler, &ch, "flaky", 3);
        let outcome = scheduler
            .handle_result(JobResult::Success {
                job_id: ok.id.clone(),
                result: None,
                task_name: "ok".to_string(),
                wall_time_ns: 1,
            })
            .unwrap();
        assert!(matches!(outcome, ResultOutcome::Success { .. }));
        let outcomes = scheduler.handle_results(vec![failure(&flaky, 0, 3)]);
        assert!(matches!(outcomes[0], Ok(ResultOutcome::Retry { .. })));
    }
}
