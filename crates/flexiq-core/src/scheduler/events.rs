//! The scheduler's lifecycle-event emit sites, kept here so the dispatch and
//! settle paths each gain a single call.
//!
//! Every helper is a no-op without a hub. With one, a transition costs a
//! constant number of allocations and no storage read: the queue, attempt and
//! epoch of a settled job come from its dispatch record, which is retired, not
//! forgotten, once the slot is released.

use crate::events::{EventHub, EventType, JobEvent};
use crate::job::Job;

use super::{JobResult, ResultOutcome, Scheduler};

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

    /// The events one settled outcome stands for. `Superseded` emits nothing:
    /// the job is proceeding elsewhere, and that attempt's events are its own.
    pub(super) fn emit_outcome(&self, outcome: &ResultOutcome, fallback_attempt: Option<i32>) {
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

        let mut event = match self.dispatch_context(job_id) {
            Some((queue, attempt, epoch)) => {
                let mut event = self.job_event(event_type, job_id, &queue, task_name);
                event.attempt = Some(attempt);
                event.epoch = epoch;
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

    /// Queue, attempt and epoch of the job's last dispatch, live or retired.
    /// Read in place rather than through `last_dispatch`, which clones the
    /// whole record.
    fn dispatch_context(&self, job_id: &str) -> Option<(String, i32, Option<i64>)> {
        self.in_flight
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .last_dispatch(job_id)
            .map(|record| (record.queue.clone(), record.attempt, record.epoch))
    }
}

/// `job.failed`, then the transition it led to (`job.retrying` or `job.dead`).
/// Both carry the error, so a sink filtering on the second alone still sees why.
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
    use crate::events::test_support::{delivered, recording_hub, Attempts};
    use crate::job::{now_millis, NewJob};
    use crate::resilience::rate_limiter::RateLimitConfig;
    use crate::scheduler::{shed, SchedulerConfig, TaskConfig};
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
