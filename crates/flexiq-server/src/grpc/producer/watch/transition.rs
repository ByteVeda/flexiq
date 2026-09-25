//! A job transition as the wire reports it, built from either of the two
//! things a watch learns from: an event the process emitted, or a row read
//! back from storage.

use flexiq_core::job::{now_millis, Job, JobStatus};
use flexiq_core::{EventType, JobEvent};

use crate::grpc::pb;
use crate::grpc::producer::convert::{status_to_wire, timestamp};

/// How far a job has got, for telling a newer state from a stale one: which
/// attempt, then how far through it.
///
/// Only consulted where the order of two observations is genuinely unknown —
/// an event racing the snapshot read, or a reconcile row racing a live event.
/// A terminal state outranks everything.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Rank {
    attempt: i32,
    phase: u8,
}

/// One transition, and the order it sits in.
#[derive(Debug, Clone, PartialEq)]
pub struct Observed {
    /// What the wire carries.
    pub transition: pb::JobTransition,
    /// Where it sits against another observation of the same job.
    pub rank: Rank,
}

impl Observed {
    /// The job's last item on a stream.
    pub fn terminal(&self) -> bool {
        self.transition.terminal
    }

    /// The job it is about.
    pub fn job_id(&self) -> &str {
        &self.transition.job_id
    }
}

/// An event the scheduler or a door emitted, or `None` for a kind this build
/// does not report on the wire.
pub fn from_event(event: &JobEvent) -> Option<Observed> {
    let attempt = event.attempt.unwrap_or(0);
    let (kind, status, terminal) = match event.event_type {
        EventType::JobEnqueued => (pb::JobTransitionKind::Enqueued, JobStatus::Pending, false),
        EventType::JobStarted => (pb::JobTransitionKind::Started, JobStatus::Running, false),
        EventType::JobCompleted => (pb::JobTransitionKind::Completed, JobStatus::Complete, true),
        // Not terminal: a failure is always followed by a retry or a
        // dead-letter, and that is the item the client is waiting for.
        EventType::JobFailed => (pb::JobTransitionKind::Failed, JobStatus::Failed, false),
        EventType::JobRetrying => (pb::JobTransitionKind::Retrying, JobStatus::Pending, false),
        EventType::JobDead => (pb::JobTransitionKind::Dead, JobStatus::Dead, true),
        EventType::JobCancelled => (pb::JobTransitionKind::Cancelled, JobStatus::Cancelled, true),
        EventType::JobSleeping => (pb::JobTransitionKind::Sleeping, JobStatus::Pending, false),
        _ => return None,
    };
    // A retry's event names the attempt that failed; the job it leaves behind
    // is the next attempt's, still pending.
    let next_attempt = if kind == pb::JobTransitionKind::Retrying {
        attempt.saturating_add(1)
    } else {
        attempt
    };
    Some(Observed {
        rank: rank(status, next_attempt, terminal),
        transition: pb::JobTransition {
            job_id: event.job_id.clone(),
            queue: event.queue.clone(),
            task_name: event.task_name.clone(),
            kind: kind as i32,
            status: status_to_wire(status) as i32,
            attempt,
            time: Some(timestamp(event.time_ms)),
            terminal,
            error: event.error.clone(),
            reason: event.reason.clone(),
            timed_out: event.timed_out,
            wake_at: event.wake_at_ms.map(timestamp),
        },
    })
}

/// A job's current state, read from storage.
pub fn from_row(job: &Job) -> Observed {
    let terminal = is_terminal(job.status);
    Observed {
        rank: rank(job.status, job.retry_count, terminal),
        transition: pb::JobTransition {
            job_id: job.id.clone(),
            queue: job.queue.clone(),
            task_name: job.task_name.clone(),
            kind: pb::JobTransitionKind::Snapshot as i32,
            status: status_to_wire(job.status) as i32,
            attempt: job.retry_count,
            time: Some(timestamp(now_millis())),
            terminal,
            error: job.error.clone().filter(|_| terminal),
            reason: None,
            timed_out: None,
            wake_at: None,
        },
    }
}

/// A stored status no further transition leaves. `Failed` among them: a row
/// only rests in it once nothing more will happen to it.
fn is_terminal(status: JobStatus) -> bool {
    match status {
        JobStatus::Pending | JobStatus::Running => false,
        JobStatus::Complete | JobStatus::Failed | JobStatus::Dead | JobStatus::Cancelled => true,
    }
}

fn rank(status: JobStatus, attempt: i32, terminal: bool) -> Rank {
    if terminal {
        return Rank {
            attempt: i32::MAX,
            phase: u8::MAX,
        };
    }
    let phase = match status {
        JobStatus::Pending => 0,
        JobStatus::Running => 1,
        _ => 2,
    };
    Rank { attempt, phase }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(event_type: EventType, attempt: i32) -> JobEvent {
        let mut event = JobEvent::new(event_type, "j", Some("ns".into()), "q", "t");
        event.attempt = Some(attempt);
        event
    }

    fn row(status: JobStatus, retry_count: i32) -> Job {
        Job {
            id: "j".into(),
            queue: "q".into(),
            task_name: "t".into(),
            payload: Vec::new(),
            status,
            priority: 0,
            created_at: 0,
            scheduled_at: 0,
            started_at: None,
            completed_at: None,
            retry_count,
            max_retries: 3,
            result: None,
            error: Some("boom".into()),
            timeout_ms: 0,
            unique_key: None,
            progress: None,
            metadata: None,
            notes: None,
            cancel_requested: false,
            expires_at: None,
            result_ttl_ms: None,
            namespace: Some("ns".into()),
            has_deps: false,
            debounce_key: None,
        }
    }

    #[test]
    fn only_completion_dead_letter_and_cancel_are_terminal_events() {
        for event_type in EventType::ALL {
            let observed = from_event(&event(*event_type, 0)).expect("every type reports");
            let expected = matches!(
                event_type,
                EventType::JobCompleted | EventType::JobDead | EventType::JobCancelled
            );
            assert_eq!(observed.terminal(), expected, "{event_type:?}");
        }
    }

    #[test]
    fn a_retry_ranks_as_the_next_attempt_pending() {
        let started = from_event(&event(EventType::JobStarted, 0)).unwrap();
        let failed = from_event(&event(EventType::JobFailed, 0)).unwrap();
        let retrying = from_event(&event(EventType::JobRetrying, 0)).unwrap();
        let snapshot = from_row(&row(JobStatus::Pending, 1));
        assert!(started.rank < failed.rank && failed.rank < retrying.rank);
        assert_eq!(retrying.rank, snapshot.rank);
        assert_eq!(retrying.transition.attempt, 0, "the attempt that failed");
        assert!(from_event(&event(EventType::JobStarted, 1)).unwrap().rank > retrying.rank);
    }

    #[test]
    fn a_terminal_row_outranks_every_live_state_and_keeps_its_error() {
        let done = from_row(&row(JobStatus::Failed, 3));
        assert!(done.terminal());
        assert!(done.rank > from_event(&event(EventType::JobStarted, 9)).unwrap().rank);
        assert_eq!(done.transition.error.as_deref(), Some("boom"));
        assert_eq!(done.transition.kind, pb::JobTransitionKind::Snapshot as i32);
        let running = from_row(&row(JobStatus::Running, 0));
        assert!(!running.terminal());
        assert_eq!(
            running.transition.error, None,
            "a live row's error is stale"
        );
    }
}
