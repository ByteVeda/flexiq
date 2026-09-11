//! Durable steps: a committed step is not re-run, and a sleep ends the attempt.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use flexiq::FlexiQ;
use flexiq_core::{Job, JobStatus, TaskError};

static CHARGES: AtomicUsize = AtomicUsize::new(0);
static ATTEMPTS: AtomicUsize = AtomicUsize::new(0);
static NAPS: AtomicUsize = AtomicUsize::new(0);

/// Fails once *after* its step commits, so the retry has to find the memo.
#[flexiq::task(max_retries = 3, retry_backoff_ms = 1)]
fn charge_once(order: String) -> flexiq::Outcome<()> {
    let mut step = flexiq::current_step();

    let receipt: String = step.run("charge", || {
        CHARGES.fetch_add(1, Ordering::SeqCst);
        Ok(format!("receipt-for-{order}"))
    })?;
    assert_eq!(receipt, "receipt-for-ord-1");

    if ATTEMPTS.fetch_add(1, Ordering::SeqCst) == 0 {
        return Err(TaskError::retryable("crash after the charge").into());
    }
    Ok(())
}

/// Sleeps once, then completes on the attempt that follows.
#[flexiq::task]
fn nap(_n: i64) -> flexiq::Outcome<()> {
    let mut step = flexiq::current_step();
    NAPS.fetch_add(1, Ordering::SeqCst);
    step.sleep_ms("wait", 300)?;
    Ok(())
}

/// Poll until `job_id` reaches a terminal state, or fail the test.
///
/// Duplicated from `tests/worker.rs` on purpose: one integration test target
/// cannot import from another, and ten lines is cheaper than a shared crate.
fn wait_terminal(q: &FlexiQ, job_id: &str) -> Job {
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline {
        let job = q.get_job(job_id).expect("reads").expect("job exists");
        if matches!(
            job.status,
            JobStatus::Complete | JobStatus::Failed | JobStatus::Dead | JobStatus::Cancelled
        ) {
            return job;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    panic!("job {job_id} never reached a terminal state");
}

/// The whole point of a durable step: a card charged once stays charged once,
/// however many times the attempt after it fails.
#[test]
fn a_committed_step_is_not_re_run_on_retry() {
    let q = FlexiQ::in_memory().expect("opens");
    let job = q
        .enqueue(charge_once::call("ord-1".into()))
        .expect("enqueues");

    let worker = q
        .worker()
        .register::<charge_once>()
        .spawn()
        .expect("spawns");
    let done = wait_terminal(&q, &job.id);
    worker.shutdown().expect("clean shutdown");

    assert_eq!(done.status, JobStatus::Complete);
    assert_eq!(
        ATTEMPTS.load(Ordering::SeqCst),
        2,
        "the job must actually have retried"
    );
    assert_eq!(
        CHARGES.load(Ordering::SeqCst),
        1,
        "the committed step must be memoized, not re-run"
    );
}

/// A sleep ends the attempt and leaves `retry_count` alone — it is not a
/// failure, and the scheduler's `Slept` arm exists to say so.
#[test]
fn a_sleep_ends_the_attempt_without_spending_a_retry() {
    let q = FlexiQ::in_memory().expect("opens");
    let job = q.enqueue(nap::call(1)).expect("enqueues");

    let worker = q.worker().register::<nap>().spawn().expect("spawns");
    let done = wait_terminal(&q, &job.id);
    worker.shutdown().expect("clean shutdown");

    assert_eq!(done.status, JobStatus::Complete);
    assert_eq!(done.retry_count, 0, "a sleep is not a retry");
    assert_eq!(
        NAPS.load(Ordering::SeqCst),
        2,
        "the body runs again from the top after the sleep"
    );
}

/// A task of its own, so this test shares no counter with the ones above:
/// statics are process-global and the harness runs tests in parallel.
///
/// It commits a run step and then sleeps for long enough that the test reads
/// the rows while the run is still alive.
#[flexiq::task]
fn record_then_wait(_n: i64) -> flexiq::Outcome<()> {
    let mut step = flexiq::current_step();
    let _: i64 = step.run("charge", || Ok(7))?;
    step.sleep_ms("settle", 600_000)?;
    Ok(())
}

/// Both kinds of step row are written, and they are written against the job.
///
/// Read mid-run on purpose. Archiving a finished job **deletes its step rows in
/// the same transaction** (`diesel_common/jobs.rs`), because a memo is
/// execution state with no value past the job's end — under an encrypting codec
/// it would be ciphertext nothing ever collects. So a completed job correctly
/// has no steps, and the only place to see them is while the run is still open.
#[test]
fn both_kinds_of_step_are_recorded_against_the_job() {
    use flexiq_core::{StepKind, Storage};

    let q = FlexiQ::in_memory().expect("opens");
    let job = q.enqueue(record_then_wait::call(1)).expect("enqueues");

    let worker = q
        .worker()
        .register::<record_then_wait>()
        .spawn()
        .expect("spawns");

    // The attempt ends at the sleep, leaving the job pending far in the future
    // with both rows committed.
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut steps = Vec::new();
    while Instant::now() < deadline {
        steps = q
            .storage()
            .get_job_steps(&job.id, None)
            .expect("reads steps");
        if steps.len() == 2 {
            break;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    worker.shutdown().expect("clean shutdown");

    assert_eq!(steps.len(), 2, "a run and a sleep were committed");

    let run = &steps[0];
    assert!(run.step_key.contains("charge"));
    assert_eq!(run.kind, StepKind::Run);
    assert!(run.result.is_some(), "a run memoizes a value");

    let sleep = &steps[1];
    assert!(sleep.step_key.contains("settle"));
    assert_eq!(sleep.kind, StepKind::Sleep);
    assert!(sleep.result.is_none(), "a sleep memoizes a deadline");
    assert!(sleep.wake_at.is_some());
}

/// A value that cannot be written.
///
/// The body already ran, so retrying repeats its side effects and fails
/// identically every time.
struct Unencodable;

impl serde::Serialize for Unencodable {
    fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom("this value does not encode"))
    }
}

impl<'de> serde::Deserialize<'de> for Unencodable {
    fn deserialize<D: serde::Deserializer<'de>>(_: D) -> Result<Self, D::Error> {
        Ok(Unencodable)
    }
}

#[flexiq::task(max_retries = 3, retry_backoff_ms = 1)]
fn returns_something_unencodable(_n: i64) -> flexiq::Outcome<()> {
    let mut step = flexiq::current_step();
    let _: Unencodable = step.run("encode-me", || Ok(Unencodable))?;
    Ok(())
}

/// An unencodable step value is fatal, not retryable.
#[test]
fn a_step_value_that_cannot_be_written_is_not_retried() {
    let q = FlexiQ::in_memory().expect("opens");
    let job = q
        .enqueue(returns_something_unencodable::call(1))
        .expect("enqueues");

    let worker = q
        .worker()
        .register::<returns_something_unencodable>()
        .spawn()
        .expect("spawns");
    let done = wait_terminal(&q, &job.id);
    worker.shutdown().expect("clean shutdown");

    assert_eq!(
        done.retry_count, 0,
        "a value that will never encode must not be retried"
    );
    let recorded = done.error.expect("an error was recorded");
    assert!(
        recorded.contains("does not encode"),
        "the error should name the cause: {recorded}"
    );
}

/// Outside a task there is no session, and saying so beats panicking on a pool
/// thread.
#[test]
fn a_step_outside_a_task_is_refused_rather_than_panicking() {
    let mut step = flexiq::current_step();
    let outcome: flexiq::Outcome<i64> = step.run("orphan", || Ok(1));

    match outcome {
        Err(flexiq::Abort::Fail(err)) => {
            assert!(
                err.message.contains("outside a running task"),
                "message: {}",
                err.message
            );
            assert!(!err.retryable, "a caller's mistake is not retryable");
        }
        _ => panic!("a detached step must fail"),
    }
}
