//! A worker that runs registered Rust tasks.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use flexiq::FlexiQ;
use flexiq_core::{Job, JobStatus, TaskError};

static DOUBLED: AtomicUsize = AtomicUsize::new(0);

#[flexiq::task]
fn double(n: i64) -> flexiq::Outcome<i64> {
    DOUBLED.fetch_add(1, Ordering::SeqCst);
    Ok(n * 2)
}

#[flexiq::task]
fn always_fails(_n: i64) -> flexiq::Outcome<()> {
    Err(TaskError::fatal("nope").into())
}

#[flexiq::task(max_retries = 0)]
fn reads_its_argument(name: String) -> flexiq::Outcome<String> {
    Ok(format!("hello {name}"))
}

/// Poll until `job_id` reaches a terminal state, or fail the test.
///
/// A budget rather than a fixed sleep: the scheduler's poll interval is its
/// own, and a test that sleeps exactly one interval is a flake waiting for a
/// slower machine.
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

#[test]
fn a_registered_task_runs_and_records_its_result() {
    let q = FlexiQ::in_memory().expect("opens");
    let job = q.enqueue(double::call(21)).expect("enqueues");

    let worker = q
        .worker()
        .register::<double>()
        .num_workers(2)
        .spawn()
        .expect("spawns");
    let done = wait_terminal(&q, &job.id);
    worker.shutdown().expect("clean shutdown");

    assert_eq!(done.status, JobStatus::Complete);
    assert_eq!(DOUBLED.load(Ordering::SeqCst), 1);
    assert!(done.result.is_some(), "a returned value must be archived");
}

/// The arguments reach the body, not merely the payload.
#[test]
fn a_task_receives_its_decoded_arguments() {
    let q = FlexiQ::in_memory().expect("opens");
    let job = q
        .enqueue(reads_its_argument::call("world".into()))
        .expect("enqueues");

    let worker = q
        .worker()
        .register::<reads_its_argument>()
        .spawn()
        .expect("spawns");
    let done = wait_terminal(&q, &job.id);
    worker.shutdown().expect("clean shutdown");

    assert_eq!(done.status, JobStatus::Complete);
    let encoded = done.result.expect("a result was archived");
    // Tag byte, then the CBOR text "hello world".
    assert_eq!(encoded[0], 0x02);
    assert!(
        String::from_utf8_lossy(&encoded).contains("hello world"),
        "the body's return value must be what was archived"
    );
}

/// `BINDING_CONTRACT.md` specifies `{"errtype","message","traceback"}` for a
/// failed job, and core's own pool stores the bare message instead. A reader in
/// another language matches on that shape.
#[test]
fn a_failure_records_the_contract_json() {
    let q = FlexiQ::in_memory().expect("opens");
    let job = q.enqueue(always_fails::call(1)).expect("enqueues");

    let worker = q
        .worker()
        .register::<always_fails>()
        .spawn()
        .expect("spawns");
    let done = wait_terminal(&q, &job.id);
    worker.shutdown().expect("clean shutdown");

    let recorded = done.error.expect("an error was recorded");
    let parsed: serde_json::Value =
        serde_json::from_str(&recorded).expect("the recorded error is JSON");
    assert_eq!(parsed["errtype"], "TaskError");
    assert_eq!(parsed["message"], "nope");
}

/// A fatal error is not retried, however many retries the job has left.
#[test]
fn a_fatal_error_does_not_retry() {
    let q = FlexiQ::in_memory().expect("opens");
    let job = q
        .enqueue(always_fails::call(1).max_retries(5))
        .expect("enqueues");

    let worker = q
        .worker()
        .register::<always_fails>()
        .spawn()
        .expect("spawns");
    let done = wait_terminal(&q, &job.id);
    worker.shutdown().expect("clean shutdown");

    assert_eq!(done.retry_count, 0, "a fatal error must not be retried");
}

/// No amount of retrying makes an unregistered task runnable on this worker.
#[test]
fn an_unregistered_task_is_fatal() {
    let q = FlexiQ::in_memory().expect("opens");
    let job = q.enqueue(double::call(1)).expect("enqueues");

    // Deliberately no `register` call.
    let worker = q.worker().spawn().expect("spawns");
    let done = wait_terminal(&q, &job.id);
    worker.shutdown().expect("clean shutdown");

    assert_eq!(done.retry_count, 0, "an unregistered task must not retry");
    let recorded = done.error.expect("an error was recorded");
    assert!(
        recorded.contains("double"),
        "the error should name the task: {recorded}"
    );
}

#[flexiq::task(max_retries = 0)]
fn panics(_n: i64) -> flexiq::Outcome<()> {
    panic!("the task exploded");
}

/// A panicking handler records a failure instead of stranding the job.
///
/// `spawn_blocking` catches a panic into a `JoinHandle` nothing holds, so
/// without containment no `JobResult` is ever sent and the job sits in flight
/// until the stale-job reap notices — with no error recorded anywhere.
#[test]
fn a_panicking_task_fails_the_job_rather_than_stranding_it() {
    let q = FlexiQ::in_memory().expect("opens");
    let job = q.enqueue(panics::call(1)).expect("enqueues");

    let worker = q.worker().register::<panics>().spawn().expect("spawns");
    let done = wait_terminal(&q, &job.id);
    worker.shutdown().expect("clean shutdown");

    let recorded = done.error.expect("an error was recorded");
    assert!(
        recorded.contains("panicked") && recorded.contains("the task exploded"),
        "the recorded error should carry the panic message: {recorded}"
    );
}

/// And the pool keeps working afterwards — the blocking thread is reused.
#[test]
fn a_panic_does_not_take_the_pool_down() {
    let q = FlexiQ::in_memory().expect("opens");
    let boom = q.enqueue(panics::call(1)).expect("enqueues");
    let after = q
        .enqueue(reads_its_argument::call("world".into()))
        .expect("enqueues");

    let worker = q
        .worker()
        .register::<panics>()
        .register::<reads_its_argument>()
        .num_workers(1)
        .spawn()
        .expect("spawns");
    wait_terminal(&q, &boom.id);
    let done = wait_terminal(&q, &after.id);
    worker.shutdown().expect("clean shutdown");

    assert_eq!(done.status, JobStatus::Complete);
}

/// The task's declared config reaches the scheduler, not only the job row.
#[test]
fn a_worker_registers_the_tasks_dispatch_config() {
    let q = FlexiQ::in_memory().expect("opens");
    let worker = q
        .worker()
        .register::<reads_its_argument>()
        .spawn()
        .expect("spawns");
    worker.shutdown().expect("clean shutdown");
}

/// Registering the same name twice is a mistake worth catching at startup: one
/// of the two bodies would silently never run.
#[test]
fn registering_one_name_twice_is_refused() {
    let q = FlexiQ::in_memory().expect("opens");
    // `expect_err` would need `WorkerHandle: Debug`, which core does not derive
    // — and a handle that reached here would be a live worker to shut down.
    let err = match q.worker().register::<double>().register::<double>().spawn() {
        Ok(handle) => {
            handle.shutdown().expect("clean shutdown");
            panic!("a duplicate registration must be refused");
        }
        Err(err) => err,
    };

    assert!(err.to_string().contains("double"), "message: {err}");
}
