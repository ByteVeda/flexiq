//! Cron tasks: registered at worker start, fired by the scheduler already
//! running inside it.

use std::time::{Duration, Instant};

use flexiq::FlexiQ;

/// Six fields, seconds first — the `cron` crate's dialect, not five-field
/// crontab.
#[flexiq::task(cron = "* * * * * *", queue = "beats")]
fn every_second() -> flexiq::Outcome<()> {
    Ok(())
}

#[flexiq::task(cron = "0 0 3 * * *", timezone = "Europe/Stockholm")]
fn nightly() -> flexiq::Outcome<()> {
    Ok(())
}

#[test]
fn a_scheduled_task_is_registered_at_worker_start() {
    let q = FlexiQ::in_memory().expect("opens");
    let worker = q
        .worker()
        .register::<nightly>()
        .queues(["default"])
        .spawn()
        .expect("spawns");

    let registered = q.list_periodic().expect("lists");
    worker.shutdown().expect("clean shutdown");

    assert_eq!(registered.len(), 1);
    assert_eq!(registered[0].name, "nightly");
    assert_eq!(registered[0].task_name, "nightly");
    assert_eq!(registered[0].cron_expr, "0 0 3 * * *");
    assert_eq!(registered[0].timezone.as_deref(), Some("Europe/Stockholm"));
    assert!(registered[0].enabled);
    assert!(
        registered[0].next_run > flexiq_core::now_millis(),
        "the next run is in the future"
    );
}

/// The task's queue reaches the periodic row, so a scheduled job lands where
/// the declaration said it would.
#[test]
fn a_scheduled_task_keeps_its_queue() {
    let q = FlexiQ::in_memory().expect("opens");
    let worker = q
        .worker()
        .register::<every_second>()
        .queues(["beats"])
        .spawn()
        .expect("spawns");
    let registered = q.list_periodic().expect("lists");
    worker.shutdown().expect("clean shutdown");

    assert_eq!(registered[0].queue, "beats");
}

/// Its own task, and observed through its own storage.
///
/// A process-global counter cannot tell this test's scheduler from another
/// test's: the harness runs them in parallel, and two of these start a worker
/// for a once-a-second task. Polling `list_jobs` on this handle's database
/// proves *this* scheduler minted the job.
#[flexiq::task(cron = "* * * * * *", queue = "ticks")]
fn tick() -> flexiq::Outcome<()> {
    Ok(())
}

#[test]
fn a_scheduled_task_actually_fires() {
    let q = FlexiQ::in_memory().expect("opens");

    let worker = q
        .worker()
        .register::<tick>()
        .queues(["ticks"])
        .spawn()
        .expect("spawns");

    let deadline = Instant::now() + Duration::from_secs(20);
    let mut minted = Vec::new();
    while Instant::now() < deadline {
        minted = q.list_jobs(10, 0).expect("lists");
        if !minted.is_empty() {
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    worker.shutdown().expect("clean shutdown");

    let job = minted.first().expect("the periodic never minted a job");
    assert_eq!(job.task_name, "tick");
    assert_eq!(job.queue, "ticks");
    assert!(
        job.unique_key
            .as_deref()
            .is_some_and(|key| key.starts_with("periodic:tick:")),
        "a fired periodic carries its dedup key: {:?}",
        job.unique_key
    );
}

/// Periodic rows are keyed by name alone in every backend — `NewPeriodicTask`
/// has no namespace — so a namespaced handle would read and overwrite another
/// namespace's schedules. Refused until the table grows one.
#[test]
fn a_namespaced_handle_refuses_every_periodic_operation() {
    let q = FlexiQ::in_memory()
        .expect("opens")
        .with_namespace("tenant-a");

    for message in [
        q.list_periodic().map(|_| ()).unwrap_err().to_string(),
        q.delete_periodic("nightly")
            .map(|_| ())
            .unwrap_err()
            .to_string(),
        q.pause_periodic("nightly")
            .map(|_| ())
            .unwrap_err()
            .to_string(),
        q.resume_periodic("nightly")
            .map(|_| ())
            .unwrap_err()
            .to_string(),
    ] {
        assert!(
            message.contains("namespaced handle"),
            "the refusal should say why: {message}"
        );
    }

    let err = match q.worker().register::<nightly>().spawn() {
        Ok(handle) => {
            handle.shutdown().expect("clean shutdown");
            panic!("a namespaced worker must refuse to register a schedule");
        }
        Err(err) => err.to_string(),
    };
    assert!(err.contains("namespaced handle"), "message: {err}");
}

/// Restarting a worker keeps the deadline a previous run computed.
///
/// Recomputing it every start means a restart landing after a deadline has
/// passed, but before the scheduler fired it, pushes the deadline to the next
/// occurrence — losing that firing, repeatedly, for a process that restarts
/// often.
#[test]
fn restarting_a_worker_keeps_the_existing_deadline() {
    let q = FlexiQ::in_memory().expect("opens");

    let first = q.worker().register::<nightly>().spawn().expect("spawns");
    first.shutdown().expect("clean shutdown");
    let before = q.list_periodic().expect("lists")[0].next_run;

    let second = q.worker().register::<nightly>().spawn().expect("spawns");
    second.shutdown().expect("clean shutdown");

    assert_eq!(
        q.list_periodic().expect("lists")[0].next_run,
        before,
        "a restart must not move a deadline it did not reach"
    );
}

/// And a paused periodic stays paused across one.
///
/// Writing `enabled: true` unconditionally would quietly resume it the next
/// time its worker came up, which is the opposite of what pausing it meant.
#[test]
fn restarting_a_worker_keeps_a_periodic_paused() {
    let q = FlexiQ::in_memory().expect("opens");

    let first = q.worker().register::<nightly>().spawn().expect("spawns");
    first.shutdown().expect("clean shutdown");
    assert!(q.pause_periodic("nightly").expect("pauses"));

    let second = q.worker().register::<nightly>().spawn().expect("spawns");
    second.shutdown().expect("clean shutdown");

    assert!(
        !q.list_periodic().expect("lists")[0].enabled,
        "a restart must not resume a task an operator paused"
    );
}

/// A worker with no scheduled tasks is unaffected by the refusal above.
#[test]
fn a_namespaced_worker_without_schedules_still_starts() {
    let q = FlexiQ::in_memory()
        .expect("opens")
        .with_namespace("tenant-a");
    let worker = q.worker().spawn().expect("spawns");
    worker.shutdown().expect("clean shutdown");
}

#[test]
fn a_periodic_can_be_paused_resumed_and_deleted() {
    let q = FlexiQ::in_memory().expect("opens");
    let worker = q.worker().register::<nightly>().spawn().expect("spawns");
    worker.shutdown().expect("clean shutdown");

    assert!(q.pause_periodic("nightly").expect("pauses"));
    assert!(!q.list_periodic().expect("lists")[0].enabled);

    assert!(q.resume_periodic("nightly").expect("resumes"));
    assert!(q.list_periodic().expect("lists")[0].enabled);

    assert!(q.delete_periodic("nightly").expect("deletes"));
    assert!(q.list_periodic().expect("lists").is_empty());

    assert!(!q.delete_periodic("nightly").expect("deletes"));
}
