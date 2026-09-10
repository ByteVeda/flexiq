//! Cron tasks: registered at worker start, fired by the scheduler already
//! running inside it.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use flexiq::FlexiQ;

static TICKS: AtomicUsize = AtomicUsize::new(0);

/// Six fields, seconds first — the `cron` crate's dialect, not five-field
/// crontab.
#[flexiq::task(cron = "* * * * * *", queue = "beats")]
fn every_second() -> flexiq::Outcome<()> {
    TICKS.fetch_add(1, Ordering::SeqCst);
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

#[test]
fn a_scheduled_task_actually_fires() {
    let q = FlexiQ::in_memory().expect("opens");
    let before = TICKS.load(Ordering::SeqCst);

    let worker = q
        .worker()
        .register::<every_second>()
        .queues(["beats"])
        .spawn()
        .expect("spawns");

    let deadline = Instant::now() + Duration::from_secs(20);
    while TICKS.load(Ordering::SeqCst) == before && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(50));
    }
    worker.shutdown().expect("clean shutdown");

    assert!(
        TICKS.load(Ordering::SeqCst) > before,
        "the periodic never fired"
    );
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
