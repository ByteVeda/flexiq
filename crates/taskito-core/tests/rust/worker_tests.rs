//! End-to-end tests for the native worker: enqueue → dispatch → execute →
//! result handling, against an in-memory SQLite backend.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use taskito_core::job::{now_millis, Job, JobStatus, NewJob};
use taskito_core::resilience::retry::RetryPolicy;
use taskito_core::scheduler::TaskConfig;
use taskito_core::storage::sqlite::SqliteStorage;
use taskito_core::storage::{Storage, StorageBackend};
use taskito_core::worker::registry::TaskError;
use taskito_core::worker::runner::Worker;

fn test_backend() -> StorageBackend {
    StorageBackend::Sqlite(SqliteStorage::in_memory().expect("in-memory sqlite"))
}

fn make_job(task_name: &str, payload: &[u8], max_retries: i32) -> NewJob {
    NewJob {
        queue: "default".to_string(),
        task_name: task_name.to_string(),
        payload: payload.to_vec(),
        priority: 0,
        scheduled_at: now_millis(),
        max_retries,
        timeout_ms: 30_000,
        unique_key: None,
        metadata: None,
        notes: None,
        depends_on: vec![],
        expires_at: None,
        result_ttl_ms: None,
        namespace: None,
    }
}

fn wait_for_status(storage: &StorageBackend, job_id: &str, wanted: JobStatus) -> Job {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(job) = storage.get_job(job_id, None).expect("get_job") {
            if job.status == wanted {
                return job;
            }
        }
        assert!(
            Instant::now() < deadline,
            "job {job_id} never reached {wanted:?}"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn wait_for_dead(storage: &StorageBackend, job_id: &str) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let dead = storage.list_dead(50, 0, None).expect("list_dead");
        if dead.iter().any(|d| d.original_job_id == job_id) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "job {job_id} never dead-lettered"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn sync_handler_executes_and_stores_result() {
    let storage = test_backend();
    let handle = Worker::new(storage.clone())
        .num_workers(2)
        .register("echo", |job: &Job| Ok(Some(job.payload.clone())))
        .spawn()
        .expect("spawn");

    let job = storage.enqueue(make_job("echo", b"ping", 3)).unwrap();
    let done = wait_for_status(&storage, &job.id, JobStatus::Complete);
    assert_eq!(done.result.as_deref(), Some(&b"ping"[..]));

    handle.shutdown().expect("shutdown");
}

#[test]
fn async_handler_executes() {
    let storage = test_backend();
    let handle = Worker::new(storage.clone())
        .register_async("sleepy", |_job: Job| async move {
            tokio::time::sleep(Duration::from_millis(5)).await;
            Ok(Some(b"woke".to_vec()))
        })
        .spawn()
        .expect("spawn");

    let job = storage.enqueue(make_job("sleepy", b"", 3)).unwrap();
    let done = wait_for_status(&storage, &job.id, JobStatus::Complete);
    assert_eq!(done.result.as_deref(), Some(&b"woke"[..]));

    handle.shutdown().expect("shutdown");
}

#[test]
fn retryable_failure_retries_then_dead_letters() {
    let storage = test_backend();
    let attempts = Arc::new(AtomicU32::new(0));
    let seen = attempts.clone();
    let handle = Worker::new(storage.clone())
        .task_config(
            "flaky",
            TaskConfig {
                retry_policy: RetryPolicy {
                    max_retries: 2,
                    base_delay_ms: 10,
                    max_delay_ms: 20,
                    custom_delays_ms: None,
                },
                ..TaskConfig::default()
            },
        )
        .register("flaky", move |_job: &Job| {
            seen.fetch_add(1, Ordering::SeqCst);
            Err(TaskError::retryable("boom"))
        })
        .spawn()
        .expect("spawn");

    let job = storage.enqueue(make_job("flaky", b"", 2)).unwrap();
    wait_for_dead(&storage, &job.id);
    // First attempt + 2 retries.
    assert_eq!(attempts.load(Ordering::SeqCst), 3);

    handle.shutdown().expect("shutdown");
}

#[test]
fn fatal_failure_skips_retries() {
    let storage = test_backend();
    let attempts = Arc::new(AtomicU32::new(0));
    let seen = attempts.clone();
    let handle = Worker::new(storage.clone())
        .register("doomed", move |_job: &Job| {
            seen.fetch_add(1, Ordering::SeqCst);
            Err(TaskError::fatal("unrecoverable"))
        })
        .spawn()
        .expect("spawn");

    let job = storage.enqueue(make_job("doomed", b"", 5)).unwrap();
    wait_for_dead(&storage, &job.id);
    assert_eq!(attempts.load(Ordering::SeqCst), 1, "fatal must not retry");

    handle.shutdown().expect("shutdown");
}

#[test]
fn unregistered_task_dead_letters_without_retry() {
    let storage = test_backend();
    let handle = Worker::new(storage.clone())
        .register("known", |_job: &Job| Ok(None))
        .spawn()
        .expect("spawn");

    let job = storage.enqueue(make_job("unknown", b"", 5)).unwrap();
    wait_for_dead(&storage, &job.id);

    handle.shutdown().expect("shutdown");
}
