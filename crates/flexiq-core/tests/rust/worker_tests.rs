//! End-to-end tests for the native worker: enqueue → dispatch → execute →
//! result handling, against an in-memory SQLite backend.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use flexiq_core::job::{now_millis, Job, JobStatus, NewJob};
use flexiq_core::resilience::retry::RetryPolicy;
use flexiq_core::scheduler::TaskConfig;
use flexiq_core::storage::sqlite::SqliteStorage;
use flexiq_core::storage::{Storage, StorageBackend};
use flexiq_core::worker::registry::TaskError;
use flexiq_core::worker::runner::Worker;
use flexiq_core::worker::WorkerDispatcher;

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
        debounce_key: None,
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

/// The registry row records what the worker can run.
///
/// Registered in the opposite order to the fingerprint's, to pin that the value
/// is over the *set*: registration order is import order, which discovery
/// decides, and a fingerprint that followed it would report divergence on every
/// worker that happened to import its modules differently.
#[test]
fn a_worker_records_a_fingerprint_of_its_task_registry() {
    let storage = test_backend();
    let handle = Worker::new(storage.clone())
        .register("reports.build", |_job: &Job| Ok(None))
        .register("invoices.send", |_job: &Job| Ok(None))
        .spawn()
        .expect("spawn");

    let workers = storage.list_workers().expect("list_workers");
    let worker = workers.first().expect("the worker registered");
    // The value `crates/flexiq-core/BINDING_CONTRACT.md` pins for this set.
    assert_eq!(
        worker.registry_fingerprint.as_deref(),
        Some("fafd30ef8ebcb7de")
    );

    handle.shutdown().expect("shutdown");
}

/// Nothing registered is nothing to report. A row that overstates what a worker
/// runs is worse than a row that says nothing: an unregistered task name is a
/// fatal, non-retryable failure.
#[test]
fn a_worker_with_nothing_registered_reports_no_fingerprint() {
    let storage = test_backend();
    let handle = Worker::new(storage.clone()).spawn().expect("spawn");

    let workers = storage.list_workers().expect("list_workers");
    assert_eq!(
        workers.first().expect("registered").registry_fingerprint,
        None
    );

    handle.shutdown().expect("shutdown");
}

/// A pool the caller supplied leaves the registered handlers unused, so they
/// must not be fingerprinted.
///
/// `register` plus `dispatcher` is a legal combination that silently drops the
/// handlers — see [`Worker::dispatcher`]. Reporting them would advertise a task
/// set this worker cannot run, from the one column that exists to make exactly
/// that kind of mismatch visible.
#[test]
fn a_supplied_pool_reports_no_fingerprint_for_handlers_it_will_not_run() {
    struct IdlePool;

    #[async_trait::async_trait]
    impl WorkerDispatcher for IdlePool {
        async fn run(
            &self,
            mut job_rx: tokio::sync::mpsc::Receiver<Job>,
            _result_tx: crossbeam_channel::Sender<flexiq_core::scheduler::JobResult>,
        ) {
            while job_rx.recv().await.is_some() {}
        }

        fn shutdown(&self) {}
    }

    let storage = test_backend();
    let handle = Worker::new(storage.clone())
        .register("invoices.send", |_job: &Job| Ok(None))
        .dispatcher("remote", Arc::new(IdlePool))
        .spawn()
        .expect("spawn");

    let workers = storage.list_workers().expect("list_workers");
    let worker = workers.first().expect("the worker registered");
    assert_eq!(worker.pool_type.as_deref(), Some("remote"));
    assert_eq!(worker.registry_fingerprint, None);

    handle.shutdown().expect("shutdown");
}
