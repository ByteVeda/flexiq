//! Namespace scoping across the dashboard-facing listings and aggregates.
//!
//! `TASKITO_NAMESPACE` is meant to be a tenancy boundary, so a caller scoped to
//! one namespace must not see another's jobs, dead letters, logs, metrics, or
//! totals — and must not be able to act on their ids. `None` stays unscoped,
//! matching `list_jobs`, so a single-tenant deployment is unaffected.

use taskito_core::job::{now_millis, JobStatus, NewJob};
use taskito_core::SqliteStorage;

const TENANT_A: &str = "tenant-a";
const TENANT_B: &str = "tenant-b";

fn storage() -> SqliteStorage {
    SqliteStorage::new(":memory:").expect("in-memory SQLite")
}

fn job_in(namespace: Option<&str>, task_name: &str) -> NewJob {
    NewJob {
        queue: "default".to_string(),
        task_name: task_name.to_string(),
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
        namespace: namespace.map(str::to_string),
    }
}

/// Dead-letter one job per namespace and return their `(a, b)` entry ids.
fn dead_letter_one_each(storage: &SqliteStorage) -> (String, String) {
    let mut ids = Vec::new();
    for tenant in [TENANT_A, TENANT_B] {
        let job = storage
            .enqueue(job_in(Some(tenant), "doomed"))
            .expect("enqueue");
        storage
            .move_to_dlq(&job, "boom", None)
            .expect("dead-letter");
        let entry = storage
            .list_dead(10, 0, Some(tenant))
            .expect("list")
            .pop()
            .expect("one entry");
        ids.push(entry.id);
    }
    (ids[0].clone(), ids[1].clone())
}

#[test]
fn dead_letters_are_scoped_and_unscoped_sees_both() {
    let storage = storage();
    dead_letter_one_each(&storage);

    assert_eq!(storage.list_dead(10, 0, Some(TENANT_A)).unwrap().len(), 1);
    assert_eq!(storage.list_dead(10, 0, Some(TENANT_B)).unwrap().len(), 1);
    assert_eq!(
        storage.list_dead(10, 0, None).unwrap().len(),
        2,
        "an unscoped caller keeps seeing every namespace"
    );

    for scoped in storage.list_dead(10, 0, Some(TENANT_A)).unwrap() {
        assert_eq!(scoped.namespace.as_deref(), Some(TENANT_A));
    }
}

#[test]
fn dead_letter_pagination_and_task_filter_are_scoped() {
    let storage = storage();
    dead_letter_one_each(&storage);

    assert_eq!(
        storage
            .list_dead_after(10, None, Some(TENANT_A))
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        storage
            .list_dead_by_task("doomed", 10, 0, Some(TENANT_A))
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        storage
            .list_dead_by_task("doomed", 10, 0, None)
            .unwrap()
            .len(),
        2
    );
}

#[test]
fn a_dead_letter_id_from_another_namespace_reads_as_missing() {
    let storage = storage();
    let (a_entry, _) = dead_letter_one_each(&storage);

    assert!(
        !storage.delete_dead(&a_entry, Some(TENANT_B)).unwrap(),
        "deleting across the boundary must report the same as an unknown id"
    );
    assert!(
        storage.retry_dead(&a_entry, Some(TENANT_B)).is_err(),
        "retrying across the boundary must report not-found"
    );
    // Untouched: the refusals must not have half-applied.
    assert_eq!(storage.list_dead(10, 0, Some(TENANT_A)).unwrap().len(), 1);

    assert!(storage.delete_dead(&a_entry, Some(TENANT_A)).unwrap());
}

#[test]
fn a_job_from_another_namespace_cannot_be_cancelled() {
    let storage = storage();
    let job = storage.enqueue(job_in(Some(TENANT_A), "work")).unwrap();

    assert!(
        !storage.cancel_job(&job.id, Some(TENANT_B)).unwrap(),
        "cancelling across the boundary must report the same as an unknown id"
    );
    assert_eq!(
        storage.get_job(&job.id).unwrap().unwrap().status,
        JobStatus::Pending
    );

    assert!(storage.cancel_job(&job.id, Some(TENANT_A)).unwrap());
}

#[test]
fn stats_are_scoped_per_namespace() {
    let storage = storage();
    storage.enqueue(job_in(Some(TENANT_A), "work")).unwrap();
    storage.enqueue(job_in(Some(TENANT_A), "work")).unwrap();
    storage.enqueue(job_in(Some(TENANT_B), "work")).unwrap();
    storage.enqueue(job_in(None, "work")).unwrap();

    assert_eq!(storage.stats(Some(TENANT_A)).unwrap().pending, 2);
    assert_eq!(storage.stats(Some(TENANT_B)).unwrap().pending, 1);
    assert_eq!(
        storage.stats(None).unwrap().pending,
        4,
        "an unscoped caller keeps counting every namespace"
    );

    assert_eq!(
        storage
            .stats_by_queue("default", Some(TENANT_A))
            .unwrap()
            .pending,
        2
    );
    assert_eq!(
        storage.stats_all_queues(Some(TENANT_A)).unwrap()["default"].pending,
        2
    );
}

#[test]
fn task_logs_are_scoped_per_namespace() {
    let storage = storage();
    storage
        .write_task_log("job-a", "work", "INFO", "from a", None, Some(TENANT_A))
        .unwrap();
    storage
        .write_task_log("job-b", "work", "INFO", "from b", None, Some(TENANT_B))
        .unwrap();

    let scoped = storage
        .query_task_logs(None, None, 0, 100, Some(TENANT_A))
        .unwrap();
    assert_eq!(scoped.len(), 1);
    assert_eq!(scoped[0].message, "from a");

    assert_eq!(
        storage
            .query_task_logs(None, None, 0, 100, None)
            .unwrap()
            .len(),
        2,
        "an unscoped caller keeps seeing every namespace"
    );

    // The scope composes with the existing filters rather than replacing them.
    assert_eq!(
        storage
            .query_task_logs(Some("work"), Some("INFO"), 0, 100, Some(TENANT_B))
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn metrics_are_scoped_per_namespace() {
    let storage = storage();
    storage
        .record_metric("work", "job-a", 10, 0, true, Some(TENANT_A))
        .unwrap();
    storage
        .record_metric("work", "job-b", 20, 0, true, Some(TENANT_B))
        .unwrap();

    let scoped = storage.get_metrics(None, 0, Some(TENANT_A)).unwrap();
    assert_eq!(scoped.len(), 1);
    assert_eq!(scoped[0].job_id, "job-a");

    assert_eq!(
        storage.get_metrics(None, 0, None).unwrap().len(),
        2,
        "an unscoped caller keeps seeing every namespace"
    );
    assert_eq!(
        storage
            .get_metrics(Some("work"), 0, Some(TENANT_B))
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn a_row_written_before_namespaces_is_invisible_to_a_scoped_read() {
    // The migration adds the column as NULL, so pre-existing rows read as
    // unscoped — visible to an unscoped caller, to no tenant in particular.
    let storage = storage();
    storage
        .write_task_log("job-old", "work", "INFO", "legacy", None, None)
        .unwrap();
    storage
        .record_metric("work", "job-old", 5, 0, true, None)
        .unwrap();

    assert!(storage
        .query_task_logs(None, None, 0, 100, Some(TENANT_A))
        .unwrap()
        .is_empty());
    assert!(storage
        .get_metrics(None, 0, Some(TENANT_A))
        .unwrap()
        .is_empty());

    assert_eq!(
        storage
            .query_task_logs(None, None, 0, 100, None)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(storage.get_metrics(None, 0, None).unwrap().len(), 1);
}
