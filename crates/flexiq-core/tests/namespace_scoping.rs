//! Namespace scoping across the dashboard-facing listings and aggregates.
//!
//! `FLEXIQ_NAMESPACE` is meant to be a tenancy boundary, so a caller scoped to
//! one namespace must not see another's jobs, dead letters, logs, metrics, or
//! totals — and must not be able to act on their ids. `None` stays unscoped,
//! matching `list_jobs`, so a single-tenant deployment is unaffected.

use flexiq_core::error::QueueError;
use flexiq_core::job::{now_millis, JobStatus, NewJob};
use flexiq_core::storage::records::DlqDisposition;
use flexiq_core::SqliteStorage;

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
        debounce_key: None,
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
            .move_to_dlq(&job, "boom", None, DlqDisposition::Failed)
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
        storage.get_job(&job.id, None).unwrap().unwrap().status,
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

// ── The id-addressed surface (#597) ────────────────────────────────────
//
// A caller scoped to A, holding a valid id from B, must get not-found from
// every read and no effect from every mutation.

/// One `Running` job per namespace, claimed by `owner`, as `(a_id, b_id)`.
fn running_one_each(storage: &SqliteStorage, owner: &str) -> (String, String) {
    let mut ids = Vec::new();
    for tenant in [TENANT_A, TENANT_B] {
        storage.enqueue(job_in(Some(tenant), "work")).unwrap();
        let job = storage
            .dequeue("default", now_millis(), Some(tenant))
            .unwrap()
            .expect("a job to claim");
        storage.claim_execution(&job.id, owner).unwrap();
        ids.push(job.id);
    }
    (ids[0].clone(), ids[1].clone())
}

#[test]
fn a_job_from_another_namespace_reads_as_missing() {
    let storage = storage();
    let job = storage.enqueue(job_in(Some(TENANT_A), "work")).unwrap();

    assert!(storage.get_job(&job.id, Some(TENANT_B)).unwrap().is_none());
    assert!(storage.get_job(&job.id, Some(TENANT_A)).unwrap().is_some());
    assert!(storage.get_job(&job.id, None).unwrap().is_some());
}

#[test]
fn an_archived_job_from_another_namespace_reads_as_missing() {
    // `get_job` falls back to `archived_jobs`, which needs the same filter as
    // the live table or a terminal job stays readable across the boundary.
    let storage = storage();
    let job = storage.enqueue(job_in(Some(TENANT_A), "work")).unwrap();
    storage.cancel_job(&job.id, Some(TENANT_A)).unwrap();

    assert!(storage.get_job(&job.id, Some(TENANT_B)).unwrap().is_none());
    assert!(storage.get_job(&job.id, Some(TENANT_A)).unwrap().is_some());
}

#[test]
fn archived_listings_are_scoped() {
    let storage = storage();
    for tenant in [TENANT_A, TENANT_B] {
        let job = storage.enqueue(job_in(Some(tenant), "work")).unwrap();
        storage.cancel_job(&job.id, Some(tenant)).unwrap();
    }

    let scoped = storage.list_archived(10, 0, Some(TENANT_A)).unwrap();
    assert_eq!(scoped.len(), 1);
    assert_eq!(scoped[0].namespace.as_deref(), Some(TENANT_A));

    assert_eq!(storage.list_archived(10, 0, None).unwrap().len(), 2);

    let keyset = storage
        .list_archived_after(10, None, Some(TENANT_A))
        .unwrap();
    assert_eq!(keyset.len(), 1);
    assert_eq!(keyset[0].namespace.as_deref(), Some(TENANT_A));
    assert_eq!(
        storage.list_archived_after(10, None, None).unwrap().len(),
        2
    );
}

#[test]
fn a_running_job_from_another_namespace_cannot_be_cancel_requested() {
    let storage = storage();
    let (a_id, _) = running_one_each(&storage, "worker-1");

    assert!(!storage.request_cancel(&a_id, Some(TENANT_B)).unwrap());
    assert!(!storage.is_cancel_requested(&a_id, Some(TENANT_B)).unwrap());

    assert!(storage.request_cancel(&a_id, Some(TENANT_A)).unwrap());
    assert!(storage.is_cancel_requested(&a_id, Some(TENANT_A)).unwrap());
    // The flag is real, so a cross-namespace reader denying it is the filter
    // at work rather than the flag never having been set.
    assert!(!storage.is_cancel_requested(&a_id, Some(TENANT_B)).unwrap());
}

#[test]
fn a_running_job_from_another_namespace_cannot_be_marked_cancelled() {
    let storage = storage();
    let (a_id, _) = running_one_each(&storage, "worker-1");

    storage.mark_cancelled(&a_id, Some(TENANT_B)).unwrap();
    assert_eq!(
        storage.get_job(&a_id, None).unwrap().unwrap().status,
        JobStatus::Running,
        "a cross-namespace mark must leave the job running"
    );

    storage.mark_cancelled(&a_id, Some(TENANT_A)).unwrap();
    assert_eq!(
        storage.get_job(&a_id, None).unwrap().unwrap().status,
        JobStatus::Cancelled
    );
}

#[test]
fn progress_cannot_be_written_across_the_boundary() {
    let storage = storage();
    let (a_id, _) = running_one_each(&storage, "worker-1");

    assert!(
        storage.update_progress(&a_id, 50, Some(TENANT_B)).is_err(),
        "a cross-namespace write must report the same as an unknown id"
    );
    assert_eq!(
        storage.get_job(&a_id, None).unwrap().unwrap().progress,
        None
    );

    storage.update_progress(&a_id, 50, Some(TENANT_A)).unwrap();
    assert_eq!(
        storage.get_job(&a_id, None).unwrap().unwrap().progress,
        Some(50)
    );
}

#[test]
fn job_errors_and_logs_are_scoped_by_their_job() {
    let storage = storage();
    let job = storage.enqueue(job_in(Some(TENANT_A), "work")).unwrap();
    storage.record_error(&job.id, 1, "boom", None).unwrap();
    storage
        .write_task_log(&job.id, "work", "INFO", "hello", None, Some(TENANT_A))
        .unwrap();

    assert!(storage
        .get_job_errors(&job.id, Some(TENANT_B))
        .unwrap()
        .is_empty());
    assert!(storage
        .get_task_logs(&job.id, Some(TENANT_B))
        .unwrap()
        .is_empty());
    assert!(storage
        .get_task_logs_after(&job.id, None, Some(TENANT_B))
        .unwrap()
        .is_empty());

    assert_eq!(
        storage
            .get_job_errors(&job.id, Some(TENANT_A))
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        storage
            .get_task_logs(&job.id, Some(TENANT_A))
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn a_cascade_cancel_reaches_every_dependent_it_can_see() {
    // Every edge is intra-namespace (see the enqueue tests below), so a cascade
    // scoped to one namespace still reaches its whole subgraph. The namespace
    // filter on the cancel itself remains as a guard for edges written before
    // the boundary was enforced.
    let storage = storage();
    let root = storage.enqueue(job_in(Some(TENANT_A), "root")).unwrap();

    let mut sibling = job_in(Some(TENANT_A), "sibling");
    sibling.depends_on = vec![root.id.clone()];
    let sibling = storage.enqueue(sibling).unwrap();

    // A job in another namespace with no edge to `root` must be untouched.
    let bystander = storage
        .enqueue(job_in(Some(TENANT_B), "bystander"))
        .unwrap();

    assert!(storage.cancel_job(&root.id, Some(TENANT_A)).unwrap());

    assert_eq!(
        storage.get_job(&sibling.id, None).unwrap().unwrap().status,
        JobStatus::Cancelled,
        "a dependent in the same namespace must cascade"
    );
    assert_eq!(
        storage
            .get_job(&bystander.id, None)
            .unwrap()
            .unwrap()
            .status,
        JobStatus::Pending,
        "an unrelated job in another namespace must be left alone"
    );
}

#[test]
fn a_dependency_may_not_cross_the_namespace_boundary() {
    // Rejected rather than filtered: the edge would let one tenant's failure
    // cascade into another's queue, and `cascade_cancel` refuses to cross it
    // anyway. It reports as an ordinary missing dependency so the caller
    // learns nothing about ids outside its own namespace.
    let storage = storage();
    let root = storage.enqueue(job_in(Some(TENANT_A), "root")).unwrap();

    let mut foreign = job_in(Some(TENANT_B), "foreign");
    foreign.depends_on = vec![root.id.clone()];
    assert!(matches!(
        storage.enqueue(foreign),
        Err(QueueError::DependencyNotFound(_))
    ));

    // The unique-keyed and batch paths agree with the single enqueue.
    let mut foreign_unique = job_in(Some(TENANT_B), "foreign-unique");
    foreign_unique.depends_on = vec![root.id.clone()];
    foreign_unique.unique_key = Some("foreign-unique".to_string());
    assert!(matches!(
        storage.enqueue_unique(foreign_unique),
        Err(QueueError::DependencyNotFound(_))
    ));

    let mut foreign_batch = job_in(Some(TENANT_B), "foreign-batch");
    foreign_batch.depends_on = vec![root.id.clone()];
    assert!(matches!(
        storage.enqueue_batch(vec![foreign_batch]),
        Err(QueueError::DependencyNotFound(_))
    ));

    // An unscoped job may not depend on a namespaced one either — "no
    // namespace" is its own tenancy group, not a wildcard.
    let mut unscoped = job_in(None, "unscoped");
    unscoped.depends_on = vec![root.id];
    assert!(matches!(
        storage.enqueue(unscoped),
        Err(QueueError::DependencyNotFound(_))
    ));
}

#[test]
fn dependency_edges_are_scoped_by_their_anchor_job() {
    // The edge rows carry no namespace of their own; the anchor job's
    // visibility is what gates the list.
    let storage = storage();
    let root = storage.enqueue(job_in(Some(TENANT_A), "root")).unwrap();

    let mut child = job_in(Some(TENANT_A), "child");
    child.depends_on = vec![root.id.clone()];
    let child = storage.enqueue(child).unwrap();

    assert_eq!(
        storage.get_dependents(&root.id, Some(TENANT_A)).unwrap(),
        vec![child.id.clone()]
    );
    assert_eq!(
        storage.get_dependencies(&child.id, Some(TENANT_A)).unwrap(),
        vec![root.id.clone()]
    );

    assert!(storage
        .get_dependents(&root.id, Some(TENANT_B))
        .unwrap()
        .is_empty());
    assert!(storage
        .get_dependencies(&child.id, Some(TENANT_B))
        .unwrap()
        .is_empty());

    // Unscoped still sees the whole graph.
    assert_eq!(storage.get_dependents(&root.id, None).unwrap().len(), 1);
}

#[test]
fn a_batched_job_still_waits_for_its_dependencies() {
    // `enqueue_batch` used to write `has_deps` without the matching
    // `job_dependencies` rows, so a batched job dispatched immediately and ran
    // ahead of the dependency it was supposed to wait for.
    let storage = storage();
    let root = storage.enqueue(job_in(Some(TENANT_A), "root")).unwrap();

    let mut child = job_in(Some(TENANT_A), "child");
    child.depends_on = vec![root.id.clone()];
    let created = storage.enqueue_batch(vec![child]).unwrap();
    let child_id = created[0].id.clone();

    assert_eq!(
        storage.get_dependencies(&child_id, Some(TENANT_A)).unwrap(),
        vec![root.id.clone()]
    );

    // `root` is the only dequeueable job until it completes.
    let first = storage
        .dequeue("default", now_millis(), Some(TENANT_A))
        .unwrap()
        .expect("root is ready");
    assert_eq!(first.id, root.id);
    assert!(storage
        .dequeue("default", now_millis(), Some(TENANT_A))
        .unwrap()
        .is_none());
}

#[test]
fn the_per_task_concurrency_count_is_scoped() {
    // An unscoped count lets a job elsewhere consume this scheduler's
    // `max_concurrent` budget and reschedule a job that should have run.
    let storage = storage();
    running_one_each(&storage, "worker-1");

    assert_eq!(
        storage
            .count_running_by_task("work", Some(TENANT_A))
            .unwrap(),
        1
    );
    assert_eq!(storage.count_running_by_task("work", None).unwrap(), 2);
}

#[test]
fn a_scheduler_never_reaps_another_namespaces_job() {
    // The recovery paths write through `handle_result`, which records the
    // outcome under the *scheduler's* namespace — so reaping across the
    // boundary both steals the job and misfiles its metric.
    let storage = storage();
    let (a_id, b_id) = running_one_each(&storage, "dead-worker");
    let past_every_timeout = now_millis() + 1_000_000;

    let stale_for_a = storage
        .reap_stale_jobs(past_every_timeout, Some(TENANT_A))
        .unwrap();
    assert_eq!(stale_for_a.len(), 1);
    assert_eq!(stale_for_a[0].id, a_id);

    let orphans_for_a = storage
        .reap_orphaned_jobs(&["live-worker".to_string()], now_millis(), Some(TENANT_A))
        .unwrap();
    assert_eq!(orphans_for_a.len(), 1);
    assert_eq!(orphans_for_a[0].0.id, a_id);

    // Unscoped still sweeps the cluster, so a single-tenant deployment and the
    // dead-worker reaper are unaffected.
    assert_eq!(
        storage
            .reap_stale_jobs(past_every_timeout, None)
            .unwrap()
            .len(),
        2
    );
    assert!(storage
        .reap_orphaned_jobs(&["live-worker".to_string()], now_millis(), None)
        .unwrap()
        .iter()
        .any(|(job, _)| job.id == b_id));
}

// ── Result-path mutations ────────────────────────────────────────────────
//
// These run from `handle_result`, off the scheduler's own result stream, so
// every id on the path came out of a namespace-scoped `dequeue`. The scoping
// here is defence-in-depth against a scheduler bug rather than a reachable
// boundary crossing — but a scheduler must never be able to finish, retry or
// annotate another tenant's job under its own namespace.

#[test]
fn a_running_job_from_another_namespace_cannot_be_completed() {
    let storage = storage();
    let (a_id, _) = running_one_each(&storage, "worker-1");

    assert!(matches!(
        storage.complete(&a_id, Some(vec![1]), Some(TENANT_B)),
        Err(QueueError::JobNotFound(_))
    ));
    assert_eq!(
        storage.get_job(&a_id, None).unwrap().unwrap().status,
        JobStatus::Running,
        "the refused completion must not have half-applied"
    );

    storage
        .complete(&a_id, Some(vec![1]), Some(TENANT_A))
        .unwrap();
    assert_eq!(
        storage.get_job(&a_id, None).unwrap().unwrap().status,
        JobStatus::Complete
    );
}

#[test]
fn a_batch_completion_stops_at_the_namespace_boundary() {
    use flexiq_core::job::JobCompletion;

    let storage = storage();
    let (a_id, b_id) = running_one_each(&storage, "worker-1");

    let completion = |job_id: &str| JobCompletion {
        job_id: job_id.to_string(),
        result: None,
        task_name: "work".to_string(),
        wall_time_ns: 1,
    };

    // One foreign entry fails the whole batch — the caller falls back to the
    // per-job path, which refuses the foreign job on its own.
    assert!(matches!(
        storage.complete_batch(&[completion(&a_id), completion(&b_id)], Some(TENANT_A)),
        Err(QueueError::JobNotFound(_))
    ));
    assert_eq!(
        storage.get_job(&b_id, None).unwrap().unwrap().status,
        JobStatus::Running
    );

    storage
        .complete_batch(&[completion(&a_id)], Some(TENANT_A))
        .unwrap();
    assert_eq!(
        storage.get_job(&a_id, None).unwrap().unwrap().status,
        JobStatus::Complete
    );
}

#[test]
fn a_job_from_another_namespace_cannot_be_retried() {
    let storage = storage();
    let (a_id, _) = running_one_each(&storage, "worker-1");
    let later = now_millis() + 60_000;

    assert!(matches!(
        storage.retry(&a_id, later, Some(TENANT_B)),
        Err(QueueError::JobNotFound(_))
    ));
    let untouched = storage.get_job(&a_id, None).unwrap().unwrap();
    assert_eq!(untouched.status, JobStatus::Running);
    assert_eq!(untouched.retry_count, 0);

    storage.retry(&a_id, later, Some(TENANT_A)).unwrap();
    assert_eq!(
        storage.get_job(&a_id, None).unwrap().unwrap().retry_count,
        1
    );
}

#[test]
fn an_error_is_not_recorded_across_the_namespace_boundary() {
    let storage = storage();
    let (a_id, _) = running_one_each(&storage, "worker-1");

    storage
        .record_error(&a_id, 0, "boom", Some(TENANT_B))
        .unwrap();
    assert!(storage.get_job_errors(&a_id, None).unwrap().is_empty());

    storage
        .record_error(&a_id, 0, "boom", Some(TENANT_A))
        .unwrap();
    assert_eq!(storage.get_job_errors(&a_id, None).unwrap().len(), 1);
}

#[test]
fn an_execution_claim_is_not_released_across_the_namespace_boundary() {
    // Releasing another namespace's claim would hand its job back to this
    // scheduler's poller while the real owner is still running it.
    let storage = storage();
    let (a_id, _) = running_one_each(&storage, "worker-1");

    storage.complete_execution(&a_id, Some(TENANT_B)).unwrap();
    assert!(
        storage
            .reap_orphaned_jobs(&["other-worker".to_string()], now_millis(), None)
            .unwrap()
            .iter()
            .any(|(job, _)| job.id == a_id),
        "a_id's own claim must still be there to be reaped"
    );

    storage.complete_execution(&a_id, Some(TENANT_A)).unwrap();
    assert!(storage
        .reap_orphaned_jobs(&["other-worker".to_string()], now_millis(), None)
        .unwrap()
        .iter()
        .all(|(job, _)| job.id != a_id));
}
