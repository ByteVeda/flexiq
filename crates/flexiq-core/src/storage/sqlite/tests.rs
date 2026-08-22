use super::*;
use crate::error::QueueError;
use crate::job::{now_millis, JobStatus, NewJob};
use crate::storage::records::{DebounceOptions, WorkerRegistration};

fn test_storage() -> SqliteStorage {
    SqliteStorage::in_memory().unwrap()
}

fn make_job(task_name: &str) -> NewJob {
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
        namespace: None,
        debounce_key: None,
    }
}

#[test]
fn test_enqueue_and_get() {
    let storage = test_storage();
    let job = storage.enqueue(make_job("test_task")).unwrap();

    let fetched = storage.get_job(&job.id, None).unwrap().unwrap();
    assert_eq!(fetched.task_name, "test_task");
    assert_eq!(fetched.status, JobStatus::Pending);
}

#[test]
fn test_notes_round_trip() {
    let storage = test_storage();
    let mut new_job = make_job("notes_task");
    new_job.notes = Some(r#"{"customer_id":"cus_abc","tier":"gold"}"#.to_string());

    let job = storage.enqueue(new_job).unwrap();
    let fetched = storage.get_job(&job.id, None).unwrap().unwrap();
    assert_eq!(
        fetched.notes.as_deref(),
        Some(r#"{"customer_id":"cus_abc","tier":"gold"}"#)
    );

    // Absence round-trips as None.
    let plain = storage.enqueue(make_job("plain_task")).unwrap();
    let plain_fetched = storage.get_job(&plain.id, None).unwrap().unwrap();
    assert!(plain_fetched.notes.is_none());
}

#[test]
fn test_notes_survive_dlq_round_trip() {
    let storage = test_storage();
    let mut new_job = make_job("dlq_notes_task");
    new_job.notes = Some(r#"{"customer_id":"cus_xyz"}"#.to_string());
    let job = storage.enqueue(new_job).unwrap();

    storage
        .move_to_dlq(&job, "boom", None)
        .expect("move_to_dlq");

    let dead = storage.list_dead(10, 0, None).unwrap();
    let entry = dead
        .iter()
        .find(|d| d.original_job_id == job.id)
        .expect("dead entry");
    assert_eq!(entry.notes.as_deref(), Some(r#"{"customer_id":"cus_xyz"}"#));

    let new_id = storage.retry_dead(&entry.id, None).expect("retry_dead");
    let retried = storage.get_job(&new_id, None).unwrap().unwrap();
    assert_eq!(
        retried.notes.as_deref(),
        Some(r#"{"customer_id":"cus_xyz"}"#)
    );
}

#[test]
fn test_metadata_survives_dlq_round_trip() {
    // User metadata attached at enqueue must survive job → DLQ → retry_dead, with
    // __dlq_retry_count merged in (it was previously dropped to NULL).
    let storage = test_storage();
    let mut new_job = make_job("dlq_meta_task");
    new_job.metadata = Some(r#"{"user_id":"u1"}"#.to_string());
    let job = storage.enqueue(new_job).unwrap();

    storage
        .move_to_dlq(&job, "boom", None)
        .expect("move_to_dlq");

    let dead = storage.list_dead(10, 0, None).unwrap();
    let entry = dead
        .iter()
        .find(|d| d.original_job_id == job.id)
        .expect("dead entry");
    assert_eq!(entry.metadata.as_deref(), Some(r#"{"user_id":"u1"}"#));

    let new_id = storage.retry_dead(&entry.id, None).expect("retry_dead");
    let retried = storage.get_job(&new_id, None).unwrap().unwrap();
    let meta: serde_json::Value =
        serde_json::from_str(retried.metadata.as_deref().expect("metadata")).unwrap();
    assert_eq!(
        meta["user_id"], "u1",
        "user metadata must survive the round trip"
    );
    assert_eq!(
        meta["__dlq_retry_count"], 1,
        "retry count must be merged in"
    );
}

#[test]
fn test_dequeue() {
    let storage = test_storage();
    let job = storage.enqueue(make_job("dequeue_task")).unwrap();

    let dequeued = storage
        .dequeue("default", now_millis() + 1000, None)
        .unwrap()
        .unwrap();
    assert_eq!(dequeued.id, job.id);
    assert_eq!(dequeued.status, JobStatus::Running);

    // Should not dequeue again
    let none = storage
        .dequeue("default", now_millis() + 1000, None)
        .unwrap();
    assert!(none.is_none());
}

#[test]
fn test_dequeue_batch_claims_n() {
    let storage = test_storage();
    for _ in 0..5 {
        storage.enqueue(make_job("batch_task")).unwrap();
    }

    let claimed = storage
        .dequeue_batch("default", now_millis() + 1000, None, 3)
        .unwrap();
    assert_eq!(claimed.len(), 3);
    for job in &claimed {
        assert_eq!(job.status, JobStatus::Running);
    }

    let running = storage
        .list_jobs(Some(JobStatus::Running as i32), None, None, 100, 0, None)
        .unwrap();
    assert_eq!(running.len(), 3);
}

#[test]
fn test_dequeue_batch_respects_available() {
    let storage = test_storage();
    storage.enqueue(make_job("batch_task")).unwrap();
    storage.enqueue(make_job("batch_task")).unwrap();

    let claimed = storage
        .dequeue_batch("default", now_millis() + 1000, None, 10)
        .unwrap();
    assert_eq!(claimed.len(), 2, "only claims what's available");
}

#[test]
fn test_dequeue_batch_empty_and_zero_max() {
    let storage = test_storage();

    // Empty queue → empty batch.
    let empty = storage
        .dequeue_batch("default", now_millis() + 1000, None, 5)
        .unwrap();
    assert!(empty.is_empty());

    // max == 0 claims nothing even when jobs exist.
    storage.enqueue(make_job("batch_task")).unwrap();
    let zero = storage
        .dequeue_batch("default", now_millis() + 1000, None, 0)
        .unwrap();
    assert!(zero.is_empty());

    // The job must still be pending after a zero-max batch.
    let pending = storage
        .list_jobs(Some(JobStatus::Pending as i32), None, None, 100, 0, None)
        .unwrap();
    assert_eq!(pending.len(), 1);
}

#[test]
fn test_dequeue_batch_no_double_claim() {
    let storage = test_storage();
    for _ in 0..4 {
        storage.enqueue(make_job("batch_task")).unwrap();
    }

    let now = now_millis() + 1000;
    let first = storage.dequeue_batch("default", now, None, 2).unwrap();
    let second = storage.dequeue_batch("default", now, None, 2).unwrap();
    assert_eq!(first.len(), 2);
    assert_eq!(second.len(), 2);

    let mut ids: Vec<String> = first
        .iter()
        .chain(second.iter())
        .map(|j| j.id.clone())
        .collect();
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), 4, "two batches must claim disjoint jobs");
}

#[test]
fn test_dequeue_batch_from_across_queues() {
    let storage = test_storage();

    let mut a = make_job("batch_task");
    a.queue = "qa".to_string();
    storage.enqueue(a).unwrap();
    storage
        .enqueue({
            let mut j = make_job("batch_task");
            j.queue = "qa".to_string();
            j
        })
        .unwrap();
    storage
        .enqueue({
            let mut j = make_job("batch_task");
            j.queue = "qb".to_string();
            j
        })
        .unwrap();

    let queues = vec!["qa".to_string(), "qb".to_string()];
    let claimed = storage
        .dequeue_batch_from(
            &queues,
            now_millis() + 1000,
            None,
            10,
            &std::collections::HashMap::new(),
        )
        .unwrap();
    assert_eq!(claimed.len(), 3, "claims across both queues");

    let queue_names: std::collections::HashSet<&str> =
        claimed.iter().map(|j| j.queue.as_str()).collect();
    assert!(queue_names.contains("qa"));
    assert!(queue_names.contains("qb"));
}

#[test]
fn test_dequeue_respects_schedule() {
    let storage = test_storage();
    let future = now_millis() + 60_000;
    let mut new_job = make_job("future_task");
    new_job.scheduled_at = future;
    storage.enqueue(new_job).unwrap();

    let none = storage.dequeue("default", now_millis(), None).unwrap();
    assert!(none.is_none());

    let some = storage.dequeue("default", future + 1, None).unwrap();
    assert!(some.is_some());
}

#[test]
fn test_priority_ordering() {
    let storage = test_storage();

    let mut low = make_job("low_priority");
    low.priority = 1;
    storage.enqueue(low).unwrap();

    let mut high = make_job("high_priority");
    high.priority = 10;
    storage.enqueue(high).unwrap();

    let now = now_millis() + 1000;
    let first = storage.dequeue("default", now, None).unwrap().unwrap();
    assert_eq!(first.task_name, "high_priority");

    let second = storage.dequeue("default", now, None).unwrap().unwrap();
    assert_eq!(second.task_name, "low_priority");
}

#[test]
fn test_complete() {
    let storage = test_storage();
    let job = storage.enqueue(make_job("complete_task")).unwrap();
    storage
        .dequeue("default", now_millis() + 1000, None)
        .unwrap();

    storage.complete(&job.id, Some(vec![42]), None).unwrap();

    let fetched = storage.get_job(&job.id, None).unwrap().unwrap();
    assert_eq!(fetched.status, JobStatus::Complete);
    assert_eq!(fetched.result, Some(vec![42]));
}

#[test]
fn test_fail_and_retry() {
    let storage = test_storage();
    let job = storage.enqueue(make_job("fail_task")).unwrap();
    storage
        .dequeue("default", now_millis() + 1000, None)
        .unwrap();

    storage.fail(&job.id, "something broke").unwrap();
    let fetched = storage.get_job(&job.id, None).unwrap().unwrap();
    assert_eq!(fetched.status, JobStatus::Failed);
    assert_eq!(fetched.error.as_deref(), Some("something broke"));
}

#[test]
fn test_retry_reschedule() {
    let storage = test_storage();
    let job = storage.enqueue(make_job("retry_task")).unwrap();
    storage
        .dequeue("default", now_millis() + 1000, None)
        .unwrap();

    let future = now_millis() + 5000;
    storage.retry(&job.id, future, None).unwrap();

    let fetched = storage.get_job(&job.id, None).unwrap().unwrap();
    assert_eq!(fetched.status, JobStatus::Pending);
    assert_eq!(fetched.retry_count, 1);
    assert_eq!(fetched.scheduled_at, future);
}

#[test]
fn test_reschedule_preserves_retry_count() {
    // Soft-gate reschedules (rate limit, circuit breaker, concurrency,
    // backpressure) must NOT consume the job's retry budget, unlike retry().
    let storage = test_storage();
    let job = storage.enqueue(make_job("reschedule_task")).unwrap();
    storage
        .dequeue("default", now_millis() + 1000, None)
        .unwrap();

    let future = now_millis() + 5000;
    storage.reschedule(&job.id, future).unwrap();

    let fetched = storage.get_job(&job.id, None).unwrap().unwrap();
    assert_eq!(fetched.status, JobStatus::Pending);
    assert_eq!(fetched.scheduled_at, future);
    assert_eq!(
        fetched.retry_count, 0,
        "reschedule must not burn retry budget"
    );

    // Repeated reschedules still never touch retry_count.
    storage
        .dequeue("default", now_millis() + 1000, None)
        .unwrap();
    storage.reschedule(&job.id, future + 1000).unwrap();
    let again = storage.get_job(&job.id, None).unwrap().unwrap();
    assert_eq!(again.retry_count, 0);

    // Unknown id is reported, matching retry().
    assert!(storage.reschedule("missing-id", future).is_err());
}

#[test]
fn test_dead_letter_queue() {
    let storage = test_storage();
    let job = storage.enqueue(make_job("dlq_task")).unwrap();
    storage
        .dequeue("default", now_millis() + 1000, None)
        .unwrap();

    storage
        .move_to_dlq(
            &storage.get_job(&job.id, None).unwrap().unwrap(),
            "max retries exceeded",
            None,
        )
        .unwrap();

    let fetched = storage.get_job(&job.id, None).unwrap().unwrap();
    assert_eq!(fetched.status, JobStatus::Dead);

    let dead = storage.list_dead(10, 0, None).unwrap();
    assert_eq!(dead.len(), 1);
    assert_eq!(dead[0].original_job_id, job.id);
}

#[test]
fn test_retry_dead() {
    let storage = test_storage();
    let job = storage.enqueue(make_job("retry_dead_task")).unwrap();
    storage
        .dequeue("default", now_millis() + 1000, None)
        .unwrap();

    let running_job = storage.get_job(&job.id, None).unwrap().unwrap();
    storage
        .move_to_dlq(&running_job, "fatal error", None)
        .unwrap();

    let dead = storage.list_dead(10, 0, None).unwrap();
    let new_id = storage.retry_dead(&dead[0].id, None).unwrap();

    let new_job = storage.get_job(&new_id, None).unwrap().unwrap();
    assert_eq!(new_job.status, JobStatus::Pending);
    assert_eq!(new_job.task_name, "retry_dead_task");

    let dead = storage.list_dead(10, 0, None).unwrap();
    assert!(dead.is_empty());
}

#[test]
fn test_retry_dead_missing_id_returns_not_found() {
    let storage = test_storage();
    match storage.retry_dead("does-not-exist", None) {
        Err(QueueError::JobNotFound(id)) => assert_eq!(id, "does-not-exist"),
        other => panic!("expected JobNotFound, got {other:?}"),
    }
}

/// A database created before `0009_worker_sdk` must gain the columns on open,
/// not fail against a schema that no longer matches the row structs.
#[test]
fn worker_sdk_columns_are_added_to_a_pre_existing_database() {
    use diesel::connection::SimpleConnection;

    let storage = test_storage();

    // Rewind to the pre-0009 shape: drop the columns and forget the ledger row.
    let mut conn = storage.conn().unwrap();
    conn.batch_execute(
        "ALTER TABLE workers DROP COLUMN sdk;
         ALTER TABLE workers DROP COLUMN sdk_version;
         DELETE FROM schema_migrations WHERE version = '0009_worker_sdk';",
    )
    .unwrap();
    drop(conn);

    storage.migrate().unwrap();

    storage
        .register_worker(&WorkerRegistration {
            worker_id: "migrated",
            queues: "default",
            threads: 1,
            sdk: Some("python"),
            sdk_version: Some("1.2.3"),
            ..Default::default()
        })
        .unwrap();

    let workers = storage.list_workers().unwrap();
    let worker = workers.iter().find(|w| w.worker_id == "migrated").unwrap();
    assert_eq!(worker.sdk.as_deref(), Some("python"));
    assert_eq!(worker.sdk_version.as_deref(), Some("1.2.3"));
}

/// The same rewind for `0012_worker_registry_fingerprint`. Kept separate from
/// the `0009` case so a failure names which migration stopped applying rather
/// than "one of the worker columns".
#[test]
fn the_registry_fingerprint_column_is_added_to_a_pre_existing_database() {
    use diesel::connection::SimpleConnection;

    let storage = test_storage();

    let mut conn = storage.conn().unwrap();
    conn.batch_execute(
        "ALTER TABLE workers DROP COLUMN registry_fingerprint;
         DELETE FROM schema_migrations WHERE version = '0012_worker_registry_fingerprint';",
    )
    .unwrap();
    drop(conn);

    storage.migrate().unwrap();
    // Twice: re-running a completed migration must be a no-op, not a
    // duplicate-column error.
    storage.migrate().unwrap();

    storage
        .register_worker(&WorkerRegistration {
            worker_id: "fingerprinted",
            queues: "default",
            threads: 1,
            registry_fingerprint: Some("fafd30ef8ebcb7de"),
            ..Default::default()
        })
        .unwrap();

    let workers = storage.list_workers().unwrap();
    let worker = workers
        .iter()
        .find(|w| w.worker_id == "fingerprinted")
        .unwrap();
    assert_eq!(
        worker.registry_fingerprint.as_deref(),
        Some("fafd30ef8ebcb7de")
    );
}

/// Re-running a completed migration is a no-op rather than a duplicate-column error.
#[test]
fn worker_sdk_migration_is_idempotent() {
    let storage = test_storage();

    storage.migrate().unwrap();
    storage.migrate().unwrap();

    storage
        .register_worker(&WorkerRegistration {
            worker_id: "twice",
            queues: "default",
            threads: 1,
            sdk: Some("node"),
            ..Default::default()
        })
        .unwrap();
    let workers = storage.list_workers().unwrap();
    let worker = workers.iter().find(|w| w.worker_id == "twice").unwrap();
    assert_eq!(worker.sdk.as_deref(), Some("node"));
    assert_eq!(worker.sdk_version, None);
}

/// The debounce key must survive both the full row read and the blob-free
/// narrow read that listings use — those are separate projections.
#[test]
fn debounce_key_round_trips_through_the_jobs_table() {
    let storage = test_storage();

    let mut new_job = make_job("build_report");
    new_job.debounce_key = Some("report:user-7".to_string());
    let job = storage.enqueue(new_job).unwrap();
    assert_eq!(job.debounce_key.as_deref(), Some("report:user-7"));

    let fetched = storage.get_job(&job.id, None).unwrap().unwrap();
    assert_eq!(fetched.debounce_key.as_deref(), Some("report:user-7"));

    let listed = storage.list_jobs(None, None, None, 10, 0, None).unwrap();
    let listed = listed.iter().find(|j| j.id == job.id).unwrap();
    assert_eq!(listed.debounce_key.as_deref(), Some("report:user-7"));
}

/// `archived_jobs` deliberately has no `debounce_key` column: a terminal job
/// has left its debounce window, so the key reads back as absent rather than
/// stale. Locks the decision recorded in `m0010_debounce`.
#[test]
fn debounce_key_is_dropped_when_a_job_is_archived() {
    let storage = test_storage();

    let mut new_job = make_job("build_report");
    new_job.debounce_key = Some("report:user-7".to_string());
    let job = storage.enqueue(new_job).unwrap();

    storage.dequeue("default", now_millis(), None).unwrap();
    storage.complete(&job.id, Some(vec![1]), None).unwrap();

    let archived = storage.get_job(&job.id, None).unwrap().unwrap();
    assert_eq!(archived.status, JobStatus::Complete);
    assert_eq!(archived.debounce_key, None);
}

/// A database created before `0010_debounce` must gain the column on the next
/// migrate, not fail against a schema that no longer matches the row structs.
#[test]
fn debounce_key_column_is_added_to_a_pre_existing_database() {
    use diesel::connection::SimpleConnection;

    let storage = test_storage();

    // Rewind to the pre-0010 shape: drop the column and its index, and forget
    // the ledger row.
    let mut conn = storage.conn().unwrap();
    conn.batch_execute(
        "DROP INDEX idx_jobs_debounce_key;
         ALTER TABLE jobs DROP COLUMN debounce_key;
         DELETE FROM schema_migrations WHERE version = '0010_debounce';",
    )
    .unwrap();
    drop(conn);

    storage.migrate().unwrap();

    let mut new_job = make_job("build_report");
    new_job.debounce_key = Some("report:migrated".to_string());
    let job = storage.enqueue(new_job).unwrap();
    let fetched = storage.get_job(&job.id, None).unwrap().unwrap();
    assert_eq!(fetched.debounce_key.as_deref(), Some("report:migrated"));
}

// ── Debounced enqueue ────────────────────────────────────────────────

fn debounce_opts(window_ms: i64, max_wait_ms: i64) -> DebounceOptions {
    DebounceOptions {
        window_ms,
        max_wait_ms,
        replace_payload: false,
    }
}

fn debounced_job(key: &str) -> NewJob {
    let mut new_job = make_job("build_report");
    new_job.debounce_key = Some(key.to_string());
    new_job
}

/// Move a pending job's `created_at` back, standing in for a window that opened
/// `age_ms` ago. The alternative is sleeping out a real `max_wait`.
fn backdate_creation(storage: &SqliteStorage, job_id: &str, age_ms: i64) {
    use crate::storage::schema::jobs;
    let mut conn = storage.conn().unwrap();
    diesel::update(jobs::table.filter(jobs::id.eq(job_id)))
        .set(jobs::created_at.eq(now_millis() - age_ms))
        .execute(&mut conn)
        .unwrap();
}

/// The whole point: repeated enqueues under one key produce one job, and each
/// one pushes its deadline further out.
#[test]
fn debounced_enqueue_collapses_a_burst_into_one_job() {
    let storage = test_storage();
    let before = now_millis();

    let mut ids = std::collections::HashSet::new();
    for _ in 0..5 {
        let job = storage
            .enqueue_debounced(debounced_job("report:user-7"), debounce_opts(5_000, 60_000))
            .unwrap();
        assert!(
            job.scheduled_at >= before + 5_000,
            "deadline slides forward"
        );
        ids.insert(job.id);
    }

    assert_eq!(ids.len(), 1, "the burst must land on one job");
    let pending = storage
        .list_jobs(Some(JobStatus::Pending as i32), None, None, 10, 0, None)
        .unwrap();
    assert_eq!(pending.len(), 1, "no second row was inserted");
}

/// `max_wait_ms` is measured from the pending job's `created_at`, so a caller
/// who never stops enqueuing cannot starve the job past that ceiling.
#[test]
fn debounce_max_wait_caps_the_slide() {
    let storage = test_storage();

    let first = storage
        .enqueue_debounced(debounced_job("report:user-7"), debounce_opts(5_000, 20_000))
        .unwrap();
    // 18s into a 20s ceiling: only 2s of slide is left, not the full window.
    backdate_creation(&storage, &first.id, 18_000);

    let now = now_millis();
    let slid = storage
        .enqueue_debounced(debounced_job("report:user-7"), debounce_opts(5_000, 20_000))
        .unwrap();

    assert_eq!(slid.id, first.id);
    assert!(
        slid.scheduled_at <= now + 2_000,
        "capped at first_seen + max_wait, got {} (now {now})",
        slid.scheduled_at
    );
    assert!(slid.scheduled_at > now, "the cap has not elapsed yet");
}

/// Once the ceiling has elapsed the job is due and further enqueues stop
/// deferring it. This is the anti-starvation guarantee: a caller holding the
/// button down can no longer push the run out.
#[test]
fn debounce_stops_deferring_once_the_ceiling_elapses() {
    let storage = test_storage();

    let first = storage
        .enqueue_debounced(debounced_job("report:user-7"), debounce_opts(5_000, 20_000))
        .unwrap();
    // 90s into a 20s ceiling — a backlogged worker has not picked it up yet.
    backdate_creation(&storage, &first.id, 90_000);

    for _ in 0..3 {
        let slid = storage
            .enqueue_debounced(debounced_job("report:user-7"), debounce_opts(5_000, 20_000))
            .unwrap();
        assert_eq!(slid.id, first.id);
        assert!(
            slid.scheduled_at <= now_millis(),
            "an elapsed ceiling leaves the job due, never further out"
        );
    }

    assert!(
        storage
            .dequeue("default", now_millis(), None)
            .unwrap()
            .is_some(),
        "the job is dispatchable despite the ongoing burst"
    );
}

/// A job a worker already holds must never be pulled back to a later deadline.
/// `claim_execution` writes its row without touching `status`, so the guard has
/// to consult the claim table, not just the status column.
#[test]
fn debounce_leaves_a_claimed_job_alone() {
    let storage = test_storage();

    let claimed = storage
        .enqueue_debounced(debounced_job("report:user-7"), debounce_opts(5_000, 60_000))
        .unwrap();
    assert!(storage.claim_execution(&claimed.id, "worker-1").unwrap());

    let fresh = storage
        .enqueue_debounced(debounced_job("report:user-7"), debounce_opts(5_000, 60_000))
        .unwrap();

    assert_ne!(fresh.id, claimed.id, "a claimed job opens a fresh window");
    let untouched = storage.get_job(&claimed.id, None).unwrap().unwrap();
    assert_eq!(untouched.scheduled_at, claimed.scheduled_at);
}

/// Once a job leaves `Pending` its window is over, so the next enqueue starts a
/// new one instead of resurrecting the running job's schedule.
#[test]
fn debounce_opens_a_fresh_window_once_the_job_is_running() {
    let storage = test_storage();

    let first = storage
        .enqueue_debounced(debounced_job("report:user-7"), debounce_opts(5_000, 60_000))
        .unwrap();
    let dequeued = storage
        .dequeue("default", first.scheduled_at, None)
        .unwrap()
        .unwrap();
    assert_eq!(dequeued.id, first.id);

    let second = storage
        .enqueue_debounced(debounced_job("report:user-7"), debounce_opts(5_000, 60_000))
        .unwrap();
    assert_ne!(second.id, first.id);
}

/// The key is payload-derived by design (`report:{user_id}`), so two keys must
/// not share a window — and neither must two tenants using the same key.
#[test]
fn distinct_debounce_keys_and_namespaces_stay_independent() {
    let storage = test_storage();

    let user_7 = storage
        .enqueue_debounced(debounced_job("report:user-7"), debounce_opts(5_000, 60_000))
        .unwrap();
    let user_9 = storage
        .enqueue_debounced(debounced_job("report:user-9"), debounce_opts(5_000, 60_000))
        .unwrap();
    assert_ne!(user_7.id, user_9.id);

    let mut tenant_job = debounced_job("report:user-7");
    tenant_job.namespace = Some("tenant-a".to_string());
    let tenant = storage
        .enqueue_debounced(tenant_job, debounce_opts(5_000, 60_000))
        .unwrap();
    assert_ne!(
        tenant.id, user_7.id,
        "a namespace boundary is also a debounce boundary"
    );
}

/// A coalescing call is a vote to run again soon, not a redefinition of the
/// run: everything but the deadline — and the payload under `replace_payload` —
/// belongs to the job that opened the window. Without this, widening the
/// coalescing `UPDATE` would break the documented contract silently.
#[test]
fn coalescing_discards_the_rest_of_the_new_call() {
    let storage = test_storage();

    let mut opening = debounced_job("report:user-7");
    opening.priority = 1;
    opening.metadata = Some(r#"{"round":1}"#.to_string());
    opening.expires_at = Some(now_millis() + 900_000);
    let first = storage
        .enqueue_debounced(opening, debounce_opts(5_000, 60_000))
        .unwrap();

    let mut louder = debounced_job("report:user-7");
    louder.priority = 9;
    louder.metadata = Some(r#"{"round":2}"#.to_string());
    louder.expires_at = Some(now_millis() + 1);
    let coalesced = storage
        .enqueue_debounced(louder, debounce_opts(5_000, 60_000))
        .unwrap();

    assert_eq!(coalesced.id, first.id);
    assert_eq!(coalesced.priority, 1, "priority belongs to the opener");
    assert_eq!(coalesced.metadata, first.metadata);
    assert_eq!(coalesced.expires_at, first.expires_at);
}

/// `replace_payload` is the difference between "run with the latest input" and
/// "run with the input that opened the window".
#[test]
fn replace_payload_controls_which_payload_survives() {
    let storage = test_storage();

    let mut opening = debounced_job("report:user-7");
    opening.payload = vec![1];
    let first = storage
        .enqueue_debounced(opening, debounce_opts(5_000, 60_000))
        .unwrap();

    let mut kept = debounced_job("report:user-7");
    kept.payload = vec![2];
    let unchanged = storage
        .enqueue_debounced(kept, debounce_opts(5_000, 60_000))
        .unwrap();
    assert_eq!(unchanged.id, first.id);
    assert_eq!(unchanged.payload, vec![1], "the window's payload is kept");

    let mut newest = debounced_job("report:user-7");
    newest.payload = vec![3];
    let replaced = storage
        .enqueue_debounced(
            newest,
            DebounceOptions {
                replace_payload: true,
                ..debounce_opts(5_000, 60_000)
            },
        )
        .unwrap();
    assert_eq!(replaced.id, first.id);
    assert_eq!(replaced.payload, vec![3]);
    assert_eq!(
        storage.get_job(&first.id, None).unwrap().unwrap().payload,
        vec![3],
        "the swap is persisted, not just reflected in the return value"
    );
}

/// Each rejected combination is a silent-misbehaviour trap, not a style
/// preference: no key debounces every job of the task against every other, a
/// non-positive window schedules into the past, and a ceiling below the window
/// makes the window meaningless.
#[test]
fn debounced_enqueue_rejects_options_that_cannot_debounce() {
    let storage = test_storage();

    let no_key = storage.enqueue_debounced(make_job("build_report"), debounce_opts(5_000, 60_000));
    assert!(matches!(no_key, Err(QueueError::Config(_))));

    let empty_key = storage.enqueue_debounced(debounced_job(""), debounce_opts(5_000, 60_000));
    assert!(matches!(empty_key, Err(QueueError::Config(_))));

    let no_window =
        storage.enqueue_debounced(debounced_job("report:user-7"), debounce_opts(0, 60_000));
    assert!(matches!(no_window, Err(QueueError::Config(_))));

    let short_ceiling =
        storage.enqueue_debounced(debounced_job("report:user-7"), debounce_opts(5_000, 1_000));
    assert!(matches!(short_ceiling, Err(QueueError::Config(_))));

    // A rejected call must not have written anything.
    let all = storage.list_jobs(None, None, None, 10, 0, None).unwrap();
    assert!(all.is_empty());
}

/// A window large enough to overflow the epoch must saturate, not wrap. Wrapping
/// lands `scheduled_at` in the *past* and dispatches the job at once — the exact
/// opposite of the request — and panics outright in a debug build.
#[test]
fn an_overflowing_window_saturates_instead_of_dispatching_at_once() {
    let storage = test_storage();

    let job = storage
        .enqueue_debounced(
            debounced_job("report:user-7"),
            debounce_opts(i64::MAX, i64::MAX),
        )
        .unwrap();

    assert_eq!(job.scheduled_at, i64::MAX);
    assert!(
        storage
            .dequeue("default", now_millis(), None)
            .unwrap()
            .is_none(),
        "a saturated deadline must not be due now"
    );
}

#[test]
fn test_reap_if_leader_only_leader_reaps_and_gets_ids() {
    use diesel::prelude::*;

    use crate::storage::schema::workers;

    let storage = test_storage();
    storage
        .register_worker(&WorkerRegistration {
            worker_id: "stale",
            queues: "default",
            threads: 1,
            ..Default::default()
        })
        .unwrap();
    let cutoff = now_millis() - crate::storage::DEAD_WORKER_THRESHOLD_MS - 1_000;
    let mut conn = storage.conn().unwrap();
    diesel::update(workers::table.filter(workers::worker_id.eq("stale")))
        .set(workers::last_heartbeat.eq(cutoff))
        .execute(&mut conn)
        .unwrap();
    drop(conn);

    // First caller wins the reaper election and gets the reaped ids; a second
    // worker on the same tick loses the election and must get an empty list.
    let reaped = crate::storage::reap_dead_workers_if_leader(&storage, "w-leader");
    assert_eq!(reaped, vec!["stale".to_string()]);
    let non_leader = crate::storage::reap_dead_workers_if_leader(&storage, "w-follower");
    assert!(non_leader.is_empty());

    // The leader keeps the lock across ticks (renew-then-acquire).
    assert!(crate::storage::reap_dead_workers_if_leader(&storage, "w-leader").is_empty());
}

#[test]
fn test_sweep_ephemeral_subscriptions_election_and_unconditional() {
    let storage = test_storage();
    // Aged past the registration grace window so the sweep may act on it.
    let created = now_millis() - crate::storage::EPHEMERAL_SUBSCRIPTION_GRACE_MS - 1_000;
    storage
        .register_subscription(&make_sub("t", "eph", "task", Some("dead-worker"), created))
        .unwrap();

    // Leader sweeps the dead-owned row; a follower on the same tick gets 0
    // without sweeping.
    let swept = crate::storage::sweep_ephemeral_subscriptions(&storage, Some("w-leader")).unwrap();
    assert_eq!(swept, 1);
    assert_eq!(
        crate::storage::sweep_ephemeral_subscriptions(&storage, Some("w-follower")).unwrap(),
        0
    );

    // An unconditional (admin) sweep needs no election.
    storage
        .register_subscription(&make_sub("t", "eph2", "task", Some("dead-worker"), created))
        .unwrap();
    let swept = crate::storage::sweep_ephemeral_subscriptions(&storage, None).unwrap();
    assert_eq!(swept, 1);
}

#[test]
fn test_reap_dead_workers_removes_stale_keeps_fresh() {
    use diesel::prelude::*;

    use crate::storage::schema::workers;

    let storage = test_storage();
    storage
        .register_worker(&WorkerRegistration {
            worker_id: "stale",
            queues: "default",
            threads: 1,
            ..Default::default()
        })
        .unwrap();
    storage
        .register_worker(&WorkerRegistration {
            worker_id: "fresh",
            queues: "default",
            threads: 1,
            ..Default::default()
        })
        .unwrap();

    // Backdate `stale` past the dead-worker threshold (30s) so it is reaped;
    // `fresh` keeps its current heartbeat. This also exercises the new
    // double-predicate in the DELETE — even if a worker_id ends up in the
    // scan list, the DELETE only removes rows whose heartbeat is still stale.
    let cutoff = now_millis() - crate::storage::DEAD_WORKER_THRESHOLD_MS - 1_000;
    let mut conn = storage.conn().unwrap();
    diesel::update(workers::table.filter(workers::worker_id.eq("stale")))
        .set(workers::last_heartbeat.eq(cutoff))
        .execute(&mut conn)
        .unwrap();
    drop(conn);

    let reaped = storage.reap_dead_workers().unwrap();
    assert_eq!(reaped, vec!["stale".to_string()]);

    let surviving: Vec<String> = storage
        .list_workers()
        .unwrap()
        .into_iter()
        .map(|w| w.worker_id)
        .collect();
    assert!(surviving.contains(&"fresh".to_string()));
    assert!(!surviving.contains(&"stale".to_string()));
}

#[test]
fn test_list_live_worker_ids_filters_stale() {
    use diesel::prelude::*;

    use crate::storage::schema::workers;

    let storage = test_storage();
    storage
        .register_worker(&WorkerRegistration {
            worker_id: "stale",
            queues: "default",
            threads: 1,
            ..Default::default()
        })
        .unwrap();
    storage
        .register_worker(&WorkerRegistration {
            worker_id: "fresh",
            queues: "default",
            threads: 1,
            ..Default::default()
        })
        .unwrap();

    let now = now_millis();
    let mut conn = storage.conn().unwrap();
    diesel::update(workers::table.filter(workers::worker_id.eq("stale")))
        .set(workers::last_heartbeat.eq(now - crate::storage::DEAD_WORKER_THRESHOLD_MS - 1_000))
        .execute(&mut conn)
        .unwrap();
    drop(conn);

    let live = storage
        .list_live_worker_ids(crate::storage::dead_worker_cutoff(now))
        .unwrap();
    assert_eq!(live, vec!["fresh".to_string()]);
}

#[test]
fn test_stats() {
    let storage = test_storage();
    storage.enqueue(make_job("t1")).unwrap();
    storage.enqueue(make_job("t2")).unwrap();

    let stats = storage.stats(None).unwrap();
    assert_eq!(stats.pending, 2);
    assert_eq!(stats.running, 0);
}

#[test]
fn test_cancel_job() {
    let storage = test_storage();
    let job = storage.enqueue(make_job("cancel_me")).unwrap();

    assert!(storage.cancel_job(&job.id, None).unwrap());

    let fetched = storage.get_job(&job.id, None).unwrap().unwrap();
    assert_eq!(fetched.status, JobStatus::Cancelled);

    // Cancelling again should return false
    assert!(!storage.cancel_job(&job.id, None).unwrap());
}

#[test]
fn test_unique_key_dedup() {
    let storage = test_storage();

    let mut job1 = make_job("unique_task");
    job1.unique_key = Some("my-key".to_string());
    let j1 = storage.enqueue_unique(job1).unwrap();

    let mut job2 = make_job("unique_task");
    job2.unique_key = Some("my-key".to_string());
    let j2 = storage.enqueue_unique(job2).unwrap();

    // Should return the same job
    assert_eq!(j1.id, j2.id);
}

#[test]
fn test_enqueue_unique_rejects_missing_dependency() {
    // enqueue_unique must validate dependencies like enqueue (it previously
    // inserted dep rows blind, treating a bogus dep as satisfied).
    let storage = test_storage();
    let mut job = make_job("unique_orphan");
    job.unique_key = Some("uk-missing-dep".to_string());
    job.depends_on = vec!["nonexistent-id".to_string()];
    assert!(matches!(
        storage.enqueue_unique(job),
        Err(QueueError::DependencyNotFound(_))
    ));
}

#[test]
fn test_enqueue_batch_dedup_is_atomic() {
    // A mixed keyed/keyless batch runs as one transaction on the Diesel
    // backends, so a row that cannot insert must not leave its batch-mates
    // behind. Rejection is forced with an unknown dependency.
    let storage = test_storage();
    let mut keyed = make_job("ebd_atomic");
    keyed.unique_key = Some("uk-ebd-atomic".to_string());
    let mut doomed = make_job("ebd_atomic");
    doomed.depends_on = vec!["nonexistent-id".to_string()];

    assert!(matches!(
        crate::storage::enqueue_batch_dedup(&storage, vec![keyed, doomed]),
        Err(QueueError::DependencyNotFound(_))
    ));
    assert_eq!(
        storage.stats(None).unwrap().pending,
        0,
        "the keyed row must roll back with the batch, not persist alone"
    );
}

#[test]
fn test_enqueue_unique_rejects_dead_dependency() {
    let storage = test_storage();
    // A cancelled (archived, non-Complete) dependency must be rejected.
    let dep = storage.enqueue(make_job("dep_to_cancel")).unwrap();
    assert!(storage.cancel_job(&dep.id, None).unwrap());

    let mut job = make_job("unique_blocked");
    job.unique_key = Some("uk-dead-dep".to_string());
    job.depends_on = vec![dep.id.clone()];
    assert!(matches!(
        storage.enqueue_unique(job),
        Err(QueueError::DependencyNotFound(_))
    ));
}

#[test]
fn test_enqueue_unique_after_dup_completes() {
    // Once the prior holder of a unique_key completes (archived, freeing the
    // partial index), a fresh enqueue_unique must return a real, persisted job —
    // never the phantom (rolled-back) job the old fallback could return.
    let storage = test_storage();
    let mut a = make_job("unique_reuse");
    a.unique_key = Some("uk-reuse".to_string());
    let a = storage.enqueue_unique(a).unwrap();
    storage
        .dequeue("default", now_millis() + 1000, None)
        .unwrap();
    storage.complete(&a.id, None, None).unwrap();

    let mut b = make_job("unique_reuse");
    b.unique_key = Some("uk-reuse".to_string());
    let b = storage.enqueue_unique(b).unwrap();

    assert_ne!(
        a.id, b.id,
        "freed unique key must yield a new job, not dedup to A"
    );
    assert!(
        storage.get_job(&b.id, None).unwrap().is_some(),
        "returned job must actually be persisted (no phantom)"
    );
}

#[test]
fn test_enqueue_batch() {
    let storage = test_storage();
    let jobs: Vec<NewJob> = (0..5)
        .map(|i| {
            let mut j = make_job(&format!("batch_task_{i}"));
            j.priority = i;
            j
        })
        .collect();

    let result = storage.enqueue_batch(jobs).unwrap();
    assert_eq!(result.len(), 5);

    let stats = storage.stats(None).unwrap();
    assert_eq!(stats.pending, 5);
}

#[test]
fn test_record_and_get_job_errors() {
    let storage = test_storage();
    let job = storage.enqueue(make_job("error_task")).unwrap();

    storage
        .record_error(&job.id, 0, "first failure", None)
        .unwrap();
    storage
        .record_error(&job.id, 1, "second failure", None)
        .unwrap();

    let errors = storage.get_job_errors(&job.id, None).unwrap();
    assert_eq!(errors.len(), 2);
    assert_eq!(errors[0].attempt, 0);
    assert_eq!(errors[0].error, "first failure");
    assert_eq!(errors[1].attempt, 1);
    assert_eq!(errors[1].error, "second failure");
}

#[test]
fn test_job_errors_empty_for_success() {
    let storage = test_storage();
    let job = storage.enqueue(make_job("ok_task")).unwrap();

    let errors = storage.get_job_errors(&job.id, None).unwrap();
    assert!(errors.is_empty());
}

#[test]
fn test_purge_job_errors() {
    let storage = test_storage();
    let job = storage.enqueue(make_job("purge_err_task")).unwrap();

    storage.record_error(&job.id, 0, "old error", None).unwrap();
    let purged = storage.purge_job_errors(now_millis() + 10_000).unwrap();
    assert_eq!(purged, 1);

    let errors = storage.get_job_errors(&job.id, None).unwrap();
    assert!(errors.is_empty());
}

#[test]
fn test_purge_job_errors_drains_across_batches() {
    // 550 rows exceed one PURGE_BATCH (500): the batched loop must drain every
    // row across iterations, not stop after the first batch.
    let storage = test_storage();
    let job = storage.enqueue(make_job("purge_err_batch")).unwrap();
    for attempt in 0..550 {
        storage
            .record_error(&job.id, attempt, "boom", None)
            .unwrap();
    }

    let purged = storage.purge_job_errors(now_millis() + 10_000).unwrap();
    assert_eq!(purged, 550, "batched purge must drain every error row");
    assert!(storage.get_job_errors(&job.id, None).unwrap().is_empty());
}

#[test]
fn test_purge_metrics_drains_across_batches() {
    let storage = test_storage();
    let job = storage.enqueue(make_job("purge_metric_batch")).unwrap();
    for _ in 0..550 {
        storage
            .record_metric("purge_metric_batch", &job.id, 1, 1, true, None)
            .unwrap();
    }

    let purged = storage.purge_metrics(now_millis() + 10_000).unwrap();
    assert_eq!(purged, 550, "batched purge must drain every metric row");
    assert!(storage.get_metrics(None, 0, None).unwrap().is_empty());
}

#[test]
fn test_purge_task_logs_drains_across_batches() {
    let storage = test_storage();
    let job = storage.enqueue(make_job("purge_log_batch")).unwrap();
    for _ in 0..550 {
        storage
            .write_task_log(&job.id, "purge_log_batch", "INFO", "msg", None, None)
            .unwrap();
    }

    let purged = storage.purge_task_logs(now_millis() + 10_000).unwrap();
    assert_eq!(purged, 550, "batched purge must drain every log row");
    assert!(storage.get_task_logs(&job.id, None).unwrap().is_empty());
}

#[test]
fn test_progress_tracking() {
    let storage = test_storage();
    let job = storage.enqueue(make_job("progress_task")).unwrap();

    storage.update_progress(&job.id, 50, None).unwrap();
    let fetched = storage.get_job(&job.id, None).unwrap().unwrap();
    assert_eq!(fetched.progress, Some(50));

    storage.update_progress(&job.id, 100, None).unwrap();
    let fetched = storage.get_job(&job.id, None).unwrap().unwrap();
    assert_eq!(fetched.progress, Some(100));
}

// ── Dependency tests ────────────────────────────────────

#[test]
fn test_enqueue_with_dependency() {
    let storage = test_storage();
    let job_a = storage.enqueue(make_job("task_a")).unwrap();

    let mut dep_job = make_job("task_b");
    dep_job.depends_on = vec![job_a.id.clone()];
    let job_b = storage.enqueue(dep_job).unwrap();

    let deps = storage.get_dependencies(&job_b.id, None).unwrap();
    assert_eq!(deps, vec![job_a.id.clone()]);

    let dependents = storage.get_dependents(&job_a.id, None).unwrap();
    assert_eq!(dependents, vec![job_b.id]);
}

#[test]
fn test_dequeue_blocks_on_unmet_dependency() {
    let storage = test_storage();
    let job_a = storage.enqueue(make_job("dep_task")).unwrap();

    let mut dep_job = make_job("dependent_task");
    dep_job.depends_on = vec![job_a.id.clone()];
    storage.enqueue(dep_job).unwrap();

    let now = now_millis() + 1000;

    let dequeued = storage.dequeue("default", now, None).unwrap().unwrap();
    assert_eq!(dequeued.id, job_a.id);

    let none = storage.dequeue("default", now, None).unwrap();
    assert!(none.is_none());

    storage.complete(&job_a.id, None, None).unwrap();

    let dequeued = storage.dequeue("default", now, None).unwrap().unwrap();
    assert_eq!(dequeued.task_name, "dependent_task");
}

#[test]
fn test_cascade_cancel_on_job_cancel() {
    let storage = test_storage();
    let job_a = storage.enqueue(make_job("root")).unwrap();

    let mut dep_b = make_job("child");
    dep_b.depends_on = vec![job_a.id.clone()];
    let job_b = storage.enqueue(dep_b).unwrap();

    let mut dep_c = make_job("grandchild");
    dep_c.depends_on = vec![job_b.id.clone()];
    let job_c = storage.enqueue(dep_c).unwrap();

    storage.cancel_job(&job_a.id, None).unwrap();

    let b = storage.get_job(&job_b.id, None).unwrap().unwrap();
    assert_eq!(b.status, JobStatus::Cancelled);

    let c = storage.get_job(&job_c.id, None).unwrap().unwrap();
    assert_eq!(c.status, JobStatus::Cancelled);
}

#[test]
fn test_cascade_cancel_on_dlq() {
    let storage = test_storage();
    let job_a = storage.enqueue(make_job("parent")).unwrap();

    let mut dep_b = make_job("child_of_dead");
    dep_b.depends_on = vec![job_a.id.clone()];
    let job_b = storage.enqueue(dep_b).unwrap();

    let now = now_millis() + 1000;
    storage.dequeue("default", now, None).unwrap();
    let running = storage.get_job(&job_a.id, None).unwrap().unwrap();
    storage.move_to_dlq(&running, "fatal error", None).unwrap();

    let b = storage.get_job(&job_b.id, None).unwrap().unwrap();
    assert_eq!(b.status, JobStatus::Cancelled);
    assert!(b.error.unwrap().contains("dependency failed"));
}

#[test]
fn test_dequeue_lifo_vs_fifo_order() {
    use crate::storage::DispatchOrder;
    use std::collections::HashMap;

    let storage = test_storage();
    let base = now_millis();
    let now = base + 1000;

    // Same priority, staggered eligibility: index 4 is the newest.
    for i in 0..5i64 {
        let mut job = make_job("t");
        job.queue = "lifoq".to_string();
        job.scheduled_at = base + i;
        storage.enqueue(job).unwrap();
    }
    let mut orders = HashMap::new();
    orders.insert("lifoq".to_string(), DispatchOrder::Lifo);
    let jobs = storage
        .dequeue_batch_from(&["lifoq".to_string()], now, None, 5, &orders)
        .unwrap();
    let sched: Vec<i64> = jobs.iter().map(|j| j.scheduled_at).collect();
    assert_eq!(
        sched,
        vec![base + 4, base + 3, base + 2, base + 1, base],
        "LIFO dispatches newest-first within a priority"
    );

    // FIFO (default, empty map) keeps oldest-first.
    for i in 0..5i64 {
        let mut job = make_job("t");
        job.queue = "fifoq".to_string();
        job.scheduled_at = base + i;
        storage.enqueue(job).unwrap();
    }
    let jobs = storage
        .dequeue_batch_from(&["fifoq".to_string()], now, None, 5, &HashMap::new())
        .unwrap();
    let sched: Vec<i64> = jobs.iter().map(|j| j.scheduled_at).collect();
    assert_eq!(
        sched,
        vec![base, base + 1, base + 2, base + 3, base + 4],
        "FIFO dispatches oldest-first (the default)"
    );
}

#[test]
fn test_dequeue_lifo_priority_still_dominates() {
    use crate::storage::DispatchOrder;
    use std::collections::HashMap;

    let storage = test_storage();
    let base = now_millis();
    let now = base + 1000;

    // A high-priority OLD job and a low-priority NEW job on a LIFO queue.
    let mut old_high = make_job("t");
    old_high.queue = "pq".to_string();
    old_high.priority = 10;
    old_high.scheduled_at = base; // oldest
    storage.enqueue(old_high).unwrap();

    let mut new_low = make_job("t");
    new_low.queue = "pq".to_string();
    new_low.priority = 0;
    new_low.scheduled_at = base + 100; // newest
    storage.enqueue(new_low).unwrap();

    let mut orders = HashMap::new();
    orders.insert("pq".to_string(), DispatchOrder::Lifo);
    let jobs = storage
        .dequeue_batch_from(&["pq".to_string()], now, None, 2, &orders)
        .unwrap();
    // Priority wins even under LIFO: the high-priority job goes first despite
    // being older; LIFO only reorders same-priority ties.
    assert_eq!(jobs[0].priority, 10);
    assert_eq!(jobs[1].priority, 0);
}

#[test]
fn test_count_running_by_task() {
    let storage = test_storage();
    storage.enqueue(make_job("task_a")).unwrap();
    storage.enqueue(make_job("task_a")).unwrap();
    storage.enqueue(make_job("task_b")).unwrap();

    // No running jobs yet
    assert_eq!(storage.count_running_by_task("task_a", None).unwrap(), 0);

    let now = now_millis() + 1000;
    // Dequeue one task_a (becomes running)
    storage.dequeue("default", now, None).unwrap().unwrap();

    assert_eq!(storage.count_running_by_task("task_a", None).unwrap(), 1);
    assert_eq!(storage.count_running_by_task("task_b", None).unwrap(), 0);

    // Dequeue second task_a
    storage.dequeue("default", now, None).unwrap().unwrap();
    assert_eq!(storage.count_running_by_task("task_a", None).unwrap(), 2);

    // Nonexistent task should return 0
    assert_eq!(
        storage.count_running_by_task("no_such_task", None).unwrap(),
        0
    );
}

#[test]
fn test_count_pending_by_queue() {
    let storage = test_storage();
    assert_eq!(storage.count_pending_by_queue("default").unwrap(), 0);

    storage.enqueue(make_job("task_a")).unwrap();
    storage.enqueue(make_job("task_a")).unwrap();
    let mut other = make_job("task_b");
    other.queue = "other".to_string();
    storage.enqueue(other).unwrap();

    assert_eq!(storage.count_pending_by_queue("default").unwrap(), 2);
    assert_eq!(storage.count_pending_by_queue("other").unwrap(), 1);
    assert_eq!(storage.count_pending_by_queue("empty").unwrap(), 0);

    // Dequeue drops the job out of Pending → count decreases.
    let now = now_millis() + 1000;
    storage.dequeue("default", now, None).unwrap().unwrap();
    assert_eq!(storage.count_pending_by_queue("default").unwrap(), 1);
}

#[test]
fn test_enqueue_rejects_missing_dependency() {
    let storage = test_storage();

    let mut dep_job = make_job("orphan");
    dep_job.depends_on = vec!["nonexistent-id".to_string()];
    let result = storage.enqueue(dep_job);
    assert!(result.is_err());
}

#[test]
fn test_setting_get_returns_none_when_unset() {
    let storage = test_storage();
    assert_eq!(storage.get_setting("missing").unwrap(), None);
}

#[test]
fn test_setting_set_and_get() {
    let storage = test_storage();
    storage.set_setting("dashboard.title", "My Queue").unwrap();
    assert_eq!(
        storage.get_setting("dashboard.title").unwrap(),
        Some("My Queue".to_string())
    );
}

#[test]
fn test_setting_set_overwrites() {
    let storage = test_storage();
    storage.set_setting("k", "v1").unwrap();
    storage.set_setting("k", "v2").unwrap();
    assert_eq!(storage.get_setting("k").unwrap(), Some("v2".to_string()));
}

#[test]
fn test_setting_delete() {
    let storage = test_storage();
    storage.set_setting("k", "v").unwrap();
    assert!(storage.delete_setting("k").unwrap());
    assert_eq!(storage.get_setting("k").unwrap(), None);
    // Deleting non-existent returns false.
    assert!(!storage.delete_setting("k").unwrap());
}

#[test]
fn test_setting_list_returns_all() {
    let storage = test_storage();
    storage.set_setting("a", "1").unwrap();
    storage.set_setting("b", "2").unwrap();
    let all = storage.list_settings().unwrap();
    assert_eq!(all.len(), 2);
    assert_eq!(all.get("a"), Some(&"1".to_string()));
    assert_eq!(all.get("b"), Some(&"2".to_string()));
}

#[test]
fn test_setting_cas_writes_only_on_the_expected_value() {
    let storage = test_storage();
    storage.set_setting("k", "v1").unwrap();

    assert!(!storage.set_setting_if("k", Some("stale"), "v2").unwrap());
    assert_eq!(storage.get_setting("k").unwrap(), Some("v1".to_string()));

    assert!(storage.set_setting_if("k", Some("v1"), "v2").unwrap());
    assert_eq!(storage.get_setting("k").unwrap(), Some("v2".to_string()));
}

#[test]
fn test_setting_cas_creates_only_when_unset() {
    let storage = test_storage();
    assert!(storage.set_setting_if("k", None, "first").unwrap());
    assert_eq!(storage.get_setting("k").unwrap(), Some("first".to_string()));

    // The key now exists, so the "must be unset" branch must not overwrite it.
    assert!(!storage.set_setting_if("k", None, "second").unwrap());
    assert_eq!(storage.get_setting("k").unwrap(), Some("first".to_string()));
}

#[test]
fn test_setting_cas_on_a_missing_key_expecting_a_value_fails() {
    let storage = test_storage();
    assert!(!storage.set_setting_if("k", Some("v1"), "v2").unwrap());
    assert_eq!(storage.get_setting("k").unwrap(), None);
}

#[test]
fn test_setting_preserves_unicode_and_json() {
    let storage = test_storage();
    let payload = r#"{"label":"Grafana ⏱️","url":"https://grafana.example/dash"}"#;
    storage.set_setting("dashboard.links.0", payload).unwrap();
    assert_eq!(
        storage.get_setting("dashboard.links.0").unwrap(),
        Some(payload.to_string())
    );
}

#[test]
fn test_reap_stale_jobs_only_returns_expired() {
    let storage = test_storage();
    let t0 = now_millis();

    // Pin both jobs to t0 rather than the wall clock make_job defaults to: a
    // millisecond ticking over between t0 and the enqueue would leave
    // `scheduled_at > t0`, and the dequeues below would claim nothing.
    let mut short = make_job("short_timeout");
    short.timeout_ms = 1;
    short.scheduled_at = t0;
    storage.enqueue(short).unwrap();
    let mut long = make_job("long_timeout");
    long.timeout_ms = 300_000;
    long.scheduled_at = t0;
    storage.enqueue(long).unwrap();

    // Run both: started_at = t0 for each.
    storage.dequeue("default", t0, None).unwrap();
    storage.dequeue("default", t0, None).unwrap();

    // Well past the short job's deadline (t0 + 1) but before the long one's.
    let stale = storage.reap_stale_jobs(t0 + 1_000, None).unwrap();
    assert_eq!(stale.len(), 1);
    assert_eq!(stale[0].task_name, "short_timeout");
}

#[test]
fn test_has_deps_flag_gates_dequeue() {
    let storage = test_storage();

    // No-dependency job: has_deps is false.
    let plain = storage.enqueue(make_job("plain")).unwrap();
    assert!(!plain.has_deps);

    // Dependency target and dependent child, each on its own queue so the
    // dequeue calls are unambiguous.
    let mut target = make_job("target");
    target.queue = "qt".to_string();
    let target = storage.enqueue(target).unwrap();
    let mut child = make_job("child");
    child.queue = "q2".to_string();
    child.depends_on = vec![target.id.clone()];
    let child = storage.enqueue(child).unwrap();
    assert!(child.has_deps);

    let t0 = now_millis();
    // Blocked while the dependency is incomplete.
    assert!(storage.dequeue("q2", t0, None).unwrap().is_none());

    // Complete the dependency, then the child becomes dequeueable.
    storage.dequeue("qt", t0, None).unwrap();
    storage.complete(&target.id, None, None).unwrap();
    let got = storage.dequeue("q2", t0, None).unwrap();
    assert_eq!(got.map(|j| j.id), Some(child.id));
}

#[test]
fn test_enqueue_batch_crosses_chunk_boundary() {
    let storage = test_storage();
    // More than the 50-row insert chunk so multiple multi-row INSERTs run.
    let count = 120;
    let jobs: Vec<NewJob> = (0..count)
        .map(|i| make_job(&format!("batch_{i}")))
        .collect();

    let result = storage.enqueue_batch(jobs).unwrap();
    assert_eq!(result.len(), count);
    assert_eq!(storage.stats(None).unwrap().pending, count as i64);
}

#[test]
fn test_purge_completed_respects_per_job_ttl() {
    let storage = test_storage();

    // Completed with a 1ms TTL — should be purged once that elapses. Each on
    // its own queue so the dequeue/complete pair is unambiguous.
    let mut expired = make_job("ttl_expired");
    expired.queue = "qa".to_string();
    expired.result_ttl_ms = Some(1);
    let expired = storage.enqueue(expired).unwrap();
    // Completed with a far-future TTL — should survive.
    let mut kept = make_job("ttl_kept");
    kept.queue = "qb".to_string();
    kept.result_ttl_ms = Some(3_600_000);
    let kept = storage.enqueue(kept).unwrap();

    // Capture `now` after enqueue so both jobs' scheduled_at are eligible.
    let now = now_millis();
    storage.dequeue("qa", now, None).unwrap();
    storage.dequeue("qb", now, None).unwrap();
    storage.complete(&expired.id, None, None).unwrap();
    storage.complete(&kept.id, None, None).unwrap();

    // Ensure the 1ms TTL has elapsed relative to purge's `now`.
    std::thread::sleep(std::time::Duration::from_millis(5));

    // global_cutoff = 0 so only the per-job TTL path can match.
    storage.purge_completed_with_ttl(Some(0)).unwrap();

    assert!(storage.get_job(&expired.id, None).unwrap().is_none());
    assert!(storage.get_job(&kept.id, None).unwrap().is_some());
}

#[test]
fn test_purge_completed_with_ttl_covers_non_complete_statuses() {
    // Retention bounds the whole archive, not just successes: a Dead archived
    // row (from a DLQ move) must be purged by the global cutoff too.
    let storage = test_storage();
    let now = now_millis();

    let job = storage.enqueue(make_job("dead_archived")).unwrap();
    storage.dequeue("default", now + 1000, None).unwrap();
    let running = storage.get_job(&job.id, None).unwrap().unwrap();
    storage.move_to_dlq(&running, "boom", None).unwrap();

    // The archived row is now status Dead. A future cutoff must delete it —
    // before all-status retention, the Complete-only filter left it forever.
    let removed = storage
        .purge_completed_with_ttl(Some(now + 10_000))
        .unwrap();
    assert_eq!(removed, 1, "the Dead archived row must be purged");
    assert!(storage.get_job(&job.id, None).unwrap().is_none());
}

#[test]
fn test_purge_completed_drains_across_batches() {
    // 550 completed rows exceed one PURGE_BATCH (500): the batched purge loop
    // must drain every row across iterations, not stop after the first batch.
    let storage = test_storage();
    let now = now_millis();
    for _ in 0..550 {
        storage.enqueue(make_job("purge_batch")).unwrap();
    }
    for _ in 0..550 {
        let job = storage
            .dequeue("default", now + 1000, None)
            .unwrap()
            .unwrap();
        storage.complete(&job.id, None, None).unwrap();
    }

    let removed = storage.purge_completed(now_millis() + 10_000).unwrap();
    assert_eq!(removed, 550, "batched purge must drain every completed row");
    assert!(storage.list_archived(1000, 0, None).unwrap().is_empty());
}

#[test]
fn test_cancel_pending_by_queue_drains_across_batches() {
    // 550 pending rows exceed one MASS_ARCHIVE_BATCH (500): the batched cancel
    // loop must archive every pending row across iterations.
    let storage = test_storage();
    for _ in 0..550 {
        storage.enqueue(make_job("cancel_batch")).unwrap();
    }

    let cancelled = storage.cancel_pending_by_queue("default").unwrap();
    assert_eq!(
        cancelled, 550,
        "batched cancel must drain every pending row"
    );
    assert!(storage
        .list_jobs(Some(JobStatus::Pending as i32), None, None, 1000, 0, None)
        .unwrap()
        .is_empty());
}

// ── Immediate terminal-job archival ──────────────────────────────────

/// Count rows in the live `jobs` table for a given id.
fn jobs_row_count(storage: &SqliteStorage, id: &str) -> i64 {
    use crate::storage::schema::jobs;
    use diesel::prelude::*;
    let mut conn = storage.conn().unwrap();
    jobs::table
        .filter(jobs::id.eq(id))
        .count()
        .get_result(&mut conn)
        .unwrap()
}

/// Count rows in the `archived_jobs` table for a given id.
fn archived_row_count(storage: &SqliteStorage, id: &str) -> i64 {
    use crate::storage::schema::archived_jobs;
    use diesel::prelude::*;
    let mut conn = storage.conn().unwrap();
    archived_jobs::table
        .filter(archived_jobs::id.eq(id))
        .count()
        .get_result(&mut conn)
        .unwrap()
}

#[test]
fn test_complete_moves_to_archived_immediately() {
    let storage = test_storage();
    let job = storage.enqueue(make_job("archive_complete")).unwrap();
    storage
        .dequeue("default", now_millis() + 1000, None)
        .unwrap();

    storage.complete(&job.id, Some(vec![7]), None).unwrap();

    // Gone from the live table, present in the archive.
    assert_eq!(jobs_row_count(&storage, &job.id), 0);
    assert_eq!(archived_row_count(&storage, &job.id), 1);
}

#[test]
fn test_get_job_finds_archived() {
    let storage = test_storage();
    let job = storage.enqueue(make_job("archive_get")).unwrap();
    storage
        .dequeue("default", now_millis() + 1000, None)
        .unwrap();
    storage.complete(&job.id, Some(vec![1]), None).unwrap();

    let fetched = storage.get_job(&job.id, None).unwrap().unwrap();
    assert_eq!(fetched.id, job.id);
    assert_eq!(fetched.status, JobStatus::Complete);
    assert_eq!(fetched.result, Some(vec![1]));
}

#[test]
fn test_stats_counts_archived_terminals() {
    let storage = test_storage();

    // Three jobs completed (archived), one left pending, one running.
    for i in 0..3 {
        let job = storage.enqueue(make_job(&format!("done_{i}"))).unwrap();
        storage
            .dequeue("default", now_millis() + 1000, None)
            .unwrap();
        storage.complete(&job.id, None, None).unwrap();
    }
    storage.enqueue(make_job("still_pending")).unwrap();
    let running = storage.enqueue(make_job("running")).unwrap();
    storage
        .dequeue("default", now_millis() + 1000, None)
        .unwrap();

    let stats = storage.stats(None).unwrap();
    assert_eq!(stats.completed, 3);
    assert_eq!(stats.pending, 1);
    assert_eq!(stats.running, 1);
    let _ = running;
}

#[test]
fn test_list_jobs_terminal_status_reads_archive() {
    let storage = test_storage();
    let job = storage.enqueue(make_job("listed")).unwrap();
    storage
        .dequeue("default", now_millis() + 1000, None)
        .unwrap();
    storage.complete(&job.id, None, None).unwrap();

    // Filtering by a terminal status returns the archived row.
    let complete = storage
        .list_jobs(Some(JobStatus::Complete as i32), None, None, 50, 0, None)
        .unwrap();
    assert!(complete.iter().any(|j| j.id == job.id));

    // No status filter merges live + archived.
    let all = storage.list_jobs(None, None, None, 50, 0, None).unwrap();
    assert!(all.iter().any(|j| j.id == job.id));

    // Pending filter must not surface the archived job.
    let pending = storage
        .list_jobs(Some(JobStatus::Pending as i32), None, None, 50, 0, None)
        .unwrap();
    assert!(!pending.iter().any(|j| j.id == job.id));
}

#[test]
fn test_fail_and_cancel_archive_immediately() {
    let storage = test_storage();

    // Failed running job is archived.
    let failed = storage.enqueue(make_job("to_fail")).unwrap();
    storage
        .dequeue("default", now_millis() + 1000, None)
        .unwrap();
    storage.fail(&failed.id, "boom").unwrap();
    assert_eq!(jobs_row_count(&storage, &failed.id), 0);
    assert_eq!(archived_row_count(&storage, &failed.id), 1);
    assert_eq!(
        storage.get_job(&failed.id, None).unwrap().unwrap().status,
        JobStatus::Failed
    );

    // Cancelled pending job is archived.
    let cancelled = storage.enqueue(make_job("to_cancel")).unwrap();
    assert!(storage.cancel_job(&cancelled.id, None).unwrap());
    assert_eq!(jobs_row_count(&storage, &cancelled.id), 0);
    assert_eq!(archived_row_count(&storage, &cancelled.id), 1);
    assert_eq!(
        storage
            .get_job(&cancelled.id, None)
            .unwrap()
            .unwrap()
            .status,
        JobStatus::Cancelled
    );
}

// ── Payload storage (inline on jobs) ─────────────────────────────────

#[test]
fn test_enqueue_stores_payload_inline() {
    let storage = test_storage();
    let mut nj = make_job("inline_payload");
    nj.payload = vec![9, 8, 7, 6];
    let job = storage.enqueue(nj).unwrap();

    let fetched = storage.get_job(&job.id, None).unwrap().unwrap();
    assert_eq!(fetched.payload, vec![9, 8, 7, 6]);
    assert_eq!(fetched.status, JobStatus::Pending);
}

#[test]
fn test_dequeue_returns_full_payload() {
    let storage = test_storage();
    let mut nj = make_job("dq_payload");
    nj.payload = vec![42, 43, 44];
    storage.enqueue(nj).unwrap();

    let dequeued = storage
        .dequeue("default", now_millis(), None)
        .unwrap()
        .unwrap();
    assert_eq!(dequeued.payload, vec![42, 43, 44]);
    assert_eq!(dequeued.status, JobStatus::Running);
}

#[test]
fn test_dequeue_batch_returns_full_payload() {
    let storage = test_storage();
    let mut a = make_job("batch_pa");
    a.payload = vec![1, 2, 3];
    let mut b = make_job("batch_pb");
    b.payload = vec![4, 5, 6];
    storage.enqueue(a).unwrap();
    storage.enqueue(b).unwrap();

    let claimed = storage
        .dequeue_batch("default", now_millis() + 1000, None, 10)
        .unwrap();
    assert_eq!(claimed.len(), 2);
    for j in &claimed {
        assert_eq!(j.status, JobStatus::Running);
    }
    let payloads: std::collections::HashSet<Vec<u8>> =
        claimed.iter().map(|j| j.payload.clone()).collect();
    assert!(payloads.contains(&vec![1, 2, 3]));
    assert!(payloads.contains(&vec![4, 5, 6]));
}

#[test]
fn test_dequeue_batch_archives_expired() {
    let storage = test_storage();
    let mut expired = make_job("batch_expired");
    expired.expires_at = Some(now_millis() - 1000);
    let expired_job = storage.enqueue(expired).unwrap();
    let fresh_job = storage.enqueue(make_job("batch_fresh")).unwrap();

    let claimed = storage
        .dequeue_batch("default", now_millis() + 1000, None, 10)
        .unwrap();

    // Only the fresh job is claimed; the expired one is archived, not stranded
    // in `jobs` as a terminal row.
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].id, fresh_job.id);
    assert_eq!(jobs_row_count(&storage, &expired_job.id), 0);
    let fetched = storage.get_job(&expired_job.id, None).unwrap().unwrap();
    assert_eq!(fetched.status, JobStatus::Cancelled);
}

#[test]
fn test_complete_archives_job() {
    let storage = test_storage();
    let job = storage.enqueue(make_job("complete_inline")).unwrap();
    storage.dequeue("default", now_millis(), None).unwrap();

    storage
        .complete(&job.id, Some(vec![5, 5, 5]), None)
        .unwrap();

    // Completing archives the job: the live row is gone; payload+result are
    // preserved inline in `archived_jobs`.
    assert_eq!(jobs_row_count(&storage, &job.id), 0);
    let fetched = storage.get_job(&job.id, None).unwrap().unwrap();
    assert_eq!(fetched.status, JobStatus::Complete);
    assert_eq!(fetched.result, Some(vec![5, 5, 5]));
}

#[test]
fn test_get_job_returns_payload_and_result() {
    let storage = test_storage();
    let mut nj = make_job("get_inline");
    nj.payload = vec![1, 1, 2, 3, 5];
    let job = storage.enqueue(nj).unwrap();
    storage.dequeue("default", now_millis(), None).unwrap();
    storage.complete(&job.id, Some(vec![8, 13]), None).unwrap();

    // After archival, get_job reads payload + result from `archived_jobs`.
    let fetched = storage.get_job(&job.id, None).unwrap().unwrap();
    assert_eq!(fetched.payload, vec![1, 1, 2, 3, 5]);
    assert_eq!(fetched.result, Some(vec![8, 13]));
}

#[test]
fn test_cancel_pending_archives_job() {
    let storage = test_storage();
    let job = storage.enqueue(make_job("cancel_inline")).unwrap();

    // Cancelling a pending job archives it: the live row leaves `jobs`.
    assert!(storage.cancel_job(&job.id, None).unwrap());
    assert_eq!(jobs_row_count(&storage, &job.id), 0);
    assert_eq!(
        storage.get_job(&job.id, None).unwrap().unwrap().status,
        JobStatus::Cancelled
    );
}

#[test]
fn test_new_indexes_present() {
    use diesel::prelude::*;
    use diesel::sql_types::Text;

    #[derive(QueryableByName)]
    struct IdxName {
        #[diesel(sql_type = Text)]
        name: String,
    }

    let storage = test_storage();
    let mut conn = storage.conn().unwrap();
    let names: Vec<String> =
        diesel::sql_query("SELECT name FROM sqlite_master WHERE type = 'index'")
            .load::<IdxName>(&mut conn)
            .unwrap()
            .into_iter()
            .map(|r| r.name)
            .collect();

    for expected in [
        "idx_dead_letter_failed_at",
        "idx_dead_letter_task",
        "idx_jobs_task_status",
        "idx_jobs_expires_at",
        "idx_jobs_namespace",
        "idx_archived_jobs_created_at",
        "idx_archived_jobs_namespace",
    ] {
        assert!(
            names.iter().any(|n| n == expected),
            "missing index {expected}"
        );
    }
}

// -- DLQ policies --

#[test]
fn test_delete_dead_existing() {
    let storage = test_storage();
    let job = storage.enqueue(make_job("del_dead")).unwrap();
    storage
        .dequeue("default", now_millis() + 1000, None)
        .unwrap();
    let running = storage.get_job(&job.id, None).unwrap().unwrap();
    storage.move_to_dlq(&running, "err", None).unwrap();

    let dead = storage.list_dead(10, 0, None).unwrap();
    assert_eq!(dead.len(), 1);

    assert!(storage.delete_dead(&dead[0].id, None).unwrap());
    assert!(storage.list_dead(10, 0, None).unwrap().is_empty());
}

#[test]
fn test_delete_dead_nonexistent() {
    let storage = test_storage();
    assert!(!storage.delete_dead("nope", None).unwrap());
}

#[test]
fn test_purge_dead_with_ttl_global() {
    let storage = test_storage();
    let now = now_millis();

    // Create a DLQ entry with no per-entry TTL
    let job = storage.enqueue(make_job("ttl_global")).unwrap();
    storage.dequeue("default", now + 1000, None).unwrap();
    let running = storage.get_job(&job.id, None).unwrap().unwrap();
    storage.move_to_dlq(&running, "err", None).unwrap();

    // Cutoff in the future purges it
    let purged = storage.purge_dead_with_ttl(Some(now + 5000)).unwrap();
    assert_eq!(purged, 1);
}

#[test]
fn test_purge_dead_with_ttl_per_entry() {
    let storage = test_storage();
    let now = now_millis();

    // Create a job with per-entry TTL
    let mut new_job = make_job("ttl_entry");
    new_job.result_ttl_ms = Some(1); // 1ms TTL
    let job = storage.enqueue(new_job).unwrap();
    storage.dequeue("default", now + 1000, None).unwrap();
    let running = storage.get_job(&job.id, None).unwrap().unwrap();
    storage.move_to_dlq(&running, "err", None).unwrap();

    std::thread::sleep(std::time::Duration::from_millis(5));

    // Global cutoff in the past — only per-entry TTL should purge
    let purged = storage.purge_dead_with_ttl(Some(0)).unwrap();
    assert_eq!(purged, 1);
}

#[test]
fn test_count_expired_rows_matches_purge_exactly() {
    // The dry-run count must equal what the purges actually remove. A fresh
    // in-memory store lets us assert exact equality per table (the shared
    // contract-suite store only supports before/after deltas).
    let storage = test_storage();
    let now = now_millis();
    let q = "default";

    // archived_jobs: 2 no-TTL + 1 per-entry-expired.
    for i in 0..2u8 {
        let job = storage.enqueue(make_job("dr_arch")).unwrap();
        storage.dequeue(q, now + 1000, None).unwrap();
        storage.complete(&job.id, Some(vec![i]), None).unwrap();
    }
    let mut nj = make_job("dr_arch_ttl");
    nj.result_ttl_ms = Some(1);
    let ttl_job = storage.enqueue(nj).unwrap();
    storage.dequeue(q, now + 1000, None).unwrap();
    storage.complete(&ttl_job.id, Some(vec![9]), None).unwrap();

    // dead_letter: 1 no-TTL + 1 per-entry-expired.
    let d1 = storage.enqueue(make_job("dr_dead")).unwrap();
    storage.dequeue(q, now + 1000, None).unwrap();
    let r1 = storage.get_job(&d1.id, None).unwrap().unwrap();
    storage.move_to_dlq(&r1, "boom", None).unwrap();
    let mut ndj = make_job("dr_dead_ttl");
    ndj.result_ttl_ms = Some(1);
    let d2 = storage.enqueue(ndj).unwrap();
    storage.dequeue(q, now + 1000, None).unwrap();
    let r2 = storage.get_job(&d2.id, None).unwrap().unwrap();
    storage.move_to_dlq(&r2, "boom", None).unwrap();

    // Side tables: 3 logs, 2 metrics, 1 error.
    let side = storage.enqueue(make_job("dr_side")).unwrap();
    for i in 0..3 {
        storage
            .write_task_log(&side.id, "dr_side", "info", &format!("l{i}"), None, None)
            .unwrap();
    }
    storage
        .record_metric("dr_metric", &side.id, 10, 20, true, None)
        .unwrap();
    storage
        .record_metric("dr_metric", &side.id, 11, 21, true, None)
        .unwrap();
    storage.record_error(&side.id, 0, "e0", None).unwrap();

    std::thread::sleep(std::time::Duration::from_millis(5));

    let future = now + 10_000;
    let cutoffs = crate::storage::RetentionCutoffs {
        archived_jobs: Some(future),
        dead_letter: Some(future),
        task_logs: Some(future),
        task_metrics: Some(future),
        job_errors: Some(future),
    };

    let counts = storage.count_expired_rows(&cutoffs, now_millis()).unwrap();
    // archived = 2 completed (no TTL) + 1 completed (per-entry) + 2 Dead rows
    // the two DLQ moves also archive (one no-TTL, one per-entry) = 5.
    assert_eq!(counts.archived_jobs, 5);
    assert_eq!(counts.dead_letter, 2);
    assert_eq!(counts.task_logs, 3);
    assert_eq!(counts.task_metrics, 2);
    assert_eq!(counts.job_errors, 1);
    assert_eq!(counts.total(), 13);

    // Each count equals the rows its purge then removes.
    assert_eq!(
        storage
            .purge_completed_with_ttl(cutoffs.archived_jobs)
            .unwrap(),
        counts.archived_jobs
    );
    assert_eq!(
        storage.purge_dead_with_ttl(cutoffs.dead_letter).unwrap(),
        counts.dead_letter
    );
    assert_eq!(
        storage.purge_task_logs(cutoffs.task_logs.unwrap()).unwrap(),
        counts.task_logs
    );
    assert_eq!(
        storage
            .purge_metrics(cutoffs.task_metrics.unwrap())
            .unwrap(),
        counts.task_metrics
    );
    assert_eq!(
        storage
            .purge_job_errors(cutoffs.job_errors.unwrap())
            .unwrap(),
        counts.job_errors
    );
}

#[test]
fn test_count_expired_rows_empty_store_is_zero() {
    let storage = test_storage();
    let cutoffs = crate::storage::RetentionCutoffs {
        archived_jobs: Some(now_millis() + 10_000),
        dead_letter: Some(now_millis() + 10_000),
        task_logs: Some(now_millis() + 10_000),
        task_metrics: Some(now_millis() + 10_000),
        job_errors: Some(now_millis() + 10_000),
    };
    let counts = storage.count_expired_rows(&cutoffs, now_millis()).unwrap();
    assert_eq!(counts.total(), 0, "an empty store has nothing to purge");
}

#[test]
fn test_purge_dead_drains_across_batches() {
    // 550 DLQ rows exceed one PURGE_BATCH (500): the batched loop must drain
    // every row across iterations, not stop after the first batch.
    let storage = test_storage();
    let now = now_millis();
    for _ in 0..550 {
        let job = storage.enqueue(make_job("dead_batch")).unwrap();
        storage.dequeue("default", now + 1000, None).unwrap();
        let running = storage.get_job(&job.id, None).unwrap().unwrap();
        storage.move_to_dlq(&running, "boom", None).unwrap();
    }

    let removed = storage.purge_dead(now_millis() + 10_000).unwrap();
    assert_eq!(removed, 550, "batched purge must drain every dead row");
    assert!(storage.list_dead(1000, 0, None).unwrap().is_empty());
}

#[test]
fn test_purge_dead_with_ttl_drains_across_batches() {
    let storage = test_storage();
    let now = now_millis();
    for _ in 0..550 {
        let job = storage.enqueue(make_job("dead_ttl_batch")).unwrap();
        storage.dequeue("default", now + 1000, None).unwrap();
        let running = storage.get_job(&job.id, None).unwrap().unwrap();
        storage.move_to_dlq(&running, "boom", None).unwrap();
    }

    let removed = storage
        .purge_dead_with_ttl(Some(now_millis() + 10_000))
        .unwrap();
    assert_eq!(removed, 550, "batched TTL purge must drain every dead row");
    assert!(storage.list_dead(1000, 0, None).unwrap().is_empty());
}

#[test]
fn test_list_dead_for_retry() {
    let storage = test_storage();
    let now = now_millis();

    let job = storage.enqueue(make_job("retry_cand")).unwrap();
    storage.dequeue("default", now + 1000, None).unwrap();
    let running = storage.get_job(&job.id, None).unwrap().unwrap();
    storage.move_to_dlq(&running, "err", None).unwrap();

    let qs = [String::from("default")];

    // Cutoff in the future, max_retries=3 — should find it
    let cands = storage
        .list_dead_for_retry(now + 5000, 3, None, &qs, 10)
        .unwrap();
    assert_eq!(cands.len(), 1);
    assert_eq!(cands[0].dlq_retry_count, 0);

    // max_retries=0 — should find nothing
    let cands = storage
        .list_dead_for_retry(now + 5000, 0, None, &qs, 10)
        .unwrap();
    assert!(cands.is_empty());

    // Cutoff in the past — should find nothing
    let cands = storage.list_dead_for_retry(0, 3, None, &qs, 10).unwrap();
    assert!(cands.is_empty());
}

#[test]
fn test_dlq_retry_count_round_trip() {
    let storage = test_storage();
    let now = now_millis();

    // Enqueue → dequeue → DLQ (count=0) → retry → dequeue → DLQ (count=1)
    let job = storage.enqueue(make_job("count_rt")).unwrap();
    storage.dequeue("default", now + 1000, None).unwrap();
    let running = storage.get_job(&job.id, None).unwrap().unwrap();
    storage.move_to_dlq(&running, "err1", None).unwrap();

    let dead = storage.list_dead(10, 0, None).unwrap();
    assert_eq!(dead[0].dlq_retry_count, 0);

    let new_id = storage.retry_dead(&dead[0].id, None).unwrap();
    storage.dequeue("default", now + 2000, None).unwrap();
    let running2 = storage.get_job(&new_id, None).unwrap().unwrap();
    storage.move_to_dlq(&running2, "err2", None).unwrap();

    let dead2 = storage.list_dead(10, 0, None).unwrap();
    assert_eq!(dead2[0].dlq_retry_count, 1);
}

fn make_periodic(name: &str, next_run: i64) -> crate::storage::records::NewPeriodicTask {
    crate::storage::records::NewPeriodicTask {
        name: name.to_string(),
        task_name: "periodic_task".to_string(),
        cron_expr: "* * * * *".to_string(),
        args: None,
        kwargs: None,
        queue: "default".to_string(),
        enabled: true,
        next_run,
        timezone: None,
    }
}

#[test]
fn test_periodic_pause_resume_and_delete() {
    let storage = test_storage();
    let now = now_millis();
    let past = now - 1000;

    storage
        .register_periodic(&make_periodic("alpha", past))
        .unwrap();
    storage
        .register_periodic(&make_periodic("beta", past))
        .unwrap();

    // Both registered tasks are listed.
    let listed = storage.list_periodic().unwrap();
    assert_eq!(listed.len(), 2);
    assert!(listed.iter().all(|row| row.enabled));

    // Both are due and fire.
    assert_eq!(storage.get_due_periodic(now).unwrap().len(), 2);

    // Pausing "alpha" toggles enabled off and stops it firing.
    assert!(storage.set_periodic_enabled("alpha", false).unwrap());
    let due_names: Vec<String> = storage
        .get_due_periodic(now)
        .unwrap()
        .into_iter()
        .map(|row| row.name)
        .collect();
    assert_eq!(due_names, vec!["beta".to_string()]);

    // But it is still listed (paused, not removed).
    assert_eq!(storage.list_periodic().unwrap().len(), 2);

    // Resuming brings it back into the due set.
    assert!(storage.set_periodic_enabled("alpha", true).unwrap());
    assert_eq!(storage.get_due_periodic(now).unwrap().len(), 2);

    // Deleting removes it from the listing.
    assert!(storage.delete_periodic("alpha").unwrap());
    let remaining = storage.list_periodic().unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].name, "beta");

    // A second delete reports nothing removed.
    assert!(!storage.delete_periodic("alpha").unwrap());

    // Toggling an unknown task reports nothing changed.
    assert!(!storage.set_periodic_enabled("ghost", false).unwrap());
}

// -- Topic pub/sub --

fn make_sub(
    topic: &str,
    name: &str,
    task_name: &str,
    owner: Option<&str>,
    created_at: i64,
) -> crate::storage::records::NewSubscription {
    crate::storage::records::NewSubscription {
        topic: topic.to_string(),
        subscription_name: name.to_string(),
        task_name: task_name.to_string(),
        queue: "default".to_string(),
        active: true,
        durable: owner.is_none(),
        owner_worker_id: owner.map(str::to_string),
        created_at,
        priority: None,
        max_retries: None,
        timeout_ms: None,
        mode: crate::storage::records::SubscriptionMode::Fanout,
    }
}

#[test]
fn test_register_subscription_is_idempotent_upsert() {
    let storage = test_storage();
    let now = now_millis();

    storage
        .register_subscription(&make_sub("orders", "emailer", "send_email", None, now))
        .unwrap();
    // Re-registering the same (topic, name) with a different task_name updates in
    // place rather than inserting a duplicate.
    storage
        .register_subscription(&make_sub("orders", "emailer", "send_email_v2", None, now))
        .unwrap();

    let subs = storage.list_subscriptions_for_topic("orders").unwrap();
    assert_eq!(subs.len(), 1, "upsert must not duplicate the composite key");
    assert_eq!(subs[0].task_name, "send_email_v2");
}

#[test]
fn test_list_subscriptions_for_topic_active_only_in_order() {
    let storage = test_storage();
    let now = now_millis();

    storage
        .register_subscription(&make_sub("orders", "first", "task_a", None, now))
        .unwrap();
    storage
        .register_subscription(&make_sub("orders", "second", "task_b", None, now + 1))
        .unwrap();
    storage
        .register_subscription(&make_sub("orders", "third", "task_c", None, now + 2))
        .unwrap();
    // A subscription on another topic must not leak into the topic listing.
    storage
        .register_subscription(&make_sub("shipments", "other", "task_d", None, now))
        .unwrap();

    // Pausing "second" drops it from the active listing.
    assert!(storage
        .set_subscription_active("orders", "second", false)
        .unwrap());

    let names: Vec<String> = storage
        .list_subscriptions_for_topic("orders")
        .unwrap()
        .into_iter()
        .map(|s| s.subscription_name)
        .collect();
    assert_eq!(
        names,
        vec!["first".to_string(), "third".to_string()],
        "active subscriptions only, in registration order"
    );

    // list_subscriptions returns every row across topics, active or paused.
    assert_eq!(storage.list_subscriptions().unwrap().len(), 4);
}

#[test]
fn test_unsubscribe_unknown_returns_false() {
    let storage = test_storage();
    assert!(!storage.unsubscribe("orders", "ghost").unwrap());

    storage
        .register_subscription(&make_sub(
            "orders",
            "emailer",
            "send_email",
            None,
            now_millis(),
        ))
        .unwrap();
    assert!(storage.unsubscribe("orders", "emailer").unwrap());
    assert!(storage
        .list_subscriptions_for_topic("orders")
        .unwrap()
        .is_empty());
    // Removing it a second time reports nothing removed.
    assert!(!storage.unsubscribe("orders", "emailer").unwrap());
}

#[test]
fn test_set_subscription_active_pause_resume_roundtrip() {
    let storage = test_storage();
    storage
        .register_subscription(&make_sub(
            "orders",
            "emailer",
            "send_email",
            None,
            now_millis(),
        ))
        .unwrap();

    // Pause: gone from the active listing but still registered.
    assert!(storage
        .set_subscription_active("orders", "emailer", false)
        .unwrap());
    assert!(storage
        .list_subscriptions_for_topic("orders")
        .unwrap()
        .is_empty());
    assert_eq!(storage.list_subscriptions().unwrap().len(), 1);

    // Resume: back in the active listing.
    assert!(storage
        .set_subscription_active("orders", "emailer", true)
        .unwrap());
    assert_eq!(
        storage
            .list_subscriptions_for_topic("orders")
            .unwrap()
            .len(),
        1
    );

    // Toggling an unknown subscription reports nothing changed.
    assert!(!storage
        .set_subscription_active("orders", "ghost", true)
        .unwrap());
}

#[test]
fn test_reap_ephemeral_subscriptions_spares_durable_and_live() {
    let storage = test_storage();
    // Aged past the registration grace window so the reaper may act on them;
    // a fresh row must survive even with a dead owner (startup race guard).
    let now = now_millis() - crate::storage::EPHEMERAL_SUBSCRIPTION_GRACE_MS - 1_000;

    // Durable (owner NULL) — must never be reaped.
    storage
        .register_subscription(&make_sub("orders", "durable", "task_a", None, now))
        .unwrap();
    // Ephemeral, owner alive — survives.
    storage
        .register_subscription(&make_sub("orders", "live", "task_b", Some("worker-1"), now))
        .unwrap();
    // Ephemeral, owner dead — reaped.
    storage
        .register_subscription(&make_sub("orders", "dead", "task_c", Some("worker-2"), now))
        .unwrap();

    let live = vec!["worker-1".to_string()];
    let removed = storage.reap_ephemeral_subscriptions(&live).unwrap();
    assert_eq!(removed, 1, "only the dead-owner ephemeral row is reaped");

    let remaining: Vec<String> = storage
        .list_subscriptions()
        .unwrap()
        .into_iter()
        .map(|s| s.subscription_name)
        .collect();
    assert!(remaining.contains(&"durable".to_string()));
    assert!(remaining.contains(&"live".to_string()));
    assert!(!remaining.contains(&"dead".to_string()));

    // Re-reaping with the same live set removes nothing more.
    assert_eq!(storage.reap_ephemeral_subscriptions(&live).unwrap(), 0);

    // With no live workers, every ephemeral row is reaped but the durable one stays.
    let removed = storage.reap_ephemeral_subscriptions(&[]).unwrap();
    assert_eq!(removed, 1);
    let names: Vec<String> = storage
        .list_subscriptions()
        .unwrap()
        .into_iter()
        .map(|s| s.subscription_name)
        .collect();
    assert_eq!(names, vec!["durable".to_string()]);
}

#[test]
fn test_unmigrated_open_applies_no_ddl() {
    let dir = std::env::temp_dir().join(format!("flexiq-unmigrated-{}", now_millis()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("q.db");
    let db = path.to_str().unwrap();

    let storage = SqliteStorage::unmigrated(db, 1).unwrap();
    // A gated deployment gets no tables at all, so a query fails rather than
    // silently reading an empty queue.
    assert!(
        storage.stats(None).is_err(),
        "unmigrated storage must not answer queries"
    );

    let report = storage.migrate().unwrap();
    assert!(
        !report.applied.is_empty(),
        "the first explicit migrate applies the whole history"
    );
    assert!(!report.schemaless);
    storage.stats(None).expect("migrated storage answers");

    // Re-running is a no-op, and reports as one.
    let again = storage.migrate().unwrap();
    assert!(again.applied.is_empty());
    assert!(again.is_empty(), "a current database reports no work");

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn test_migrate_reports_the_backlog_sweep() {
    let storage = test_storage();

    // A terminal job left in `jobs` is what the one-time sweep exists to move;
    // an explicit migrate must run it, not just the DDL.
    let id = storage.enqueue(make_job("swept")).unwrap().id;
    let complete = JobStatus::Complete as i32;
    diesel::sql_query(format!(
        "INSERT INTO jobs (id, queue, task_name, payload, status, priority, retry_count, \
         max_retries, created_at, scheduled_at, completed_at, timeout_ms) VALUES ('{id}-old', \
         'default', 'swept', X'01', {complete}, 0, 0, 3, 1, 1, 2, 1000)"
    ))
    .execute(&mut storage.conn().unwrap())
    .unwrap();

    let report = storage.migrate().unwrap();
    assert!(report.applied.is_empty(), "schema was already current");
    assert_eq!(
        report.archived_jobs, 1,
        "the stale terminal row is archived"
    );
    assert!(!report.is_empty(), "a sweep that moved rows is not a no-op");
}

#[test]
fn test_is_migrated_distinguishes_an_empty_database() {
    let dir = std::env::temp_dir().join(format!("flexiq-probe-{}", now_millis()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("q.db");
    let db = path.to_str().unwrap();

    // The probe is what lets a gated open tell "nothing here yet" apart from
    // "an existing deployment I must be checked against".
    let storage = SqliteStorage::unmigrated(db, 1).unwrap();
    assert!(!storage.is_migrated().unwrap());

    storage.migrate().unwrap();
    assert!(storage.is_migrated().unwrap());

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn test_dlq_shed_backfill_flags_pre_migration_rows() {
    use crate::storage::migrate::Backend;
    use crate::storage::schema::dead_letter;
    use diesel::prelude::*;

    // A row written before `0011_dlq_shed` existed carries the reserved reason
    // prefix but not the flag — `NOT NULL DEFAULT false` gives it `false`.
    // `move_to_dlq` reproduces exactly that shape.
    let storage = SqliteStorage::in_memory().unwrap();
    let shed = storage.enqueue(make_job("legacy_shed")).unwrap();
    storage
        .move_to_dlq(&shed, "codel: sojourn 900ms exceeded target", None)
        .unwrap();
    let failed = storage.enqueue(make_job("legacy_failure")).unwrap();
    storage
        .move_to_dlq(&failed, "ConnectionError: refused", None)
        .unwrap();

    let flagged = |job_id: &str| -> bool {
        let mut conn = storage.conn().unwrap();
        dead_letter::table
            .filter(dead_letter::original_job_id.eq(job_id))
            .select(dead_letter::shed)
            .first::<bool>(&mut conn)
            .unwrap()
    };
    assert!(!flagged(&shed.id), "the legacy row starts unflagged");

    let all = crate::storage::migrations::all();
    let m = all
        .iter()
        .find(|m| m.version() == "0011_dlq_shed")
        .expect("the migration is registered");
    let backfill = m
        .up(Backend::Sqlite)
        .into_iter()
        .find(|s| s.sql().starts_with("UPDATE"))
        .expect("the migration carries a backfill");
    let mut conn = storage.conn().unwrap();
    diesel::sql_query(backfill.sql())
        .execute(&mut conn)
        .unwrap();
    // Idempotent: guarded on `shed = false`, so a second pass is a no-op.
    diesel::sql_query(backfill.sql())
        .execute(&mut conn)
        .unwrap();
    drop(conn);

    assert!(flagged(&shed.id), "the reserved prefix backfills to shed");
    assert!(
        !flagged(&failed.id),
        "an ordinary failure is left alone by the backfill"
    );

    // And the point of the flag: the backfilled row stops being a candidate.
    let qs = ["default".to_string()];
    let cands = storage
        .list_dead_for_retry(now_millis() + 5000, 3, None, &qs, 50)
        .unwrap();
    assert_eq!(cands.len(), 1, "only the ordinary failure is a candidate");
    assert_eq!(cands[0].original_job_id, failed.id);
}

// ── Durable inline steps ─────────────────────────────────────────────

use crate::step::StepLimits;
use crate::storage::records::{NewJobStep, SleepOutcome, StepCommit, StepKind};

/// Enqueue, dequeue and claim a job, returning it ready for a step write.
fn claimed_job(storage: &SqliteStorage, owner: &str) -> crate::job::Job {
    let job = storage.enqueue(make_job("stepped_task")).unwrap();
    storage
        .dequeue("default", now_millis() + 1000, None)
        .unwrap();
    assert!(storage.claim_execution(&job.id, owner).unwrap());
    storage.get_job(&job.id, None).unwrap().unwrap()
}

fn run_step<'a>(job_id: &'a str, seq: i32, key: &'a str, result: &'a [u8]) -> NewJobStep<'a> {
    NewJobStep {
        job_id,
        seq,
        step_key: key,
        kind: StepKind::Run,
        result: Some(result),
    }
}

#[test]
fn steps_commit_and_replay_in_order() {
    let storage = test_storage();
    let job = claimed_job(&storage, "worker-a");
    let limits = StepLimits::default();

    for (seq, key) in [(0, "charge#0"), (1, "email#0")] {
        assert_eq!(
            storage
                .record_step_result(
                    &run_step(&job.id, seq, key, key.as_bytes()),
                    "worker-a",
                    0,
                    &limits,
                    None
                )
                .unwrap(),
            StepCommit::Committed
        );
    }

    let steps = storage.get_job_steps(&job.id, None).unwrap();
    assert_eq!(steps.len(), 2);
    assert_eq!(steps[0].step_key, "charge#0");
    assert_eq!(steps[0].result.as_deref(), Some(b"charge#0".as_slice()));
    assert_eq!(steps[1].seq, 1);
}

#[test]
fn an_identical_recommit_is_a_success() {
    let storage = test_storage();
    let job = claimed_job(&storage, "worker-a");
    let limits = StepLimits::default();
    let step = run_step(&job.id, 0, "charge#0", b"ok");

    storage
        .record_step_result(&step, "worker-a", 0, &limits, None)
        .unwrap();
    assert_eq!(
        storage
            .record_step_result(&step, "worker-a", 0, &limits, None)
            .unwrap(),
        StepCommit::AlreadyCommitted
    );
    assert_eq!(storage.get_job_steps(&job.id, None).unwrap().len(), 1);
}

#[test]
fn a_different_result_at_the_same_position_diverges() {
    let storage = test_storage();
    let job = claimed_job(&storage, "worker-a");
    let limits = StepLimits::default();

    storage
        .record_step_result(
            &run_step(&job.id, 0, "charge#0", b"first"),
            "worker-a",
            0,
            &limits,
            None,
        )
        .unwrap();
    let err = storage
        .record_step_result(
            &run_step(&job.id, 0, "charge#0", b"second"),
            "worker-a",
            0,
            &limits,
            None,
        )
        .unwrap_err();
    assert!(matches!(err, QueueError::StepDiverged { .. }), "{err}");
}

#[test]
fn a_purged_claim_on_a_running_job_re_asserts() {
    let storage = test_storage();
    let job = claimed_job(&storage, "worker-a");
    let limits = StepLimits::default();

    // Claims are swept by age, so a long job legitimately outlives its own.
    assert_eq!(
        storage.purge_execution_claims(now_millis() + 1000).unwrap(),
        1
    );
    assert_eq!(
        storage
            .record_step_result(
                &run_step(&job.id, 0, "charge#0", b"ok"),
                "worker-a",
                0,
                &limits,
                None
            )
            .unwrap(),
        StepCommit::Committed
    );
    assert_eq!(
        storage.list_claims_by_worker("worker-a").unwrap(),
        vec![job.id.clone()],
        "the sweep's victim must be re-asserted, not treated as lost"
    );
}

#[test]
fn a_write_from_a_superseded_owner_is_refused() {
    let storage = test_storage();
    let job = claimed_job(&storage, "worker-a");
    let limits = StepLimits::default();

    assert!(storage
        .reclaim_execution(&job.id, "worker-a", "worker-b")
        .unwrap());
    let err = storage
        .record_step_result(
            &run_step(&job.id, 0, "charge#0", b"ok"),
            "worker-a",
            0,
            &limits,
            None,
        )
        .unwrap_err();
    assert!(matches!(err, QueueError::ClaimLost(_)), "{err}");
}

#[test]
fn an_over_cap_step_is_refused_at_the_storage_boundary() {
    let storage = test_storage();
    let job = claimed_job(&storage, "worker-a");
    let limits = StepLimits {
        max_step_bytes: 8,
        ..StepLimits::default()
    };

    let err = storage
        .record_step_result(
            &run_step(&job.id, 0, "render#0", &[0u8; 64]),
            "worker-a",
            0,
            &limits,
            None,
        )
        .unwrap_err();
    match err {
        QueueError::StepLimitExceeded {
            limit,
            actual,
            allowed,
            ..
        } => {
            assert_eq!(limit, "step bytes");
            assert_eq!((actual, allowed), (64, 8));
        }
        other => panic!("expected a cap refusal, got {other}"),
    }
    assert!(storage.get_job_steps(&job.id, None).unwrap().is_empty());
}

#[test]
fn the_step_count_cap_holds_where_the_byte_cap_cannot() {
    let storage = test_storage();
    let job = claimed_job(&storage, "worker-a");
    let limits = StepLimits {
        max_steps: 2,
        ..StepLimits::default()
    };

    for seq in 0..2 {
        storage
            .record_step_result(
                &run_step(&job.id, seq, &format!("noop#{seq}"), &[]),
                "worker-a",
                0,
                &limits,
                None,
            )
            .unwrap();
    }
    let err = storage
        .record_step_result(
            &run_step(&job.id, 2, "noop#2", &[]),
            "worker-a",
            0,
            &limits,
            None,
        )
        .unwrap_err();
    assert!(
        matches!(&err, QueueError::StepLimitExceeded { limit, .. } if limit == "step count"),
        "{err}"
    );
}

#[test]
fn a_sleep_pins_its_deadline_on_the_first_commit() {
    let storage = test_storage();
    let job = claimed_job(&storage, "worker-a");
    let limits = StepLimits::default();
    let sleep = NewJobStep {
        job_id: &job.id,
        seq: 0,
        step_key: "cool_off#0",
        kind: StepKind::Sleep,
        result: None,
    };
    let first_deadline = now_millis() + 3_600_000;

    let outcome = storage
        .sleep_job(&sleep, "worker-a", 0, first_deadline, &limits, None)
        .unwrap();
    assert_eq!(
        outcome,
        SleepOutcome::Slept {
            wake_at: first_deadline
        }
    );

    let slept = storage.get_job(&job.id, None).unwrap().unwrap();
    assert_eq!(slept.status, JobStatus::Pending);
    assert_eq!(slept.scheduled_at, first_deadline);
    assert!(
        slept.started_at.is_none(),
        "a sleeping job is not stale-reapable"
    );
    assert!(storage
        .list_claims_by_worker("worker-a")
        .unwrap()
        .is_empty());

    // Replaying the same `sleep("1h")` must not push the deadline an hour out.
    storage
        .dequeue("default", first_deadline + 1, None)
        .unwrap();
    assert!(storage.claim_execution(&job.id, "worker-a").unwrap());
    let replayed = storage
        .sleep_job(
            &sleep,
            "worker-a",
            0,
            first_deadline + 3_600_000,
            &limits,
            None,
        )
        .unwrap();
    assert_eq!(
        replayed,
        SleepOutcome::AlreadySleeping {
            wake_at: first_deadline
        }
    );
    assert_eq!(
        storage
            .get_job(&job.id, None)
            .unwrap()
            .unwrap()
            .scheduled_at,
        first_deadline
    );
}

#[test]
fn a_run_commit_onto_a_stored_sleep_diverges() {
    let storage = test_storage();
    let job = claimed_job(&storage, "worker-a");
    let limits = StepLimits::default();
    let sleep = NewJobStep {
        job_id: &job.id,
        seq: 0,
        step_key: "cool_off#0",
        kind: StepKind::Sleep,
        result: None,
    };
    storage
        .sleep_job(&sleep, "worker-a", 0, now_millis() + 1000, &limits, None)
        .unwrap();

    storage
        .dequeue("default", now_millis() + 100_000, None)
        .unwrap();
    assert!(storage.claim_execution(&job.id, "worker-a").unwrap());
    let err = storage
        .record_step_result(
            &run_step(&job.id, 0, "cool_off#0", b"ok"),
            "worker-a",
            0,
            &limits,
            None,
        )
        .unwrap_err();
    assert!(matches!(err, QueueError::StepDiverged { .. }), "{err}");
}

#[test]
fn an_explicit_key_cannot_be_spent_twice() {
    let storage = test_storage();
    let job = claimed_job(&storage, "worker-a");
    let limits = StepLimits::default();

    storage
        .record_step_result(
            &run_step(&job.id, 0, "charge:order-7", b"ok"),
            "worker-a",
            0,
            &limits,
            None,
        )
        .unwrap();
    let err = storage
        .record_step_result(
            &run_step(&job.id, 1, "charge:order-7", b"ok"),
            "worker-a",
            0,
            &limits,
            None,
        )
        .unwrap_err();
    assert!(matches!(err, QueueError::StepDiverged { .. }), "{err}");
}

#[test]
fn a_gap_in_the_sequence_is_refused() {
    let storage = test_storage();
    let job = claimed_job(&storage, "worker-a");
    let limits = StepLimits::default();

    let err = storage
        .record_step_result(
            &run_step(&job.id, 3, "charge#0", b"ok"),
            "worker-a",
            0,
            &limits,
            None,
        )
        .unwrap_err();
    assert!(matches!(err, QueueError::StepDiverged { .. }), "{err}");
}
