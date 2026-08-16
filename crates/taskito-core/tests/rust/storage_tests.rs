//! Backend-agnostic storage integration tests.
//!
//! These tests exercise the `Storage` trait contract and can run against any
//! backend. Currently wired for SQLite (always) and Redis (behind the `redis`
//! feature flag + a running redis-server).
//!
//! Each test function uses a unique queue name to avoid cross-contamination
//! when all tests share a single storage instance.

use taskito_core::job::{now_millis, JobCompletion, JobStatus, NewJob};
use taskito_core::storage::records::{SubscriptionMode, WorkerRegistration, WorkerStatus};
use taskito_core::storage::{DeadJob, RetentionCutoffs, Storage};
use taskito_core::SqliteStorage;

fn make_job(queue: &str, task_name: &str) -> NewJob {
    NewJob {
        queue: queue.to_string(),
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

// ── Generic test functions ───────────────────────────────────────────

fn test_enqueue_and_get(s: &impl Storage) {
    let job = s.enqueue(make_job("q-enqueue", "test_task")).unwrap();
    let fetched = s.get_job(&job.id, None).unwrap().unwrap();
    assert_eq!(fetched.task_name, "test_task");
    assert_eq!(fetched.status, JobStatus::Pending);
}

fn test_dequeue(s: &impl Storage) {
    let q = "q-dequeue";
    let job = s.enqueue(make_job(q, "dequeue_task")).unwrap();
    let dequeued = s.dequeue(q, now_millis() + 1000, None).unwrap().unwrap();
    assert_eq!(dequeued.id, job.id);
    assert_eq!(dequeued.status, JobStatus::Running);

    let none = s.dequeue(q, now_millis() + 1000, None).unwrap();
    assert!(none.is_none());
}

fn test_dequeue_batch(s: &impl Storage) {
    let q = "q-dequeue-batch";
    let mut ids = Vec::new();
    for _ in 0..5 {
        ids.push(s.enqueue(make_job(q, "batch_task")).unwrap().id);
    }

    // Claim 3 of the 5 in one round-trip.
    let now = now_millis() + 1000;
    let first = s.dequeue_batch(q, now, None, 3).unwrap();
    assert_eq!(first.len(), 3);
    for job in &first {
        assert_eq!(job.status, JobStatus::Running);
    }

    // A second batch of 10 returns only the 2 remaining — and no id overlaps.
    let second = s.dequeue_batch(q, now, None, 10).unwrap();
    assert_eq!(second.len(), 2);

    let mut all: Vec<String> = first
        .iter()
        .chain(second.iter())
        .map(|j| j.id.clone())
        .collect();
    all.sort();
    all.dedup();
    assert_eq!(all.len(), 5, "batches must claim disjoint jobs");

    // Queue is now empty.
    let empty = s.dequeue_batch(q, now, None, 4).unwrap();
    assert!(empty.is_empty());

    // max == 0 claims nothing even when jobs exist.
    s.enqueue(make_job(q, "batch_task")).unwrap();
    let zero = s.dequeue_batch(q, now, None, 0).unwrap();
    assert!(zero.is_empty());
}

fn test_complete(s: &impl Storage) {
    let q = "q-complete";
    let job = s.enqueue(make_job(q, "complete_task")).unwrap();
    s.dequeue(q, now_millis() + 1000, None).unwrap();
    s.complete(&job.id, Some(vec![42]), None).unwrap();

    let fetched = s.get_job(&job.id, None).unwrap().unwrap();
    assert_eq!(fetched.status, JobStatus::Complete);
    assert_eq!(fetched.result, Some(vec![42]));
}

fn test_fail(s: &impl Storage) {
    let q = "q-fail";
    let job = s.enqueue(make_job(q, "fail_task")).unwrap();
    s.dequeue(q, now_millis() + 1000, None).unwrap();
    s.fail(&job.id, "something broke").unwrap();

    let fetched = s.get_job(&job.id, None).unwrap().unwrap();
    assert_eq!(fetched.status, JobStatus::Failed);
    assert_eq!(fetched.error.as_deref(), Some("something broke"));
}

fn test_retry(s: &impl Storage) {
    let q = "q-retry";
    let job = s.enqueue(make_job(q, "retry_task")).unwrap();
    s.dequeue(q, now_millis() + 1000, None).unwrap();

    let future = now_millis() + 5000;
    s.retry(&job.id, future, None).unwrap();

    let fetched = s.get_job(&job.id, None).unwrap().unwrap();
    assert_eq!(fetched.status, JobStatus::Pending);
    assert_eq!(fetched.retry_count, 1);
    assert_eq!(fetched.scheduled_at, future);
}

fn test_reschedule(s: &impl Storage) {
    // reschedule() must restore the job to Pending without incrementing
    // retry_count — the soft-gate parity contract across all backends.
    let q = "q-reschedule";
    let job = s.enqueue(make_job(q, "reschedule_task")).unwrap();
    s.dequeue(q, now_millis() + 1000, None).unwrap();

    let future = now_millis() + 5000;
    s.reschedule(&job.id, future).unwrap();

    let fetched = s.get_job(&job.id, None).unwrap().unwrap();
    assert_eq!(fetched.status, JobStatus::Pending);
    assert_eq!(fetched.scheduled_at, future);
    assert_eq!(
        fetched.retry_count, 0,
        "reschedule must not burn retry budget"
    );
}

fn test_cancel_job(s: &impl Storage) {
    let job = s.enqueue(make_job("q-cancel", "cancel_me")).unwrap();
    assert!(s.cancel_job(&job.id, None).unwrap());

    let fetched = s.get_job(&job.id, None).unwrap().unwrap();
    assert_eq!(fetched.status, JobStatus::Cancelled);
    assert!(!s.cancel_job(&job.id, None).unwrap());
}

fn test_stats(s: &impl Storage) {
    let q = "q-stats";
    s.enqueue(make_job(q, "t1")).unwrap();
    s.enqueue(make_job(q, "t2")).unwrap();

    let stats = s.stats(None).unwrap();
    assert!(stats.pending >= 2);
}

fn test_stats_by_queue_and_task(s: &impl Storage) {
    let q = "q-stats-breakdown";
    let task = "stats_breakdown_task";
    s.enqueue(make_job(q, task)).unwrap();
    s.enqueue(make_job(q, task)).unwrap();
    s.enqueue(make_job(q, task)).unwrap();

    // 3 pending, none running yet.
    let st = s.stats_by_queue(q, None).unwrap();
    assert_eq!(st.pending, 3);
    assert_eq!(st.running, 0);
    assert_eq!(s.count_running_by_task(task, None).unwrap(), 0);
    // Lean pending-count primitive agrees with the full breakdown.
    assert_eq!(s.count_pending_by_queue(q).unwrap(), 3);

    // Run two of them.
    let d1 = s.dequeue(q, now_millis() + 1000, None).unwrap().unwrap();
    s.dequeue(q, now_millis() + 1000, None).unwrap().unwrap();
    assert_eq!(s.count_running_by_task(task, None).unwrap(), 2);
    let st = s.stats_by_queue(q, None).unwrap();
    assert_eq!(st.running, 2);
    assert_eq!(st.pending, 1);
    assert_eq!(s.count_pending_by_queue(q).unwrap(), 1);

    // Complete one — running drops, completed rises.
    s.complete(&d1.id, None, None).unwrap();
    assert_eq!(s.count_running_by_task(task, None).unwrap(), 1);
    let st = s.stats_by_queue(q, None).unwrap();
    assert_eq!(st.pending, 1);
    assert_eq!(st.running, 1);
    assert_eq!(st.completed, 1);

    // stats_all_queues reports the same breakdown for this queue.
    let all = s.stats_all_queues(None).unwrap();
    let qs = all.get(q).expect("queue should appear in stats_all_queues");
    assert_eq!(qs.pending, 1);
    assert_eq!(qs.running, 1);
    assert_eq!(qs.completed, 1);
}

fn test_unique_key_dedup(s: &impl Storage) {
    let mut job1 = make_job("q-unique", "unique_task");
    job1.unique_key = Some("dedup-key".to_string());
    let j1 = s.enqueue_unique(job1).unwrap();

    let mut job2 = make_job("q-unique", "unique_task");
    job2.unique_key = Some("dedup-key".to_string());
    let j2 = s.enqueue_unique(job2).unwrap();

    assert_eq!(j1.id, j2.id);
}

fn test_enqueue_unique_validates_deps(s: &impl Storage) {
    // enqueue_unique must reject a missing dependency on every backend, matching
    // enqueue (Redis already validated; the Diesel backends did not).
    let mut job = make_job("q-unique-deps", "unique_dep_task");
    job.unique_key = Some("unique-dep-key".to_string());
    job.depends_on = vec!["nonexistent-dep".to_string()];
    assert!(matches!(
        s.enqueue_unique(job),
        Err(taskito_core::error::QueueError::DependencyNotFound(_))
    ));
}

fn test_enqueue_batch(s: &impl Storage) {
    let jobs: Vec<NewJob> = (0..5)
        .map(|i| {
            let mut j = make_job("q-batch", &format!("batch_task_{i}"));
            j.priority = i;
            j
        })
        .collect();

    let result = s.enqueue_batch(jobs).unwrap();
    assert_eq!(result.len(), 5);
}

fn test_dead_letter_queue(s: &impl Storage) {
    let q = "q-dlq";
    let job = s.enqueue(make_job(q, "dlq_task")).unwrap();
    s.dequeue(q, now_millis() + 1000, None).unwrap();

    let running = s.get_job(&job.id, None).unwrap().unwrap();
    s.move_to_dlq(&running, "max retries exceeded", None)
        .unwrap();

    let fetched = s.get_job(&job.id, None).unwrap().unwrap();
    assert_eq!(fetched.status, JobStatus::Dead);

    let dead = s.list_dead(10, 0, None).unwrap();
    assert!(!dead.is_empty());
}

fn test_purge_retention_covers_every_status(s: &impl Storage) {
    // Retention bounds the whole archive, not just successes: a Dead archived
    // row (from a DLQ move) must be purged by the global cutoff on every backend.
    let q = "q-retain-status";
    let job = s.enqueue(make_job(q, "retain_dead")).unwrap();
    s.dequeue(q, now_millis() + 1000, None).unwrap();
    let running = s.get_job(&job.id, None).unwrap().unwrap();
    s.move_to_dlq(&running, "boom", None).unwrap();
    assert_eq!(
        s.get_job(&job.id, None).unwrap().unwrap().status,
        JobStatus::Dead
    );

    s.purge_completed_with_ttl(Some(now_millis() + 10_000))
        .unwrap();
    assert!(
        s.get_job(&job.id, None).unwrap().is_none(),
        "a Dead archived row must be purged by retention"
    );
}

fn test_purge_retention_honors_per_entry_ttl(s: &impl Storage) {
    // A per-entry TTL expires by its own window even with no global cutoff.
    let q = "q-retain-perentry";
    let mut nj = make_job(q, "retain_ttl");
    nj.result_ttl_ms = Some(1);
    let job = s.enqueue(nj).unwrap();
    s.dequeue(q, now_millis() + 1000, None).unwrap();
    s.complete(&job.id, Some(vec![1]), None).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(5));

    // Global cutoff None → only the per-entry TTL can purge this row.
    s.purge_completed_with_ttl(None).unwrap();
    assert!(
        s.get_job(&job.id, None).unwrap().is_none(),
        "a per-entry TTL must purge without a global cutoff"
    );
}

fn test_purge_retention_keeps_job_errors(s: &impl Storage) {
    // Per-table independence: retention-purging the archived job must leave its
    // job_errors to their own window, not cascade-delete them.
    let q = "q-retain-errors";
    let job = s.enqueue(make_job(q, "retain_err")).unwrap();
    s.dequeue(q, now_millis() + 1000, None).unwrap();
    s.record_error(&job.id, 0, "boom", None).unwrap();
    s.complete(&job.id, Some(vec![1]), None).unwrap();

    s.purge_completed_with_ttl(Some(now_millis() + 10_000))
        .unwrap();
    assert!(
        s.get_job(&job.id, None).unwrap().is_none(),
        "the archived job is purged"
    );
    assert_eq!(
        s.get_job_errors(&job.id, None).unwrap().len(),
        1,
        "job_errors have no window here, so they must survive"
    );
}

/// Seed a known mix of purgeable rows and return the queue used. Callers diff
/// `count_expired_rows` before and after to isolate the delta from whatever
/// else the shared store already holds. Seeds: 2 no-TTL + 1 per-entry-expired
/// archived jobs, 1 no-TTL + 1 per-entry-expired dead entries, 3 task logs,
/// 2 metrics, 1 job error.
fn seed_purgeable_rows(s: &impl Storage, q: &str) -> String {
    // Each dequeue reads the clock itself rather than reusing the caller's `now`:
    // `make_job` stamps `scheduled_at` at enqueue time, so a caller whose `now` is
    // even a second stale (one `count_expired_rows` against a remote backend is
    // enough) would leave every job below its own scheduled time and dequeue
    // nothing, failing the `complete` that follows with `JobNotFound`.
    let due = || now_millis() + 1000;

    // archived_jobs: two with no per-entry TTL.
    for i in 0..2u8 {
        let job = s.enqueue(make_job(q, "dr_arch")).unwrap();
        s.dequeue(q, due(), None).unwrap();
        s.complete(&job.id, Some(vec![i]), None).unwrap();
    }
    // archived_jobs: one with a 1ms per-entry TTL (expires almost immediately).
    let mut nj = make_job(q, "dr_arch_ttl");
    nj.result_ttl_ms = Some(1);
    let ttl_job = s.enqueue(nj).unwrap();
    s.dequeue(q, due(), None).unwrap();
    s.complete(&ttl_job.id, Some(vec![9]), None).unwrap();

    // dead_letter: one no-TTL.
    let d1 = s.enqueue(make_job(q, "dr_dead")).unwrap();
    s.dequeue(q, due(), None).unwrap();
    let running = s.get_job(&d1.id, None).unwrap().unwrap();
    s.move_to_dlq(&running, "boom", None).unwrap();
    // dead_letter: one per-entry TTL (carried from the job).
    let mut ndj = make_job(q, "dr_dead_ttl");
    ndj.result_ttl_ms = Some(1);
    let d2 = s.enqueue(ndj).unwrap();
    s.dequeue(q, due(), None).unwrap();
    let running2 = s.get_job(&d2.id, None).unwrap().unwrap();
    s.move_to_dlq(&running2, "boom", None).unwrap();

    // Side tables: logs, metrics, one error.
    let side = s.enqueue(make_job(q, "dr_side")).unwrap();
    for i in 0..3 {
        s.write_task_log(&side.id, "dr_side", "info", &format!("l{i}"), None, None)
            .unwrap();
    }
    s.record_metric("dr_metric", &side.id, 10, 20, true, None)
        .unwrap();
    s.record_metric("dr_metric", &side.id, 11, 21, true, None)
        .unwrap();
    s.record_error(&side.id, 0, "e0", None).unwrap();

    // Let the 1ms per-entry TTLs lapse so `now` classifies them expired.
    std::thread::sleep(std::time::Duration::from_millis(5));
    side.id
}

fn test_count_expired_rows_matches_seeded_rows(s: &impl Storage) {
    // Non-destructive: a far-future cutoff makes every seeded row eligible, and
    // diffing the count before/after seeding isolates our rows from the shared
    // store. A per-entry row counted twice (global + per-entry) would break the
    // exact deltas, so this also guards the double-count boundary on every
    // backend.
    let now = now_millis();
    let cutoffs = RetentionCutoffs {
        archived_jobs: Some(now + 10_000),
        dead_letter: Some(now + 10_000),
        task_logs: Some(now + 10_000),
        task_metrics: Some(now + 10_000),
        job_errors: Some(now + 10_000),
    };

    let before = s.count_expired_rows(&cutoffs, now).unwrap();
    seed_purgeable_rows(s, "q-dryrun-delta");
    let after = s.count_expired_rows(&cutoffs, now_millis()).unwrap();

    // archived = 2 no-TTL + 1 per-entry completed, plus the 2 Dead rows the DLQ
    // moves also archive (one no-TTL, one per-entry) = 5.
    assert_eq!(after.archived_jobs - before.archived_jobs, 5);
    assert_eq!(
        after.dead_letter - before.dead_letter,
        2,
        "1 no-TTL + 1 per-entry dead"
    );
    assert_eq!(after.task_logs - before.task_logs, 3);
    assert_eq!(after.task_metrics - before.task_metrics, 2);
    assert_eq!(after.job_errors - before.job_errors, 1);
    assert_eq!(after.total() - before.total(), 13, "total sums every table");
}

fn test_count_expired_rows_none_cutoff_counts_per_entry_only(s: &impl Storage) {
    // With no window on any table, only the per-entry-TTL rows of the two
    // blob-carrying tables are counted; the side tables count nothing.
    let now = now_millis();
    let none = RetentionCutoffs::default();

    let before = s.count_expired_rows(&none, now).unwrap();
    seed_purgeable_rows(s, "q-dryrun-none");
    let after = s.count_expired_rows(&none, now_millis()).unwrap();

    // Per-entry archived rows: the completed per-entry job plus the per-entry
    // Dead row its DLQ move archived = 2. The no-TTL archived rows need a global
    // window, so they are not counted here.
    assert_eq!(
        after.archived_jobs - before.archived_jobs,
        2,
        "per-entry archived rows only"
    );
    assert_eq!(
        after.dead_letter - before.dead_letter,
        1,
        "only the per-entry dead entry"
    );
    assert_eq!(
        after.task_logs, before.task_logs,
        "no window → no logs counted"
    );
    assert_eq!(after.task_metrics, before.task_metrics);
    assert_eq!(after.job_errors, before.job_errors);
}

fn test_dead_letter_by_task(s: &impl Storage) {
    let q = "q-dlq-by-task";

    // Move 2x "task_a" and 1x "task_b" to the DLQ.
    let move_to_dlq = |task_name: &str| {
        let job = s.enqueue(make_job(q, task_name)).unwrap();
        s.dequeue(q, now_millis() + 1000, None).unwrap();
        let running = s.get_job(&job.id, None).unwrap().unwrap();
        s.move_to_dlq(&running, "boom", None).unwrap();
    };
    move_to_dlq("task_a");
    move_to_dlq("task_a");
    move_to_dlq("task_b");

    let task_a = s.list_dead_by_task("task_a", 10, 0, None).unwrap();
    assert_eq!(task_a.len(), 2);
    assert!(task_a.iter().all(|d| d.task_name == "task_a"));

    // Pagination: one entry per page.
    let page = s.list_dead_by_task("task_a", 1, 1, None).unwrap();
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].task_name, "task_a");

    // Purge removes only the matching task's entries.
    assert_eq!(s.purge_dead_by_task("task_a").unwrap(), 2);
    assert!(s
        .list_dead_by_task("task_a", 10, 0, None)
        .unwrap()
        .is_empty());

    let task_b = s.list_dead_by_task("task_b", 10, 0, None).unwrap();
    assert_eq!(task_b.len(), 1);
    assert_eq!(task_b[0].task_name, "task_b");
}

fn test_delete_dead(s: &impl Storage) {
    let q = "q-del-dead";
    let job = s.enqueue(make_job(q, "del_dead_task")).unwrap();
    s.dequeue(q, now_millis() + 1000, None).unwrap();
    let running = s.get_job(&job.id, None).unwrap().unwrap();
    s.move_to_dlq(&running, "err", None).unwrap();

    let dead = s.list_dead(100, 0, None).unwrap();
    let entry = dead
        .iter()
        .find(|d| d.original_job_id == job.id)
        .expect("our DLQ entry should exist");
    let dead_id = entry.id.clone();

    assert!(s.delete_dead(&dead_id, None).unwrap());
    assert!(!s.delete_dead(&dead_id, None).unwrap());
}

fn test_list_dead_for_retry(s: &impl Storage) {
    let q = "q-dlq-retry";
    let job = s.enqueue(make_job(q, "dlq_retry_task")).unwrap();
    s.dequeue(q, now_millis() + 1000, None).unwrap();
    let running = s.get_job(&job.id, None).unwrap().unwrap();
    s.move_to_dlq(&running, "err", None).unwrap();

    let now = now_millis();
    let qs = [q.to_string()];
    let cands = s
        .list_dead_for_retry(now + 5000, 3, None, &qs, 100)
        .unwrap();
    let ours = cands
        .iter()
        .find(|d| d.original_job_id == job.id)
        .expect("our entry should be eligible");
    assert_eq!(ours.dlq_retry_count, 0);

    // max_retries=0 should exclude everything
    let empty = s
        .list_dead_for_retry(now + 5000, 0, None, &qs, 100)
        .unwrap();
    assert!(
        empty.iter().all(|d| d.original_job_id != job.id),
        "max_retries=0 should exclude our entry"
    );

    // Scoping: a different namespace or a queue we don't serve must exclude it
    // (our entry has no namespace and lives in queue `q`).
    let other_ns = s
        .list_dead_for_retry(now + 5000, 3, Some("other-ns"), &qs, 100)
        .unwrap();
    assert!(
        other_ns.iter().all(|d| d.original_job_id != job.id),
        "a different namespace must exclude our entry"
    );
    let other_q = [String::from("q-not-served")];
    let other_queue = s
        .list_dead_for_retry(now + 5000, 3, None, &other_q, 100)
        .unwrap();
    assert!(
        other_queue.iter().all(|d| d.original_job_id != job.id),
        "an unserved queue must exclude our entry"
    );
}

fn test_progress_tracking(s: &impl Storage) {
    let job = s.enqueue(make_job("q-progress", "progress_task")).unwrap();
    s.update_progress(&job.id, 50, None).unwrap();

    let fetched = s.get_job(&job.id, None).unwrap().unwrap();
    assert_eq!(fetched.progress, Some(50));
}

fn test_record_and_get_errors(s: &impl Storage) {
    let job = s.enqueue(make_job("q-errors", "error_task")).unwrap();
    s.record_error(&job.id, 0, "first failure", None).unwrap();
    s.record_error(&job.id, 1, "second failure", None).unwrap();

    let errors = s.get_job_errors(&job.id, None).unwrap();
    assert_eq!(errors.len(), 2);
}

fn test_workers(s: &impl Storage) {
    let resources = Some(r#"["db","redis"]"#);
    let health = Some(r#"{"db":"healthy","redis":"healthy"}"#);

    s.register_worker(&WorkerRegistration {
        worker_id: "w-test-1",
        queues: "q-workers",
        resources,
        resource_health: health,
        threads: 4,
        hostname: Some("test-host"),
        pid: Some(12345),
        pool_type: Some("thread"),
        sdk: Some("rust"),
        sdk_version: Some("9.9.9"),
        ..Default::default()
    })
    .unwrap();
    s.heartbeat("w-test-1", Some(r#"{"db":"unhealthy","redis":"healthy"}"#))
        .unwrap();

    let workers = s.list_workers().unwrap();
    assert!(!workers.is_empty());
    let w = workers.iter().find(|w| w.worker_id == "w-test-1").unwrap();
    assert_eq!(w.threads, 4);
    assert!(w.resources.as_deref().unwrap().contains("db"));
    assert!(w.resource_health.as_deref().unwrap().contains("unhealthy"));
    assert_eq!(w.hostname.as_deref(), Some("test-host"));
    assert_eq!(w.pid, Some(12345));
    assert_eq!(w.pool_type.as_deref(), Some("thread"));
    assert!(w.started_at.is_some());
    // Every backend must round-trip the SDK identity, including Redis, which
    // stores workers as a hash rather than a migrated table.
    assert_eq!(w.sdk.as_deref(), Some("rust"));
    assert_eq!(w.sdk_version.as_deref(), Some("9.9.9"));

    // Test update_worker_status
    s.update_worker_status("w-test-1", WorkerStatus::Draining)
        .unwrap();
    let workers = s.list_workers().unwrap();
    let w = workers.iter().find(|w| w.worker_id == "w-test-1").unwrap();
    assert_eq!(w.status, "draining");

    // list_live_worker_ids applies the cutoff without loading the row: a fresh
    // worker is live under a past cutoff and excluded under a future one.
    let now = taskito_core::job::now_millis();
    let live = s.list_live_worker_ids(now - 10_000).unwrap();
    assert!(live.contains(&"w-test-1".to_string()));
    let none_live = s.list_live_worker_ids(now + 10_000).unwrap();
    assert!(!none_live.contains(&"w-test-1".to_string()));

    s.unregister_worker("w-test-1").unwrap();
}

fn test_pause_resume_queue(s: &impl Storage) {
    let q = "q-pause-test";
    s.pause_queue(q).unwrap();
    let paused = s.list_paused_queues().unwrap();
    assert!(paused.contains(&q.to_string()));

    s.resume_queue(q).unwrap();
    let paused = s.list_paused_queues().unwrap();
    assert!(!paused.contains(&q.to_string()));
}

fn test_execution_claims_purge(s: &impl Storage) {
    // Regression: Redis `purge_execution_claims` was a silent no-op. The
    // scheduler's maintenance loop relies on this method to reap stale claims,
    // so all backends must honor the `older_than_ms` cutoff.
    let worker = "w-purge";
    let old_job = "old-claim-job-id";
    let fresh_job = "fresh-claim-job-id";

    assert!(s.claim_execution(old_job, worker).unwrap());
    // Advance past the old claim so the cutoff below can catch it but miss
    // the fresh claim (claimed after the cutoff below is computed).
    std::thread::sleep(std::time::Duration::from_millis(20));
    let cutoff = now_millis();
    std::thread::sleep(std::time::Duration::from_millis(20));
    assert!(s.claim_execution(fresh_job, worker).unwrap());

    let purged = s.purge_execution_claims(cutoff).unwrap();
    assert!(
        purged >= 1,
        "purge must delete at least the one claim older than the cutoff"
    );

    // The old claim is gone — a fresh claim_execution for the same job succeeds.
    assert!(s.claim_execution(old_job, worker).unwrap());
    // The fresh claim must still be held.
    assert!(!s.claim_execution(fresh_job, worker).unwrap());

    s.complete_execution(old_job, None).unwrap();
    s.complete_execution(fresh_job, None).unwrap();
}

fn test_reap_stale_jobs(s: &impl Storage) {
    // A running job past its timeout is reported by reap_stale_jobs (the
    // scheduler then requeues it). Within-budget jobs are left alone.
    let q = "q-reap-stale";
    let mut nj = make_job(q, "stale_task");
    nj.timeout_ms = 1;
    let job = s.enqueue(nj).unwrap();
    let t0 = now_millis();
    s.dequeue(q, t0, None).unwrap().unwrap(); // Running, started_at = t0

    let stale = s.reap_stale_jobs(t0 + 1000, None).unwrap();
    assert!(
        stale.iter().any(|j| j.id == job.id),
        "a running job past its timeout must be reaped"
    );
    // Clean up so this Running job doesn't bleed into later shared-instance tests.
    s.complete(&job.id, None, None).unwrap();
}

fn test_reclaim_execution(s: &impl Storage) {
    // Atomic claim transfer: only the rescuer expecting the current owner wins.
    let job = "reclaim-job-id";
    assert!(s.claim_execution(job, "dead").unwrap());
    assert!(s.reclaim_execution(job, "dead", "rescuer").unwrap());
    // A second rescuer still expecting "dead" loses — owner is now "rescuer".
    assert!(!s.reclaim_execution(job, "dead", "other").unwrap());
    // The current owner can hand it on.
    assert!(s.reclaim_execution(job, "rescuer", "rescuer2").unwrap());
    // No claim row → no-op.
    assert!(!s.reclaim_execution("no-such-claim", "x", "y").unwrap());
    s.complete_execution(job, None).unwrap();

    // Owners may contain ':' (e.g. "host:pid"). The numeric timestamp suffix is
    // split off from the LAST ':', so the full owner must match — a truncated
    // prefix must not.
    let colon_job = "reclaim-colon-job";
    assert!(s.claim_execution(colon_job, "host:42").unwrap());
    assert!(
        !s.reclaim_execution(colon_job, "host", "x").unwrap(),
        "a truncated owner prefix must not match"
    );
    assert!(s
        .reclaim_execution(colon_job, "host:42", "rescuer")
        .unwrap());
    s.complete_execution(colon_job, None).unwrap();
}

fn test_claim_execution_batch(s: &impl Storage) {
    // Batch claim returns one flag per id, in order, and matches single-claim
    // semantics: an id already claimed (by any owner) comes back `false`.
    let pre = "batch-claim-pre"; // already held before the batch runs
    assert!(s.claim_execution(pre, "other").unwrap());

    let ids = ["batch-claim-a", pre, "batch-claim-c"];
    let won = s.claim_execution_batch(&ids, "batch-worker").unwrap();
    assert_eq!(won, vec![true, false, true]);

    // The won claims are now real: a follow-up single claim is rejected, and the
    // one we lost is still owned by the original holder (also rejected).
    assert!(!s.claim_execution("batch-claim-a", "batch-worker").unwrap());
    assert!(!s.claim_execution("batch-claim-c", "batch-worker").unwrap());
    assert!(!s.claim_execution(pre, "batch-worker").unwrap());

    // Empty input is a no-op, not an error.
    assert!(s
        .claim_execution_batch(&[], "batch-worker")
        .unwrap()
        .is_empty());

    for id in ["batch-claim-a", "batch-claim-c", pre] {
        s.complete_execution(id, None).unwrap();
    }
}

fn test_complete_batch(s: &impl Storage) {
    // Batch completion archives every job, clears its claim, and records a
    // success metric — the same effect as N single `complete` calls, in one txn.
    let q = "q-complete-batch";
    let task = "complete_batch_task";
    let mut ids = Vec::new();
    for _ in 0..3 {
        let job = s.enqueue(make_job(q, task)).unwrap();
        s.dequeue(q, now_millis(), None).unwrap().unwrap(); // -> Running
        assert!(s.claim_execution(&job.id, "cb-worker").unwrap());
        ids.push(job.id);
    }

    let completions: Vec<JobCompletion> = ids
        .iter()
        .map(|id| JobCompletion {
            job_id: id.clone(),
            result: Some(vec![7, 7]),
            task_name: task.to_string(),
            wall_time_ns: 42,
        })
        .collect();
    s.complete_batch(&completions, None).unwrap();

    let claims = s.list_claims_by_worker("cb-worker").unwrap();
    for id in &ids {
        let job = s.get_job(id, None).unwrap().unwrap();
        assert_eq!(job.status, JobStatus::Complete);
        assert_eq!(job.result, Some(vec![7, 7]));
        assert!(!claims.contains(id), "claim row must be cleared");
    }

    let metrics = s.get_metrics(Some(task), 0, None).unwrap();
    assert_eq!(metrics.len(), 3, "one success metric per completed job");

    // Empty input is a no-op, not an error.
    s.complete_batch(&[], None).unwrap();
}

fn test_requeue_stuck(s: &impl Storage) {
    // Operator rescue for a stuck Running job: back to Pending, claim
    // released, retry budget and cancel flag reset — all atomically.
    let q = "q-requeue-stuck";
    let job = s.enqueue(make_job(q, "stuck_task")).unwrap();
    let t0 = now_millis();
    s.dequeue(q, t0, None).unwrap().unwrap(); // Running
    assert!(s.claim_execution(&job.id, "hung-worker").unwrap());
    assert!(s.request_cancel(&job.id, None).unwrap());

    assert!(s.requeue_stuck(&job.id, t0).unwrap());

    let requeued = s.get_job(&job.id, None).unwrap().unwrap();
    assert_eq!(requeued.status, JobStatus::Pending);
    assert_eq!(
        requeued.retry_count, 0,
        "operator rescue must not consume retry budget"
    );
    assert!(requeued.started_at.is_none());
    assert!(
        !s.is_cancel_requested(&job.id, None).unwrap(),
        "a stale cancel request must not kill the fresh attempt"
    );
    // The claim was deleted, not transferred — an insert-only claim succeeds.
    assert!(s.claim_execution(&job.id, "rescuer").unwrap());
    // And the job is dequeuable again.
    let redispatched = s.dequeue(q, now_millis() + 1000, None).unwrap().unwrap();
    assert_eq!(redispatched.id, job.id);

    // Not-Running and missing jobs are a no-op `false`, never an error.
    s.complete(&job.id, None, None).unwrap();
    s.complete_execution(&job.id, None).unwrap();
    assert!(
        !s.requeue_stuck(&job.id, t0).unwrap(),
        "completed jobs are not requeueable"
    );
    assert!(!s.requeue_stuck("no-such-job", t0).unwrap());
}

fn test_reap_orphaned_jobs(s: &impl Storage) {
    // A running job whose claim owner is not in the live set is orphaned and
    // paired with that dead owner; a live owner or an empty set yields nothing.
    let q = "q-orphan-recovery";
    let job = s.enqueue(make_job(q, "orphan_task")).unwrap();
    s.dequeue(q, now_millis() + 1000, None).unwrap().unwrap();
    assert!(s.claim_execution(&job.id, "dead-worker").unwrap());

    let orphans = s
        .reap_orphaned_jobs(&["other".to_string()], now_millis(), None)
        .unwrap();
    assert!(
        orphans
            .iter()
            .any(|(j, owner)| j.id == job.id && owner == "dead-worker"),
        "claim owned by a non-live worker must be reported as orphaned"
    );

    let live = s
        .reap_orphaned_jobs(&["dead-worker".to_string()], now_millis(), None)
        .unwrap();
    assert!(
        !live.iter().any(|(j, _)| j.id == job.id),
        "a live owner's job must not be orphaned"
    );

    // Empty live set is a defensive no-op (never sweeps).
    assert!(s
        .reap_orphaned_jobs(&[], now_millis(), None)
        .unwrap()
        .is_empty());

    // Once the job leaves Running it is no longer orphaned.
    s.complete(&job.id, None, None).unwrap();
    let after = s
        .reap_orphaned_jobs(&["other".to_string()], now_millis(), None)
        .unwrap();
    assert!(!after.iter().any(|(j, _)| j.id == job.id));
    s.complete_execution(&job.id, None).unwrap();

    // Owners containing ':' must be parsed whole (split on the LAST ':'), so a
    // truncated prefix is neither reported as the owner nor matched as live.
    let cq = "q-orphan-colon";
    let cjob = s.enqueue(make_job(cq, "orphan_colon_task")).unwrap();
    s.dequeue(cq, now_millis() + 1000, None).unwrap().unwrap();
    assert!(s.claim_execution(&cjob.id, "host:7").unwrap());
    let co = s
        .reap_orphaned_jobs(&["other".to_string()], now_millis(), None)
        .unwrap();
    assert!(
        co.iter()
            .any(|(j, owner)| j.id == cjob.id && owner == "host:7"),
        "the full colon-containing owner must be reported"
    );
    let cl = s
        .reap_orphaned_jobs(&["host:7".to_string()], now_millis(), None)
        .unwrap();
    assert!(
        !cl.iter().any(|(j, _)| j.id == cjob.id),
        "the full colon-containing owner being live means not orphaned"
    );
    s.complete(&cjob.id, None, None).unwrap();
    s.complete_execution(&cjob.id, None).unwrap();
}

fn test_dashboard_settings(s: &impl Storage) {
    // get on missing key
    assert!(s.get_setting("settings-nonexistent").unwrap().is_none());

    // set then get
    s.set_setting("settings-key", "settings-value").unwrap();
    assert_eq!(
        s.get_setting("settings-key").unwrap(),
        Some("settings-value".to_string())
    );

    // overwrite
    s.set_setting("settings-key", "settings-new").unwrap();
    assert_eq!(
        s.get_setting("settings-key").unwrap(),
        Some("settings-new".to_string())
    );

    // list contains the key
    let all = s.list_settings().unwrap();
    assert_eq!(all.get("settings-key"), Some(&"settings-new".to_string()));

    // delete returns true once, false the second time
    assert!(s.delete_setting("settings-key").unwrap());
    assert!(!s.delete_setting("settings-key").unwrap());
    assert!(s.get_setting("settings-key").unwrap().is_none());
}

fn test_circuit_breakers(s: &impl Storage) {
    let task = "cb-test-task";
    let cb = s.get_circuit_breaker(task).unwrap();
    assert!(cb.is_none());

    let row = taskito_core::CircuitBreakerState {
        task_name: task.to_string(),
        state: 0, // closed
        failure_count: 0,
        last_failure_at: None,
        opened_at: None,
        half_open_at: None,
        threshold: 5,
        window_ms: 60_000,
        cooldown_ms: 30_000,
        half_open_max_probes: 5,
        half_open_success_rate: 0.8,
        half_open_probe_count: 0,
        half_open_success_count: 0,
        half_open_failure_count: 0,
    };
    s.upsert_circuit_breaker(&row).unwrap();

    let cb = s.get_circuit_breaker(task).unwrap();
    assert!(cb.is_some());
}

// ── Run all generic tests against a storage impl ─────────────────────

fn test_immediate_archival(s: &impl Storage) {
    let q = "q-archival";

    // Complete, fail, and cancel are all terminal: they archive immediately but
    // remain readable via get_job and surface in the per-queue terminal stats.
    let done = s.enqueue(make_job(q, "arch_done")).unwrap();
    s.dequeue(q, now_millis() + 1000, None).unwrap();
    s.complete(&done.id, Some(vec![9]), None).unwrap();

    let failed = s.enqueue(make_job(q, "arch_fail")).unwrap();
    s.dequeue(q, now_millis() + 1000, None).unwrap();
    s.fail(&failed.id, "boom").unwrap();

    let cancelled = s.enqueue(make_job(q, "arch_cancel")).unwrap();
    assert!(s.cancel_job(&cancelled.id, None).unwrap());

    // One running and one pending left live. Enqueue the to-be-running job
    // first so the FIFO dequeue claims it, leaving the later one pending.
    s.enqueue(make_job(q, "arch_running")).unwrap();
    s.dequeue(q, now_millis() + 1000, None).unwrap();
    let pending_job = s.enqueue(make_job(q, "arch_pending")).unwrap();

    // get_job resolves archived terminals.
    assert_eq!(
        s.get_job(&done.id, None).unwrap().unwrap().status,
        JobStatus::Complete
    );
    assert_eq!(
        s.get_job(&failed.id, None).unwrap().unwrap().status,
        JobStatus::Failed
    );
    assert_eq!(
        s.get_job(&cancelled.id, None).unwrap().unwrap().status,
        JobStatus::Cancelled
    );

    // Per-queue stats: terminals from the archive, pending/running live.
    let stats = s.stats_by_queue(q, None).unwrap();
    assert_eq!(stats.completed, 1, "completed");
    assert_eq!(stats.failed, 1, "failed");
    assert_eq!(stats.cancelled, 1, "cancelled");
    assert_eq!(stats.pending, 1, "pending");
    assert_eq!(stats.running, 1, "running");

    // Listing by a terminal status reads the archive; pending must not surface
    // the archived row.
    let complete = s
        .list_jobs(Some(JobStatus::Complete as i32), Some(q), None, 50, 0, None)
        .unwrap();
    assert!(complete.iter().any(|j| j.id == done.id));

    let pending = s
        .list_jobs(Some(JobStatus::Pending as i32), Some(q), None, 50, 0, None)
        .unwrap();
    assert!(!pending.iter().any(|j| j.id == done.id));
    assert!(pending.iter().any(|j| j.id == pending_job.id));
}

fn test_enqueue_dep_on_completed_archived_job(s: &impl Storage) {
    let q = "q-dep-archived-complete";

    // Run A to completion — it now lives in `archived_jobs`, not `jobs`.
    let a = s.enqueue(make_job(q, "dep_parent_done")).unwrap();
    s.dequeue(q, now_millis() + 1000, None).unwrap();
    s.complete(&a.id, None, None).unwrap();

    // Enqueuing B with a completed (archived) dependency must succeed: the
    // existence check has to fall back to the archive.
    let mut b_job = make_job(q, "dep_child");
    b_job.depends_on = vec![a.id.clone()];
    let b = s.enqueue(b_job).unwrap();

    // And B must be dequeuable: a completed archived parent counts as satisfied.
    let dequeued = s.dequeue(q, now_millis() + 1000, None).unwrap();
    assert_eq!(
        dequeued.map(|j| j.id),
        Some(b.id),
        "B should dequeue once its archived-complete dependency is satisfied"
    );
}

fn test_dependent_blocked_by_cancelled_parent(s: &impl Storage) {
    let q = "q-dep-cancelled-parent";

    let a = s.enqueue(make_job(q, "dep_parent_cancel")).unwrap();
    let mut b_job = make_job(q, "dep_child_blocked");
    b_job.depends_on = vec![a.id.clone()];
    let b = s.enqueue(b_job).unwrap();

    // Cancelling A archives it as Cancelled. B's dependency is now unsatisfiable.
    assert!(s.cancel_job(&a.id, None).unwrap());

    // A dequeue attempt must not return B (its archived parent is non-Complete).
    // Cascade-cancel may also have archived B; either way it must not dequeue.
    let dequeued = s.dequeue(q, now_millis() + 1000, None).unwrap();
    assert!(
        dequeued.as_ref().map(|j| &j.id) != Some(&b.id),
        "B must not dequeue while its parent is archived-cancelled"
    );
}

/// Exercise the payload/result round-trip through the full job lifecycle:
/// payload stored on enqueue, returned by dequeue, and read back by get_job
/// after the job is archived. On the Diesel backends payload/result live inline
/// on `jobs`/`archived_jobs`; Redis carries them in the Job JSON.
fn test_payload_roundtrip(s: &impl Storage) {
    let q = "q-payload-side-table";
    let mut nj = make_job(q, "payload_side_task");
    nj.payload = vec![0xDE, 0xAD, 0xBE, 0xEF];
    let job = s.enqueue(nj).unwrap();

    let dequeued = s.dequeue(q, now_millis() + 1000, None).unwrap().unwrap();
    assert_eq!(dequeued.id, job.id);
    assert_eq!(dequeued.payload, vec![0xDE, 0xAD, 0xBE, 0xEF]);

    s.complete(&job.id, Some(vec![0x01, 0x02, 0x03]), None)
        .unwrap();

    let fetched = s.get_job(&job.id, None).unwrap().unwrap();
    assert_eq!(fetched.status, JobStatus::Complete);
    assert_eq!(fetched.payload, vec![0xDE, 0xAD, 0xBE, 0xEF]);
    assert_eq!(fetched.result, Some(vec![0x01, 0x02, 0x03]));
}

/// A job run to completion is archived: its blobs move into `archived_jobs` and
/// the live `jobs` row is removed. `get_job` must still resolve the full payload
/// and result from the archive. Listing (S13) returns a blob-free narrow
/// projection: the row is present with its metadata, but `payload`/`result`
/// come back empty on every backend (fetch the full job via `get_job`).
fn test_archived_job_payload_resolves(s: &impl Storage) {
    let q = "q-archived-payload-resolves";
    let mut nj = make_job(q, "archived_payload_task");
    nj.payload = vec![0xCA, 0xFE, 0xBA, 0xBE];
    let job = s.enqueue(nj).unwrap();

    s.dequeue(q, now_millis() + 1000, None).unwrap();
    s.complete(&job.id, Some(vec![0x11, 0x22]), None).unwrap();

    // Detail lookup: the job now lives only in `archived_jobs`; the side-table
    // row is gone, yet `get_job` still resolves the full payload and result.
    let fetched = s.get_job(&job.id, None).unwrap().unwrap();
    assert_eq!(fetched.status, JobStatus::Complete);
    assert_eq!(fetched.payload, vec![0xCA, 0xFE, 0xBA, 0xBE]);
    assert_eq!(fetched.result, Some(vec![0x11, 0x22]));

    // Listing by the terminal status reads the archive but drops the blobs:
    // the row is there with its non-blob columns, payload/result are empty.
    let listed = s
        .list_jobs(Some(JobStatus::Complete as i32), Some(q), None, 50, 0, None)
        .unwrap();
    let row = listed.iter().find(|j| j.id == job.id).unwrap();
    assert_eq!(row.task_name, "archived_payload_task");
    assert_eq!(row.status, JobStatus::Complete);
    assert!(
        row.payload.is_empty(),
        "listing must not carry the arg blob"
    );
    assert!(
        row.result.is_none(),
        "listing must not carry the result blob"
    );
}

/// S13 for the live and DLQ tables: `list_jobs` on a live status and `list_dead`
/// both return blob-free rows, while `get_job` still resolves the full payload.
fn test_listing_is_blob_free(s: &impl Storage) {
    // Live path: a pending job lists without its arg blob but resolves in full.
    let q = "q-blob-free-listing";
    let mut nj = make_job(q, "blob_free_task");
    nj.payload = vec![0xAB, 0xCD, 0xEF];
    let job = s.enqueue(nj).unwrap();

    let listed = s
        .list_jobs(Some(JobStatus::Pending as i32), Some(q), None, 50, 0, None)
        .unwrap();
    let row = listed.iter().find(|j| j.id == job.id).unwrap();
    assert_eq!(row.task_name, "blob_free_task");
    assert!(
        row.payload.is_empty(),
        "live listing must drop the arg blob"
    );
    assert_eq!(
        s.get_job(&job.id, None).unwrap().unwrap().payload,
        vec![0xAB, 0xCD, 0xEF],
        "get_job must still resolve the full payload"
    );

    // DLQ path: a dead-lettered entry lists without its arg blob.
    s.dequeue(q, now_millis() + 1000, None).unwrap();
    let running = s.get_job(&job.id, None).unwrap().unwrap();
    s.move_to_dlq(&running, "boom", None).unwrap();

    let dead = s.list_dead(10, 0, None).unwrap();
    let entry = dead.iter().find(|d| d.original_job_id == job.id).unwrap();
    assert_eq!(entry.task_name, "blob_free_task");
    assert!(
        entry.payload.is_empty(),
        "DLQ listing must drop the arg blob"
    );
}

fn due_periodic_names(s: &impl Storage) -> Vec<String> {
    s.get_due_periodic(now_millis())
        .unwrap()
        .into_iter()
        .map(|p| p.name)
        .collect()
}

fn test_periodic_crud(s: &impl Storage) {
    use taskito_core::NewPeriodicTask;
    let past = now_millis() - 1_000;
    let row = |name: &'static str| NewPeriodicTask {
        name: name.to_string(),
        task_name: "periodic-task".to_string(),
        cron_expr: "* * * * *".to_string(),
        args: None,
        kwargs: None,
        queue: "default".to_string(),
        enabled: true,
        next_run: past,
        timezone: None,
    };
    s.register_periodic(&row("pc-a")).unwrap();
    s.register_periodic(&row("pc-b")).unwrap();

    // list_periodic returns every registered task.
    let listed: Vec<String> = s
        .list_periodic()
        .unwrap()
        .into_iter()
        .map(|p| p.name)
        .collect();
    assert!(listed.contains(&"pc-a".to_string()) && listed.contains(&"pc-b".to_string()));

    // Pausing drops it from the due set but keeps it in the catalog.
    assert!(s.set_periodic_enabled("pc-a", false).unwrap());
    assert!(!due_periodic_names(s).contains(&"pc-a".to_string()));
    assert!(s.list_periodic().unwrap().iter().any(|p| p.name == "pc-a"));

    // Resuming makes it due again.
    assert!(s.set_periodic_enabled("pc-a", true).unwrap());
    assert!(due_periodic_names(s).contains(&"pc-a".to_string()));

    // Toggling or deleting an unknown task reports "not found".
    assert!(!s.set_periodic_enabled("pc-missing", true).unwrap());

    assert!(s.delete_periodic("pc-a").unwrap());
    assert!(!s.list_periodic().unwrap().iter().any(|p| p.name == "pc-a"));
    assert!(!s.delete_periodic("pc-a").unwrap());
}

fn test_topic_subscriptions_crud(s: &impl Storage) {
    use taskito_core::NewSubscription;
    // Aged past the registration grace window so the reaper may act on the
    // ephemeral rows created below; freshness is covered by the grace test.
    let now = now_millis() - taskito_core::storage::EPHEMERAL_SUBSCRIPTION_GRACE_MS - 1_000;
    let sub = |topic: &'static str,
               name: &'static str,
               task_name: &'static str,
               owner: Option<&'static str>,
               created_at: i64| NewSubscription {
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
        mode: SubscriptionMode::Fanout,
    };

    // Upsert idempotency: re-registering (topic, name) updates in place.
    s.register_subscription(&sub("ts-orders", "emailer", "send_email", None, now))
        .unwrap();
    s.register_subscription(&sub("ts-orders", "emailer", "send_email_v2", None, now))
        .unwrap();
    s.register_subscription(&sub("ts-orders", "analytics", "track", None, now + 1))
        .unwrap();

    let listed = s.list_subscriptions_for_topic("ts-orders").unwrap();
    assert_eq!(
        listed.len(),
        2,
        "upsert must not duplicate the composite key"
    );
    // Registration order (created_at, then name).
    assert_eq!(
        listed
            .iter()
            .map(|r| r.subscription_name.as_str())
            .collect::<Vec<_>>(),
        vec!["emailer", "analytics"]
    );
    assert_eq!(listed[0].task_name, "send_email_v2");

    // Pausing drops from the active listing but keeps the registration.
    assert!(s
        .set_subscription_active("ts-orders", "emailer", false)
        .unwrap());
    let active_names: Vec<String> = s
        .list_subscriptions_for_topic("ts-orders")
        .unwrap()
        .into_iter()
        .map(|r| r.subscription_name)
        .collect();
    assert_eq!(active_names, vec!["analytics".to_string()]);
    assert!(s
        .list_subscriptions()
        .unwrap()
        .iter()
        .any(|r| r.topic == "ts-orders" && r.subscription_name == "emailer"));

    // Resuming brings it back.
    assert!(s
        .set_subscription_active("ts-orders", "emailer", true)
        .unwrap());
    assert_eq!(
        s.list_subscriptions_for_topic("ts-orders").unwrap().len(),
        2
    );

    // Toggling / unsubscribing an unknown row reports "not found".
    assert!(!s
        .set_subscription_active("ts-orders", "ghost", true)
        .unwrap());
    assert!(!s.unsubscribe("ts-orders", "ghost").unwrap());

    // Re-registering must not resume a paused subscription.
    assert!(s
        .set_subscription_active("ts-orders", "emailer", false)
        .unwrap());
    s.register_subscription(&sub("ts-orders", "emailer", "send_email_v3", None, now))
        .unwrap();
    assert!(
        !s.list_subscriptions()
            .unwrap()
            .iter()
            .any(|r| r.subscription_name == "emailer" && r.active),
        "re-registration must preserve the paused state"
    );
    assert!(s
        .set_subscription_active("ts-orders", "emailer", true)
        .unwrap());

    // A fresh ephemeral row (inside the grace window) survives a reap even
    // with a dead owner — startup registers subscriptions before the first
    // heartbeat lands.
    s.register_subscription(&sub(
        "ts-live",
        "fresh",
        "task_a",
        Some("ts-worker-gone"),
        now_millis(),
    ))
    .unwrap();
    assert_eq!(s.reap_ephemeral_subscriptions(&[]).unwrap(), 0);
    assert!(s.unsubscribe("ts-live", "fresh").unwrap());

    // Reaper: only dead-owner ephemeral rows go; durable rows never do.
    s.register_subscription(&sub("ts-live", "live", "task_b", Some("ts-worker-1"), now))
        .unwrap();
    s.register_subscription(&sub("ts-live", "dead", "task_c", Some("ts-worker-2"), now))
        .unwrap();
    let removed = s
        .reap_ephemeral_subscriptions(&["ts-worker-1".to_string()])
        .unwrap();
    assert_eq!(removed, 1, "only the dead-owner ephemeral row is reaped");
    let live_topic: Vec<String> = s
        .list_subscriptions_for_topic("ts-live")
        .unwrap()
        .into_iter()
        .map(|r| r.subscription_name)
        .collect();
    assert_eq!(live_topic, vec!["live".to_string()]);
    // Durable rows on ts-orders untouched by the reaper.
    assert_eq!(
        s.list_subscriptions_for_topic("ts-orders").unwrap().len(),
        2
    );

    // Unsubscribe removes the row.
    assert!(s.unsubscribe("ts-orders", "emailer").unwrap());
    assert!(s.unsubscribe("ts-orders", "analytics").unwrap());
    assert!(s
        .list_subscriptions_for_topic("ts-orders")
        .unwrap()
        .is_empty());
    assert!(s.unsubscribe("ts-live", "live").unwrap());
}

/// Two workers draining one queue concurrently must claim disjoint jobs — every
/// enqueued job is handed out exactly once, never twice. Exercises the Postgres
/// `FOR UPDATE SKIP LOCKED` dequeue path and the SQLite `BEGIN IMMEDIATE` /
/// affected-row-count guard, and the Redis Lua claim. Uses scoped threads so the
/// shared `&Storage` needs no `Arc`.
fn test_concurrent_dequeue_no_double_claim(s: &impl Storage) {
    let q = "q-concurrent-claim";
    const N: usize = 60;
    for i in 0..N {
        s.enqueue(make_job(q, &format!("cc_{i}"))).unwrap();
    }

    let claimed = std::sync::Mutex::new(Vec::<String>::new());
    let now = now_millis() + 1000;
    std::thread::scope(|scope| {
        for _ in 0..2 {
            scope.spawn(|| {
                while let Some(job) = s.dequeue(q, now, None).unwrap() {
                    claimed.lock().unwrap().push(job.id);
                }
            });
        }
    });

    let mut ids = claimed.into_inner().unwrap();
    let total = ids.len();
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), total, "a job was claimed more than once");
    assert_eq!(
        ids.len(),
        N,
        "every enqueued job must be claimed exactly once"
    );
}

fn test_topic_backlog_stats(s: &impl Storage) {
    use taskito_core::pubsub::{publish_to_topic, DeliveryDefaults, PublishRequest};
    use taskito_core::NewSubscription;

    let sub = |name: &'static str, task: &'static str| NewSubscription {
        topic: "tbs-orders".to_string(),
        subscription_name: name.to_string(),
        task_name: task.to_string(),
        queue: "default".to_string(),
        active: true,
        durable: true,
        owner_worker_id: None,
        created_at: now_millis(),
        priority: None,
        max_retries: None,
        timeout_ms: None,
        mode: SubscriptionMode::Fanout,
    };
    s.register_subscription(&sub("tbs-email", "tbs_send"))
        .unwrap();
    s.register_subscription(&sub("tbs-analytics", "tbs_track"))
        .unwrap();

    let request = |topic: &str| PublishRequest {
        topic: topic.to_string(),
        payload: vec![0x02, 0xf5],
        idempotency_key: None,
        metadata: None,
        notes: None,
        priority: None,
        scheduled_at: now_millis(),
        max_retries: None,
        timeout_ms: None,
        expires_at: None,
        result_ttl_ms: None,
        namespace: None,
        queue_defaults: DeliveryDefaults {
            priority: 0,
            max_retries: 3,
            timeout_ms: 300_000,
        },
    };
    publish_to_topic(s, &request("tbs-orders")).unwrap();
    publish_to_topic(s, &request("tbs-orders")).unwrap();

    let stats = s.topic_backlog_stats().unwrap();
    let by_name: std::collections::HashMap<_, _> = stats
        .iter()
        .filter(|st| st.topic == "tbs-orders")
        .map(|st| (st.subscription_name.as_str(), st))
        .collect();
    assert_eq!(by_name.len(), 2, "both subscriptions appear in the stats");
    assert_eq!(by_name["tbs-email"].pending, 2);
    assert_eq!(by_name["tbs-analytics"].pending, 2);
    assert_eq!(by_name["tbs-email"].running, 0);
    assert_eq!(by_name["tbs-email"].dead, 0);
    assert!(
        by_name["tbs-email"].oldest_pending_age_ms.is_some(),
        "a pending backlog yields an oldest-pending age"
    );

    // A dequeued delivery moves from pending to running.
    let claimed = s.dequeue("default", now_millis(), None).unwrap().unwrap();
    let stats = s.topic_backlog_stats().unwrap();
    let claimed_sub = stats
        .iter()
        .find(|st| st.running == 1)
        .expect("one delivery is now running");
    assert_eq!(
        claimed_sub.pending, 1,
        "its backlog dropped by the claimed one"
    );
    // The claimed job belongs to one of our subscriptions.
    assert!(claimed.task_name == "tbs_send" || claimed.task_name == "tbs_track");
}

/// A log subscription for the given topic/name (mode = "log").
fn log_sub(topic: &str, name: &str) -> taskito_core::NewSubscription {
    taskito_core::NewSubscription {
        topic: topic.to_string(),
        subscription_name: name.to_string(),
        task_name: String::new(),
        queue: "default".to_string(),
        active: true,
        durable: true,
        owner_worker_id: None,
        created_at: now_millis(),
        priority: None,
        max_retries: None,
        timeout_ms: None,
        mode: SubscriptionMode::Log,
    }
}

fn test_topic_log_messages(s: &impl Storage) {
    let topic = "tlog-msgs";
    s.register_subscription(&log_sub(topic, "reader")).unwrap();

    // Publish three messages: each is one row, ids are time-ordered.
    let m0 = s.publish_message(topic, b"m0", None, None, None).unwrap();
    let m1 = s.publish_message(topic, b"m1", None, None, None).unwrap();
    let m2 = s.publish_message(topic, b"m2", None, None, None).unwrap();
    assert!(m0.id < m1.id && m1.id < m2.id, "ids are monotonic");

    // Read from the start: all three, oldest first, payloads intact.
    let read = s.read_topic_messages(topic, "reader", 10).unwrap();
    assert_eq!(
        read.iter().map(|m| m.payload.clone()).collect::<Vec<_>>(),
        vec![b"m0".to_vec(), b"m1".to_vec(), b"m2".to_vec()]
    );

    // Ack through m1: a re-read returns only what follows (exclusive cursor).
    assert!(s.ack_topic_cursor(topic, "reader", &m1.id).unwrap());
    let after = s.read_topic_messages(topic, "reader", 10).unwrap();
    assert_eq!(after.len(), 1);
    assert_eq!(after[0].payload, b"m2".to_vec());

    // Ack is monotonic: acking an older cursor is a no-op.
    assert!(!s.ack_topic_cursor(topic, "reader", &m0.id).unwrap());
    assert_eq!(s.read_topic_messages(topic, "reader", 10).unwrap().len(), 1);

    // Lag reflects the one un-acked message; unknown subscription reads empty.
    let stats = s.topic_log_stats().unwrap();
    let mine = stats
        .iter()
        .find(|st| st.topic == topic && st.subscription_name == "reader")
        .expect("log subscription appears in stats");
    assert_eq!(mine.lag, 1);
    assert!(s
        .read_topic_messages(topic, "ghost", 10)
        .unwrap()
        .is_empty());

    // A fan-out subscription on the same topic can neither read the log nor
    // advance a cursor — the log is for log subscriptions only.
    let mut fan = log_sub(topic, "fan");
    fan.mode = SubscriptionMode::Fanout;
    fan.task_name = "deliver".to_string();
    s.register_subscription(&fan).unwrap();
    assert!(s.read_topic_messages(topic, "fan", 10).unwrap().is_empty());
    assert!(!s.ack_topic_cursor(topic, "fan", &m2.id).unwrap());
    s.unsubscribe(topic, "fan").unwrap();

    // Drop the subscription so the global purge/stats in later tests are not
    // affected by this topic's leftover cursor.
    s.unsubscribe(topic, "reader").unwrap();
}

fn test_topic_registry(s: &impl Storage) {
    // An undeclared topic has no registry row.
    assert!(s.get_topic("treg-a").unwrap().is_none());

    // Declare with a retention window; get_topic round-trips every field.
    s.declare_topic("treg-a", SubscriptionMode::Log, Some(1500))
        .unwrap();
    let a = s.get_topic("treg-a").unwrap().expect("declared topic");
    assert_eq!(a.name, "treg-a");
    assert!(a.is_log());
    assert_eq!(a.retention_ms, Some(1500));
    let created = a.created_at;

    // Re-declaring is idempotent: retention updates, created_at is preserved.
    s.declare_topic("treg-a", SubscriptionMode::Log, Some(3000))
        .unwrap();
    let a2 = s.get_topic("treg-a").unwrap().unwrap();
    assert_eq!(a2.retention_ms, Some(3000));
    assert_eq!(a2.created_at, created);

    // A topic can be declared with no retention (unbounded backlog).
    s.declare_topic("treg-b", SubscriptionMode::Log, None)
        .unwrap();
    assert_eq!(s.get_topic("treg-b").unwrap().unwrap().retention_ms, None);

    // Both declarations appear in the registry listing.
    let names: std::collections::HashSet<String> = s
        .list_declared_topics()
        .unwrap()
        .into_iter()
        .map(|t| t.name)
        .collect();
    assert!(names.contains("treg-a"));
    assert!(names.contains("treg-b"));
}

fn test_topic_log_purge(s: &impl Storage) {
    let topic = "tlog-purge";
    s.register_subscription(&log_sub(topic, "a")).unwrap();
    s.register_subscription(&log_sub(topic, "b")).unwrap();

    let m0 = s.publish_message(topic, b"m0", None, None, None).unwrap();
    let _m1 = s.publish_message(topic, b"m1", None, None, None).unwrap();
    let m2 = s.publish_message(topic, b"m2", None, None, None).unwrap();

    // Only "a" has acked (through m2); "b" has read nothing, so no message is
    // safe to drop yet — the floor is the min cursor across all log subs.
    assert!(s.ack_topic_cursor(topic, "a", &m2.id).unwrap());
    assert_eq!(s.purge_topic_messages(now_millis(), 100).unwrap(), 0);

    // Once "b" acks through m0, everything at or before m0 is fully consumed.
    assert!(s.ack_topic_cursor(topic, "b", &m0.id).unwrap());
    let removed = s.purge_topic_messages(now_millis(), 100).unwrap();
    assert_eq!(removed, 1, "only m0 is at/below the min cursor");

    // A fresh reader now sees only the surviving messages (m1, m2).
    s.register_subscription(&log_sub(topic, "fresh")).unwrap();
    let survivors = s.read_topic_messages(topic, "fresh", 10).unwrap();
    assert_eq!(
        survivors
            .iter()
            .map(|m| m.payload.clone())
            .collect::<Vec<_>>(),
        vec![b"m1".to_vec(), b"m2".to_vec()]
    );
}

fn payloads(msgs: &[taskito_core::storage::records::TopicMessage]) -> Vec<Vec<u8>> {
    msgs.iter().map(|m| m.payload.clone()).collect()
}

fn test_per_message_ack(s: &impl Storage) {
    let topic = "pm-ack";
    s.register_subscription(&log_sub(topic, "w")).unwrap();
    let now = now_millis();
    let vis = 60_000;
    let m0 = s.publish_message(topic, b"m0", None, None, None).unwrap();
    let m1 = s.publish_message(topic, b"m1", None, None, None).unwrap();
    let _m2 = s.publish_message(topic, b"m2", None, None, None).unwrap();

    // Lease 2 (m0, m1). A second lease within the window returns only m2 — the
    // in-flight ones are not re-leased.
    assert_eq!(
        payloads(&s.lease_topic_messages(topic, "w", 2, vis, now).unwrap()),
        vec![b"m0".to_vec(), b"m1".to_vec()]
    );
    assert_eq!(
        payloads(&s.lease_topic_messages(topic, "w", 10, vis, now).unwrap()),
        vec![b"m2".to_vec()]
    );

    // Ack m0 (done forever); nack m1 (available now). Acking m0 again is a no-op.
    assert!(s.ack_message(topic, "w", &m0.id).unwrap());
    assert!(s.nack_message(topic, "w", &m1.id).unwrap());
    assert!(!s.ack_message(topic, "w", &m0.id).unwrap());

    // Within the window: only the nacked m1 comes back (m0 acked, m2 in-flight).
    assert_eq!(
        payloads(&s.lease_topic_messages(topic, "w", 10, vis, now).unwrap()),
        vec![b"m1".to_vec()]
    );

    // After the visibility timeout: every un-acked lease (m1, m2) is redelivered
    // oldest-first; the acked m0 never returns.
    let later = now + vis + 1;
    let redelivered = s.lease_topic_messages(topic, "w", 10, vis, later).unwrap();
    assert_eq!(payloads(&redelivered), vec![b"m1".to_vec(), b"m2".to_vec()]);

    // Drain the topic so its acked deliveries don't get compacted by a later
    // test's (globally-scanning) purge.
    for m in &redelivered {
        assert!(s.ack_message(topic, "w", &m.id).unwrap());
    }
    s.purge_topic_messages(later, 100).unwrap();
    s.unsubscribe(topic, "w").unwrap();
}

fn test_per_message_purge(s: &impl Storage) {
    let topic = "pm-purge";
    s.register_subscription(&log_sub(topic, "w")).unwrap();
    let now = now_millis();
    let vis = 60_000;
    let m0 = s.publish_message(topic, b"m0", None, None, None).unwrap();
    let m1 = s.publish_message(topic, b"m1", None, None, None).unwrap();

    // Lease both; ack only m0. A purge compacts the message every per-message
    // subscriber acked (m0); the un-acked m1 survives.
    s.lease_topic_messages(topic, "w", 10, vis, now).unwrap();
    assert!(s.ack_message(topic, "w", &m0.id).unwrap());
    assert_eq!(s.purge_topic_messages(now, 100).unwrap(), 1);

    // m0 is gone (delivery row too); past the timeout only m1 redelivers.
    let later = now + vis + 1;
    assert_eq!(
        payloads(&s.lease_topic_messages(topic, "w", 10, vis, later).unwrap()),
        vec![b"m1".to_vec()]
    );

    // Acking m1 lets the next purge drain the topic.
    assert!(s.ack_message(topic, "w", &m1.id).unwrap());
    assert_eq!(s.purge_topic_messages(later, 100).unwrap(), 1);

    s.unsubscribe(topic, "w").unwrap();
}

fn test_enqueue_unique_batch(s: &impl Storage) {
    let q = "q-eub";
    let keyed = |uk: &str| {
        let mut j = make_job(q, "eub_task");
        j.unique_key = Some(uk.to_string());
        j
    };

    // First fan-out: three distinct keys → three fresh jobs, one transaction.
    let first = s
        .enqueue_unique_batch(vec![keyed("uk-a"), keyed("uk-b"), keyed("uk-c")])
        .unwrap();
    assert_eq!(first.len(), 3);
    assert_eq!(s.stats_by_queue(q, None).unwrap().pending, 3);

    // Replay the same keys: each active job is returned in place (dedup), and
    // no duplicate rows are created.
    let replay = s
        .enqueue_unique_batch(vec![keyed("uk-a"), keyed("uk-b"), keyed("uk-c")])
        .unwrap();
    assert_eq!(replay.len(), 3);
    for (a, b) in first.iter().zip(&replay) {
        assert_eq!(
            a.id, b.id,
            "replay must return the existing job, not a new one"
        );
    }
    assert_eq!(
        s.stats_by_queue(q, None).unwrap().pending,
        3,
        "replay must not create duplicate deliveries"
    );
}

fn test_enqueue_batch_dedup(s: &impl Storage) {
    use taskito_core::storage::enqueue_batch_dedup;
    let q = "q-ebd";
    let keyed = |uk: &str| {
        let mut j = make_job(q, "ebd_task");
        j.unique_key = Some(uk.to_string());
        j
    };

    let active = s.enqueue_unique(keyed("ebd-a")).unwrap();

    // Mixed batch: a key colliding with `active`, a fresh key repeated twice,
    // and two keyless rows. Keyed rows dedup, keyless rows always insert.
    let created = enqueue_batch_dedup(
        s,
        vec![
            keyed("ebd-a"),
            make_job(q, "ebd_task"),
            keyed("ebd-b"),
            keyed("ebd-b"),
            make_job(q, "ebd_task"),
        ],
    )
    .unwrap();

    assert_eq!(created.len(), 5, "one id per input row, in input order");
    assert_eq!(created[0].id, active.id, "collision returns the active job");
    assert_eq!(
        created[2].id, created[3].id,
        "a key repeated inside the batch dedups against its own insert"
    );
    let distinct: std::collections::HashSet<&str> =
        created.iter().map(|job| job.id.as_str()).collect();
    assert_eq!(distinct.len(), 4, "only the two keyless rows are new jobs");
    assert_eq!(
        s.stats_by_queue(q, None).unwrap().pending,
        4,
        "duplicates create no rows: active + ebd-b + two keyless"
    );

    // A batch with no unique keys still round-trips through the plain path.
    let plain = enqueue_batch_dedup(s, vec![make_job(q, "ebd_task")]).unwrap();
    assert_eq!(plain.len(), 1);
    assert_eq!(s.stats_by_queue(q, None).unwrap().pending, 5);
}

/// Every backend validates dependencies across a mixed batch, including the
/// keyless rows the raw `enqueue_batch` path inserts unchecked. Whether the
/// batch's other rows roll back is backend-specific — the Diesel backends run it
/// as one transaction, Redis loops per row — so that is asserted in the SQLite
/// unit tests rather than here.
fn test_enqueue_batch_dedup_validates_deps(s: &impl Storage) {
    use taskito_core::storage::enqueue_batch_dedup;
    let q = "q-ebd-deps";
    let mut keyed = make_job(q, "ebd_deps_task");
    keyed.unique_key = Some("ebd-deps".to_string());
    let mut doomed = make_job(q, "ebd_deps_task");
    doomed.depends_on = vec!["no-such-job".to_string()];

    let failed = enqueue_batch_dedup(s, vec![keyed, doomed]);
    assert!(failed.is_err(), "unknown dependency must reject the batch");
}

/// A `Lifo` orders map plumbs through `dequeue_batch_from` on every backend and
/// claims exactly the eligible jobs. Order is asserted per-backend in the
/// SQLite unit tests; Redis is a documented FIFO fallback, so this shared test
/// only checks the set of claimed jobs, not their order.
fn test_dispatch_order_lifo_map(s: &impl Storage) {
    use std::collections::HashMap;
    let q = "q-dispatch-order";
    let mut ids = std::collections::HashSet::new();
    for _ in 0..4 {
        ids.insert(s.enqueue(make_job(q, "ord")).unwrap().id);
    }
    let mut orders = HashMap::new();
    orders.insert(q.to_string(), taskito_core::storage::DispatchOrder::Lifo);
    let claimed = s
        .dequeue_batch_from(&[q.to_string()], now_millis() + 1000, None, 10, &orders)
        .unwrap();
    let claimed_ids: std::collections::HashSet<String> =
        claimed.into_iter().map(|j| j.id).collect();
    assert_eq!(
        claimed_ids, ids,
        "LIFO map claims exactly the eligible jobs"
    );
}

fn run_storage_tests(s: &impl Storage) {
    test_enqueue_and_get(s);
    test_dequeue(s);
    test_dequeue_batch(s);
    test_dispatch_order_lifo_map(s);
    test_complete(s);
    test_fail(s);
    test_retry(s);
    test_reschedule(s);
    test_cancel_job(s);
    test_stats(s);
    test_stats_by_queue_and_task(s);
    test_unique_key_dedup(s);
    test_enqueue_unique_validates_deps(s);
    test_enqueue_batch(s);
    test_enqueue_unique_batch(s);
    test_enqueue_batch_dedup(s);
    test_enqueue_batch_dedup_validates_deps(s);
    test_dead_letter_queue(s);
    test_dead_letter_by_task(s);
    test_purge_retention_covers_every_status(s);
    test_purge_retention_honors_per_entry_ttl(s);
    test_purge_retention_keeps_job_errors(s);
    test_count_expired_rows_matches_seeded_rows(s);
    test_count_expired_rows_none_cutoff_counts_per_entry_only(s);
    test_delete_dead(s);
    test_list_dead_for_retry(s);
    test_progress_tracking(s);
    test_record_and_get_errors(s);
    test_workers(s);
    test_pause_resume_queue(s);
    test_periodic_crud(s);
    test_topic_subscriptions_crud(s);
    test_topic_backlog_stats(s);
    test_topic_log_messages(s);
    test_topic_log_purge(s);
    test_topic_registry(s);
    test_per_message_ack(s);
    test_per_message_purge(s);
    test_circuit_breakers(s);
    test_execution_claims_purge(s);
    test_reap_stale_jobs(s);
    test_reclaim_execution(s);
    test_claim_execution_batch(s);
    test_complete_batch(s);
    test_requeue_stuck(s);
    test_reap_orphaned_jobs(s);
    test_dashboard_settings(s);
    test_immediate_archival(s);
    test_enqueue_dep_on_completed_archived_job(s);
    test_dependent_blocked_by_cancelled_parent(s);
    test_payload_roundtrip(s);
    test_archived_job_payload_resolves(s);
    test_listing_is_blob_free(s);
    test_concurrent_dequeue_no_double_claim(s);
    test_rate_limit_token_exhaustion(s);
    test_task_logs_after_cursor(s);
    test_keyset_pagination_jobs(s);
    test_keyset_pagination_dlq_and_archive(s);
    test_debounce_key_round_trip(s);
}

/// The debounce key must survive a write and both read projections on every
/// backend — the Diesel backends store it in a column, the Redis backend in the
/// job's JSON document, and only this suite runs against all three.
fn test_debounce_key_round_trip(s: &impl Storage) {
    let q = "q-debounce-key";
    let mut new_job = make_job(q, "debounce_task");
    new_job.debounce_key = Some("report:user-7".to_string());

    let job = s.enqueue(new_job).unwrap();
    assert_eq!(job.debounce_key.as_deref(), Some("report:user-7"));

    let fetched = s.get_job(&job.id, None).unwrap().unwrap();
    assert_eq!(fetched.debounce_key.as_deref(), Some("report:user-7"));

    // Listings read a blob-free projection, which is a separate column list.
    let listed = s
        .list_jobs(Some(JobStatus::Pending as i32), Some(q), None, 10, 0, None)
        .unwrap();
    let listed = listed.iter().find(|j| j.id == job.id).unwrap();
    assert_eq!(listed.debounce_key.as_deref(), Some("report:user-7"));

    // A job enqueued without one reads back as absent, never as an empty key.
    let plain = s.enqueue(make_job(q, "debounce_task")).unwrap();
    assert_eq!(plain.debounce_key, None);
    assert_eq!(
        s.get_job(&plain.id, None).unwrap().unwrap().debounce_key,
        None
    );

    test_debounce_key_absent_once_terminal(s);
}

/// A terminal job must report no debounce key on **every** backend: it has left
/// its debounce window, so a stale key would read as if one were still open.
///
/// The two backends get there differently and can drift apart silently. Diesel
/// drops it structurally — `archived_jobs` has no such column. Redis archives
/// the whole `Job` document, so it keeps the field on disk and normalizes it on
/// read. This asserts the observable behaviour both must agree on.
fn test_debounce_key_absent_once_terminal(s: &impl Storage) {
    let q = "q-debounce-terminal";
    let mut new_job = make_job(q, "debounce_terminal_task");
    new_job.debounce_key = Some("report:user-9".to_string());
    let job = s.enqueue(new_job).unwrap();

    s.dequeue(q, now_millis() + 1000, None).unwrap().unwrap();
    s.complete(&job.id, Some(vec![7]), None).unwrap();

    let fetched = s.get_job(&job.id, None).unwrap().unwrap();
    assert_eq!(fetched.status, JobStatus::Complete);
    assert_eq!(
        fetched.debounce_key, None,
        "a terminal job must not expose a debounce key"
    );

    // Terminal listings read the archive too, on a different code path.
    let listed = s
        .list_jobs(Some(JobStatus::Complete as i32), Some(q), None, 10, 0, None)
        .unwrap();
    let listed = listed.iter().find(|j| j.id == job.id).unwrap();
    assert_eq!(
        listed.debounce_key, None,
        "archived listings must not expose a debounce key"
    );
}

/// S12: keyset-paginated `list_jobs_after` must page through every row exactly
/// once, in `(created_at, id)` descending order, and stay stable when new rows
/// are inserted mid-pagination (the property offset pagination lacks).
fn test_keyset_pagination_jobs(s: &impl Storage) {
    let q = "q-keyset-jobs";
    let total = 25;
    for _ in 0..total {
        s.enqueue(make_job(q, "keyset_task")).unwrap();
    }

    let page_size = 10;
    let mut seen: Vec<String> = Vec::new();
    let mut cursor: Option<(i64, String)> = None;
    let mut inserted_extra = false;
    loop {
        let after = cursor.as_ref().map(|(k, id)| (*k, id.as_str()));
        let page = s
            .list_jobs_after(
                Some(JobStatus::Pending as i32),
                Some(q),
                None,
                page_size,
                after,
                None,
            )
            .unwrap();
        if page.is_empty() {
            break;
        }

        // Order within the page is strictly descending by (created_at, id).
        for w in page.windows(2) {
            assert!(
                (w[0].created_at, &w[0].id) > (w[1].created_at, &w[1].id),
                "page must be strictly descending by (created_at, id)"
            );
        }

        for j in &page {
            seen.push(j.id.clone());
        }
        let last = page.last().unwrap();
        cursor = Some((last.created_at, last.id.clone()));

        // Insert rows mid-pagination: keyset must not skip or duplicate the
        // rows already paged past. The new rows are newer, so they sort ahead
        // of the cursor and are correctly excluded from later pages.
        if !inserted_extra {
            for _ in 0..5 {
                s.enqueue(make_job(q, "keyset_task")).unwrap();
            }
            inserted_extra = true;
        }

        if page.len() < page_size as usize {
            break;
        }
    }

    // Every original row seen exactly once (the mid-pagination inserts are
    // newer than the cursor, so they never appear).
    assert_eq!(
        seen.len(),
        total,
        "keyset must page every original row once"
    );
    let unique: std::collections::HashSet<&String> = seen.iter().collect();
    assert_eq!(unique.len(), total, "keyset must never duplicate a row");
}

/// S12 for the DLQ and archive tables: `list_dead_after` / `list_archived_after`
/// page through every row exactly once.
fn test_keyset_pagination_dlq_and_archive(s: &impl Storage) {
    let q = "q-keyset-terminal";
    let total = 15;
    let mut dead_job_ids = Vec::new();
    for _ in 0..total {
        let job = s.enqueue(make_job(q, "keyset_terminal")).unwrap();
        s.dequeue(q, now_millis() + 1000, None).unwrap();
        let running = s.get_job(&job.id, None).unwrap().unwrap();
        s.move_to_dlq(&running, "boom", None).unwrap();
        dead_job_ids.push(job.id);
    }

    // DLQ paging. Assert against the rows this test created: a `>= total` count
    // over the whole table would let rows from earlier cases mask a skipped one.
    let dlq = page_all_dead(s, 6);
    let paged_originals: Vec<&String> = dlq.iter().map(|d| &d.original_job_id).collect();
    for job_id in &dead_job_ids {
        assert_eq!(
            paged_originals.iter().filter(|o| **o == job_id).count(),
            1,
            "keyset DLQ paging must yield every dead row exactly once"
        );
    }
    let unique: std::collections::HashSet<&String> = dlq.iter().map(|d| &d.id).collect();
    assert_eq!(unique.len(), dlq.len(), "DLQ keyset must not duplicate");

    // Archive paging: complete a fresh batch so archived rows exist.
    let qa = "q-keyset-archive";
    let mut archived_job_ids = Vec::new();
    for _ in 0..total {
        let job = s.enqueue(make_job(qa, "keyset_archive")).unwrap();
        s.dequeue(qa, now_millis() + 1000, None).unwrap();
        s.complete(&job.id, None, None).unwrap();
        archived_job_ids.push(job.id);
    }
    let arch_ids = page_all_archived(s, 6);
    for job_id in &archived_job_ids {
        assert_eq!(
            arch_ids.iter().filter(|id| *id == job_id).count(),
            1,
            "keyset archive paging must yield every archived row exactly once"
        );
    }
    let unique: std::collections::HashSet<&String> = arch_ids.iter().collect();
    assert_eq!(
        unique.len(),
        arch_ids.len(),
        "archive keyset must not duplicate"
    );
}

/// Page the whole DLQ via `list_dead_after`, returning every row seen.
fn page_all_dead(s: &impl Storage, page_size: i64) -> Vec<DeadJob> {
    let mut seen = Vec::new();
    let mut cursor: Option<(i64, String)> = None;
    loop {
        let after = cursor.as_ref().map(|(k, id)| (*k, id.as_str()));
        let page = s.list_dead_after(page_size, after, None).unwrap();
        if page.is_empty() {
            break;
        }
        let last = page.last().unwrap();
        cursor = Some((last.failed_at, last.id.clone()));
        let page_len = page.len();
        seen.extend(page);
        if page_len < page_size as usize {
            break;
        }
    }
    seen
}

/// Page the whole archive via `list_archived_after`, returning every id seen.
fn page_all_archived(s: &impl Storage, page_size: i64) -> Vec<String> {
    let mut seen = Vec::new();
    let mut cursor: Option<(i64, String)> = None;
    loop {
        let after = cursor.as_ref().map(|(k, id)| (*k, id.as_str()));
        let page = s.list_archived_after(page_size, after, None).unwrap();
        if page.is_empty() {
            break;
        }
        let last = page.last().unwrap();
        cursor = Some((last.completed_at.unwrap_or(0), last.id.clone()));
        for j in &page {
            seen.push(j.id.clone());
        }
        if page.len() < page_size as usize {
            break;
        }
    }
    seen
}

fn test_task_logs_after_cursor(s: &impl Storage) {
    let job = s.enqueue(make_job("q-logs", "log_task")).unwrap();
    for i in 0..3 {
        s.write_task_log(&job.id, "log_task", "result", &format!("m{i}"), None, None)
            .unwrap();
    }

    // No cursor → everything, in id (time) order, matching get_task_logs.
    let all = s.get_task_logs_after(&job.id, None, None).unwrap();
    assert_eq!(all.len(), 3);
    assert!(all.windows(2).all(|w| w[0].id < w[1].id));

    // A cursor at entry N yields only the entries written after it.
    let after_first = s
        .get_task_logs_after(&job.id, Some(&all[0].id), None)
        .unwrap();
    assert_eq!(
        after_first
            .iter()
            .map(|r| r.id.as_str())
            .collect::<Vec<_>>(),
        all[1..].iter().map(|r| r.id.as_str()).collect::<Vec<_>>()
    );
    let after_last = s
        .get_task_logs_after(&job.id, Some(&all[2].id), None)
        .unwrap();
    assert!(after_last.is_empty());

    // A zero limit is an empty page, even on the filtered (unindexed) path.
    let zero = s
        .query_task_logs(Some("log_task"), None, 0, 0, None)
        .unwrap();
    assert!(zero.is_empty());
}

fn test_rate_limit_token_exhaustion(s: &impl Storage) {
    // With no refill, exactly `max_tokens` acquisitions succeed and the next
    // fails. Locks the token-bucket contract on every backend (Postgres reads
    // the row FOR UPDATE so this also guards the lost-update fix).
    let key = "q-rate-exhaust";
    let max_tokens = 5.0;
    for i in 0..5 {
        assert!(
            s.try_acquire_token(key, max_tokens, 0.0).unwrap(),
            "token {i} should be granted"
        );
    }
    assert!(
        !s.try_acquire_token(key, max_tokens, 0.0).unwrap(),
        "bucket must be empty after max_tokens acquisitions"
    );
}

// ── Backend-specific wiring ──────────────────────────────────────────

#[test]
fn sqlite_storage_tests() {
    let storage = SqliteStorage::in_memory().unwrap();
    run_storage_tests(&storage);
}

#[cfg(feature = "redis")]
#[test]
fn redis_storage_tests() {
    use taskito_core::RedisStorage;

    // Use DB 15 to avoid interfering with other data.
    let url = std::env::var("TASKITO_REDIS_TEST_URL")
        .unwrap_or_else(|_| "redis://localhost:6379/15".to_string());

    let storage = match RedisStorage::new(&url) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Skipping Redis tests (cannot connect): {e}");
            return;
        }
    };

    // The contract tests use fixed queue names and assert exact counts, so they
    // need a clean DB. Flush it up front (DB 15 is the designated throwaway test
    // database per the URL default) so the suite is deterministic across repeated
    // local runs, not only against a fresh CI container.
    let mut conn = storage.conn().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut conn).unwrap();
    drop(conn);

    run_storage_tests(&storage);
    redis_mutators_reject_archived_jobs(&storage);
    redis_purge_preserves_reused_unique_key(&storage);
    redis_claim_skips_job_dropped_from_pending_set(&storage);
    redis_retry_keeps_job_dequeuable(&storage);
    redis_complete_preserves_reused_unique_key(&storage);
    redis_update_progress_never_resurrects_archived(&storage);
    redis_move_to_dlq_leaves_consistent_state(&storage);
    redis_move_to_dlq_skips_already_archived(&storage);
    redis_purge_dead_drains_across_batches(&storage);
    redis_keyset_pages_a_large_tie_bucket(&storage);
    redis_backfills_expiry_for_preupgrade_rows(&storage);
    redis_purge_metrics_drains_across_batches(&storage);
}

/// A per-entry-TTL row archived before the `archived:expiry` index existed must
/// still expire: the purge backfills the index for it. Simulated by archiving a
/// per-entry row, then stripping its expiry entry and the done marker so it
/// looks pre-upgrade.
#[cfg(feature = "redis")]
fn redis_backfills_expiry_for_preupgrade_rows(s: &taskito_core::RedisStorage) {
    let q = "q-redis-backfill";
    let mut nj = make_job(q, "backfill_ttl");
    nj.result_ttl_ms = Some(1);
    let job = s.enqueue(nj).unwrap();
    s.dequeue(q, now_millis() + 1000, None).unwrap();
    s.complete(&job.id, Some(vec![1]), None).unwrap();

    let prefix = s.prefix();
    let mut conn = s.conn().unwrap();
    // Strip the expiry index entry and the backfill marker so the row looks like
    // it predates the index.
    let _: () = redis::cmd("ZREM")
        .arg(format!("{prefix}archived:expiry"))
        .arg(&job.id)
        .query(&mut conn)
        .unwrap();
    let _: () = redis::cmd("DEL")
        .arg(format!("{prefix}archived:expiry:backfilled"))
        .arg(format!("{prefix}archived:expiry:cursor"))
        .query(&mut conn)
        .unwrap();
    drop(conn);

    std::thread::sleep(std::time::Duration::from_millis(5));

    // No global cutoff: only the backfilled expiry index can purge this row. The
    // backfill advances one ZSCAN batch per call, so drive it to completion —
    // other tests leave enough archived rows to span several batches.
    let mut purged = false;
    for _ in 0..64 {
        s.purge_completed_with_ttl(None).unwrap();
        if s.get_job(&job.id, None).unwrap().is_none() {
            purged = true;
            break;
        }
    }
    assert!(
        purged,
        "a pre-upgrade per-entry TTL row must be backfilled and purged"
    );
}

/// S12: `cancel_pending_by_queue` archives a whole batch under a single `now`,
/// so every one of these rows lands in `archived:all` with the *same* score.
/// Paging must still yield each exactly once — the tie bucket is not bounded by
/// the clock, so a page that reads it whole would degrade with the batch size.
#[cfg(feature = "redis")]
fn redis_keyset_pages_a_large_tie_bucket(s: &taskito_core::RedisStorage) {
    let q = "q-redis-tie-bucket";
    let total = 600;
    let mut created = Vec::new();
    for _ in 0..total {
        created.push(s.enqueue(make_job(q, "tie_task")).unwrap().id);
    }
    // One `now` for the whole batch → 600 archived rows sharing one score.
    assert_eq!(s.cancel_pending_by_queue(q).unwrap(), total as u64);

    let paged = page_all_archived(s, 50);
    for job_id in &created {
        assert_eq!(
            paged.iter().filter(|id| *id == job_id).count(),
            1,
            "every row of a same-score batch must be paged exactly once"
        );
    }
}

/// S15: the batched `purge_dead` must drain more than one SCAN_BATCH (500) of
/// expired entries in a single call — proving the LIMIT-window loop iterates and
/// clears the remainder, not just the first batch.
#[cfg(feature = "redis")]
fn redis_purge_dead_drains_across_batches(s: &taskito_core::RedisStorage) {
    let q = "q-redis-purge-batches";
    for _ in 0..550 {
        let job = s.enqueue(make_job(q, "purge_batch_task")).unwrap();
        s.move_to_dlq(&job, "boom", None).unwrap();
    }

    // Cutoff far in the future so every dead entry is eligible.
    let removed = s.purge_dead(now_millis() + 3_600_000).unwrap();
    assert!(
        removed >= 550,
        "batched purge_dead must remove all >500 eligible entries, got {removed}"
    );
    assert!(
        s.list_dead(10_000, 0, None).unwrap().is_empty(),
        "batched purge_dead must fully drain the DLQ"
    );
}

/// S15: `purge_metrics` was the one retention purge that read its whole
/// below-cutoff window in a single ZRANGEBYSCORE. Batching it must still drain
/// more than one SCAN_BATCH (500) per call, and must keep clearing the
/// `metrics:by_task` index for every batch — not just the first.
#[cfg(feature = "redis")]
fn redis_purge_metrics_drains_across_batches(s: &taskito_core::RedisStorage) {
    let task = "purge_metrics_batch_task";
    for i in 0..550 {
        s.record_metric(task, &format!("job-{i}"), 10, 20, true, None)
            .unwrap();
    }

    // Cutoff far in the future so every recorded metric is eligible.
    let removed = s.purge_metrics(now_millis() + 3_600_000).unwrap();
    assert!(
        removed >= 550,
        "batched purge_metrics must remove all >500 eligible rows, got {removed}"
    );
    assert!(
        s.get_metrics(None, 0, None).unwrap().is_empty(),
        "batched purge_metrics must fully drain the metric store"
    );

    // The blobs are gone either way; the by_task index is only cleaned from the
    // row loaded per batch, so an unbatched second page would leave it populated.
    let mut conn = s.conn().unwrap();
    let remaining: i64 = redis::cmd("ZCARD")
        .arg(rkey(s, &["metrics", "by_task", task]))
        .query(&mut conn)
        .unwrap();
    assert_eq!(
        remaining, 0,
        "every batch must clear its by_task index entries"
    );
}

/// Build a raw key under the storage's prefix, matching `RedisStorage::key`.
#[cfg(feature = "redis")]
fn rkey(s: &taskito_core::RedisStorage, parts: &[&str]) -> String {
    format!("{}{}", s.prefix(), parts.join(":"))
}

/// Drain any pending jobs left in `q` by earlier runs so the test that follows
/// deterministically dequeues the job it just enqueued (the shared test DB is
/// not flushed between runs).
#[cfg(feature = "redis")]
fn drain_queue(s: &taskito_core::RedisStorage, q: &str) {
    while s
        .dequeue(q, now_millis() + 1_000_000, None)
        .unwrap()
        .is_some()
    {}
}

/// The atomic claim must refuse a candidate that a concurrent cancel/expire
/// already removed from the pending status set, rather than resurrecting it as a
/// Running orphan. Simulated by dropping the job from `jobs:status:0` while it
/// lingers in the pending zset.
#[cfg(feature = "redis")]
fn redis_claim_skips_job_dropped_from_pending_set(s: &taskito_core::RedisStorage) {
    use redis::Commands;
    let q = "q-redis-claim-guard";
    drain_queue(s, q);
    let job = s.enqueue(make_job(q, "claim_guard")).unwrap();

    let mut conn = s.conn().unwrap();
    let status_pending = rkey(s, &["jobs", "status", "0"]);
    let _: () = conn.srem(&status_pending, &job.id).unwrap();

    // No claimable candidate remains, and the job is not flipped to Running.
    assert!(s.dequeue(q, now_millis() + 1000, None).unwrap().is_none());
    let fetched = s.get_job(&job.id, None).unwrap().unwrap();
    assert_eq!(
        fetched.status,
        JobStatus::Pending,
        "claim guard must not resurrect a job dropped from the pending set"
    );
}

/// Retry must leave the job dequeuable — the status-set move and the pending-zset
/// add commit together, so the job is never stranded Pending but absent from the
/// queue.
#[cfg(feature = "redis")]
fn redis_retry_keeps_job_dequeuable(s: &taskito_core::RedisStorage) {
    let q = "q-redis-retry-requeue";
    drain_queue(s, q);
    let job = s.enqueue(make_job(q, "retry_requeue")).unwrap();
    s.dequeue(q, now_millis() + 1000, None).unwrap();

    s.retry(&job.id, now_millis(), None).unwrap();

    let again = s.dequeue(q, now_millis() + 1000, None).unwrap();
    assert_eq!(
        again.map(|j| j.id),
        Some(job.id.clone()),
        "retried job must be back in the pending zset and dequeuable"
    );
}

/// Completing a job must not clobber a `jobs:unique` pointer a different live job
/// has reused — the release is a compare-and-delete. Simulated by repointing the
/// pointer before `complete`.
#[cfg(feature = "redis")]
fn redis_complete_preserves_reused_unique_key(s: &taskito_core::RedisStorage) {
    use redis::Commands;
    let q = "q-redis-complete-unique";
    let shared = "redis-complete-reuse";
    drain_queue(s, q);

    let mut a = make_job(q, "complete_unique_a");
    a.unique_key = Some(shared.to_string());
    let a = s.enqueue_unique(a).unwrap();
    s.dequeue(q, now_millis() + 1000, None).unwrap();

    let mut conn = s.conn().unwrap();
    let ukey = rkey(s, &["jobs", "unique", shared]);
    let _: () = conn.set(&ukey, "other-live-job-id").unwrap();

    s.complete(&a.id, None, None).unwrap();

    let owner: Option<String> = conn.get(&ukey).unwrap();
    assert_eq!(
        owner.as_deref(),
        Some("other-live-job-id"),
        "complete must not delete a unique key reused by another job"
    );
    let _: () = conn.del(&ukey).unwrap();
}

/// A progress update must never recreate `job:<id>` once the job has been
/// archived. The Lua existence gate (and the live-only required lookup) keep a
/// stale update from leaving an orphan key outside every index.
#[cfg(feature = "redis")]
fn redis_update_progress_never_resurrects_archived(s: &taskito_core::RedisStorage) {
    use redis::Commands;
    let q = "q-redis-progress-guard";
    drain_queue(s, q);
    let job = s.enqueue(make_job(q, "progress_guard")).unwrap();
    s.dequeue(q, now_millis() + 1000, None).unwrap();

    // Live update goes through the guard and writes.
    s.update_progress(&job.id, 42, None).unwrap();
    assert_eq!(
        s.get_job(&job.id, None).unwrap().unwrap().progress,
        Some(42)
    );

    // After archival the job key is gone; a stale update must not resurrect it.
    s.complete(&job.id, None, None).unwrap();
    assert!(matches!(
        s.update_progress(&job.id, 99, None),
        Err(taskito_core::error::QueueError::JobNotFound(_))
    ));
    let mut conn = s.conn().unwrap();
    let jkey = rkey(s, &["job", &job.id]);
    let exists: bool = conn.exists(&jkey).unwrap();
    assert!(
        !exists,
        "archived job key must not be resurrected by a progress update"
    );
}

/// The DLQ write and the live→archive move commit in one atomic pipeline, so a
/// dead-lettered job is fully out of every live index and present in the DLQ —
/// never a half state.
#[cfg(feature = "redis")]
fn redis_move_to_dlq_leaves_consistent_state(s: &taskito_core::RedisStorage) {
    use redis::Commands;
    let q = "q-redis-dlq-atomic";
    drain_queue(s, q);
    let job = s.enqueue(make_job(q, "dlq_atomic")).unwrap();
    s.dequeue(q, now_millis() + 1000, None).unwrap();
    let running = s.get_job(&job.id, None).unwrap().unwrap();

    s.move_to_dlq(&running, "boom", None).unwrap();

    let dead = s.list_dead(10, 0, None).unwrap();
    assert!(
        dead.iter().any(|d| d.original_job_id == job.id),
        "job must be present in the DLQ"
    );

    let mut conn = s.conn().unwrap();
    for set in [
        rkey(s, &["jobs", "status", "1"]),
        rkey(s, &["jobs", "by_queue", q]),
    ] {
        let member: bool = conn.sismember(&set, &job.id).unwrap();
        assert!(!member, "dead job must be removed from live index {set}");
    }
    let all = rkey(s, &["jobs", "all"]);
    let score: Option<f64> = conn.zscore(&all, &job.id).unwrap();
    assert!(score.is_none(), "dead job must be removed from jobs:all");
}

/// A stale caller that lost a race to `complete`/`fail`/the reaper must not
/// dead-letter a job that was already archived — no duplicate DLQ entry, and the
/// terminal archive is left intact.
#[cfg(feature = "redis")]
fn redis_move_to_dlq_skips_already_archived(s: &taskito_core::RedisStorage) {
    let q = "q-redis-dlq-guard";
    drain_queue(s, q);
    let job = s.enqueue(make_job(q, "dlq_guard")).unwrap();
    s.dequeue(q, now_millis() + 1000, None).unwrap();
    let running = s.get_job(&job.id, None).unwrap().unwrap();

    // A racer archives the job first (Complete).
    s.complete(&job.id, None, None).unwrap();
    let before = s.list_dead(1000, 0, None).unwrap().len();

    // The stale move_to_dlq must be a no-op.
    s.move_to_dlq(&running, "boom", None).unwrap();

    assert_eq!(
        s.list_dead(1000, 0, None).unwrap().len(),
        before,
        "move_to_dlq must not dead-letter an already-archived job"
    );
    assert_eq!(
        s.get_job(&job.id, None).unwrap().unwrap().status,
        JobStatus::Complete,
        "terminal archive must not be overwritten to Dead"
    );
}

/// A terminal job has left the live indices, so a mutator that resolves the
/// live row (`get_job_required`) must return `JobNotFound` rather than partially
/// reindexing an archived row.
#[cfg(feature = "redis")]
fn redis_mutators_reject_archived_jobs(s: &taskito_core::RedisStorage) {
    let q = "q-redis-mutate-archived";

    // Cancel a pending job → archived as Cancelled.
    let cancelled = s.enqueue(make_job(q, "redis_archived_cancel")).unwrap();
    assert!(s.cancel_job(&cancelled.id, None).unwrap());
    assert!(matches!(
        s.retry(&cancelled.id, now_millis(), None),
        Err(taskito_core::error::QueueError::JobNotFound(_))
    ));
    assert!(matches!(
        s.mark_cancelled(&cancelled.id, None),
        Err(taskito_core::error::QueueError::JobNotFound(_))
    ));

    // Complete a job → archived as Complete; the same guard applies.
    let done = s.enqueue(make_job(q, "redis_archived_done")).unwrap();
    s.dequeue(q, now_millis() + 1000, None).unwrap();
    s.complete(&done.id, None, None).unwrap();
    assert!(matches!(
        s.retry(&done.id, now_millis(), None),
        Err(taskito_core::error::QueueError::JobNotFound(_))
    ));
}

/// Purging an archived job must not delete a `jobs:unique` pointer now owned by
/// a different live job that reused the same `unique_key`.
#[cfg(feature = "redis")]
fn redis_purge_preserves_reused_unique_key(s: &taskito_core::RedisStorage) {
    let q = "q-redis-unique-reuse";
    let shared_key = "redis-reused-unique";

    // Run A to completion under the shared unique key.
    let mut a_job = make_job(q, "unique_reuse_a");
    a_job.unique_key = Some(shared_key.to_string());
    let a = s.enqueue_unique(a_job).unwrap();
    s.dequeue(q, now_millis() + 1000, None).unwrap();
    s.complete(&a.id, None, None).unwrap();

    // A new live job B reuses the freed unique key and owns the lock.
    let mut b_job = make_job(q, "unique_reuse_b");
    b_job.unique_key = Some(shared_key.to_string());
    let b = s.enqueue_unique(b_job).unwrap();
    assert_ne!(
        a.id, b.id,
        "B should be a distinct live job, not deduped to A"
    );

    // Purge A's archived row — must leave B's unique lock intact.
    s.purge_completed(now_millis() + 1000).unwrap();

    // Re-enqueuing under the same key must still dedup to B, proving the lock
    // survived the purge.
    let mut c_job = make_job(q, "unique_reuse_c");
    c_job.unique_key = Some(shared_key.to_string());
    let c = s.enqueue_unique(c_job).unwrap();
    assert_eq!(
        c.id, b.id,
        "unique lock for B must survive purging archived A"
    );
}

#[cfg(feature = "postgres")]
#[test]
fn postgres_storage_tests() {
    use diesel::connection::SimpleConnection;
    use diesel::{Connection, PgConnection};
    use taskito_core::PostgresStorage;

    let url = match std::env::var("TASKITO_POSTGRES_TEST_URL") {
        Ok(u) => u,
        Err(_) => {
            eprintln!("Skipping Postgres tests (TASKITO_POSTGRES_TEST_URL not set)");
            return;
        }
    };

    // This raw connection reaches libpq before `PostgresStorage` can, so claim
    // OpenSSL's initialization here too — see `init_openssl_without_atexit`.
    openssl_sys::init();

    // Reset the `taskito` schema so the count-exact contract is deterministic on
    // a persistent test DB (the Postgres analogue of the Redis suite's FLUSHDB).
    // `PostgresStorage::new` recreates the schema and re-runs migrations. Harmless
    // on a fresh CI database.
    if let Ok(mut conn) = PgConnection::establish(&url) {
        conn.batch_execute("DROP SCHEMA IF EXISTS taskito CASCADE")
            .expect("reset taskito schema");
    }

    let storage = match PostgresStorage::new(&url) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Skipping Postgres tests (cannot connect): {e}");
            return;
        }
    };

    run_storage_tests(&storage);
}
