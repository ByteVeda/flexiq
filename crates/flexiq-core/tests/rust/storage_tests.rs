//! Backend-agnostic storage integration tests.
//!
//! These tests exercise the `Storage` trait contract and can run against any
//! backend. Currently wired for SQLite (always) and Redis (behind the `redis`
//! feature flag + a running redis-server).
//!
//! Each test function uses a unique queue name to avoid cross-contamination
//! when all tests share a single storage instance.

use flexiq_core::error::QueueError;
use flexiq_core::job::{now_millis, JobCompletion, JobStatus, NewJob};
use flexiq_core::step::{classify_step_failure, StepLimits, StepSession, StepSleep};
use flexiq_core::storage::records::{
    DebounceOptions, NewJobStep, SleepOutcome, StepCommit, StepKind, SubscriptionMode,
    WorkerRegistration, WorkerStatus,
};
use flexiq_core::storage::{DeadJob, RetentionCutoffs, Storage};
use flexiq_core::SqliteStorage;

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

/// A batch dequeue archives the expired candidates it skips, like the single-job
/// `dequeue`. Asserted through the listings because they are what a status move
/// alone would fool: a terminal status is read from the archive, so a job merely
/// flipped to `Cancelled` in place is reachable from neither list.
fn test_dequeue_batch_archives_expired_jobs(s: &impl Storage) {
    let q = "q-dequeue-batch-expired";
    let now = now_millis();

    let mut expiring = make_job(q, "batch_expired");
    expiring.expires_at = Some(now - 1_000);
    let expired = s.enqueue(expiring).unwrap();
    let live = s.enqueue(make_job(q, "batch_live")).unwrap();

    let claimed = s.dequeue_batch(q, now + 1_000, None, 10).unwrap();
    assert_eq!(claimed.len(), 1, "the expired job is not claimable");
    assert_eq!(claimed[0].id, live.id);

    let cancelled = s
        .list_jobs(
            Some(JobStatus::Cancelled as i32),
            Some(q),
            None,
            50,
            0,
            None,
        )
        .unwrap();
    assert!(
        cancelled.iter().any(|job| job.id == expired.id),
        "the expired job must be archived as cancelled"
    );
    let pending = s
        .list_jobs(Some(JobStatus::Pending as i32), Some(q), None, 50, 0, None)
        .unwrap();
    assert!(!pending.iter().any(|job| job.id == expired.id));
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
    s.reschedule(&job.id, future, None).unwrap();

    let fetched = s.get_job(&job.id, None).unwrap().unwrap();
    assert_eq!(fetched.status, JobStatus::Pending);
    assert_eq!(fetched.scheduled_at, future);
    assert_eq!(
        fetched.retry_count, 0,
        "reschedule must not burn retry budget"
    );

    // Scoped like every other id-addressed method: a step sleep reschedules
    // with an id that reached the queue through task code, so an id from
    // another namespace must read as unknown rather than move a job.
    assert!(
        s.reschedule(&job.id, future + 1000, Some("other")).is_err(),
        "a job outside the namespace must not be rescheduled"
    );
    let untouched = s.get_job(&job.id, None).unwrap().unwrap();
    assert_eq!(untouched.scheduled_at, future);
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
        Err(flexiq_core::error::QueueError::DependencyNotFound(_))
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

fn test_list_dead_for_retry_excludes_shed(s: &impl Storage) {
    // Shed entries are never retried, so their `dlq_retry_count` never moves
    // and they keep their place at the head of the `failed_at` ordering. The
    // limit is applied by the query, so excluding them anywhere but in the
    // query would let them fill the page and hide the failures behind them.
    let q = "q-dlq-retry-shed";
    const FLOOD: usize = 5;
    const LIMIT: i64 = 3;

    for i in 0..FLOOD {
        let job = s.enqueue(make_job(q, "shed_task")).unwrap();
        s.shed_to_dlq(&job, &format!("codel: sojourn {i}ms exceeded target"), None)
            .unwrap();
    }
    // `failed_at` has millisecond resolution: make the ordinary failure
    // strictly the newest entry, so it is genuinely behind the whole flood.
    std::thread::sleep(std::time::Duration::from_millis(2));
    let failed = s.enqueue(make_job(q, "failed_task")).unwrap();
    s.move_to_dlq(&failed, "ConnectionError: refused", None)
        .unwrap();

    let qs = [q.to_string()];
    let cands = s
        .list_dead_for_retry(now_millis() + 5000, 3, None, &qs, LIMIT)
        .unwrap();
    assert_eq!(
        cands.len(),
        1,
        "only the ordinary failure is a retry candidate"
    );
    assert_eq!(
        cands[0].original_job_id, failed.id,
        "the failure behind the shed flood is still reachable within the limit"
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

    s.register_worker(
        &WorkerRegistration::new("w-test-1", "q-workers", 4)
            .resources(resources)
            .resource_health(health)
            .hostname(Some("test-host"))
            .pid(Some(12345))
            .pool_type(Some("thread"))
            .sdk(Some("rust"), Some("9.9.9"))
            .registry_fingerprint(Some("fafd30ef8ebcb7de")),
    )
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
    // What the worker can run, so the one host in a fleet that discovered a
    // different task set is visible from the registry alone.
    assert_eq!(w.registry_fingerprint.as_deref(), Some("fafd30ef8ebcb7de"));

    // A shell that reports no registry must read back as absent, not as an
    // empty string: "reports nothing" and "runs nothing" are the same answer
    // here, and neither may look like a registry that differs from its peers'.
    s.register_worker(&WorkerRegistration::new(
        "w-test-no-registry",
        "q-workers",
        1,
    ))
    .unwrap();
    let quiet = s
        .list_workers()
        .unwrap()
        .into_iter()
        .find(|w| w.worker_id == "w-test-no-registry")
        .unwrap();
    assert_eq!(quiet.registry_fingerprint, None);

    // Test update_worker_status
    s.update_worker_status("w-test-1", WorkerStatus::Draining)
        .unwrap();
    let workers = s.list_workers().unwrap();
    let w = workers.iter().find(|w| w.worker_id == "w-test-1").unwrap();
    assert_eq!(w.status, "draining");

    // list_live_worker_ids applies the cutoff without loading the row: a fresh
    // worker is live under a past cutoff and excluded under a future one.
    let now = flexiq_core::job::now_millis();
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

    let row = flexiq_core::CircuitBreakerState {
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
    use flexiq_core::NewPeriodicTask;
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
    use flexiq_core::NewSubscription;
    // Aged past the registration grace window so the reaper may act on the
    // ephemeral rows created below; freshness is covered by the grace test.
    let now = now_millis() - flexiq_core::storage::EPHEMERAL_SUBSCRIPTION_GRACE_MS - 1_000;
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
    use flexiq_core::pubsub::{publish_to_topic, DeliveryDefaults, PublishRequest};
    use flexiq_core::NewSubscription;

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
fn log_sub(topic: &str, name: &str) -> flexiq_core::NewSubscription {
    flexiq_core::NewSubscription {
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

fn payloads(msgs: &[flexiq_core::storage::records::TopicMessage]) -> Vec<Vec<u8>> {
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
    use flexiq_core::storage::enqueue_batch_dedup;
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
    use flexiq_core::storage::enqueue_batch_dedup;
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
    orders.insert(q.to_string(), flexiq_core::storage::DispatchOrder::Lifo);
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
    test_dequeue_batch_archives_expired_jobs(s);
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
    test_list_dead_for_retry_excludes_shed(s);
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
    test_enqueue_debounced_collapses_a_burst(s);
    test_enqueue_debounced_caps_at_max_wait(s);
    test_enqueue_debounced_skips_a_claimed_job(s);
    test_enqueue_debounced_isolates_keys_and_namespaces(s);
    test_enqueue_debounced_replaces_the_payload_on_request(s);
    test_enqueue_debounced_rejects_unusable_options(s);
    test_steps_commit_and_replay_in_order(s);
    test_steps_identical_recommit_is_a_success(s);
    test_steps_refuse_a_result_over_the_cap(s);
    test_steps_re_assert_a_swept_claim(s);
    test_steps_refuse_a_superseded_owner(s);
    test_steps_refuse_the_previous_attempt(s);
    test_steps_survive_a_retry_and_a_requeue(s);
    test_steps_leave_no_orphan_after_a_terminal_write(s);
    test_steps_refuse_a_commit_racing_a_terminal_write(s);
    test_steps_sleep_pins_its_deadline(s);
    test_steps_reject_a_reused_explicit_key(s);
    test_delete_job_steps_is_namespace_scoped(s);
    test_authorize_attempt_writes_nothing(s);
    test_a_step_at_the_cap_round_trips_byte_for_byte(s);
    test_step_session_memoizes_across_attempts(s);
    test_step_idempotency_key_survives_a_dlq_retry(s);
    test_step_session_refuses_a_changed_sequence(s);
    test_step_session_sleeps_by_ending_the_attempt(s);
    test_an_elapsed_sleep_wakes_the_job_immediately(s);
}

// ── Durable inline steps ─────────────────────────────────────────────

/// Enqueue, dequeue and claim one job, ready for a step write.
fn stepped_job(s: &impl Storage, queue: &str, owner: &str) -> flexiq_core::job::Job {
    let job = s.enqueue(make_job(queue, "stepped_task")).unwrap();
    s.dequeue(queue, now_millis() + 1000, None).unwrap();
    assert!(s.claim_execution(&job.id, owner).unwrap());
    s.get_job(&job.id, None).unwrap().unwrap()
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

fn commit(
    s: &impl Storage,
    step: &NewJobStep<'_>,
    owner: &str,
) -> flexiq_core::error::Result<StepCommit> {
    s.record_step_result(step, owner, 0, &StepLimits::default(), None)
}

fn test_steps_commit_and_replay_in_order(s: &impl Storage) {
    let job = stepped_job(s, "q-steps-order", "w-order");

    for (seq, key) in [(0, "charge#0"), (1, "email#0")] {
        assert_eq!(
            commit(s, &run_step(&job.id, seq, key, key.as_bytes()), "w-order").unwrap(),
            StepCommit::Committed
        );
    }

    let steps = s.get_job_steps(&job.id, None).unwrap();
    assert_eq!(steps.len(), 2);
    assert_eq!(steps[0].seq, 0);
    assert_eq!(steps[0].step_key, "charge#0");
    assert_eq!(steps[0].kind, StepKind::Run);
    assert_eq!(steps[0].result.as_deref(), Some(b"charge#0".as_slice()));
    assert_eq!(steps[1].step_key, "email#0");
}

fn test_steps_identical_recommit_is_a_success(s: &impl Storage) {
    let job = stepped_job(s, "q-steps-recommit", "w-recommit");
    let step = run_step(&job.id, 0, "charge#0", b"ok");

    assert_eq!(
        commit(s, &step, "w-recommit").unwrap(),
        StepCommit::Committed
    );
    assert_eq!(
        commit(s, &step, "w-recommit").unwrap(),
        StepCommit::AlreadyCommitted,
        "a retransmission of a commit that already landed is a success"
    );
    assert_eq!(s.get_job_steps(&job.id, None).unwrap().len(), 1);

    let err = commit(
        s,
        &run_step(&job.id, 0, "charge#0", b"different"),
        "w-recommit",
    )
    .unwrap_err();
    assert!(matches!(err, QueueError::StepDiverged { .. }), "{err}");
}

fn test_steps_refuse_a_result_over_the_cap(s: &impl Storage) {
    let job = stepped_job(s, "q-steps-cap", "w-cap");
    let limits = StepLimits {
        max_step_bytes: 8,
        ..StepLimits::default()
    };

    let err = s
        .record_step_result(
            &run_step(&job.id, 0, "render#0", &[7u8; 64]),
            "w-cap",
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
        } => assert_eq!((limit.as_str(), actual, allowed), ("step bytes", 64, 8)),
        other => panic!("expected a cap refusal, got {other}"),
    }
    assert!(s.get_job_steps(&job.id, None).unwrap().is_empty());

    let counted = StepLimits {
        max_steps: 1,
        ..StepLimits::default()
    };
    s.record_step_result(
        &run_step(&job.id, 0, "noop#0", &[]),
        "w-cap",
        0,
        &counted,
        None,
    )
    .unwrap();
    let err = s
        .record_step_result(
            &run_step(&job.id, 1, "noop#1", &[]),
            "w-cap",
            0,
            &counted,
            None,
        )
        .unwrap_err();
    assert!(
        matches!(&err, QueueError::StepLimitExceeded { limit, .. } if limit == "step count"),
        "a loop of empty steps must still hit a cap: {err}"
    );
}

fn test_steps_re_assert_a_swept_claim(s: &impl Storage) {
    let job = stepped_job(s, "q-steps-swept", "worker-swept");

    // Claims are swept by age, so a job that legitimately runs longer than the
    // cutoff finds its own claim gone while still being the only thing running.
    s.purge_execution_claims(now_millis() + 1000).unwrap();
    assert_eq!(
        commit(s, &run_step(&job.id, 0, "charge#0", b"ok"), "worker-swept").unwrap(),
        StepCommit::Committed,
        "an absent claim on a still-Running job is re-asserted, not treated as lost"
    );
    assert_eq!(
        s.list_claims_by_worker("worker-swept").unwrap(),
        vec![job.id.clone()]
    );
}

fn test_steps_refuse_a_superseded_owner(s: &impl Storage) {
    let job = stepped_job(s, "q-steps-superseded", "w-superseded");
    assert!(s
        .reclaim_execution(&job.id, "w-superseded", "worker-b")
        .unwrap());

    let err = commit(s, &run_step(&job.id, 0, "charge#0", b"ok"), "w-superseded").unwrap_err();
    assert!(matches!(err, QueueError::ClaimLost(_)), "{err}");
    assert!(s.get_job_steps(&job.id, None).unwrap().is_empty());
}

fn test_steps_refuse_the_previous_attempt(s: &impl Storage) {
    let q = "q-steps-attempt";
    let job = stepped_job(s, q, "w-attempt");

    // `retry` bumps `retry_count` without changing who may claim next, so the
    // owner alone cannot separate two runs of the same job.
    s.retry(&job.id, now_millis(), None).unwrap();
    s.dequeue(q, now_millis() + 1000, None).unwrap();
    assert!(
        s.claim_execution(&job.id, "w-attempt").unwrap(),
        "the retry must have revoked the ended attempt's claim"
    );

    let err = commit(s, &run_step(&job.id, 0, "charge#0", b"ok"), "w-attempt").unwrap_err();
    assert!(matches!(err, QueueError::ClaimLost(_)), "{err}");
}

fn test_steps_survive_a_retry_and_a_requeue(s: &impl Storage) {
    let q = "q-steps-survive";
    let job = stepped_job(s, q, "w-survive");
    commit(s, &run_step(&job.id, 0, "charge#0", b"ok"), "w-survive").unwrap();

    s.retry(&job.id, now_millis(), None).unwrap();
    assert_eq!(
        s.get_job_steps(&job.id, None).unwrap().len(),
        1,
        "replaying the memo is the whole point of a retry"
    );
    assert!(s.list_claims_by_worker("w-survive").unwrap().is_empty());

    s.dequeue(q, now_millis() + 1000, None).unwrap();
    assert!(s.requeue_stuck(&job.id, now_millis()).unwrap());
    assert_eq!(
        s.get_job_steps(&job.id, None).unwrap().len(),
        1,
        "a requeue exists to let another worker resume, which is when the memo matters"
    );
}

fn test_steps_leave_no_orphan_after_a_terminal_write(s: &impl Storage) {
    for (queue, terminal) in [
        ("q-steps-term-ok", "complete"),
        ("q-steps-term-fail", "fail"),
        ("q-steps-term-cancel", "cancel"),
        ("q-steps-term-dlq", "dlq"),
    ] {
        let job = stepped_job(s, queue, "w-terminal");
        commit(s, &run_step(&job.id, 0, "charge#0", b"ok"), "w-terminal").unwrap();

        match terminal {
            "complete" => s.complete(&job.id, Some(vec![1]), None).unwrap(),
            "fail" => s.fail(&job.id, "boom").unwrap(),
            "cancel" => s.mark_cancelled(&job.id, None).unwrap(),
            _ => {
                let running = s.get_job(&job.id, None).unwrap().unwrap();
                s.move_to_dlq(&running, "boom", None).unwrap();
            }
        }

        assert!(
            s.get_job_steps(&job.id, None).unwrap().is_empty(),
            "{terminal} left orphan step rows"
        );
        assert!(
            s.list_claims_by_worker("w-terminal").unwrap().is_empty(),
            "{terminal} left the execution claim behind"
        );
    }
}

fn test_steps_refuse_a_commit_racing_a_terminal_write(s: &impl Storage) {
    let job = stepped_job(s, "q-steps-race", "w-race");
    s.complete(&job.id, Some(vec![1]), None).unwrap();

    // The terminal write revoked the claim in its own transaction, so the fence
    // finds neither a claim nor a Running job — which is a lost claim, not the
    // re-assert branch, and the orphan never lands.
    let err = commit(s, &run_step(&job.id, 0, "charge#0", b"late"), "w-race").unwrap_err();
    assert!(matches!(err, QueueError::ClaimLost(_)), "{err}");
    assert!(s.get_job_steps(&job.id, None).unwrap().is_empty());
}

fn test_steps_sleep_pins_its_deadline(s: &impl Storage) {
    let q = "q-steps-sleep";
    let job = stepped_job(s, q, "w-sleep");
    let limits = StepLimits::default();
    let sleep = NewJobStep {
        job_id: &job.id,
        seq: 0,
        step_key: "cool_off#0",
        kind: StepKind::Sleep,
        result: None,
    };
    let deadline = now_millis() + 3_600_000;

    assert_eq!(
        s.sleep_job(&sleep, "w-sleep", 0, deadline, &limits, None)
            .unwrap(),
        SleepOutcome::Slept { wake_at: deadline }
    );
    let slept = s.get_job(&job.id, None).unwrap().unwrap();
    assert_eq!(slept.status, JobStatus::Pending);
    assert_eq!(slept.scheduled_at, deadline);
    assert!(
        slept.started_at.is_none(),
        "a sleeping job must not be eligible for the stale reaper"
    );
    assert!(s.list_claims_by_worker("w-sleep").unwrap().is_empty());

    // A replay of the same `sleep("1h")` must not push the deadline an hour out.
    s.dequeue(q, deadline + 1, None).unwrap();
    assert!(s.claim_execution(&job.id, "w-sleep").unwrap());
    assert_eq!(
        s.sleep_job(&sleep, "w-sleep", 0, deadline + 3_600_000, &limits, None)
            .unwrap(),
        SleepOutcome::AlreadySleeping { wake_at: deadline }
    );
    assert_eq!(
        s.get_job(&job.id, None).unwrap().unwrap().scheduled_at,
        deadline
    );

    // `kind` is part of the replay match: a run commit onto a stored sleep is a
    // divergence, not a digest mismatch.
    s.dequeue(q, deadline + 1, None).unwrap();
    assert!(s.claim_execution(&job.id, "w-sleep").unwrap());
    let err = commit(s, &run_step(&job.id, 0, "cool_off#0", b"ok"), "w-sleep").unwrap_err();
    assert!(matches!(err, QueueError::StepDiverged { .. }), "{err}");
    s.complete(&job.id, None, None).unwrap();
}

fn test_steps_reject_a_reused_explicit_key(s: &impl Storage) {
    let job = stepped_job(s, "q-steps-keys", "w-keys");
    commit(s, &run_step(&job.id, 0, "charge:order-7", b"ok"), "w-keys").unwrap();

    let err = commit(s, &run_step(&job.id, 1, "charge:order-7", b"ok"), "w-keys").unwrap_err();
    assert!(matches!(err, QueueError::StepDiverged { .. }), "{err}");

    let err = commit(s, &run_step(&job.id, 4, "gap#0", b"ok"), "w-keys").unwrap_err();
    assert!(
        matches!(err, QueueError::StepDiverged { .. }),
        "a gap is refused: {err}"
    );
}

/// The session over this backend: one snapshot read, memo hits that skip the
/// closure, and bytes that come back exactly as they went in.
fn test_step_session_memoizes_across_attempts(s: &impl Storage) {
    let job = stepped_job(s, "q-step-session", "w-session");
    let limits = StepLimits::default();
    let mut first = StepSession::load(s.clone(), &job, "w-session", limits).unwrap();

    // Bytes a codec would produce: the store must not interpret them.
    let ciphertext = b"\x00\x9fENCRYPTED\xff\xfe".to_vec();
    first
        .run("charge", None, |_| Ok(ciphertext.clone()))
        .unwrap();
    first
        .run("notify", Some("a"), |_| Ok(b"sent".to_vec()))
        .unwrap();

    // The attempt died; the next one replays from the recorded steps.
    let ran = std::cell::Cell::new(false);
    let mut second = StepSession::load(s.clone(), &job, "w-session", limits).unwrap();
    let replayed = second
        .run("charge", None, |_| {
            ran.set(true);
            Ok(vec![])
        })
        .unwrap();
    assert_eq!(replayed, ciphertext, "a memo must return the stored bytes");
    let keyed = second
        .run("notify", Some("a"), |_| {
            ran.set(true);
            Ok(vec![])
        })
        .unwrap();
    assert_eq!(keyed, b"sent");
    assert!(!ran.get(), "a memoized step must not run its closure");

    // New ground still appends after the replayed prefix.
    second.run("receipt", None, |_| Ok(b"r".to_vec())).unwrap();
    let keys: Vec<String> = s
        .get_job_steps(&job.id, None)
        .unwrap()
        .into_iter()
        .map(|step| step.step_key)
        .collect();
    assert_eq!(keys, ["charge#0", "notify:a", "receipt#0"]);
}

/// The key a step hands downstream survives the one boundary that changes a
/// job's id: an operator retrying a dead-lettered charge from the DLQ.
///
/// Without the stamped origin the resurrected job mints a fresh key, the
/// payment API sees a new request and the customer is charged twice — three
/// days later, deliberately, through the admin UI.
fn test_step_idempotency_key_survives_a_dlq_retry(s: &impl Storage) {
    let q = "q-step-dlq-key";
    let job = stepped_job(s, q, "w-dlq-key");
    let limits = StepLimits::default();
    let expected = format!("{}:charge#0", job.id);

    // The attempt reaches the charge and dies before the step row commits.
    let minted = std::cell::RefCell::new(Vec::new());
    let mut first = StepSession::load(s.clone(), &job, "w-dlq-key", limits).unwrap();
    first
        .run("charge", None, |key| {
            minted.borrow_mut().push(key.to_string());
            Err(QueueError::Other("connection reset".into()))
        })
        .unwrap_err();
    assert_eq!(first.run_key(), job.id);

    s.move_to_dlq(&job, "boom", None).unwrap();
    let dead = s
        .list_dead(1000, 0, None)
        .unwrap()
        .into_iter()
        .find(|entry| entry.original_job_id == job.id)
        .expect("dead entry");
    let resurrected = s.retry_dead(&dead.id, None).unwrap();
    assert_ne!(resurrected, job.id, "retry_dead mints a new job id");

    s.dequeue(q, now_millis() + 1000, None).unwrap();
    assert!(s.claim_execution(&resurrected, "w-dlq-key").unwrap());
    let retried = s.get_job(&resurrected, None).unwrap().unwrap();
    let mut second = StepSession::load(s.clone(), &retried, "w-dlq-key", limits).unwrap();
    assert_eq!(
        second.run_key(),
        job.id,
        "the run key is the id the run began under, not the row it runs on"
    );
    second
        .run("charge", None, |key| {
            minted.borrow_mut().push(key.to_string());
            Ok(b"receipt".to_vec())
        })
        .unwrap();
    assert_eq!(minted.into_inner(), [expected.clone(), expected]);

    // A second death and resurrection still answers with the first run.
    s.move_to_dlq(&retried, "boom again", None).unwrap();
    let dead = s
        .list_dead(1000, 0, None)
        .unwrap()
        .into_iter()
        .find(|entry| entry.original_job_id == resurrected)
        .expect("second dead entry");
    let twice = s.retry_dead(&dead.id, None).unwrap();
    let twice = s.get_job(&twice, None).unwrap().unwrap();
    assert_eq!(
        StepSession::load(s.clone(), &twice, "w-dlq-key", limits)
            .unwrap()
            .run_key(),
        job.id,
    );
}

/// A deploy that changed the step sequence fails the attempt before the closure
/// runs, and writes nothing.
fn test_step_session_refuses_a_changed_sequence(s: &impl Storage) {
    let job = stepped_job(s, "q-step-diverge", "w-diverge");
    let limits = StepLimits::default();
    let mut first = StepSession::load(s.clone(), &job, "w-diverge", limits).unwrap();
    first.run("charge", None, |_| Ok(b"a".to_vec())).unwrap();
    first.run("notify", None, |_| Ok(b"b".to_vec())).unwrap();

    let mut second = StepSession::load(s.clone(), &job, "w-diverge", limits).unwrap();
    second.run("charge", None, |_| Ok(vec![])).unwrap();
    let ran = std::cell::Cell::new(false);
    let err = second
        .run("audit", None, |_| {
            ran.set(true);
            Ok(vec![])
        })
        .unwrap_err();

    assert!(
        !ran.get(),
        "the divergence must be caught before the closure"
    );
    assert!(
        matches!(&err, QueueError::StepSequenceDiverged(divergence)
            if divergence.position == 1
                && divergence.recorded.contains("notify#0")
                && divergence.running.contains("audit#0")),
        "{err}"
    );
    assert!(
        !classify_step_failure(&err).should_retry(),
        "a divergence reproduces itself on every attempt"
    );
    assert_eq!(
        s.get_job_steps(&job.id, None).unwrap().len(),
        2,
        "a diverged attempt commits nothing"
    );
}

fn test_delete_job_steps_is_namespace_scoped(s: &impl Storage) {
    let job = stepped_job(s, "q-steps-delete", "w-delete");
    commit(s, &run_step(&job.id, 0, "charge#0", b"ok"), "w-delete").unwrap();

    assert_eq!(s.delete_job_steps(&job.id, Some("other")).unwrap(), 0);
    assert!(s.get_job_steps(&job.id, Some("other")).unwrap().is_empty());
    assert_eq!(s.delete_job_steps(&job.id, None).unwrap(), 1);
    assert!(s.get_job_steps(&job.id, None).unwrap().is_empty());
}

/// A sleep through the session: the attempt ends, the deadline is fixed by the
/// first commit, and the wake replays the steps before it.
fn test_step_session_sleeps_by_ending_the_attempt(s: &impl Storage) {
    let q = "q-step-sleep";
    let job = stepped_job(s, q, "w-sleeper");
    let limits = StepLimits::default();

    let mut first = StepSession::load(s.clone(), &job, "w-sleeper", limits).unwrap();
    first
        .run("charge", None, |_| Ok(b"receipt".to_vec()))
        .unwrap();
    let slept = first.sleep_for(Some("cool_off"), None, 3_600_000).unwrap();
    let StepSleep::Sleeping { wake_at, .. } = slept else {
        panic!("{slept:?}");
    };

    // Released, not held: `Pending` at the deadline with no claim and no
    // `started_at`, so the stale reaper leaves it alone while it sleeps.
    let sleeping = s.get_job(&job.id, None).unwrap().unwrap();
    assert_eq!(sleeping.status, JobStatus::Pending);
    assert_eq!(sleeping.scheduled_at, wake_at);
    assert_eq!(sleeping.started_at, None);
    assert_eq!(sleeping.retry_count, 0, "a sleep is not a retry");

    // Picked up early — an operator requeue, or an orphan reclaim. The stored
    // deadline stands rather than starting another hour.
    s.dequeue(q, wake_at, None).unwrap();
    assert!(s.claim_execution(&job.id, "w-sleeper").unwrap());
    let early = s.get_job(&job.id, None).unwrap().unwrap();
    let mut second = StepSession::load(s.clone(), &job, "w-sleeper", limits).unwrap();
    let ran = std::cell::Cell::new(false);
    assert_eq!(
        second
            .run("charge", None, |_| {
                ran.set(true);
                Ok(vec![])
            })
            .unwrap(),
        b"receipt"
    );
    assert!(!ran.get(), "the step before the sleep is memoized");
    assert_eq!(
        second.sleep_for(Some("cool_off"), None, 3_600_000).unwrap(),
        StepSleep::Sleeping {
            step_key: "cool_off#0".to_string(),
            wake_at,
        },
        "the first commit fixes the deadline"
    );
    assert_eq!(early.retry_count, 0);

    // Once the deadline has passed the sleep is a memo hit and the attempt
    // carries on past it.
    let steps = s.get_job_steps(&job.id, None).unwrap();
    assert_eq!(steps.len(), 2, "a replay commits no second sleep");
    assert_eq!(steps[1].kind, StepKind::Sleep);
    assert_eq!(steps[1].wake_at, Some(wake_at));
    assert_eq!(steps[1].result, None);

    let past = now_millis() - 1;
    let elapsed = stepped_job(s, "q-step-sleep-done", "w-woken");
    let mut third = StepSession::load(s.clone(), &elapsed, "w-woken", limits).unwrap();
    assert!(matches!(
        third.sleep_until(Some("nap"), None, past).unwrap(),
        StepSleep::Sleeping { .. }
    ));
    s.dequeue("q-step-sleep-done", past, None).unwrap();
    assert!(s.claim_execution(&elapsed.id, "w-woken").unwrap());
    let woken = s.get_job(&elapsed.id, None).unwrap().unwrap();
    let mut fourth = StepSession::load(s.clone(), &woken, "w-woken", limits).unwrap();
    assert_eq!(
        fourth.sleep_until(Some("nap"), None, past).unwrap(),
        StepSleep::Elapsed {
            step_key: "nap#0".to_string(),
            wake_at: past,
        }
    );
    fourth.run("after", None, |_| Ok(b"ok".to_vec())).unwrap();
    let keys: Vec<String> = s
        .get_job_steps(&elapsed.id, None)
        .unwrap()
        .into_iter()
        .map(|step| step.step_key)
        .collect();
    assert_eq!(keys, ["nap#0", "after#0"]);
}

fn debounced(queue: &str, key: &str) -> NewJob {
    let mut new_job = make_job(queue, "debounced_task");
    new_job.debounce_key = Some(key.to_string());
    new_job
}

fn debounce_opts(window_ms: i64, max_wait_ms: i64) -> DebounceOptions {
    DebounceOptions {
        window_ms,
        max_wait_ms,
        replace_payload: false,
    }
}

/// A burst under one key produces one job whose deadline keeps sliding out.
fn test_enqueue_debounced_collapses_a_burst(s: &impl Storage) {
    let q = "q-debounce-burst";
    let before = now_millis();

    let mut ids = std::collections::HashSet::new();
    for _ in 0..5 {
        let job = s
            .enqueue_debounced(debounced(q, "burst:user-1"), debounce_opts(5_000, 60_000))
            .unwrap();
        assert!(job.scheduled_at >= before + 5_000);
        ids.insert(job.id);
    }

    assert_eq!(ids.len(), 1, "the burst must land on one job");
    let pending = s
        .list_jobs(Some(JobStatus::Pending as i32), Some(q), None, 10, 0, None)
        .unwrap();
    assert_eq!(pending.len(), 1, "no second row was inserted");
}

/// The slide is capped at `first_seen + max_wait`, so a caller who never stops
/// enqueuing cannot starve the job. Asserted through the public surface only:
/// with `max_wait_ms == window_ms` the ceiling binds on the very first slide,
/// which needs no clock-skewing to observe.
fn test_enqueue_debounced_caps_at_max_wait(s: &impl Storage) {
    let q = "q-debounce-maxwait";
    let first = s
        .enqueue_debounced(debounced(q, "cap:user-1"), debounce_opts(30_000, 30_000))
        .unwrap();

    for _ in 0..3 {
        let slid = s
            .enqueue_debounced(debounced(q, "cap:user-1"), debounce_opts(30_000, 30_000))
            .unwrap();
        assert_eq!(slid.id, first.id);
        assert_eq!(
            slid.scheduled_at, first.scheduled_at,
            "a ceiling equal to the window admits no slide at all"
        );
    }
}

/// A job a worker already holds is never pulled back to a later deadline —
/// `claim_execution` writes its row without touching `status`, so the guard has
/// to consult the claim, not just the status column.
fn test_enqueue_debounced_skips_a_claimed_job(s: &impl Storage) {
    let q = "q-debounce-claimed";
    let claimed = s
        .enqueue_debounced(debounced(q, "claimed:user-1"), debounce_opts(5_000, 60_000))
        .unwrap();
    assert!(s.claim_execution(&claimed.id, "w-debounce").unwrap());

    let fresh = s
        .enqueue_debounced(debounced(q, "claimed:user-1"), debounce_opts(5_000, 60_000))
        .unwrap();
    assert_ne!(fresh.id, claimed.id);
    assert_eq!(
        s.get_job(&claimed.id, None).unwrap().unwrap().scheduled_at,
        claimed.scheduled_at
    );
}

/// Different keys never share a window, and neither do two tenants using the
/// same key.
fn test_enqueue_debounced_isolates_keys_and_namespaces(s: &impl Storage) {
    let q = "q-debounce-isolation";
    let user_1 = s
        .enqueue_debounced(debounced(q, "iso:user-1"), debounce_opts(5_000, 60_000))
        .unwrap();
    let user_2 = s
        .enqueue_debounced(debounced(q, "iso:user-2"), debounce_opts(5_000, 60_000))
        .unwrap();
    assert_ne!(user_1.id, user_2.id);

    let mut tenant_job = debounced(q, "iso:user-1");
    tenant_job.namespace = Some("tenant-debounce".to_string());
    let tenant = s
        .enqueue_debounced(tenant_job, debounce_opts(5_000, 60_000))
        .unwrap();
    assert_ne!(tenant.id, user_1.id);
}

/// `replace_payload` decides whether the run uses the newest input or the one
/// that opened the window.
fn test_enqueue_debounced_replaces_the_payload_on_request(s: &impl Storage) {
    let q = "q-debounce-payload";
    let mut opening = debounced(q, "payload:user-1");
    opening.payload = vec![1];
    let first = s
        .enqueue_debounced(opening, debounce_opts(5_000, 60_000))
        .unwrap();

    let mut kept = debounced(q, "payload:user-1");
    kept.payload = vec![2];
    let unchanged = s
        .enqueue_debounced(kept, debounce_opts(5_000, 60_000))
        .unwrap();
    assert_eq!(unchanged.id, first.id);
    assert_eq!(unchanged.payload, vec![1]);

    let mut newest = debounced(q, "payload:user-1");
    newest.payload = vec![3];
    let replaced = s
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
        s.get_job(&first.id, None).unwrap().unwrap().payload,
        vec![3]
    );
}

/// Options that cannot debounce are rejected on every backend, and a rejected
/// call writes nothing.
fn test_enqueue_debounced_rejects_unusable_options(s: &impl Storage) {
    let q = "q-debounce-invalid";
    assert!(s
        .enqueue_debounced(make_job(q, "debounced_task"), debounce_opts(5_000, 60_000))
        .is_err());
    assert!(s
        .enqueue_debounced(debounced(q, ""), debounce_opts(5_000, 60_000))
        .is_err());
    assert!(s
        .enqueue_debounced(debounced(q, "bad:user-1"), debounce_opts(0, 60_000))
        .is_err());
    assert!(s
        .enqueue_debounced(debounced(q, "bad:user-1"), debounce_opts(5_000, 1_000))
        .is_err());

    let written = s.list_jobs(None, Some(q), None, 10, 0, None).unwrap();
    assert!(written.is_empty(), "a rejected call must write nothing");
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
    use flexiq_core::RedisStorage;

    // Use DB 15 to avoid interfering with other data.
    let url = std::env::var("FLEXIQ_REDIS_TEST_URL")
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
    redis_debounce_index_never_outlives_its_job(&storage);
    redis_debounce_coalesces_onto_a_plainly_enqueued_job(&storage);
    redis_debounce_slides_an_empty_payload(&storage);
    redis_purge_metrics_drains_across_batches(&storage);
}

/// A per-entry-TTL row archived before the `archived:expiry` index existed must
/// still expire: the purge backfills the index for it. Simulated by archiving a
/// per-entry row, then stripping its expiry entry and the done marker so it
/// looks pre-upgrade.
#[cfg(feature = "redis")]
fn redis_backfills_expiry_for_preupgrade_rows(s: &flexiq_core::RedisStorage) {
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
fn redis_keyset_pages_a_large_tie_bucket(s: &flexiq_core::RedisStorage) {
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
fn redis_purge_dead_drains_across_batches(s: &flexiq_core::RedisStorage) {
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
fn redis_purge_metrics_drains_across_batches(s: &flexiq_core::RedisStorage) {
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
fn rkey(s: &flexiq_core::RedisStorage, parts: &[&str]) -> String {
    format!("{}{}", s.prefix(), parts.join(":"))
}

/// Drain any pending jobs left in `q` by earlier runs so the test that follows
/// deterministically dequeues the job it just enqueued (the shared test DB is
/// not flushed between runs).
#[cfg(feature = "redis")]
fn drain_queue(s: &flexiq_core::RedisStorage, q: &str) {
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
fn redis_claim_skips_job_dropped_from_pending_set(s: &flexiq_core::RedisStorage) {
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
fn redis_retry_keeps_job_dequeuable(s: &flexiq_core::RedisStorage) {
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
fn redis_complete_preserves_reused_unique_key(s: &flexiq_core::RedisStorage) {
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
fn redis_update_progress_never_resurrects_archived(s: &flexiq_core::RedisStorage) {
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
        Err(flexiq_core::error::QueueError::JobNotFound(_))
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
fn redis_move_to_dlq_leaves_consistent_state(s: &flexiq_core::RedisStorage) {
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
fn redis_move_to_dlq_skips_already_archived(s: &flexiq_core::RedisStorage) {
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
fn redis_mutators_reject_archived_jobs(s: &flexiq_core::RedisStorage) {
    let q = "q-redis-mutate-archived";

    // Cancel a pending job → archived as Cancelled.
    let cancelled = s.enqueue(make_job(q, "redis_archived_cancel")).unwrap();
    assert!(s.cancel_job(&cancelled.id, None).unwrap());
    assert!(matches!(
        s.retry(&cancelled.id, now_millis(), None),
        Err(flexiq_core::error::QueueError::JobNotFound(_))
    ));
    assert!(matches!(
        s.mark_cancelled(&cancelled.id, None),
        Err(flexiq_core::error::QueueError::JobNotFound(_))
    ));

    // Complete a job → archived as Complete; the same guard applies.
    let done = s.enqueue(make_job(q, "redis_archived_done")).unwrap();
    s.dequeue(q, now_millis() + 1000, None).unwrap();
    s.complete(&done.id, None, None).unwrap();
    assert!(matches!(
        s.retry(&done.id, now_millis(), None),
        Err(flexiq_core::error::QueueError::JobNotFound(_))
    ));
}

/// Purging an archived job must not delete a `jobs:unique` pointer now owned by
/// a different live job that reused the same `unique_key`.
#[cfg(feature = "redis")]
fn redis_purge_preserves_reused_unique_key(s: &flexiq_core::RedisStorage) {
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

/// Members of the debounce index of a default-namespace key. The index is an
/// implementation detail of the Redis backend, so the key is rebuilt here from
/// the same shape `debounce_index_key` writes (`-` is the default namespace).
#[cfg(feature = "redis")]
fn redis_debounce_index_size(s: &flexiq_core::RedisStorage, debounce_key: &str) -> i64 {
    let mut conn = s.conn().unwrap();
    redis::cmd("ZCARD")
        .arg(format!("{}jobs:debounce:-:{debounce_key}", s.prefix()))
        .query(&mut conn)
        .unwrap()
}

/// The index entry cannot outlive the job it points at: claiming drops it, and
/// so does any terminal move. Diesel gets this from a partial index the engine
/// maintains; Redis has to write both sides itself.
#[cfg(feature = "redis")]
fn redis_debounce_index_never_outlives_its_job(s: &flexiq_core::RedisStorage) {
    let q = "q-redis-debounce-index";
    let key = "redis-index:user-1";

    let claimed = s
        .enqueue_debounced(debounced(q, key), debounce_opts(5_000, 60_000))
        .unwrap();
    assert_eq!(redis_debounce_index_size(s, key), 1);

    let dequeued = s.dequeue(q, now_millis() + 10_000, None).unwrap().unwrap();
    assert_eq!(dequeued.id, claimed.id);
    assert_eq!(
        redis_debounce_index_size(s, key),
        0,
        "claiming a job closes its window"
    );
    s.complete(&claimed.id, None, None).unwrap();

    let cancelled = s
        .enqueue_debounced(debounced(q, key), debounce_opts(5_000, 60_000))
        .unwrap();
    assert_ne!(cancelled.id, claimed.id, "the archived job is not a target");
    assert_eq!(redis_debounce_index_size(s, key), 1);

    assert!(s.cancel_job(&cancelled.id, None).unwrap());
    assert_eq!(
        redis_debounce_index_size(s, key),
        0,
        "a terminal job leaves the index with its live rows"
    );
}

/// A job enqueued plainly with a `debounce_key` is still a slide target — the
/// Diesel partial index covers every pending row, not only debounced writes, so
/// the Redis index has to be written on the ordinary enqueue paths too.
#[cfg(feature = "redis")]
fn redis_debounce_coalesces_onto_a_plainly_enqueued_job(s: &flexiq_core::RedisStorage) {
    let q = "q-redis-debounce-plain";
    let plain = s.enqueue(debounced(q, "redis-plain:user-1")).unwrap();

    let slid = s
        .enqueue_debounced(
            debounced(q, "redis-plain:user-1"),
            debounce_opts(5_000, 60_000),
        )
        .unwrap();
    assert_eq!(slid.id, plain.id, "the plain row opened the window");
    assert!(slid.scheduled_at > plain.scheduled_at);
    assert_eq!(
        s.list_jobs(Some(JobStatus::Pending as i32), Some(q), None, 10, 0, None)
            .unwrap()
            .len(),
        1
    );
}

/// A slide preserves an empty payload. `payload` is a byte vector, so an empty
/// one is `[]` in the stored document — a `cjson` decode/encode round trip in
/// Lua would rewrite it as `{}` and the job would stop deserializing, which is
/// why the patch is applied with serde in Rust.
#[cfg(feature = "redis")]
fn redis_debounce_slides_an_empty_payload(s: &flexiq_core::RedisStorage) {
    let q = "q-redis-debounce-empty";
    let mut opening = debounced(q, "redis-empty:user-1");
    opening.payload = Vec::new();
    let first = s
        .enqueue_debounced(opening, debounce_opts(5_000, 60_000))
        .unwrap();

    let mut sliding = debounced(q, "redis-empty:user-1");
    sliding.payload = Vec::new();
    let slid = s
        .enqueue_debounced(sliding, debounce_opts(5_000, 60_000))
        .unwrap();

    assert_eq!(slid.id, first.id);
    assert!(slid.payload.is_empty());
    assert!(
        s.get_job(&first.id, None)
            .unwrap()
            .unwrap()
            .payload
            .is_empty(),
        "the slid document must still decode"
    );
}

#[cfg(feature = "postgres")]
#[test]
fn postgres_storage_tests() {
    use diesel::connection::SimpleConnection;
    use diesel::{Connection, PgConnection};
    use flexiq_core::PostgresStorage;

    let url = match std::env::var("FLEXIQ_POSTGRES_TEST_URL") {
        Ok(u) => u,
        Err(_) => {
            eprintln!("Skipping Postgres tests (FLEXIQ_POSTGRES_TEST_URL not set)");
            return;
        }
    };

    // This raw connection reaches libpq before `PostgresStorage` can, so claim
    // OpenSSL's initialization here too — see `init_openssl_without_atexit`.
    openssl_sys::init();

    // Reset the `flexiq` schema so the count-exact contract is deterministic on
    // a persistent test DB (the Postgres analogue of the Redis suite's FLUSHDB).
    // `PostgresStorage::new` recreates the schema and re-runs migrations. Harmless
    // on a fresh CI database.
    if let Ok(mut conn) = PgConnection::establish(&url) {
        conn.batch_execute("DROP SCHEMA IF EXISTS flexiq CASCADE")
            .expect("reset flexiq schema");
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

fn test_authorize_attempt_writes_nothing(s: &impl Storage) {
    use flexiq_core::storage::records::AttemptFence;

    let job = stepped_job(s, "q-steps-authorize", "w-authorize");

    // Runs once per result on the drain path, so it must not take a write
    // transaction — and an authorization check must not leave behind a claim
    // its caller never asked for. The age-sweep case is where that would show:
    // a step *write* re-asserts here, a check must not.
    s.purge_execution_claims(now_millis() + 1000).unwrap();
    assert_eq!(
        s.authorize_attempt(&job.id, "w-authorize", 0, None)
            .unwrap(),
        AttemptFence::Authorized,
        "an absent claim on a still-Running job at the same attempt is not a lost one"
    );
    assert!(
        s.list_claims_by_worker("w-authorize").unwrap().is_empty(),
        "a read-only check must write no claim"
    );

    s.complete(&job.id, None, None).unwrap();
    assert_eq!(
        s.authorize_attempt(&job.id, "w-authorize", 0, None)
            .unwrap(),
        AttemptFence::Superseded
    );
}

fn test_a_step_at_the_cap_round_trips_byte_for_byte(s: &impl Storage) {
    let job = stepped_job(s, "q-steps-bytes", "w-bytes");
    let limits = StepLimits {
        max_step_bytes: 4096,
        max_total_bytes: 6000,
        ..StepLimits::default()
    };
    // Values a text encoding would mangle or inflate: every byte, high bits set,
    // and the NUL and newline the Redis row format uses as separators.
    let payload: Vec<u8> = (0..4096).map(|i| (i % 256) as u8).collect();

    assert_eq!(
        s.record_step_result(
            &run_step(&job.id, 0, "blob#0", &payload),
            "w-bytes",
            0,
            &limits,
            None
        )
        .unwrap(),
        StepCommit::Committed,
        "a step exactly at the cap fits on every backend"
    );
    assert_eq!(
        s.get_job_steps(&job.id, None).unwrap()[0].result.as_deref(),
        Some(payload.as_slice())
    );

    // The caps count payload bytes, not whatever the backend's own encoding
    // costs, so one more full step must break the per-job total on every
    // backend at the same place.
    let err = s
        .record_step_result(
            &run_step(&job.id, 1, "blob#1", &payload),
            "w-bytes",
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
        } => assert_eq!(
            (limit.as_str(), actual, allowed),
            ("total bytes", 8192, 6000)
        ),
        other => panic!("expected the per-job cap, got {other}"),
    }
}

fn test_an_elapsed_sleep_wakes_the_job_immediately(s: &impl Storage) {
    let q = "q-steps-elapsed";
    let job = stepped_job(s, q, "w-elapsed");
    let limits = StepLimits::default();
    let sleep = NewJobStep {
        job_id: &job.id,
        seq: 0,
        step_key: "cool_off#0",
        kind: StepKind::Sleep,
        result: None,
    };
    let deadline = now_millis() - 60_000;

    // A deadline already in the past is committed and reported as it stands.
    // Refusing it here would be the wrong layer: the worker decides whether a
    // sleep is still pending, and a stored row it has passed is a memo hit it
    // continues through. Storage's job is to answer truthfully about the
    // deadline it holds.
    assert_eq!(
        s.sleep_job(&sleep, "w-elapsed", 0, deadline, &limits, None)
            .unwrap(),
        SleepOutcome::Slept { wake_at: deadline }
    );

    // And an elapsed sleep leaves the job runnable *now* rather than parked:
    // `scheduled_at` in the past is exactly what "this sleep is over" means, so
    // the next poll picks it up and the worker replays past the committed row.
    let woken = s.get_job(&job.id, None).unwrap().unwrap();
    assert_eq!(woken.status, JobStatus::Pending);
    assert_eq!(woken.scheduled_at, deadline);
    assert_eq!(
        s.dequeue(q, now_millis(), None).unwrap().map(|j| j.id),
        Some(job.id.clone()),
        "an elapsed sleep must not park the job until some later poll"
    );

    // Replaying it keeps the original instant, elapsed or not.
    assert!(s.claim_execution(&job.id, "w-elapsed").unwrap());
    assert_eq!(
        s.sleep_job(
            &sleep,
            "w-elapsed",
            0,
            now_millis() + 3_600_000,
            &limits,
            None
        )
        .unwrap(),
        SleepOutcome::AlreadySleeping { wake_at: deadline }
    );
    assert_eq!(
        s.get_job(&job.id, None).unwrap().unwrap().scheduled_at,
        deadline,
        "a replay must never push an already-elapsed deadline into the future"
    );
}
