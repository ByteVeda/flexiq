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
    DebounceOptions, NewJobStep, SettleClaimant, SettleGrant, SleepOutcome, StepCommit, StepKind,
    SubscriptionMode, WorkerRegistration, WorkerStatus,
};
use flexiq_core::storage::{DeadJob, RetentionCutoffs, Storage};
use flexiq_core::{SqliteStorage, RETRY_BUDGET_EXHAUSTED};

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

/// #836: terminal jobs in a window, per queue and per namespace. Pending and
/// running jobs are not throughput.
fn test_queue_throughput(s: &impl Storage) {
    let q = "q-throughput";
    let ns = Some("tp-tenant");
    let before = now_millis() - 1;
    let in_ns = |task: &str| {
        let mut job = make_job(q, task);
        job.namespace = ns.map(str::to_owned);
        s.enqueue(job).unwrap()
    };

    for _ in 0..2 {
        let job = in_ns("tp_task");
        s.dequeue(q, now_millis() + 1000, ns).unwrap();
        s.complete(&job.id, None, ns).unwrap();
    }
    let cancelled = in_ns("tp_task");
    assert!(s.cancel_job(&cancelled.id, ns).unwrap());
    in_ns("tp_task"); // pending: not throughput
                      // Another namespace's completion stays out of this one's count.
    let other = s.enqueue(make_job(q, "tp_task")).unwrap();
    s.dequeue(q, now_millis() + 1000, None).unwrap();
    s.complete(&other.id, None, None).unwrap();

    let counts = s.queue_throughput(before, ns).unwrap();
    let stats = counts.get(q).expect("the queue had terminal jobs");
    assert_eq!(stats.completed, 2);
    assert_eq!(stats.cancelled, 1);
    assert_eq!((stats.pending, stats.running, stats.failed), (0, 0, 0));

    // A window that starts after everything finished is empty.
    let later = s.queue_throughput(now_millis() + 60_000, ns).unwrap();
    assert!(!later.contains_key(q), "{later:?}");
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

fn test_cancel_requested_among(s: &impl Storage) {
    // One running job with a cancel request, one running without, one unknown
    // id: only the first comes back.
    let q = "q-cancel-among";
    let asked = s.enqueue(make_job(q, "cancel_among")).unwrap();
    let quiet = s.enqueue(make_job(q, "cancel_among")).unwrap();
    s.dequeue(q, now_millis() + 1000, None).unwrap().unwrap();
    s.dequeue(q, now_millis() + 1000, None).unwrap().unwrap();
    assert!(s.request_cancel(&asked.id, None).unwrap());

    let ids = vec![
        asked.id.clone(),
        quiet.id.clone(),
        "no-such-job".to_string(),
    ];
    assert_eq!(
        s.cancel_requested_among(&ids, None).unwrap(),
        vec![asked.id]
    );
    assert!(s.cancel_requested_among(&[], None).unwrap().is_empty());
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

fn test_unique_key_dedup_is_reported(s: &impl Storage) {
    // The flag is what EnqueueResponse.deduplicated carries, and nothing above
    // the backend can derive it: the id is generated inside the insert, so the
    // caller has no candidate to compare the answer against.
    let mut first = make_job("q-unique-reported", "unique_task");
    first.unique_key = Some("dedup-reported".to_string());
    let (j1, deduplicated) = s.enqueue_unique_reporting(first).unwrap();
    assert!(!deduplicated, "the first enqueue inserted the job");

    let mut second = make_job("q-unique-reported", "unique_task");
    second.unique_key = Some("dedup-reported".to_string());
    let (j2, deduplicated) = s.enqueue_unique_reporting(second).unwrap();
    assert!(deduplicated, "the second enqueue found the active job");
    assert_eq!(j1.id, j2.id);

    // No key means nothing to dedupe against, on every backend.
    let (_, deduplicated) = s
        .enqueue_unique_reporting(make_job("q-unique-reported", "unique_task"))
        .unwrap();
    assert!(!deduplicated);

    // A batch reports one flag per item, in input order.
    let keyed = |uk: &str| {
        let mut j = make_job("q-unique-reported-batch", "unique_task");
        j.unique_key = Some(uk.to_string());
        j
    };
    let flags: Vec<bool> = s
        .enqueue_unique_batch_reporting(vec![keyed("batch-uk-a"), keyed("batch-uk-b")])
        .unwrap()
        .into_iter()
        .map(|(_, deduplicated)| deduplicated)
        .collect();
    assert_eq!(flags, vec![false, false]);

    let flags: Vec<bool> = s
        .enqueue_unique_batch_reporting(vec![keyed("batch-uk-a"), keyed("batch-uk-c")])
        .unwrap()
        .into_iter()
        .map(|(_, deduplicated)| deduplicated)
        .collect();
    assert_eq!(flags, vec![true, false]);
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

/// #773: `unique_key` dedup must not read across the namespace boundary
/// `get_job`/`cancel_job`/debounce already keep — two namespaces sending the
/// same key must each get their own job, and the default (unnamespaced) case
/// must keep deduping, which is the NULL trap a naive `(namespace, key)`
/// unique index falls into.
fn test_unique_key_dedup_is_namespace_scoped(s: &impl Storage) {
    let q = "q-unique-namespace";

    // Two namespaces, same key: two jobs, neither reported as deduplicated.
    let mut a = make_job(q, "unique_task");
    a.unique_key = Some("ns-key".to_string());
    a.namespace = Some("tenant-a".to_string());
    let (job_a, dedup_a) = s.enqueue_unique_reporting(a).unwrap();
    assert!(!dedup_a);

    let mut b = make_job(q, "unique_task");
    b.unique_key = Some("ns-key".to_string());
    b.namespace = Some("tenant-b".to_string());
    let (job_b, dedup_b) = s.enqueue_unique_reporting(b).unwrap();
    assert!(!dedup_b, "tenant-b must get its own job, not tenant-a's");
    assert_ne!(job_a.id, job_b.id);
    assert_eq!(job_b.namespace.as_deref(), Some("tenant-b"));

    // Same key, same namespace: the second call dedupes onto the first.
    let mut a2 = make_job(q, "unique_task");
    a2.unique_key = Some("ns-key".to_string());
    a2.namespace = Some("tenant-a".to_string());
    let (job_a2, dedup_a2) = s.enqueue_unique_reporting(a2).unwrap();
    assert!(dedup_a2);
    assert_eq!(job_a2.id, job_a.id);

    // The default namespace (None) must still dedupe against itself -- the
    // NULL trap a plain (namespace, unique_key) unique index falls into.
    let d1 = {
        let mut j = make_job(q, "unique_task");
        j.unique_key = Some("default-ns-key".to_string());
        j
    };
    let (job_d1, dedup_d1) = s.enqueue_unique_reporting(d1).unwrap();
    assert!(!dedup_d1);

    let d2 = {
        let mut j = make_job(q, "unique_task");
        j.unique_key = Some("default-ns-key".to_string());
        j
    };
    let (job_d2, dedup_d2) = s.enqueue_unique_reporting(d2).unwrap();
    assert!(dedup_d2, "default namespace must still dedupe");
    assert_eq!(job_d2.id, job_d1.id);

    // The default namespace (None) and a named one must not collide either --
    // catches an implementation that treats None as a wildcard.
    let mut none_vs_named = make_job(q, "unique_task");
    none_vs_named.unique_key = Some("none-vs-named-key".to_string());
    let (job_none, dedup_none) = s.enqueue_unique_reporting(none_vs_named).unwrap();
    assert!(!dedup_none);

    let mut named = make_job(q, "unique_task");
    named.unique_key = Some("none-vs-named-key".to_string());
    named.namespace = Some("tenant-a".to_string());
    let (job_named, dedup_named) = s.enqueue_unique_reporting(named).unwrap();
    assert!(
        !dedup_named,
        "a named namespace must not dedupe onto None's job"
    );
    assert_ne!(job_none.id, job_named.id);

    // None and Some("") must not collide either -- COALESCE(namespace, '')
    // alone would fold both to the same index slot even though the query
    // treats them as different namespaces (`is_null()` vs `.eq("")`).
    let mut none_vs_empty = make_job(q, "unique_task");
    none_vs_empty.unique_key = Some("none-vs-empty-key".to_string());
    let (job_empty_none, dedup_empty_none) = s.enqueue_unique_reporting(none_vs_empty).unwrap();
    assert!(!dedup_empty_none);

    let mut empty = make_job(q, "unique_task");
    empty.unique_key = Some("none-vs-empty-key".to_string());
    empty.namespace = Some(String::new());
    let (job_empty, dedup_empty) = s.enqueue_unique_reporting(empty).unwrap();
    assert!(
        !dedup_empty,
        "the empty-string namespace must not dedupe onto None's job"
    );
    assert_ne!(job_empty_none.id, job_empty.id);
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

/// Dead-letter an entry for `task_name` in `namespace`, returning its DLQ id.
fn dead_letter_in(s: &impl Storage, q: &str, task_name: &str, namespace: Option<&str>) -> String {
    let mut new_job = make_job(q, task_name);
    new_job.namespace = namespace.map(str::to_owned);
    let job = s.enqueue(new_job).unwrap();
    s.dequeue(q, now_millis() + 1000, namespace).unwrap();
    let running = s.get_job(&job.id, None).unwrap().unwrap();
    s.move_to_dlq(&running, "boom", None).unwrap();
    s.list_dead(100, 0, namespace)
        .unwrap()
        .into_iter()
        .find(|d| d.original_job_id == job.id)
        .unwrap()
        .id
}

/// #836: a scoped purge and a scoped read reach one namespace's dead letters;
/// `None` stays unscoped, like `list_dead`.
fn test_dead_letter_purge_and_get_are_namespace_scoped(s: &impl Storage) {
    let q = "q-dlq-ns";
    let (a, b) = (Some("dns-tenant-a"), Some("dns-tenant-b"));
    let in_a = dead_letter_in(s, q, "dns_task", a);
    let in_b = dead_letter_in(s, q, "dns_task", b);

    // A read is scoped, and carries the payload a listing omits.
    let read = s.get_dead(&in_a, a).unwrap().expect("own entry");
    assert_eq!(read.payload, make_job(q, "dns_task").payload);
    assert!(
        s.get_dead(&in_a, b).unwrap().is_none(),
        "read across tenants"
    );
    assert!(
        s.get_dead(&in_a, None).unwrap().is_some(),
        "None is unscoped"
    );
    assert!(s.get_dead("no-such-dead-id", a).unwrap().is_none());

    // A scoped purge by task leaves the other tenant's entry.
    assert_eq!(s.purge_dead_by_task("dns_task", a).unwrap(), 1);
    assert!(s.get_dead(&in_a, None).unwrap().is_none());
    assert!(s.get_dead(&in_b, None).unwrap().is_some());

    // So does a scoped purge by age.
    let in_a = dead_letter_in(s, q, "dns_task", a);
    assert_eq!(s.purge_dead(now_millis() + 60_000, a).unwrap(), 1);
    assert!(s.get_dead(&in_a, None).unwrap().is_none());
    assert!(s.get_dead(&in_b, None).unwrap().is_some());
    assert_eq!(s.purge_dead(now_millis() + 60_000, b).unwrap(), 1);
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
    assert_eq!(s.purge_dead_by_task("task_a", None).unwrap(), 2);
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

    let workers = s.list_workers(None).unwrap();
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
        .list_workers(None)
        .unwrap()
        .into_iter()
        .find(|w| w.worker_id == "w-test-no-registry")
        .unwrap();
    assert_eq!(quiet.registry_fingerprint, None);

    // Test update_worker_status
    s.update_worker_status("w-test-1", WorkerStatus::Draining)
        .unwrap();
    let workers = s.list_workers(None).unwrap();
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

/// An operator's drain request reaches only its own namespace's worker, and
/// that worker reads it back on its next heartbeat.
fn test_worker_drain_request(s: &impl Storage) {
    let ns = Some("wdrain-tenant");
    s.register_worker(&WorkerRegistration::new("w-drain", "q", 1).namespace(ns))
        .unwrap();

    assert_eq!(
        s.heartbeat("w-drain", None).unwrap(),
        Some(WorkerStatus::Active)
    );
    // Another namespace, the default one included, cannot reach it.
    assert!(!s
        .request_worker_drain("w-drain", Some("wdrain-other"))
        .unwrap());
    assert!(!s.request_worker_drain("w-drain", None).unwrap());
    assert!(!s.request_worker_drain("w-nobody", ns).unwrap());
    assert_eq!(
        s.heartbeat("w-drain", None).unwrap(),
        Some(WorkerStatus::Active)
    );

    assert!(s.request_worker_drain("w-drain", ns).unwrap());
    assert_eq!(
        s.heartbeat("w-drain", None).unwrap(),
        Some(WorkerStatus::Draining)
    );

    // Gone once it unregisters, and a later request does not resurrect it.
    s.unregister_worker("w-drain").unwrap();
    assert!(!s.request_worker_drain("w-drain", ns).unwrap());
    assert!(s
        .list_workers(ns)
        .unwrap()
        .iter()
        .all(|w| w.worker_id != "w-drain"));
}

/// A worker registers with its namespace, and a listing shows one namespace's
/// workers (#836). `None` is the default namespace, not every namespace.
fn test_workers_are_namespace_scoped(s: &impl Storage) {
    let ns = Some("wns-tenant");
    s.register_worker(&WorkerRegistration::new("w-ns-tenant", "q", 1).namespace(ns))
        .unwrap();
    s.register_worker(&WorkerRegistration::new("w-ns-default", "q", 1))
        .unwrap();

    let ids = |namespace| -> Vec<String> {
        s.list_workers(namespace)
            .unwrap()
            .into_iter()
            .map(|w| w.worker_id)
            .collect()
    };
    let tenant = ids(ns);
    assert!(tenant.contains(&"w-ns-tenant".to_string()));
    assert!(!tenant.contains(&"w-ns-default".to_string()));
    let default = ids(None);
    assert!(default.contains(&"w-ns-default".to_string()));
    assert!(!default.contains(&"w-ns-tenant".to_string()));
    assert!(ids(Some("wns-nobody")).is_empty());

    let row = s
        .list_workers(ns)
        .unwrap()
        .into_iter()
        .find(|w| w.worker_id == "w-ns-tenant")
        .unwrap();
    assert_eq!(row.namespace.as_deref(), ns);

    // The id-keyed members stay namespace-blind: the live set spans both.
    let live = s
        .list_live_worker_ids(flexiq_core::job::now_millis() - 10_000)
        .unwrap();
    assert!(live.contains(&"w-ns-tenant".to_string()));
    assert!(live.contains(&"w-ns-default".to_string()));

    s.unregister_worker("w-ns-tenant").unwrap();
    s.unregister_worker("w-ns-default").unwrap();
}

fn test_pause_resume_queue(s: &impl Storage) {
    let q = "q-pause-test";
    s.pause_queue(q, None).unwrap();
    // A second pause is an update, not a duplicate row.
    s.pause_queue(q, None).unwrap();
    let paused = s.list_paused_queues(None).unwrap();
    assert_eq!(paused.iter().filter(|name| *name == q).count(), 1);

    s.resume_queue(q, None).unwrap();
    let paused = s.list_paused_queues(None).unwrap();
    assert!(!paused.contains(&q.to_string()));
}

/// A pause is identified by `(namespace, queue_name)` (#836). Before it, the
/// row was keyed by name alone and the scheduler read it unscoped, so pausing
/// a queue in one tenant stopped the same-named queue in every tenant.
fn test_pause_resume_queue_is_namespace_scoped(s: &impl Storage) {
    let q = "q-pause-ns";
    let (a, b) = (Some("qns-tenant-a"), Some("qns-tenant-b"));
    let paused_in = |ns| s.list_paused_queues(ns).unwrap().contains(&q.to_string());

    s.pause_queue(q, a).unwrap();
    assert!(paused_in(a));
    assert!(!paused_in(b), "a pause in one tenant reached another");
    assert!(!paused_in(None), "a pause in a tenant reached the default");

    // `None` is the default namespace, not a wildcard: pausing and resuming
    // it leaves the tenant's pause alone.
    s.pause_queue(q, None).unwrap();
    s.resume_queue(q, None).unwrap();
    assert!(paused_in(a));

    s.resume_queue(q, b).unwrap();
    assert!(paused_in(a), "a resume in one tenant reached another");
    s.resume_queue(q, a).unwrap();
    assert!(!paused_in(a));
}

fn test_execution_claims_purge(s: &impl Storage) {
    // Regression: Redis `purge_execution_claims` was a silent no-op. The
    // scheduler's maintenance loop relies on this method to reap stale claims,
    // so all backends must honor the `older_than_ms` cutoff.
    let worker = "w-purge";
    let old_job = "old-claim-job-id";
    let fresh_job = "fresh-claim-job-id";

    assert!(s.claim_execution(old_job, worker).unwrap().is_some());
    // Advance past the old claim so the cutoff below can catch it but miss
    // the fresh claim (claimed after the cutoff below is computed).
    std::thread::sleep(std::time::Duration::from_millis(20));
    let cutoff = now_millis();
    std::thread::sleep(std::time::Duration::from_millis(20));
    assert!(s.claim_execution(fresh_job, worker).unwrap().is_some());

    let purged = s.purge_execution_claims(cutoff).unwrap();
    assert!(
        purged >= 1,
        "purge must delete at least the one claim older than the cutoff"
    );

    // The old claim is gone — a fresh claim_execution for the same job succeeds.
    assert!(s.claim_execution(old_job, worker).unwrap().is_some());
    // The fresh claim must still be held.
    assert!(s.claim_execution(fresh_job, worker).unwrap().is_none());

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
    let found = stale
        .iter()
        .find(|s| s.job.id == job.id)
        .expect("a running job past its timeout must be reaped");
    assert!(
        !found.awaiting_settle,
        "an ordinary job was never accepted out of band"
    );
    // Clean up so this Running job doesn't bleed into later shared-instance tests.
    s.complete(&job.id, None, None).unwrap();
}

fn test_reap_skips_and_flags_a_dispatch_awaiting_settle(s: &impl Storage) {
    // The operator-visible half of #845: a target that accepted a job and went
    // quiet must read as "accepted, never settled", not as "retried" — and
    // while it is still inside the deadline it asked for, it must not be
    // reaped at all.
    let q = "q-reap-awaiting-settle";
    let mut nj = make_job(q, "settle_task");
    nj.timeout_ms = 1;
    let job = s.enqueue(nj).unwrap();
    let t0 = now_millis();
    s.dequeue(q, t0, None).unwrap().unwrap();
    let epoch = s.claim_execution(&job.id, "settle-owner").unwrap().unwrap();

    // Past its own timeout, so the job-side predicate already selects it.
    let later = t0 + 1_000;
    s.await_settle(
        &job.id,
        "settle-owner",
        0,
        Some(epoch),
        later + 60_000,
        None,
    )
    .unwrap();

    assert!(
        !s.reap_stale_jobs(later, None)
            .unwrap()
            .iter()
            .any(|s| s.job.id == job.id),
        "a job inside the deadline its target extended must not be reaped"
    );

    // Once the settle deadline passes, it is stale — and flagged.
    let expired = later + 61_000;
    let found = s
        .reap_stale_jobs(expired, None)
        .unwrap()
        .into_iter()
        .find(|s| s.job.id == job.id)
        .expect("past its settle deadline the job is stale");
    assert!(
        found.awaiting_settle,
        "the reaper must be able to say the target accepted this and never came back"
    );

    s.complete(&job.id, None, None).unwrap();
}

/// Claim a fresh running job and return `(job_id, epoch)`.
///
/// The settle marker is fenced on `(owner, attempt, epoch)`, so every test
/// below needs a job that is genuinely `Running` at attempt 0 under a claim it
/// knows the epoch of — a bare id would be refused by the fence rather than by
/// the thing under test.
fn running_under_claim(s: &impl Storage, queue: &str, owner: &str) -> (String, i64) {
    let mut nj = make_job(queue, "settle_task");
    nj.timeout_ms = 60_000;
    let job = s.enqueue(nj).unwrap();
    s.dequeue(queue, now_millis(), None).unwrap().unwrap();
    let epoch = s
        .claim_execution(&job.id, owner)
        .unwrap()
        .expect("a fresh job's claim is unheld");
    (job.id, epoch)
}

fn test_settle_marker_round_trip(s: &impl Storage) {
    assert!(
        s.supports_settle(),
        "every shipped backend implements the settle marker; the default refuses"
    );

    let q = "q-settle-round-trip";
    let (job_id, epoch) = running_under_claim(s, q, "settle-owner");
    let deadline = now_millis() + 60_000;

    assert_eq!(
        s.await_settle(&job_id, "settle-owner", 0, Some(epoch), deadline, None)
            .unwrap(),
        Some(deadline),
        "accepting a dispatch records the deadline it will be waited on until"
    );

    // Monotonic. A retransmitted accept, or an extension that lost a race to a
    // longer one, must not shorten the window the target is working inside.
    assert_eq!(
        s.await_settle(
            &job_id,
            "settle-owner",
            0,
            Some(epoch),
            deadline - 30_000,
            None
        )
        .unwrap(),
        Some(deadline),
        "a deadline must never move backwards"
    );
    assert_eq!(
        s.await_settle(
            &job_id,
            "settle-owner",
            0,
            Some(epoch),
            deadline + 30_000,
            None
        )
        .unwrap(),
        Some(deadline + 30_000),
        "an extension moves it forward"
    );

    // Single-use: the marker is removed in the statement that tests it, so the
    // second caller — whichever of the three racers it is — gets nothing.
    assert_eq!(
        s.claim_settle(&job_id, SettleClaimant::Lease(epoch), None)
            .unwrap(),
        SettleGrant::Granted
    );
    assert_eq!(
        s.claim_settle(&job_id, SettleClaimant::Lease(epoch), None)
            .unwrap(),
        SettleGrant::Refused,
        "a settle is single-use for the attempt it names"
    );

    s.complete(&job_id, None, None).unwrap();
}

fn test_settle_marker_has_exactly_one_winner_under_contention(s: &impl Storage) {
    // The sequential tests prove replay is refused. They cannot prove the
    // property the whole design rests on — that two callbacks racing one
    // marker produce *one* winner — because a check-then-act bug passes every
    // sequential test there is. This one races them on purpose.
    let q = "q-settle-contended";
    let (job_id, epoch) = running_under_claim(s, q, "settle-owner");
    s.await_settle(
        &job_id,
        "settle-owner",
        0,
        Some(epoch),
        now_millis() + 60_000,
        None,
    )
    .unwrap();

    const RACERS: usize = 8;
    let start = std::sync::Barrier::new(RACERS);
    let granted = std::sync::atomic::AtomicUsize::new(0);

    std::thread::scope(|scope| {
        for _ in 0..RACERS {
            scope.spawn(|| {
                // Released together, so the claims genuinely overlap rather
                // than queueing behind each other's setup.
                start.wait();
                if matches!(
                    s.claim_settle(&job_id, SettleClaimant::Lease(epoch), None),
                    Ok(SettleGrant::Granted)
                ) {
                    granted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
            });
        }
    });

    assert_eq!(
        granted.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "exactly one of {RACERS} concurrent claims may win the marker"
    );

    s.complete(&job_id, None, None).unwrap();
}

fn test_settle_marker_refuses_a_lease_that_is_not_this_claims(s: &impl Storage) {
    let q = "q-settle-foreign-lease";
    let (job_id, epoch) = running_under_claim(s, q, "settle-owner");
    let deadline = now_millis() + 60_000;
    s.await_settle(&job_id, "settle-owner", 0, Some(epoch), deadline, None)
        .unwrap();

    // The strict fence, which is the whole point of the feature: a settle
    // arriving under a superseded dispatch's lease is refused rather than
    // applied. `epochs_agree` would have let the third case through.
    assert_eq!(
        s.claim_settle(&job_id, SettleClaimant::Lease(epoch ^ 1), None)
            .unwrap(),
        SettleGrant::Refused,
        "a lease naming another dispatch settles nothing"
    );

    // A superseded attempt cannot buy itself more time either. Attempt 1 when
    // the job is still at 0.
    assert_eq!(
        s.await_settle(&job_id, "settle-owner", 1, Some(epoch), deadline, None)
            .unwrap(),
        None,
        "the accept is fenced on the attempt, not just the owner"
    );
    assert_eq!(
        s.await_settle(&job_id, "someone-else", 0, Some(epoch), deadline, None)
            .unwrap(),
        None,
        "and on the owner"
    );

    // Still claimable by the real one — none of the refusals above consumed
    // anything.
    assert_eq!(
        s.claim_settle(&job_id, SettleClaimant::Lease(epoch), None)
            .unwrap(),
        SettleGrant::Granted
    );

    // Consuming the marker must leave the claim — and its epoch — in place:
    // the fence that runs when the settled result is applied has nothing else
    // to compare against.
    assert!(
        s.claim_execution(&job_id, "another").unwrap().is_none(),
        "a consumed settle must not take the execution claim with it"
    );

    s.complete(&job_id, None, None).unwrap();
}

fn test_settle_marker_waits_for_its_deadline(s: &impl Storage) {
    let q = "q-settle-deadline";
    let (job_id, epoch) = running_under_claim(s, q, "settle-owner");
    let deadline = now_millis() + 60_000;
    s.await_settle(&job_id, "settle-owner", 0, Some(epoch), deadline, None)
        .unwrap();

    // The scheduler is a claimant like any other, and it may only collect a
    // marker whose deadline has actually passed. Compared inside the statement
    // so an extension that committed a millisecond ago still wins.
    assert_eq!(
        s.claim_settle(&job_id, SettleClaimant::Expired { now: deadline - 1 }, None)
            .unwrap(),
        SettleGrant::Refused,
        "the reaper must not take a marker out from under a live target"
    );
    assert_eq!(
        s.claim_settle(&job_id, SettleClaimant::Expired { now: deadline }, None)
            .unwrap(),
        SettleGrant::Granted,
        "at the deadline the scheduler gives up"
    );

    s.complete(&job_id, None, None).unwrap();
}

fn test_settle_marker_survives_the_claim_purge(s: &impl Storage) {
    // The hole #845 turns from theoretical into the normal case: claims are
    // swept by age, and a swept claim takes its epoch with it. An absent epoch
    // is not a mismatch, so the fence would then authorize the one thing it
    // exists to refuse.
    let q = "q-settle-purge";
    let (awaiting, epoch) = running_under_claim(s, q, "purge-owner");
    let (ordinary, _) = running_under_claim(s, q, "purge-owner");

    let now = now_millis();
    s.await_settle(&awaiting, "purge-owner", 0, Some(epoch), now + 60_000, None)
        .unwrap();

    // Sweep with a cutoff far in the future: both claims are "old".
    s.purge_execution_claims(now + 3_600_000).unwrap();

    assert!(
        s.claim_execution(&ordinary, "another").unwrap().is_some(),
        "an ordinary aged claim is still collected"
    );
    assert!(
        s.claim_execution(&awaiting, "another").unwrap().is_none(),
        "a claim awaiting a settle outlives the age sweep — its epoch is the fence"
    );
    // And the marker it was protecting is intact.
    assert_eq!(
        s.claim_settle(&awaiting, SettleClaimant::Lease(epoch), None)
            .unwrap(),
        SettleGrant::Granted
    );

    s.complete(&awaiting, None, None).unwrap();
    s.complete(&ordinary, None, None).unwrap();
}

fn test_reclaim_execution(s: &impl Storage) {
    // Atomic claim transfer: only the rescuer expecting the current owner wins.
    let job = "reclaim-job-id";
    assert!(s.claim_execution(job, "dead").unwrap().is_some());
    assert!(s
        .reclaim_execution(job, "dead", "rescuer")
        .unwrap()
        .is_some());
    // A second rescuer still expecting "dead" loses — owner is now "rescuer".
    assert!(s.reclaim_execution(job, "dead", "other").unwrap().is_none());
    // The current owner can hand it on.
    assert!(s
        .reclaim_execution(job, "rescuer", "rescuer2")
        .unwrap()
        .is_some());
    // No claim row → no-op.
    assert!(s
        .reclaim_execution("no-such-claim", "x", "y")
        .unwrap()
        .is_none());
    s.complete_execution(job, None).unwrap();

    // Owners may contain ':' (e.g. "host:pid"). The numeric timestamp suffix is
    // split off from the LAST ':', so the full owner must match — a truncated
    // prefix must not.
    let colon_job = "reclaim-colon-job";
    assert!(s.claim_execution(colon_job, "host:42").unwrap().is_some());
    assert!(
        s.reclaim_execution(colon_job, "host", "x")
            .unwrap()
            .is_none(),
        "a truncated owner prefix must not match"
    );
    assert!(s
        .reclaim_execution(colon_job, "host:42", "rescuer")
        .unwrap()
        .is_some());
    s.complete_execution(colon_job, None).unwrap();
}

fn test_claim_execution_batch(s: &impl Storage) {
    // Batch claim returns one result per id, in order, and matches single-claim
    // semantics: an id already claimed (by any owner) comes back `None`.
    let pre = "batch-claim-pre"; // already held before the batch runs
    assert!(s.claim_execution(pre, "other").unwrap().is_some());

    let ids = ["batch-claim-a", pre, "batch-claim-c"];
    let won = s.claim_execution_batch(&ids, "batch-worker").unwrap();
    assert_eq!(
        won.iter().map(Option::is_some).collect::<Vec<_>>(),
        vec![true, false, true]
    );
    // Each won claim is its own claim, so each carries its own epoch — two jobs
    // claimed in one round trip are still two dispatches to tell apart.
    assert_ne!(
        won[0], won[2],
        "a batch must not hand two claims the same epoch"
    );

    // The won claims are now real: a follow-up single claim is rejected, and the
    // one we lost is still owned by the original holder (also rejected).
    assert!(s
        .claim_execution("batch-claim-a", "batch-worker")
        .unwrap()
        .is_none());
    assert!(s
        .claim_execution("batch-claim-c", "batch-worker")
        .unwrap()
        .is_none());
    assert!(s.claim_execution(pre, "batch-worker").unwrap().is_none());

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
        assert!(s.claim_execution(&job.id, "cb-worker").unwrap().is_some());
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
    assert!(s.claim_execution(&job.id, "hung-worker").unwrap().is_some());
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
    assert!(s.claim_execution(&job.id, "rescuer").unwrap().is_some());
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
    assert!(s.claim_execution(&job.id, "dead-worker").unwrap().is_some());

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
    assert!(s.claim_execution(&cjob.id, "host:7").unwrap().is_some());
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

/// Every path that creates a job must persist its dependency edges.
///
/// `NewJob::into_job()` folds `depends_on` into the `has_deps` flag, so a path
/// that writes the flag but skips the edges produces a job that reads as "no
/// dependencies" at dequeue and dispatches straight past its DAG — a silent
/// wrong answer, not an error. The five entry points share one insert helper on
/// the Diesel backends; this pins the contract per path so a future divergence
/// (or a Redis path that drifts) fails here.
fn test_every_enqueue_path_writes_dependency_rows(s: &impl Storage) {
    let q = "q-dep-rows-every-path";
    let anchor = s.enqueue(make_job(q, "dep_anchor")).unwrap();

    let dependent = |task: &str| {
        let mut job = make_job(q, task);
        job.depends_on = vec![anchor.id.clone()];
        job
    };
    let keyed = |task: &str, key: &str| {
        let mut job = dependent(task);
        job.unique_key = Some(key.to_string());
        job
    };

    let created = [
        ("enqueue", s.enqueue(dependent("dep_plain")).unwrap()),
        (
            "enqueue_batch",
            s.enqueue_batch(vec![dependent("dep_batch")])
                .unwrap()
                .remove(0),
        ),
        (
            "enqueue_unique",
            s.enqueue_unique(keyed("dep_unique", "dep-rows-uk-single"))
                .unwrap(),
        ),
        (
            "enqueue_unique_batch",
            s.enqueue_unique_batch(vec![keyed("dep_unique_batch", "dep-rows-uk-batch")])
                .unwrap()
                .remove(0),
        ),
        ("enqueue_debounced", {
            let mut job = dependent("dep_debounced");
            job.debounce_key = Some("dep-rows-debounce".to_string());
            s.enqueue_debounced(job, debounce_opts(5_000, 60_000))
                .unwrap()
        }),
    ];

    for (path, job) in &created {
        assert!(job.has_deps, "{path} lost the has_deps flag");
        assert_eq!(
            s.get_dependencies(&job.id, None).unwrap(),
            vec![anchor.id.clone()],
            "{path} did not persist its dependency edge",
        );
    }

    // The edges are also readable from the other side — a dependent written
    // without its row would be invisible to the completion fan-out that wakes
    // blocked jobs.
    let mut dependents = s.get_dependents(&anchor.id, None).unwrap();
    dependents.sort();
    let mut expected: Vec<String> = created.iter().map(|(_, job)| job.id.clone()).collect();
    expected.sort();
    assert_eq!(
        dependents, expected,
        "one enqueue path is missing from the DAG"
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

fn due_periodic_names(s: &impl Storage, namespace: Option<&str>) -> Vec<String> {
    s.get_due_periodic(now_millis(), namespace)
        .unwrap()
        .into_iter()
        .map(|p| p.name)
        .collect()
}

fn periodic_row(name: &str, namespace: Option<&str>) -> flexiq_core::NewPeriodicTask {
    flexiq_core::NewPeriodicTask {
        name: name.to_string(),
        task_name: "periodic-task".to_string(),
        cron_expr: "* * * * *".to_string(),
        args: None,
        kwargs: None,
        queue: "default".to_string(),
        enabled: true,
        next_run: now_millis() - 1_000,
        timezone: None,
        namespace: namespace.map(str::to_string),
    }
}

fn test_periodic_crud(s: &impl Storage) {
    s.register_periodic(&periodic_row("pc-a", None)).unwrap();
    s.register_periodic(&periodic_row("pc-b", None)).unwrap();

    // list_periodic returns every task registered in the namespace.
    let listed: Vec<String> = s
        .list_periodic(None)
        .unwrap()
        .into_iter()
        .map(|p| p.name)
        .collect();
    assert!(listed.contains(&"pc-a".to_string()) && listed.contains(&"pc-b".to_string()));

    // Pausing drops it from the due set but keeps it in the catalog.
    assert!(s.set_periodic_enabled("pc-a", false, None).unwrap());
    assert!(!due_periodic_names(s, None).contains(&"pc-a".to_string()));
    assert!(s
        .list_periodic(None)
        .unwrap()
        .iter()
        .any(|p| p.name == "pc-a"));

    // Resuming makes it due again.
    assert!(s.set_periodic_enabled("pc-a", true, None).unwrap());
    assert!(due_periodic_names(s, None).contains(&"pc-a".to_string()));

    // Toggling or deleting an unknown task reports "not found".
    assert!(!s.set_periodic_enabled("pc-missing", true, None).unwrap());

    assert!(s.delete_periodic("pc-a", None).unwrap());
    assert!(!s
        .list_periodic(None)
        .unwrap()
        .iter()
        .any(|p| p.name == "pc-a"));
    assert!(!s.delete_periodic("pc-a", None).unwrap());
}

/// Two namespaces registering the same schedule name are two schedules (#918).
///
/// Before this, `periodic_tasks` was keyed by `name` alone: the second
/// registration overwrote the first, a listing returned both tenants' rows, and
/// a delete or a pause reached a name the caller did not own.
fn test_periodic_is_namespace_scoped(s: &impl Storage) {
    let (a, b) = (Some("pns-tenant-a"), Some("pns-tenant-b"));

    // Same name in two namespaces, plus the default namespace's own.
    s.register_periodic(&periodic_row("nightly", a)).unwrap();
    s.register_periodic(&periodic_row("nightly", b)).unwrap();
    s.register_periodic(&periodic_row("nightly", None)).unwrap();

    // Neither registration overwrote the other, and a listing is one tenant's.
    // Filtered by name: an earlier case leaves its own rows in the default
    // namespace, and this is about isolation, not about the whole catalog.
    for ns in [a, b, None] {
        let listed: Vec<_> = s
            .list_periodic(ns)
            .unwrap()
            .into_iter()
            .filter(|p| p.name == "nightly")
            .collect();
        assert_eq!(
            listed.len(),
            1,
            "{ns:?} must see only its own schedule, saw {listed:?}"
        );
        assert_eq!(listed[0].namespace.as_deref(), ns);
    }

    // A pause reaches one namespace's row. The other two stay due.
    assert!(s.set_periodic_enabled("nightly", false, a).unwrap());
    assert!(!due_periodic_names(s, a).contains(&"nightly".to_string()));
    assert!(due_periodic_names(s, b).contains(&"nightly".to_string()));
    assert!(due_periodic_names(s, None).contains(&"nightly".to_string()));
    assert!(s.set_periodic_enabled("nightly", true, a).unwrap());

    // An unscoped due read is the engine's, and sees every namespace: one
    // scheduler with no namespace fires every tenant's schedules.
    let due_everywhere = s.get_due_periodic(now_millis(), None).unwrap();
    assert_eq!(
        due_everywhere
            .iter()
            .filter(|p| p.name == "nightly")
            .count(),
        3,
        "an unscoped scheduler must see all three, saw {due_everywhere:?}"
    );

    // A delete reaches one namespace's row and reports "not found" for a name
    // only another namespace holds.
    assert!(s.delete_periodic("nightly", a).unwrap());
    assert!(!s.delete_periodic("nightly", a).unwrap());
    for ns in [b, None] {
        assert!(
            s.list_periodic(ns)
                .unwrap()
                .iter()
                .any(|p| p.name == "nightly"),
            "deleting {a:?}'s row must leave {ns:?}'s alone"
        );
    }

    assert!(s.delete_periodic("nightly", b).unwrap());
    assert!(s.delete_periodic("nightly", None).unwrap());
}

/// Re-registering a schedule rewrites it in place and keeps `last_run`.
///
/// SQLite used to upsert with `REPLACE INTO`, which deletes the row before
/// inserting — so every worker restart forgot when the task last fired, while
/// Postgres' `ON CONFLICT … DO UPDATE` kept it. Both now run the same
/// UPDATE-else-INSERT.
fn test_periodic_re_registration_keeps_last_run(s: &impl Storage) {
    let fired_at = now_millis() - 5_000;
    s.register_periodic(&periodic_row("pc-rerun", None))
        .unwrap();
    s.update_periodic_schedule("pc-rerun", fired_at, now_millis() + 60_000, None)
        .unwrap();

    let mut changed = periodic_row("pc-rerun", None);
    changed.cron_expr = "0 * * * *".to_string();
    s.register_periodic(&changed).unwrap();

    let row = s
        .list_periodic(None)
        .unwrap()
        .into_iter()
        .find(|p| p.name == "pc-rerun")
        .expect("the re-registration must update the row, not move it");
    assert_eq!(row.cron_expr, "0 * * * *");
    assert_eq!(row.last_run, Some(fired_at));

    assert!(s.delete_periodic("pc-rerun", None).unwrap());
}

/// A declaration writes what it owns and nothing else (#919).
///
/// `register_periodic` replaces every column, so a worker writing its
/// code-declared schedules back at every start had to read the row first to
/// keep a deadline and a pause — and that read-then-write is a lost update:
/// a scheduler or an operator can commit in between. `declare_periodic` moves
/// the condition into the backend, where it is one statement.
fn test_periodic_declaration_preserves_operator_state(s: &impl Storage) {
    let stored = || {
        s.list_periodic(None)
            .unwrap()
            .into_iter()
            .find(|p| p.name == "pc-declared")
            .expect("the declaration is on record")
    };

    // No row yet, so the declaration is written whole.
    let mut declared = periodic_row("pc-declared", None);
    declared.next_run = now_millis() + 30_000;
    s.declare_periodic(&declared).unwrap();
    assert!(stored().enabled);
    assert_eq!(stored().next_run, declared.next_run);

    // Stand in for another worker's scheduler having fired it, and for an
    // operator having paused it.
    let fired_at = now_millis() - 5_000;
    let advanced = now_millis() + 600_000;
    s.update_periodic_schedule("pc-declared", fired_at, advanced, None)
        .unwrap();
    assert!(s.set_periodic_enabled("pc-declared", false, None).unwrap());

    // A restart re-declares the same schedule and moves none of the three.
    let mut restart = declared.clone();
    restart.next_run = now_millis() + 1_000;
    s.declare_periodic(&restart).unwrap();
    let row = stored();
    assert_eq!(
        row.next_run, advanced,
        "a restart must not reset a deadline the scheduler advanced"
    );
    assert!(
        !row.enabled,
        "a restart must not resume a task an operator paused"
    );
    assert_eq!(row.last_run, Some(fired_at));

    // A queue rename is not a schedule change, so the deadline still stands.
    let mut requeued = restart.clone();
    requeued.queue = "beats".to_string();
    s.declare_periodic(&requeued).unwrap();
    let row = stored();
    assert_eq!(row.queue, "beats");
    assert_eq!(row.next_run, advanced, "a queue rename keeps the deadline");

    // A changed cron expression does not: the stored deadline was computed
    // from a schedule that no longer exists.
    let mut rescheduled = requeued.clone();
    rescheduled.cron_expr = "0 * * * *".to_string();
    rescheduled.next_run = now_millis() + 3_000;
    s.declare_periodic(&rescheduled).unwrap();
    let row = stored();
    assert_eq!(row.cron_expr, "0 * * * *");
    assert_eq!(
        row.next_run, rescheduled.next_run,
        "a changed schedule replaces the deadline"
    );
    assert!(!row.enabled, "a schedule change is not a resume");
    assert_eq!(row.last_run, Some(fired_at));

    // So does a changed timezone — the nullable half of the same comparison,
    // where `timezone <> 'Europe/Stockholm'` is NULL on a row storing none.
    let mut zoned = rescheduled.clone();
    zoned.timezone = Some("Europe/Stockholm".to_string());
    zoned.next_run = now_millis() + 4_000;
    s.declare_periodic(&zoned).unwrap();
    let row = stored();
    assert_eq!(row.timezone.as_deref(), Some("Europe/Stockholm"));
    assert_eq!(
        row.next_run, zoned.next_run,
        "adding a timezone changes the schedule"
    );

    // And dropping it again, which is the other direction of that comparison.
    let mut unzoned = zoned.clone();
    unzoned.timezone = None;
    unzoned.next_run = now_millis() + 5_000;
    s.declare_periodic(&unzoned).unwrap();
    let row = stored();
    assert_eq!(row.timezone, None);
    assert_eq!(
        row.next_run, unzoned.next_run,
        "dropping a timezone changes the schedule"
    );

    // Identity is `(namespace, name)` here too: a tenant declaring the same
    // name inserts its own row rather than updating this one.
    let tenant = Some("pc-declare-tenant");
    let mut theirs = periodic_row("pc-declared", tenant);
    theirs.next_run = now_millis() + 900_000;
    s.declare_periodic(&theirs).unwrap();
    let tenant_rows = s.list_periodic(tenant).unwrap();
    assert_eq!(tenant_rows.len(), 1);
    assert_eq!(tenant_rows[0].next_run, theirs.next_run);
    assert_eq!(
        stored().next_run,
        unzoned.next_run,
        "a tenant's declaration must not reach the default namespace's row"
    );

    assert!(s.delete_periodic("pc-declared", tenant).unwrap());
    assert!(s.delete_periodic("pc-declared", None).unwrap());
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
    test_queue_throughput(s);
    test_fail(s);
    test_retry(s);
    test_reschedule(s);
    test_cancel_job(s);
    test_cancel_requested_among(s);
    test_stats(s);
    test_stats_by_queue_and_task(s);
    test_unique_key_dedup(s);
    test_unique_key_dedup_is_reported(s);
    test_enqueue_unique_validates_deps(s);
    test_unique_key_dedup_is_namespace_scoped(s);
    test_enqueue_batch(s);
    test_enqueue_unique_batch(s);
    test_enqueue_batch_dedup(s);
    test_enqueue_batch_dedup_validates_deps(s);
    test_every_enqueue_path_writes_dependency_rows(s);
    test_dead_letter_queue(s);
    test_dead_letter_by_task(s);
    test_dead_letter_purge_and_get_are_namespace_scoped(s);
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
    test_workers_are_namespace_scoped(s);
    test_worker_drain_request(s);
    test_pause_resume_queue(s);
    test_pause_resume_queue_is_namespace_scoped(s);
    test_periodic_crud(s);
    test_periodic_is_namespace_scoped(s);
    test_periodic_re_registration_keeps_last_run(s);
    test_periodic_declaration_preserves_operator_state(s);
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
    test_reap_skips_and_flags_a_dispatch_awaiting_settle(s);
    test_settle_marker_round_trip(s);
    test_settle_marker_has_exactly_one_winner_under_contention(s);
    test_settle_marker_refuses_a_lease_that_is_not_this_claims(s);
    test_settle_marker_waits_for_its_deadline(s);
    test_settle_marker_survives_the_claim_purge(s);
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
    test_enqueue_debounced_collapses_onto_a_full_queue(s);
    test_enqueue_debounced_refuses_to_open_a_window_on_a_full_queue(s);
    test_enqueue_debounced_counts_only_its_own_queue(s);
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
    test_the_epoch_separates_two_claims_of_one_attempt(s);
    test_reclaim_mints_a_new_epoch(s);
    test_a_step_commit_is_fenced_on_the_epoch(s);
    test_a_step_at_the_cap_round_trips_byte_for_byte(s);
    test_step_session_memoizes_across_attempts(s);
    test_step_idempotency_key_survives_a_dlq_retry(s);
    test_step_idempotency_key_survives_a_budget_exhausted_dlq_retry(s);
    test_dlq_retry_restores_the_jobs_own_metadata(s);
    test_step_session_refuses_a_changed_sequence(s);
    test_step_session_sleeps_by_ending_the_attempt(s);
    test_an_elapsed_sleep_wakes_the_job_immediately(s);
}

// ── Durable inline steps ─────────────────────────────────────────────

/// Enqueue, dequeue and claim one job, ready for a step write.
fn stepped_job(s: &impl Storage, queue: &str, owner: &str) -> flexiq_core::job::Job {
    let job = s.enqueue(make_job(queue, "stepped_task")).unwrap();
    s.dequeue(queue, now_millis() + 1000, None).unwrap();
    assert!(s.claim_execution(&job.id, owner).unwrap().is_some());
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
    s.record_step_result(step, owner, 0, None, &StepLimits::default(), None)
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
            None,
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
        None,
        &counted,
        None,
    )
    .unwrap();
    let err = s
        .record_step_result(
            &run_step(&job.id, 1, "noop#1", &[]),
            "w-cap",
            0,
            None,
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
        .unwrap()
        .is_some());

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
        s.claim_execution(&job.id, "w-attempt").unwrap().is_some(),
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
        s.sleep_job(&sleep, "w-sleep", 0, None, deadline, &limits, None)
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
    assert!(s.claim_execution(&job.id, "w-sleep").unwrap().is_some());
    assert_eq!(
        s.sleep_job(
            &sleep,
            "w-sleep",
            0,
            None,
            deadline + 3_600_000,
            &limits,
            None
        )
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
    assert!(s.claim_execution(&job.id, "w-sleep").unwrap().is_some());
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
    assert!(s
        .claim_execution(&resurrected, "w-dlq-key")
        .unwrap()
        .is_some());
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

    // A second death and resurrection still answers with the first run — and
    // it dies down a path that hands the DLQ *replacement* metadata, which
    // would otherwise drop the origin and restamp the intermediate job id.
    s.move_to_dlq(&retried, "boom again", Some(r#"{"killed":"budget"}"#))
        .unwrap();
    let dead = s
        .list_dead(1000, 0, None)
        .unwrap()
        .into_iter()
        .find(|entry| entry.original_job_id == resurrected)
        .expect("second dead entry");
    assert_eq!(
        dead.metadata
            .as_deref()
            .and_then(|m| serde_json::from_str::<serde_json::Value>(m).ok())
            .and_then(|m| m["killed"].as_str().map(str::to_string))
            .as_deref(),
        Some("budget"),
        "the caller's marker still wins the blob"
    );
    let twice = s.retry_dead(&dead.id, None).unwrap();
    let twice = s.get_job(&twice, None).unwrap().unwrap();
    assert_eq!(
        StepSession::load(s.clone(), &twice, "w-dlq-key", limits)
            .unwrap()
            .run_key(),
        job.id,
    );
}

/// The run survives a dead-letter whose metadata a caller replaced with
/// something that is not a JSON object.
///
/// `RETRY_BUDGET_EXHAUSTED` is the bare string `"retry_budget_exhausted"`,
/// matched byte-for-byte by three SDK suites, so it has no object an origin
/// could be merged into. While the run rode the metadata blob, a job already
/// resurrected once and then killed by the budget lost it: the next
/// `retry_dead` stamped the *intermediate* id and the operator's retry charged
/// the customer a second time. The origin rides a column now.
fn test_step_idempotency_key_survives_a_budget_exhausted_dlq_retry(s: &impl Storage) {
    let q = "q-step-budget-key";
    let job = stepped_job(s, q, "w-budget-key");
    let limits = StepLimits::default();
    let expected = format!("{}:charge#0", job.id);

    // 1. The charge is sent and the attempt dies before the step row commits.
    let minted = std::cell::RefCell::new(Vec::new());
    let mut first = StepSession::load(s.clone(), &job, "w-budget-key", limits).unwrap();
    first
        .run("charge", None, |key| {
            minted.borrow_mut().push(key.to_string());
            Err(QueueError::Other("connection reset".into()))
        })
        .unwrap_err();

    // 2. An operator retries it out of the DLQ, which mints a new job id.
    s.move_to_dlq(&job, "boom", None).unwrap();
    let resurrected = s.retry_dead(&newest_dead_for(s, &job.id).id, None).unwrap();

    // 3. The resurrection is killed by the retry budget, which replaces the
    //    metadata blob with a marker that cannot carry anything.
    let retried = s.get_job(&resurrected, None).unwrap().unwrap();
    s.move_to_dlq(
        &retried,
        "retry budget exhausted",
        Some(RETRY_BUDGET_EXHAUSTED),
    )
    .unwrap();
    let dead = newest_dead_for(s, &resurrected);
    assert_eq!(
        dead.metadata.as_deref(),
        Some(RETRY_BUDGET_EXHAUSTED),
        "the marker keeps its exact value — three SDK suites match it byte-for-byte"
    );
    assert_eq!(
        dead.origin_job_id.as_deref(),
        Some(job.id.as_str()),
        "the run rides a column the replacement cannot reach"
    );

    // 4. The second operator retry still belongs to the run that began at `job`,
    //    so the payment API sees the request it has already answered.
    let twice = s.retry_dead(&dead.id, None).unwrap();
    s.dequeue(q, now_millis() + 1000, None).unwrap();
    assert!(s.claim_execution(&twice, "w-budget-key").unwrap().is_some());
    let twice = s.get_job(&twice, None).unwrap().unwrap();
    let mut third = StepSession::load(s.clone(), &twice, "w-budget-key", limits).unwrap();
    assert_eq!(third.run_key(), job.id);
    third
        .run("charge", None, |key| {
            minted.borrow_mut().push(key.to_string());
            Ok(b"receipt".to_vec())
        })
        .unwrap();
    assert_eq!(
        minted.into_inner(),
        [expected.clone(), expected],
        "one idempotency key across two resurrections, so one charge"
    );
}

/// An operator's DLQ retry gives back the job the user enqueued, not one
/// stripped by the marker that recorded how it died.
///
/// `move_to_dlq`'s `metadata` argument *replaces* the blob, so every marker
/// path — `{"codel":true}`, `{"shed":"rate_limit"}`, `RETRY_BUDGET_EXHAUSTED` —
/// used to take the job's `tenant`/`user_id`/correlation keys with it. Worse
/// for the bare-string marker, which is not even an object: the resurrection
/// came back with an empty blob. The marker still owns the DLQ row's
/// `metadata`, which three SDK suites match on; the job's own rides
/// `job_metadata` now.
fn test_dlq_retry_restores_the_jobs_own_metadata(s: &impl Storage) {
    let q = "q-dlq-metadata-roundtrip";
    let enqueued = r#"{"tenant":"acme","user_id":"u1"}"#;

    // Both marker shapes: an object the old merge could survive, and the bare
    // string it could not.
    for (label, marker) in [
        ("object marker", r#"{"shed":"rate_limit"}"#),
        ("bare-string marker", RETRY_BUDGET_EXHAUSTED),
    ] {
        let mut new_job = make_job(q, "charge_card");
        new_job.metadata = Some(enqueued.to_string());
        let job = s.enqueue(new_job).unwrap();
        s.move_to_dlq(&job, "boom", Some(marker)).unwrap();

        let dead = newest_dead_for(s, &job.id);
        assert_eq!(
            dead.metadata.as_deref(),
            Some(marker),
            "{label}: the marker still owns the DLQ row's metadata"
        );

        let resurrected = s.retry_dead(&dead.id, None).unwrap();
        let retried = s.get_job(&resurrected, None).unwrap().unwrap();
        let meta: serde_json::Value =
            serde_json::from_str(retried.metadata.as_deref().expect("metadata")).unwrap();
        assert_eq!(meta["tenant"], "acme", "{label}");
        assert_eq!(meta["user_id"], "u1", "{label}");
        assert_eq!(
            meta["__dlq_retry_count"], 1,
            "{label}: the runtime's own keys are still stamped"
        );
        assert!(
            meta.get("shed").is_none(),
            "{label}: the marker describes that death, not the new job"
        );
    }
}

/// The newest dead-letter entry for a job that died.
fn newest_dead_for(s: &impl Storage, original_job_id: &str) -> DeadJob {
    s.list_dead(1000, 0, None)
        .unwrap()
        .into_iter()
        .find(|entry| entry.original_job_id == original_job_id)
        .expect("dead entry")
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
    assert!(s.claim_execution(&job.id, "w-sleeper").unwrap().is_some());
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
    assert!(s.claim_execution(&elapsed.id, "w-woken").unwrap().is_some());
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
        max_pending: None,
    }
}

/// The same window under an admission cap.
fn capped_opts(max_pending: i64) -> DebounceOptions {
    DebounceOptions {
        max_pending: Some(max_pending),
        ..debounce_opts(5_000, 60_000)
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
    assert!(s
        .claim_execution(&claimed.id, "w-debounce")
        .unwrap()
        .is_some());

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
    // A negative cap has no reading the backends agree on — a Diesel count can
    // never be under it, while the Redis script reserves a negative for the
    // uncapped case — so it is refused as a caller mistake before either sees
    // it. Asserted as `Config` rather than merely an error: left to the
    // backends this is `QueueFull` on one and a successful insert on the other,
    // and both of those are also `is_err()`-shaped answers to the wrong
    // question.
    let bad_cap = s
        .enqueue_debounced(debounced(q, "bad:user-1"), capped_opts(-1))
        .unwrap_err();
    assert!(
        matches!(bad_cap, QueueError::Config(_)),
        "a negative cap is a caller mistake, got {bad_cap:?}"
    );

    let written = s.list_jobs(None, Some(q), None, 10, 0, None).unwrap();
    assert!(written.is_empty(), "a rejected call must write nothing");
}

/// The whole point of the cap moving into the write: a queue sitting at its cap
/// still admits an enqueue that only slides the open window, because that
/// enqueue inserts nothing for the cap to be about.
fn test_enqueue_debounced_collapses_onto_a_full_queue(s: &impl Storage) {
    let q = "q-debounce-cap-slide";
    let opened = s
        .enqueue_debounced(debounced(q, "capslide:user-1"), capped_opts(4))
        .unwrap();
    // Fill the rest of the cap with plain jobs so the window's own row is not
    // the only thing standing between the queue and its limit.
    for _ in 0..3 {
        s.enqueue(make_job(q, "filler")).unwrap();
    }
    assert_eq!(s.count_pending_by_queue(q).unwrap(), 4);

    let slid = s
        .enqueue_debounced(debounced(q, "capslide:user-1"), capped_opts(4))
        .unwrap();
    assert_eq!(slid.id, opened.id, "a full queue still takes a slide");
    assert_eq!(s.count_pending_by_queue(q).unwrap(), 4);
}

/// With no window open there is a row to insert, so the same full queue refuses
/// it — and refuses it having written nothing.
fn test_enqueue_debounced_refuses_to_open_a_window_on_a_full_queue(s: &impl Storage) {
    let q = "q-debounce-cap-insert";
    for _ in 0..2 {
        s.enqueue(make_job(q, "filler")).unwrap();
    }

    let err = s
        .enqueue_debounced(debounced(q, "capinsert:user-1"), capped_opts(2))
        .unwrap_err();
    match err {
        QueueError::QueueFull {
            queue,
            pending,
            cap,
        } => {
            assert_eq!(queue, q);
            assert_eq!(pending, 2);
            assert_eq!(cap, 2);
        }
        other => panic!("expected QueueFull, got {other:?}"),
    }
    assert_eq!(
        s.count_pending_by_queue(q).unwrap(),
        2,
        "a refused insert must leave the queue exactly as it found it"
    );

    // Raising the cap by one admits the window that was just refused, so the
    // refusal was the cap and not the debounce write failing for its own reason.
    let opened = s
        .enqueue_debounced(debounced(q, "capinsert:user-1"), capped_opts(3))
        .unwrap();
    assert_eq!(s.get_job(&opened.id, None).unwrap().unwrap().queue, q);
}

/// The cap is per queue, and an uncapped call counts nothing at all.
fn test_enqueue_debounced_counts_only_its_own_queue(s: &impl Storage) {
    let noisy = "q-debounce-cap-noisy";
    let quiet = "q-debounce-cap-quiet";
    for _ in 0..3 {
        s.enqueue(make_job(noisy, "filler")).unwrap();
    }

    s.enqueue_debounced(debounced(quiet, "capquiet:user-1"), capped_opts(1))
        .unwrap();
    assert_eq!(s.count_pending_by_queue(quiet).unwrap(), 1);

    // Same queue, now over its cap, but the call carries none.
    s.enqueue_debounced(
        debounced(quiet, "capquiet:user-2"),
        debounce_opts(5_000, 60_000),
    )
    .unwrap();
    assert_eq!(s.count_pending_by_queue(quiet).unwrap(), 2);
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
    redis_namespaced_debounce_claim_clears_its_index(&storage);
    redis_debounce_coalesces_onto_a_plainly_enqueued_job(&storage);
    redis_debounce_slides_an_empty_payload(&storage);
    redis_purge_metrics_drains_across_batches(&storage);
    redis_prunes_a_legacy_due_member(&storage);
    redis_claim_preserves_empty_payload(&storage);
    redis_claim_respects_priority_then_schedule_order(&storage);
    redis_claim_skips_future_and_foreign_namespace(&storage);
    redis_claim_waits_for_dependencies(&storage);
    redis_claim_does_not_starve_ready_dependents(&storage);
    redis_select_and_claim_is_one_round_trip(&storage);

    // The ready-notify fold only exists on a `push-dispatch` build (#4).
    #[cfg(feature = "push-dispatch")]
    {
        redis_enqueue_ready_job_publishes_once(&storage);
        redis_enqueue_future_job_does_not_publish(&storage);
        redis_enqueue_batch_publishes_once_per_ready_queue(&storage);
        redis_enqueue_unique_publishes_ready_job_once(&storage);
    }
}

/// A schedule registered before #918 lives at `periodic:<name>` and is a bare
/// name in the due index, not a key. It must never fire — firing it would write
/// the advance to the *new* key and leave the old `next_run` to fire again on
/// every tick — and it must not be handed back forever either: nothing advances
/// its score, so the due read drops it from the index as it finds it.
#[cfg(feature = "redis")]
fn redis_prunes_a_legacy_due_member(s: &flexiq_core::RedisStorage) {
    use redis::Commands;

    let legacy_key = format!("{}periodic:pre918", s.prefix());
    let due_key = format!("{}periodic:due", s.prefix());
    // Written by a pre-#918 binary: no `namespace` field, and the due member is
    // the bare name.
    let document = r#"{"name":"pre918","task_name":"legacy_task","cron_expr":"* * * * *","args":null,"kwargs":null,"queue":"default","enabled":true,"last_run":null,"next_run":0,"timezone":null}"#;

    let mut conn = s.conn().unwrap();
    let _: () = conn.set(&legacy_key, document).unwrap();
    let _: () = conn.zadd(&due_key, "pre918", 0.0).unwrap();

    let due = s.get_due_periodic(now_millis(), None).unwrap();
    assert!(
        !due.iter().any(|p| p.name == "pre918"),
        "a legacy row must not fire: {due:?}"
    );

    let score: Option<f64> = conn.zscore(&due_key, "pre918").unwrap();
    assert!(
        score.is_none(),
        "the legacy member must be pruned from the due index"
    );

    // The document itself is left where it is — an operator's to delete — but
    // no listing owns up to it, because its key is not the one its identity
    // would compute.
    let listed = s.list_periodic(None).unwrap();
    assert!(
        !listed.iter().any(|p| p.name == "pre918"),
        "a legacy row must not be listed: {listed:?}"
    );

    let _: () = conn.del(&legacy_key).unwrap();
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
    let removed = s.purge_dead(now_millis() + 3_600_000, None).unwrap();
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
    // `-` is the default-namespace segment `unique_key_key` writes (mirrors
    // `debounce_index_key`'s scheme) — `a` was enqueued with no namespace.
    let ukey = rkey(s, &["jobs", "unique", "-", shared]);
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

/// Members of the debounce index of a default-namespace key.
#[cfg(feature = "redis")]
fn redis_debounce_index_size(s: &flexiq_core::RedisStorage, debounce_key: &str) -> i64 {
    redis_debounce_index_size_in(s, None, debounce_key)
}

/// Members of the debounce index of `(namespace, debounce_key)`. The index is
/// an implementation detail of the Redis backend, so the key is rebuilt here
/// from the same shape `debounce_index_key` writes: `-` for the default
/// namespace, `<len>:<ns>` otherwise.
#[cfg(feature = "redis")]
fn redis_debounce_index_size_in(
    s: &flexiq_core::RedisStorage,
    namespace: Option<&str>,
    debounce_key: &str,
) -> i64 {
    let segment = match namespace {
        Some(ns) => format!("{}:{ns}", ns.len()),
        None => "-".to_string(),
    };
    let mut conn = s.conn().unwrap();
    redis::cmd("ZCARD")
        .arg(format!(
            "{}jobs:debounce:{segment}:{debounce_key}",
            s.prefix()
        ))
        .query(&mut conn)
        .unwrap()
}

/// The claim script drops a namespaced job's debounce entry too: it rebuilds
/// the index key from the job's namespace in Lua, which must match
/// `namespace_segment` byte for byte or the entry would outlive the claim.
#[cfg(feature = "redis")]
fn redis_namespaced_debounce_claim_clears_its_index(s: &flexiq_core::RedisStorage) {
    let q = "q-redis-debounce-ns";
    let ns = Some("t");
    let in_ns = |key: &str| {
        let mut job = debounced(q, key);
        job.namespace = ns.map(str::to_string);
        job
    };

    let single = s
        .enqueue_debounced(in_ns("ns-single"), debounce_opts(5_000, 60_000))
        .unwrap();
    assert_eq!(redis_debounce_index_size_in(s, ns, "ns-single"), 1);
    let claimed = s.dequeue(q, now_millis() + 10_000, ns).unwrap().unwrap();
    assert_eq!(claimed.id, single.id);
    assert_eq!(
        redis_debounce_index_size_in(s, ns, "ns-single"),
        0,
        "dequeue must close a namespaced window"
    );

    let batched = s
        .enqueue_debounced(in_ns("ns-batch"), debounce_opts(5_000, 60_000))
        .unwrap();
    assert_eq!(redis_debounce_index_size_in(s, ns, "ns-batch"), 1);
    let claimed = s.dequeue_batch(q, now_millis() + 10_000, ns, 8).unwrap();
    assert_eq!(
        claimed.iter().map(|j| j.id.as_str()).collect::<Vec<_>>(),
        vec![batched.id.as_str()]
    );
    assert_eq!(
        redis_debounce_index_size_in(s, ns, "ns-batch"),
        0,
        "dequeue_batch must close a namespaced window"
    );
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

/// The claim script patches the stored document by token swap rather than a
/// `cjson` round trip, so an empty payload (`[]`) must survive the claim.
#[cfg(feature = "redis")]
fn redis_claim_preserves_empty_payload(s: &flexiq_core::RedisStorage) {
    let q = "q-redis-claim-empty-payload";
    drain_queue(s, q);
    let mut job = make_job(q, "claim_empty_payload");
    job.payload = Vec::new();
    let job = s.enqueue(job).unwrap();

    let now = now_millis() + 1_000;
    let claimed = s.dequeue(q, now, None).unwrap().unwrap();
    assert_eq!(claimed.id, job.id);
    assert!(claimed.payload.is_empty());
    assert_eq!(claimed.started_at, Some(now));

    let stored = s.get_job(&job.id, None).unwrap().unwrap();
    assert!(
        stored.payload.is_empty(),
        "the claimed document must decode"
    );
    assert_eq!(stored.status, JobStatus::Running);
    assert_eq!(stored.started_at, Some(now));
}

/// One batch claim returns jobs in dispatch order: higher priority first, then
/// earlier `scheduled_at`, whatever order they were enqueued in.
#[cfg(feature = "redis")]
fn redis_claim_respects_priority_then_schedule_order(s: &flexiq_core::RedisStorage) {
    let q = "q-redis-claim-order";
    drain_queue(s, q);
    let base = now_millis() - 10_000;
    let enqueue = |priority: i32, scheduled_at: i64| {
        let mut job = make_job(q, "claim_order");
        job.priority = priority;
        job.scheduled_at = scheduled_at;
        s.enqueue(job).unwrap().id
    };
    let low = enqueue(0, base);
    let high_late = enqueue(5, base + 10);
    let mid_early = enqueue(1, base - 5);
    let high_early = enqueue(5, base);

    let claimed: Vec<String> = s
        .dequeue_batch(q, now_millis() + 1_000, None, 4)
        .unwrap()
        .into_iter()
        .map(|job| job.id)
        .collect();
    assert_eq!(claimed, vec![high_early, high_late, mid_early, low]);
}

/// Jobs that are not yet due, or belong to another namespace, stay Pending: a
/// `None` namespace claims only jobs without one, and `Some(ns)` only its own.
#[cfg(feature = "redis")]
fn redis_claim_skips_future_and_foreign_namespace(s: &flexiq_core::RedisStorage) {
    let q = "q-redis-claim-filters";
    drain_queue(s, q);
    let now = now_millis() + 1_000;
    let namespaced = |ns: &str| {
        let mut job = make_job(q, "claim_filters");
        job.namespace = Some(ns.to_string());
        s.enqueue(job).unwrap().id
    };

    let mut future = make_job(q, "claim_filters");
    future.scheduled_at = now + 60_000;
    let future = s.enqueue(future).unwrap().id;
    let tenant_a = namespaced("tenant-a");
    let tenant_b = namespaced("tenant-b");
    let plain = s.enqueue(make_job(q, "claim_filters")).unwrap().id;

    let claimed = s.dequeue_batch(q, now, None, 10).unwrap();
    assert_eq!(
        claimed.iter().map(|j| &j.id).collect::<Vec<_>>(),
        vec![&plain],
        "None claims only the due, un-namespaced job"
    );

    let other_plain = s.enqueue(make_job(q, "claim_filters")).unwrap().id;
    let claimed = s.dequeue_batch(q, now, Some("tenant-a"), 10).unwrap();
    assert_eq!(
        claimed.iter().map(|j| &j.id).collect::<Vec<_>>(),
        vec![&tenant_a],
        "Some(ns) claims only its own namespace"
    );

    let pending = |id: &str, ns: Option<&str>| s.get_job(id, ns).unwrap().unwrap().status;
    assert_eq!(pending(&future, None), JobStatus::Pending);
    assert_eq!(pending(&other_plain, None), JobStatus::Pending);
    assert_eq!(pending(&tenant_b, Some("tenant-b")), JobStatus::Pending);
}

/// A job with an incomplete dependency is left Pending; once the dependency
/// completes (and is archived) the next dequeue claims it.
#[cfg(feature = "redis")]
fn redis_claim_waits_for_dependencies(s: &flexiq_core::RedisStorage) {
    let q = "q-redis-claim-deps";
    drain_queue(s, q);
    let parent = s.enqueue(make_job(q, "claim_parent")).unwrap();
    let mut child = make_job(q, "claim_child");
    child.depends_on = vec![parent.id.clone()];
    let child = s.enqueue(child).unwrap();

    let now = now_millis() + 1_000;
    let claimed = s.dequeue_batch(q, now, None, 10).unwrap();
    assert_eq!(
        claimed.iter().map(|j| &j.id).collect::<Vec<_>>(),
        vec![&parent.id],
        "the child waits for its parent"
    );
    assert_eq!(
        s.get_job(&child.id, None).unwrap().unwrap().status,
        JobStatus::Pending
    );

    s.complete(&parent.id, None, None).unwrap();
    let claimed = s.dequeue_batch(q, now, None, 10).unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].id, child.id);
    assert_eq!(claimed[0].status, JobStatus::Running);
    assert_eq!(claimed[0].started_at, Some(now));
}

/// A ready dependent is claimed in its score-order turn, even with at least
/// `max` ready plain jobs behind it — the batch budget must not starve it.
#[cfg(feature = "redis")]
fn redis_claim_does_not_starve_ready_dependents(s: &flexiq_core::RedisStorage) {
    let q = "q-redis-claim-deps-order";
    drain_queue(s, q);
    let now = now_millis() + 1_000;
    let parent = s.enqueue(make_job(q, "claim_order_parent")).unwrap();
    assert_eq!(s.dequeue(q, now, None).unwrap().unwrap().id, parent.id);
    s.complete(&parent.id, None, None).unwrap();

    let base = now_millis() - 10_000;
    let mut child = make_job(q, "claim_order_child");
    child.depends_on = vec![parent.id.clone()];
    child.scheduled_at = base;
    let child = s.enqueue(child).unwrap();
    assert!(child.has_deps);
    let plain: Vec<String> = (1..=4)
        .map(|i| {
            let mut job = make_job(q, "claim_order_plain");
            job.scheduled_at = base + i;
            s.enqueue(job).unwrap().id
        })
        .collect();

    let claimed: Vec<String> = s
        .dequeue_batch(q, now, None, 4)
        .unwrap()
        .into_iter()
        .map(|job| job.id)
        .collect();
    assert_eq!(
        claimed,
        vec![
            child.id.clone(),
            plain[0].clone(),
            plain[1].clone(),
            plain[2].clone()
        ]
    );
}

/// `calls` for one command in an `INFO commandstats` reply, or 0 before its
/// first call.
#[cfg(feature = "redis")]
fn redis_command_calls(info: &str, command: &str) -> u64 {
    let prefix = format!("cmdstat_{command}:calls=");
    info.lines()
        .find_map(|line| line.strip_prefix(&prefix))
        .and_then(|rest| rest.split(',').next())
        .map_or(0, |calls| calls.parse().unwrap())
}

/// `INFO commandstats` taken after every command sent so far. A hosted Redis
/// may serve a snapshot refreshed only every few seconds, so send an `ECHO`
/// marker and wait for a snapshot that counts it.
#[cfg(feature = "redis")]
fn redis_fresh_commandstats(conn: &mut redis::Connection) -> String {
    let read = |conn: &mut redis::Connection| -> String {
        redis::cmd("INFO").arg("commandstats").query(conn).unwrap()
    };
    let marked = redis_command_calls(&read(conn), "echo") + 1;
    let _: String = redis::cmd("ECHO").arg("marker").query(conn).unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while std::time::Instant::now() < deadline {
        let info = read(conn);
        if redis_command_calls(&info, "echo") >= marked {
            return info;
        }
        std::thread::sleep(std::time::Duration::from_millis(200));
    }
    panic!("INFO commandstats never counted the ECHO marker");
}

/// A batch claim is one script call: exactly one `EVALSHA`, and no client-side
/// `ZRANGEBYSCORE` or `MGET` beside it.
#[cfg(feature = "redis")]
fn redis_select_and_claim_is_one_round_trip(s: &flexiq_core::RedisStorage) {
    let q = "q-redis-claim-round-trip";
    drain_queue(s, q);
    let now = now_millis() + 1_000;
    // The first invocation of a script may be EVALSHA → NOSCRIPT → SCRIPT LOAD.
    assert!(s.dequeue_batch(q, now, None, 4).unwrap().is_empty());
    for _ in 0..4 {
        s.enqueue(make_job(q, "claim_round_trip")).unwrap();
    }

    let mut stats = s.conn().unwrap();
    let commands = ["zrangebyscore", "mget", "evalsha", "eval"];
    let before = redis_fresh_commandstats(&mut stats);
    assert_eq!(s.dequeue_batch(q, now, None, 4).unwrap().len(), 4);
    let after = redis_fresh_commandstats(&mut stats);
    let grown: Vec<u64> = commands
        .iter()
        .map(|c| redis_command_calls(&after, c) - redis_command_calls(&before, c))
        .collect();

    // Commands a script runs are counted too, so the scripted ZRANGEBYSCORE
    // shows as 1; a client-side scan would add a second, an MGET and 4 claims.
    assert_eq!(grown[0], 1, "zrangebyscore");
    assert_eq!(grown[1], 0, "mget");
    assert_eq!(grown[2], 1, "evalsha");
    assert_eq!(grown[3], 0, "eval");
}

/// The ready-job channel for `queue` under `s`'s prefix, computed the same
/// way `RedisStorage::notify_channel` builds it (`notify_channel` itself is
/// `pub(crate)`, so an external integration test rebuilds it from the public
/// `prefix()` instead of reaching into the crate).
#[cfg(all(feature = "redis", feature = "push-dispatch"))]
fn redis_notify_channel(s: &flexiq_core::RedisStorage, queue: &str) -> String {
    format!("{}notify:{}", s.prefix(), queue)
}

/// Subscribe to `channel` on a dedicated connection, run `action`, then count
/// every message that arrives within `timeout` after the subscribe
/// acknowledgement (so nothing `action` publishes can race the SUBSCRIBE).
/// Reading stops at the first timed-out `get_message`, which — on a
/// `push-dispatch` build — is exactly the round trip a fold-in must not add:
/// `INFO commandstats` counts a scripted or pipelined command the same as a
/// standalone one, so it cannot show a saved round trip; this instead proves
/// the *count* of publishes is exactly what folding promises (no drop, no
/// double-publish from `notify_if_ready`'s Redis arm still firing).
#[cfg(all(feature = "redis", feature = "push-dispatch"))]
fn redis_publish_count(
    client: &redis::Client,
    channel: &str,
    timeout: std::time::Duration,
    action: impl FnOnce(),
) -> usize {
    let mut conn = client.get_connection().unwrap();
    let mut pubsub = conn.as_pubsub();
    pubsub.subscribe(channel).unwrap();
    action();
    pubsub.set_read_timeout(Some(timeout)).unwrap();
    let mut count = 0;
    while pubsub.get_message().is_ok() {
        count += 1;
    }
    count
}

/// #4: a ready `enqueue` publishes exactly once through the full
/// `StorageBackend` wrapper — proving both that the pipeline fold fires and
/// that `notify_if_ready`'s Redis arm (now a no-op) does not also fire and
/// double-publish.
#[cfg(all(feature = "redis", feature = "push-dispatch"))]
fn redis_enqueue_ready_job_publishes_once(s: &flexiq_core::RedisStorage) {
    use flexiq_core::storage::{Storage, StorageBackend};
    let q = "q-notify-ready-once";
    drain_queue(s, q);
    let channel = redis_notify_channel(s, q);
    let backend = StorageBackend::Redis(s.clone());
    let count = redis_publish_count(
        s.client(),
        &channel,
        std::time::Duration::from_secs(5),
        || {
            backend.enqueue(make_job(q, "notify_ready")).unwrap();
        },
    );
    assert_eq!(count, 1, "a ready enqueue must publish exactly once");
}

/// #4: a future-scheduled `enqueue` never publishes — `notify_if_ready`'s own
/// `scheduled_at > now` guard, mirrored inside the folded pipeline.
#[cfg(all(feature = "redis", feature = "push-dispatch"))]
fn redis_enqueue_future_job_does_not_publish(s: &flexiq_core::RedisStorage) {
    use flexiq_core::storage::{Storage, StorageBackend};
    let q = "q-notify-future";
    drain_queue(s, q);
    let channel = redis_notify_channel(s, q);
    let backend = StorageBackend::Redis(s.clone());
    let mut job = make_job(q, "notify_future");
    job.scheduled_at = now_millis() + 60_000;
    let count = redis_publish_count(
        s.client(),
        &channel,
        std::time::Duration::from_secs(2),
        || {
            backend.enqueue(job).unwrap();
        },
    );
    assert_eq!(count, 0, "a future-scheduled enqueue must not publish");
}

/// #4: `enqueue_batch` publishes once per distinct *ready* queue, not once
/// per job — a queue holding only a future job stays silent even though the
/// batch also touches a queue with a ready job.
#[cfg(all(feature = "redis", feature = "push-dispatch"))]
fn redis_enqueue_batch_publishes_once_per_ready_queue(s: &flexiq_core::RedisStorage) {
    use flexiq_core::storage::{Storage, StorageBackend};
    let q_ready = "q-notify-batch-ready";
    let q_future = "q-notify-batch-future";
    drain_queue(s, q_ready);
    drain_queue(s, q_future);
    let chan_ready = redis_notify_channel(s, q_ready);
    let chan_future = redis_notify_channel(s, q_future);
    let backend = StorageBackend::Redis(s.clone());

    let mut conn = s.client().get_connection().unwrap();
    let mut pubsub = conn.as_pubsub();
    pubsub
        .subscribe(vec![chan_ready.clone(), chan_future.clone()])
        .unwrap();

    let ready_job = make_job(q_ready, "notify_batch_ready");
    let mut ready_queue_future_job = make_job(q_ready, "notify_batch_ready_future");
    ready_queue_future_job.scheduled_at = now_millis() + 60_000;
    let mut future_job = make_job(q_future, "notify_batch_future");
    future_job.scheduled_at = now_millis() + 60_000;

    backend
        .enqueue_batch(vec![ready_job, ready_queue_future_job, future_job])
        .unwrap();

    pubsub
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .unwrap();
    let (mut ready_count, mut future_count) = (0, 0);
    while let Ok(msg) = pubsub.get_message() {
        match msg.get_channel_name() {
            c if c == chan_ready => ready_count += 1,
            c if c == chan_future => future_count += 1,
            other => panic!("unexpected channel: {other}"),
        }
    }
    assert_eq!(
        ready_count, 1,
        "one publish for the whole batch on the ready queue"
    );
    assert_eq!(
        future_count, 0,
        "a queue with only a future job stays silent"
    );
}

/// #4: `enqueue_unique`'s store script (the Lua path carrying the trailing,
/// optional notify ARGV) also publishes exactly once for a fresh, ready
/// insert — sanity on the ARGV-position surgery beside the plain paths above.
#[cfg(all(feature = "redis", feature = "push-dispatch"))]
fn redis_enqueue_unique_publishes_ready_job_once(s: &flexiq_core::RedisStorage) {
    use flexiq_core::storage::{Storage, StorageBackend};
    let q = "q-notify-unique-once";
    drain_queue(s, q);
    let channel = redis_notify_channel(s, q);
    let backend = StorageBackend::Redis(s.clone());
    let mut job = make_job(q, "notify_unique");
    job.unique_key = Some(format!("notify-unique-{}", uuid::Uuid::now_v7()));
    let count = redis_publish_count(
        s.client(),
        &channel,
        std::time::Duration::from_secs(5),
        || {
            backend.enqueue_unique(job).unwrap();
        },
    );
    assert_eq!(
        count, 1,
        "a ready enqueue_unique insert must publish exactly once"
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
        s.authorize_attempt(&job.id, "w-authorize", 0, None, None)
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
        s.authorize_attempt(&job.id, "w-authorize", 0, None, None)
            .unwrap(),
        AttemptFence::Superseded
    );
}

/// The epoch separates two claims one owner won at one attempt.
///
/// `requeue_stuck` is what produces that pair in the wild: it returns a
/// `Running` job to `Pending` and deletes its claim without touching
/// `retry_count`, so the next dispatch carries the identical `(owner,
/// attempt)`. Before the epoch, the stalled executor's late result authorized
/// against the *new* attempt's claim and wrote over it.
fn test_the_epoch_separates_two_claims_of_one_attempt(s: &impl Storage) {
    use flexiq_core::storage::records::AttemptFence;

    // Claimed here rather than through `stepped_job`, because the epoch is
    // deliberately not readable back — the only way to hold one is to be the
    // caller that won the claim.
    let job = s
        .enqueue(make_job("q-steps-epoch", "stepped_task"))
        .unwrap();
    s.dequeue("q-steps-epoch", now_millis() + 1000, None)
        .unwrap();
    let stalled = s
        .claim_execution(&job.id, "w-epoch")
        .unwrap()
        .expect("the first claim wins");

    assert_eq!(
        s.authorize_attempt(&job.id, "w-epoch", 0, Some(stalled), None)
            .unwrap(),
        AttemptFence::Authorized,
        "the claim it was dispatched under still speaks for the job"
    );

    // The dashboard's requeue button, then the poller re-claiming it.
    assert!(s.requeue_stuck(&job.id, now_millis()).unwrap());
    s.dequeue("q-steps-epoch", now_millis() + 1000, None)
        .unwrap();
    let current = s
        .claim_execution(&job.id, "w-epoch")
        .unwrap()
        .expect("the re-claim wins, the old claim having been deleted");

    assert_ne!(
        current, stalled,
        "a re-claim must not reuse the epoch the deleted one held"
    );
    assert_eq!(
        s.authorize_attempt(&job.id, "w-epoch", 0, Some(current), None)
            .unwrap(),
        AttemptFence::Authorized,
        "the live dispatch still authorizes"
    );
    assert_eq!(
        s.authorize_attempt(&job.id, "w-epoch", 0, Some(stalled), None)
            .unwrap(),
        AttemptFence::Superseded,
        "the stalled attempt's result must not settle the job the new one holds"
    );
    // Same owner, same attempt — so nothing but the epoch could have told them
    // apart. Stated as an assertion rather than a comment, because a change
    // that made the requeue bump `retry_count` would make this test pass for
    // the wrong reason.
    assert_eq!(s.get_job(&job.id, None).unwrap().unwrap().retry_count, 0);

    // A caller holding no epoch is fenced as it was before the column existed:
    // the give-up an executor without `CAP_LEASE` accepts.
    assert_eq!(
        s.authorize_attempt(&job.id, "w-epoch", 0, None, None)
            .unwrap(),
        AttemptFence::Authorized
    );
}

/// A reclaim mints a new epoch, so the dead owner's executor — which may still
/// be on its way to reporting — cannot authorize against the rescuer's claim.
fn test_reclaim_mints_a_new_epoch(s: &impl Storage) {
    let job = "reclaim-epoch-job";
    let first = s.claim_execution(job, "dead").unwrap().expect("claimed");
    let rescued = s
        .reclaim_execution(job, "dead", "rescuer")
        .unwrap()
        .expect("the transfer wins");
    assert_ne!(
        first, rescued,
        "a reclaim must move the epoch with the owner"
    );
    assert!(
        s.reclaim_execution(job, "dead", "other").unwrap().is_none(),
        "a rescuer expecting the old owner still loses"
    );
    s.complete_execution(job, None).unwrap();
}

/// A step commit is fenced on the epoch too, not only on `(owner, attempt)`.
///
/// The same pair the result path faces: an executor still running the stalled
/// attempt would otherwise write into the live attempt's step sequence.
fn test_a_step_commit_is_fenced_on_the_epoch(s: &impl Storage) {
    let job = s
        .enqueue(make_job("q-steps-epoch-commit", "stepped_task"))
        .unwrap();
    s.dequeue("q-steps-epoch-commit", now_millis() + 1000, None)
        .unwrap();
    let stalled = s
        .claim_execution(&job.id, "w-epoch-step")
        .unwrap()
        .expect("the first claim wins");

    assert!(s.requeue_stuck(&job.id, now_millis()).unwrap());
    s.dequeue("q-steps-epoch-commit", now_millis() + 1000, None)
        .unwrap();
    let current = s
        .claim_execution(&job.id, "w-epoch-step")
        .unwrap()
        .expect("the re-claim wins");

    let step = run_step(&job.id, 0, "charge#0", b"receipt");
    assert!(
        matches!(
            s.record_step_result(
                &step,
                "w-epoch-step",
                0,
                Some(stalled),
                &StepLimits::default(),
                None,
            ),
            Err(flexiq_core::QueueError::ClaimLost(_))
        ),
        "the stalled attempt must not commit into the live attempt's sequence"
    );
    assert!(
        s.get_job_steps(&job.id, None).unwrap().is_empty(),
        "a refused commit writes nothing"
    );
    assert_eq!(
        s.record_step_result(
            &step,
            "w-epoch-step",
            0,
            Some(current),
            &StepLimits::default(),
            None,
        )
        .unwrap(),
        StepCommit::Committed,
        "the live dispatch still commits"
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
            None,
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
            None,
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
        s.sleep_job(&sleep, "w-elapsed", 0, None, deadline, &limits, None)
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
    assert!(s.claim_execution(&job.id, "w-elapsed").unwrap().is_some());
    assert_eq!(
        s.sleep_job(
            &sleep,
            "w-elapsed",
            0,
            None,
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
