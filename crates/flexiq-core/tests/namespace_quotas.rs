//! Namespace quotas at enqueue (#841): depth and rate, admitted in the
//! `StorageBackend` forwarders every producer door shares.

use flexiq_core::error::QueueError;
use flexiq_core::job::{now_millis, JobStatus, NewJob};
use flexiq_core::storage::records::DebounceOptions;
use flexiq_core::{
    quota_key, NamespaceQuota, QuotaOverflow, SqliteStorage, Storage, StorageBackend,
};

const TENANT: Option<&str> = Some("tenant");

fn storage() -> StorageBackend {
    StorageBackend::Sqlite(SqliteStorage::in_memory().expect("in-memory SQLite"))
}

fn job_in(namespace: Option<&str>) -> NewJob {
    NewJob {
        queue: "emails".to_string(),
        task_name: "send".to_string(),
        payload: vec![1],
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

fn pending(s: &StorageBackend, namespace: Option<&str>) -> i64 {
    s.count_by_namespace(namespace, JobStatus::Pending).unwrap()
}

fn depth(cap: i64, on_excess: QuotaOverflow) -> NamespaceQuota {
    NamespaceQuota {
        max_pending: Some(cap),
        on_excess,
        ..Default::default()
    }
}

#[test]
fn a_namespace_without_a_quota_is_unlimited() {
    let s = storage();
    for _ in 0..50 {
        s.enqueue(job_in(TENANT)).unwrap();
    }
    assert_eq!(pending(&s, TENANT), 50);
}

#[test]
fn depth_over_cap_rejects_with_queue_full() {
    let s = storage();
    s.set_namespace_quota(TENANT, &depth(2, QuotaOverflow::Reject))
        .unwrap();
    s.enqueue(job_in(TENANT)).unwrap();
    s.enqueue(job_in(TENANT)).unwrap();

    let error = s.enqueue(job_in(TENANT)).unwrap_err();
    assert!(
        matches!(error, QueueError::QueueFull { pending: 2, cap: 2, ref queue } if queue == "emails"),
        "{error:?}"
    );
    // The cross-SDK wire: SDKs parse the two integers off the tail.
    assert_eq!(
        error.to_string(),
        "queue 'emails' is full: 2 pending >= max_pending 2"
    );
    assert_eq!(pending(&s, TENANT), 2);

    // Other tenants, the default namespace included, are untouched.
    s.enqueue(job_in(Some("other"))).unwrap();
    s.enqueue(job_in(None)).unwrap();
}

#[test]
fn a_batch_straddling_the_cap_is_refused_whole() {
    let s = storage();
    s.set_namespace_quota(TENANT, &depth(3, QuotaOverflow::Reject))
        .unwrap();
    s.enqueue(job_in(TENANT)).unwrap();

    let batch = (0..3).map(|_| job_in(TENANT)).collect();
    assert!(matches!(
        s.enqueue_batch(batch),
        Err(QueueError::QueueFull { .. })
    ));
    assert_eq!(pending(&s, TENANT), 1, "nothing of the batch landed");

    let batch = (0..2).map(|_| job_in(TENANT)).collect();
    assert_eq!(s.enqueue_batch(batch).unwrap().len(), 2);
}

#[test]
fn a_running_job_frees_depth() {
    let s = storage();
    s.set_namespace_quota(TENANT, &depth(1, QuotaOverflow::Reject))
        .unwrap();
    s.enqueue(job_in(TENANT)).unwrap();
    s.dequeue("emails", now_millis() + 1, TENANT)
        .unwrap()
        .expect("claimed");
    s.enqueue(job_in(TENANT)).unwrap();
}

#[test]
fn depth_over_cap_with_drop_dead_letters_without_going_live() {
    let s = storage();
    s.set_namespace_quota(TENANT, &depth(1, QuotaOverflow::Drop))
        .unwrap();
    let live = s.enqueue(job_in(TENANT)).unwrap();
    let shed = s.enqueue(job_in(TENANT)).unwrap();

    assert_eq!(live.status, JobStatus::Pending);
    assert_eq!(shed.status, JobStatus::Dead);
    let reason = shed.error.as_deref().unwrap();
    assert!(reason.starts_with("quota:"), "{reason}");
    assert!(reason.contains("max_pending 1"), "{reason}");

    assert_eq!(pending(&s, TENANT), 1);
    let stored = s.get_job(&shed.id, TENANT).unwrap().expect("archived");
    assert_eq!(stored.status, JobStatus::Dead);
    let dead = s.list_dead(10, 0, TENANT).unwrap();
    assert_eq!(dead.len(), 1);
    assert_eq!(dead[0].original_job_id, shed.id);
    assert_eq!(dead[0].metadata.as_deref(), Some(r#"{"shed":"quota"}"#));
}

#[test]
fn a_mixed_batch_sheds_only_the_namespace_over_its_quota() {
    let s = storage();
    s.set_namespace_quota(TENANT, &depth(0, QuotaOverflow::Drop))
        .unwrap();
    let batch = vec![job_in(None), job_in(TENANT), job_in(Some("other"))];
    let jobs = s.enqueue_batch(batch).unwrap();

    let statuses: Vec<_> = jobs.iter().map(|j| j.status).collect();
    assert_eq!(
        statuses,
        [JobStatus::Pending, JobStatus::Dead, JobStatus::Pending],
        "results keep call order"
    );
    assert_eq!(jobs[1].namespace.as_deref(), TENANT);
    assert_eq!(pending(&s, TENANT), 0);
}

/// A batch every job of which is shed, for different reasons in different
/// namespaces, is recorded in full: one write, not one per reason.
#[test]
fn an_all_shed_batch_records_every_reason() {
    let s = storage();
    s.set_namespace_quota(TENANT, &depth(0, QuotaOverflow::Drop))
        .unwrap();
    s.set_namespace_quota(Some("other"), &depth(0, QuotaOverflow::Drop))
        .unwrap();
    let jobs = s
        .enqueue_batch(vec![job_in(TENANT), job_in(Some("other"))])
        .unwrap();
    assert!(jobs.iter().all(|job| job.status == JobStatus::Dead));
    for (job, namespace) in jobs.iter().zip([TENANT, Some("other")]) {
        let dead = s.list_dead(10, 0, namespace).unwrap();
        assert_eq!(dead.len(), 1);
        assert_eq!(dead[0].original_job_id, job.id);
        assert_eq!(dead[0].error, job.error, "each keeps its own reason");
    }
}

/// A mixed call whose admitted enqueue fails writes nothing at all: the shed
/// half is recorded only after the admitted half committed.
#[test]
fn a_failed_admitted_enqueue_leaves_no_shed_record() {
    let s = storage();
    s.set_namespace_quota(TENANT, &depth(0, QuotaOverflow::Drop))
        .unwrap();
    let mut broken = job_in(None);
    broken.depends_on = vec!["no-such-job".to_string()];
    let error = s.enqueue_batch(vec![broken, job_in(TENANT)]).unwrap_err();
    assert!(
        matches!(error, QueueError::DependencyNotFound(_)),
        "{error:?}"
    );
    assert!(s.list_dead(10, 0, TENANT).unwrap().is_empty());
}

#[test]
fn a_shed_unique_enqueue_reports_no_dedup_and_holds_no_key() {
    let s = storage();
    s.set_namespace_quota(TENANT, &depth(0, QuotaOverflow::Drop))
        .unwrap();
    let mut job = job_in(TENANT);
    job.unique_key = Some("k".to_string());
    let (shed, deduplicated) = s.enqueue_unique_reporting(job).unwrap();
    assert_eq!(shed.status, JobStatus::Dead);
    assert!(!deduplicated);
    assert_eq!(shed.unique_key, None);
}

#[test]
fn debounced_enqueues_are_admitted_too() {
    let s = storage();
    s.set_namespace_quota(TENANT, &depth(0, QuotaOverflow::Reject))
        .unwrap();
    let mut job = job_in(TENANT);
    job.debounce_key = Some("d".to_string());
    let options = DebounceOptions {
        window_ms: 1_000,
        max_wait_ms: 10_000,
        replace_payload: false,
        max_pending: None,
    };
    assert!(matches!(
        s.enqueue_debounced(job, options),
        Err(QueueError::QueueFull { .. })
    ));
}

#[test]
fn enqueue_rate_rejects_once_the_bucket_is_empty() {
    let s = storage();
    let quota = NamespaceQuota {
        enqueue_rate: Some("2/h".to_string()),
        ..Default::default()
    };
    s.set_namespace_quota(TENANT, &quota).unwrap();
    s.enqueue(job_in(TENANT)).unwrap();
    s.enqueue(job_in(TENANT)).unwrap();

    let error = s.enqueue(job_in(TENANT)).unwrap_err();
    assert!(
        matches!(error, QueueError::RateLimitExceeded(_)),
        "{error:?}"
    );
    assert!(error
        .to_string()
        .contains("namespace 'tenant' enqueue_rate 2/h"));
    // Another namespace draws on its own bucket.
    s.enqueue(job_in(Some("other"))).unwrap();
}

#[test]
fn a_depth_refusal_spends_no_rate_tokens() {
    let s = storage();
    let quota = NamespaceQuota {
        max_pending: Some(1),
        enqueue_rate: Some("2/h".to_string()),
        ..Default::default()
    };
    s.set_namespace_quota(TENANT, &quota).unwrap();
    let first = s.enqueue(job_in(TENANT)).unwrap(); // token 1
    for _ in 0..5 {
        assert!(matches!(
            s.enqueue(job_in(TENANT)),
            Err(QueueError::QueueFull { .. })
        ));
    }
    let claimed = s.dequeue("emails", now_millis() + 1, TENANT).unwrap();
    assert_eq!(claimed.map(|j| j.id), Some(first.id));
    s.enqueue(job_in(TENANT)).unwrap(); // token 2 was still there
}

#[test]
fn an_unreadable_quota_fails_closed() {
    let s = storage();
    s.set_setting(&quota_key(TENANT), r#"{"max_pending":"lots"}"#)
        .unwrap();
    let error = s.enqueue(job_in(TENANT)).unwrap_err();
    assert!(matches!(error, QueueError::Config(_)), "{error:?}");
    assert_eq!(pending(&s, TENANT), 0);
}

#[test]
fn a_cleared_quota_is_unlimited_at_once() {
    let s = storage();
    s.set_namespace_quota(TENANT, &depth(0, QuotaOverflow::Reject))
        .unwrap();
    assert!(s.enqueue(job_in(TENANT)).is_err());
    assert!(s.clear_namespace_quota(TENANT).unwrap());
    s.enqueue(job_in(TENANT)).unwrap();
    assert!(!s.clear_namespace_quota(TENANT).unwrap());
}

#[test]
fn a_quota_that_would_fail_every_enqueue_is_never_stored() {
    let s = storage();
    let quota = NamespaceQuota {
        max_pending: Some(-1),
        ..Default::default()
    };
    assert!(s.set_namespace_quota(TENANT, &quota).is_err());
    assert!(s.namespace_quota(TENANT).unwrap().is_none());
}
