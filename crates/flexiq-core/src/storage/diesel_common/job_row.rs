//! The single mapping from a `Job` onto the `jobs` table columns.
//!
//! Every Diesel write path that creates a job — the five enqueue variants and
//! the DLQ retry — used to spell the `NewJobRow` literal out for itself. A
//! struct literal gives the compiler nothing to check across sites, so a new
//! column added to four of six produced rows that silently differed rather
//! than a build error; `debounce_key` (#649) paid exactly that. Building the
//! row in one place turns a missed column back into a compile failure.
//!
//! Backend-agnostic on purpose: `NewJobRow` names no connection type, so this
//! is a plain module rather than another macro, and the DLQ macro can reach it
//! without depending on what `impl_diesel_job_ops!` happens to generate.

use crate::error::QueueError;
use crate::job::Job;
use crate::storage::models::NewJobRow;

/// Owned pub/sub attribution for one job.
///
/// Separate from the row because the extraction allocates and `NewJobRow`
/// borrows: the caller holds this for as long as the row it builds.
pub(crate) struct JobAttribution {
    topic: Option<String>,
    subscription_name: Option<String>,
}

impl JobAttribution {
    /// Attribution derived from a job's `notes`, so backlog and lag stats index
    /// by subscription. Empty for ordinary (non-delivery) jobs.
    pub(crate) fn of(job: &Job) -> Self {
        let (topic, subscription_name) =
            crate::pubsub::extract_topic_subscription(job.notes.as_deref())
                .map_or((None, None), |(t, s)| (Some(t), Some(s)));
        Self {
            topic,
            subscription_name,
        }
    }
}

/// Build the insertable row for a job. The one place `jobs` columns are listed —
/// add a field to `NewJobRow` and every creating path fails to compile until it
/// is mapped here.
pub(crate) fn new_job_row<'a>(job: &'a Job, attribution: &'a JobAttribution) -> NewJobRow<'a> {
    NewJobRow {
        id: &job.id,
        queue: &job.queue,
        task_name: &job.task_name,
        payload: &job.payload,
        status: job.status as i32,
        priority: job.priority,
        created_at: job.created_at,
        scheduled_at: job.scheduled_at,
        retry_count: job.retry_count,
        max_retries: job.max_retries,
        timeout_ms: job.timeout_ms,
        unique_key: job.unique_key.as_deref(),
        metadata: job.metadata.as_deref(),
        notes: job.notes.as_deref(),
        cancel_requested: 0,
        expires_at: job.expires_at,
        result_ttl_ms: job.result_ttl_ms,
        namespace: job.namespace.as_deref(),
        has_deps: job.has_deps,
        topic: attribution.topic.as_deref(),
        subscription_name: attribution.subscription_name.as_deref(),
        debounce_key: job.debounce_key.as_deref(),
    }
}

/// Translate the sentinel a failed dependency check rolls back with into the
/// error callers see. `validate_dependency` has only Diesel's error type to
/// signal with, so it borrows `RollbackTransaction`; every enqueue path has to
/// map it back on the way out.
pub(crate) fn dependency_not_found(err: QueueError) -> QueueError {
    match err {
        QueueError::Storage(diesel::result::Error::RollbackTransaction) => {
            QueueError::DependencyNotFound(
                "dependency not found or already dead/cancelled".to_string(),
            )
        }
        other => other,
    }
}
