//! Core records → the JSON the dashboard SPA expects.
//!
//! Shapes are pinned by `dashboard/src/lib/api-types.ts` and must match the SDK
//! dashboards field for field, because one SPA build is served by all of them.
//! Two rules carry most of the weight:
//!
//! - every timestamp is Unix **milliseconds**, never seconds;
//! - listings are blob-free — `payload` and `result` never leave the server.

use flexiq_core::storage::records::{CircuitBreakerState, JobError, TaskLogEntry};
use flexiq_core::{DeadJob, Job, QueueStats, ReplayEntry, SubscriptionBacklogStats, WorkerInfo};
use serde_json::{json, Value};

/// Longest error summary surfaced in a listing, matching the SDK dashboards.
const ERROR_SUMMARY_MAX: usize = 500;

/// One job, without its payload or result blobs.
pub fn job(job: &Job) -> Value {
    json!({
        "id": job.id,
        "queue": job.queue,
        "task_name": job.task_name,
        "status": job.status.as_str(),
        "priority": job.priority,
        "progress": job.progress,
        "retry_count": job.retry_count,
        "max_retries": job.max_retries,
        "created_at": job.created_at,
        "scheduled_at": job.scheduled_at,
        "started_at": job.started_at,
        "completed_at": job.completed_at,
        "error": summarize_error(job.error.as_deref()),
        "timeout_ms": job.timeout_ms,
        "unique_key": job.unique_key,
        "metadata": job.metadata,
        // The raw canonical JSON string: the contract is `notes: string | null`
        // and the client parses it itself.
        "notes": job.notes,
        "namespace": job.namespace,
    })
}

/// One dead-letter entry.
pub fn dead_job(entry: &DeadJob) -> Value {
    json!({
        "id": entry.id,
        "original_job_id": entry.original_job_id,
        "queue": entry.queue,
        "task_name": entry.task_name,
        "error": entry.error,
        "retry_count": entry.retry_count,
        "failed_at": entry.failed_at,
        "metadata": entry.metadata,
        "dlq_retry_count": entry.dlq_retry_count,
    })
}

/// Queue status counts.
pub fn queue_stats(stats: &QueueStats) -> Value {
    json!({
        "pending": stats.pending,
        "running": stats.running,
        "completed": stats.completed,
        "failed": stats.failed,
        "dead": stats.dead,
        "cancelled": stats.cancelled,
    })
}

/// One registered worker.
pub fn worker(worker: &WorkerInfo) -> Value {
    json!({
        "worker_id": worker.worker_id,
        "last_heartbeat": worker.last_heartbeat,
        "queues": worker.queues,
        "status": worker.status,
        "tags": worker.tags,
        "resources": worker.resources,
        "resource_health": worker.resource_health,
        "threads": worker.threads,
        "started_at": worker.started_at,
        // The SPA reads `registered_at`, and the peer SDKs have always emitted
        // it; this route emitted only `started_at`, so the field rendered empty.
        "registered_at": worker.started_at.unwrap_or(worker.last_heartbeat),
        "hostname": worker.hostname,
        "pid": worker.pid,
        "pool_type": worker.pool_type,
        "sdk": worker.sdk,
        "sdk_version": worker.sdk_version,
    })
}

/// One circuit breaker. The numeric state is mapped to the name the SPA
/// renders.
pub fn circuit_breaker(breaker: &CircuitBreakerState) -> Value {
    json!({
        "task_name": breaker.task_name,
        "state": match breaker.state {
            1 => "open",
            2 => "half_open",
            _ => "closed",
        },
        "failure_count": breaker.failure_count,
        "last_failure_at": breaker.last_failure_at,
        "opened_at": breaker.opened_at,
        "threshold": breaker.threshold,
        "window_ms": breaker.window_ms,
        "cooldown_ms": breaker.cooldown_ms,
    })
}

/// One attempt's failure record.
pub fn job_error(error: &JobError) -> Value {
    json!({
        "id": error.id,
        "job_id": error.job_id,
        "attempt": error.attempt,
        "error": error.error,
        "failed_at": error.failed_at,
    })
}

/// One structured log line.
pub fn task_log(entry: &TaskLogEntry) -> Value {
    json!({
        "id": entry.id,
        "job_id": entry.job_id,
        "task_name": entry.task_name,
        "level": entry.level,
        "message": entry.message,
        "extra": entry.extra,
        "logged_at": entry.logged_at,
    })
}

/// One replay pairing.
pub fn replay_entry(entry: &ReplayEntry) -> Value {
    json!({
        "id": entry.id,
        "original_job_id": entry.original_job_id,
        "replay_job_id": entry.replay_job_id,
        "replayed_at": entry.replayed_at,
        "original_error": entry.original_error,
        "replay_error": entry.replay_error,
    })
}

/// One subscription's backlog row.
pub fn subscription_stats(row: &SubscriptionBacklogStats) -> Value {
    json!({
        "topic": row.topic,
        "subscription": row.subscription_name,
        "task_name": row.task_name,
        "queue": row.queue,
        "active": row.active,
        "durable": row.durable,
        "pending": row.pending,
        "running": row.running,
        "dead": row.dead,
        "oldest_pending_age_ms": row.oldest_pending_age_ms,
    })
}

/// Reduce a stored error to one `ExceptionType: message` line.
///
/// Structured errors are the canonical cross-SDK JSON
/// (`{errtype, message, traceback}`); anything else is a legacy plain
/// traceback, whose last non-empty line is the useful part. Frames carry file
/// paths and source snippets that do not belong in a broadly-readable listing.
pub fn summarize_error(error: Option<&str>) -> Option<String> {
    let raw = error?;
    if raw.is_empty() {
        return Some(String::new());
    }
    let summary = structured_summary(raw).unwrap_or_else(|| {
        raw.lines()
            .rev()
            .find(|line| !line.trim().is_empty())
            .unwrap_or(raw)
            .to_string()
    });
    let summary = summary.trim();

    // Truncate on a character boundary — a byte slice could split a multi-byte
    // sequence and produce invalid UTF-8.
    match summary.char_indices().nth(ERROR_SUMMARY_MAX) {
        Some((cut, _)) => Some(format!("{}…", &summary[..cut])),
        None => Some(summary.to_string()),
    }
}

/// `errtype: message` from a canonical structured error, if it is one.
fn structured_summary(raw: &str) -> Option<String> {
    let decoded: Value = serde_json::from_str(raw).ok()?;
    let errtype = decoded.get("errtype")?.as_str()?;
    let message = decoded.get("message").and_then(Value::as_str).unwrap_or("");
    Some(if message.is_empty() {
        errtype.to_string()
    } else {
        format!("{errtype}: {message}")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_structured_error_summarizes_to_one_line() {
        let raw =
            r#"{"errtype":"ValueError","message":"boom","traceback":["frame one","frame two"]}"#;
        assert_eq!(
            summarize_error(Some(raw)).as_deref(),
            Some("ValueError: boom")
        );
    }

    #[test]
    fn a_structured_error_without_a_message_is_just_the_type() {
        let raw = r#"{"errtype":"KeyboardInterrupt","message":"","traceback":[]}"#;
        assert_eq!(
            summarize_error(Some(raw)).as_deref(),
            Some("KeyboardInterrupt")
        );
    }

    #[test]
    fn a_plain_traceback_falls_back_to_its_last_line() {
        let raw =
            "Traceback (most recent call last):\n  File \"x.py\", line 1\nRuntimeError: nope\n";
        assert_eq!(
            summarize_error(Some(raw)).as_deref(),
            Some("RuntimeError: nope")
        );
    }

    #[test]
    fn a_long_summary_is_truncated_on_a_character_boundary() {
        let raw = format!("é{}", "x".repeat(1_000));
        let summary = summarize_error(Some(&raw)).expect("a summary");
        assert!(summary.ends_with('…'));
        assert_eq!(summary.chars().count(), ERROR_SUMMARY_MAX + 1);
    }

    #[test]
    fn no_error_stays_absent() {
        assert_eq!(summarize_error(None), None);
    }
}
