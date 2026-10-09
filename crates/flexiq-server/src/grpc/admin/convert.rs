//! Core records as the admin door's messages.
//!
//! One direction only: nothing this service reads off a request is a record.
//! Times cross as `Timestamp`, via the producer door's conversions, so the two
//! doors agree on the Unix-millisecond boundary.

use flexiq_core::{DeadJob, PeriodicTask, QueueStats, WorkerInfo, WorkerStatus};

use crate::grpc::pb::admin as pb;
use crate::grpc::producer::convert::timestamp;
use crate::tokens::{ApiToken, TokenStatus};

/// A queue, with the namespace's pause state and counts for it.
pub fn queue(name: String, paused: bool, stats: &QueueStats) -> pb::Queue {
    pb::Queue {
        name,
        paused,
        pending: stats.pending,
        running: stats.running,
        completed: stats.completed,
        failed: stats.failed,
        dead: stats.dead,
        cancelled: stats.cancelled,
    }
}

/// One queue's terminal counts inside a window.
pub fn throughput(queue: String, stats: &QueueStats) -> pb::QueueThroughput {
    pb::QueueThroughput {
        queue,
        completed: stats.completed,
        failed: stats.failed,
        dead: stats.dead,
        cancelled: stats.cancelled,
    }
}

/// A dead-letter entry. The payload is carried only when `payload` says so; a
/// listing's rows never load it.
pub fn dead_letter(entry: DeadJob, payload: bool) -> pb::DeadLetter {
    pb::DeadLetter {
        id: entry.id,
        original_job_id: entry.original_job_id,
        queue: entry.queue,
        task_name: entry.task_name,
        failed_at: Some(timestamp(entry.failed_at)),
        retry_count: entry.retry_count,
        max_retries: entry.max_retries,
        priority: entry.priority,
        replay_count: entry.dlq_retry_count,
        error: entry.error,
        metadata: entry.metadata,
        payload: payload.then_some(entry.payload),
    }
}

/// A registry row.
///
/// The status is a string in storage; one this build does not know reads as
/// `UNSPECIFIED`, which the contract tells a reader to show as unknown.
pub fn worker(info: WorkerInfo) -> pb::Worker {
    let status = match WorkerStatus::parse(&info.status) {
        Some(WorkerStatus::Active) => pb::WorkerStatus::Active,
        Some(WorkerStatus::Draining) => pb::WorkerStatus::Draining,
        None => pb::WorkerStatus::Unspecified,
    };
    pb::Worker {
        worker_id: info.worker_id,
        queues: info
            .queues
            .split(',')
            .map(str::trim)
            .filter(|queue| !queue.is_empty())
            .map(str::to_owned)
            .collect(),
        status: status as i32,
        last_heartbeat: Some(timestamp(info.last_heartbeat)),
        concurrency: info.threads,
        started_at: info.started_at.map(timestamp),
        hostname: info.hostname,
        pid: info.pid,
        pool_type: info.pool_type,
        sdk: info.sdk,
        sdk_version: info.sdk_version,
    }
}

/// An API token as it stands at `now`. Built field by field so the hash has no
/// way onto the wire.
pub fn api_token(token: ApiToken, now: i64) -> pb::ApiToken {
    let status = match token.status(now) {
        TokenStatus::Active => pb::TokenStatus::Active,
        TokenStatus::Expired => pb::TokenStatus::Expired,
        TokenStatus::Revoked => pb::TokenStatus::Revoked,
    };
    pb::ApiToken {
        id: token.id,
        name: token.name,
        scopes: token.scopes.spelled(),
        namespace: token.namespace,
        created_at: Some(timestamp(token.created_at)),
        last_used_at: token.last_used_at.map(timestamp),
        expires_at: Some(timestamp(token.expires_at)),
        revoked_at: token.revoked_at.map(timestamp),
        status: status as i32,
        created_by: token.created_by,
    }
}

/// A periodic task. The payload only when `payload` says so.
pub fn periodic_task(task: PeriodicTask, payload: bool) -> pb::PeriodicTask {
    pb::PeriodicTask {
        name: task.name,
        task_name: task.task_name,
        cron: task.cron_expr,
        queue: task.queue,
        enabled: task.enabled,
        next_run: Some(timestamp(task.next_run)),
        last_run: task.last_run.map(timestamp),
        timezone: task.timezone,
        payload: payload.then(|| task.args.unwrap_or_default()),
    }
}
