//! `GetJob`, `ListJobs` and `QueueStats` — the three reads.
//!
//! All three are `NO_SIDE_EFFECTS`, which is what lets the JSON facade serve
//! them over `GET`, and what makes them safe for a client to retry on any
//! failure at all, a dropped connection included.

use flexiq_core::storage::Storage;
use tonic::{Response, Status};

use super::convert::{self, Blobs};
use super::cursor::Cursor;
use super::Producer;
use crate::grpc::blocking::on_storage;
use crate::grpc::pb;
use crate::grpc::status::WireError;

/// Rows per page when the request asks for none.
const DEFAULT_PAGE_SIZE: i32 = 50;
/// The most rows one page will carry, however many are asked for.
///
/// A listing is blob-free, so the ceiling is about the response message and the
/// scan behind it rather than about payload size.
const MAX_PAGE_SIZE: i32 = 500;

/// Read one job by id.
pub async fn get_job(
    producer: &Producer,
    request: pb::GetJobRequest,
) -> Result<Response<pb::GetJobResponse>, Status> {
    let id = require_job_id(&request.job_id)?;
    let namespace = producer.namespace().to_string();
    let blobs = Blobs {
        payload: request.include_payload,
        result: request.include_result,
    };

    let job = on_storage(producer.storage(), move |storage| {
        storage.get_job(&id, Some(&namespace))
    })
    .await?
    // A job in another namespace reads as missing, and so does one retention
    // has already deleted. Neither is distinguishable from an id that never
    // existed, which is deliberate: a distinguishable answer is an oracle for
    // ids outside the caller's own namespace.
    .ok_or_else(|| not_found(&request.job_id))?;

    Ok(Response::new(pb::GetJobResponse {
        job: Some(convert::job_to_wire(job, blobs)),
    }))
}

/// Page through jobs, newest first.
pub async fn list_jobs(
    producer: &Producer,
    request: pb::ListJobsRequest,
) -> Result<Response<pb::ListJobsResponse>, Status> {
    let limit = page_size(request.page_size)?;
    let cursor = match request.page_token.as_str() {
        "" => None,
        token => Some(Cursor::decode(token)?),
    };

    // `Unspecified` is the absent filter, so an explicitly-sent zero and an
    // omitted field mean the same thing — which is what a proto3 reader that
    // does not know the field would produce anyway.
    let status = request
        .status
        .map(|value| {
            pb::JobStatus::try_from(value).map_err(|_| {
                WireError::invalid_request(format!("status {value} is not a JobStatus"))
            })
        })
        .transpose()?
        .and_then(convert::status_from_wire)
        .map(|status| status as i32);

    let namespace = producer.namespace().to_string();
    let queue = request.queue;
    let task_name = request.task_name;

    let jobs = on_storage(producer.storage(), move |storage| {
        storage.list_jobs_after(
            status,
            queue.as_deref(),
            task_name.as_deref(),
            i64::from(limit),
            cursor
                .as_ref()
                .map(|cursor| (cursor.created_at, cursor.id.as_str())),
            Some(&namespace),
        )
    })
    .await?;

    // A full page means there may be another; a short one is the end. Reading
    // the cursor off the last row rather than counting rows keeps the token
    // correct when the backend returns fewer than asked for.
    let next_page_token = (jobs.len() == limit as usize)
        .then(|| jobs.last())
        .flatten()
        .map(|job| {
            Cursor {
                created_at: job.created_at,
                id: job.id.clone(),
            }
            .encode()
        })
        .unwrap_or_default();

    Ok(Response::new(pb::ListJobsResponse {
        // Never a payload or a result. Without that rule a page of a hundred
        // jobs is a page of a hundred payloads.
        jobs: jobs
            .into_iter()
            .map(|job| convert::job_to_wire(job, Blobs::NONE))
            .collect(),
        next_page_token,
    }))
}

/// Per-status counts, for one queue or for the whole namespace.
pub async fn queue_stats(
    producer: &Producer,
    request: pb::QueueStatsRequest,
) -> Result<Response<pb::QueueStatsResponse>, Status> {
    let namespace = producer.namespace().to_string();
    let queue = request.queue;

    let stats = on_storage(producer.storage(), move |storage| match queue {
        Some(queue) => storage.stats_by_queue(&queue, Some(&namespace)),
        None => storage.stats(Some(&namespace)),
    })
    .await?;

    Ok(Response::new(pb::QueueStatsResponse {
        pending: stats.pending,
        running: stats.running,
        completed: stats.completed,
        failed: stats.failed,
        dead: stats.dead,
        cancelled: stats.cancelled,
    }))
}

/// A job id the caller actually sent.
pub(super) fn require_job_id(id: &str) -> Result<String, WireError> {
    if id.is_empty() {
        return Err(WireError::invalid_request("job_id must not be empty"));
    }
    Ok(id.to_string())
}

/// The one answer for "no such job", wherever it is reached from.
pub(super) fn not_found(id: &str) -> Status {
    WireError::from_queue_error(&flexiq_core::error::QueueError::JobNotFound(id.to_string())).into()
}

fn page_size(requested: i32) -> Result<i32, WireError> {
    match requested {
        0 => Ok(DEFAULT_PAGE_SIZE),
        n if n < 0 => Err(WireError::invalid_request("page_size must not be negative")),
        n => Ok(n.min(MAX_PAGE_SIZE)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_size_defaults_caps_and_refuses() {
        assert_eq!(page_size(0).unwrap(), DEFAULT_PAGE_SIZE);
        assert_eq!(page_size(10).unwrap(), 10);
        assert_eq!(page_size(i32::MAX).unwrap(), MAX_PAGE_SIZE);
        assert_eq!(
            page_size(-1).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }

    #[test]
    fn an_empty_job_id_is_refused_rather_than_looked_up() {
        assert_eq!(
            require_job_id("").unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
        assert_eq!(require_job_id("j").unwrap(), "j");
    }
}
