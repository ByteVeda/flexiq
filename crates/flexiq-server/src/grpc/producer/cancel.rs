//! `CancelJob`.
//!
//! Idempotent, and idempotent because the response describes state rather than
//! what the call did. `Storage::cancel_job` answers `false` for a job that is
//! no longer pending, so a bool on the wire would tell a retrying client "I did
//! not cancel it" about a job it cancelled a moment ago. The resulting job goes
//! back instead, and a second call answers the same thing as the first.

use flexiq_core::storage::Storage;
use tonic::{Response, Status};

use super::convert::{self, Blobs};
use super::reads::{not_found, require_job_id};
use super::Producer;
use crate::grpc::blocking::on_storage;
use crate::grpc::pb;

/// Cancel a job, and report the state that leaves it in.
pub async fn cancel_job(
    producer: &Producer,
    request: pb::CancelJobRequest,
) -> Result<Response<pb::CancelJobResponse>, Status> {
    let id = require_job_id(&request.job_id)?;
    let namespace = producer.namespace().to_string();

    // All three calls share one hop onto the blocking pool: they are one
    // logical operation, and three round trips through the pool would be three
    // chances to interleave with something else.
    let job = on_storage(producer.storage(), move |storage| {
        // A pending job is cancelled outright. A running one cannot be stopped
        // from outside — only the task can notice — so the flag is set and the
        // task stops at its next check. A job already terminal matches neither,
        // and comes back unchanged, which is what makes a retry safe.
        if !storage.cancel_job(&id, Some(&namespace))? {
            storage.request_cancel(&id, Some(&namespace))?;
        }
        storage.get_job(&id, Some(&namespace))
    })
    .await?
    .ok_or_else(|| not_found(&request.job_id))?;

    Ok(Response::new(pb::CancelJobResponse {
        // A cancel is not a read: a caller that wants the payload asks GetJob.
        job: Some(convert::job_to_wire(job, Blobs::NONE)),
    }))
}
