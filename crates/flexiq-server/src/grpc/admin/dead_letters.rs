//! The dead-letter RPCs: list, read, replay, delete and purge.
//!
//! Every storage call passes `Some(namespace)`. The dead-letter methods treat
//! `None` as "every namespace", so that is the difference between an operator
//! door and a cross-tenant one.

use flexiq_core::error::QueueError;
use flexiq_core::Storage;
use tonic::{Response, Status};

use super::{convert, require, Scoped};
use crate::grpc::blocking::on_storage;
use crate::grpc::pb::admin as pb;
use crate::grpc::pb::admin::purge_dead_letters_request::Filter;
use crate::grpc::producer::convert::{job_to_wire, millis_from_timestamp, Blobs};
use crate::grpc::producer::cursor::Cursor;
use crate::grpc::producer::reads::page_size;
use crate::grpc::status::{reason, WireError};

/// A page of the dead-letter queue, newest first.
///
/// The page token is the producer door's opaque cursor, carrying the last
/// row's `(failed_at, id)` — the keyset `list_dead_after` resumes from.
pub(crate) async fn list(
    scoped: &Scoped,
    request: pb::ListDeadLettersRequest,
) -> Result<Response<pb::ListDeadLettersResponse>, Status> {
    let limit = page_size(request.page_size)?;
    let cursor = match request.page_token.as_str() {
        "" => None,
        token => Some(Cursor::decode(token)?),
    };

    let namespace = scoped.namespace_owned();
    let entries = on_storage(scoped.storage(), move |storage| {
        storage.list_dead_after(
            i64::from(limit),
            cursor
                .as_ref()
                .map(|cursor| (cursor.created_at, cursor.id.as_str())),
            Some(&namespace),
        )
    })
    .await?;

    // A full page may have a successor; a short one is the end.
    let next_page_token = (entries.len() == limit as usize)
        .then(|| entries.last())
        .flatten()
        .map(|entry| {
            Cursor {
                created_at: entry.failed_at,
                id: entry.id.clone(),
            }
            .encode()
        })
        .unwrap_or_default();

    Ok(Response::new(pb::ListDeadLettersResponse {
        dead_letters: entries
            .into_iter()
            .map(|entry| convert::dead_letter(entry, false))
            .collect(),
        next_page_token,
    }))
}

/// One entry, with its payload when asked for.
pub(crate) async fn get(
    scoped: &Scoped,
    request: pb::GetDeadLetterRequest,
) -> Result<Response<pb::GetDeadLetterResponse>, Status> {
    let id = require("dead_letter_id", request.dead_letter_id)?;
    let namespace = scoped.namespace_owned();
    let lookup = id.clone();
    let entry = on_storage(scoped.storage(), move |storage| {
        storage.get_dead(&lookup, Some(&namespace))
    })
    .await?
    .ok_or_else(|| not_found(&id))?;

    Ok(Response::new(pb::GetDeadLetterResponse {
        dead_letter: Some(convert::dead_letter(entry, request.include_payload)),
    }))
}

/// Enqueue the entry again as a fresh job and answer with that job.
pub(crate) async fn replay(
    scoped: &Scoped,
    request: pb::ReplayDeadLetterRequest,
) -> Result<Response<pb::ReplayDeadLetterResponse>, Status> {
    let id = require("dead_letter_id", request.dead_letter_id)?;
    let namespace = scoped.namespace_owned();
    let lookup = id.clone();
    let events = scoped.events();
    let job = on_storage(scoped.storage(), move |storage| {
        // `retry_dead` answers an absent or foreign entry `JobNotFound` — about
        // the entry, not a job — so it is caught here and renamed rather than
        // reaching the wire as `JOB_NOT_FOUND`.
        let job_id = match storage.retry_dead(&lookup, Some(&namespace)) {
            Err(QueueError::JobNotFound(_)) => return Ok(None),
            other => other?,
        };
        let job = storage.get_job(&job_id, Some(&namespace))?;
        // A replay is a fresh job, so it is announced as an enqueue.
        if let Some(job) = &job {
            crate::events::enqueued(events.as_deref(), job);
        }
        Ok(job)
    })
    .await?
    .ok_or_else(|| not_found(&id))?;

    Ok(Response::new(pb::ReplayDeadLetterResponse {
        job: Some(job_to_wire(job, Blobs::NONE)),
    }))
}

/// Delete one entry. An absent one is `NOT_FOUND`, the same answer a second
/// delete gets.
pub(crate) async fn delete(
    scoped: &Scoped,
    request: pb::DeleteDeadLetterRequest,
) -> Result<Response<pb::DeleteDeadLetterResponse>, Status> {
    let id = require("dead_letter_id", request.dead_letter_id)?;
    let namespace = scoped.namespace_owned();
    let lookup = id.clone();
    let deleted = on_storage(scoped.storage(), move |storage| {
        storage.delete_dead(&lookup, Some(&namespace))
    })
    .await?;
    if !deleted {
        return Err(not_found(&id));
    }
    Ok(Response::new(pb::DeleteDeadLetterResponse {}))
}

/// Delete entries in bulk: before an instant, of one task, or all of them.
pub(crate) async fn purge(
    scoped: &Scoped,
    request: pb::PurgeDeadLettersRequest,
) -> Result<Response<pb::PurgeDeadLettersResponse>, Status> {
    let namespace = scoped.namespace_owned();
    let purged = match request.filter {
        Some(Filter::TaskName(task)) => {
            let task = require("task_name", task)?;
            on_storage(scoped.storage(), move |storage| {
                storage.purge_dead_by_task(&task, Some(&namespace))
            })
            .await?
        }
        Some(Filter::FailedBefore(before)) => {
            let cutoff = millis_from_timestamp(&before);
            on_storage(scoped.storage(), move |storage| {
                storage.purge_dead(cutoff, Some(&namespace))
            })
            .await?
        }
        // Every entry: a cutoff past any `failed_at`, including one stamped by
        // a clock ahead of this one.
        None => {
            on_storage(scoped.storage(), move |storage| {
                storage.purge_dead(i64::MAX, Some(&namespace))
            })
            .await?
        }
    };

    Ok(Response::new(pb::PurgeDeadLettersResponse {
        purged: i64::try_from(purged).unwrap_or(i64::MAX),
    }))
}

fn not_found(id: &str) -> Status {
    WireError::not_found(reason::DEAD_LETTER_NOT_FOUND, "dead-letter entry", id).into()
}
