//! `ListQueues`, `PauseQueue`, `ResumeQueue` and `GetThroughput`.

use std::collections::BTreeSet;
use std::time::Duration;

use flexiq_core::{now_millis, Storage};
use prost_types::Duration as ProtoDuration;
use tonic::{Response, Status};

use super::{convert, require, Scoped};
use crate::dashboard::stores::overrides::{self, Scope};
use crate::grpc::blocking::on_storage;
use crate::grpc::pb::admin as pb;
use crate::grpc::producer::convert::{duration, millis_from_duration, timestamp};
use crate::grpc::status::WireError;

/// The window `GetThroughput` counts over when the request names none.
const DEFAULT_WINDOW: Duration = Duration::from_secs(5 * 60);
/// The longest window it will count over: the query reads every terminal job
/// in the window, so the bound is on the scan, not on the answer.
const MAX_WINDOW: Duration = Duration::from_secs(24 * 60 * 60);

/// Every queue the namespace has a job, a pause or an override for.
///
/// No single storage method knows every queue, so the listing is the union of
/// the three places one can appear — every one of them scoped to the caller's
/// namespace, so no other tenant's queue name leaks in.
pub(crate) async fn list(scoped: &Scoped) -> Result<Response<pb::ListQueuesResponse>, Status> {
    let namespace = scoped.namespace_owned();
    let (stats, paused, overridden) = on_storage(scoped.storage(), move |storage| {
        Ok((
            storage.stats_all_queues(Some(&namespace))?,
            storage.list_paused_queues(Some(&namespace))?,
            overrides::list(Scope::Queue, storage, Some(&namespace))?,
        ))
    })
    .await?;

    let mut names: BTreeSet<String> = stats.keys().cloned().collect();
    names.extend(paused.iter().cloned());
    names.extend(overridden.into_iter().map(|(name, _)| name));

    let queues = names
        .into_iter()
        .map(|name| {
            let is_paused = paused.contains(&name);
            let counts = stats.get(&name).cloned().unwrap_or_default();
            convert::queue(name, is_paused, &counts)
        })
        .collect();
    Ok(Response::new(pb::ListQueuesResponse { queues }))
}

/// Pause a queue and answer with the state that leaves it in.
pub(crate) async fn pause(
    scoped: &Scoped,
    request: pb::PauseQueueRequest,
) -> Result<Response<pb::PauseQueueResponse>, Status> {
    let queue = set_paused(scoped, request.queue, true).await?;
    Ok(Response::new(pb::PauseQueueResponse { queue: Some(queue) }))
}

/// Resume a queue and answer with the state that leaves it in.
pub(crate) async fn resume(
    scoped: &Scoped,
    request: pb::ResumeQueueRequest,
) -> Result<Response<pb::ResumeQueueResponse>, Status> {
    let queue = set_paused(scoped, request.queue, false).await?;
    Ok(Response::new(pb::ResumeQueueResponse {
        queue: Some(queue),
    }))
}

/// Write the pause, then read the queue back — both in one blocking task, so
/// the answer is the state this call left.
async fn set_paused(scoped: &Scoped, queue: String, paused: bool) -> Result<pb::Queue, Status> {
    let queue = require("queue", queue)?;
    let namespace = scoped.namespace_owned();
    on_storage(scoped.storage(), move |storage| {
        if paused {
            storage.pause_queue(&queue, Some(&namespace))?;
        } else {
            storage.resume_queue(&queue, Some(&namespace))?;
        }
        let is_paused = storage
            .list_paused_queues(Some(&namespace))?
            .contains(&queue);
        let stats = storage.stats_by_queue(&queue, Some(&namespace))?;
        Ok(convert::queue(queue, is_paused, &stats))
    })
    .await
}

/// Terminal jobs per queue over a window ending now.
pub(crate) async fn throughput(
    scoped: &Scoped,
    request: pb::GetThroughputRequest,
) -> Result<Response<pb::GetThroughputResponse>, Status> {
    let window = window(request.window.as_ref())?;
    let window_ms = i64::try_from(window.as_millis()).unwrap_or(i64::MAX);
    let since = now_millis().saturating_sub(window_ms);

    let namespace = scoped.namespace_owned();
    let counts = on_storage(scoped.storage(), move |storage| {
        storage.queue_throughput(since, Some(&namespace))
    })
    .await?;

    let mut queues: Vec<pb::QueueThroughput> = counts
        .into_iter()
        .map(|(queue, stats)| convert::throughput(queue, &stats))
        .collect();
    queues.sort_by(|a, b| a.queue.cmp(&b.queue));

    Ok(Response::new(pb::GetThroughputResponse {
        window: Some(duration(window_ms)),
        since: Some(timestamp(since)),
        queues,
    }))
}

/// The window a request asks for: the default when unset, refused when it is
/// not a positive span of at most a day.
fn window(requested: Option<&ProtoDuration>) -> Result<Duration, WireError> {
    let Some(requested) = requested else {
        return Ok(DEFAULT_WINDOW);
    };
    let millis = millis_from_duration(requested);
    let max = MAX_WINDOW.as_millis() as i64;
    if millis <= 0 || millis > max {
        return Err(WireError::invalid_request(format!(
            "window must be more than zero and at most {}s",
            MAX_WINDOW.as_secs()
        )));
    }
    Ok(Duration::from_millis(millis as u64))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_unset_window_is_five_minutes() {
        assert_eq!(window(None).unwrap(), DEFAULT_WINDOW);
    }

    #[test]
    fn a_window_outside_a_day_is_refused() {
        for seconds in [0, -1, 24 * 60 * 60 + 1] {
            let requested = ProtoDuration { seconds, nanos: 0 };
            let error = window(Some(&requested)).expect_err("out of range");
            assert_eq!(error.code(), tonic::Code::InvalidArgument, "{seconds}s");
        }
        let day = ProtoDuration {
            seconds: 24 * 60 * 60,
            nanos: 0,
        };
        assert_eq!(window(Some(&day)).unwrap(), MAX_WINDOW);
    }
}
