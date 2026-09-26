//! `WatchJobs`: follow jobs as they change state, instead of polling `GetJob`.
//!
//! Two sources, one per kind of news:
//!
//! - [`feed`] — every transition *this* process emits, off the event hub the
//!   scheduler and doors already announce on. Immediate, and no storage read.
//! - [`reconcile`] — one batched re-read of every watched id per tick, for a
//!   transition another process made: another replica, or a worker on the same
//!   database. Up to one tick late, and one read however many streams.
//!
//! An id watch uses both; a queue watch only the feed, because a queue has no
//! bounded set of ids to re-read. Streams are bounded per credential
//! ([`quota`]) and never wait long on a client that stopped reading
//! ([`stream`]). The wire contract is on `WatchJobsRequest` in
//! `producer_service.proto`.

pub mod cursor;
pub mod feed;
pub mod quota;
pub mod reconcile;
pub mod stream;
pub mod transition;

use std::collections::HashSet;
use std::sync::Arc;

use flexiq_core::{EventHub, StorageBackend};
use tonic::{Response, Status};

use crate::config::watch::WatchConfig;
use crate::grpc::auth::Principal;
use crate::grpc::pb::{self, watch_jobs_request::Target};
use crate::grpc::producer::convert::DEFAULT_QUEUE;
use crate::grpc::status::WireError;
use crate::runtime::shutdown::Shutdown;

use feed::{Seq, WatchFeed};
use quota::Quota;
use reconcile::Reconciler;
use stream::{Outlet, Shared};

/// Ids one watch may name. Part of the wire contract, so a constant rather
/// than a setting: a client written against one server works against all.
pub const MAX_IDS: usize = 100;

/// Every stream's shared state: the feed, the re-read, and the per-credential
/// count.
pub struct Watches {
    shared: Shared,
    quota: Quota,
}

impl std::fmt::Debug for Watches {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Watches").finish_non_exhaustive()
    }
}

/// What a validated request asks for.
#[derive(Debug, PartialEq, Eq)]
enum Want {
    Ids(Vec<String>),
    Queue { queue: String, from: Option<Seq> },
}

impl Watches {
    /// Tap `hub` and, unless the interval is zero, start the re-read. Must be
    /// called inside a Tokio runtime; both stop at `shutdown`.
    pub fn start(
        storage: StorageBackend,
        hub: &EventHub,
        config: &WatchConfig,
        shutdown: Shutdown,
    ) -> Arc<Self> {
        let feed = Arc::new(WatchFeed::new(config.buffer));
        hub.add_tap(Arc::clone(&feed) as Arc<dyn flexiq_core::EventTap>);
        let reconciler = Arc::new(Reconciler::new());
        if !config.reconcile_interval.is_zero() {
            tokio::spawn(Arc::clone(&reconciler).run(
                storage.clone(),
                config.reconcile_interval,
                shutdown.clone(),
            ));
        }
        Arc::new(Self {
            shared: Shared {
                storage,
                feed,
                reconciler,
                shutdown,
                stall: config.stall,
            },
            quota: Quota::new(config.max_per_credential),
        })
    }

    /// Streams open right now, across every credential.
    pub fn open(&self) -> usize {
        self.quota.total()
    }

    /// Validate `request`, take a slot for `principal`'s credential, and start
    /// the stream.
    pub fn watch(
        &self,
        principal: &Principal,
        request: pb::WatchJobsRequest,
    ) -> Result<Response<Outlet>, Status> {
        let target = self.target(request)?;
        let slot = self
            .quota
            .acquire(principal.credential())
            .ok_or_else(|| Status::from(WireError::watch_limit(self.quota.cap())))?;
        let (session, outlet) = stream::open(slot, self.shared.stall);
        let shared = self.shared.clone();
        let namespace = Arc::clone(principal.namespace());
        match target {
            Want::Ids(ids) => {
                tokio::spawn(stream::watch_ids(shared, session, namespace, ids));
            }
            Want::Queue { queue, from } => {
                // Taken now, not in the task: a transition that lands after
                // this call returns must reach the stream, however late the
                // task first runs.
                let from = from.unwrap_or_else(|| self.shared.feed.head());
                tokio::spawn(stream::watch_queue(shared, session, namespace, queue, from));
            }
        }
        Ok(Response::new(outlet))
    }

    fn target(&self, request: pb::WatchJobsRequest) -> Result<Want, Status> {
        let resume = (!request.resume_cursor.is_empty()).then_some(request.resume_cursor);
        match request.target {
            None => Err(invalid("set `job_ids` or `queue`")),
            Some(Target::JobIds(set)) => {
                if resume.is_some() {
                    return Err(invalid(
                        "`resume_cursor` is for a queue watch; an id watch resumes by opening again",
                    ));
                }
                Ok(Want::Ids(ids(set.job_ids)?))
            }
            Some(Target::Queue(queue)) => {
                let queue = if queue.is_empty() {
                    DEFAULT_QUEUE.to_string()
                } else {
                    queue
                };
                let from = resume.map(|raw| self.resume_from(&raw)).transpose()?;
                Ok(Want::Queue { queue, from })
            }
        }
    }

    /// The feed position a cursor resumes from, if this process still holds it.
    fn resume_from(&self, raw: &str) -> Result<Seq, Status> {
        let (instance, seq) = cursor::decode(raw)
            .ok_or_else(|| invalid("`resume_cursor` is not a cursor this server issued"))?;
        let feed = &self.shared.feed;
        if instance != feed.instance() || !feed.holds(seq) {
            return Err(WireError::watch_cursor_expired().into());
        }
        Ok(seq)
    }
}

/// The requested ids, deduplicated in order, each non-empty, at most
/// [`MAX_IDS`].
fn ids(requested: Vec<String>) -> Result<Vec<String>, Status> {
    if requested.is_empty() {
        return Err(invalid("`job_ids` names no job"));
    }
    if requested.iter().any(String::is_empty) {
        return Err(invalid("`job_ids` contains an empty id"));
    }
    let mut seen = HashSet::with_capacity(requested.len());
    let ids: Vec<String> = requested
        .into_iter()
        .filter(|id| seen.insert(id.clone()))
        .collect();
    if ids.len() > MAX_IDS {
        return Err(invalid(format!(
            "`job_ids` names {} jobs; one watch takes at most {MAX_IDS}",
            ids.len()
        )));
    }
    Ok(ids)
}

fn invalid(message: impl Into<String>) -> Status {
    WireError::invalid_request(message).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_are_deduplicated_in_order() {
        let got = ids(vec!["b".into(), "a".into(), "b".into()]).unwrap();
        assert_eq!(got, ["b", "a"]);
    }

    #[test]
    fn an_empty_set_an_empty_id_and_too_many_are_refused() {
        assert!(ids(vec![]).is_err());
        assert!(ids(vec!["a".into(), String::new()]).is_err());
        let many: Vec<String> = (0..=MAX_IDS).map(|i| i.to_string()).collect();
        assert!(ids(many).is_err());
        let duplicates: Vec<String> = (0..=MAX_IDS).map(|_| "same".to_string()).collect();
        assert_eq!(
            ids(duplicates).unwrap(),
            ["same"],
            "the cap counts distinct ids"
        );
    }
}
