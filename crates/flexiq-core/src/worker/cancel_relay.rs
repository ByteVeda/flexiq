//! Carries a storage cancel request to a dispatcher that cannot read storage.
//!
//! `Storage::request_cancel` only sets a flag. The native pool reads that flag
//! itself; an attached executor and a push target never touch storage, so for
//! them the flag is invisible unless something turns it into
//! [`WorkerDispatcher::notify_cancel`]. Polling storage, rather than hooking
//! each cancel entry point, is deliberate: a cancel may come from another
//! replica, another process, or an SDK writing to the same database, and
//! storage is the one place all of them reach.

use std::collections::HashSet;
use std::time::Duration;

use crate::error::Result;
use crate::storage::Storage;

use super::WorkerDispatcher;

/// How often the relay looks. The latency a cancel takes to reach a remote or
/// push dispatcher, and one indexed read per tick while anything is in flight.
pub const CANCEL_RELAY_INTERVAL: Duration = Duration::from_secs(1);

/// Remembers which dispatches it has already relayed a cancel for.
///
/// Keyed by `(job id, epoch)`, not id alone: a job that settles and is
/// re-dispatched between two ticks is a new dispatch, and must not inherit the
/// old one's "already relayed".
#[derive(Default)]
pub struct CancelRelay {
    relayed: HashSet<(String, Option<i64>)>,
}

impl CancelRelay {
    /// A relay that has relayed nothing yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// One pass over `in_flight`. Returns how many cancels were relayed.
    ///
    /// Each dispatch is relayed at most once — a remote dispatcher turns every
    /// call into a frame on the wire.
    pub fn tick(
        &mut self,
        storage: &impl Storage,
        namespace: Option<&str>,
        in_flight: &[(String, Option<i64>)],
        dispatcher: &dyn WorkerDispatcher,
    ) -> Result<usize> {
        // Bounded by what is in flight: anything that settled is forgotten.
        let current: HashSet<&(String, Option<i64>)> = in_flight.iter().collect();
        self.relayed.retain(|dispatch| current.contains(dispatch));

        let unrelayed: Vec<&(String, Option<i64>)> = in_flight
            .iter()
            .filter(|dispatch| !self.relayed.contains(*dispatch))
            .collect();
        if unrelayed.is_empty() {
            return Ok(0);
        }

        let ids: Vec<String> = unrelayed.iter().map(|(id, _)| id.clone()).collect();
        let requested: HashSet<String> = storage
            .cancel_requested_among(&ids, namespace)?
            .into_iter()
            .collect();

        let mut relayed = 0;
        for dispatch in unrelayed {
            if requested.contains(&dispatch.0) {
                dispatcher.notify_cancel(&dispatch.0);
                self.relayed.insert(dispatch.clone());
                relayed += 1;
            }
        }
        Ok(relayed)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;
    use crossbeam_channel::Sender;

    use super::*;
    use crate::job::{now_millis, Job, NewJob};
    use crate::scheduler::JobResult;
    use crate::SqliteStorage;

    #[derive(Default)]
    struct Recording(Mutex<Vec<String>>);

    #[async_trait]
    impl WorkerDispatcher for Recording {
        async fn run(&self, _job_rx: tokio::sync::mpsc::Receiver<Job>, _tx: Sender<JobResult>) {}
        fn shutdown(&self) {}
        fn notify_cancel(&self, job_id: &str) {
            self.0.lock().unwrap().push(job_id.to_string());
        }
    }

    fn running(storage: &SqliteStorage) -> String {
        let job = storage
            .enqueue(NewJob {
                queue: "relay".to_string(),
                task_name: "relay".to_string(),
                payload: Vec::new(),
                priority: 0,
                scheduled_at: now_millis(),
                max_retries: 0,
                timeout_ms: 60_000,
                unique_key: None,
                metadata: None,
                notes: None,
                depends_on: Vec::new(),
                expires_at: None,
                result_ttl_ms: None,
                namespace: None,
                debounce_key: None,
            })
            .unwrap();
        storage
            .dequeue("relay", now_millis(), None)
            .unwrap()
            .unwrap();
        job.id
    }

    #[test]
    fn a_requested_cancel_is_relayed_once_per_dispatch() {
        let storage = SqliteStorage::in_memory().unwrap();
        let cancelled = running(&storage);
        let untouched = running(&storage);
        storage.request_cancel(&cancelled, None).unwrap();
        let dispatcher = Recording::default();
        let mut relay = CancelRelay::new();
        let in_flight = vec![(cancelled.clone(), Some(1)), (untouched, Some(1))];

        assert_eq!(
            relay.tick(&storage, None, &in_flight, &dispatcher).unwrap(),
            1
        );
        // The flag is still set, but this dispatch has already been told.
        assert_eq!(
            relay.tick(&storage, None, &in_flight, &dispatcher).unwrap(),
            0
        );
        assert_eq!(*dispatcher.0.lock().unwrap(), vec![cancelled]);
    }

    #[test]
    fn a_redispatch_of_the_same_id_is_relayed_again() {
        let storage = SqliteStorage::in_memory().unwrap();
        let job = running(&storage);
        storage.request_cancel(&job, None).unwrap();
        let dispatcher = Recording::default();
        let mut relay = CancelRelay::new();

        relay
            .tick(&storage, None, &[(job.clone(), Some(1))], &dispatcher)
            .unwrap();
        // A new claim is a new epoch: the old "already relayed" must not cover it.
        relay
            .tick(&storage, None, &[(job.clone(), Some(2))], &dispatcher)
            .unwrap();
        assert_eq!(dispatcher.0.lock().unwrap().len(), 2);
    }

    #[test]
    fn nothing_in_flight_reads_nothing() {
        let storage = SqliteStorage::in_memory().unwrap();
        let dispatcher = Recording::default();
        let mut relay = CancelRelay::new();
        assert_eq!(relay.tick(&storage, None, &[], &dispatcher).unwrap(), 0);
    }
}
