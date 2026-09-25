//! The re-read that lets an id watch see what another process did.
//!
//! The feed carries only what *this* process emits. A job run by another
//! replica, or by a worker that reads the database directly, changes nowhere
//! this process can hear. So on a fixed cadence one task reads back every id
//! any stream is still watching, in one batched call per namespace, and hands
//! the rows to every stream at once. The cost is one read per tick, however
//! many streams and clients there are — not one per client per poll.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use flexiq_core::job::Job;
use flexiq_core::{Storage, StorageBackend};
use tokio::sync::watch;

use crate::grpc::blocking;
use crate::runtime::shutdown::Shutdown;

/// One tick's rows, per namespace.
#[derive(Debug, Default)]
pub struct Round {
    namespaces: HashMap<Arc<str>, Rows>,
}

/// What one namespace's read asked for and found.
#[derive(Debug, Default)]
struct Rows {
    asked: HashSet<String>,
    found: HashMap<String, Job>,
}

/// What a round says about one id.
#[derive(Debug)]
pub enum Reading<'a> {
    /// The round did not ask about it — it was registered after the read.
    Unasked,
    /// Asked, and no job this namespace can see answers to it any more.
    Gone,
    /// Its current row.
    Row(&'a Job),
}

impl Round {
    /// What this round read for `id` in `namespace`.
    pub fn reading(&self, namespace: &str, id: &str) -> Reading<'_> {
        let Some(rows) = self.namespaces.get(namespace) else {
            return Reading::Unasked;
        };
        if !rows.asked.contains(id) {
            return Reading::Unasked;
        }
        rows.found.get(id).map_or(Reading::Gone, Reading::Row)
    }
}

/// Which ids each open stream still watches.
type Interest = HashMap<u64, (Arc<str>, Vec<String>)>;

/// The registry of watched ids and the rounds read for them.
pub struct Reconciler {
    interest: Arc<Mutex<Interest>>,
    next: AtomicU64,
    rounds: watch::Sender<Arc<Round>>,
}

/// One stream's entry in the registry, removed when dropped.
pub struct Registration {
    id: u64,
    interest: Arc<Mutex<Interest>>,
}

impl Reconciler {
    /// An empty registry that has read nothing yet.
    pub fn new() -> Self {
        Self {
            interest: Arc::default(),
            next: AtomicU64::new(0),
            rounds: watch::Sender::new(Arc::default()),
        }
    }

    /// Start watching `ids` in `namespace`.
    pub fn register(&self, namespace: Arc<str>, ids: Vec<String>) -> Registration {
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        lock(&self.interest).insert(id, (namespace, ids));
        Registration {
            id,
            interest: Arc::clone(&self.interest),
        }
    }

    /// A receiver that wakes on every new round.
    pub fn subscribe(&self) -> watch::Receiver<Arc<Round>> {
        self.rounds.subscribe()
    }

    /// Read every watched id each `interval` until `shutdown`.
    ///
    /// A failed read is logged and skipped: the next tick tries again, and a
    /// stream keeps what it already has meanwhile.
    pub async fn run(
        self: Arc<Self>,
        storage: StorageBackend,
        interval: Duration,
        shutdown: Shutdown,
    ) {
        let mut ticks = tokio::time::interval(interval);
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                () = shutdown.wait() => return,
                _ = ticks.tick() => {}
            }
            let wanted = self.wanted();
            if wanted.is_empty() {
                continue;
            }
            match read(&storage, wanted).await {
                Ok(round) => {
                    self.rounds.send_replace(Arc::new(round));
                }
                Err(status) => {
                    log::warn!("grpc: watch reconcile read failed: {}", status.message());
                }
            }
        }
    }

    /// Every watched id, deduplicated, per namespace.
    fn wanted(&self) -> HashMap<Arc<str>, HashSet<String>> {
        let mut wanted: HashMap<Arc<str>, HashSet<String>> = HashMap::new();
        for (namespace, ids) in lock(&self.interest).values() {
            wanted
                .entry(Arc::clone(namespace))
                .or_default()
                .extend(ids.iter().cloned());
        }
        wanted
    }
}

impl Default for Reconciler {
    fn default() -> Self {
        Self::new()
    }
}

impl Registration {
    /// Narrow the ids this stream still watches.
    pub fn update(&self, ids: Vec<String>) {
        if let Some(entry) = lock(&self.interest).get_mut(&self.id) {
            entry.1 = ids;
        }
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        lock(&self.interest).remove(&self.id);
    }
}

async fn read(
    storage: &StorageBackend,
    wanted: HashMap<Arc<str>, HashSet<String>>,
) -> Result<Round, tonic::Status> {
    let mut round = Round::default();
    for (namespace, asked) in wanted {
        let ids: Vec<String> = asked.iter().cloned().collect();
        let scope = Arc::clone(&namespace);
        let rows = blocking::on_storage(storage, move |storage| {
            let ids: Vec<&str> = ids.iter().map(String::as_str).collect();
            storage.get_jobs_by_ids(&ids, Some(&scope))
        })
        .await?;
        let found = rows.into_iter().map(|job| (job.id.clone(), job)).collect();
        round.namespaces.insert(namespace, Rows { asked, found });
    }
    Ok(round)
}

fn lock(interest: &Mutex<Interest>) -> std::sync::MutexGuard<'_, Interest> {
    interest.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interest_is_merged_per_namespace_and_released_on_drop() {
        let reconciler = Reconciler::new();
        let a = reconciler.register("ns".into(), vec!["1".into(), "2".into()]);
        let _b = reconciler.register("ns".into(), vec!["2".into(), "3".into()]);
        let wanted = reconciler.wanted();
        let ids: HashSet<&str> = wanted["ns"].iter().map(String::as_str).collect();
        assert_eq!(ids, HashSet::from(["1", "2", "3"]));

        a.update(vec![]);
        assert_eq!(reconciler.wanted()["ns"].len(), 2);
        drop(a);
        drop(_b);
        assert!(reconciler.wanted().is_empty());
    }

    #[test]
    fn a_round_tells_unasked_from_gone() {
        let mut round = Round::default();
        round.namespaces.insert(
            "ns".into(),
            Rows {
                asked: HashSet::from(["gone".to_string()]),
                found: HashMap::new(),
            },
        );
        assert!(matches!(round.reading("ns", "gone"), Reading::Gone));
        assert!(matches!(round.reading("ns", "later"), Reading::Unasked));
        assert!(matches!(round.reading("other", "gone"), Reading::Unasked));
    }
}
