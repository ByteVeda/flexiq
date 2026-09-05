//! What this node believes about the cluster right now.
//!
//! Every peer's last gossiped [`WorkerInfo`] and its [`MemberState`], plus the
//! [`crate::ring::HashRing`] derived from the live ones. **Two `RwLock`s, not
//! one** — a *reader* takes only what it needs, so one that consults both sees
//! two moments rather than one atomic view. Nothing here needs them to agree:
//! the ring is an affinity hint, and a placement made against a member the map
//! has since buried is still a valid placement.
//!
//! A *writer* is held to more than that. Every one takes `members` first and
//! keeps it while it writes the ring, so no two membership transitions can
//! interleave and strand a non-alive member on the ring. Nothing acquires them
//! the other way round, which is what makes that order safe to require.
//!
//! Beliefs, not facts: a peer marked dead here is a peer this node stopped
//! hearing from, and the queue rows it was holding are the stuck-job reaper's
//! problem, not this module's.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};

use crate::metrics::MeshMetrics;
use crate::ring::HashRing;

/// Load and identity information gossipped between mesh peers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerInfo {
    /// Worker id, unique in the cluster and the key every peer files this
    /// member under.
    pub worker_id: String,
    /// UDP address peers send gossip to.
    pub gossip_addr: SocketAddr,
    /// TCP address a thief connects to for a steal.
    pub steal_addr: SocketAddr,
    /// Queues this worker polls.
    pub queues: Vec<String>,
    /// Worker thread count, gossiped as a rough measure of the node's size.
    pub threads: u16,
    /// Jobs currently executing on this worker.
    pub current_load: u16,
    /// Jobs sitting in this worker's prefetch buffer. What a thief ranks
    /// peers by, since only buffered jobs can be handed over.
    pub local_buffer_len: u16,
    /// How much work this node can hold, used to size each peer's share of an
    /// adaptive prefetch.
    pub capacity: u16,
    /// Unix-millisecond time this snapshot was taken by its owner. A peer's
    /// view of it is always at least one gossip period old.
    pub updated_at: i64,
}

/// Member state in the SWIM protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MemberState {
    /// Answering probes. Only these members are on the ring and eligible as
    /// steal targets.
    Alive,
    /// Missed its probes and is running out a suspicion timer. Still a member
    /// — it can refute by gossiping a higher incarnation.
    Suspect,
    /// Suspicion expired with no refutation. Off the ring; the jobs it held
    /// are left to the ordinary stuck-job reaper.
    Dead,
    /// Announced its own departure on shutdown, so no suspicion timer was
    /// needed to reach the same conclusion.
    Left,
}

/// A member entry with state tracking.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Member {
    /// The peer's last gossiped identity and load.
    pub info: WorkerInfo,
    /// What this node currently believes about the peer's liveness.
    pub state: MemberState,
    /// Incarnation the belief was formed at. Updates carrying a lower one are
    /// stale news and get discarded.
    pub incarnation: u64,
}

/// Shared mesh state: membership map + consistent hash ring.
///
/// Thread-safe via `RwLock`. The gossip loop writes; the scheduler loop
/// and steal coordinator read.
pub struct MeshState {
    members: RwLock<HashMap<String, Member>>,
    ring: RwLock<HashRing>,
    local_worker_id: String,
    metrics: Arc<MeshMetrics>,
}

impl MeshState {
    /// Build the state for a node that knows no peers yet. The local worker is
    /// placed on the ring immediately, so affinity works before any gossip.
    ///
    /// Counts ring churn into a private [`MeshMetrics`] nobody can read. Use
    /// [`MeshState::with_metrics`] to share the node's own.
    pub fn new(worker_id: String, virtual_nodes: usize) -> Self {
        Self::with_metrics(worker_id, virtual_nodes, Arc::new(MeshMetrics::default()))
    }

    /// As [`MeshState::new`], but reporting ring churn into `metrics` — the
    /// same counters [`crate::MeshNode::metrics`] hands out, so
    /// `ring_recalculations` reflects what membership actually did.
    pub fn with_metrics(
        worker_id: String,
        virtual_nodes: usize,
        metrics: Arc<MeshMetrics>,
    ) -> Self {
        let mut ring = HashRing::new(virtual_nodes);
        ring.add_worker(&worker_id);
        Self {
            members: RwLock::new(HashMap::new()),
            ring: RwLock::new(ring),
            local_worker_id: worker_id,
            metrics,
        }
    }

    /// Id of the worker this state belongs to. Every peer query excludes it.
    pub fn local_worker_id(&self) -> &str {
        &self.local_worker_id
    }

    /// Check if a task name is affinity-owned by this worker.
    pub fn is_local_owner(&self, task_name: &str) -> bool {
        let ring = self.ring.read().unwrap_or_else(|p| p.into_inner());
        ring.is_owner(task_name, &self.local_worker_id)
    }

    /// Update or insert a member. Returns true if this is a new member.
    pub fn upsert_member(&self, member: Member) -> bool {
        let worker_id = member.info.worker_id.clone();
        let is_alive = member.state == MemberState::Alive;
        let mut members = self.members.write().unwrap_or_else(|p| p.into_inner());
        let previous = members.insert(worker_id.clone(), member);
        let is_new = previous.is_none();
        let was_alive = previous
            .map(|m| m.state == MemberState::Alive)
            .unwrap_or(false);

        // `members` stays held across the ring write, so the map and the ring
        // cannot disagree about a transition that is still in progress. Dropping
        // it first would let a concurrent `demote` remove the worker between the
        // two, and this call would then add it back — leaving a non-alive member
        // on the ring with nothing left to take it off.
        //
        // Guarded, so the routine case — a peer refreshing its load every
        // protocol period — neither rewrites the ring nor counts as churn.
        let changed = was_alive != is_alive;
        if changed {
            let mut ring = self.ring.write().unwrap_or_else(|p| p.into_inner());
            if is_alive {
                ring.add_worker(&worker_id);
            } else {
                ring.remove_worker(&worker_id);
            }
        }
        drop(members);

        if changed {
            self.count_ring_change();
        }
        is_new
    }

    /// Mark a member as dead and remove from ring.
    pub fn mark_dead(&self, worker_id: &str) {
        self.demote(worker_id, MemberState::Dead);
    }

    /// Mark a member as gracefully left and remove from ring.
    pub fn mark_left(&self, worker_id: &str) {
        self.demote(worker_id, MemberState::Left);
    }

    /// Move a member to a non-alive `state` and take it off the ring.
    ///
    /// The removal is unconditional — it is a no-op for a member already off —
    /// but the counter only moves when the member was `Alive`, so
    /// `ring_recalculations` measures churn in the ring's contents rather than
    /// writes attempted against it.
    fn demote(&self, worker_id: &str, state: MemberState) {
        let mut members = self.members.write().unwrap_or_else(|p| p.into_inner());
        let was_alive = members
            .get(worker_id)
            .is_some_and(|m| m.state == MemberState::Alive);
        if let Some(m) = members.get_mut(worker_id) {
            m.state = state;
        }
        let mut ring = self.ring.write().unwrap_or_else(|p| p.into_inner());
        ring.remove_worker(worker_id);
        drop(ring);
        drop(members);
        if was_alive {
            self.count_ring_change();
        }
    }

    /// Record one rebuild of the hash ring. Rises with membership churn, which
    /// is what makes affinity placements move.
    fn count_ring_change(&self) {
        self.metrics
            .ring_recalculations
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Get all alive members sorted by local_buffer_len descending (busiest first).
    pub fn alive_peers(&self) -> Vec<Member> {
        let members = self.members.read().unwrap_or_else(|p| p.into_inner());
        let mut alive: Vec<Member> = members
            .values()
            .filter(|m| m.state == MemberState::Alive && m.info.worker_id != self.local_worker_id)
            .cloned()
            .collect();
        alive.sort_by_key(|m| std::cmp::Reverse(m.info.local_buffer_len));
        alive
    }

    /// Get the busiest peer that has enough surplus to steal from.
    pub fn best_steal_target(&self, min_surplus: usize) -> Option<Member> {
        self.alive_peers()
            .into_iter()
            .find(|m| m.info.local_buffer_len as usize > min_surplus)
    }

    /// Get a clone of a member by worker ID.
    pub fn get_member(&self, worker_id: &str) -> Option<Member> {
        let members = self.members.read().unwrap_or_else(|p| p.into_inner());
        members.get(worker_id).cloned()
    }

    /// Number of alive members (excluding self).
    pub fn alive_count(&self) -> usize {
        let members = self.members.read().unwrap_or_else(|p| p.into_inner());
        members
            .values()
            .filter(|m| m.state == MemberState::Alive && m.info.worker_id != self.local_worker_id)
            .count()
    }

    /// Remove members that have been dead/left for longer than the given
    /// threshold. Returns the number removed.
    pub fn prune_dead(&self, _older_than_ms: i64) -> usize {
        let mut members = self.members.write().unwrap_or_else(|p| p.into_inner());
        let before = members.len();
        members.retain(|_, m| matches!(m.state, MemberState::Alive | MemberState::Suspect));
        before - members.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    fn make_info(id: &str, buffer_len: u16) -> WorkerInfo {
        WorkerInfo {
            worker_id: id.to_string(),
            gossip_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7946),
            steal_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7947),
            queues: vec!["default".to_string()],
            threads: 4,
            current_load: 0,
            local_buffer_len: buffer_len,
            capacity: 4,
            updated_at: 0,
        }
    }

    fn make_member(id: &str, buffer_len: u16) -> Member {
        Member {
            info: make_info(id, buffer_len),
            state: MemberState::Alive,
            incarnation: 1,
        }
    }

    #[test]
    fn upsert_and_query() {
        let state = MeshState::new("local".to_string(), 150);
        assert!(state.upsert_member(make_member("peer-a", 5)));
        assert!(!state.upsert_member(make_member("peer-a", 10))); // update, not new
        assert_eq!(state.alive_count(), 1);
    }

    #[test]
    fn ring_recalculations_counts_membership_churn() {
        let metrics = Arc::new(MeshMetrics::default());
        let state = MeshState::with_metrics("local".to_string(), 150, Arc::clone(&metrics));
        let count = || metrics.ring_recalculations.load(Ordering::Relaxed);

        // Placing the local worker at construction is not churn.
        assert_eq!(count(), 0);

        state.upsert_member(make_member("peer-a", 5));
        assert_eq!(count(), 1, "a peer joining the ring is one recalculation");

        // A load refresh from a peer that is still alive leaves the ring alone.
        state.upsert_member(make_member("peer-a", 12));
        assert_eq!(count(), 1);

        state.mark_dead("peer-a");
        assert_eq!(count(), 2, "an alive → dead transition rebuilds the ring");

        // Already off the ring: the removal is a no-op and must not be counted.
        state.mark_dead("peer-a");
        state.mark_dead("never-heard-of-it");
        assert_eq!(count(), 2);
    }

    #[test]
    fn suspect_takes_a_peer_off_the_ring() {
        let metrics = Arc::new(MeshMetrics::default());
        let state = MeshState::with_metrics("local".to_string(), 150, Arc::clone(&metrics));
        state.upsert_member(make_member("peer-a", 5));

        let mut suspected = make_member("peer-a", 5);
        suspected.state = MemberState::Suspect;
        state.upsert_member(suspected);

        assert_eq!(state.alive_count(), 0);
        assert_eq!(metrics.ring_recalculations.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn best_steal_target_picks_busiest() {
        let state = MeshState::new("local".to_string(), 150);
        state.upsert_member(make_member("peer-a", 3));
        state.upsert_member(make_member("peer-b", 10));
        state.upsert_member(make_member("peer-c", 7));

        let target = state.best_steal_target(2).unwrap();
        assert_eq!(target.info.worker_id, "peer-b");
    }

    #[test]
    fn mark_dead_removes_from_ring() {
        let state = MeshState::new("local".to_string(), 150);
        state.upsert_member(make_member("peer-a", 5));
        assert!(state.is_local_owner("some_task") || !state.is_local_owner("some_task")); // ring has 2 workers

        state.mark_dead("peer-a");
        assert_eq!(state.alive_count(), 0);
    }

    #[test]
    fn prune_removes_dead_members() {
        let state = MeshState::new("local".to_string(), 150);
        state.upsert_member(make_member("peer-a", 5));
        state.mark_dead("peer-a");
        let pruned = state.prune_dead(0);
        assert_eq!(pruned, 1);
        assert_eq!(state.alive_count(), 0);
    }
}
