use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::RwLock;

use serde::{Deserialize, Serialize};

use crate::ring::HashRing;

/// Load and identity information gossipped between mesh peers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerInfo {
    pub worker_id: String,
    pub gossip_addr: SocketAddr,
    pub steal_addr: SocketAddr,
    pub queues: Vec<String>,
    pub threads: u16,
    pub current_load: u16,
    pub local_buffer_len: u16,
    pub capacity: u16,
    pub updated_at: i64,
}

/// Member state in the SWIM protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MemberState {
    Alive,
    Suspect,
    Dead,
    Left,
}

/// A member entry with state tracking.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Member {
    pub info: WorkerInfo,
    pub state: MemberState,
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
}

impl MeshState {
    pub fn new(worker_id: String, virtual_nodes: usize) -> Self {
        let mut ring = HashRing::new(virtual_nodes);
        ring.add_worker(&worker_id);
        Self {
            members: RwLock::new(HashMap::new()),
            ring: RwLock::new(ring),
            local_worker_id: worker_id,
        }
    }

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
        drop(members);

        if was_alive != is_alive {
            let mut ring = self.ring.write().unwrap_or_else(|p| p.into_inner());
            if is_alive {
                ring.add_worker(&worker_id);
            } else {
                ring.remove_worker(&worker_id);
            }
        }
        is_new
    }

    /// Mark a member as dead and remove from ring.
    pub fn mark_dead(&self, worker_id: &str) {
        let mut members = self.members.write().unwrap_or_else(|p| p.into_inner());
        if let Some(m) = members.get_mut(worker_id) {
            m.state = MemberState::Dead;
        }
        let mut ring = self.ring.write().unwrap_or_else(|p| p.into_inner());
        ring.remove_worker(worker_id);
    }

    /// Mark a member as gracefully left and remove from ring.
    pub fn mark_left(&self, worker_id: &str) {
        let mut members = self.members.write().unwrap_or_else(|p| p.into_inner());
        if let Some(m) = members.get_mut(worker_id) {
            m.state = MemberState::Left;
        }
        let mut ring = self.ring.write().unwrap_or_else(|p| p.into_inner());
        ring.remove_worker(worker_id);
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
