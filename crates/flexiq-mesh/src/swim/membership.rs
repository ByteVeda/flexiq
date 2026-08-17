use std::collections::HashMap;

use crate::state::{Member, MemberState};

use super::message::MemberUpdate;

/// Manages SWIM membership: incarnation numbers, state transitions,
/// and pending update dissemination.
pub struct Membership {
    local_id: String,
    local_incarnation: u64,
    /// Updates waiting to be piggybacked on outgoing messages.
    /// Bounded to MAX_PENDING to prevent unbounded growth.
    pending_updates: Vec<MemberUpdate>,
}

const MAX_PENDING: usize = 64;
const PIGGYBACK_LIMIT: usize = 8;

impl Membership {
    pub fn new(local_id: String) -> Self {
        Self {
            local_id,
            local_incarnation: 1,
            pending_updates: Vec::new(),
        }
    }

    pub fn local_id(&self) -> &str {
        &self.local_id
    }

    pub fn local_incarnation(&self) -> u64 {
        self.local_incarnation
    }

    /// Increment local incarnation (used to refute suspicion).
    pub fn refute(&mut self) -> u64 {
        self.local_incarnation += 1;
        self.local_incarnation
    }

    /// Process an incoming member update. Returns true if state changed.
    pub fn apply_update(
        &mut self,
        update: &MemberUpdate,
        members: &mut HashMap<String, Member>,
    ) -> bool {
        if update.member_id == self.local_id {
            return self.handle_self_update(update);
        }

        let changed = match members.get(&update.member_id) {
            Some(existing) => {
                update.incarnation > existing.incarnation
                    || (update.incarnation == existing.incarnation
                        && state_priority(update.state) > state_priority(existing.state))
            }
            None => true,
        };

        if changed {
            members.insert(
                update.member_id.clone(),
                Member {
                    info: update.info.clone(),
                    state: update.state,
                    incarnation: update.incarnation,
                },
            );
        }

        changed
    }

    /// Handle updates about ourselves — refute if suspected.
    pub fn handle_self_update(&mut self, update: &MemberUpdate) -> bool {
        if matches!(update.state, MemberState::Suspect | MemberState::Dead)
            && update.incarnation >= self.local_incarnation
        {
            self.refute();
            true
        } else {
            false
        }
    }

    /// Check if an update should override existing member state
    /// based on incarnation number and state priority.
    pub fn should_apply(&self, update: &MemberUpdate, existing: &Member) -> bool {
        update.incarnation > existing.incarnation
            || (update.incarnation == existing.incarnation
                && state_priority(update.state) > state_priority(existing.state))
    }

    /// Queue an update for piggybacking on outgoing messages.
    pub fn queue_update(&mut self, update: MemberUpdate) {
        if self.pending_updates.len() >= MAX_PENDING {
            self.pending_updates.remove(0);
        }
        self.pending_updates.push(update);
    }

    /// Take up to PIGGYBACK_LIMIT updates for piggybacking.
    /// Consumed updates are removed from the pending queue.
    pub fn take_updates(&mut self) -> Vec<MemberUpdate> {
        let n = self.pending_updates.len().min(PIGGYBACK_LIMIT);
        self.pending_updates.drain(..n).collect()
    }

    /// Check if there are pending updates to disseminate.
    pub fn has_pending(&self) -> bool {
        !self.pending_updates.is_empty()
    }
}

/// Higher priority wins in state conflict resolution.
fn state_priority(state: MemberState) -> u8 {
    match state {
        MemberState::Alive => 0,
        MemberState::Suspect => 1,
        MemberState::Dead => 2,
        MemberState::Left => 3,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::WorkerInfo;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn test_info(id: &str) -> WorkerInfo {
        WorkerInfo {
            worker_id: id.to_string(),
            gossip_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7946),
            steal_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7947),
            queues: vec!["default".to_string()],
            threads: 4,
            current_load: 0,
            local_buffer_len: 0,
            capacity: 4,
            updated_at: 0,
        }
    }

    fn make_update(id: &str, state: MemberState, incarnation: u64) -> MemberUpdate {
        MemberUpdate {
            member_id: id.to_string(),
            state,
            incarnation,
            info: test_info(id),
        }
    }

    #[test]
    fn apply_new_member() {
        let mut membership = Membership::new("local".to_string());
        let mut members = HashMap::new();
        let update = make_update("peer-a", MemberState::Alive, 1);
        assert!(membership.apply_update(&update, &mut members));
        assert_eq!(members.len(), 1);
    }

    #[test]
    fn higher_incarnation_wins() {
        let mut membership = Membership::new("local".to_string());
        let mut members = HashMap::new();
        membership.apply_update(&make_update("peer-a", MemberState::Alive, 1), &mut members);
        assert!(
            membership.apply_update(&make_update("peer-a", MemberState::Alive, 2), &mut members)
        );
        assert_eq!(members["peer-a"].incarnation, 2);
    }

    #[test]
    fn same_incarnation_higher_state_wins() {
        let mut membership = Membership::new("local".to_string());
        let mut members = HashMap::new();
        membership.apply_update(&make_update("peer-a", MemberState::Alive, 1), &mut members);
        assert!(membership.apply_update(
            &make_update("peer-a", MemberState::Suspect, 1),
            &mut members
        ));
        assert_eq!(members["peer-a"].state, MemberState::Suspect);
    }

    #[test]
    fn lower_incarnation_ignored() {
        let mut membership = Membership::new("local".to_string());
        let mut members = HashMap::new();
        membership.apply_update(&make_update("peer-a", MemberState::Alive, 5), &mut members);
        assert!(
            !membership.apply_update(&make_update("peer-a", MemberState::Dead, 3), &mut members)
        );
        assert_eq!(members["peer-a"].incarnation, 5);
    }

    #[test]
    fn self_suspicion_triggers_refute() {
        let mut membership = Membership::new("local".to_string());
        let mut members = HashMap::new();
        let update = make_update("local", MemberState::Suspect, 1);
        assert!(membership.apply_update(&update, &mut members));
        assert_eq!(membership.local_incarnation(), 2);
    }

    #[test]
    fn piggyback_queue() {
        let mut membership = Membership::new("local".to_string());
        for i in 0..10 {
            membership.queue_update(make_update(&format!("w{i}"), MemberState::Alive, 1));
        }
        let batch = membership.take_updates();
        assert_eq!(batch.len(), 8); // PIGGYBACK_LIMIT
        assert_eq!(membership.pending_updates.len(), 2);
    }
}
