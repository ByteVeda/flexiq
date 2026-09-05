//! The gossip datagrams, and the updates they carry for free.
//!
//! Every message has room for piggybacked [`MemberUpdate`]s, so membership
//! news travels on traffic the failure detector was sending anyway. Sent over
//! UDP and therefore assumed lossy — nothing here is retried, because the next
//! period will carry the same update again.

use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

use crate::state::{MemberState, WorkerInfo};

/// How a member names itself on the wire: its worker id.
pub type MemberId = String;

/// SWIM protocol message, serialized via bincode over UDP.
/// Must fit in a single UDP datagram (< 1400 bytes).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GossipMessage {
    /// Direct probe, sent to one random peer each protocol period. The
    /// receiver replies with an [`GossipMessage::Ack`] carrying the same `seq`.
    Ping {
        /// Probe number the ack must echo, so the prober can match the reply
        /// to the peer it was waiting on.
        seq: u64,
        /// Worker id of the prober.
        from: MemberId,
        /// The prober's advertised gossip address. Informational: the ack goes
        /// back to the datagram's source, so nothing reads this today. It stays
        /// because it is on the wire between mesh nodes.
        from_addr: SocketAddr,
    },
    /// Reply to a `Ping`. Its arrival clears any suspicion the prober held.
    Ack {
        /// Probe number copied from the ping being answered.
        seq: u64,
        /// Worker id of the peer confirming it is alive.
        from: MemberId,
    },
    /// Asks an intermediary to probe `target` on the sender's behalf, sent
    /// after a direct ping went unanswered — one bad link between two nodes
    /// must not be enough to evict a healthy worker.
    PingReq {
        /// Probe number the requester registered this indirect probe under —
        /// freshly minted, not the stalled direct ping's. The intermediary
        /// mints a separate one for the relayed `Ping` and echoes this one back
        /// in the [`GossipMessage::AckRelay`].
        seq: u64,
        /// Worker id of the requester, which the relayed ack goes back to.
        from: MemberId,
        /// Worker id the intermediary should probe.
        target: MemberId,
        /// Gossip address of `target`, so the intermediary can reach it even
        /// if its own membership map has not learned it yet.
        target_addr: SocketAddr,
    },
    /// Sent by an intermediary that got an ack out of a `PingReq` target:
    /// evidence for the requester that the peer it could not reach is alive.
    AckRelay {
        /// The **requester's** probe number, copied from the `PingReq`, not the
        /// seq the intermediary minted for the relayed ping. Counters are per
        /// node, so anything else is unmatchable at the requester.
        seq: u64,
        /// Worker id of the target that answered.
        original_from: MemberId,
        /// Worker id of the intermediary that carried the probe.
        via: MemberId,
    },
    /// Membership updates piggybacked on any message.
    Sync {
        /// Changes to disseminate. Also how a node announces its own `Left`
        /// state to every peer on shutdown.
        updates: Vec<MemberUpdate>,
    },
    /// Compound: a primary message + piggybacked sync updates.
    Compound {
        /// The message that would have been sent on its own.
        primary: Box<GossipMessage>,
        /// Membership news riding along, applied before `primary` is handled.
        updates: Vec<MemberUpdate>,
    },
}

/// A single membership state change to disseminate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemberUpdate {
    /// Worker this update is about.
    pub member_id: MemberId,
    /// State the sender believes that worker is in.
    pub state: MemberState,
    /// The subject's incarnation number. The higher number wins a conflict,
    /// which is how a node refutes a suspicion raised about itself.
    pub incarnation: u64,
    /// The subject's last gossiped addresses and load, so a peer first heard
    /// of through this update is immediately reachable and steal-rankable.
    pub info: WorkerInfo,
}

impl GossipMessage {
    /// Serialize to the bincode bytes that go into one UDP datagram.
    pub fn encode(&self) -> Result<Vec<u8>, bincode::Error> {
        bincode::serialize(self)
    }

    /// Parse a received datagram. An error means a corrupt, truncated or
    /// foreign packet — or a wrong `encryption_key` — and the caller drops it.
    pub fn decode(data: &[u8]) -> Result<Self, bincode::Error> {
        bincode::deserialize(data)
    }

    /// Wrap this message with piggybacked membership updates.
    pub fn with_updates(self, updates: Vec<MemberUpdate>) -> Self {
        if updates.is_empty() {
            return self;
        }
        GossipMessage::Compound {
            primary: Box::new(self),
            updates,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    fn test_addr() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7946)
    }

    fn test_info() -> WorkerInfo {
        WorkerInfo {
            worker_id: "w1".to_string(),
            gossip_addr: test_addr(),
            steal_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7947),
            queues: vec!["default".to_string()],
            threads: 4,
            current_load: 2,
            local_buffer_len: 5,
            capacity: 2,
            updated_at: 1000,
        }
    }

    #[test]
    fn ping_round_trip() {
        let msg = GossipMessage::Ping {
            seq: 42,
            from: "w1".to_string(),
            from_addr: test_addr(),
        };
        let bytes = msg.encode().unwrap();
        let decoded = GossipMessage::decode(&bytes).unwrap();
        match decoded {
            GossipMessage::Ping { seq, from, .. } => {
                assert_eq!(seq, 42);
                assert_eq!(from, "w1");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn compound_round_trip() {
        let ping = GossipMessage::Ping {
            seq: 1,
            from: "w1".to_string(),
            from_addr: test_addr(),
        };
        let updates = vec![MemberUpdate {
            member_id: "w2".to_string(),
            state: MemberState::Alive,
            incarnation: 3,
            info: test_info(),
        }];
        let compound = ping.with_updates(updates);
        let bytes = compound.encode().unwrap();
        assert!(bytes.len() < 1400, "must fit in UDP datagram");

        let decoded = GossipMessage::decode(&bytes).unwrap();
        match decoded {
            GossipMessage::Compound { primary, updates } => {
                assert_eq!(updates.len(), 1);
                assert_eq!(updates[0].member_id, "w2");
                matches!(*primary, GossipMessage::Ping { .. });
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn empty_updates_no_wrap() {
        let ping = GossipMessage::Ping {
            seq: 1,
            from: "w1".to_string(),
            from_addr: test_addr(),
        };
        let result = ping.with_updates(vec![]);
        matches!(result, GossipMessage::Ping { .. });
    }
}
