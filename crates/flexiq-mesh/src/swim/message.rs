use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

use crate::state::{MemberState, WorkerInfo};

pub type MemberId = String;

/// SWIM protocol message, serialized via bincode over UDP.
/// Must fit in a single UDP datagram (< 1400 bytes).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GossipMessage {
    Ping {
        seq: u64,
        from: MemberId,
        from_addr: SocketAddr,
    },
    Ack {
        seq: u64,
        from: MemberId,
    },
    PingReq {
        seq: u64,
        from: MemberId,
        target: MemberId,
        target_addr: SocketAddr,
    },
    AckRelay {
        seq: u64,
        original_from: MemberId,
        via: MemberId,
    },
    /// Membership updates piggybacked on any message.
    Sync {
        updates: Vec<MemberUpdate>,
    },
    /// Compound: a primary message + piggybacked sync updates.
    Compound {
        primary: Box<GossipMessage>,
        updates: Vec<MemberUpdate>,
    },
}

/// A single membership state change to disseminate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemberUpdate {
    pub member_id: MemberId,
    pub state: MemberState,
    pub incarnation: u64,
    pub info: WorkerInfo,
}

impl GossipMessage {
    pub fn encode(&self) -> Result<Vec<u8>, bincode::Error> {
        bincode::serialize(self)
    }

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
