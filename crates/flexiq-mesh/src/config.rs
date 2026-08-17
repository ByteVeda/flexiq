use std::net::{IpAddr, Ipv4Addr};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeshConfig {
    /// UDP port for SWIM gossip protocol.
    pub gossip_port: u16,
    /// TCP port for work-stealing connections (default: gossip_port + 1).
    pub steal_port: u16,
    /// Bind address for gossip and steal listeners.
    pub bind_addr: String,
    /// Seed nodes for initial cluster join (e.g., `["host1:7946"]`).
    pub seeds: Vec<String>,
    /// SWIM protocol period in milliseconds.
    pub protocol_period_ms: u64,
    /// Indirect ping targets for failure detection.
    pub indirect_ping_count: usize,
    /// Suspicion timeout multiplier (applied to `log(N+1) * protocol_period`).
    pub suspicion_multiplier: u32,
    /// Virtual nodes per worker on the consistent hash ring.
    pub virtual_nodes: usize,
    /// Max jobs in the local deque before refusing to prefetch.
    pub local_buffer_capacity: usize,
    /// Max jobs to steal per request.
    pub max_steal_batch: usize,
    /// Steal when own deque length is at or below this threshold.
    pub steal_threshold: usize,
    /// Affinity weight: 0.0 = ignore affinity, 1.0 = strict affinity.
    pub affinity_weight: f64,
    /// Whether work-stealing is enabled.
    pub enable_stealing: bool,
    /// IP address to advertise to peers for gossip and steal connections.
    /// Required when `bind_addr` is `0.0.0.0` and peers run on other hosts.
    /// Falls back to `bind_addr` when unset.
    pub advertise_addr: Option<String>,
    /// Shared encryption key for gossip messages (base64-encoded, 32 bytes).
    /// When set, gossip datagrams are XOR-encrypted with this key.
    /// Not cryptographically strong — prevents casual sniffing only.
    pub encryption_key: Option<String>,
    /// Max steal requests per peer per second. 0 = unlimited.
    pub steal_rate_limit: u32,
}

impl Default for MeshConfig {
    fn default() -> Self {
        Self {
            gossip_port: 7946,
            steal_port: 7947,
            bind_addr: "0.0.0.0".to_string(),
            seeds: Vec::new(),
            protocol_period_ms: 500,
            indirect_ping_count: 3,
            suspicion_multiplier: 4,
            virtual_nodes: 150,
            local_buffer_capacity: 64,
            max_steal_batch: 4,
            steal_threshold: 2,
            affinity_weight: 0.7,
            enable_stealing: true,
            advertise_addr: None,
            encryption_key: None,
            steal_rate_limit: 10,
        }
    }
}

impl MeshConfig {
    /// Resolve the IP to advertise to peers.
    /// Prefers `advertise_addr`, falls back to `bind_addr`.
    pub fn advertise_ip(&self) -> IpAddr {
        self.advertise_addr
            .as_deref()
            .or(Some(self.bind_addr.as_str()))
            .and_then(|s| s.parse::<IpAddr>().ok())
            .unwrap_or(IpAddr::V4(Ipv4Addr::UNSPECIFIED))
    }

    /// Decode the encryption key from base64. Returns None if unset or invalid.
    pub fn decoded_encryption_key(&self) -> Option<Vec<u8>> {
        self.encryption_key.as_ref().and_then(|k| {
            use base64::Engine;
            base64::engine::general_purpose::STANDARD.decode(k).ok()
        })
    }
}
