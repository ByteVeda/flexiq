pub mod failure;
pub mod membership;
pub mod message;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use log::{debug, info, warn};
use rand::prelude::IndexedRandom;
use tokio::net::UdpSocket;
use tokio::sync::Notify;

use crate::config::MeshConfig;
use crate::state::{Member, MemberState, MeshState, WorkerInfo};

use self::failure::FailureDetector;
use self::membership::Membership;
use self::message::{GossipMessage, MemberUpdate};

fn xor_cipher(data: &[u8], key: &[u8]) -> Vec<u8> {
    data.iter()
        .enumerate()
        .map(|(i, b)| b ^ key[i % key.len()])
        .collect()
}

/// SWIM protocol node. Runs a gossip loop on a tokio UDP socket.
pub struct SwimNode {
    config: MeshConfig,
    state: Arc<MeshState>,
    membership: Membership,
    failure_detector: FailureDetector,
    local_info: WorkerInfo,
    seq: u64,
    shutdown: Arc<Notify>,
    encryption_key: Option<Vec<u8>>,
    /// Tracks PingReq relays: seq → (requester_id, requester_addr).
    /// When an ACK arrives for a relayed seq, we forward AckRelay
    /// back to the original requester.
    pending_relays: HashMap<u64, (String, SocketAddr)>,
}

impl SwimNode {
    pub fn new(
        config: MeshConfig,
        state: Arc<MeshState>,
        local_info: WorkerInfo,
        shutdown: Arc<Notify>,
    ) -> Self {
        let membership = Membership::new(local_info.worker_id.clone());
        let ping_timeout_ms = config.protocol_period_ms / 2;
        let failure_detector = FailureDetector::new(
            ping_timeout_ms,
            config.suspicion_multiplier,
            config.protocol_period_ms,
        );
        let encryption_key = config.decoded_encryption_key();
        Self {
            config,
            state,
            membership,
            failure_detector,
            local_info,
            seq: 0,
            shutdown,
            encryption_key,
            pending_relays: HashMap::new(),
        }
    }

    fn encrypt(&self, data: &[u8]) -> Vec<u8> {
        match &self.encryption_key {
            Some(key) => xor_cipher(data, key),
            None => data.to_vec(),
        }
    }

    fn decrypt(&self, data: &[u8]) -> Vec<u8> {
        self.encrypt(data) // XOR is symmetric
    }

    fn next_seq(&mut self) -> u64 {
        self.seq += 1;
        self.seq
    }

    async fn send_msg(&self, socket: &UdpSocket, msg: &GossipMessage, addr: SocketAddr) {
        if let Ok(bytes) = msg.encode() {
            let encrypted = self.encrypt(&bytes);
            let _ = socket.send_to(&encrypted, addr).await;
        }
    }

    /// Run the SWIM gossip loop. Blocks until shutdown.
    pub async fn run(mut self) {
        let bind = format!("{}:{}", self.config.bind_addr, self.config.gossip_port);
        let socket = match UdpSocket::bind(&bind).await {
            Ok(s) => Arc::new(s),
            Err(e) => {
                warn!("[mesh] failed to bind gossip socket {bind}: {e}");
                return;
            }
        };
        info!(
            "[mesh] gossip listening on {} (worker={})",
            bind, self.local_info.worker_id
        );

        self.join_seeds(&socket).await;

        let period = std::time::Duration::from_millis(self.config.protocol_period_ms);
        let mut ticker = tokio::time::interval(period);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut recv_buf = vec![0u8; 2048];

        loop {
            tokio::select! {
                _ = self.shutdown.notified() => {
                    self.broadcast_leave(&socket).await;
                    break;
                }
                _ = ticker.tick() => {
                    self.protocol_tick(&socket).await;
                }
                result = socket.recv_from(&mut recv_buf) => {
                    match result {
                        Ok((len, from)) => {
                            self.handle_datagram(&recv_buf[..len], from, &socket).await;
                        }
                        Err(e) => {
                            debug!("[mesh] recv error: {e}");
                        }
                    }
                }
            }
        }

        info!("[mesh] gossip loop stopped");
    }

    /// Send join pings to seed nodes.
    async fn join_seeds(&mut self, socket: &UdpSocket) {
        for seed in &self.config.seeds.clone() {
            let seq = self.next_seq();
            let ping = GossipMessage::Ping {
                seq,
                from: self.local_info.worker_id.clone(),
                from_addr: self.local_info.gossip_addr,
            };
            let local_update = self.make_local_update();
            let msg = ping.with_updates(vec![local_update]);
            if let Ok(addr) = seed.parse::<SocketAddr>() {
                self.send_msg(socket, &msg, addr).await;
                debug!("[mesh] join ping sent to {seed}");
            }
        }
    }

    /// One SWIM protocol period: ping a random peer, check timeouts.
    async fn protocol_tick(&mut self, socket: &UdpSocket) {
        self.failure_detector.gc_stale_probes();
        // Stale relays: any seq older than current - 100 is expired
        let cutoff = self.seq.saturating_sub(100);
        self.pending_relays.retain(|&seq, _| seq > cutoff);

        let timed_out = self.failure_detector.check_ping_timeouts();
        for target_id in timed_out {
            self.initiate_indirect_probe(&target_id, socket).await;
        }

        let member_count = self.state.alive_count() + 1;
        let newly_dead = self.failure_detector.check_suspicion_timeouts(member_count);
        for dead_id in newly_dead {
            info!("[mesh] member {dead_id} declared dead (suspicion expired)");
            let (incarnation, info) = self
                .state
                .get_member(&dead_id)
                .map(|m| (m.incarnation, m.info))
                .unwrap_or((0, self.local_info.clone()));
            self.state.mark_dead(&dead_id);
            self.membership.queue_update(MemberUpdate {
                member_id: dead_id,
                state: MemberState::Dead,
                incarnation,
                info,
            });
        }

        if let Some(target) = self.pick_random_alive_peer() {
            let seq = self.next_seq();
            let ping = GossipMessage::Ping {
                seq,
                from: self.local_info.worker_id.clone(),
                from_addr: self.local_info.gossip_addr,
            };
            let updates = self.membership.take_updates();
            let msg = ping.with_updates(updates);
            self.send_msg(socket, &msg, target.info.gossip_addr).await;
            self.failure_detector
                .ping_sent(seq, target.info.worker_id.clone());
        }
    }

    /// Send PingReq to random peers asking them to probe the target.
    async fn initiate_indirect_probe(&mut self, target_id: &str, socket: &UdpSocket) {
        let peers = self.state.alive_peers();
        let intermediaries: Vec<&Member> = peers
            .iter()
            .filter(|m| m.info.worker_id != target_id)
            .take(self.config.indirect_ping_count)
            .collect();

        if intermediaries.is_empty() {
            if self.failure_detector.suspect(target_id) {
                info!("[mesh] member {target_id} suspected (no intermediaries)");
                let (incarnation, info) = self
                    .state
                    .get_member(target_id)
                    .map(|m| (m.incarnation, m.info))
                    .unwrap_or((0, self.local_info.clone()));
                self.membership.queue_update(MemberUpdate {
                    member_id: target_id.to_string(),
                    state: MemberState::Suspect,
                    incarnation,
                    info,
                });
            }
            return;
        }

        // Find target addr from state
        let target_addr = peers
            .iter()
            .find(|m| m.info.worker_id == target_id)
            .map(|m| m.info.gossip_addr);

        if let Some(addr) = target_addr {
            let seq = self.next_seq();
            for intermediary in intermediaries {
                let ping_req = GossipMessage::PingReq {
                    seq,
                    from: self.local_info.worker_id.clone(),
                    target: target_id.to_string(),
                    target_addr: addr,
                };
                self.send_msg(socket, &ping_req, intermediary.info.gossip_addr)
                    .await;
            }
            self.failure_detector
                .ping_req_sent(seq, target_id.to_string());
        } else if self.failure_detector.suspect(target_id) {
            info!("[mesh] member {target_id} suspected (no addr found)");
        }
    }

    /// Handle an incoming UDP datagram (decrypt if key set).
    async fn handle_datagram(&mut self, data: &[u8], from: SocketAddr, socket: &UdpSocket) {
        let decrypted = self.decrypt(data);
        let msg = match GossipMessage::decode(&decrypted) {
            Ok(m) => m,
            Err(e) => {
                debug!("[mesh] decode error from {from}: {e}");
                return;
            }
        };

        match msg {
            GossipMessage::Compound { primary, updates } => {
                self.apply_updates(&updates);
                self.handle_primary(*primary, from, socket).await;
            }
            GossipMessage::Sync { updates } => {
                self.apply_updates(&updates);
            }
            other => {
                self.handle_primary(other, from, socket).await;
            }
        }
    }

    async fn handle_primary(&mut self, msg: GossipMessage, from: SocketAddr, socket: &UdpSocket) {
        match msg {
            GossipMessage::Ping {
                seq, from: sender, ..
            } => {
                let ack = GossipMessage::Ack {
                    seq,
                    from: self.local_info.worker_id.clone(),
                };
                let local_update = self.make_local_update();
                let mut all_updates = vec![local_update];
                // Include all known alive peers so the pinger discovers them
                for peer in self.state.alive_peers() {
                    all_updates.push(MemberUpdate {
                        member_id: peer.info.worker_id.clone(),
                        state: peer.state,
                        incarnation: peer.incarnation,
                        info: peer.info,
                    });
                }
                all_updates.extend(self.membership.take_updates());
                let msg = ack.with_updates(all_updates);
                self.send_msg(socket, &msg, from).await;
                debug!("[mesh] ack sent to {sender} at {from}");
            }
            GossipMessage::Ack { seq, from: sender } => {
                if let Some((requester_id, requester_addr)) = self.pending_relays.remove(&seq) {
                    let relay = GossipMessage::AckRelay {
                        seq,
                        original_from: sender.clone(),
                        via: self.local_info.worker_id.clone(),
                    };
                    self.send_msg(socket, &relay, requester_addr).await;
                    debug!("[mesh] relayed ack from {sender} back to {requester_id}");
                }
                if let Some(resolved) = self.failure_detector.ack_received(seq) {
                    debug!("[mesh] ack from {sender} resolved probe for {resolved}");
                }
            }
            GossipMessage::PingReq {
                seq,
                from: requester,
                target,
                target_addr,
            } => {
                let relay_seq = self.next_seq();
                let ping = GossipMessage::Ping {
                    seq: relay_seq,
                    from: self.local_info.worker_id.clone(),
                    from_addr: self.local_info.gossip_addr,
                };
                self.pending_relays
                    .insert(relay_seq, (requester.clone(), from));
                self.send_msg(socket, &ping, target_addr).await;
                debug!("[mesh] relayed ping-req from {requester} to {target} (relay_seq={relay_seq}, orig_seq={seq})");
            }
            GossipMessage::AckRelay {
                seq, original_from, ..
            } => {
                if let Some(resolved) = self.failure_detector.ack_received(seq) {
                    debug!("[mesh] relay-ack from {original_from} resolved {resolved}");
                }
            }
            _ => {}
        }
    }

    fn apply_updates(&mut self, updates: &[MemberUpdate]) {
        for update in updates {
            if update.member_id == self.local_info.worker_id {
                if self.membership.handle_self_update(update) {
                    let refute = self.make_local_update();
                    self.membership.queue_update(refute);
                }
                continue;
            }

            let should_apply = match self.state.get_member(&update.member_id) {
                Some(existing) => self.membership.should_apply(update, &existing),
                None => true,
            };

            if should_apply {
                let is_new = self.state.upsert_member(Member {
                    info: update.info.clone(),
                    state: update.state,
                    incarnation: update.incarnation,
                });
                if is_new {
                    info!(
                        "[mesh] discovered peer {} at {}",
                        update.member_id, update.info.gossip_addr
                    );
                }
                self.membership.queue_update(update.clone());
            }

            if update.state == MemberState::Alive {
                self.failure_detector.clear_suspect(&update.member_id);
            }
        }
    }

    fn pick_random_alive_peer(&self) -> Option<Member> {
        let peers = self.state.alive_peers();
        if peers.is_empty() {
            return None;
        }
        let mut rng = rand::rng();
        peers.choose(&mut rng).cloned()
    }

    fn make_local_update(&self) -> MemberUpdate {
        MemberUpdate {
            member_id: self.local_info.worker_id.clone(),
            state: MemberState::Alive,
            incarnation: self.membership.local_incarnation(),
            info: self.local_info.clone(),
        }
    }

    /// Broadcast a Leave message to all known peers.
    async fn broadcast_leave(&mut self, socket: &UdpSocket) {
        let leave = MemberUpdate {
            member_id: self.local_info.worker_id.clone(),
            state: MemberState::Left,
            incarnation: self.membership.local_incarnation() + 1,
            info: self.local_info.clone(),
        };
        let msg = GossipMessage::Sync {
            updates: vec![leave],
        };
        for peer in self.state.alive_peers() {
            self.send_msg(socket, &msg, peer.info.gossip_addr).await;
        }
        info!("[mesh] leave broadcast sent");
    }
}
