//! Peer-to-peer scheduling for FlexiQ workers.
//!
//! Workers that share a database already share work; this crate lets them
//! share it *sooner*. Each node gossips its address, queues and load to its
//! peers over UDP, places jobs on a consistent hash ring so the same key tends
//! to reach the same worker, buffers a small prefetch locally, and steals from
//! a busier peer over TCP when its own buffer runs dry.
//!
//! **The database stays the source of truth.** Nothing here claims, completes
//! or retries a job — a stolen job is one already claimed by the peer that
//! hands it over, and a node that dies loses only its prefetch buffer, which
//! the ordinary stuck-job reaper returns to the queue. That is what makes the
//! mesh safe to enable on some workers and not others.
//!
//! # Shape
//!
//! [`MeshNode`] owns the four pieces and is the only type an SDK holds:
//!
//! - [`swim`] — SWIM gossip: membership, failure detection, the wire messages.
//! - [`ring`] — the consistent hash ring, for affinity rather than ownership.
//! - [`local_deque`] — the bounded prefetch buffer a steal draws from.
//! - [`steal`] — the TCP work-stealing client and server.
//!
//! # Using it
//!
//! An SDK constructs a node, spawns the two listeners, and then drives it
//! alongside its own scheduler loop — [`MeshNode::should_prefetch`],
//! [`MeshNode::prefetch`], [`MeshNode::pop_local`] and
//! [`MeshNode::try_steal`]. The scheduler itself never learns the mesh exists,
//! which is why enabling it changes no dispatch semantics.
//!
//! ```no_run
//! use flexiq_mesh::{MeshConfig, MeshNode};
//! use std::sync::Arc;
//!
//! let node = Arc::new(MeshNode::new("worker-1".to_string(), MeshConfig::default()));
//! node.spawn_gossip(vec!["default".to_string()], 4);
//! node.spawn_steal_server();
//! ```
//!
//! Reached from the `flexiq` facade behind its `mesh` feature; nothing pays for
//! a gossip mesh unasked.

pub mod config;
pub mod local_deque;
pub mod metrics;
pub mod ring;
pub mod state;
pub mod steal;
pub mod swim;

use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use flexiq_core::job::Job;
use tokio::sync::Notify;

pub use config::MeshConfig;
pub use local_deque::LocalDeque;
pub use metrics::{MeshMetrics, MetricsSnapshot};
pub use state::{MemberState, MeshState, WorkerInfo};

/// Snapshot of mesh cluster state for observability.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ClusterInfo {
    pub peer_count: usize,
    pub total_capacity: u16,
    pub total_load: u16,
    pub total_buffered: u16,
    pub local_buffer_len: u16,
    pub adaptive_prefetch: u16,
}

/// Manages local deque, consistent-hash ring, SWIM gossip, and work-stealing.
pub struct MeshNode {
    config: MeshConfig,
    state: Arc<MeshState>,
    deque: LocalDeque,
    metrics: Arc<MeshMetrics>,
    shutdown: Arc<Notify>,
}

impl MeshNode {
    pub fn new(worker_id: String, config: MeshConfig) -> Self {
        let state = Arc::new(MeshState::new(worker_id, config.virtual_nodes));
        let deque = LocalDeque::new(config.local_buffer_capacity);
        Self {
            config,
            state,
            deque,
            metrics: Arc::new(MeshMetrics::default()),
            shutdown: Arc::new(Notify::new()),
        }
    }

    /// Spawn the SWIM gossip loop as a tokio task.
    /// Call this inside the tokio runtime before the scheduler loop.
    pub fn spawn_gossip(&self, queues: Vec<String>, threads: u16) -> tokio::task::JoinHandle<()> {
        let advertise_ip = self.config.advertise_ip();
        let gossip_addr = SocketAddr::new(advertise_ip, self.config.gossip_port);
        let steal_addr = SocketAddr::new(advertise_ip, self.config.steal_port);
        let local_info = WorkerInfo {
            worker_id: self.state.local_worker_id().to_string(),
            gossip_addr,
            steal_addr,
            queues,
            threads,
            current_load: 0,
            local_buffer_len: 0,
            capacity: threads,
            updated_at: flexiq_core::job::now_millis(),
        };

        let swim = swim::SwimNode::new(
            self.config.clone(),
            self.state.clone(),
            local_info,
            self.shutdown.clone(),
        );

        tokio::spawn(async move {
            swim.run().await;
        })
    }

    /// Spawn the TCP steal server as a tokio task.
    pub fn spawn_steal_server(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let node = self.clone();
        let shutdown = self.shutdown.clone();
        tokio::spawn(async move {
            steal::server::run_steal_server(node, shutdown).await;
        })
    }

    /// Attempt to steal work from the busiest peer.
    /// Returns stolen jobs pushed into local deque.
    pub async fn try_steal(self: &Arc<Self>) -> usize {
        if !self.should_steal() {
            return 0;
        }
        let min_surplus = self.config.steal_threshold + self.config.max_steal_batch;
        let target = match self.state.best_steal_target(min_surplus) {
            Some(t) => t,
            None => return 0,
        };

        self.metrics
            .steals_initiated
            .fetch_add(1, Ordering::Relaxed);
        let stolen = steal::steal_from_peer(self, &target).await;
        let count = stolen.len();
        if count > 0 {
            self.metrics
                .steals_succeeded
                .fetch_add(1, Ordering::Relaxed);
            self.metrics
                .jobs_stolen_in
                .fetch_add(count as u64, Ordering::Relaxed);
            self.prefetch(stolen);
        }
        count
    }

    /// Signal the gossip loop and steal server to stop.
    pub fn request_shutdown(&self) {
        self.shutdown.notify_one();
    }

    pub fn config(&self) -> &MeshConfig {
        &self.config
    }

    pub fn state(&self) -> &Arc<MeshState> {
        &self.state
    }

    pub fn metrics(&self) -> MetricsSnapshot {
        self.metrics.snapshot()
    }

    // ── Local deque operations ──────────────────────────────────────────

    /// Pop a job from the local deque (front/hot end).
    /// Returns `None` if the deque is empty.
    pub fn pop_local(&self) -> Option<Job> {
        let job = self.deque.pop();
        if job.is_some() {
            self.metrics.local_pops.fetch_add(1, Ordering::Relaxed);
        }
        job
    }

    /// Push a batch of jobs into the local deque, sorted by affinity.
    /// Affinity-owned tasks go to the front (popped first), non-owned
    /// settle at the back (stealable).
    pub fn prefetch(&self, jobs: Vec<Job>) -> usize {
        if jobs.is_empty() {
            return 0;
        }
        self.metrics.prefetch_count.fetch_add(1, Ordering::Relaxed);
        let state = self.state.clone();
        let pushed = self
            .deque
            .push_sorted(jobs, |task_name| state.is_local_owner(task_name));
        self.metrics
            .prefetch_jobs
            .fetch_add(pushed as u64, Ordering::Relaxed);
        pushed
    }

    /// Whether the local deque has room for more prefetched jobs.
    pub fn should_prefetch(&self) -> bool {
        self.deque.len() < self.config.local_buffer_capacity / 2
    }

    /// Adaptive prefetch budget based on this worker's share of total
    /// mesh capacity. With no peers, returns the full local_buffer_capacity.
    /// With peers, scales proportionally: capacity / total_capacity.
    pub fn adaptive_prefetch_size(&self) -> usize {
        let peers = self.state.alive_peers();
        if peers.is_empty() {
            return self.config.local_buffer_capacity;
        }
        let my_capacity = self.config.local_buffer_capacity as f64;
        let total: f64 = peers.iter().map(|p| p.info.capacity as f64).sum::<f64>() + my_capacity;
        let share = my_capacity / total;
        let budget = (my_capacity * share * 2.0).ceil() as usize;
        budget.max(1).min(self.config.local_buffer_capacity)
    }

    /// Jitter delay to stagger DB polls across mesh peers.
    /// Returns 0..protocol_period_ms based on position in the hash ring.
    pub fn poll_jitter_ms(&self) -> u64 {
        let worker_id = self.state.local_worker_id();
        let hash = xxhash_rust::xxh3::xxh3_64(worker_id.as_bytes());
        hash % self.config.protocol_period_ms.max(1)
    }

    /// Summary of mesh cluster state for observability.
    pub fn cluster_info(&self) -> ClusterInfo {
        let peers = self.state.alive_peers();
        let total_capacity: u16 = peers.iter().map(|p| p.info.capacity).sum();
        let total_load: u16 = peers.iter().map(|p| p.info.current_load).sum();
        let total_buffered: u16 = peers.iter().map(|p| p.info.local_buffer_len).sum();
        ClusterInfo {
            peer_count: peers.len(),
            total_capacity,
            total_load,
            total_buffered,
            local_buffer_len: self.deque.len() as u16,
            adaptive_prefetch: self.adaptive_prefetch_size() as u16,
        }
    }

    /// Whether the local deque has jobs ready to dispatch.
    pub fn has_local_work(&self) -> bool {
        !self.deque.is_empty()
    }

    /// Current local deque length.
    pub fn local_len(&self) -> usize {
        self.deque.len()
    }

    // ── Affinity queries ────────────────────────────────────────────────

    /// Check if this worker is the affinity owner for a task name.
    pub fn is_affinity_owner(&self, task_name: &str) -> bool {
        self.state.is_local_owner(task_name)
    }

    // ── Steal operations (Phase 4 — stubs for now) ──────────────────────

    /// Steal up to `count` jobs from the cold end of the local deque.
    /// Used by the steal server to respond to steal requests.
    pub fn give_jobs(&self, count: usize) -> Vec<Job> {
        let stolen = self.deque.steal(count);
        self.metrics
            .jobs_stolen_out
            .fetch_add(stolen.len() as u64, Ordering::Relaxed);
        stolen
    }

    /// Whether this node should attempt to steal work from a peer.
    pub fn should_steal(&self) -> bool {
        self.config.enable_stealing && self.deque.len() <= self.config.steal_threshold
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flexiq_core::job::{now_millis, NewJob};

    fn make_job(task_name: &str) -> Job {
        NewJob {
            queue: "default".to_string(),
            task_name: task_name.to_string(),
            payload: vec![],
            priority: 0,
            scheduled_at: now_millis(),
            max_retries: 0,
            timeout_ms: 30_000,
            unique_key: None,
            metadata: None,
            notes: None,
            depends_on: vec![],
            expires_at: None,
            result_ttl_ms: None,
            namespace: None,
            debounce_key: None,
        }
        .into_job()
    }

    #[test]
    fn prefetch_and_pop() {
        let node = MeshNode::new("worker-1".to_string(), MeshConfig::default());
        let jobs = vec![make_job("task_a"), make_job("task_b"), make_job("task_c")];

        let pushed = node.prefetch(jobs);
        assert_eq!(pushed, 3);
        assert!(node.has_local_work());

        let metrics = node.metrics();
        assert_eq!(metrics.prefetch_count, 1);
        assert_eq!(metrics.prefetch_jobs, 3);

        let j1 = node.pop_local().unwrap();
        assert!(!j1.task_name.is_empty());

        let metrics = node.metrics();
        assert_eq!(metrics.local_pops, 1);
    }

    #[test]
    fn should_prefetch_tracks_capacity() {
        let config = MeshConfig {
            local_buffer_capacity: 4,
            ..MeshConfig::default()
        };
        let node = MeshNode::new("worker-1".to_string(), config);
        assert!(node.should_prefetch()); // empty = should prefetch

        node.prefetch(vec![make_job("a"), make_job("b"), make_job("c")]);
        assert!(!node.should_prefetch()); // 3/4 > capacity/2
    }

    #[test]
    fn should_steal_respects_config() {
        let config = MeshConfig {
            enable_stealing: false,
            ..MeshConfig::default()
        };
        let node = MeshNode::new("worker-1".to_string(), config);
        assert!(!node.should_steal());

        let config2 = MeshConfig {
            enable_stealing: true,
            steal_threshold: 2,
            ..MeshConfig::default()
        };
        let node2 = MeshNode::new("worker-2".to_string(), config2);
        assert!(node2.should_steal()); // empty deque ≤ threshold
    }

    #[test]
    fn adaptive_prefetch_no_peers() {
        let config = MeshConfig {
            local_buffer_capacity: 64,
            ..MeshConfig::default()
        };
        let node = MeshNode::new("solo".to_string(), config);
        assert_eq!(node.adaptive_prefetch_size(), 64);
    }

    #[test]
    fn adaptive_prefetch_scales_with_peers() {
        use std::net::{IpAddr, Ipv4Addr, SocketAddr};
        let config = MeshConfig {
            local_buffer_capacity: 64,
            ..MeshConfig::default()
        };
        let node = MeshNode::new("w1".to_string(), config);

        // Add 3 peers with capacity 64 each — total 256, share = 1/4
        for i in 0..3 {
            node.state().upsert_member(state::Member {
                info: WorkerInfo {
                    worker_id: format!("peer-{i}"),
                    gossip_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7946 + i),
                    steal_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8946 + i),
                    queues: vec!["default".to_string()],
                    threads: 4,
                    current_load: 0,
                    local_buffer_len: 0,
                    capacity: 64,
                    updated_at: 0,
                },
                state: MemberState::Alive,
                incarnation: 1,
            });
        }

        let size = node.adaptive_prefetch_size();
        assert!(size < 64, "with 4 workers, prefetch should be < solo");
        assert!(size >= 1, "prefetch must be at least 1");
    }

    #[test]
    fn poll_jitter_within_bounds() {
        let config = MeshConfig {
            protocol_period_ms: 500,
            ..MeshConfig::default()
        };
        let node = MeshNode::new("w1".to_string(), config);
        let jitter = node.poll_jitter_ms();
        assert!(jitter < 500);
    }

    #[test]
    fn cluster_info_reflects_state() {
        use std::net::{IpAddr, Ipv4Addr, SocketAddr};
        let node = MeshNode::new("w1".to_string(), MeshConfig::default());
        node.state().upsert_member(state::Member {
            info: WorkerInfo {
                worker_id: "peer-a".to_string(),
                gossip_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7946),
                steal_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7947),
                queues: vec!["default".to_string()],
                threads: 4,
                current_load: 2,
                local_buffer_len: 5,
                capacity: 4,
                updated_at: 0,
            },
            state: MemberState::Alive,
            incarnation: 1,
        });

        let info = node.cluster_info();
        assert_eq!(info.peer_count, 1);
        assert_eq!(info.total_capacity, 4);
        assert_eq!(info.total_load, 2);
        assert_eq!(info.total_buffered, 5);
    }
}
