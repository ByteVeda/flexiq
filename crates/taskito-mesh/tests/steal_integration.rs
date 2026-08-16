use std::sync::Arc;
use std::time::Duration;

use taskito_core::job::{now_millis, NewJob};
use taskito_mesh::config::MeshConfig;
use taskito_mesh::MeshNode;
use tokio::sync::Notify;

fn make_config(gossip_port: u16, steal_port: u16) -> MeshConfig {
    MeshConfig {
        gossip_port,
        steal_port,
        bind_addr: "127.0.0.1".to_string(),
        seeds: vec![],
        protocol_period_ms: 100,
        indirect_ping_count: 2,
        suspicion_multiplier: 2,
        virtual_nodes: 10,
        local_buffer_capacity: 32,
        max_steal_batch: 4,
        steal_threshold: 2,
        affinity_weight: 0.7,
        enable_stealing: true,
        advertise_addr: None,
        encryption_key: None,
        steal_rate_limit: 0,
    }
}

fn make_job(task_name: &str) -> taskito_core::job::Job {
    NewJob {
        queue: "default".to_string(),
        task_name: task_name.to_string(),
        payload: vec![1, 2, 3],
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

#[tokio::test]
async fn steal_transfers_jobs_between_nodes() {
    let victim_node = Arc::new(MeshNode::new(
        "victim".to_string(),
        make_config(19400, 19500),
    ));
    let thief_node = Arc::new(MeshNode::new(
        "thief".to_string(),
        make_config(19401, 19501),
    ));

    // Load victim with jobs
    let jobs: Vec<_> = (0..10).map(|i| make_job(&format!("task_{i}"))).collect();
    victim_node.prefetch(jobs);
    assert_eq!(victim_node.local_len(), 10);
    assert_eq!(thief_node.local_len(), 0);

    // Start victim's steal server
    let victim_shutdown = Arc::new(Notify::new());
    let vs = victim_shutdown.clone();
    let vn = victim_node.clone();
    let server_handle = tokio::spawn(async move {
        taskito_mesh::steal::server::run_steal_server(vn, vs).await;
    });

    // Give server time to bind
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Register victim as peer in thief's state so try_steal can find it
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use taskito_mesh::state::{Member, MemberState, WorkerInfo};
    thief_node.state().upsert_member(Member {
        info: WorkerInfo {
            worker_id: "victim".to_string(),
            gossip_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 19400),
            steal_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 19500),
            queues: vec!["default".to_string()],
            threads: 4,
            current_load: 0,
            local_buffer_len: 10,
            capacity: 4,
            updated_at: now_millis(),
        },
        state: MemberState::Alive,
        incarnation: 1,
    });

    // Thief steals from victim
    let stolen = thief_node.try_steal().await;
    assert!(stolen > 0, "should have stolen at least 1 job");
    assert!(stolen <= 4, "max_steal_batch is 4");

    assert_eq!(thief_node.local_len(), stolen);
    assert_eq!(victim_node.local_len(), 10 - stolen);

    let metrics = thief_node.metrics();
    assert_eq!(metrics.steals_initiated, 1);
    assert_eq!(metrics.steals_succeeded, 1);
    assert_eq!(metrics.jobs_stolen_in, stolen as u64);

    let victim_metrics = victim_node.metrics();
    assert_eq!(victim_metrics.jobs_stolen_out, stolen as u64);

    victim_shutdown.notify_one();
    let _ = server_handle.await;
}

#[tokio::test]
async fn steal_returns_empty_when_victim_has_no_jobs() {
    let victim_node = Arc::new(MeshNode::new(
        "victim-empty".to_string(),
        make_config(19402, 19502),
    ));
    let thief_node = Arc::new(MeshNode::new(
        "thief-empty".to_string(),
        make_config(19403, 19503),
    ));

    let victim_shutdown = Arc::new(Notify::new());
    let vs = victim_shutdown.clone();
    let vn = victim_node.clone();
    let server_handle = tokio::spawn(async move {
        taskito_mesh::steal::server::run_steal_server(vn, vs).await;
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use taskito_mesh::state::{Member, MemberState, WorkerInfo};
    thief_node.state().upsert_member(Member {
        info: WorkerInfo {
            worker_id: "victim-empty".to_string(),
            gossip_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 19402),
            steal_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 19502),
            queues: vec!["default".to_string()],
            threads: 4,
            current_load: 0,
            local_buffer_len: 10, // lie about buffer to trigger steal
            capacity: 4,
            updated_at: now_millis(),
        },
        state: MemberState::Alive,
        incarnation: 1,
    });

    let stolen = thief_node.try_steal().await;
    assert_eq!(stolen, 0);

    victim_shutdown.notify_one();
    let _ = server_handle.await;
}
