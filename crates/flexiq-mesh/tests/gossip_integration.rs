use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use flexiq_mesh::config::MeshConfig;
use flexiq_mesh::state::{MeshState, WorkerInfo};
use flexiq_mesh::swim::SwimNode;
use tokio::sync::Notify;

fn make_config(port: u16, seeds: Vec<String>) -> MeshConfig {
    MeshConfig {
        gossip_port: port,
        steal_port: port + 100,
        bind_addr: "127.0.0.1".to_string(),
        seeds,
        protocol_period_ms: 100,
        indirect_ping_count: 2,
        suspicion_multiplier: 2,
        virtual_nodes: 10,
        local_buffer_capacity: 16,
        max_steal_batch: 4,
        steal_threshold: 2,
        affinity_weight: 0.7,
        enable_stealing: false,
        advertise_addr: None,
        encryption_key: None,
        steal_rate_limit: 10,
    }
}

fn make_info(id: &str, port: u16) -> WorkerInfo {
    WorkerInfo {
        worker_id: id.to_string(),
        gossip_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port),
        steal_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port + 100),
        queues: vec!["default".to_string()],
        threads: 4,
        current_load: 0,
        local_buffer_len: 0,
        capacity: 4,
        updated_at: 0,
    }
}

#[tokio::test]
async fn two_nodes_discover_each_other() {
    let port_a = 19100;
    let port_b = 19101;

    let state_a = Arc::new(MeshState::new("node-a".to_string(), 10));
    let state_b = Arc::new(MeshState::new("node-b".to_string(), 10));

    let shutdown_a = Arc::new(Notify::new());
    let shutdown_b = Arc::new(Notify::new());

    let config_a = make_config(port_a, vec![]);
    let config_b = make_config(port_b, vec![format!("127.0.0.1:{port_a}")]);

    let swim_a = SwimNode::new(
        config_a,
        state_a.clone(),
        make_info("node-a", port_a),
        shutdown_a.clone(),
    );
    let swim_b = SwimNode::new(
        config_b,
        state_b.clone(),
        make_info("node-b", port_b),
        shutdown_b.clone(),
    );

    let ha = tokio::spawn(async move { swim_a.run().await });
    let hb = tokio::spawn(async move { swim_b.run().await });

    // Wait for convergence (2-3 protocol periods)
    tokio::time::sleep(Duration::from_millis(500)).await;

    assert_eq!(state_a.alive_count(), 1, "node-a should see node-b");
    assert_eq!(state_b.alive_count(), 1, "node-b should see node-a");

    shutdown_a.notify_one();
    shutdown_b.notify_one();
    let _ = tokio::join!(ha, hb);
}

#[tokio::test]
async fn three_nodes_converge_via_piggyback() {
    let port_a = 19200;
    let port_b = 19201;
    let port_c = 19202;

    let state_a = Arc::new(MeshState::new("node-a".to_string(), 10));
    let state_b = Arc::new(MeshState::new("node-b".to_string(), 10));
    let state_c = Arc::new(MeshState::new("node-c".to_string(), 10));

    let shutdown_a = Arc::new(Notify::new());
    let shutdown_b = Arc::new(Notify::new());
    let shutdown_c = Arc::new(Notify::new());

    // b seeds from a, c seeds from a — c discovers b via piggybacked updates
    let config_a = make_config(port_a, vec![]);
    let config_b = make_config(port_b, vec![format!("127.0.0.1:{port_a}")]);
    let config_c = make_config(port_c, vec![format!("127.0.0.1:{port_a}")]);

    let swim_a = SwimNode::new(
        config_a,
        state_a.clone(),
        make_info("node-a", port_a),
        shutdown_a.clone(),
    );
    let swim_b = SwimNode::new(
        config_b,
        state_b.clone(),
        make_info("node-b", port_b),
        shutdown_b.clone(),
    );
    let swim_c = SwimNode::new(
        config_c,
        state_c.clone(),
        make_info("node-c", port_c),
        shutdown_c.clone(),
    );

    let ha = tokio::spawn(async move { swim_a.run().await });
    let hb = tokio::spawn(async move { swim_b.run().await });
    let hc = tokio::spawn(async move { swim_c.run().await });

    // Piggybacked dissemination takes multiple rounds: b→a→c and c→a→b
    tokio::time::sleep(Duration::from_millis(1500)).await;

    assert_eq!(state_a.alive_count(), 2, "node-a should see b and c");
    assert_eq!(state_b.alive_count(), 2, "node-b should see a and c");
    assert_eq!(state_c.alive_count(), 2, "node-c should see a and b");

    shutdown_a.notify_one();
    shutdown_b.notify_one();
    shutdown_c.notify_one();
    let _ = tokio::join!(ha, hb, hc);
}

#[tokio::test]
async fn graceful_leave_removes_from_peers() {
    let port_a = 19300;
    let port_b = 19301;

    let state_a = Arc::new(MeshState::new("node-a".to_string(), 10));
    let state_b = Arc::new(MeshState::new("node-b".to_string(), 10));

    let shutdown_a = Arc::new(Notify::new());
    let shutdown_b = Arc::new(Notify::new());

    let config_a = make_config(port_a, vec![]);
    let config_b = make_config(port_b, vec![format!("127.0.0.1:{port_a}")]);

    let swim_a = SwimNode::new(
        config_a,
        state_a.clone(),
        make_info("node-a", port_a),
        shutdown_a.clone(),
    );
    let swim_b = SwimNode::new(
        config_b,
        state_b.clone(),
        make_info("node-b", port_b),
        shutdown_b.clone(),
    );

    let ha = tokio::spawn(async move { swim_a.run().await });
    let hb = tokio::spawn(async move { swim_b.run().await });

    // Wait for discovery
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(state_a.alive_count(), 1);
    assert_eq!(state_b.alive_count(), 1);

    // Node B leaves gracefully
    shutdown_b.notify_one();
    let _ = hb.await;

    // Give node A time to process the leave broadcast
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(state_a.alive_count(), 0, "node-a should see node-b as left");

    shutdown_a.notify_one();
    let _ = ha.await;
}
