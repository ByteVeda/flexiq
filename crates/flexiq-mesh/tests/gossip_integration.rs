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

/// The relayed ack has to come back under the *requester's* probe number.
///
/// Seq counters are per node, so an `AckRelay` carrying the intermediary's own
/// relay seq is unmatchable at the requester — and worse than unmatchable, since
/// it can collide with an unrelated pending direct ping and resolve that one
/// instead. Driven with raw sockets: one real node as the intermediary, and
/// fake requester/target sockets so the seq under test is one we chose.
#[tokio::test]
async fn ping_req_relay_echoes_the_requester_seq() {
    use flexiq_mesh::swim::message::GossipMessage;
    use tokio::net::UdpSocket;

    let port_i = 19400;
    let requester = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let target = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let target_addr = target.local_addr().unwrap();
    let intermediary_addr: SocketAddr = format!("127.0.0.1:{port_i}").parse().unwrap();

    let shutdown = Arc::new(Notify::new());
    let swim = SwimNode::new(
        make_config(port_i, vec![]),
        Arc::new(MeshState::new("node-i".to_string(), 10)),
        make_info("node-i", port_i),
        shutdown.clone(),
    );
    let handle = tokio::spawn(async move { swim.run().await });
    tokio::time::sleep(Duration::from_millis(100)).await;

    // A number the intermediary's own counter will not reach on its own.
    const REQUESTER_SEQ: u64 = 4242;
    let ping_req = GossipMessage::PingReq {
        seq: REQUESTER_SEQ,
        from: "node-req".to_string(),
        target: "node-tgt".to_string(),
        target_addr,
    };
    requester
        .send_to(&ping_req.encode().unwrap(), intermediary_addr)
        .await
        .unwrap();

    // The intermediary forwards a Ping under a seq of its own; answer that one.
    let mut buf = [0u8; 2048];
    let (n, from) = tokio::time::timeout(Duration::from_millis(20_000), target.recv_from(&mut buf))
        .await
        .expect("intermediary never relayed the ping")
        .unwrap();
    let relay_seq = match GossipMessage::decode(&buf[..n]).unwrap() {
        GossipMessage::Ping { seq, .. } => seq,
        other => panic!("expected a relayed Ping, got {other:?}"),
    };
    assert_ne!(
        relay_seq, REQUESTER_SEQ,
        "the intermediary must mint its own seq, or this test proves nothing"
    );
    let ack = GossipMessage::Ack {
        seq: relay_seq,
        from: "node-tgt".to_string(),
    };
    target.send_to(&ack.encode().unwrap(), from).await.unwrap();

    let (n, _) = tokio::time::timeout(Duration::from_millis(20_000), requester.recv_from(&mut buf))
        .await
        .expect("intermediary never relayed the ack back")
        .unwrap();
    match GossipMessage::decode(&buf[..n]).unwrap() {
        GossipMessage::AckRelay { seq, .. } => assert_eq!(
            seq, REQUESTER_SEQ,
            "AckRelay must echo the requester's seq, not the relay seq"
        ),
        other => panic!("expected an AckRelay, got {other:?}"),
    }

    shutdown.notify_one();
    let _ = handle.await;
}

/// Poll until `cond` holds. The budget is a failure deadline, not a delay — the
/// assertions below settle in well under a second, so a generous ceiling costs
/// nothing on a green run and keeps a slow CI box from reading as a bug.
async fn wait_until(label: &str, mut cond: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + Duration::from_millis(20_000);
    while std::time::Instant::now() < deadline {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for {label}");
}

/// A node that dies without announcing it must still be evicted.
///
/// Three nodes, so the direct probe escalates to a real `PingReq` rather than
/// taking the two-node "no intermediaries" shortcut. That escalation is the
/// path where an unanswered indirect probe used to be dropped on the floor,
/// leaving a dead peer `Alive` on every survivor's ring forever.
#[tokio::test]
async fn abrupt_death_is_detected() {
    let port_a = 19500;
    let port_b = 19501;
    let port_c = 19502;

    let state_a = Arc::new(MeshState::new("node-a".to_string(), 10));
    let state_b = Arc::new(MeshState::new("node-b".to_string(), 10));
    let state_c = Arc::new(MeshState::new("node-c".to_string(), 10));

    let shutdown_a = Arc::new(Notify::new());
    let shutdown_b = Arc::new(Notify::new());
    let shutdown_c = Arc::new(Notify::new());

    let seed = vec![format!("127.0.0.1:{port_a}")];
    let swim_a = SwimNode::new(
        make_config(port_a, vec![]),
        state_a.clone(),
        make_info("node-a", port_a),
        shutdown_a.clone(),
    );
    let swim_b = SwimNode::new(
        make_config(port_b, seed.clone()),
        state_b.clone(),
        make_info("node-b", port_b),
        shutdown_b.clone(),
    );
    let swim_c = SwimNode::new(
        make_config(port_c, seed),
        state_c.clone(),
        make_info("node-c", port_c),
        shutdown_c.clone(),
    );

    let ha = tokio::spawn(async move { swim_a.run().await });
    let hb = tokio::spawn(async move { swim_b.run().await });
    let hc = tokio::spawn(async move { swim_c.run().await });

    wait_until("all three nodes to find each other", || {
        state_a.alive_count() == 2 && state_b.alive_count() == 2
    })
    .await;

    // Abort rather than notify: a graceful shutdown broadcasts `Left`, which is
    // the path that already worked and would hide the bug under test.
    hc.abort();

    wait_until("node-a to stop counting node-c as alive", || {
        state_a.alive_count() == 1
    })
    .await;
    wait_until("node-b to stop counting node-c as alive", || {
        state_b.alive_count() == 1
    })
    .await;

    shutdown_a.notify_one();
    shutdown_b.notify_one();
    let _ = ha.await;
    let _ = hb.await;
}

/// The node that raises a suspicion must act on it, not just announce it.
///
/// Two nodes, so there is nobody to relay the `Suspect` update back and no
/// intermediary to probe through — the surviving node's own view is the only
/// thing that can drop the peer. The suspicion multiplier is set so high that
/// the `Dead` timer cannot expire inside the wait budget, which is what rules
/// out the survivor merely waiting the peer out.
#[tokio::test]
async fn a_suspicion_leaves_the_suspecting_node_s_own_view() {
    let port_a = 19600;
    let port_b = 19601;

    let mut config_a = make_config(port_a, vec![]);
    config_a.suspicion_multiplier = 600;
    let mut config_b = make_config(port_b, vec![format!("127.0.0.1:{port_a}")]);
    config_b.suspicion_multiplier = 600;

    let state_a = Arc::new(MeshState::new("node-a".to_string(), 10));
    let state_b = Arc::new(MeshState::new("node-b".to_string(), 10));
    let shutdown_a = Arc::new(Notify::new());
    let shutdown_b = Arc::new(Notify::new());

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

    wait_until("the two nodes to find each other", || {
        state_a.alive_count() == 1
    })
    .await;

    hb.abort();

    wait_until("node-a to drop node-b from its own alive set", || {
        state_a.alive_count() == 0
    })
    .await;

    shutdown_a.notify_one();
    let _ = ha.await;
}
