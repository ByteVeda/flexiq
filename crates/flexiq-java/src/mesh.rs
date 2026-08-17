//! Mesh scheduling bridge (feature `mesh`).
//!
//! When a worker is started with a `meshConfig`, [`spawn_mesh`] interposes a
//! [`flexiq_mesh::MeshNode`] between the scheduler and the dispatcher: the
//! scheduler emits ready jobs into an intermediate channel, the bridge sorts
//! them into the node's affinity-aware local deque (and steals from busy peers),
//! then drains the deque to the real dispatcher channel. The DB stays the source
//! of truth, so the scheduler and dispatcher remain mesh-unaware — identical to
//! the Python and Node shells.

use std::sync::Arc;

use flexiq_core::job::Job;
use flexiq_core::scheduler::Scheduler;
use flexiq_mesh::{MeshConfig, MeshNode};
use tokio::sync::mpsc::Sender;

/// Build the mesh node, start its gossip + steal-server tasks and the bridge
/// loop on `runtime`, and return the node so the worker can request shutdown and
/// read cluster info. `threads` is advertised to peers as this worker's capacity.
pub fn spawn_mesh(
    runtime: &tokio::runtime::Runtime,
    scheduler: Arc<Scheduler>,
    job_tx: Sender<Job>,
    config_json: &str,
    worker_id: String,
    queues: Vec<String>,
    threads: u16,
) -> Arc<MeshNode> {
    let config: MeshConfig = serde_json::from_str(config_json).unwrap_or_else(|e| {
        log::error!("[flexiq-java] invalid mesh config, falling back to defaults: {e}");
        MeshConfig::default()
    });
    log::info!(
        "[flexiq-java] mesh scheduling enabled (gossip_port={}, local_buffer={})",
        config.gossip_port,
        config.local_buffer_capacity,
    );
    let node = Arc::new(MeshNode::new(worker_id, config));

    // `spawn_gossip`/`spawn_steal_server` call the free `tokio::spawn` internally,
    // so they must run inside the runtime context (we are on the JNI thread here).
    let (gossip, steal_server) = {
        let _guard = runtime.enter();
        (
            node.spawn_gossip(queues, threads),
            node.spawn_steal_server(),
        )
    };

    let bridge_node = node.clone();
    runtime.spawn(async move {
        run_mesh_bridge(scheduler, bridge_node, job_tx).await;
    });
    // Keep the gossip + steal-server tasks alive for the worker's lifetime; they
    // stop when the node's shutdown is signalled (see `request_shutdown`).
    runtime.spawn(async move {
        let (gossip_res, steal_res) = tokio::join!(gossip, steal_server);
        if let Err(e) = gossip_res {
            log::error!("[flexiq-java] mesh gossip task failed: {e}");
        }
        if let Err(e) = steal_res {
            log::error!("[flexiq-java] mesh steal-server task failed: {e}");
        }
    });

    node
}

/// Pump jobs from the scheduler through the mesh local deque to the dispatcher.
async fn run_mesh_bridge(scheduler: Arc<Scheduler>, mesh_node: Arc<MeshNode>, job_tx: Sender<Job>) {
    let (mesh_tx, mut mesh_rx) = tokio::sync::mpsc::channel::<Job>(64);

    let sched = scheduler.clone();
    let sched_task = tokio::spawn(async move {
        sched.run(mesh_tx).await;
    });

    loop {
        // Drain the local deque to the dispatcher first.
        while let Some(job) = mesh_node.pop_local() {
            if job_tx.send(job).await.is_err() {
                let _ = sched_task.await;
                return;
            }
        }

        // Steal from a busier peer when our deque is low.
        if mesh_node.should_steal() {
            mesh_node.try_steal().await;
            while let Some(job) = mesh_node.pop_local() {
                if job_tx.send(job).await.is_err() {
                    let _ = sched_task.await;
                    return;
                }
            }
        }

        // Wait for the scheduler to produce jobs, then prefetch the batch.
        match mesh_rx.recv().await {
            Some(job) => {
                let mut batch = vec![job];
                while let Ok(extra) = mesh_rx.try_recv() {
                    batch.push(extra);
                }
                mesh_node.prefetch(batch);
            }
            None => break,
        }
    }

    // Scheduler done: drain whatever remains in the deque.
    while let Some(job) = mesh_node.pop_local() {
        if job_tx.send(job).await.is_err() {
            break;
        }
    }

    let _ = sched_task.await;
}
