use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use log::{debug, info, warn};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, Notify};

use crate::MeshNode;

use super::protocol::{read_frame, write_frame, StealRequest, StealResponse};

/// TCP server that responds to steal requests from peer workers.
pub async fn run_steal_server(mesh_node: Arc<MeshNode>, shutdown: Arc<Notify>) {
    let bind = format!(
        "{}:{}",
        mesh_node.config().bind_addr,
        mesh_node.config().steal_port
    );
    let listener = match TcpListener::bind(&bind).await {
        Ok(l) => l,
        Err(e) => {
            warn!("[mesh] failed to bind steal server on {bind}: {e}");
            return;
        }
    };
    info!("[mesh] steal server listening on {bind}");

    let rate_limit = mesh_node.config().steal_rate_limit;
    let limiter = Arc::new(Mutex::new(StealRateLimiter::new(rate_limit)));

    loop {
        tokio::select! {
            _ = shutdown.notified() => break,
            result = listener.accept() => {
                match result {
                    Ok((stream, peer)) => {
                        let node = mesh_node.clone();
                        let lim = limiter.clone();
                        tokio::spawn(async move {
                            if let Err(e) = handle_steal(node, stream, lim).await {
                                debug!("[mesh] steal handler error from {peer}: {e}");
                            }
                        });
                    }
                    Err(e) => {
                        debug!("[mesh] accept error: {e}");
                    }
                }
            }
        }
    }

    info!("[mesh] steal server stopped");
}

async fn handle_steal(
    mesh_node: Arc<MeshNode>,
    stream: tokio::net::TcpStream,
    limiter: Arc<Mutex<StealRateLimiter>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let (mut reader, mut writer) = stream.into_split();
    let frame = read_frame(&mut reader).await?;
    let req: StealRequest = bincode::deserialize(&frame)?;

    let allowed = {
        let mut lim = limiter.lock().await;
        lim.allow(&req.thief_id)
    };

    let stolen = if allowed {
        mesh_node.give_jobs(req.max_count)
    } else {
        debug!("[mesh] rate-limited steal from {}", req.thief_id);
        vec![]
    };

    debug!(
        "[mesh] giving {} jobs to thief {}",
        stolen.len(),
        req.thief_id
    );

    let resp = StealResponse { jobs: stolen };
    let resp_bytes = bincode::serialize(&resp)?;
    write_frame(&mut writer, &resp_bytes).await?;

    Ok(())
}

/// Simple per-peer rate limiter: max N requests per second per peer.
struct StealRateLimiter {
    max_per_second: u32,
    /// peer_id → list of request timestamps in the last second.
    windows: HashMap<String, Vec<Instant>>,
}

impl StealRateLimiter {
    fn new(max_per_second: u32) -> Self {
        Self {
            max_per_second,
            windows: HashMap::new(),
        }
    }

    fn allow(&mut self, peer_id: &str) -> bool {
        if self.max_per_second == 0 {
            return true;
        }
        let now = Instant::now();
        let window = Duration::from_secs(1);
        let timestamps = self.windows.entry(peer_id.to_string()).or_default();
        timestamps.retain(|t| now.duration_since(*t) < window);
        if timestamps.len() >= self.max_per_second as usize {
            return false;
        }
        timestamps.push(now);
        true
    }
}
