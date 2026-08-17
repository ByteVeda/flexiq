pub mod protocol;
pub mod server;

use std::sync::Arc;
use std::time::Duration;

use log::{debug, warn};
use tokio::net::TcpStream;

use crate::state::Member;
use crate::MeshNode;

use self::protocol::{read_frame, write_frame, StealRequest, StealResponse};

/// Attempt to steal jobs from a peer worker over TCP.
/// Returns stolen jobs on success, empty vec on failure.
pub async fn steal_from_peer(
    mesh_node: &Arc<MeshNode>,
    target: &Member,
) -> Vec<flexiq_core::job::Job> {
    let addr = target.info.steal_addr;
    let max_count = mesh_node.config().max_steal_batch;
    let thief_id = mesh_node.state().local_worker_id().to_string();

    let stream =
        match tokio::time::timeout(Duration::from_millis(500), TcpStream::connect(addr)).await {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => {
                debug!("[mesh] steal connect to {addr} failed: {e}");
                return vec![];
            }
            Err(_) => {
                debug!("[mesh] steal connect to {addr} timed out");
                return vec![];
            }
        };

    let (mut reader, mut writer) = stream.into_split();

    let req = StealRequest {
        thief_id,
        max_count,
    };
    let req_bytes = match bincode::serialize(&req) {
        Ok(b) => b,
        Err(e) => {
            warn!("[mesh] steal serialize error: {e}");
            return vec![];
        }
    };

    if let Err(e) = write_frame(&mut writer, &req_bytes).await {
        debug!("[mesh] steal write error: {e}");
        return vec![];
    }

    let resp_frame =
        match tokio::time::timeout(Duration::from_secs(2), read_frame(&mut reader)).await {
            Ok(Ok(f)) => f,
            Ok(Err(e)) => {
                debug!("[mesh] steal read error: {e}");
                return vec![];
            }
            Err(_) => {
                debug!("[mesh] steal response timed out");
                return vec![];
            }
        };

    match bincode::deserialize::<StealResponse>(&resp_frame) {
        Ok(resp) => {
            if !resp.jobs.is_empty() {
                debug!(
                    "[mesh] stole {} jobs from {}",
                    resp.jobs.len(),
                    target.info.worker_id
                );
            }
            resp.jobs
        }
        Err(e) => {
            warn!("[mesh] steal deserialize error: {e}");
            vec![]
        }
    }
}
