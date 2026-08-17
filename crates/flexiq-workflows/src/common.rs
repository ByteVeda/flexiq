//! Backend-agnostic helpers shared by every `WorkflowStorage` implementation.
//!
//! Lives outside both `diesel_common.rs` (used by SQLite + Postgres) and any
//! per-backend module so that future backends — including non-Diesel ones
//! like Redis — can call into the same logic without pulling in their
//! neighbours' dependencies.

use std::collections::HashMap;

use flexiq_core::error::{QueueError, Result};

use crate::{WorkflowNode, WorkflowNodeStatus};

/// Filter `all_nodes` down to those whose status is `Pending` and whose DAG
/// predecessors are all `Completed`.
///
/// `dag_json` is the serialized `dagron_core::SerializableGraph` from the
/// workflow definition — caller is responsible for fetching it once per call.
/// Pure Rust; no I/O. Backends compose this with their own `get_workflow_nodes`
/// to implement `WorkflowStorage::get_ready_workflow_nodes`.
pub(crate) fn compute_ready_nodes(
    all_nodes: Vec<WorkflowNode>,
    dag_json: &str,
) -> Result<Vec<WorkflowNode>> {
    let graph: dagron_core::SerializableGraph = serde_json::from_str(dag_json).map_err(|e| {
        QueueError::Serialization(format!("failed to deserialize workflow DAG: {e}"))
    })?;

    let mut predecessors: HashMap<String, Vec<String>> = HashMap::new();
    for node in &graph.nodes {
        predecessors.entry(node.name.clone()).or_default();
    }
    for edge in &graph.edges {
        predecessors
            .entry(edge.to.clone())
            .or_default()
            .push(edge.from.clone());
    }

    let status_map: HashMap<String, WorkflowNodeStatus> = all_nodes
        .iter()
        .map(|n| (n.node_name.clone(), n.status))
        .collect();

    Ok(all_nodes
        .into_iter()
        .filter(|node| {
            if node.status != WorkflowNodeStatus::Pending {
                return false;
            }
            match predecessors.get(&node.node_name) {
                None => true,
                Some(preds) => preds.iter().all(|pred_name| {
                    status_map.get(pred_name.as_str()).copied()
                        == Some(WorkflowNodeStatus::Completed)
                }),
            }
        })
        .collect())
}
