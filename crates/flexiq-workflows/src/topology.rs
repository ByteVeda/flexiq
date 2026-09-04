//! Reading a stored graph back into an order something can be run in.
//!
//! [`topological_order`] is the seam between `dagron_core`'s graph algebra and
//! the rows this crate persists: it decodes a definition's
//! [`SerializableGraph`] and returns each node with its dependencies attached,
//! which is what both submission (what may be enqueued now) and completion
//! (what this node unblocked) ask of it. A cycle is refused here rather than
//! discovered by a run that never finishes.

use std::collections::HashMap;

use flexiq_core::error::{QueueError, Result};

use dagron_core::{SerializableGraph, DAG};

/// A node in a workflow DAG, returned in topological order.
#[derive(Debug, Clone)]
pub struct TopologicalNode {
    /// The node's name in the DAG, matching its `step_metadata` key.
    pub name: String,
    /// Names of the node's direct predecessors — the edges a submission turns
    /// into the job's `depends_on`.
    pub predecessors: Vec<String>,
}

/// Parse a JSON-encoded DAG and return nodes in topological order.
///
/// Each returned entry carries the node name and the names of its direct
/// predecessors. Callers (e.g. the binding's submit path) use this to create
/// jobs with the correct `depends_on` chain.
pub fn topological_order(dag_bytes: &[u8]) -> Result<Vec<TopologicalNode>> {
    let dag_json = std::str::from_utf8(dag_bytes)
        .map_err(|e| QueueError::Serialization(format!("workflow DAG is not valid UTF-8: {e}")))?;

    let graph: SerializableGraph = serde_json::from_str(dag_json).map_err(|e| {
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

    let dag: DAG<()> = DAG::from_serializable(graph, |_| ())
        .map_err(|e| QueueError::Serialization(format!("failed to build DAG: {e}")))?;

    let sorted = dag
        .topological_sort()
        .map_err(|e| QueueError::Serialization(format!("topological sort failed: {e}")))?;

    Ok(sorted
        .into_iter()
        .map(|node_id| TopologicalNode {
            predecessors: predecessors.remove(&node_id.name).unwrap_or_default(),
            name: node_id.name,
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dag(json: serde_json::Value) -> Vec<u8> {
        serde_json::to_vec(&json).unwrap()
    }

    #[test]
    fn test_linear_chain() {
        let bytes = dag(serde_json::json!({
            "nodes": [{"name": "a"}, {"name": "b"}, {"name": "c"}],
            "edges": [
                {"from": "a", "to": "b", "weight": 1.0},
                {"from": "b", "to": "c", "weight": 1.0}
            ]
        }));

        let order = topological_order(&bytes).unwrap();
        let names: Vec<&str> = order.iter().map(|n| n.name.as_str()).collect();
        assert_eq!(names, vec!["a", "b", "c"]);

        assert!(order[0].predecessors.is_empty());
        assert_eq!(order[1].predecessors, vec!["a".to_string()]);
        assert_eq!(order[2].predecessors, vec!["b".to_string()]);
    }

    #[test]
    fn test_diamond_topology() {
        let bytes = dag(serde_json::json!({
            "nodes": [{"name": "a"}, {"name": "b"}, {"name": "c"}, {"name": "d"}],
            "edges": [
                {"from": "a", "to": "b", "weight": 1.0},
                {"from": "a", "to": "c", "weight": 1.0},
                {"from": "b", "to": "d", "weight": 1.0},
                {"from": "c", "to": "d", "weight": 1.0}
            ]
        }));

        let order = topological_order(&bytes).unwrap();
        let names: Vec<&str> = order.iter().map(|n| n.name.as_str()).collect();
        let pos = |n: &str| names.iter().position(|&x| x == n).unwrap();
        assert!(pos("a") < pos("b"));
        assert!(pos("a") < pos("c"));
        assert!(pos("b") < pos("d"));
        assert!(pos("c") < pos("d"));

        let d_preds: &[String] = &order[pos("d")].predecessors;
        assert_eq!(d_preds.len(), 2);
        assert!(d_preds.contains(&"b".to_string()));
        assert!(d_preds.contains(&"c".to_string()));
    }

    #[test]
    fn test_invalid_json() {
        let err = topological_order(b"not json").unwrap_err();
        assert!(err.to_string().contains("deserialize"));
    }
}
