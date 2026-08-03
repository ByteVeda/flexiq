//! Unit tests for backend-agnostic helpers.
//!
//! Cross-backend storage scenarios live in the integration suite at
//! `tests/storage_contract.rs` so they can run against every backend
//! (SQLite, Postgres, Redis) without duplication. The tests here exercise
//! pure-Rust state-machine logic and the diamond-DAG ready-nodes case,
//! which has too much setup to be worth porting to the contract format.

use std::collections::HashMap;

use taskito_core::job::now_millis;
use taskito_core::storage::sqlite::SqliteStorage;

use crate::sqlite_store::WorkflowSqliteStorage;
use crate::storage::WorkflowStorage;
use crate::*;

fn make_node(run_id: &str, name: &str) -> WorkflowNode {
    WorkflowNode {
        id: uuid::Uuid::now_v7().to_string(),
        run_id: run_id.to_string(),
        node_name: name.to_string(),
        job_id: None,
        status: WorkflowNodeStatus::Pending,
        result_hash: None,
        fan_out_count: None,
        fan_in_data: None,
        started_at: None,
        completed_at: None,
        error: None,
        compensation_job_id: None,
        compensation_started_at: None,
        compensation_completed_at: None,
        compensation_error: None,
    }
}

fn make_run(definition_id: &str) -> WorkflowRun {
    WorkflowRun {
        id: uuid::Uuid::now_v7().to_string(),
        definition_id: definition_id.to_string(),
        params: None,
        state: WorkflowState::Pending,
        started_at: None,
        completed_at: None,
        error: None,
        parent_run_id: None,
        parent_node_name: None,
        created_at: now_millis(),
    }
}

#[test]
fn test_state_transitions() {
    assert!(WorkflowState::Pending.can_transition_to(WorkflowState::Running));
    assert!(WorkflowState::Running.can_transition_to(WorkflowState::Completed));
    assert!(WorkflowState::Running.can_transition_to(WorkflowState::CompletedWithFailures));
    assert!(WorkflowState::Running.can_transition_to(WorkflowState::Failed));
    assert!(WorkflowState::Running.can_transition_to(WorkflowState::Cancelled));
    assert!(WorkflowState::Running.can_transition_to(WorkflowState::Paused));
    assert!(WorkflowState::Paused.can_transition_to(WorkflowState::Running));
    assert!(WorkflowState::Paused.can_transition_to(WorkflowState::Cancelled));
    assert!(WorkflowState::CompletedWithFailures.can_transition_to(WorkflowState::Compensating));

    assert!(!WorkflowState::Pending.can_transition_to(WorkflowState::Completed));
    assert!(!WorkflowState::Completed.can_transition_to(WorkflowState::Running));
    assert!(!WorkflowState::Failed.can_transition_to(WorkflowState::Running));
}

#[test]
fn test_state_is_terminal() {
    assert!(WorkflowState::Completed.is_terminal());
    assert!(WorkflowState::CompletedWithFailures.is_terminal());
    assert!(WorkflowState::Failed.is_terminal());
    assert!(WorkflowState::Cancelled.is_terminal());
    assert!(!WorkflowState::Pending.is_terminal());
    assert!(!WorkflowState::Running.is_terminal());
    assert!(!WorkflowState::Paused.is_terminal());
}

#[test]
fn test_state_round_trip() {
    for s in [
        WorkflowState::Pending,
        WorkflowState::Running,
        WorkflowState::Paused,
        WorkflowState::Completed,
        WorkflowState::CompletedWithFailures,
        WorkflowState::Failed,
        WorkflowState::Cancelled,
        WorkflowState::Compensating,
        WorkflowState::Compensated,
        WorkflowState::CompensationFailed,
    ] {
        assert_eq!(WorkflowState::from_str_val(s.as_str()), Some(s));
    }
    assert_eq!(WorkflowState::from_str_val("unknown"), None);
}

/// Diamond DAG (a → b, a → c, b → d, c → d) is not covered by the linear
/// case in `tests/storage_contract.rs`; we keep it here because the setup is
/// long and the topology logic is backend-agnostic (`compute_ready_nodes` is
/// shared Rust). It uses the SQLite store for convenience.
#[test]
fn test_get_ready_nodes_diamond_dag() {
    let base = SqliteStorage::in_memory().unwrap();
    let storage = WorkflowSqliteStorage::new(base, None).unwrap();

    let dag_json = serde_json::json!({
        "nodes": [{"name": "a"}, {"name": "b"}, {"name": "c"}, {"name": "d"}],
        "edges": [
            {"from": "a", "to": "b", "weight": 1.0},
            {"from": "a", "to": "c", "weight": 1.0},
            {"from": "b", "to": "d", "weight": 1.0},
            {"from": "c", "to": "d", "weight": 1.0}
        ]
    });

    let mut step_meta = HashMap::new();
    for name in &["a", "b", "c", "d"] {
        step_meta.insert(
            name.to_string(),
            StepMetadata {
                task_name: format!("task_{name}"),
                ..Default::default()
            },
        );
    }

    let def = WorkflowDefinition {
        id: uuid::Uuid::now_v7().to_string(),
        name: "diamond".to_string(),
        version: 1,
        dag_data: serde_json::to_vec(&dag_json).unwrap(),
        step_metadata: step_meta,
        created_at: now_millis(),
    };
    let dag_str = serde_json::to_string(&dag_json).unwrap();
    storage.create_workflow_definition(&def).unwrap();

    let run = make_run(&def.id);
    let run_id = run.id.clone();
    storage.create_workflow_run(&run).unwrap();

    for name in &["a", "b", "c", "d"] {
        storage
            .create_workflow_node(&make_node(&run_id, name))
            .unwrap();
    }

    let ready = storage.get_ready_workflow_nodes(&run_id, &dag_str).unwrap();
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].node_name, "a");

    storage
        .set_workflow_node_completed(&run_id, "a", now_millis(), None)
        .unwrap();
    let ready = storage.get_ready_workflow_nodes(&run_id, &dag_str).unwrap();
    assert_eq!(ready.len(), 2);
    let names: Vec<&str> = ready.iter().map(|n| n.node_name.as_str()).collect();
    assert!(names.contains(&"b"));
    assert!(names.contains(&"c"));

    storage
        .set_workflow_node_completed(&run_id, "b", now_millis(), None)
        .unwrap();
    let ready = storage.get_ready_workflow_nodes(&run_id, &dag_str).unwrap();
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].node_name, "c");

    storage
        .set_workflow_node_completed(&run_id, "c", now_millis(), None)
        .unwrap();
    let ready = storage.get_ready_workflow_nodes(&run_id, &dag_str).unwrap();
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].node_name, "d");
}
