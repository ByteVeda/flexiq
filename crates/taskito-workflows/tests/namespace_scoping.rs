//! Namespace scoping for workflow runs and their nodes.
//!
//! `workflow_runs` gained a `namespace` column in `0003_workflow_run_namespace`;
//! before it, a run id reached its run — and through it every node — from any
//! scope. The scope lives on the store handle rather than in the trait
//! signatures: a store is built from one queue, which belongs to one tenant.
//!
//! `workflow_nodes` has no namespace of its own, so a node inherits its run's.

use std::collections::HashMap;

use taskito_core::job::now_millis;
use taskito_core::storage::sqlite::SqliteStorage;
use taskito_workflows::{
    StepMetadata, WorkflowDefinition, WorkflowNode, WorkflowNodeStatus, WorkflowRun,
    WorkflowSqliteStorage, WorkflowState, WorkflowStorage,
};

const TENANT_A: &str = "tenant-a";
const TENANT_B: &str = "tenant-b";

/// Two stores over one database, scoped to different tenants, plus an
/// unscoped one — the shape a multi-tenant deployment actually has.
fn stores() -> (
    WorkflowSqliteStorage,
    WorkflowSqliteStorage,
    WorkflowSqliteStorage,
) {
    // One `SqliteStorage`, cloned: clones share the pool, so all three handles
    // read the same in-memory database — which is the point, they differ only
    // in namespace.
    let base = SqliteStorage::in_memory().expect("in-memory SQLite");
    (
        WorkflowSqliteStorage::new(base.clone(), Some(TENANT_A.to_string())).expect("store a"),
        WorkflowSqliteStorage::new(base.clone(), Some(TENANT_B.to_string())).expect("store b"),
        WorkflowSqliteStorage::new(base, None).expect("unscoped store"),
    )
}

fn make_definition(name: &str) -> WorkflowDefinition {
    let mut step_metadata = HashMap::new();
    step_metadata.insert(
        "a".to_string(),
        StepMetadata {
            task_name: "task_a".to_string(),
            ..Default::default()
        },
    );
    let dag_json = serde_json::json!({"nodes": [{"name": "a"}], "edges": []});
    WorkflowDefinition {
        id: uuid::Uuid::now_v7().to_string(),
        name: name.to_string(),
        version: 1,
        dag_data: serde_json::to_vec(&dag_json).expect("dag json"),
        step_metadata,
        created_at: now_millis(),
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

/// Create a definition and one run on `store`, returning the run id.
fn seed_run(store: &WorkflowSqliteStorage, definition_name: &str) -> String {
    let definition = make_definition(definition_name);
    store
        .create_workflow_definition(&definition)
        .expect("definition");
    let run = make_run(&definition.id);
    store.create_workflow_run(&run).expect("run");
    run.id
}

#[test]
fn a_run_from_another_namespace_reads_as_missing() {
    let (a, b, unscoped) = stores();
    let run_id = seed_run(&a, "wf_read");

    assert!(a.get_workflow_run(&run_id).expect("read a").is_some());
    assert!(b.get_workflow_run(&run_id).expect("read b").is_none());
    assert!(unscoped
        .get_workflow_run(&run_id)
        .expect("unscoped read")
        .is_some());
}

#[test]
fn run_listings_are_scoped() {
    let (a, b, unscoped) = stores();
    seed_run(&a, "wf_list_a");
    seed_run(&b, "wf_list_b");

    assert_eq!(a.list_workflow_runs(None, None, 10, 0).unwrap().len(), 1);
    assert_eq!(b.list_workflow_runs(None, None, 10, 0).unwrap().len(), 1);
    assert_eq!(
        unscoped
            .list_workflow_runs(None, None, 10, 0)
            .unwrap()
            .len(),
        2,
        "an unscoped console keeps seeing every namespace"
    );

    assert_eq!(
        a.list_workflow_runs_after(None, None, 10, None)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        unscoped
            .list_workflow_runs_after(None, None, 10, None)
            .unwrap()
            .len(),
        2
    );
}

#[test]
fn a_run_from_another_namespace_cannot_be_mutated() {
    let (a, b, unscoped) = stores();
    let run_id = seed_run(&a, "wf_mutate");

    b.update_workflow_run_state(&run_id, WorkflowState::Cancelled, Some("nope"))
        .expect("no error, no effect");
    b.set_workflow_run_started(&run_id, 111).expect("no effect");
    b.set_workflow_run_completed(&run_id, 222)
        .expect("no effect");

    let run = unscoped.get_workflow_run(&run_id).unwrap().unwrap();
    assert_eq!(run.state, WorkflowState::Pending);
    assert_eq!(run.started_at, None);
    assert_eq!(run.completed_at, None);

    // The owning store still mutates it.
    a.update_workflow_run_state(&run_id, WorkflowState::Cancelled, None)
        .unwrap();
    assert_eq!(
        unscoped.get_workflow_run(&run_id).unwrap().unwrap().state,
        WorkflowState::Cancelled
    );
}

#[test]
fn nodes_inherit_their_runs_namespace() {
    let (a, b, unscoped) = stores();
    let run_id = seed_run(&a, "wf_nodes");
    a.create_workflow_node(&make_node(&run_id, "a"))
        .expect("node");

    assert_eq!(a.get_workflow_nodes(&run_id).unwrap().len(), 1);
    assert!(a.get_workflow_node(&run_id, "a").unwrap().is_some());

    assert!(b.get_workflow_nodes(&run_id).unwrap().is_empty());
    assert!(b.get_workflow_node(&run_id, "a").unwrap().is_none());
    assert!(b
        .get_workflow_nodes_by_prefix(&run_id, "a")
        .unwrap()
        .is_empty());

    assert_eq!(unscoped.get_workflow_nodes(&run_id).unwrap().len(), 1);
}

#[test]
fn a_node_in_another_namespace_cannot_be_mutated() {
    let (a, b, unscoped) = stores();
    let run_id = seed_run(&a, "wf_node_mutate");
    a.create_workflow_node(&make_node(&run_id, "a"))
        .expect("node");

    b.update_workflow_node_status(&run_id, "a", WorkflowNodeStatus::Failed)
        .expect("no effect");
    b.set_workflow_node_job(&run_id, "a", "job-from-b")
        .expect("no effect");
    b.set_workflow_node_completed(&run_id, "a", 333, Some("hash"))
        .expect("no effect");
    b.set_workflow_node_error(&run_id, "a", "boom")
        .expect("no effect");
    assert!(
        !b.finalize_fan_out_parent(&run_id, "a", true, None, 444)
            .expect("no effect"),
        "a foreign fan-out parent must report untransitioned"
    );

    let node = unscoped
        .get_workflow_node(&run_id, "a")
        .unwrap()
        .expect("node still there");
    assert_eq!(node.status, WorkflowNodeStatus::Pending);
    assert_eq!(node.job_id, None);
    assert_eq!(node.error, None);
    assert_eq!(node.completed_at, None);

    // A node created against a foreign run must not appear either.
    b.create_workflow_node(&make_node(&run_id, "smuggled"))
        .expect("no effect");
    b.create_workflow_nodes_batch(&[make_node(&run_id, "smuggled-batch")])
        .expect("no effect");
    assert_eq!(unscoped.get_workflow_nodes(&run_id).unwrap().len(), 1);
}

#[test]
fn child_runs_are_scoped() {
    let (a, b, _) = stores();
    let definition = make_definition("wf_children");
    a.create_workflow_definition(&definition).unwrap();
    let parent = make_run(&definition.id);
    a.create_workflow_run(&parent).unwrap();

    let mut child = make_run(&definition.id);
    child.parent_run_id = Some(parent.id.clone());
    child.parent_node_name = Some("a".to_string());
    a.create_workflow_run(&child).unwrap();

    assert_eq!(a.get_child_workflow_runs(&parent.id).unwrap().len(), 1);
    assert!(b.get_child_workflow_runs(&parent.id).unwrap().is_empty());
}

#[test]
fn a_run_written_before_namespaces_is_invisible_to_a_scoped_store() {
    // The migration adds the column as NULL, so pre-existing runs read as
    // unscoped — visible to an unscoped console, to no tenant in particular.
    let (a, _, unscoped) = stores();
    let run_id = seed_run(&unscoped, "wf_legacy");

    assert!(unscoped.get_workflow_run(&run_id).unwrap().is_some());
    assert!(a.get_workflow_run(&run_id).unwrap().is_none());
}
