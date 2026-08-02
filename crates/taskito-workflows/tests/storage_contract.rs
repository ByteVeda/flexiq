//! Multi-backend contract suite for the `WorkflowStorage` trait.
//!
//! Every test body takes `&impl WorkflowStorage` so the same scenarios can
//! run against SQLite (always), PostgreSQL (when `TASKITO_POSTGRES_TEST_URL`
//! is set + `--features postgres`), and Redis (when `TASKITO_REDIS_TEST_URL`
//! is set + `--features redis`). The three entry-point tests live at the
//! bottom of this file and select the right storage handle for the active
//! feature set, mirroring the per-backend pattern in
//! `taskito-core/tests/rust/storage_tests.rs`.
//!
//! Skip-when-env-unset is the same convention used by the core contract
//! suite — local `cargo test --features workflows,postgres,redis` runs all
//! three only when the URLs are present; CI provides the URLs via service
//! containers.

use std::collections::HashMap;

use taskito_core::job::now_millis;
use taskito_workflows::{
    StepMetadata, WorkflowDefinition, WorkflowNode, WorkflowNodeStatus, WorkflowRun, WorkflowState,
    WorkflowStorage,
};

// ── Fixtures ───────────────────────────────────────────────────────────────

fn make_step(task_name: &str) -> StepMetadata {
    StepMetadata {
        task_name: task_name.to_string(),
        ..Default::default()
    }
}

fn make_definition(name: &str) -> WorkflowDefinition {
    let mut step_metadata = HashMap::new();
    step_metadata.insert("a".to_string(), make_step("task_a"));
    step_metadata.insert("b".to_string(), make_step("task_b"));
    step_metadata.insert("c".to_string(), make_step("task_c"));

    let dag_json = serde_json::json!({
        "nodes": [{"name": "a"}, {"name": "b"}, {"name": "c"}],
        "edges": [
            {"from": "a", "to": "b", "weight": 1.0},
            {"from": "b", "to": "c", "weight": 1.0}
        ]
    });

    WorkflowDefinition {
        id: uuid::Uuid::now_v7().to_string(),
        name: name.to_string(),
        version: 1,
        dag_data: serde_json::to_vec(&dag_json).unwrap(),
        step_metadata,
        created_at: now_millis(),
    }
}

fn make_run(definition_id: &str) -> WorkflowRun {
    WorkflowRun {
        id: uuid::Uuid::now_v7().to_string(),
        definition_id: definition_id.to_string(),
        params: Some(r#"{"region":"eu"}"#.to_string()),
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

// ── Per-test cases (each takes &impl WorkflowStorage) ─────────────────────

fn case_create_and_get_definition(s: &impl WorkflowStorage) {
    let def = make_definition("my_pipeline");
    s.create_workflow_definition(&def).unwrap();

    let fetched = s
        .get_workflow_definition("my_pipeline", None)
        .unwrap()
        .unwrap();
    assert_eq!(fetched.name, "my_pipeline");
    assert_eq!(fetched.version, 1);
    assert_eq!(fetched.step_metadata.len(), 3);
    assert!(fetched.step_metadata.contains_key("a"));
}

fn case_get_definition_by_version(s: &impl WorkflowStorage) {
    let mut def_v1 = make_definition("versioned");
    def_v1.version = 1;
    s.create_workflow_definition(&def_v1).unwrap();

    let mut def_v2 = make_definition("versioned");
    def_v2.id = uuid::Uuid::now_v7().to_string();
    def_v2.version = 2;
    s.create_workflow_definition(&def_v2).unwrap();

    let latest = s
        .get_workflow_definition("versioned", None)
        .unwrap()
        .unwrap();
    assert_eq!(latest.version, 2);

    let v1 = s
        .get_workflow_definition("versioned", Some(1))
        .unwrap()
        .unwrap();
    assert_eq!(v1.version, 1);
}

fn case_get_definition_by_id(s: &impl WorkflowStorage) {
    let def = make_definition("by_id_test");
    let def_id = def.id.clone();
    s.create_workflow_definition(&def).unwrap();

    let fetched = s.get_workflow_definition_by_id(&def_id).unwrap().unwrap();
    assert_eq!(fetched.id, def_id);
}

fn case_definition_not_found(s: &impl WorkflowStorage) {
    let result = s.get_workflow_definition("nonexistent", None).unwrap();
    assert!(result.is_none());
}

fn case_create_and_get_run(s: &impl WorkflowStorage) {
    let def = make_definition("run_test");
    s.create_workflow_definition(&def).unwrap();

    let run = make_run(&def.id);
    let run_id = run.id.clone();
    s.create_workflow_run(&run).unwrap();

    let fetched = s.get_workflow_run(&run_id).unwrap().unwrap();
    assert_eq!(fetched.id, run_id);
    assert_eq!(fetched.state, WorkflowState::Pending);
    assert_eq!(fetched.params, Some(r#"{"region":"eu"}"#.to_string()));
}

fn case_update_run_state(s: &impl WorkflowStorage) {
    let def = make_definition("state_test");
    s.create_workflow_definition(&def).unwrap();

    let run = make_run(&def.id);
    let run_id = run.id.clone();
    s.create_workflow_run(&run).unwrap();

    s.update_workflow_run_state(&run_id, WorkflowState::Running, None)
        .unwrap();
    let fetched = s.get_workflow_run(&run_id).unwrap().unwrap();
    assert_eq!(fetched.state, WorkflowState::Running);

    s.update_workflow_run_state(&run_id, WorkflowState::Failed, Some("node X blew up"))
        .unwrap();
    let fetched = s.get_workflow_run(&run_id).unwrap().unwrap();
    assert_eq!(fetched.state, WorkflowState::Failed);
    assert_eq!(fetched.error, Some("node X blew up".to_string()));
}

fn case_set_run_started_and_completed(s: &impl WorkflowStorage) {
    let def = make_definition("timing_test");
    s.create_workflow_definition(&def).unwrap();

    let run = make_run(&def.id);
    let run_id = run.id.clone();
    s.create_workflow_run(&run).unwrap();

    let start_time = now_millis();
    s.set_workflow_run_started(&run_id, start_time).unwrap();
    let fetched = s.get_workflow_run(&run_id).unwrap().unwrap();
    assert_eq!(fetched.state, WorkflowState::Running);
    assert_eq!(fetched.started_at, Some(start_time));

    let end_time = now_millis();
    s.set_workflow_run_completed(&run_id, end_time).unwrap();
    let fetched = s.get_workflow_run(&run_id).unwrap().unwrap();
    assert_eq!(fetched.completed_at, Some(end_time));
}

fn case_list_runs(s: &impl WorkflowStorage) {
    let def = make_definition("list_test");
    s.create_workflow_definition(&def).unwrap();

    for _ in 0..5 {
        s.create_workflow_run(&make_run(&def.id)).unwrap();
    }

    let all = s
        .list_workflow_runs(Some("list_test"), None, 100, 0)
        .unwrap();
    assert_eq!(all.len(), 5);

    let limited = s.list_workflow_runs(Some("list_test"), None, 2, 0).unwrap();
    assert_eq!(limited.len(), 2);

    let offset = s
        .list_workflow_runs(Some("list_test"), None, 100, 3)
        .unwrap();
    assert_eq!(offset.len(), 2);
}

fn case_list_runs_by_state(s: &impl WorkflowStorage) {
    let def = make_definition("state_filter");
    s.create_workflow_definition(&def).unwrap();

    let run1 = make_run(&def.id);
    let run1_id = run1.id.clone();
    s.create_workflow_run(&run1).unwrap();

    let run2 = make_run(&def.id);
    s.create_workflow_run(&run2).unwrap();

    s.update_workflow_run_state(&run1_id, WorkflowState::Running, None)
        .unwrap();

    let running = s
        .list_workflow_runs(Some("state_filter"), Some(WorkflowState::Running), 100, 0)
        .unwrap();
    assert_eq!(running.len(), 1);
    assert_eq!(running[0].id, run1_id);

    let pending = s
        .list_workflow_runs(Some("state_filter"), Some(WorkflowState::Pending), 100, 0)
        .unwrap();
    assert_eq!(pending.len(), 1);
}

fn case_list_runs_after(s: &impl WorkflowStorage) {
    let def = make_definition("keyset_runs");
    s.create_workflow_definition(&def).unwrap();

    let total = 12;
    for _ in 0..total {
        s.create_workflow_run(&make_run(&def.id)).unwrap();
    }

    // Page through in threes; every run appears exactly once, newest first.
    let page_size = 3;
    let mut seen: Vec<String> = Vec::new();
    let mut cursor: Option<(i64, String)> = None;
    loop {
        let after = cursor.as_ref().map(|(k, id)| (*k, id.as_str()));
        let page = s
            .list_workflow_runs_after(Some("keyset_runs"), None, page_size, after)
            .unwrap();
        if page.is_empty() {
            break;
        }
        for w in page.windows(2) {
            assert!(
                (w[0].created_at, &w[0].id) > (w[1].created_at, &w[1].id),
                "page must be strictly descending by (created_at, id)"
            );
        }
        let last = page.last().unwrap();
        cursor = Some((last.created_at, last.id.clone()));
        for r in &page {
            seen.push(r.id.clone());
        }
        if page.len() < page_size as usize {
            break;
        }
    }

    assert_eq!(seen.len(), total, "keyset must page every run once");
    let unique: std::collections::HashSet<&String> = seen.iter().collect();
    assert_eq!(unique.len(), total, "keyset must never duplicate a run");
}

fn case_create_and_get_node(s: &impl WorkflowStorage) {
    let def = make_definition("node_test");
    s.create_workflow_definition(&def).unwrap();
    let run = make_run(&def.id);
    let run_id = run.id.clone();
    s.create_workflow_run(&run).unwrap();

    s.create_workflow_node(&make_node(&run_id, "a")).unwrap();

    let fetched = s.get_workflow_node(&run_id, "a").unwrap().unwrap();
    assert_eq!(fetched.node_name, "a");
    assert_eq!(fetched.status, WorkflowNodeStatus::Pending);
    assert!(fetched.job_id.is_none());
}

fn case_create_nodes_batch(s: &impl WorkflowStorage) {
    let def = make_definition("batch_test");
    s.create_workflow_definition(&def).unwrap();
    let run = make_run(&def.id);
    let run_id = run.id.clone();
    s.create_workflow_run(&run).unwrap();

    let nodes = vec![
        make_node(&run_id, "a"),
        make_node(&run_id, "b"),
        make_node(&run_id, "c"),
    ];
    s.create_workflow_nodes_batch(&nodes).unwrap();

    let all = s.get_workflow_nodes(&run_id).unwrap();
    assert_eq!(all.len(), 3);
}

fn case_update_node_status(s: &impl WorkflowStorage) {
    let def = make_definition("status_test");
    s.create_workflow_definition(&def).unwrap();
    let run = make_run(&def.id);
    let run_id = run.id.clone();
    s.create_workflow_run(&run).unwrap();
    s.create_workflow_node(&make_node(&run_id, "a")).unwrap();

    s.update_workflow_node_status(&run_id, "a", WorkflowNodeStatus::Running)
        .unwrap();
    let fetched = s.get_workflow_node(&run_id, "a").unwrap().unwrap();
    assert_eq!(fetched.status, WorkflowNodeStatus::Running);
}

fn case_set_node_job_and_timing(s: &impl WorkflowStorage) {
    let def = make_definition("job_test");
    s.create_workflow_definition(&def).unwrap();
    let run = make_run(&def.id);
    let run_id = run.id.clone();
    s.create_workflow_run(&run).unwrap();
    s.create_workflow_node(&make_node(&run_id, "a")).unwrap();

    s.set_workflow_node_job(&run_id, "a", "job-123").unwrap();
    let fetched = s.get_workflow_node(&run_id, "a").unwrap().unwrap();
    assert_eq!(fetched.job_id, Some("job-123".to_string()));

    let start = now_millis();
    s.set_workflow_node_started(&run_id, "a", start).unwrap();
    let fetched = s.get_workflow_node(&run_id, "a").unwrap().unwrap();
    assert_eq!(fetched.status, WorkflowNodeStatus::Running);
    assert_eq!(fetched.started_at, Some(start));

    let end = now_millis();
    s.set_workflow_node_completed(&run_id, "a", end, Some("abc123"))
        .unwrap();
    let fetched = s.get_workflow_node(&run_id, "a").unwrap().unwrap();
    assert_eq!(fetched.status, WorkflowNodeStatus::Completed);
    assert_eq!(fetched.completed_at, Some(end));
    assert_eq!(fetched.result_hash, Some("abc123".to_string()));
}

fn case_set_node_error(s: &impl WorkflowStorage) {
    let def = make_definition("error_test");
    s.create_workflow_definition(&def).unwrap();
    let run = make_run(&def.id);
    let run_id = run.id.clone();
    s.create_workflow_run(&run).unwrap();
    s.create_workflow_node(&make_node(&run_id, "a")).unwrap();

    s.set_workflow_node_error(&run_id, "a", "kaboom").unwrap();
    let fetched = s.get_workflow_node(&run_id, "a").unwrap().unwrap();
    assert_eq!(fetched.status, WorkflowNodeStatus::Failed);
    assert_eq!(fetched.error, Some("kaboom".to_string()));
}

fn case_ready_nodes_roots(s: &impl WorkflowStorage) {
    let def = make_definition("ready_test");
    let dag_json = String::from_utf8(def.dag_data.clone()).unwrap();
    s.create_workflow_definition(&def).unwrap();

    let run = make_run(&def.id);
    let run_id = run.id.clone();
    s.create_workflow_run(&run).unwrap();

    let nodes = vec![
        make_node(&run_id, "a"),
        make_node(&run_id, "b"),
        make_node(&run_id, "c"),
    ];
    s.create_workflow_nodes_batch(&nodes).unwrap();

    let ready = s.get_ready_workflow_nodes(&run_id, &dag_json).unwrap();
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].node_name, "a");
}

fn case_ready_nodes_after_completion(s: &impl WorkflowStorage) {
    let def = make_definition("completion_ready");
    let dag_json = String::from_utf8(def.dag_data.clone()).unwrap();
    s.create_workflow_definition(&def).unwrap();

    let run = make_run(&def.id);
    let run_id = run.id.clone();
    s.create_workflow_run(&run).unwrap();

    s.create_workflow_nodes_batch(&[
        make_node(&run_id, "a"),
        make_node(&run_id, "b"),
        make_node(&run_id, "c"),
    ])
    .unwrap();

    s.set_workflow_node_completed(&run_id, "a", now_millis(), None)
        .unwrap();

    let ready = s.get_ready_workflow_nodes(&run_id, &dag_json).unwrap();
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].node_name, "b");
}

fn case_ready_nodes_all_completed(s: &impl WorkflowStorage) {
    let def = make_definition("all_done");
    let dag_json = String::from_utf8(def.dag_data.clone()).unwrap();
    s.create_workflow_definition(&def).unwrap();

    let run = make_run(&def.id);
    let run_id = run.id.clone();
    s.create_workflow_run(&run).unwrap();

    s.create_workflow_nodes_batch(&[
        make_node(&run_id, "a"),
        make_node(&run_id, "b"),
        make_node(&run_id, "c"),
    ])
    .unwrap();

    for name in &["a", "b", "c"] {
        s.set_workflow_node_completed(&run_id, name, now_millis(), None)
            .unwrap();
    }

    let ready = s.get_ready_workflow_nodes(&run_id, &dag_json).unwrap();
    assert!(ready.is_empty());
}

fn case_set_fan_out_count(s: &impl WorkflowStorage) {
    let def = make_definition("fanout_pipe");
    s.create_workflow_definition(&def).unwrap();
    let run = make_run(&def.id);
    let run_id = run.id.clone();
    s.create_workflow_run(&run).unwrap();

    s.create_workflow_node(&make_node(&run_id, "process"))
        .unwrap();

    let fetched = s.get_workflow_node(&run_id, "process").unwrap().unwrap();
    assert_eq!(fetched.status, WorkflowNodeStatus::Pending);
    assert_eq!(fetched.fan_out_count, None);

    s.set_workflow_node_fan_out_count(&run_id, "process", 5)
        .unwrap();

    let fetched = s.get_workflow_node(&run_id, "process").unwrap().unwrap();
    assert_eq!(fetched.status, WorkflowNodeStatus::Running);
    assert_eq!(fetched.fan_out_count, Some(5));
}

fn case_get_nodes_by_prefix(s: &impl WorkflowStorage) {
    let def = make_definition("prefix_pipe");
    s.create_workflow_definition(&def).unwrap();
    let run = make_run(&def.id);
    let run_id = run.id.clone();
    s.create_workflow_run(&run).unwrap();

    for name in &[
        "process",
        "process[0]",
        "process[1]",
        "process[2]",
        "aggregate",
    ] {
        s.create_workflow_node(&make_node(&run_id, name)).unwrap();
    }

    let children = s.get_workflow_nodes_by_prefix(&run_id, "process[").unwrap();
    assert_eq!(children.len(), 3);
    let names: Vec<&str> = children.iter().map(|n| n.node_name.as_str()).collect();
    assert!(names.contains(&"process[0]"));
    assert!(names.contains(&"process[1]"));
    assert!(names.contains(&"process[2]"));

    let agg = s
        .get_workflow_nodes_by_prefix(&run_id, "aggregate")
        .unwrap();
    assert_eq!(agg.len(), 1);

    let empty = s
        .get_workflow_nodes_by_prefix(&run_id, "nonexistent[")
        .unwrap();
    assert!(empty.is_empty());
}

fn case_set_workflow_node_running(s: &impl WorkflowStorage) {
    let def = make_definition("sub_wf_parent");
    s.create_workflow_definition(&def).unwrap();
    let run = make_run(&def.id);
    let run_id = run.id.clone();
    s.create_workflow_run(&run).unwrap();
    s.create_workflow_node(&make_node(&run_id, "parent"))
        .unwrap();

    let before = now_millis();
    s.set_workflow_node_running(&run_id, "parent", before)
        .unwrap();

    let fetched = s.get_workflow_node(&run_id, "parent").unwrap().unwrap();
    assert_eq!(fetched.status, WorkflowNodeStatus::Running);
    assert_eq!(fetched.started_at, Some(before));
    assert_eq!(
        fetched.fan_out_count, None,
        "set_workflow_node_running must not set fan_out_count"
    );
}

fn case_finalize_fan_out_parent_is_idempotent(s: &impl WorkflowStorage) {
    let def = make_definition("fan_in_race");
    s.create_workflow_definition(&def).unwrap();
    let run = make_run(&def.id);
    let run_id = run.id.clone();
    s.create_workflow_run(&run).unwrap();
    s.create_workflow_node(&make_node(&run_id, "process"))
        .unwrap();

    let now = now_millis();
    let first = s
        .finalize_fan_out_parent(&run_id, "process", true, None, now)
        .unwrap();
    assert!(first, "first caller must perform the transition");

    let after_first = s.get_workflow_node(&run_id, "process").unwrap().unwrap();
    assert_eq!(after_first.status, WorkflowNodeStatus::Completed);

    let second = s
        .finalize_fan_out_parent(&run_id, "process", true, None, now + 1000)
        .unwrap();
    assert!(!second, "second caller must be rejected by the CAS");

    let after_second = s.get_workflow_node(&run_id, "process").unwrap().unwrap();
    assert_eq!(after_second.status, WorkflowNodeStatus::Completed);
    assert_eq!(
        after_second.completed_at, after_first.completed_at,
        "second caller must not overwrite completed_at"
    );
}

fn case_finalize_fan_out_parent_failure_branch(s: &impl WorkflowStorage) {
    let def = make_definition("fan_in_fail");
    s.create_workflow_definition(&def).unwrap();
    let run = make_run(&def.id);
    let run_id = run.id.clone();
    s.create_workflow_run(&run).unwrap();
    s.create_workflow_node(&make_node(&run_id, "process"))
        .unwrap();

    let now = now_millis();
    let transitioned = s
        .finalize_fan_out_parent(&run_id, "process", false, Some("boom"), now)
        .unwrap();
    assert!(transitioned);

    let node = s.get_workflow_node(&run_id, "process").unwrap().unwrap();
    assert_eq!(node.status, WorkflowNodeStatus::Failed);
    assert_eq!(node.error.as_deref(), Some("boom"));
}

fn case_finalize_fan_out_parent_no_op_on_terminal_node(s: &impl WorkflowStorage) {
    let def = make_definition("skipped_parent");
    s.create_workflow_definition(&def).unwrap();
    let run = make_run(&def.id);
    let run_id = run.id.clone();
    s.create_workflow_run(&run).unwrap();
    s.create_workflow_node(&make_node(&run_id, "process"))
        .unwrap();
    s.update_workflow_node_status(&run_id, "process", WorkflowNodeStatus::Skipped)
        .unwrap();

    let transitioned = s
        .finalize_fan_out_parent(&run_id, "process", true, None, now_millis())
        .unwrap();
    assert!(!transitioned, "already-skipped nodes must be left alone");

    let node = s.get_workflow_node(&run_id, "process").unwrap().unwrap();
    assert_eq!(node.status, WorkflowNodeStatus::Skipped);
}

fn case_set_workflow_node_compensation_job(s: &impl WorkflowStorage) {
    let def = make_definition("saga_compensation_job");
    s.create_workflow_definition(&def).unwrap();
    let run = make_run(&def.id);
    let run_id = run.id.clone();
    s.create_workflow_run(&run).unwrap();
    s.create_workflow_node(&make_node(&run_id, "charge"))
        .unwrap();
    s.set_workflow_node_completed(&run_id, "charge", now_millis(), Some("h"))
        .unwrap();

    let started = now_millis();
    s.set_workflow_node_compensation_job(&run_id, "charge", "comp-job-123", started)
        .unwrap();

    let node = s.get_workflow_node(&run_id, "charge").unwrap().unwrap();
    assert_eq!(node.status, WorkflowNodeStatus::Compensating);
    assert_eq!(node.compensation_job_id.as_deref(), Some("comp-job-123"));
    assert_eq!(node.compensation_started_at, Some(started));
}

fn case_set_workflow_node_compensated(s: &impl WorkflowStorage) {
    let def = make_definition("saga_compensated");
    s.create_workflow_definition(&def).unwrap();
    let run = make_run(&def.id);
    let run_id = run.id.clone();
    s.create_workflow_run(&run).unwrap();
    s.create_workflow_node(&make_node(&run_id, "charge"))
        .unwrap();
    s.set_workflow_node_compensation_job(&run_id, "charge", "comp-1", now_millis())
        .unwrap();

    let completed = now_millis();
    s.set_workflow_node_compensated(&run_id, "charge", completed)
        .unwrap();

    let node = s.get_workflow_node(&run_id, "charge").unwrap().unwrap();
    assert_eq!(node.status, WorkflowNodeStatus::Compensated);
    assert_eq!(node.compensation_completed_at, Some(completed));
}

fn case_set_workflow_node_compensation_failed(s: &impl WorkflowStorage) {
    let def = make_definition("saga_compensation_failed");
    s.create_workflow_definition(&def).unwrap();
    let run = make_run(&def.id);
    let run_id = run.id.clone();
    s.create_workflow_run(&run).unwrap();
    s.create_workflow_node(&make_node(&run_id, "charge"))
        .unwrap();
    s.set_workflow_node_compensation_job(&run_id, "charge", "comp-2", now_millis())
        .unwrap();

    let completed = now_millis();
    s.set_workflow_node_compensation_failed(&run_id, "charge", "refund api down", completed)
        .unwrap();

    let node = s.get_workflow_node(&run_id, "charge").unwrap().unwrap();
    assert_eq!(node.status, WorkflowNodeStatus::CompensationFailed);
    assert_eq!(node.compensation_completed_at, Some(completed));
    assert_eq!(node.compensation_error.as_deref(), Some("refund api down"));
}

fn case_get_child_workflow_runs(s: &impl WorkflowStorage) {
    let def = make_definition("parent_def");
    s.create_workflow_definition(&def).unwrap();

    let parent = make_run(&def.id);
    let parent_id = parent.id.clone();
    s.create_workflow_run(&parent).unwrap();

    let mut child = make_run(&def.id);
    child.parent_run_id = Some(parent_id.clone());
    child.parent_node_name = Some("subflow_node".to_string());
    let child_id = child.id.clone();
    s.create_workflow_run(&child).unwrap();

    let children = s.get_child_workflow_runs(&parent_id).unwrap();
    assert_eq!(children.len(), 1);
    assert_eq!(children[0].id, child_id);
}

// ── Contract runner ────────────────────────────────────────────────────────

fn run_contract(s: &impl WorkflowStorage) {
    case_create_and_get_definition(s);
    case_get_definition_by_version(s);
    case_get_definition_by_id(s);
    case_definition_not_found(s);
    case_create_and_get_run(s);
    case_update_run_state(s);
    case_set_run_started_and_completed(s);
    case_list_runs(s);
    case_list_runs_by_state(s);
    case_list_runs_after(s);
    case_create_and_get_node(s);
    case_create_nodes_batch(s);
    case_update_node_status(s);
    case_set_node_job_and_timing(s);
    case_set_node_error(s);
    case_ready_nodes_roots(s);
    case_ready_nodes_after_completion(s);
    case_ready_nodes_all_completed(s);
    case_set_fan_out_count(s);
    case_get_nodes_by_prefix(s);
    case_set_workflow_node_running(s);
    case_finalize_fan_out_parent_is_idempotent(s);
    case_finalize_fan_out_parent_failure_branch(s);
    case_finalize_fan_out_parent_no_op_on_terminal_node(s);
    case_set_workflow_node_compensation_job(s);
    case_set_workflow_node_compensated(s);
    case_set_workflow_node_compensation_failed(s);
    case_get_child_workflow_runs(s);
}

// ── Per-backend entry points ──────────────────────────────────────────────

#[test]
fn sqlite_storage_contract() {
    use taskito_core::storage::sqlite::SqliteStorage;
    use taskito_workflows::WorkflowSqliteStorage;

    let base = SqliteStorage::in_memory().unwrap();
    let s = WorkflowSqliteStorage::new(base, None).unwrap();
    run_contract(&s);
}

#[cfg(feature = "postgres")]
#[test]
fn postgres_storage_contract() {
    use taskito_core::storage::postgres::PostgresStorage;
    use taskito_workflows::WorkflowPostgresStorage;

    let url = match std::env::var("TASKITO_POSTGRES_TEST_URL") {
        Ok(u) => u,
        Err(_) => {
            eprintln!("skipping postgres_storage_contract: TASKITO_POSTGRES_TEST_URL not set");
            return;
        }
    };
    // Use a unique schema per run so concurrent test invocations and stale
    // workflow rows from a prior run can't bleed across.
    let schema = format!("wf_contract_{}", uuid::Uuid::now_v7().simple());
    let base = PostgresStorage::with_schema(&url, &schema).unwrap();
    let s = WorkflowPostgresStorage::new(base, None).unwrap();
    run_contract(&s);
}

#[cfg(feature = "redis")]
#[test]
fn redis_storage_contract() {
    use taskito_core::storage::redis_backend::RedisStorage;
    use taskito_workflows::WorkflowRedisStorage;

    let url = match std::env::var("TASKITO_REDIS_TEST_URL") {
        Ok(u) => u,
        Err(_) => {
            eprintln!("skipping redis_storage_contract: TASKITO_REDIS_TEST_URL not set");
            return;
        }
    };
    // Use a unique prefix per run to isolate keys.
    let prefix = format!("wf_contract_{}:", uuid::Uuid::now_v7().simple());
    let base = RedisStorage::with_prefix(&url, &prefix).unwrap();
    let s = WorkflowRedisStorage::new(base, None).unwrap();
    run_contract(&s);
}
