//! End-to-end: `flexiq.v1.ProducerService.SubmitWorkflow` and `.GetWorkflowRun`
//! over a real socket.
//!
//! `SubmitWorkflow` pre-enqueues real `Job` rows with `depends_on` chains —
//! the same shape `PyQueue::submit_workflow`'s static path produces
//! (`flexiq_workflows::lifecycle::submit_workflow` is the one implementation
//! both call) — so what proves the acceptance criterion here is that an
//! *unmodified worker* advances the run: this suite completes the jobs
//! through the same `WorkflowStorage` calls a worker's tracker makes on its
//! own job completion (`set_workflow_node_completed`,
//! `update_workflow_run_state`), never through a second, test-only code path.
#![cfg(feature = "grpc")]

mod support;

use flexiq_core::job::JobStatus;
use flexiq_core::storage::Storage;
use flexiq_server::config::grpc::GrpcConfig;
use flexiq_server::config::listen::ListenAddress;
use flexiq_server::grpc::pb::producer_service_client::ProducerServiceClient;
use flexiq_server::grpc::pb::{
    workflow_node_config, EdgeCondition, GateConfig, GetWorkflowRunRequest, SubmitWorkflowRequest,
    WorkflowGraph, WorkflowGraphEdge, WorkflowGraphNode, WorkflowNodeConfig, WorkflowNodeStatus,
    WorkflowState,
};
use flexiq_server::grpc::status::reason;
use flexiq_server::grpc::Listener;
use flexiq_server::runtime::shutdown::Shutdown;
use flexiq_workflows::WorkflowStorage;
use tonic::transport::Channel;
use tonic::Code;
use tonic_types::StatusExt;

use support::{mint_token, temp_storage, temp_workflows, Bearer, TempStorage};

const NAMESPACE: &str = "grpc-workflow-tests";

struct Harness {
    client: ProducerServiceClient<tonic::service::interceptor::InterceptedService<Channel, Bearer>>,
    storage: TempStorage,
    workflows: flexiq_workflows::WorkflowStorageBackend,
    shutdown: Shutdown,
    served: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl Harness {
    async fn start(label: &str) -> Self {
        let storage = temp_storage(label);
        let workflows = temp_workflows(&storage);
        let token = mint_token(&storage, NAMESPACE, flexiq_server::tokens::ScopeSet::ALL);
        let shutdown = Shutdown::default();
        let listener = Listener::bind(&GrpcConfig {
            listen: ListenAddress::Tcp("127.0.0.1:0".parse().expect("valid address")),
            namespace: NAMESPACE.to_string(),
            executor_stream_max_age: std::time::Duration::ZERO,
        })
        .await
        .expect("bind");
        let addr = listener
            .local_addr()
            .expect("a TCP listener knows what it bound");
        let served = tokio::spawn(listener.serve(
            (*storage).clone(),
            temp_workflows(&storage),
            None,
            shutdown.clone(),
        ));

        let channel = Channel::from_shared(format!("http://{addr}"))
            .expect("a valid endpoint")
            .connect()
            .await
            .expect("the listener must accept a connection");

        Self {
            client: ProducerServiceClient::with_interceptor(channel, Bearer::new(&token)),
            storage,
            workflows,
            shutdown,
            served,
        }
    }

    async fn stop(self) {
        self.shutdown.trigger();
        self.served
            .await
            .expect("the serve task must not panic")
            .expect("a shutdown is not an error");
    }
}

/// One node, no body arguments, no dynamic construct.
fn plain_node(name: &str, task: &str) -> WorkflowNodeConfig {
    WorkflowNodeConfig {
        name: name.to_string(),
        task_name: task.to_string(),
        queue: None,
        body: Some(workflow_node_config::Body::Raw(Vec::new())),
        max_retries: None,
        timeout_ms: None,
        priority: None,
        condition: EdgeCondition::Unspecified as i32,
        gate: None,
        cache: None,
        fan_out: None,
        fan_in: None,
        sub_workflow: None,
        compensate: None,
    }
}

/// A linear two-node graph, `a -> b`.
fn linear_graph() -> WorkflowGraph {
    WorkflowGraph {
        nodes: vec![
            WorkflowGraphNode {
                name: "a".to_string(),
            },
            WorkflowGraphNode {
                name: "b".to_string(),
            },
        ],
        edges: vec![WorkflowGraphEdge {
            from: "a".to_string(),
            to: "b".to_string(),
        }],
        node_configs: vec![plain_node("a", "task_a"), plain_node("b", "task_b")],
    }
}

/// Mimic a worker's tracker completing one node: the same `WorkflowStorage`
/// calls `mark_workflow_node_result` makes, minus the PyO3 layer around them.
fn complete_node(
    workflows: &flexiq_workflows::WorkflowStorageBackend,
    run_id: &str,
    node_name: &str,
) {
    workflows
        .set_workflow_node_completed(run_id, node_name, flexiq_core::job::now_millis(), None)
        .expect("mark node completed");
}

#[tokio::test]
async fn a_static_graph_pre_enqueues_jobs_an_unmodified_worker_advances() {
    let harness = Harness::start("static-graph").await;

    let response = harness
        .client
        .clone()
        .submit_workflow(SubmitWorkflowRequest {
            name: "linear".to_string(),
            graph: Some(linear_graph()),
            params_json: None,
        })
        .await
        .expect("submit_workflow")
        .into_inner();
    let run_id = response.run_id;
    assert!(!run_id.is_empty());

    // The run starts Running (the static path pre-enqueues every node, so
    // there is nothing left in Pending once submission returns).
    let read = harness
        .client
        .clone()
        .get_workflow_run(GetWorkflowRunRequest {
            run_id: run_id.clone(),
        })
        .await
        .expect("get_workflow_run")
        .into_inner();
    let run = read.run.expect("a response carries its run");
    assert_eq!(run.state, WorkflowState::Running as i32);
    assert_eq!(read.nodes.len(), 2);
    for node in &read.nodes {
        assert_eq!(node.status, WorkflowNodeStatus::Pending as i32);
        assert!(node.job_id.is_some(), "a static node is pre-enqueued");
    }

    // The jobs are real rows in the same database, chained by depends_on —
    // exactly what an unmodified worker claims and runs, no gRPC involved.
    let node_b = read.nodes.iter().find(|n| n.name == "b").expect("node b");
    let job_b = harness
        .storage
        .get_job(node_b.job_id.as_deref().unwrap(), Some(NAMESPACE))
        .expect("read")
        .expect("the job exists");
    assert_eq!(job_b.status, JobStatus::Pending);
    let node_a = read.nodes.iter().find(|n| n.name == "a").expect("node a");
    let deps = harness
        .storage
        .get_dependencies(node_b.job_id.as_deref().unwrap(), Some(NAMESPACE))
        .expect("read deps");
    assert_eq!(deps, vec![node_a.job_id.clone().unwrap()]);

    // A worker's tracker completes both nodes and the run — the same two
    // calls `mark_workflow_node_result` makes.
    complete_node(&harness.workflows, &run_id, "a");
    complete_node(&harness.workflows, &run_id, "b");
    harness
        .workflows
        .update_workflow_run_state(&run_id, flexiq_workflows::WorkflowState::Completed, None)
        .expect("finalize run");
    harness
        .workflows
        .set_workflow_run_completed(&run_id, flexiq_core::job::now_millis())
        .expect("stamp completion");

    let read = harness
        .client
        .clone()
        .get_workflow_run(GetWorkflowRunRequest { run_id })
        .await
        .expect("get_workflow_run")
        .into_inner();
    let run = read.run.expect("a response carries its run");
    assert_eq!(run.state, WorkflowState::Completed as i32);
    assert!(run.completed_at.is_some());
    assert!(read
        .nodes
        .iter()
        .all(|n| n.status == WorkflowNodeStatus::Completed as i32));

    harness.stop().await;
}

#[tokio::test]
async fn a_dynamic_construct_is_refused_before_anything_is_written() {
    let harness = Harness::start("dynamic-refusal").await;

    let mut graph = linear_graph();
    graph.node_configs[1].gate = Some(GateConfig {
        timeout_ms: None,
        on_timeout: 0,
        message: None,
    });

    let error = harness
        .client
        .clone()
        .submit_workflow(SubmitWorkflowRequest {
            name: "gated".to_string(),
            graph: Some(graph),
            params_json: None,
        })
        .await
        .expect_err("a gate node must be refused");

    assert_eq!(error.code(), Code::FailedPrecondition);
    let info = error.get_details_error_info().expect("an ErrorInfo");
    assert_eq!(info.reason, reason::WORKFLOW_CONSTRUCT_UNSUPPORTED);
    assert_eq!(info.metadata["node"], "b");
    assert_eq!(info.metadata["field"], "gate");

    // Nothing was written: no definition, no run, no job — the refusal is
    // whole, not partial.
    let jobs = harness
        .storage
        .list_jobs(None, None, None, 10, 0, Some(NAMESPACE))
        .expect("list");
    assert!(jobs.is_empty(), "a refused submission enqueues nothing");

    harness.stop().await;
}

#[tokio::test]
async fn an_unknown_run_id_is_not_found() {
    let harness = Harness::start("workflow-not-found").await;

    let error = harness
        .client
        .clone()
        .get_workflow_run(GetWorkflowRunRequest {
            run_id: "no-such-run".to_string(),
        })
        .await
        .expect_err("an unknown run must be refused");
    assert_eq!(error.code(), Code::NotFound);

    harness.stop().await;
}
