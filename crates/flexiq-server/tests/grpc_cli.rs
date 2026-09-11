//! End-to-end: the `fq` command line against a real producer door.
//!
//! `flexiq-cli` links nothing from this workspace, so it cannot host this
//! suite: a dev-dependency the other way round would unify the `grpc` feature
//! into every `cargo check --workspace` and put tonic and a code generator into
//! the default build of a crate that gates them behind a feature precisely so
//! they are not there. Inverting it costs nothing — this crate already has the
//! feature, the harness and the CI step.
//!
//! What only a round trip can show: that the CLI's own client reaches a
//! credentialled door, that the arguments an operator types arrive as the
//! payload an SDK would have sent, and that the CLI's proto3 JSON is the same
//! object the server's `/v1` facade renders.
#![cfg(feature = "grpc")]

mod support;

use flexiq_cli::cli::{
    EnqueueArgs, JobsCancelArgs, JobsCommand, JobsGetArgs, JobsListArgs, QueuesArgs,
};
use flexiq_cli::commands;
use flexiq_core::wire::{encode_call, WireValue};
use flexiq_server::config::grpc::GrpcConfig;
use flexiq_server::config::listen::ListenAddress;
use flexiq_server::grpc::Listener;
use flexiq_server::runtime::shutdown::Shutdown;
use flexiq_server::tokens::{Scope, ScopeSet};
use prost::Message as _;

use support::{mint_token, temp_storage, temp_workflows, TempStorage};

/// The one namespace this door serves.
const NAMESPACE: &str = "grpc-cli-tests";

/// A running listener, and the CLI's own client pointed at it.
struct Harness {
    client: flexiq_cli::connect::Client,
    endpoint: String,
    storage: TempStorage,
    shutdown: Shutdown,
    served: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl Harness {
    async fn start(label: &str) -> Self {
        let storage = temp_storage(label);
        let token = mint_token(&storage, NAMESPACE, ScopeSet::ALL);
        let shutdown = Shutdown::default();
        let listener = Listener::bind(&GrpcConfig::new(
            ListenAddress::Tcp("127.0.0.1:0".parse().expect("valid address")),
            NAMESPACE,
        ))
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

        let endpoint = format!("http://{addr}");
        // The CLI's own dialler, not a hand-built channel: the interceptor and
        // the scheme handling are part of what this suite is testing.
        let client = flexiq_cli::connect::connect(&endpoint, &token)
            .await
            .expect("the listener must accept a credentialled connection");

        Self {
            client,
            endpoint,
            storage,
            shutdown,
            served,
        }
    }

    /// A second client, on a token with the scopes given.
    async fn client_with(&self, scopes: ScopeSet) -> flexiq_cli::connect::Client {
        let token = mint_token(&self.storage, NAMESPACE, scopes);
        flexiq_cli::connect::connect(&self.endpoint, &token)
            .await
            .expect("connect")
    }

    async fn stop(self) {
        self.shutdown.trigger();
        self.served
            .await
            .expect("the serve task must not panic")
            .expect("a shutdown is not an error");
    }
}

/// `fq enqueue send_email a@b.c --kw retries=2`, as clap would have parsed it.
fn enqueue_args(task: &str, args: &[&str], kwargs: &[&str]) -> EnqueueArgs {
    EnqueueArgs {
        task: task.to_string(),
        args: args.iter().map(|value| (*value).to_string()).collect(),
        kwargs: kwargs.iter().map(|value| (*value).to_string()).collect(),
        queue: None,
        priority: None,
        max_retries: None,
        delay_ms: None,
        timeout_ms: None,
        expires_in_ms: None,
        result_ttl_ms: None,
        unique_key: None,
        metadata: None,
        notes: None,
        depends_on: Vec::new(),
    }
}

/// An empty listing filter.
fn list_args() -> JobsListArgs {
    JobsListArgs {
        status: None,
        queue: None,
        task: None,
        limit: None,
        page_token: None,
    }
}

/// What an operator typed reaches the queue as the payload an SDK would have
/// sent for the same call.
#[tokio::test]
async fn enqueue_then_get_round_trips_the_arguments() {
    let mut harness = Harness::start("enqueue-round-trip").await;

    let request = commands::enqueue::request(
        &enqueue_args("send_email", &["a@b.c", "3"], &["retries=2"]),
        0,
    )
    .expect("builds");
    let enqueued = harness
        .client
        .enqueue(request)
        .await
        .expect("enqueue")
        .into_inner()
        .job
        .expect("a job comes back");

    let fetched = harness
        .client
        .get_job(flexiq_cli::pb::GetJobRequest {
            job_id: enqueued.id.clone(),
            include_payload: true,
            include_result: false,
        })
        .await
        .expect("get")
        .into_inner()
        .job
        .expect("a job comes back");

    // The canonical encoder, called directly: what the door produced from the
    // structured arm must equal what a Rust producer would have sent.
    let expected = encode_call(
        &[WireValue::Text("a@b.c".to_string()), WireValue::Integer(3)],
        &[("retries".to_string(), WireValue::Integer(2))],
    );
    assert_eq!(fetched.payload.as_deref(), Some(expected.as_slice()));
    assert_eq!(fetched.task_name, "send_email");

    harness.stop().await;
}

/// A listing filter is applied by the door, not by the CLI.
#[tokio::test]
async fn a_listing_filters_by_status() {
    let mut harness = Harness::start("list-filter").await;

    for task in ["one", "two"] {
        let request = commands::enqueue::request(&enqueue_args(task, &[], &[]), 0).expect("builds");
        harness.client.enqueue(request).await.expect("enqueue");
    }

    let mut pending = list_args();
    pending.status = Some("pending".to_string());
    let listed = harness
        .client
        .list_jobs(commands::jobs::list_request(&pending).expect("builds"))
        .await
        .expect("list")
        .into_inner();
    assert_eq!(listed.jobs.len(), 2);

    let mut dead = list_args();
    dead.status = Some("dead".to_string());
    let listed = harness
        .client
        .list_jobs(commands::jobs::list_request(&dead).expect("builds"))
        .await
        .expect("list")
        .into_inner();
    assert!(listed.jobs.is_empty());

    harness.stop().await;
}

/// Cancelling twice is the same answer, so a retried `fq jobs cancel` is safe.
#[tokio::test]
async fn cancelling_twice_is_the_same_answer() {
    let mut harness = Harness::start("cancel-idempotent").await;

    let request = commands::enqueue::request(&enqueue_args("doomed", &[], &[]), 0).expect("builds");
    let job = harness
        .client
        .enqueue(request)
        .await
        .expect("enqueue")
        .into_inner()
        .job
        .expect("a job comes back");

    let cancel = || {
        let mut client = harness.client.clone();
        let id = job.id.clone();
        async move {
            client
                .cancel_job(flexiq_cli::pb::CancelJobRequest { job_id: id })
                .await
                .expect("cancel")
                .into_inner()
                .job
                .expect("a job comes back")
                .status
        }
    };

    let first = cancel().await;
    let second = cancel().await;
    assert_eq!(first, flexiq_cli::pb::JobStatus::Cancelled as i32);
    assert_eq!(first, second);

    harness.stop().await;
}

/// The CLI renders proto3 JSON a second time, in a crate that cannot see the
/// server's renderer. This is what holds the two together: a field added to
/// `Job` that only one side learns fails here.
#[tokio::test]
async fn the_json_render_matches_the_facade() {
    let mut harness = Harness::start("json-parity").await;

    let mut args = enqueue_args("rendered", &["1"], &["k=v"]);
    args.queue = Some("mail".to_string());
    args.priority = Some(7);
    args.max_retries = Some(2);
    args.timeout_ms = Some(30_500);
    args.result_ttl_ms = Some(3_600_000);
    args.unique_key = Some("parity".to_string());
    args.metadata = Some(r#"{"a":1}"#.to_string());
    args.notes = Some("a note".to_string());
    args.expires_in_ms = Some(600_000);

    let request = commands::enqueue::request(&args, 1_757_500_000_000).expect("builds");
    let enqueued = harness
        .client
        .enqueue(request)
        .await
        .expect("enqueue")
        .into_inner()
        .job
        .expect("a job comes back");

    let cli_job = harness
        .client
        .get_job(flexiq_cli::pb::GetJobRequest {
            job_id: enqueued.id.clone(),
            include_payload: true,
            include_result: true,
        })
        .await
        .expect("get")
        .into_inner()
        .job
        .expect("a job comes back");

    // Two generated types describing one message. Re-encoding is how a value
    // crosses between them without either crate depending on the other's
    // codegen.
    let server_job =
        flexiq_server::grpc::pb::Job::decode(cli_job.encode_to_vec().as_slice()).expect("decode");

    assert_eq!(
        flexiq_cli::output::job_json(&cli_job),
        flexiq_server::grpc::facade::json::response::job(&server_job),
    );

    harness.stop().await;
}

/// A credential that is valid but wrong tells the operator which scope it
/// lacked, rather than "permission denied".
#[tokio::test]
async fn a_token_without_produce_is_told_which_scope() {
    let harness = Harness::start("scope-denied").await;
    let mut client = harness.client_with(ScopeSet::of(&[Scope::Execute])).await;

    let request =
        commands::enqueue::request(&enqueue_args("refused", &[], &[]), 0).expect("builds");
    let status = client.enqueue(request).await.expect_err("no produce scope");

    let text = flexiq_cli::error::describe(&status);
    assert!(text.contains("SCOPE_DENIED"), "{text}");
    assert!(text.contains("scope=produce"), "{text}");

    harness.stop().await;
}

/// The print paths themselves, over a real door. They write to stdout, so what
/// is asserted is that none of them errors or panics on a real response.
#[tokio::test]
async fn every_command_runs_against_a_real_door() {
    let mut harness = Harness::start("print-paths").await;

    let args = enqueue_args("printed", &["1"], &["k=v"]);
    for json in [false, true] {
        commands::enqueue::run(&mut harness.client, &args, json)
            .await
            .expect("enqueue prints");
        commands::jobs::run(&mut harness.client, &JobsCommand::List(list_args()), json)
            .await
            .expect("list prints");
        commands::queues::run(&mut harness.client, &QueuesArgs { queue: None }, json)
            .await
            .expect("queues prints");
    }

    let listed = harness
        .client
        .list_jobs(commands::jobs::list_request(&list_args()).expect("builds"))
        .await
        .expect("list")
        .into_inner();
    let id = listed.jobs.first().expect("a job").id.clone();

    commands::jobs::run(
        &mut harness.client,
        &JobsCommand::Get(JobsGetArgs {
            id: id.clone(),
            payload: true,
            result: true,
        }),
        false,
    )
    .await
    .expect("get prints");
    commands::jobs::run(
        &mut harness.client,
        &JobsCommand::Cancel(JobsCancelArgs { id }),
        true,
    )
    .await
    .expect("cancel prints");

    harness.stop().await;
}
