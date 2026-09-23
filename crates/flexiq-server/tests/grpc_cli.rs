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
//! object the server's `/v1` facade renders — for the producer door's `Job`
//! and for every `flexiq.admin.v1` response the admin verbs print.
#![cfg(feature = "grpc")]

mod support;

use flexiq_cli::cli::{
    DeadLetterIdArgs, DlqCommand, DlqListArgs, DlqPurgeArgs, DlqShowArgs, EnqueueArgs,
    JobsCancelArgs, JobsCommand, JobsGetArgs, JobsListArgs, OverridesCommand, PeriodicCommand,
    PeriodicNameArgs, PeriodicPutArgs, PeriodicShowArgs, QueueNameArgs, QueuesArgs,
    SetQueueOverrideArgs, SetTaskOverrideArgs, TaskNameArgs, ThroughputArgs,
};
use flexiq_cli::commands;
use flexiq_cli::output::admin as cli_render;
use flexiq_cli::pb::admin as cli_pb;
use flexiq_core::job::{now_millis, NewJob};
use flexiq_core::storage::records::WorkerRegistration;
use flexiq_core::wire::{encode_call, WireValue};
use flexiq_core::Storage;
use flexiq_server::config::grpc::GrpcConfig;
use flexiq_server::config::listen::ListenAddress;
use flexiq_server::grpc::facade::json::admin_response as server_render;
use flexiq_server::grpc::pb::admin as server_pb;
use flexiq_server::grpc::Listener;
use flexiq_server::runtime::shutdown::Shutdown;
use flexiq_server::tokens::{Scope, ScopeSet};
use prost::Message;
use serde_json::Value;

use support::{mint_token, temp_storage, temp_workflows, TempStorage};

/// The one namespace this door serves.
const NAMESPACE: &str = "grpc-cli-tests";

/// A running listener, and the CLI's own clients pointed at it.
struct Harness {
    client: flexiq_cli::connect::Client,
    /// The admin door, on the same token: [`ScopeSet::ALL`] carries `inspect`
    /// and `admin` beside `produce`.
    admin: flexiq_cli::connect::AdminClient,
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
        let admin = flexiq_cli::connect::connect_admin(&endpoint, &token)
            .await
            .expect("the listener must accept a credentialled admin connection");

        Self {
            client,
            admin,
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

    /// A second admin client, on a token with the scopes given.
    async fn admin_with(&self, scopes: ScopeSet) -> flexiq_cli::connect::AdminClient {
        let token = mint_token(&self.storage, NAMESPACE, scopes);
        flexiq_cli::connect::connect_admin(&self.endpoint, &token)
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
        commands::queues::run(
            &mut harness.client,
            &QueuesArgs {
                queue: None,
                list: false,
            },
            json,
        )
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

// ── The admin door ───────────────────────────────────────────────────

/// A job in this door's namespace, written straight to storage.
fn stored_job(queue: &str, task: &str) -> NewJob {
    NewJob {
        queue: queue.to_string(),
        task_name: task.to_string(),
        payload: vec![0x02, 0x82, 0x80, 0xa0],
        priority: 0,
        scheduled_at: now_millis(),
        max_retries: 0,
        timeout_ms: 30_000,
        unique_key: None,
        metadata: None,
        notes: None,
        depends_on: vec![],
        expires_at: None,
        result_ttl_ms: None,
        namespace: Some(NAMESPACE.to_string()),
        debounce_key: None,
    }
}

/// Dead-letter one job of `task` in queue `dlq`, returning the entry's id.
fn dead_letter(storage: &TempStorage, task: &str) -> String {
    let job = storage.enqueue(stored_job("dlq", task)).expect("enqueue");
    storage
        .dequeue("dlq", now_millis() + 1_000, Some(NAMESPACE))
        .expect("dequeue");
    let running = storage
        .get_job(&job.id, None)
        .expect("read")
        .expect("present");
    storage
        .move_to_dlq(&running, "boom", None)
        .expect("dead-letter");
    storage
        .list_dead(100, 0, Some(NAMESPACE))
        .expect("list")
        .into_iter()
        .find(|entry| entry.original_job_id == job.id)
        .expect("the entry")
        .id
}

/// Run one job of `task` in `queue` to completion, for a throughput count.
fn completed_job(storage: &TempStorage, queue: &str, task: &str) {
    let job = storage.enqueue(stored_job(queue, task)).expect("enqueue");
    storage
        .dequeue(queue, now_millis() + 1_000, Some(NAMESPACE))
        .expect("dequeue");
    storage
        .complete(&job.id, None, Some(NAMESPACE))
        .expect("complete");
}

fn queue_name(queue: &str) -> QueueNameArgs {
    QueueNameArgs {
        queue: queue.to_string(),
    }
}

fn periodic_name(name: &str) -> PeriodicNameArgs {
    PeriodicNameArgs {
        name: name.to_string(),
    }
}

/// `fq periodic put NAME --task T --cron EXPR ARGS... --kw K=V...`.
fn put_args(name: &str, args: &[&str], kwargs: &[&str]) -> PeriodicPutArgs {
    PeriodicPutArgs {
        name: name.to_string(),
        args: args.iter().map(|value| (*value).to_string()).collect(),
        kwargs: kwargs.iter().map(|value| (*value).to_string()).collect(),
        task: "report".to_string(),
        cron: "0 0 3 * * *".to_string(),
        queue: None,
        timezone: None,
        paused: false,
    }
}

/// An empty `fq overrides set-task TASK`, to fill in per test.
fn task_override_args(task: &str) -> SetTaskOverrideArgs {
    SetTaskOverrideArgs {
        task: task.to_string(),
        rate_limit: None,
        max_concurrent: None,
        max_retries: None,
        retry_backoff_ms: None,
        timeout_ms: None,
        priority: None,
        paused: None,
    }
}

/// `fq pause` and `fq resume` reach this namespace's queue, and `fq queues
/// --list` sees the pause.
#[tokio::test]
async fn pause_and_resume_through_the_cli() {
    let mut harness = Harness::start("admin-pause").await;
    harness
        .storage
        .enqueue(stored_job("emails", "send"))
        .expect("enqueue");

    let paused = harness
        .admin
        .pause_queue(commands::queues::pause_request(&queue_name("emails")))
        .await
        .expect("pause")
        .into_inner()
        .queue
        .expect("the queue");
    assert!(paused.paused);
    assert_eq!(paused.pending, 1);

    let listed = harness
        .admin
        .list_queues(cli_pb::ListQueuesRequest {})
        .await
        .expect("list")
        .into_inner()
        .queues;
    assert_eq!(listed.len(), 1);
    assert!(listed[0].paused);

    let resumed = harness
        .admin
        .resume_queue(commands::queues::resume_request(&queue_name("emails")))
        .await
        .expect("resume")
        .into_inner()
        .queue
        .expect("the queue");
    assert!(!resumed.paused);
    harness.stop().await;
}

/// A page, a replay that comes back as a fresh job, and a filtered purge.
#[tokio::test]
async fn dead_letters_are_paged_replayed_and_purged_through_the_cli() {
    let mut harness = Harness::start("admin-dlq").await;
    dead_letter(&harness.storage, "charge");
    dead_letter(&harness.storage, "refund");

    let first = harness
        .admin
        .list_dead_letters(commands::dlq::list_request(&DlqListArgs {
            page_size: Some(1),
            page_token: None,
            all: false,
        }))
        .await
        .expect("list")
        .into_inner();
    assert_eq!(first.dead_letters.len(), 1);
    assert!(!first.next_page_token.is_empty(), "a second page exists");

    let entry = first.dead_letters[0].clone();
    let job = harness
        .admin
        .replay_dead_letter(commands::dlq::replay_request(&DeadLetterIdArgs {
            id: entry.id.clone(),
        }))
        .await
        .expect("replay")
        .into_inner()
        .job
        .expect("the new job");
    assert_eq!(job.task_name, entry.task_name);
    assert_eq!(job.queue, "dlq");
    assert_eq!(job.status, flexiq_cli::pb::JobStatus::Pending as i32);
    assert_ne!(job.id, entry.original_job_id, "a replay is a fresh job");

    let remaining = if entry.task_name == "charge" {
        "refund"
    } else {
        "charge"
    };
    let request = commands::dlq::purge_request(
        &DlqPurgeArgs {
            task: Some(remaining.to_string()),
            before: None,
            older_than_ms: None,
            all: false,
        },
        now_millis(),
    )
    .expect("builds");
    let purged = harness
        .admin
        .purge_dead_letters(request)
        .await
        .expect("purge")
        .into_inner()
        .purged;
    assert_eq!(purged, 1);

    let left = harness
        .admin
        .list_dead_letters(cli_pb::ListDeadLettersRequest::default())
        .await
        .expect("list")
        .into_inner();
    assert!(left.dead_letters.is_empty(), "{left:?}");
    harness.stop().await;
}

/// What an operator types after `fq periodic put` reaches each firing as the
/// payload an SDK would have sent for the same call.
#[tokio::test]
async fn a_periodic_task_put_through_the_cli_fires_the_arguments_typed() {
    let mut harness = Harness::start("admin-periodic").await;
    let request =
        commands::periodic::put_request(&put_args("nightly", &["a@b.c", "3"], &["retries=2"]))
            .expect("builds");
    let task = harness
        .admin
        .put_periodic_task(request)
        .await
        .expect("put")
        .into_inner()
        .periodic_task
        .expect("the task");
    assert_eq!(task.name, "nightly");
    assert!(task.enabled);

    let job = harness
        .admin
        .trigger_periodic_task(commands::periodic::trigger_request(&periodic_name(
            "nightly",
        )))
        .await
        .expect("trigger")
        .into_inner()
        .job
        .expect("the job");
    assert_eq!(job.task_name, "report");

    let stored = harness
        .storage
        .get_job(&job.id, Some(NAMESPACE))
        .expect("read")
        .expect("enqueued");
    let expected = encode_call(
        &[WireValue::Text("a@b.c".to_string()), WireValue::Integer(3)],
        &[("retries".to_string(), WireValue::Integer(2))],
    );
    assert_eq!(stored.payload, expected);
    harness.stop().await;
}

/// `set-task` replaces rather than merges, and `list` shows what is stored.
#[tokio::test]
async fn a_task_override_set_through_the_cli_is_listed_and_replaced() {
    let mut harness = Harness::start("admin-overrides").await;
    let mut args = task_override_args("send");
    args.rate_limit = Some("10/s".to_string());
    args.timeout_ms = Some(30_000);
    let stored = harness
        .admin
        .set_task_override(commands::overrides::set_task_request(&args).expect("builds"))
        .await
        .expect("set")
        .into_inner()
        .task_override
        .expect("as stored");
    assert_eq!(stored.rate_limit.as_deref(), Some("10/s"));
    assert_eq!(stored.timeout.expect("set").seconds, 30);

    let listed = harness
        .admin
        .list_overrides(cli_pb::ListOverridesRequest {})
        .await
        .expect("list")
        .into_inner();
    assert_eq!(listed.tasks["send"].rate_limit.as_deref(), Some("10/s"));

    let mut args = task_override_args("send");
    args.max_retries = Some(2);
    let replaced = harness
        .admin
        .set_task_override(commands::overrides::set_task_request(&args).expect("builds"))
        .await
        .expect("replace")
        .into_inner()
        .task_override
        .expect("as stored");
    assert_eq!(replaced.rate_limit, None, "a set is a replace, not a merge");
    assert_eq!(replaced.max_retries, Some(2));
    harness.stop().await;
}

/// A producer's token on an admin verb names the scope it lacked.
#[tokio::test]
async fn a_produce_only_token_is_told_it_lacks_inspect() {
    let harness = Harness::start("admin-scope").await;
    let mut admin = harness.admin_with(ScopeSet::of(&[Scope::Produce])).await;
    let status = admin
        .list_queues(cli_pb::ListQueuesRequest {})
        .await
        .expect_err("no inspect scope");
    let text = flexiq_cli::error::describe(&status);
    assert!(text.contains("SCOPE_DENIED"), "{text}");
    assert!(text.contains("scope=inspect"), "{text}");
    harness.stop().await;
}

/// Render one admin message both ways — fq's writer on fq's generated type, the
/// facade's on the server's, re-encoded across — and assert the objects equal.
fn assert_same_render<C, S>(cli: &C, cli_json: impl Fn(&C) -> Value, server_json: fn(&S) -> Value)
where
    C: Message + std::fmt::Debug,
    S: Message + Default,
{
    let server = S::decode(cli.encode_to_vec().as_slice()).expect("the same message");
    assert_eq!(cli_json(cli), server_json(&server), "{cli:?}");
}

/// The admin verbs' `--json` against the facade's writer, message for
/// message, on real responses from a door holding one of everything.
#[tokio::test]
async fn the_admin_json_render_matches_the_facade() {
    let mut harness = Harness::start("admin-json-parity").await;
    let storage = &harness.storage;
    storage
        .enqueue(stored_job("emails", "send"))
        .expect("enqueue");
    completed_job(storage, "reports", "build");
    let entry = dead_letter(storage, "charge");
    storage
        .register_worker(
            &WorkerRegistration::new("w-1", "emails,reports", 4)
                .namespace(Some(NAMESPACE))
                .sdk(Some("python"), Some("2.0.0")),
        )
        .expect("register");
    let admin = &mut harness.admin;

    let queues = admin
        .list_queues(cli_pb::ListQueuesRequest {})
        .await
        .expect("list")
        .into_inner();
    assert_same_render(
        &queues,
        cli_render::list_queues_json,
        server_render::list_queues,
    );

    let paused = admin
        .pause_queue(commands::queues::pause_request(&queue_name("emails")))
        .await
        .expect("pause")
        .into_inner();
    assert_same_render(
        &paused,
        |r: &cli_pb::PauseQueueResponse| cli_render::queue_envelope_json(r.queue.as_ref()),
        server_render::pause_queue,
    );
    let resumed = admin
        .resume_queue(commands::queues::resume_request(&queue_name("emails")))
        .await
        .expect("resume")
        .into_inner();
    assert_same_render(
        &resumed,
        |r: &cli_pb::ResumeQueueResponse| cli_render::queue_envelope_json(r.queue.as_ref()),
        server_render::resume_queue,
    );

    let throughput = admin
        .get_throughput(
            commands::throughput::request(&ThroughputArgs {
                window_ms: Some(600_000),
            })
            .expect("builds"),
        )
        .await
        .expect("throughput")
        .into_inner();
    assert!(!throughput.queues.is_empty(), "a completed job counts");
    assert_same_render(
        &throughput,
        cli_render::throughput_json,
        server_render::get_throughput,
    );

    let workers = admin
        .list_workers(cli_pb::ListWorkersRequest {})
        .await
        .expect("workers")
        .into_inner();
    assert_eq!(workers.workers.len(), 1);
    assert_same_render(
        &workers,
        cli_render::list_workers_json,
        server_render::list_workers,
    );

    let listed = admin
        .list_dead_letters(cli_pb::ListDeadLettersRequest::default())
        .await
        .expect("list")
        .into_inner();
    assert_same_render(
        &listed,
        cli_render::list_dead_letters_json,
        server_render::list_dead_letters,
    );
    let read = admin
        .get_dead_letter(commands::dlq::show_request(&DlqShowArgs {
            id: entry.clone(),
            payload: true,
        }))
        .await
        .expect("get")
        .into_inner();
    assert!(read
        .dead_letter
        .as_ref()
        .is_some_and(|d| d.payload.is_some()));
    assert_same_render(
        &read,
        |r: &cli_pb::GetDeadLetterResponse| {
            cli_render::dead_letter_envelope_json(r.dead_letter.as_ref())
        },
        server_render::get_dead_letter,
    );
    let replayed = admin
        .replay_dead_letter(commands::dlq::replay_request(&DeadLetterIdArgs {
            id: entry,
        }))
        .await
        .expect("replay")
        .into_inner();
    assert_same_render(
        &replayed,
        |r: &cli_pb::ReplayDeadLetterResponse| {
            flexiq_cli::output::job_envelope_json(r.job.as_ref())
        },
        server_render::replay_dead_letter,
    );
    dead_letter(&harness.storage, "charge");
    let purged = admin
        .purge_dead_letters(cli_pb::PurgeDeadLettersRequest { filter: None })
        .await
        .expect("purge")
        .into_inner();
    assert_eq!(purged.purged, 1);
    assert_same_render(
        &purged,
        cli_render::purge_json,
        server_render::purge_dead_letters,
    );

    let mut put = put_args("nightly", &["1"], &[]);
    put.timezone = Some("Europe/Paris".to_string());
    let put = admin
        .put_periodic_task(commands::periodic::put_request(&put).expect("builds"))
        .await
        .expect("put")
        .into_inner();
    assert_same_render(
        &put,
        |r: &cli_pb::PutPeriodicTaskResponse| {
            cli_render::periodic_task_envelope_json(r.periodic_task.as_ref())
        },
        server_render::put_periodic_task,
    );
    let read = admin
        .get_periodic_task(commands::periodic::show_request(&PeriodicShowArgs {
            name: "nightly".to_string(),
            payload: true,
        }))
        .await
        .expect("get")
        .into_inner();
    assert!(read
        .periodic_task
        .as_ref()
        .is_some_and(|t| t.payload.is_some()));
    assert_same_render(
        &read,
        |r: &cli_pb::GetPeriodicTaskResponse| {
            cli_render::periodic_task_envelope_json(r.periodic_task.as_ref())
        },
        server_render::get_periodic_task,
    );
    let paused = admin
        .pause_periodic_task(commands::periodic::pause_request(&periodic_name("nightly")))
        .await
        .expect("pause")
        .into_inner();
    assert_same_render(
        &paused,
        |r: &cli_pb::PausePeriodicTaskResponse| {
            cli_render::periodic_task_envelope_json(r.periodic_task.as_ref())
        },
        server_render::pause_periodic_task,
    );
    let resumed = admin
        .resume_periodic_task(commands::periodic::resume_request(&periodic_name(
            "nightly",
        )))
        .await
        .expect("resume")
        .into_inner();
    assert_same_render(
        &resumed,
        |r: &cli_pb::ResumePeriodicTaskResponse| {
            cli_render::periodic_task_envelope_json(r.periodic_task.as_ref())
        },
        server_render::resume_periodic_task,
    );
    let listed = admin
        .list_periodic_tasks(cli_pb::ListPeriodicTasksRequest {})
        .await
        .expect("list")
        .into_inner();
    assert_same_render(
        &listed,
        cli_render::list_periodic_tasks_json,
        server_render::list_periodic_tasks,
    );
    let triggered = admin
        .trigger_periodic_task(commands::periodic::trigger_request(&periodic_name(
            "nightly",
        )))
        .await
        .expect("trigger")
        .into_inner();
    assert_same_render(
        &triggered,
        |r: &cli_pb::TriggerPeriodicTaskResponse| {
            flexiq_cli::output::job_envelope_json(r.job.as_ref())
        },
        server_render::trigger_periodic_task,
    );

    let mut task = task_override_args("send");
    task.rate_limit = Some("100/m".to_string());
    task.max_concurrent = Some(2);
    task.max_retries = Some(3);
    task.retry_backoff_ms = Some(1_500);
    task.timeout_ms = Some(30_000);
    task.priority = Some(5);
    task.paused = Some(false);
    let set_task = admin
        .set_task_override(commands::overrides::set_task_request(&task).expect("builds"))
        .await
        .expect("set")
        .into_inner();
    assert_same_render(
        &set_task,
        |r: &cli_pb::SetTaskOverrideResponse| {
            cli_render::task_override_envelope_json(r.task_override.as_ref())
        },
        server_render::set_task_override,
    );
    let set_queue = admin
        .set_queue_override(commands::overrides::set_queue_request(
            &SetQueueOverrideArgs {
                queue: "emails".to_string(),
                rate_limit: Some("10/s".to_string()),
                max_concurrent: Some(4),
            },
        ))
        .await
        .expect("set")
        .into_inner();
    assert_same_render(
        &set_queue,
        |r: &cli_pb::SetQueueOverrideResponse| {
            cli_render::queue_override_envelope_json(r.queue_override.as_ref())
        },
        server_render::set_queue_override,
    );
    let overrides = admin
        .list_overrides(cli_pb::ListOverridesRequest {})
        .await
        .expect("list")
        .into_inner();
    assert_eq!((overrides.tasks.len(), overrides.queues.len()), (1, 1));
    assert_same_render(
        &overrides,
        cli_render::list_overrides_json,
        server_render::list_overrides,
    );

    // The empty responses, which both sides must write as `{}`.
    assert_same_render(
        &cli_pb::DeleteDeadLetterResponse {},
        |_| cli_render::empty_json(),
        server_render::empty::<server_pb::DeleteDeadLetterResponse>,
    );
    harness.stop().await;
}

/// Fields a real door leaves unset in the parity test above — a worker's pid
/// and pool, a dead letter's metadata — rendered fully populated, so a field
/// only one side learns fails here too.
#[test]
fn fully_populated_admin_messages_render_alike() {
    let at = |seconds| prost_types::Timestamp { seconds, nanos: 0 };
    let worker = cli_pb::ListWorkersResponse {
        workers: vec![cli_pb::Worker {
            worker_id: "w-1".into(),
            queues: vec!["emails".into()],
            status: cli_pb::WorkerStatus::Draining as i32,
            last_heartbeat: Some(at(1_756_900_000)),
            concurrency: 4,
            started_at: Some(at(1_756_899_000)),
            hostname: Some("host".into()),
            pid: Some(42),
            pool_type: Some("thread".into()),
            sdk: Some("rust".into()),
            sdk_version: Some("2.0.0".into()),
        }],
    };
    assert_same_render(
        &worker,
        cli_render::list_workers_json,
        server_render::list_workers,
    );

    let unknown = cli_pb::ListWorkersResponse {
        workers: vec![cli_pb::Worker {
            status: 99,
            ..Default::default()
        }],
    };
    assert_same_render(
        &unknown,
        cli_render::list_workers_json,
        server_render::list_workers,
    );

    let entry = cli_pb::GetDeadLetterResponse {
        dead_letter: Some(cli_pb::DeadLetter {
            id: "dl-1".into(),
            original_job_id: "job-1".into(),
            queue: "emails".into(),
            task_name: "send".into(),
            failed_at: Some(prost_types::Timestamp {
                seconds: 1_756_900_000,
                nanos: 250_000_000,
            }),
            retry_count: 3,
            max_retries: 3,
            priority: 1,
            replay_count: 2,
            error: Some("boom".into()),
            metadata: Some("{}".into()),
            payload: Some(vec![]),
        }),
    };
    assert_same_render(
        &entry,
        |r: &cli_pb::GetDeadLetterResponse| {
            cli_render::dead_letter_envelope_json(r.dead_letter.as_ref())
        },
        server_render::get_dead_letter,
    );

    let task = cli_pb::GetPeriodicTaskResponse {
        periodic_task: Some(cli_pb::PeriodicTask {
            name: "nightly".into(),
            task_name: "report".into(),
            cron: "0 0 3 * * *".into(),
            queue: "default".into(),
            enabled: false,
            next_run: Some(at(1_756_900_000)),
            last_run: Some(at(1_756_800_000)),
            timezone: Some("UTC".into()),
            payload: Some(vec![0x02]),
        }),
    };
    assert_same_render(
        &task,
        |r: &cli_pb::GetPeriodicTaskResponse| {
            cli_render::periodic_task_envelope_json(r.periodic_task.as_ref())
        },
        server_render::get_periodic_task,
    );

    let throughput = cli_pb::GetThroughputResponse {
        window: Some(prost_types::Duration {
            seconds: 90,
            nanos: 500_000_000,
        }),
        since: Some(at(1_756_900_000)),
        queues: vec![cli_pb::QueueThroughput {
            queue: "emails".into(),
            completed: 1,
            failed: 2,
            dead: 3,
            cancelled: 4,
        }],
    };
    assert_same_render(
        &throughput,
        cli_render::throughput_json,
        server_render::get_throughput,
    );
}

/// The admin print paths over a real door, both renderings. They write to
/// stdout, so what is asserted is that none of them errors on a real response.
#[tokio::test]
async fn every_admin_command_runs_against_a_real_door() {
    let mut harness = Harness::start("admin-print-paths").await;
    harness
        .storage
        .enqueue(stored_job("emails", "send"))
        .expect("enqueue");
    completed_job(&harness.storage, "reports", "build");
    let entry = dead_letter(&harness.storage, "charge");
    harness
        .storage
        .register_worker(
            &WorkerRegistration::new("w-1", "emails,reports", 4)
                .namespace(Some(NAMESPACE))
                .sdk(Some("python"), Some("2.0.0")),
        )
        .expect("register");
    let admin = &mut harness.admin;

    let set_task = || {
        OverridesCommand::SetTask(SetTaskOverrideArgs {
            rate_limit: Some("100/m".to_string()),
            timeout_ms: Some(30_000),
            ..task_override_args("send")
        })
    };
    let set_queue = || {
        OverridesCommand::SetQueue(SetQueueOverrideArgs {
            queue: "emails".to_string(),
            rate_limit: None,
            max_concurrent: Some(4),
        })
    };

    for json in [false, true] {
        commands::queues::list(admin, json)
            .await
            .expect("queues --list");
        commands::queues::pause(admin, &queue_name("emails"), json)
            .await
            .expect("pause");
        commands::queues::resume(admin, &queue_name("emails"), json)
            .await
            .expect("resume");
        commands::throughput::run(admin, &ThroughputArgs { window_ms: None }, json)
            .await
            .expect("throughput");
        commands::workers::run(admin, json).await.expect("workers");
        commands::dlq::run(
            admin,
            &DlqCommand::List(DlqListArgs {
                page_size: None,
                page_token: None,
                all: true,
            }),
            json,
        )
        .await
        .expect("dlq list --all");
        commands::dlq::run(
            admin,
            &DlqCommand::Show(DlqShowArgs {
                id: entry.clone(),
                payload: true,
            }),
            json,
        )
        .await
        .expect("dlq show");
        commands::periodic::run(
            admin,
            &PeriodicCommand::Put(put_args("nightly", &["a@b.c"], &["n=1"])),
            json,
        )
        .await
        .expect("periodic put");
        for command in [
            PeriodicCommand::List,
            PeriodicCommand::Show(PeriodicShowArgs {
                name: "nightly".to_string(),
                payload: true,
            }),
            PeriodicCommand::Pause(periodic_name("nightly")),
            PeriodicCommand::Resume(periodic_name("nightly")),
            PeriodicCommand::Trigger(periodic_name("nightly")),
        ] {
            commands::periodic::run(admin, &command, json)
                .await
                .expect("periodic");
        }
        for command in [set_task(), set_queue(), OverridesCommand::List] {
            commands::overrides::run(admin, &command, json)
                .await
                .expect("overrides");
        }
    }

    // The verbs that consume what they act on, once each.
    commands::dlq::run(
        admin,
        &DlqCommand::Replay(DeadLetterIdArgs { id: entry }),
        false,
    )
    .await
    .expect("dlq replay");
    let again = dead_letter(&harness.storage, "charge");
    commands::dlq::run(
        admin,
        &DlqCommand::Delete(DeadLetterIdArgs { id: again }),
        false,
    )
    .await
    .expect("dlq delete");
    commands::dlq::run(
        admin,
        &DlqCommand::Purge(DlqPurgeArgs {
            task: None,
            before: None,
            older_than_ms: None,
            all: true,
        }),
        true,
    )
    .await
    .expect("dlq purge");
    commands::periodic::run(
        admin,
        &PeriodicCommand::Delete(periodic_name("nightly")),
        false,
    )
    .await
    .expect("periodic delete");
    commands::overrides::run(
        admin,
        &OverridesCommand::ClearTask(TaskNameArgs {
            task: "send".to_string(),
        }),
        false,
    )
    .await
    .expect("clear-task");
    commands::overrides::run(
        admin,
        &OverridesCommand::ClearQueue(queue_name("emails")),
        true,
    )
    .await
    .expect("clear-queue");
    harness.stop().await;
}
