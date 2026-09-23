//! End-to-end: `flexiq.admin.v1.AdminService` over a real socket (#836).
//!
//! What is pinned is what an operator depends on and cannot see: every RPC
//! reaches only the credential's namespace, another tenant's resource answers
//! like an absent one, `inspect` reads and `admin` writes and neither implies
//! the other, and a periodic trigger enqueues the job the schedule would.
#![cfg(feature = "grpc")]

mod support;

use flexiq_core::job::{now_millis, NewJob};
use flexiq_core::storage::records::WorkerRegistration;
use flexiq_core::{override_key, OverrideScope, Storage};
use flexiq_server::config::grpc::GrpcConfig;
use flexiq_server::config::listen::ListenAddress;
use flexiq_server::grpc::pb::admin::admin_service_client::AdminServiceClient;
use flexiq_server::grpc::pb::admin::{
    purge_dead_letters_request, put_periodic_task_request, ClearTaskOverrideRequest,
    DeleteDeadLetterRequest, DeletePeriodicTaskRequest, GetDeadLetterRequest,
    GetPeriodicTaskRequest, GetThroughputRequest, ListDeadLettersRequest, ListOverridesRequest,
    ListPeriodicTasksRequest, ListQueuesRequest, ListWorkersRequest, PausePeriodicTaskRequest,
    PauseQueueRequest, PurgeDeadLettersRequest, PutPeriodicTaskRequest, QueueOverride,
    ReplayDeadLetterRequest, ResumePeriodicTaskRequest, ResumeQueueRequest,
    SetQueueOverrideRequest, SetTaskOverrideRequest, TaskOverride, TriggerPeriodicTaskRequest,
    WorkerStatus,
};
use flexiq_server::grpc::pb::{JobStatus, StructuredArgs};
use flexiq_server::grpc::status::reason;
use flexiq_server::grpc::Listener;
use flexiq_server::runtime::shutdown::Shutdown;
use flexiq_server::tokens::{Scope, ScopeSet};
use prost_types::value::Kind;
use prost_types::Value;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;
use tonic::{Code, Status};
use tonic_types::StatusExt;

use support::{mint_token, temp_storage, temp_workflows, Bearer, TempStorage};

/// The namespace this door serves.
const NAMESPACE: &str = "grpc-admin-tests";
/// Another tenant on the same database.
const OTHER: &str = "grpc-admin-other";

type Client = AdminServiceClient<InterceptedService<Channel, Bearer>>;

/// A running listener, and an admin client holding a token with `scopes`.
struct Harness {
    client: Client,
    storage: TempStorage,
    shutdown: Shutdown,
    served: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl Harness {
    async fn start(label: &str) -> Self {
        Self::with_scopes(label, ScopeSet::of(&[Scope::Inspect, Scope::Admin])).await
    }

    async fn with_scopes(label: &str, scopes: ScopeSet) -> Self {
        let storage = temp_storage(label);
        let token = mint_token(&storage, NAMESPACE, scopes);
        let shutdown = Shutdown::default();
        let listener = Listener::bind(&GrpcConfig::new(
            ListenAddress::Tcp("127.0.0.1:0".parse().expect("valid address")),
            NAMESPACE,
        ))
        .await
        .expect("bind");
        let addr = listener
            .local_addr()
            .expect("a TCP listener has an address");
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
            client: AdminServiceClient::with_interceptor(channel, Bearer::new(&token)),
            storage,
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

fn job_in(namespace: Option<&str>, queue: &str, task: &str) -> NewJob {
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
        namespace: namespace.map(str::to_owned),
        debounce_key: None,
    }
}

/// Dead-letter one job of `task` in `namespace`, returning the entry's id.
fn dead_letter(storage: &TempStorage, namespace: &str, task: &str) -> String {
    let job = storage
        .enqueue(job_in(Some(namespace), "dlq", task))
        .expect("enqueue");
    storage
        .dequeue("dlq", now_millis() + 1_000, Some(namespace))
        .expect("dequeue");
    let running = storage
        .get_job(&job.id, None)
        .expect("read")
        .expect("present");
    storage
        .move_to_dlq(&running, "boom", None)
        .expect("dead-letter");
    storage
        .list_dead(100, 0, Some(namespace))
        .expect("list")
        .into_iter()
        .find(|entry| entry.original_job_id == job.id)
        .expect("the entry")
        .id
}

fn assert_reason(status: &Status, code: Code, want: &str) {
    assert_eq!(status.code(), code, "{status:?}");
    let details = status.get_error_details();
    let info = details
        .error_info()
        .expect("every error carries an ErrorInfo");
    assert_eq!(info.reason, want, "{status:?}");
}

#[tokio::test]
async fn a_pause_is_the_namespaces_and_shows_in_the_listing() {
    let mut harness = Harness::start("admin-queues").await;
    harness
        .storage
        .enqueue(job_in(Some(NAMESPACE), "emails", "send"))
        .expect("enqueue");
    // Another tenant's queue of another name must not appear at all.
    harness
        .storage
        .enqueue(job_in(Some(OTHER), "secret-queue", "send"))
        .expect("enqueue");
    harness
        .storage
        .pause_queue("emails", Some(OTHER))
        .expect("a pause elsewhere");

    let queue = harness
        .client
        .pause_queue(PauseQueueRequest {
            queue: "emails".into(),
        })
        .await
        .expect("pause")
        .into_inner()
        .queue
        .expect("the resulting queue");
    assert!(queue.paused);
    assert_eq!(queue.pending, 1);
    // The pause is this namespace's, not the default's.
    assert!(harness.storage.list_paused_queues(None).unwrap().is_empty());

    let listed = harness
        .client
        .list_queues(ListQueuesRequest {})
        .await
        .expect("list")
        .into_inner()
        .queues;
    let names: Vec<_> = listed.iter().map(|q| q.name.as_str()).collect();
    assert_eq!(
        names,
        ["emails"],
        "another tenant's queue leaked: {names:?}"
    );

    let queue = harness
        .client
        .resume_queue(ResumeQueueRequest {
            queue: "emails".into(),
        })
        .await
        .expect("resume")
        .into_inner()
        .queue
        .expect("the resulting queue");
    assert!(!queue.paused);
    // The other tenant's pause of the same name is untouched.
    assert_eq!(
        harness.storage.list_paused_queues(Some(OTHER)).unwrap(),
        ["emails"]
    );

    let status = harness
        .client
        .pause_queue(PauseQueueRequest { queue: "".into() })
        .await
        .expect_err("an empty name");
    assert_reason(&status, Code::InvalidArgument, reason::INVALID_REQUEST);
    harness.stop().await;
}

#[tokio::test]
async fn throughput_counts_this_namespaces_finished_jobs() {
    let mut harness = Harness::start("admin-throughput").await;
    for namespace in [NAMESPACE, OTHER] {
        let job = harness
            .storage
            .enqueue(job_in(Some(namespace), "reports", "build"))
            .expect("enqueue");
        harness
            .storage
            .dequeue("reports", now_millis() + 1_000, Some(namespace))
            .expect("dequeue");
        harness
            .storage
            .complete(&job.id, None, Some(namespace))
            .expect("complete");
    }

    let response = harness
        .client
        .get_throughput(GetThroughputRequest { window: None })
        .await
        .expect("throughput")
        .into_inner();
    assert_eq!(response.window.expect("echoed").seconds, 300);
    assert_eq!(response.queues.len(), 1);
    assert_eq!(response.queues[0].queue, "reports");
    assert_eq!(
        response.queues[0].completed, 1,
        "another tenant's job counted"
    );

    let status = harness
        .client
        .get_throughput(GetThroughputRequest {
            window: Some(prost_types::Duration {
                seconds: 0,
                nanos: 0,
            }),
        })
        .await
        .expect_err("a zero window");
    assert_reason(&status, Code::InvalidArgument, reason::INVALID_REQUEST);
    harness.stop().await;
}

#[tokio::test]
async fn dead_letters_are_listed_read_replayed_deleted_and_purged_in_one_namespace() {
    let mut harness = Harness::start("admin-dlq").await;
    let mine = dead_letter(&harness.storage, NAMESPACE, "charge");
    let theirs = dead_letter(&harness.storage, OTHER, "charge");

    let listed = harness
        .client
        .list_dead_letters(ListDeadLettersRequest::default())
        .await
        .expect("list")
        .into_inner();
    let ids: Vec<_> = listed.dead_letters.iter().map(|d| d.id.as_str()).collect();
    assert_eq!(ids, [mine.as_str()]);
    assert!(
        listed.dead_letters[0].payload.is_none(),
        "a listing carries no payload"
    );
    assert!(listed.next_page_token.is_empty());

    let read = harness
        .client
        .get_dead_letter(GetDeadLetterRequest {
            dead_letter_id: mine.clone(),
            include_payload: true,
        })
        .await
        .expect("read")
        .into_inner()
        .dead_letter
        .expect("the entry");
    assert_eq!(read.payload.as_deref(), Some(&[0x02, 0x82, 0x80, 0xa0][..]));
    assert_eq!(read.error.as_deref(), Some("boom"));

    // Another tenant's entry is absent, whichever RPC asks.
    let status = harness
        .client
        .get_dead_letter(GetDeadLetterRequest {
            dead_letter_id: theirs.clone(),
            include_payload: false,
        })
        .await
        .expect_err("a foreign entry");
    assert_reason(&status, Code::NotFound, reason::DEAD_LETTER_NOT_FOUND);
    let status = harness
        .client
        .replay_dead_letter(ReplayDeadLetterRequest {
            dead_letter_id: theirs.clone(),
        })
        .await
        .expect_err("a foreign replay");
    assert_reason(&status, Code::NotFound, reason::DEAD_LETTER_NOT_FOUND);

    let job = harness
        .client
        .replay_dead_letter(ReplayDeadLetterRequest {
            dead_letter_id: mine.clone(),
        })
        .await
        .expect("replay")
        .into_inner()
        .job
        .expect("the new job");
    assert_eq!(job.task_name, "charge");
    assert_eq!(job.status, JobStatus::Pending as i32);
    assert_eq!(job.namespace, NAMESPACE);

    let again = dead_letter(&harness.storage, NAMESPACE, "charge");
    harness
        .client
        .delete_dead_letter(DeleteDeadLetterRequest {
            dead_letter_id: again.clone(),
        })
        .await
        .expect("delete");
    let status = harness
        .client
        .delete_dead_letter(DeleteDeadLetterRequest {
            dead_letter_id: again,
        })
        .await
        .expect_err("a second delete");
    assert_reason(&status, Code::NotFound, reason::DEAD_LETTER_NOT_FOUND);

    dead_letter(&harness.storage, NAMESPACE, "charge");
    dead_letter(&harness.storage, NAMESPACE, "refund");
    let purged = harness
        .client
        .purge_dead_letters(PurgeDeadLettersRequest {
            filter: Some(purge_dead_letters_request::Filter::TaskName(
                "charge".into(),
            )),
        })
        .await
        .expect("purge by task")
        .into_inner()
        .purged;
    assert_eq!(purged, 1);
    let purged = harness
        .client
        .purge_dead_letters(PurgeDeadLettersRequest { filter: None })
        .await
        .expect("purge everything")
        .into_inner()
        .purged;
    assert_eq!(purged, 1, "only this namespace's refund was left");
    // The other tenant's entry survived both.
    assert!(harness.storage.get_dead(&theirs, None).unwrap().is_some());
    harness.stop().await;
}

#[tokio::test]
async fn workers_are_listed_for_this_namespace_only() {
    let mut harness = Harness::start("admin-workers").await;
    harness
        .storage
        .register_worker(
            &WorkerRegistration::new("w-mine", "emails,reports", 4)
                .namespace(Some(NAMESPACE))
                .sdk(Some("python"), Some("2.0.0")),
        )
        .expect("register");
    harness
        .storage
        .register_worker(&WorkerRegistration::new("w-theirs", "emails", 1).namespace(Some(OTHER)))
        .expect("register");

    let workers = harness
        .client
        .list_workers(ListWorkersRequest {})
        .await
        .expect("list")
        .into_inner()
        .workers;
    assert_eq!(workers.len(), 1, "{workers:?}");
    let worker = &workers[0];
    assert_eq!(worker.worker_id, "w-mine");
    assert_eq!(worker.queues, ["emails", "reports"]);
    assert_eq!(worker.status, WorkerStatus::Active as i32);
    assert_eq!(worker.concurrency, 4);
    assert_eq!(worker.sdk.as_deref(), Some("python"));
    assert!(worker.last_heartbeat.is_some());
    harness.stop().await;
}

/// `f(1)`, sent as structured arguments.
fn one_arg() -> put_periodic_task_request::Body {
    put_periodic_task_request::Body::Structured(StructuredArgs {
        args: vec![Value {
            kind: Some(Kind::NumberValue(1.0)),
        }],
        kwargs: Default::default(),
    })
}

fn put(name: &str, cron: &str) -> PutPeriodicTaskRequest {
    PutPeriodicTaskRequest {
        name: name.into(),
        task_name: "report".into(),
        cron: cron.into(),
        queue: String::new(),
        body: Some(one_arg()),
        start_paused: false,
        timezone: None,
    }
}

#[tokio::test]
async fn a_periodic_task_is_declared_paused_triggered_and_deleted() {
    let mut harness = Harness::start("admin-periodic").await;

    let task = harness
        .client
        .put_periodic_task(put("nightly", "0 0 0 * * *"))
        .await
        .expect("create")
        .into_inner()
        .periodic_task
        .expect("the task");
    assert_eq!(task.queue, "default");
    assert!(task.enabled);
    assert!(task.payload.is_none());
    // It is this namespace's row.
    let rows = harness.storage.list_periodic(Some(NAMESPACE)).unwrap();
    assert_eq!(rows.len(), 1);
    assert!(harness.storage.list_periodic(None).unwrap().is_empty());

    // A pause survives a redeclaration, as a code declaration's would.
    let paused = harness
        .client
        .pause_periodic_task(PausePeriodicTaskRequest {
            name: "nightly".into(),
        })
        .await
        .expect("pause")
        .into_inner()
        .periodic_task
        .expect("the task");
    assert!(!paused.enabled);
    let redeclared = harness
        .client
        .put_periodic_task(put("nightly", "0 30 0 * * *"))
        .await
        .expect("replace")
        .into_inner()
        .periodic_task
        .expect("the task");
    assert!(!redeclared.enabled, "a replace resumed a paused task");
    assert_eq!(redeclared.cron, "0 30 0 * * *");
    harness
        .client
        .resume_periodic_task(ResumePeriodicTaskRequest {
            name: "nightly".into(),
        })
        .await
        .expect("resume");

    let read = harness
        .client
        .get_periodic_task(GetPeriodicTaskRequest {
            name: "nightly".into(),
            include_payload: true,
        })
        .await
        .expect("read")
        .into_inner()
        .periodic_task
        .expect("the task");
    // `f(1)`: the envelope a structured enqueue of the same call carries.
    let envelope = vec![0x02, 0x82, 0x81, 0x01, 0xa0];
    assert_eq!(read.payload.as_deref(), Some(&envelope[..]));

    let job = harness
        .client
        .trigger_periodic_task(TriggerPeriodicTaskRequest {
            name: "nightly".into(),
        })
        .await
        .expect("trigger")
        .into_inner()
        .job
        .expect("the job");
    assert_eq!(job.task_name, "report");
    assert_eq!(job.namespace, NAMESPACE);
    let stored = harness
        .storage
        .get_job(&job.id, Some(NAMESPACE))
        .unwrap()
        .expect("enqueued");
    assert_eq!(stored.payload, envelope);
    // A trigger leaves the schedule alone.
    let after = harness.storage.list_periodic(Some(NAMESPACE)).unwrap();
    assert_eq!(after[0].last_run, None);

    let listed = harness
        .client
        .list_periodic_tasks(ListPeriodicTasksRequest {})
        .await
        .expect("list")
        .into_inner()
        .periodic_tasks;
    assert_eq!(listed.len(), 1);

    harness
        .client
        .delete_periodic_task(DeletePeriodicTaskRequest {
            name: "nightly".into(),
        })
        .await
        .expect("delete");
    let status = harness
        .client
        .trigger_periodic_task(TriggerPeriodicTaskRequest {
            name: "nightly".into(),
        })
        .await
        .expect_err("gone");
    assert_reason(&status, Code::NotFound, reason::PERIODIC_TASK_NOT_FOUND);

    for (cron, timezone) in [("not a cron", None), ("0 0 0 * * *", Some("Not/AZone"))] {
        let mut request = put("bad", cron);
        request.timezone = timezone.map(str::to_owned);
        let status = harness
            .client
            .put_periodic_task(request)
            .await
            .expect_err("refused before anything is written");
        assert_reason(&status, Code::InvalidArgument, reason::INVALID_REQUEST);
    }
    assert!(harness
        .storage
        .list_periodic(Some(NAMESPACE))
        .unwrap()
        .is_empty());
    harness.stop().await;
}

#[tokio::test]
async fn another_namespaces_periodic_task_is_absent() {
    let mut harness = Harness::start("admin-periodic-ns").await;
    harness
        .storage
        .register_periodic(&flexiq_core::NewPeriodicTask {
            name: "theirs".into(),
            task_name: "report".into(),
            cron_expr: "0 0 0 * * *".into(),
            args: None,
            kwargs: None,
            queue: "default".into(),
            enabled: true,
            next_run: now_millis(),
            timezone: None,
            namespace: Some(OTHER.into()),
        })
        .expect("register");

    let status = harness
        .client
        .get_periodic_task(GetPeriodicTaskRequest {
            name: "theirs".into(),
            include_payload: false,
        })
        .await
        .expect_err("a foreign task");
    assert_reason(&status, Code::NotFound, reason::PERIODIC_TASK_NOT_FOUND);
    let status = harness
        .client
        .pause_periodic_task(PausePeriodicTaskRequest {
            name: "theirs".into(),
        })
        .await
        .expect_err("a foreign pause");
    assert_reason(&status, Code::NotFound, reason::PERIODIC_TASK_NOT_FOUND);
    assert!(harness.storage.list_periodic(Some(OTHER)).unwrap()[0].enabled);
    harness.stop().await;
}

#[tokio::test]
async fn overrides_are_replaced_under_the_namespaced_key() {
    let mut harness = Harness::start("admin-overrides").await;
    let stored = harness
        .client
        .set_task_override(SetTaskOverrideRequest {
            task_name: "send".into(),
            task_override: Some(TaskOverride {
                rate_limit: Some("10/s".into()),
                timeout: Some(prost_types::Duration {
                    seconds: 30,
                    nanos: 0,
                }),
                ..Default::default()
            }),
        })
        .await
        .expect("set")
        .into_inner()
        .task_override
        .expect("the stored override");
    assert_eq!(stored.rate_limit.as_deref(), Some("10/s"));
    assert!(stored.update_time.is_some());

    let key = override_key(OverrideScope::Task, Some(NAMESPACE), "send");
    let raw = harness
        .storage
        .get_setting(&key)
        .unwrap()
        .expect("namespaced key");
    assert!(raw.contains("\"timeout\":30"), "{raw}");
    assert!(harness
        .storage
        .get_setting(&override_key(OverrideScope::Task, None, "send"))
        .unwrap()
        .is_none());

    // A replace, not a merge: the field left out is gone.
    let stored = harness
        .client
        .set_task_override(SetTaskOverrideRequest {
            task_name: "send".into(),
            task_override: Some(TaskOverride {
                max_retries: Some(2),
                ..Default::default()
            }),
        })
        .await
        .expect("replace")
        .into_inner()
        .task_override
        .expect("the stored override");
    assert_eq!(stored.rate_limit, None);
    assert_eq!(stored.max_retries, Some(2));

    // A queue override keeps a `paused` the dashboard stored.
    let queue_key = override_key(OverrideScope::Queue, Some(NAMESPACE), "emails");
    harness
        .storage
        .set_setting(&queue_key, r#"{"paused":true,"max_concurrent":1}"#)
        .unwrap();
    harness
        .client
        .set_queue_override(SetQueueOverrideRequest {
            queue: "emails".into(),
            queue_override: Some(QueueOverride {
                max_concurrent: Some(5),
                ..Default::default()
            }),
        })
        .await
        .expect("set queue");
    let raw = harness
        .storage
        .get_setting(&queue_key)
        .unwrap()
        .expect("stored");
    assert!(raw.contains("\"paused\":true"), "{raw}");
    assert!(raw.contains("\"max_concurrent\":5"), "{raw}");

    let listed = harness
        .client
        .list_overrides(ListOverridesRequest {})
        .await
        .expect("list")
        .into_inner();
    assert_eq!(listed.tasks.len(), 1);
    assert_eq!(listed.queues["emails"].max_concurrent, Some(5));

    harness
        .client
        .clear_task_override(ClearTaskOverrideRequest {
            task_name: "send".into(),
        })
        .await
        .expect("clear");
    assert!(harness.storage.get_setting(&key).unwrap().is_none());

    let status = harness
        .client
        .set_task_override(SetTaskOverrideRequest {
            task_name: "send".into(),
            task_override: Some(TaskOverride {
                rate_limit: Some("0/s".into()),
                ..Default::default()
            }),
        })
        .await
        .expect_err("a rate that never releases a job");
    assert_reason(&status, Code::InvalidArgument, reason::INVALID_REQUEST);
    harness.stop().await;
}

/// `inspect` reads and `admin` writes; neither implies the other, and a
/// producer's token reaches neither.
#[tokio::test]
async fn each_scope_reaches_its_half_of_the_service_and_no_more() {
    let denied = |status: Status, scope: &str| {
        assert_reason(&status, Code::PermissionDenied, reason::SCOPE_DENIED);
        let details = status.get_error_details();
        let info = details.error_info().expect("details");
        assert_eq!(
            info.metadata.get(reason::KEY_SCOPE).map(String::as_str),
            Some(scope)
        );
    };
    let pause = || PauseQueueRequest {
        queue: "emails".into(),
    };

    let mut produce =
        Harness::with_scopes("admin-scope-produce", ScopeSet::of(&[Scope::Produce])).await;
    denied(
        produce
            .client
            .list_queues(ListQueuesRequest {})
            .await
            .expect_err("read"),
        "inspect",
    );
    denied(
        produce
            .client
            .pause_queue(pause())
            .await
            .expect_err("write"),
        "admin",
    );
    produce.stop().await;

    let mut inspect =
        Harness::with_scopes("admin-scope-inspect", ScopeSet::of(&[Scope::Inspect])).await;
    inspect
        .client
        .list_queues(ListQueuesRequest {})
        .await
        .expect("inspect reads");
    denied(
        inspect
            .client
            .pause_queue(pause())
            .await
            .expect_err("write"),
        "admin",
    );
    assert!(inspect
        .storage
        .list_paused_queues(Some(NAMESPACE))
        .unwrap()
        .is_empty());
    inspect.stop().await;

    let mut admin = Harness::with_scopes("admin-scope-admin", ScopeSet::of(&[Scope::Admin])).await;
    admin
        .client
        .pause_queue(pause())
        .await
        .expect("admin writes");
    denied(
        admin
            .client
            .list_workers(ListWorkersRequest {})
            .await
            .expect_err("read"),
        "inspect",
    );
    admin.stop().await;
}
