//! End-to-end: an operator token narrowed to some queues and tasks, over a
//! real socket (#989).
//!
//! The unit tests pin the grammar. What only a running door can show is that
//! every operator RPC that names a queue or a task honours a narrowed grant,
//! that a row keyed by id outside the grants reads as absent, that a listing
//! must name what it lists, and that every other RPC refuses a narrowed token
//! rather than serve it the whole namespace.
#![cfg(feature = "grpc")]

mod support;

use flexiq_core::job::{now_millis, NewJob};
use flexiq_core::Storage;
use flexiq_server::config::grpc::GrpcConfig;
use flexiq_server::config::listen::ListenAddress;
use flexiq_server::grpc::pb::admin::admin_service_client::AdminServiceClient;
use flexiq_server::grpc::pb::admin::{
    purge_dead_letters_request, ClearQueueOverrideRequest, ClearTaskOverrideRequest,
    DeleteDeadLetterRequest, DeletePeriodicTaskRequest, DrainWorkerRequest, GetDeadLetterRequest,
    GetNamespaceQuotaRequest, GetPeriodicTaskRequest, GetThroughputRequest, ListDeadLettersRequest,
    ListOverridesRequest, ListPeriodicTasksRequest, ListQueuesRequest, ListWorkersRequest,
    PausePeriodicTaskRequest, PauseQueueRequest, PurgeDeadLettersRequest, PutPeriodicTaskRequest,
    QueueOverride, ReplayDeadLetterRequest, ResumePeriodicTaskRequest, ResumeQueueRequest,
    SetQueueOverrideRequest, SetTaskOverrideRequest, TaskOverride, TriggerPeriodicTaskRequest,
};
use flexiq_server::grpc::status::reason;
use flexiq_server::grpc::Listener;
use flexiq_server::runtime::shutdown::Shutdown;
use flexiq_server::tokens::{Grants, ScopeSet};
use reqwest::StatusCode;
use serde_json::Value;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;
use tonic::{Code, Status};
use tonic_types::StatusExt;

use support::{mint_token, temp_storage, temp_workflows, Bearer, TempStorage};

const NAMESPACE: &str = "grpc-admin-grants-tests";

type Client = AdminServiceClient<InterceptedService<Channel, Bearer>>;

struct Harness {
    channel: Channel,
    base: String,
    storage: TempStorage,
    shutdown: Shutdown,
    served: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl Harness {
    async fn start(label: &str) -> Self {
        let storage = temp_storage(label);
        let shutdown = Shutdown::default();
        let listener = Listener::bind(&GrpcConfig::new(
            ListenAddress::Tcp("127.0.0.1:0".parse().expect("valid address")),
            NAMESPACE,
        ))
        .await
        .expect("bind");
        let addr = listener
            .local_addr()
            .expect("a TCP listener knows its port");
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
            channel,
            base: format!("http://{addr}"),
            storage,
            shutdown,
            served,
        }
    }

    /// A token carrying exactly `grants`.
    fn token(&self, grants: &[&str]) -> String {
        let grants = Grants::parse_all(grants.iter().copied()).expect("valid grants");
        mint_token(&self.storage, NAMESPACE, grants)
    }

    /// A client presenting a token carrying exactly `grants`.
    fn client(&self, grants: &[&str]) -> Client {
        AdminServiceClient::with_interceptor(self.channel.clone(), Bearer::new(&self.token(grants)))
    }

    /// A client whose token reaches everything, to seed state with.
    fn whole(&self) -> Client {
        let token = mint_token(&self.storage, NAMESPACE, ScopeSet::ALL);
        AdminServiceClient::with_interceptor(self.channel.clone(), Bearer::new(&token))
    }

    /// One JSON facade `GET`.
    async fn facade_get(&self, token: &str, path: &str) -> (StatusCode, Value) {
        let response = reqwest::Client::new()
            .get(format!("{}{path}", self.base))
            .bearer_auth(token)
            .send()
            .await
            .expect("the listener answers");
        let status = response.status();
        let text = response.text().await.expect("a body");
        (status, serde_json::from_str(&text).unwrap_or(Value::Null))
    }

    async fn stop(self) {
        self.shutdown.trigger();
        self.served
            .await
            .expect("the serve task must not panic")
            .expect("a shutdown is not an error");
    }
}

fn job_in(queue: &str, task: &str) -> NewJob {
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

/// Dead-letter one job of `task` on `queue`, returning the entry's id.
fn dead_letter(storage: &TempStorage, queue: &str, task: &str) -> String {
    let job = storage.enqueue(job_in(queue, task)).expect("enqueue");
    storage
        .dequeue(queue, now_millis() + 1_000, Some(NAMESPACE))
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

fn assert_not_found(status: &Status, reason: &str) {
    assert_eq!(status.code(), Code::NotFound, "{status:?}");
    assert_eq!(refusal(status).0, reason);
}

/// The refusal's reason and metadata, which is what a client branches on.
fn refusal(status: &Status) -> (String, std::collections::HashMap<String, String>) {
    let info = status
        .get_error_details()
        .error_info()
        .cloned()
        .expect("every refusal carries an ErrorInfo");
    (info.reason, info.metadata)
}

fn assert_beyond(status: &Status, scope: &str, queue: Option<&str>, task: Option<&str>) {
    assert_eq!(status.code(), Code::PermissionDenied, "{status:?}");
    let (reason, metadata) = refusal(status);
    assert_eq!(reason, reason::SCOPE_DENIED);
    assert_eq!(
        metadata.get(reason::KEY_SCOPE).map(String::as_str),
        Some(scope)
    );
    assert_eq!(metadata.get(reason::KEY_QUEUE).map(String::as_str), queue);
    assert_eq!(metadata.get(reason::KEY_TASK).map(String::as_str), task);
}

/// One team's operator: pauses and resumes its own queue, and nothing else.
#[tokio::test]
async fn a_queue_grant_pauses_its_queue_only() {
    let harness = Harness::start("admin-grants-pause").await;
    let mut client = harness.client(&["admin:queue=billing"]);

    let paused = client
        .pause_queue(PauseQueueRequest {
            queue: "billing".into(),
        })
        .await
        .expect("its own queue")
        .into_inner();
    assert!(paused.queue.expect("the queue").paused);
    client
        .resume_queue(ResumeQueueRequest {
            queue: "billing".into(),
        })
        .await
        .expect("its own queue");

    let status = client
        .pause_queue(PauseQueueRequest {
            queue: "emails".into(),
        })
        .await
        .expect_err("another queue");
    assert_beyond(&status, "admin", Some("emails"), None);
    assert!(harness
        .storage
        .list_paused_queues(Some(NAMESPACE))
        .expect("read")
        .is_empty());

    // A pause stops every task on the queue, so a grant of one task on it
    // does not reach it.
    let status = harness
        .client(&["admin:queue=billing,task=charge"])
        .pause_queue(PauseQueueRequest {
            queue: "billing".into(),
        })
        .await
        .expect_err("a task grant cannot pause the queue");
    assert_beyond(&status, "admin", Some("billing"), None);
    harness.stop().await;
}

/// A listing names what it lists: a narrowed token that names nothing would
/// otherwise learn every queue from the answer, or from how short it is.
#[tokio::test]
async fn a_queue_listing_must_name_a_queue_the_grants_reach() {
    let harness = Harness::start("admin-grants-list-queues").await;
    for queue in ["billing", "emails"] {
        harness.storage.enqueue(job_in(queue, "t")).expect("seed");
    }
    let mut client = harness.client(&["inspect:queue=billing"]);

    let status = client
        .list_queues(ListQueuesRequest::default())
        .await
        .expect_err("names no queue");
    assert_beyond(&status, "inspect", None, None);

    let listed = client
        .list_queues(ListQueuesRequest {
            queue: Some("billing".into()),
        })
        .await
        .expect("its own queue")
        .into_inner();
    let names: Vec<_> = listed.queues.iter().map(|q| q.name.as_str()).collect();
    assert_eq!(names, ["billing"]);
    assert_eq!(listed.queues[0].pending, 1);

    let status = client
        .list_queues(ListQueuesRequest {
            queue: Some("emails".into()),
        })
        .await
        .expect_err("another queue");
    assert_beyond(&status, "inspect", Some("emails"), None);

    let status = client
        .get_throughput(GetThroughputRequest::default())
        .await
        .expect_err("names no queue");
    assert_beyond(&status, "inspect", None, None);
    client
        .get_throughput(GetThroughputRequest {
            window: None,
            queue: Some("billing".into()),
        })
        .await
        .expect("its own queue");

    // The filter narrows a whole token's answer too.
    let listed = harness
        .whole()
        .list_queues(ListQueuesRequest {
            queue: Some("emails".into()),
        })
        .await
        .expect("whole")
        .into_inner();
    assert_eq!(listed.queues.len(), 1);
    assert_eq!(listed.queues[0].name, "emails");
    harness.stop().await;
}

/// A queue override is the queue's; a task override applies to the task on
/// every queue, so only a grant reaching that task everywhere may touch it.
#[tokio::test]
async fn overrides_are_checked_by_what_they_apply_to() {
    let harness = Harness::start("admin-grants-overrides").await;
    let mut queue_grant = harness.client(&["admin:queue=billing", "inspect:queue=billing"]);
    let mut task_grant = harness.client(&["admin:task=charge", "inspect:task=charge"]);

    queue_grant
        .set_queue_override(SetQueueOverrideRequest {
            queue: "billing".into(),
            queue_override: Some(QueueOverride {
                max_concurrent: Some(2),
                ..Default::default()
            }),
        })
        .await
        .expect("its own queue");
    let status = queue_grant
        .set_queue_override(SetQueueOverrideRequest {
            queue: "emails".into(),
            queue_override: Some(QueueOverride::default()),
        })
        .await
        .expect_err("another queue");
    assert_beyond(&status, "admin", Some("emails"), None);
    let status = queue_grant
        .set_task_override(SetTaskOverrideRequest {
            task_name: "charge".into(),
            task_override: Some(TaskOverride::default()),
        })
        .await
        .expect_err("a task override reaches every queue");
    assert_beyond(&status, "admin", None, Some("charge"));

    task_grant
        .set_task_override(SetTaskOverrideRequest {
            task_name: "charge".into(),
            task_override: Some(TaskOverride {
                max_retries: Some(7),
                ..Default::default()
            }),
        })
        .await
        .expect("its own task");
    let status = task_grant
        .clear_queue_override(ClearQueueOverrideRequest {
            queue: "billing".into(),
        })
        .await
        .expect_err("a queue override covers every task");
    assert_beyond(&status, "admin", Some("billing"), None);

    // Listing: each names its own key and sees only it.
    let status = queue_grant
        .list_overrides(ListOverridesRequest::default())
        .await
        .expect_err("names nothing");
    assert_beyond(&status, "inspect", None, None);
    let listed = queue_grant
        .list_overrides(ListOverridesRequest {
            queue: Some("billing".into()),
            task_name: None,
        })
        .await
        .expect("its own queue")
        .into_inner();
    assert_eq!(listed.queues.keys().collect::<Vec<_>>(), ["billing"]);
    assert!(listed.tasks.is_empty(), "no task was named");
    let listed = task_grant
        .list_overrides(ListOverridesRequest {
            queue: None,
            task_name: Some("charge".into()),
        })
        .await
        .expect("its own task")
        .into_inner();
    assert_eq!(listed.tasks.keys().collect::<Vec<_>>(), ["charge"]);
    assert!(listed.queues.is_empty(), "no queue was named");
    let status = task_grant
        .list_overrides(ListOverridesRequest {
            queue: Some("billing".into()),
            task_name: Some("charge".into()),
        })
        .await
        .expect_err("both named, one beyond");
    assert_beyond(&status, "inspect", Some("billing"), None);

    task_grant
        .clear_task_override(ClearTaskOverrideRequest {
            task_name: "charge".into(),
        })
        .await
        .expect("its own task");
    harness.stop().await;
}

/// The issue's own example: `admin:queue=billing` finds and replays its dead
/// letters, and every other entry reads as absent.
#[tokio::test]
async fn a_queue_grant_works_its_own_dead_letters_only() {
    let harness = Harness::start("admin-grants-dlq").await;
    let ours = dead_letter(&harness.storage, "billing", "charge");
    let theirs = dead_letter(&harness.storage, "emails", "send");
    let mut client = harness.client(&["admin:queue=billing", "inspect:queue=billing"]);

    let status = client
        .list_dead_letters(ListDeadLettersRequest::default())
        .await
        .expect_err("names no queue");
    assert_beyond(&status, "inspect", None, None);
    let status = client
        .list_dead_letters(ListDeadLettersRequest {
            queue: Some("emails".into()),
            ..Default::default()
        })
        .await
        .expect_err("another queue");
    assert_beyond(&status, "inspect", Some("emails"), None);
    let listed = client
        .list_dead_letters(ListDeadLettersRequest {
            queue: Some("billing".into()),
            ..Default::default()
        })
        .await
        .expect("its own queue")
        .into_inner();
    let ids: Vec<_> = listed.dead_letters.iter().map(|d| d.id.as_str()).collect();
    assert_eq!(ids, [ours.as_str()]);

    client
        .get_dead_letter(GetDeadLetterRequest {
            dead_letter_id: ours.clone(),
            include_payload: false,
        })
        .await
        .expect("its own entry");
    let status = client
        .get_dead_letter(GetDeadLetterRequest {
            dead_letter_id: theirs.clone(),
            include_payload: false,
        })
        .await
        .expect_err("another queue's entry");
    assert_not_found(&status, reason::DEAD_LETTER_NOT_FOUND);
    let status = client
        .replay_dead_letter(ReplayDeadLetterRequest {
            dead_letter_id: theirs.clone(),
        })
        .await
        .expect_err("another queue's entry");
    assert_not_found(&status, reason::DEAD_LETTER_NOT_FOUND);
    let status = client
        .delete_dead_letter(DeleteDeadLetterRequest {
            dead_letter_id: theirs.clone(),
        })
        .await
        .expect_err("another queue's entry");
    assert_not_found(&status, reason::DEAD_LETTER_NOT_FOUND);
    assert!(
        harness
            .storage
            .get_dead(&theirs, Some(NAMESPACE))
            .expect("read")
            .is_some(),
        "a refused replay or delete must leave the entry"
    );

    let replayed = client
        .replay_dead_letter(ReplayDeadLetterRequest {
            dead_letter_id: ours,
        })
        .await
        .expect("its own entry")
        .into_inner();
    assert_eq!(replayed.job.expect("the job").queue, "billing");

    // A purge by task reaches the task on every queue; the other arms reach
    // the whole namespace.
    let status = client
        .purge_dead_letters(PurgeDeadLettersRequest {
            filter: Some(purge_dead_letters_request::Filter::TaskName("send".into())),
        })
        .await
        .expect_err("a task on every queue");
    assert_beyond(&status, "admin", None, Some("send"));
    let status = client
        .purge_dead_letters(PurgeDeadLettersRequest { filter: None })
        .await
        .expect_err("every entry");
    assert_beyond(&status, "admin", None, None);
    let purged = harness
        .client(&["admin:task=send"])
        .purge_dead_letters(PurgeDeadLettersRequest {
            filter: Some(purge_dead_letters_request::Filter::TaskName("send".into())),
        })
        .await
        .expect("its own task")
        .into_inner();
    assert_eq!(purged.purged, 1);
    harness.stop().await;
}

/// A token narrowed to one task on one queue must name both.
#[tokio::test]
async fn a_dead_letter_listing_names_every_qualifier_the_grant_has() {
    let harness = Harness::start("admin-grants-dlq-both").await;
    let ours = dead_letter(&harness.storage, "billing", "charge");
    dead_letter(&harness.storage, "billing", "refund");
    let mut client = harness.client(&["inspect:queue=billing,task=charge"]);

    let status = client
        .list_dead_letters(ListDeadLettersRequest {
            queue: Some("billing".into()),
            ..Default::default()
        })
        .await
        .expect_err("the queue's other tasks are beyond it");
    assert_beyond(&status, "inspect", Some("billing"), None);
    let listed = client
        .list_dead_letters(ListDeadLettersRequest {
            queue: Some("billing".into()),
            task_name: Some("charge".into()),
            ..Default::default()
        })
        .await
        .expect("both named")
        .into_inner();
    let ids: Vec<_> = listed.dead_letters.iter().map(|d| d.id.as_str()).collect();
    assert_eq!(ids, [ours.as_str()]);
    harness.stop().await;
}

fn schedule(name: &str, queue: &str, task: &str) -> PutPeriodicTaskRequest {
    PutPeriodicTaskRequest {
        name: name.into(),
        task_name: task.into(),
        cron: "0 0 3 * * *".into(),
        queue: queue.into(),
        ..Default::default()
    }
}

/// A periodic task is reached through the queue and task it fires; one outside
/// the grants reads as absent, and its name cannot be taken over by a put.
#[tokio::test]
async fn a_queue_grant_manages_its_own_schedules_only() {
    let harness = Harness::start("admin-grants-periodic").await;
    let mut whole = harness.whole();
    for (name, queue, task) in [
        ("nightly-billing", "billing", "charge"),
        ("nightly-emails", "emails", "send"),
    ] {
        whole
            .put_periodic_task(schedule(name, queue, task))
            .await
            .expect("seed");
    }
    let mut client = harness.client(&["admin:queue=billing", "inspect:queue=billing"]);

    let status = client
        .list_periodic_tasks(ListPeriodicTasksRequest::default())
        .await
        .expect_err("names no queue");
    assert_beyond(&status, "inspect", None, None);
    let listed = client
        .list_periodic_tasks(ListPeriodicTasksRequest {
            queue: Some("billing".into()),
            task_name: None,
        })
        .await
        .expect("its own queue")
        .into_inner();
    let names: Vec<_> = listed
        .periodic_tasks
        .iter()
        .map(|t| t.name.as_str())
        .collect();
    assert_eq!(names, ["nightly-billing"]);

    // Every call keyed by name answers a hidden task as absent.
    let theirs = "nightly-emails".to_string();
    let hidden = [
        client
            .get_periodic_task(GetPeriodicTaskRequest {
                name: theirs.clone(),
                include_payload: false,
            })
            .await
            .map(drop),
        client
            .pause_periodic_task(PausePeriodicTaskRequest {
                name: theirs.clone(),
            })
            .await
            .map(drop),
        client
            .resume_periodic_task(ResumePeriodicTaskRequest {
                name: theirs.clone(),
            })
            .await
            .map(drop),
        client
            .trigger_periodic_task(TriggerPeriodicTaskRequest {
                name: theirs.clone(),
            })
            .await
            .map(drop),
        client
            .delete_periodic_task(DeletePeriodicTaskRequest {
                name: theirs.clone(),
            })
            .await
            .map(drop),
    ];
    for answer in hidden {
        assert_not_found(
            &answer.expect_err("another queue's task"),
            reason::PERIODIC_TASK_NOT_FOUND,
        );
    }

    // Taking the hidden task's name would rewrite it: refused, naming nothing
    // about it, and the stored task is untouched.
    let status = client
        .put_periodic_task(schedule(&theirs, "billing", "charge"))
        .await
        .expect_err("the name is another queue's");
    assert_beyond(&status, "admin", None, None);
    let stored = whole
        .get_periodic_task(GetPeriodicTaskRequest {
            name: theirs,
            include_payload: false,
        })
        .await
        .expect("still there")
        .into_inner()
        .periodic_task
        .expect("the task");
    assert_eq!(stored.queue, "emails");
    assert!(stored.enabled, "a refused pause must not land");

    let status = client
        .put_periodic_task(schedule("new", "emails", "send"))
        .await
        .expect_err("fires into another queue");
    assert_beyond(&status, "admin", Some("emails"), Some("send"));
    // An unnamed queue is `default`, checked like any other.
    let status = client
        .put_periodic_task(schedule("new", "", "charge"))
        .await
        .expect_err("the default queue");
    assert_beyond(&status, "admin", Some("default"), Some("charge"));

    client
        .put_periodic_task(schedule("hourly-billing", "billing", "charge"))
        .await
        .expect("its own queue");
    client
        .pause_periodic_task(PausePeriodicTaskRequest {
            name: "nightly-billing".into(),
        })
        .await
        .expect("its own task");
    let job = client
        .trigger_periodic_task(TriggerPeriodicTaskRequest {
            name: "nightly-billing".into(),
        })
        .await
        .expect("its own task")
        .into_inner()
        .job
        .expect("a job");
    assert_eq!(job.queue, "billing");
    client
        .delete_periodic_task(DeletePeriodicTaskRequest {
            name: "nightly-billing".into(),
        })
        .await
        .expect("its own task");
    harness.stop().await;
}

/// Workers serve many queues and the quota is the namespace's: neither has a
/// narrowed form, so a narrowed token is refused rather than served.
#[tokio::test]
async fn an_rpc_that_checks_no_queue_refuses_a_narrowed_token() {
    let harness = Harness::start("admin-grants-closed").await;
    let mut client = harness.client(&["admin:queue=billing", "inspect:queue=billing"]);

    let status = client
        .list_workers(ListWorkersRequest {})
        .await
        .expect_err("workers span queues");
    assert_beyond(&status, "inspect", None, None);
    let status = client
        .drain_worker(DrainWorkerRequest {
            worker_id: "w".into(),
        })
        .await
        .expect_err("workers span queues");
    assert_beyond(&status, "admin", None, None);
    let status = client
        .get_namespace_quota(GetNamespaceQuotaRequest {})
        .await
        .expect_err("the quota is the namespace's");
    assert_beyond(&status, "inspect", None, None);
    harness.stop().await;
}

/// The JSON facade calls the same handlers, so it gives the same answers.
#[tokio::test]
async fn the_json_facade_gives_the_same_answers() {
    let harness = Harness::start("admin-grants-facade").await;
    harness
        .storage
        .enqueue(job_in("billing", "t"))
        .expect("seed");
    let token = harness.token(&["inspect:queue=billing"]);

    let (status, body) = harness.facade_get(&token, "/v1/admin/queues").await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert_eq!(body["error"]["details"][0]["reason"], "SCOPE_DENIED");

    let (status, body) = harness
        .facade_get(&token, "/v1/admin/queues?queue=billing")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["queues"][0]["name"], "billing");

    let (status, body) = harness
        .facade_get(&token, "/v1/admin/queues?queue=emails")
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert_eq!(body["error"]["details"][0]["metadata"]["queue"], "emails");
    harness.stop().await;
}
