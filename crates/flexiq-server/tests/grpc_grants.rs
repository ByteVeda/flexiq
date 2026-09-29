//! End-to-end: a token narrowed to some queues and tasks, over a real socket
//! (#839).
//!
//! The unit tests pin the grammar and the union rule. What only a running door
//! can show is that every producer RPC honours a narrowed grant, that the JSON
//! facade gives the same answers, and that the RPCs never taught to check a
//! queue refuse a narrowed token rather than serve it everything.
#![cfg(feature = "grpc")]

mod support;

use flexiq_core::storage::Storage;
use flexiq_server::config::grpc::GrpcConfig;
use flexiq_server::config::listen::ListenAddress;
use flexiq_server::grpc::pb::producer_service_client::ProducerServiceClient;
use flexiq_server::grpc::pb::{
    enqueue_batch_item_result, enqueue_request, watch_jobs_request, watch_jobs_response,
    CancelJobRequest, Debounce, EnqueueBatchRequest, EnqueueOptions, EnqueueRequest, GetJobRequest,
    GetWorkflowRunRequest, ListJobsRequest, QueueStatsRequest, SubmitWorkflowRequest, WatchJobIds,
    WatchJobsRequest,
};
use flexiq_server::grpc::status::reason;
use flexiq_server::grpc::Listener;
use flexiq_server::runtime::shutdown::Shutdown;
use flexiq_server::tokens::{store, Grants, ScopeSet};
use reqwest::StatusCode;
use serde_json::{json, Value};
use tonic::transport::Channel;
use tonic::{Code, Status};
use tonic_types::StatusExt;

use support::{mint_token, temp_storage, temp_workflows, Bearer, TempStorage};

const NAMESPACE: &str = "grpc-grants-tests";

/// How long a watch may take to answer before the test fails.
const WAIT: std::time::Duration = std::time::Duration::from_secs(5);

type Client =
    ProducerServiceClient<tonic::service::interceptor::InterceptedService<Channel, Bearer>>;

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
        self.client_for(&self.token(grants))
    }

    fn client_for(&self, token: &str) -> Client {
        ProducerServiceClient::with_interceptor(self.channel.clone(), Bearer::new(token))
    }

    /// A client whose token reaches everything, to seed jobs with.
    fn whole(&self) -> Client {
        self.client_for(&mint_token(&self.storage, NAMESPACE, ScopeSet::ALL))
    }

    /// One JSON facade call.
    async fn facade(
        &self,
        token: &str,
        method: reqwest::Method,
        path: &str,
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let mut request = reqwest::Client::new()
            .request(method, format!("{}{path}", self.base))
            .bearer_auth(token);
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request.send().await.expect("the listener answers");
        let status = response.status();
        let text = response.text().await.expect("a body");
        let body = serde_json::from_str(&text).unwrap_or(Value::Null);
        (status, body)
    }

    async fn stop(self) {
        self.shutdown.trigger();
        self.served
            .await
            .expect("the serve task must not panic")
            .expect("a shutdown is not an error");
    }
}

fn job(queue: &str, task: &str) -> EnqueueRequest {
    EnqueueRequest {
        task_name: task.into(),
        body: Some(enqueue_request::Body::Raw(vec![1])),
        options: Some(EnqueueOptions {
            queue: queue.into(),
            ..Default::default()
        }),
    }
}

async fn enqueue(client: &mut Client, queue: &str, task: &str) -> String {
    client
        .enqueue(job(queue, task))
        .await
        .expect("enqueue")
        .into_inner()
        .job
        .expect("a job")
        .id
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

fn assert_not_found(status: &Status) {
    assert_eq!(status.code(), Code::NotFound, "{status:?}");
    assert_eq!(refusal(status).0, reason::JOB_NOT_FOUND);
}

/// A dashboard, an alerting job, a support tool: sees jobs, submits and
/// cancels none.
#[tokio::test]
async fn a_read_token_reads_and_writes_nothing() {
    let harness = Harness::start("grants-read").await;
    let id = enqueue(&mut harness.whole(), "emails", "send_receipt").await;
    let mut reader = harness.client(&["read"]);

    reader
        .get_job(GetJobRequest {
            job_id: id.clone(),
            ..Default::default()
        })
        .await
        .expect("read reaches GetJob");
    let page = reader
        .list_jobs(ListJobsRequest::default())
        .await
        .expect("read reaches ListJobs")
        .into_inner();
    assert_eq!(page.jobs.len(), 1);
    reader
        .queue_stats(QueueStatsRequest::default())
        .await
        .expect("read reaches QueueStats");

    let status = reader
        .enqueue(job("emails", "send_receipt"))
        .await
        .expect_err("read must not enqueue");
    assert_eq!(status.code(), Code::PermissionDenied);
    assert_eq!(
        refusal(&status)
            .1
            .get(reason::KEY_SCOPE)
            .map(String::as_str),
        Some("produce")
    );
    let status = reader
        .cancel_job(CancelJobRequest { job_id: id.clone() })
        .await
        .expect_err("read must not cancel");
    assert_eq!(status.code(), Code::PermissionDenied);

    let pending = harness
        .storage
        .get_job(&id, Some(NAMESPACE))
        .expect("read")
        .expect("present");
    assert_eq!(pending.status, flexiq_core::JobStatus::Pending);
    harness.stop().await;
}

#[tokio::test]
async fn a_queue_grant_enqueues_to_its_queue_only() {
    let harness = Harness::start("grants-queue").await;
    let mut emails = harness.client(&["produce:queue=emails-*"]);

    enqueue(&mut emails, "emails-eu", "send_receipt").await;
    let status = emails
        .enqueue(job("billing", "charge"))
        .await
        .expect_err("billing is not granted");
    assert_beyond(&status, "produce", Some("billing"), Some("charge"));

    // An empty queue is `default` by the time it is checked, and `default` is
    // not a queue this grant names.
    let status = emails
        .enqueue(job("", "send_receipt"))
        .await
        .expect_err("the default queue is not granted");
    assert_beyond(&status, "produce", Some("default"), Some("send_receipt"));
    harness.stop().await;
}

#[tokio::test]
async fn a_task_grant_enqueues_its_task_only() {
    let harness = Harness::start("grants-task").await;
    let mut edge = harness.client(&["produce:queue=emails,task=send_receipt"]);

    enqueue(&mut edge, "emails", "send_receipt").await;
    let status = edge
        .enqueue(job("emails", "delete_account"))
        .await
        .expect_err("another task is not granted");
    assert_beyond(&status, "produce", Some("emails"), Some("delete_account"));
    harness.stop().await;
}

/// One refused item refuses the batch before anything is written.
#[tokio::test]
async fn a_batch_with_one_ungranted_item_writes_nothing() {
    let harness = Harness::start("grants-batch").await;
    let mut emails = harness.client(&["produce:queue=emails"]);

    let status = emails
        .enqueue_batch(EnqueueBatchRequest {
            items: vec![job("emails", "a"), job("billing", "b")],
        })
        .await
        .expect_err("the batch names an ungranted queue");
    assert_beyond(&status, "produce", Some("billing"), Some("b"));
    assert_eq!(
        refusal(&status)
            .1
            .get(reason::KEY_INDEX)
            .map(String::as_str),
        Some("1")
    );
    let written = harness
        .storage
        .list_jobs_after(None, None, None, 10, None, Some(NAMESPACE))
        .expect("list");
    assert!(written.is_empty(), "nothing may land: {written:?}");
    harness.stop().await;
}

/// Not "you may not see it": that answer would confirm the id exists.
#[tokio::test]
async fn a_job_outside_the_grants_reads_as_missing_and_is_not_cancelled() {
    let harness = Harness::start("grants-hidden").await;
    let billing = enqueue(&mut harness.whole(), "billing", "charge").await;
    let mut emails = harness.client(&["produce:queue=emails"]);
    let own = enqueue(&mut emails, "emails", "send_receipt").await;

    let status = emails
        .get_job(GetJobRequest {
            job_id: billing.clone(),
            ..Default::default()
        })
        .await
        .expect_err("hidden");
    assert_not_found(&status);
    let status = emails
        .cancel_job(CancelJobRequest {
            job_id: billing.clone(),
        })
        .await
        .expect_err("hidden");
    assert_not_found(&status);
    let untouched = harness
        .storage
        .get_job(&billing, Some(NAMESPACE))
        .expect("read")
        .expect("present");
    assert_eq!(untouched.status, flexiq_core::JobStatus::Pending);

    let cancelled = emails
        .cancel_job(CancelJobRequest { job_id: own })
        .await
        .expect("its own job cancels")
        .into_inner();
    assert_eq!(
        cancelled.job.expect("a job").status,
        flexiq_server::grpc::pb::JobStatus::Cancelled as i32
    );
    harness.stop().await;
}

/// A narrowed caller names what it lists; a page is never filtered after the
/// scan.
#[tokio::test]
async fn a_listing_must_name_a_queue_the_grants_reach() {
    let harness = Harness::start("grants-list").await;
    let mut whole = harness.whole();
    enqueue(&mut whole, "emails", "send_receipt").await;
    enqueue(&mut whole, "billing", "charge").await;
    let mut emails = harness.client(&["read:queue=emails"]);

    let status = emails
        .list_jobs(ListJobsRequest::default())
        .await
        .expect_err("every queue is not granted");
    assert_beyond(&status, "read", None, None);
    let status = emails
        .list_jobs(ListJobsRequest {
            queue: Some("billing".into()),
            ..Default::default()
        })
        .await
        .expect_err("billing is not granted");
    assert_beyond(&status, "read", Some("billing"), None);
    let page = emails
        .list_jobs(ListJobsRequest {
            queue: Some("emails".into()),
            ..Default::default()
        })
        .await
        .expect("its own queue lists")
        .into_inner();
    assert_eq!(page.jobs.len(), 1);
    assert_eq!(page.jobs[0].queue, "emails");

    emails
        .queue_stats(QueueStatsRequest {
            queue: Some("emails".into()),
        })
        .await
        .expect("its own queue counts");
    let status = emails
        .queue_stats(QueueStatsRequest { queue: None })
        .await
        .expect_err("the namespace total spans ungranted queues");
    assert_beyond(&status, "read", None, None);

    // Queue counts span every task, so a task grant cannot read them.
    let mut receipts = harness.client(&["read:queue=emails,task=send_receipt"]);
    receipts
        .queue_stats(QueueStatsRequest {
            queue: Some("emails".into()),
        })
        .await
        .expect_err("the counts include other tasks");
    receipts
        .list_jobs(ListJobsRequest {
            queue: Some("emails".into()),
            task_name: Some("send_receipt".into()),
            ..Default::default()
        })
        .await
        .expect("a listing naming its task");
    harness.stop().await;
}

/// A `unique_key` is matched across the namespace, so a deduplicated answer
/// could be another queue's job; a narrowed caller must not be handed it.
#[tokio::test]
async fn a_deduplicated_answer_outside_the_grants_is_refused() {
    let harness = Harness::start("grants-dedup").await;
    let mut seeded = job("billing", "charge");
    seeded.options.as_mut().expect("options").unique_key = Some("k".into());
    harness.whole().enqueue(seeded).await.expect("seed");

    let mut emails = harness.client(&["produce:queue=emails"]);
    let mut probe = job("emails", "refund");
    probe.options.as_mut().expect("options").unique_key = Some("k".into());
    let status = emails
        .enqueue(probe.clone())
        .await
        .expect_err("the answer would be billing's job");
    // The refusal names nothing about the job it hides: naming its queue or
    // task would be the same read in a smaller form.
    assert_beyond(&status, "produce", None, None);
    assert!(
        !status.message().contains("billing") && !status.message().contains("charge"),
        "{}",
        status.message()
    );

    let outcome = emails
        .enqueue_batch(EnqueueBatchRequest { items: vec![probe] })
        .await
        .expect("a batch reports per item")
        .into_inner();
    let Some(enqueue_batch_item_result::Outcome::Error(error)) = &outcome.results[0].outcome else {
        panic!("the item must be refused: {outcome:?}");
    };
    let rendered = format!("{error:?}");
    assert!(
        !rendered.contains("billing") && !rendered.contains("charge"),
        "{rendered}"
    );
    harness.stop().await;
}

/// A debounce key is matched across the namespace too, and a coalescing call
/// slides the job it finds; a narrowed caller may not do that blind.
#[tokio::test]
async fn a_narrowed_caller_cannot_debounce() {
    let harness = Harness::start("grants-debounce").await;
    let mut emails = harness.client(&["produce:queue=emails"]);
    let mut debounced = job("emails", "send_receipt");
    debounced.options.as_mut().expect("options").debounce = Some(Debounce {
        key: "k".into(),
        window: Some(prost_types::Duration {
            seconds: 5,
            nanos: 0,
        }),
        max_wait: Some(prost_types::Duration {
            seconds: 30,
            nanos: 0,
        }),
        ..Default::default()
    });
    let status = emails.enqueue(debounced).await.expect_err("refused");
    assert_beyond(&status, "produce", None, None);
    harness.stop().await;
}

/// The RPCs no one has taught to check a queue refuse a narrowed token, rather
/// than serve it the whole namespace.
#[tokio::test]
async fn an_rpc_that_checks_no_queue_refuses_a_narrowed_token() {
    let harness = Harness::start("grants-closed").await;
    let mut emails = harness.client(&["produce:queue=emails", "read:queue=emails"]);

    let status = emails
        .submit_workflow(SubmitWorkflowRequest::default())
        .await
        .expect_err("workflows check no queue");
    assert_beyond(&status, "produce", None, None);
    let status = emails
        .get_workflow_run(GetWorkflowRunRequest::default())
        .await
        .expect_err("workflows check no queue");
    assert_beyond(&status, "read", None, None);
    harness.stop().await;
}

#[tokio::test]
async fn a_watch_reaches_only_granted_queues_and_jobs() {
    let harness = Harness::start("grants-watch").await;
    let mut whole = harness.whole();
    let billing = enqueue(&mut whole, "billing", "charge").await;
    let own = enqueue(&mut whole, "emails", "send_receipt").await;
    let mut emails = harness.client(&["read:queue=emails"]);

    let status = emails
        .watch_jobs(WatchJobsRequest {
            target: Some(watch_jobs_request::Target::Queue("billing".into())),
            resume_cursor: String::new(),
        })
        .await
        .expect_err("billing is not granted");
    assert_beyond(&status, "read", Some("billing"), None);
    emails
        .watch_jobs(WatchJobsRequest {
            target: Some(watch_jobs_request::Target::Queue("emails".into())),
            resume_cursor: String::new(),
        })
        .await
        .expect("its own queue watches");

    let mut stream = emails
        .watch_jobs(WatchJobsRequest {
            target: Some(watch_jobs_request::Target::JobIds(WatchJobIds {
                job_ids: vec![billing.clone(), own.clone()],
            })),
            resume_cursor: String::new(),
        })
        .await
        .expect("an id watch opens")
        .into_inner();
    let first = tokio::time::timeout(WAIT, stream.message())
        .await
        .expect("in time")
        .expect("no error")
        .expect("an item");
    assert_eq!(
        first.item,
        Some(watch_jobs_response::Item::NotFoundJobId(billing)),
        "a job outside the grants reads as missing"
    );
    let second = tokio::time::timeout(WAIT, stream.message())
        .await
        .expect("in time")
        .expect("no error")
        .expect("an item");
    match second.item {
        Some(watch_jobs_response::Item::Transition(t)) => assert_eq!(t.job_id, own),
        other => panic!("expected its own job's snapshot, got {other:?}"),
    }
    harness.stop().await;
}

/// The facade calls the same handlers, so it gives the same answers.
#[tokio::test]
async fn the_json_facade_gives_the_same_answers() {
    let harness = Harness::start("grants-facade").await;
    let billing = enqueue(&mut harness.whole(), "billing", "charge").await;
    let token = harness.token(&["produce:queue=emails"]);

    let (status, body) = harness
        .facade(
            &token,
            reqwest::Method::POST,
            "/v1/jobs",
            Some(json!({"taskName": "charge", "raw": "", "options": {"queue": "billing"}})),
        )
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert_eq!(body["error"]["details"][0]["reason"], "SCOPE_DENIED");
    assert_eq!(body["error"]["details"][0]["metadata"]["queue"], "billing");

    let (status, _) = harness
        .facade(
            &token,
            reqwest::Method::POST,
            "/v1/jobs",
            Some(json!({"taskName": "send_receipt", "raw": "", "options": {"queue": "emails"}})),
        )
        .await;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = harness
        .facade(
            &token,
            reqwest::Method::GET,
            &format!("/v1/jobs/{billing}"),
            None,
        )
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    harness.stop().await;
}

/// A row stored before grants existed — a bare array of scope names, written
/// straight into the settings store — keeps every queue it always reached.
#[tokio::test]
async fn a_token_minted_before_grants_keeps_its_grants() {
    let harness = Harness::start("grants-legacy").await;
    let request = flexiq_server::tokens::NewToken::new("old", ScopeSet::ALL, NAMESPACE, None, None)
        .expect("valid");
    let (row, token) = store::create(&*harness.storage, request).expect("mint");
    let mut stored: Value = serde_json::to_value(&row).expect("encode");
    stored["scopes"] = json!(["produce"]);
    harness
        .storage
        .set_setting(
            &format!("{}{}", store::KEY_PREFIX, row.id),
            &stored.to_string(),
        )
        .expect("rewrite as an old row");

    let mut old = harness.client_for(&token);
    enqueue(&mut old, "billing", "charge").await;
    enqueue(&mut old, "", "anything").await;
    old.list_jobs(ListJobsRequest::default())
        .await
        .expect("produce still reaches the reads");
    let outcome = old
        .enqueue_batch(EnqueueBatchRequest {
            items: vec![job("emails", "a")],
        })
        .await
        .expect("batch")
        .into_inner();
    assert!(matches!(
        outcome.results[0].outcome,
        Some(enqueue_batch_item_result::Outcome::Enqueued(_))
    ));
    harness.stop().await;
}
