//! End-to-end: the audit trail over a real socket (#840).
//!
//! The unit tests pin which paths are audited and how a slot becomes records.
//! What only a running door can show is the whole chain — layer, auth, handler,
//! sink, table — for the gRPC door and the JSON facade alike, and that a read,
//! an unbelieved credential, and the token's secret leave nothing behind.
#![cfg(feature = "grpc")]

mod support;

use std::time::Duration;

use flexiq_core::storage::Storage;
use flexiq_core::{AuditFilter, AuditRecord};
use flexiq_server::config::grpc::GrpcConfig;
use flexiq_server::config::listen::ListenAddress;
use flexiq_server::grpc::pb::admin::admin_service_client::AdminServiceClient;
use flexiq_server::grpc::pb::admin::{ListAuditRecordsRequest, PauseQueueRequest};
use flexiq_server::grpc::pb::producer_service_client::ProducerServiceClient;
use flexiq_server::grpc::pb::{
    enqueue_batch_item_result, enqueue_request, CancelJobRequest, EnqueueBatchRequest,
    EnqueueOptions, EnqueueRequest, GetJobRequest,
};
use flexiq_server::grpc::Listener;
use flexiq_server::runtime::shutdown::Shutdown;
use flexiq_server::tokens::{store, NewToken, Scope, ScopeSet};
use serde_json::json;
use tonic::transport::Channel;
use tonic::Code;

use support::{temp_storage, temp_workflows, Bearer, TempStorage};

const NAMESPACE: &str = "grpc-audit-tests";

/// How long the writer may take to append before the test fails.
const WAIT: Duration = Duration::from_secs(5);

struct Harness {
    channel: Channel,
    base: String,
    storage: TempStorage,
    shutdown: Shutdown,
    served: tokio::task::JoinHandle<anyhow::Result<()>>,
}

/// A minted token: what the caller presents, and what the trail should name.
struct Minted {
    plaintext: String,
    id: String,
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

    fn mint(&self, name: &str, scopes: ScopeSet) -> Minted {
        let request = NewToken::new(name, scopes, NAMESPACE, None, None).expect("valid mint");
        let (row, plaintext) = store::create(&*self.storage, request).expect("mint");
        Minted {
            plaintext,
            id: row.id,
        }
    }

    fn producer(
        &self,
        token: &Minted,
    ) -> ProducerServiceClient<tonic::service::interceptor::InterceptedService<Channel, Bearer>>
    {
        ProducerServiceClient::with_interceptor(self.channel.clone(), Bearer::new(&token.plaintext))
    }

    /// Wait for the trail to hold `count` records, and answer them oldest first.
    async fn trail(&self, count: usize) -> Vec<AuditRecord> {
        let deadline = tokio::time::Instant::now() + WAIT;
        loop {
            let mut records = self
                .storage
                .list_audit_after(NAMESPACE, &AuditFilter::default(), 1_000, None)
                .expect("list the trail");
            if records.len() >= count {
                records.reverse();
                return records;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "the trail held {} record(s), wanted {count}: {records:?}",
                records.len()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
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

fn produce() -> ScopeSet {
    ScopeSet::of(&[Scope::Produce])
}

/// No field of any record carries the token's secret, nor the token whole.
fn assert_no_secret(records: &[AuditRecord], token: &Minted) {
    let (_, secret) = token
        .plaintext
        .rsplit_once('.')
        .expect("a token is fqt_<id>.<secret>");
    let dump = format!("{records:?}");
    assert!(
        !dump.contains(secret),
        "a record carried the secret: {dump}"
    );
    assert!(
        !dump.contains(&token.plaintext),
        "a record carried the token: {dump}"
    );
}

/// #992: the job itself names the token that submitted it — each caller its
/// own, on a single enqueue and on every item of a batch — so "who sent this"
/// needs no trip to the trail and outlives its retention.
#[tokio::test]
async fn a_job_names_the_token_that_submitted_it() {
    let harness = Harness::start("audit-enqueued-by").await;
    let alice = harness.mint("alice", produce());
    let bob = harness.mint("bob", produce());

    let single = harness
        .producer(&alice)
        .enqueue(job("emails", "send_receipt"))
        .await
        .expect("enqueue")
        .into_inner()
        .job
        .expect("a job")
        .id;
    let batch: Vec<String> = harness
        .producer(&bob)
        .enqueue_batch(EnqueueBatchRequest {
            items: vec![job("emails", "a"), job("emails", "b")],
        })
        .await
        .expect("enqueue_batch")
        .into_inner()
        .results
        .into_iter()
        .map(|result| match result.outcome {
            Some(enqueue_batch_item_result::Outcome::Enqueued(enqueued)) => {
                enqueued.job.expect("a job").id
            }
            other => panic!("every item lands: {other:?}"),
        })
        .collect();
    assert_eq!(batch.len(), 2);

    let submitter = |id: &str| {
        harness
            .storage
            .get_job(id, Some(NAMESPACE))
            .expect("read")
            .expect("the job exists")
            .enqueued_by
    };
    assert_eq!(submitter(&single), Some(alice.id.clone()));
    for id in &batch {
        assert_eq!(submitter(id), Some(bob.id.clone()));
    }
    // The public id, never the credential presented.
    assert_ne!(
        submitter(&single).as_deref(),
        Some(alice.plaintext.as_str())
    );

    harness.stop().await;
}

#[tokio::test]
async fn an_enqueue_names_the_token_its_name_and_the_job() {
    let harness = Harness::start("audit-enqueue").await;
    let token = harness.mint("billing-api", produce());
    let job_id = harness
        .producer(&token)
        .enqueue(job("emails", "send_receipt"))
        .await
        .expect("enqueue")
        .into_inner()
        .job
        .expect("a job")
        .id;

    let records = harness.trail(1).await;
    assert_eq!(records.len(), 1, "{records:?}");
    let record = &records[0];
    assert_eq!(record.token_id, token.id);
    assert_eq!(record.principal, "billing-api");
    assert_eq!(record.namespace, NAMESPACE);
    assert_eq!(record.operation, "flexiq.v1.ProducerService/Enqueue");
    assert_eq!(record.target_kind.as_deref(), Some("job"));
    assert_eq!(record.target.as_deref(), Some(job_id.as_str()));
    assert_eq!(record.outcome, "OK");
    assert_no_secret(&records, &token);
    harness.stop().await;
}

#[tokio::test]
async fn a_refused_write_is_recorded_against_the_token() {
    let harness = Harness::start("audit-refused").await;
    let reader = harness.mint("dashboard", ScopeSet::of(&[Scope::Read]));
    let status = harness
        .producer(&reader)
        .enqueue(job("emails", "send_receipt"))
        .await
        .expect_err("a read token must not enqueue");
    assert_eq!(status.code(), Code::PermissionDenied);

    let records = harness.trail(1).await;
    assert_eq!(records[0].token_id, reader.id);
    assert_eq!(records[0].outcome, "PERMISSION_DENIED");
    assert_eq!(records[0].target, None, "refused before any target");
    harness.stop().await;
}

#[tokio::test]
async fn reads_and_unbelieved_credentials_leave_nothing() {
    let harness = Harness::start("audit-reads").await;
    let token = harness.mint("ci", produce());
    let mut client = harness.producer(&token);

    let status = client
        .get_job(GetJobRequest {
            job_id: "no-such-job".into(),
            ..Default::default()
        })
        .await
        .expect_err("absent");
    assert_eq!(status.code(), Code::NotFound);
    let (public, _) = token.plaintext.rsplit_once('.').expect("fqt_<id>.<secret>");
    let forged = Minted {
        plaintext: format!("{public}.not-the-secret"),
        id: token.id.clone(),
    };
    harness
        .producer(&forged)
        .enqueue(job("emails", "send_receipt"))
        .await
        .expect_err("a wrong secret is refused");

    // The writer is FIFO: once this enqueue's record is in, anything the two
    // calls above left would already be there too.
    client
        .enqueue(job("emails", "send_receipt"))
        .await
        .expect("enqueue");
    let records = harness.trail(1).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let records_after = harness.trail(1).await;
    assert_eq!(records, records_after);
    assert_eq!(records.len(), 1, "only the enqueue: {records:?}");
    assert_eq!(records[0].operation, "flexiq.v1.ProducerService/Enqueue");
    harness.stop().await;
}

#[tokio::test]
async fn a_batch_leaves_one_record_per_job() {
    let harness = Harness::start("audit-batch").await;
    let token = harness.mint("ci", produce());
    let results = harness
        .producer(&token)
        .enqueue_batch(EnqueueBatchRequest {
            items: vec![job("emails", "a"), job("emails", "b"), job("emails", "c")],
        })
        .await
        .expect("batch")
        .into_inner()
        .results;
    assert_eq!(results.len(), 3);

    let records = harness.trail(3).await;
    assert_eq!(records.len(), 3, "{records:?}");
    let mut targets: Vec<_> = records.iter().filter_map(|r| r.target.clone()).collect();
    targets.sort();
    targets.dedup();
    assert_eq!(targets.len(), 3, "three distinct jobs: {records:?}");
    assert!(records
        .iter()
        .all(|r| r.operation == "flexiq.v1.ProducerService/EnqueueBatch"));
    harness.stop().await;
}

#[tokio::test]
async fn a_cancel_names_the_job_it_aimed_at() {
    let harness = Harness::start("audit-cancel").await;
    let token = harness.mint("ops", produce());
    let mut client = harness.producer(&token);
    let job_id = client
        .enqueue(job("emails", "send_receipt"))
        .await
        .expect("enqueue")
        .into_inner()
        .job
        .expect("a job")
        .id;
    client
        .cancel_job(CancelJobRequest {
            job_id: job_id.clone(),
        })
        .await
        .expect("cancel");

    let records = harness.trail(2).await;
    let cancel = &records[1];
    assert_eq!(cancel.operation, "flexiq.v1.ProducerService/CancelJob");
    assert_eq!(cancel.target.as_deref(), Some(job_id.as_str()));
    let by_job = harness
        .storage
        .list_audit_after(
            NAMESPACE,
            &AuditFilter {
                target_kind: Some("job".into()),
                target: Some(job_id.clone()),
                ..Default::default()
            },
            100,
            None,
        )
        .expect("list by job");
    assert_eq!(by_job.len(), 2, "who enqueued it and who cancelled it");
    harness.stop().await;
}

#[tokio::test]
async fn an_admin_write_names_its_queue() {
    let harness = Harness::start("audit-admin").await;
    let token = harness.mint("operator", ScopeSet::of(&[Scope::Admin]));
    AdminServiceClient::with_interceptor(harness.channel.clone(), Bearer::new(&token.plaintext))
        .pause_queue(PauseQueueRequest {
            queue: "emails".into(),
        })
        .await
        .expect("pause");

    let records = harness.trail(1).await;
    assert_eq!(
        records[0].operation,
        "flexiq.admin.v1.AdminService/PauseQueue"
    );
    assert_eq!(records[0].target_kind.as_deref(), Some("queue"));
    assert_eq!(records[0].target.as_deref(), Some("emails"));
    assert_eq!(records[0].principal, "operator");
    harness.stop().await;
}

#[tokio::test]
async fn the_json_facade_leaves_the_same_record() {
    let harness = Harness::start("audit-facade").await;
    let token = harness.mint("webhook", produce());
    let response = reqwest::Client::new()
        .post(format!("{}/v1/jobs", harness.base))
        .bearer_auth(&token.plaintext)
        .json(&json!({"taskName": "send_receipt", "raw": "", "options": {"queue": "emails"}}))
        .send()
        .await
        .expect("the listener answers");
    assert!(response.status().is_success(), "{}", response.status());
    let body: serde_json::Value = response.json().await.expect("a JSON body");
    let job_id = body["job"]["id"].as_str().expect("a job id").to_string();

    let records = harness.trail(1).await;
    assert_eq!(records[0].operation, "flexiq.v1.ProducerService/Enqueue");
    assert_eq!(records[0].target.as_deref(), Some(job_id.as_str()));
    assert_eq!(records[0].token_id, token.id);
    assert_no_secret(&records, &token);
    harness.stop().await;
}

#[tokio::test]
async fn an_inspect_token_reads_the_trail_by_token_and_by_target() {
    let harness = Harness::start("audit-list").await;
    let (alice, bob) = (
        harness.mint("alice", produce()),
        harness.mint("bob", produce()),
    );
    let mut first = None;
    for token in [&alice, &bob, &alice] {
        let id = harness
            .producer(token)
            .enqueue(job("emails", "send_receipt"))
            .await
            .expect("enqueue")
            .into_inner()
            .job
            .expect("a job")
            .id;
        first.get_or_insert(id);
    }
    harness.trail(3).await;

    let inspector = harness.mint("auditor", ScopeSet::of(&[Scope::Inspect]));
    let mut admin = AdminServiceClient::with_interceptor(
        harness.channel.clone(),
        Bearer::new(&inspector.plaintext),
    );

    let by_token = admin
        .list_audit_records(ListAuditRecordsRequest {
            token_id: alice.id.clone(),
            ..Default::default()
        })
        .await
        .expect("inspect reaches the trail")
        .into_inner();
    assert_eq!(by_token.records.len(), 2);
    assert!(by_token.records.iter().all(|r| r.principal == "alice"));
    assert!(
        by_token.next_page_token.is_empty(),
        "a short page is the last"
    );

    let by_job = admin
        .list_audit_records(ListAuditRecordsRequest {
            target_kind: "job".into(),
            target: first.clone().expect("enqueued"),
            ..Default::default()
        })
        .await
        .expect("list by job")
        .into_inner();
    assert_eq!(by_job.records.len(), 1, "who enqueued this job");
    assert_eq!(by_job.records[0].token_id, alice.id);

    // Paging: one at a time walks all three, newest first, then stops.
    let mut seen = Vec::new();
    let mut token = String::new();
    loop {
        let page = admin
            .list_audit_records(ListAuditRecordsRequest {
                page_size: 1,
                page_token: token,
                ..Default::default()
            })
            .await
            .expect("page")
            .into_inner();
        seen.extend(page.records.into_iter().map(|r| r.principal));
        if page.next_page_token.is_empty() {
            break;
        }
        token = page.next_page_token;
    }
    assert_eq!(seen, ["alice", "bob", "alice"]);

    // The same read over the facade, under the same `inspect` credential.
    let response = reqwest::Client::new()
        .get(format!(
            "{}/v1/admin/auditRecords?tokenId={}",
            harness.base, bob.id
        ))
        .bearer_auth(&inspector.plaintext)
        .send()
        .await
        .expect("the listener answers");
    assert!(response.status().is_success(), "{}", response.status());
    let body: serde_json::Value = response.json().await.expect("a JSON body");
    assert_eq!(body["records"].as_array().map(Vec::len), Some(1), "{body}");
    assert_eq!(body["records"][0]["principal"], "bob");
    assert_eq!(body["records"][0]["targetKind"], "job");
    harness.stop().await;
}

#[tokio::test]
async fn a_produce_token_cannot_read_the_trail() {
    let harness = Harness::start("audit-list-denied").await;
    let token = harness.mint("ci", produce());
    let status = AdminServiceClient::with_interceptor(
        harness.channel.clone(),
        Bearer::new(&token.plaintext),
    )
    .list_audit_records(ListAuditRecordsRequest::default())
    .await
    .expect_err("the trail is an operator read");
    assert_eq!(status.code(), Code::PermissionDenied);
    harness.stop().await;
}
