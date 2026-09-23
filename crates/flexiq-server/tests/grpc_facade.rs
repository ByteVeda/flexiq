//! End to end: the JSON facade over a real socket, with an HTTP/1.1 client.
//!
//! The client is `reqwest`, not the generated one, and that is the point — the
//! acceptance criterion for this door is that something with no protobuf
//! toolchain and no CBOR library can submit a job that an ordinary worker then
//! runs. So the assertions are about what a `curl` user sees: the status code,
//! the JSON body, the reason inside a failure, and the row that ends up in
//! storage.
//!
//! What is *not* re-tested here is the behaviour of the RPCs themselves. The
//! handlers are the same ones `grpc_producer.rs` drives over gRPC; if the
//! facade re-tested them, it would be asserting that two doors agree by
//! checking one of them twice.
#![cfg(feature = "grpc")]

mod support;

use flexiq_core::job::{now_millis, NewJob};
use flexiq_core::storage::records::WorkerRegistration;
use flexiq_core::storage::Storage;
use flexiq_server::config::grpc::GrpcConfig;
use flexiq_server::config::listen::ListenAddress;
use flexiq_server::grpc::pb::producer_service_client::ProducerServiceClient;
use flexiq_server::grpc::pb::{enqueue_request, EnqueueOptions, EnqueueRequest};
use flexiq_server::grpc::Listener;
use flexiq_server::runtime::shutdown::Shutdown;
use flexiq_server::tokens::{Scope, ScopeSet};
use reqwest::StatusCode;
use serde_json::{json, Value};

use support::{mint_token, temp_storage, temp_workflows, Bearer, TempStorage};

/// The one namespace this door serves.
const NAMESPACE: &str = "grpc-facade-tests";

/// The envelope for `f("a@b.c")`, as `flexiq_core::wire` encodes it: the CBOR
/// tag byte, then `[["a@b.c"], {}]`.
const CALL_ENVELOPE: [u8; 10] = [0x02, 0x82, 0x81, 0x65, b'a', b'@', b'b', b'.', b'c', 0xa0];

/// A running listener, a bearer token and an HTTP client pointed at it.
struct Harness {
    base: String,
    token: String,
    client: reqwest::Client,
    storage: TempStorage,
    shutdown: Shutdown,
    served: tokio::task::JoinHandle<anyhow::Result<()>>,
}

/// One answer, in the two parts a caller acts on.
struct Answer {
    status: StatusCode,
    body: Value,
}

impl Answer {
    /// The `reason` a client branches on, which every failure carries.
    fn reason(&self) -> &str {
        self.body["error"]["details"][0]["reason"]
            .as_str()
            .unwrap_or_default()
    }

    /// The `google.rpc.Code`'s own name.
    fn code(&self) -> &str {
        self.body["error"]["status"].as_str().unwrap_or_default()
    }
}

impl Harness {
    async fn start(label: &str) -> Self {
        Self::start_with_scopes(label, ScopeSet::ALL).await
    }

    async fn start_with_scopes(label: &str, scopes: ScopeSet) -> Self {
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
            .expect("a TCP listener knows what it bound");
        let served = tokio::spawn(listener.serve(
            (*storage).clone(),
            temp_workflows(&storage),
            None,
            shutdown.clone(),
        ));

        Self {
            base: format!("http://{addr}"),
            token,
            client: reqwest::Client::new(),
            storage,
            shutdown,
            served,
        }
    }

    async fn send(&self, request: reqwest::RequestBuilder) -> Answer {
        let response = request.send().await.expect("the listener answers");
        let status = response.status();
        let text = response.text().await.expect("a body");
        let body = if text.is_empty() {
            Value::Null
        } else {
            serde_json::from_str(&text)
                .unwrap_or_else(|error| panic!("the answer is not JSON ({error}): {text}"))
        };
        Answer { status, body }
    }

    async fn get(&self, path: &str) -> Answer {
        self.send(
            self.client
                .get(format!("{}{path}", self.base))
                .bearer_auth(&self.token),
        )
        .await
    }

    async fn post(&self, path: &str, body: Value) -> Answer {
        self.send(
            self.client
                .post(format!("{}{path}", self.base))
                .bearer_auth(&self.token)
                .json(&body),
        )
        .await
    }

    /// The job id out of an enqueue answer.
    async fn enqueue(&self, body: Value) -> String {
        let answer = self.post("/v1/jobs", body).await;
        assert_eq!(answer.status, StatusCode::OK, "body: {}", answer.body);
        answer.body["job"]["id"]
            .as_str()
            .expect("an enqueue answers with its job")
            .to_string()
    }

    async fn stop(self) {
        self.shutdown.trigger();
        self.served
            .await
            .expect("the serve task must not panic")
            .expect("a shutdown is not an error");
    }
}

/// The acceptance criterion, as literally as a test can state it: a JSON body
/// with no protobuf and no CBOR in it produces a row an ordinary worker claims,
/// carrying the same payload envelope an SDK would have sent.
#[tokio::test]
async fn a_json_post_enqueues_a_job_an_unmodified_worker_would_run() {
    let harness = Harness::start("grpc-facade-enqueue").await;

    let id = harness
        .enqueue(json!({
            "taskName": "send_email",
            "structured": {"args": ["a@b.c"]},
            "options": {"queue": "emails", "priority": 5}
        }))
        .await;

    let stored = harness
        .storage
        .get_job(&id, Some(NAMESPACE))
        .expect("read")
        .expect("the job exists in storage");
    assert_eq!(stored.queue, "emails");
    assert_eq!(stored.task_name, "send_email");
    assert_eq!(stored.priority, 5);
    assert_eq!(stored.namespace.as_deref(), Some(NAMESPACE));
    assert_eq!(
        stored.payload,
        CALL_ENVELOPE.to_vec(),
        "the server encodes structured arguments into the one payload envelope"
    );

    harness.stop().await;
}

/// The other body arm, for a client that does have a codec: bytes in, the same
/// bytes in storage.
#[tokio::test]
async fn a_raw_body_reaches_storage_untouched() {
    let harness = Harness::start("grpc-facade-raw").await;

    let id = harness
        .enqueue(json!({"taskName": "t", "raw": "AoKCAWFhoA=="}))
        .await;

    let stored = harness
        .storage
        .get_job(&id, Some(NAMESPACE))
        .expect("read")
        .expect("the job exists");
    assert_eq!(
        stored.payload,
        vec![0x02, 0x82, 0x82, 0x01, 0x61, 0x61, 0xa0]
    );

    harness.stop().await;
}

#[tokio::test]
async fn a_job_reads_back_and_its_blobs_are_opt_in() {
    let harness = Harness::start("grpc-facade-read").await;
    let id = harness
        .enqueue(json!({"taskName": "t", "raw": "AQID", "options": {"notes": "a note"}}))
        .await;

    let plain = harness.get(&format!("/v1/jobs/{id}")).await;
    assert_eq!(plain.status, StatusCode::OK);
    let job = &plain.body["job"];
    assert_eq!(job["id"], Value::from(id.as_str()));
    assert_eq!(job["status"], Value::from("JOB_STATUS_PENDING"));
    assert_eq!(job["notes"], Value::from("a note"));
    assert!(
        job.get("payload").is_none(),
        "a blob nobody asked for is a missing key: {job}"
    );

    let with_blob = harness
        .get(&format!("/v1/jobs/{id}?includePayload=true"))
        .await;
    assert_eq!(with_blob.body["job"]["payload"], Value::from("AQID"));
    // Absent, not empty: the job has not run.
    assert!(with_blob.body["job"].get("result").is_none());

    harness.stop().await;
}

#[tokio::test]
async fn a_listing_filters_by_the_enum_name_and_carries_no_payloads() {
    let harness = Harness::start("grpc-facade-list").await;
    harness
        .enqueue(json!({"taskName": "t", "raw": "AQID", "options": {"queue": "emails"}}))
        .await;
    harness
        .enqueue(json!({"taskName": "t", "raw": "AQID", "options": {"queue": "reports"}}))
        .await;

    let all = harness.get("/v1/jobs?status=JOB_STATUS_PENDING").await;
    assert_eq!(all.status, StatusCode::OK);
    assert_eq!(all.body["jobs"].as_array().expect("an array").len(), 2);
    assert!(all.body["jobs"][0].get("payload").is_none());

    let filtered = harness.get("/v1/jobs?queue=emails").await;
    assert_eq!(filtered.body["jobs"].as_array().expect("an array").len(), 1);

    let unknown = harness.get("/v1/jobs?status=pending").await;
    assert_eq!(unknown.status, StatusCode::BAD_REQUEST);
    assert_eq!(unknown.reason(), "INVALID_REQUEST");

    harness.stop().await;
}

/// The custom-method path, and the property that makes `CancelJob` idempotent:
/// the answer describes state, so the second call says what the first did.
#[tokio::test]
async fn cancelling_twice_answers_the_same_state_twice() {
    let harness = Harness::start("grpc-facade-cancel").await;
    let id = harness.enqueue(json!({"taskName": "t", "raw": ""})).await;

    let first = harness
        .post(&format!("/v1/jobs/{id}:cancel"), json!({}))
        .await;
    assert_eq!(first.status, StatusCode::OK, "body: {}", first.body);
    assert_eq!(
        first.body["job"]["status"],
        Value::from("JOB_STATUS_CANCELLED")
    );

    let second = harness
        .post(&format!("/v1/jobs/{id}:cancel"), json!({}))
        .await;
    assert_eq!(second.body["job"]["status"], first.body["job"]["status"]);

    harness.stop().await;
}

/// A custom method nobody implements is not a job id — it is an address with
/// no RPC at it, and answers as one.
#[tokio::test]
async fn an_unknown_custom_method_is_not_mistaken_for_a_job_id() {
    let harness = Harness::start("grpc-facade-verb").await;
    let id = harness.enqueue(json!({"taskName": "t", "raw": ""})).await;

    for path in [format!("/v1/jobs/{id}:pause"), format!("/v1/jobs/{id}")] {
        let answer = harness.post(&path, json!({})).await;
        assert_eq!(answer.status, StatusCode::NOT_IMPLEMENTED, "path: {path}");
        assert_eq!(answer.reason(), "NO_SUCH_METHOD", "path: {path}");
    }

    harness.stop().await;
}

#[tokio::test]
async fn stats_count_one_queue_and_the_whole_namespace() {
    let harness = Harness::start("grpc-facade-stats").await;
    harness
        .enqueue(json!({"taskName": "t", "raw": "", "options": {"queue": "emails"}}))
        .await;
    harness
        .enqueue(json!({"taskName": "t", "raw": "", "options": {"queue": "reports"}}))
        .await;

    let one = harness.get("/v1/queues/emails/stats").await;
    assert_eq!(one.status, StatusCode::OK);
    // int64 on the wire is a string in JSON, because a JSON number is a double.
    assert_eq!(one.body["pending"], Value::from("1"));

    let every = harness.get("/v1/stats").await;
    assert_eq!(every.body["pending"], Value::from("2"));

    // The contract binds `queue` to a path on one binding and leaves it
    // unbound on the other, which makes it a query parameter there. The two
    // spellings must answer the same thing, or the generated document
    // advertises a filter the server ignores.
    let filtered = harness.get("/v1/stats?queue=emails").await;
    assert_eq!(filtered.status, StatusCode::OK);
    assert_eq!(filtered.body["pending"], one.body["pending"]);

    // And a parameter nobody declared is refused here as it is everywhere else.
    let refused = harness.get("/v1/stats?nope=1").await;
    assert_eq!(refused.status, StatusCode::BAD_REQUEST);
    assert_eq!(refused.reason(), "INVALID_REQUEST");

    harness.stop().await;
}

#[tokio::test]
async fn a_batch_answers_one_result_per_item_in_input_order() {
    let harness = Harness::start("grpc-facade-batch").await;

    let answer = harness
        .post(
            "/v1/jobs:batchEnqueue",
            json!({"items": [
                {"taskName": "first", "raw": ""},
                {"taskName": "second", "structured": {"args": [1]}}
            ]}),
        )
        .await;

    assert_eq!(answer.status, StatusCode::OK, "body: {}", answer.body);
    let results = answer.body["results"].as_array().expect("an array");
    assert_eq!(results.len(), 2);
    assert_eq!(
        results[0]["enqueued"]["job"]["taskName"],
        Value::from("first")
    );
    assert_eq!(
        results[1]["enqueued"]["job"]["taskName"],
        Value::from("second")
    );

    harness.stop().await;
}

/// The refusal a `curl` user meets first. It must be a JSON body with an HTTP
/// status, and never the gRPC framing — which is HTTP 200 with a trailer, and
/// reads to a JSON client as an empty success.
#[tokio::test]
async fn no_credential_is_a_json_401_and_not_a_grpc_trailer() {
    let harness = Harness::start("grpc-facade-anon").await;

    let response = harness
        .client
        .get(format!("{}/v1/jobs", harness.base))
        .send()
        .await
        .expect("the listener answers");
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(response.headers()["content-type"], "application/json");
    assert!(
        !response.headers().contains_key("grpc-status"),
        "a JSON client must not be handed gRPC framing"
    );
    let body: Value = response.json().await.expect("a JSON body");
    assert_eq!(
        body["error"]["details"][0]["reason"],
        Value::from("UNAUTHENTICATED")
    );
    assert_eq!(
        body["error"]["details"][0]["domain"],
        Value::from("flexiq.byteveda.org")
    );

    harness.stop().await;
}

/// A facade route is the producer package by another spelling, so it asks for
/// the same scope. A transcoded RPC must not be a way to call it with a
/// credential the RPC itself would refuse.
#[tokio::test]
async fn an_execute_only_credential_cannot_reach_the_facade() {
    let harness =
        Harness::start_with_scopes("grpc-facade-scope", ScopeSet::of(&[Scope::Execute])).await;

    let answer = harness.get("/v1/jobs").await;
    assert_eq!(answer.status, StatusCode::FORBIDDEN);
    assert_eq!(answer.reason(), "SCOPE_DENIED");
    assert_eq!(
        answer.body["error"]["details"][0]["metadata"]["scope"],
        Value::from("produce")
    );

    harness.stop().await;
}

#[tokio::test]
async fn a_missing_job_carries_its_reason_and_a_404() {
    let harness = Harness::start("grpc-facade-missing").await;

    let answer = harness.get("/v1/jobs/no-such-job").await;
    assert_eq!(answer.status, StatusCode::NOT_FOUND);
    assert_eq!(answer.code(), "NOT_FOUND");
    assert_eq!(answer.reason(), "JOB_NOT_FOUND");

    // A run answers without an `ErrorInfo`, so this is the arm that used to
    // render a 404 with `"status": "OK"` in the body — a client branching on
    // the name, as the contract tells it to, read a failure as a success.
    let answer = harness.get("/v1/workflows/no-such-run").await;
    assert_eq!(answer.status, StatusCode::NOT_FOUND);
    assert_eq!(answer.code(), "NOT_FOUND");
    assert_eq!(answer.body["error"]["code"], Value::from(404));

    harness.stop().await;
}

/// D15 from the outside: a `GET` reaches only the `NO_SIDE_EFFECTS` RPCs, and
/// a path with no binding at all answers the same way a gRPC caller's unknown
/// method does.
#[tokio::test]
async fn a_get_on_a_write_and_a_path_with_no_binding_are_both_unimplemented() {
    let harness = Harness::start("grpc-facade-unrouted").await;

    for path in [
        "/v1/jobs:batchEnqueue",
        "/v1/nothing-here",
        "/v1/executors",
        "/",
    ] {
        let answer = harness.get(path).await;
        assert_eq!(answer.status, StatusCode::NOT_IMPLEMENTED, "path: {path}");
        assert_eq!(answer.reason(), "NO_SUCH_METHOD", "path: {path}");
    }

    harness.stop().await;
}

#[tokio::test]
async fn a_body_that_is_not_the_message_is_refused_by_name() {
    let harness = Harness::start("grpc-facade-malformed").await;

    let broken = harness.post("/v1/jobs", json!({"taskname": "t"})).await;
    assert_eq!(broken.status, StatusCode::BAD_REQUEST);
    assert_eq!(broken.reason(), "MALFORMED_PAYLOAD");
    assert!(
        broken.body["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("taskname"),
        "the answer must name the field: {}",
        broken.body
    );

    // Decoded, but not a request this service accepts. A different reason,
    // because a client acts on them differently.
    let no_body_arm = harness.post("/v1/jobs", json!({"taskName": "t"})).await;
    assert_eq!(no_body_arm.status, StatusCode::BAD_REQUEST);
    assert_eq!(no_body_arm.reason(), "INVALID_REQUEST");

    harness.stop().await;
}

/// The two doors agree about what is too large, because they read the same
/// number — and the facade refuses before it parses.
#[tokio::test]
async fn a_body_over_the_cap_is_refused() {
    let harness = Harness::start("grpc-facade-cap").await;

    let oversized = "x".repeat(5 * 1024 * 1024);
    let answer = harness
        .send(
            harness
                .client
                .post(format!("{}/v1/jobs", harness.base))
                .bearer_auth(&harness.token)
                .header("content-type", "application/json")
                .body(format!("{{\"taskName\": \"{oversized}\"}}")),
        )
        .await;

    assert_eq!(answer.status, StatusCode::BAD_REQUEST);
    assert_eq!(answer.code(), "OUT_OF_RANGE");
    assert_eq!(answer.reason(), "INVALID_REQUEST");

    harness.stop().await;
}

/// One listener, two doors. Accepting HTTP/1.1 must not have cost the gRPC one
/// anything, and a job enqueued through either is the same kind of row.
#[tokio::test]
async fn both_doors_answer_on_the_one_port() {
    let harness = Harness::start("grpc-facade-both-doors").await;

    let over_json = harness
        .enqueue(json!({"taskName": "t", "raw": "AQ=="}))
        .await;

    let channel = tonic::transport::Channel::from_shared(harness.base.clone())
        .expect("a valid endpoint")
        .connect()
        .await
        .expect("the listener still accepts an HTTP/2 client");
    let mut client = ProducerServiceClient::with_interceptor(channel, Bearer::new(&harness.token));
    let over_grpc = client
        .enqueue(EnqueueRequest {
            task_name: "t".to_string(),
            body: Some(enqueue_request::Body::Raw(vec![0x01])),
            options: Some(EnqueueOptions::default()),
        })
        .await
        .expect("the gRPC door still answers")
        .into_inner()
        .job
        .expect("a response carries its job")
        .id;

    assert_ne!(over_json, over_grpc);
    for id in [over_json, over_grpc] {
        let stored = harness
            .storage
            .get_job(&id, Some(NAMESPACE))
            .expect("read")
            .expect("the job exists");
        assert_eq!(stored.payload, vec![0x01]);
    }

    harness.stop().await;
}

// ── flexiq.admin.v1 ──────────────────────────────────────────────────
//
// The operator door, spelled in JSON. As above, what is pinned is what a
// `curl` user sees; `grpc_admin.rs` owns the behaviour of the RPCs themselves.

/// The scopes an operator's token carries: `inspect` to read, `admin` to write.
fn operator() -> ScopeSet {
    ScopeSet::of(&[Scope::Inspect, Scope::Admin])
}

/// Seed a job straight into storage, since an operator token cannot enqueue.
fn seed_job(storage: &TempStorage, queue: &str, task: &str) -> String {
    storage
        .enqueue(NewJob {
            queue: queue.to_string(),
            task_name: task.to_string(),
            payload: CALL_ENVELOPE.to_vec(),
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
        })
        .expect("enqueue")
        .id
}

/// Dead-letter one job of `task`, returning the entry's id.
fn seed_dead_letter(storage: &TempStorage, task: &str) -> String {
    let id = seed_job(storage, "dlq", task);
    storage
        .dequeue("dlq", now_millis() + 1_000, Some(NAMESPACE))
        .expect("dequeue");
    let running = storage.get_job(&id, None).expect("read").expect("present");
    storage
        .move_to_dlq(&running, "boom", None)
        .expect("dead-letter");
    storage
        .list_dead(100, 0, Some(NAMESPACE))
        .expect("list")
        .into_iter()
        .find(|dead| dead.original_job_id == id)
        .expect("the entry")
        .id
}

#[tokio::test]
async fn an_operator_pauses_and_resumes_a_queue_over_json() {
    let harness = Harness::start_with_scopes("grpc-facade-admin-queues", operator()).await;
    seed_job(&harness.storage, "emails", "send");

    let paused = harness
        .post("/v1/admin/queues/emails:pause", json!({}))
        .await;
    assert_eq!(paused.status, StatusCode::OK, "body: {}", paused.body);
    assert_eq!(paused.body["queue"]["name"], Value::from("emails"));
    assert_eq!(paused.body["queue"]["paused"], Value::from(true));
    assert_eq!(paused.body["queue"]["pending"], Value::from("1"));

    let listed = harness.get("/v1/admin/queues").await;
    assert_eq!(listed.status, StatusCode::OK, "body: {}", listed.body);
    let queues = listed.body["queues"].as_array().expect("an array");
    let emails = queues
        .iter()
        .find(|queue| queue["name"] == "emails")
        .expect("the paused queue is listed");
    assert_eq!(emails["paused"], Value::from(true));

    let resumed = harness
        .post("/v1/admin/queues/emails:resume", json!({}))
        .await;
    assert_eq!(resumed.body["queue"]["paused"], Value::from(false));

    let throughput = harness.get("/v1/admin/throughput?window=300s").await;
    assert_eq!(
        throughput.status,
        StatusCode::OK,
        "body: {}",
        throughput.body
    );
    assert_eq!(throughput.body["window"], Value::from("300s"));

    let workers = harness.get("/v1/admin/workers").await;
    assert_eq!(workers.status, StatusCode::OK, "body: {}", workers.body);
    assert_eq!(workers.body["workers"], json!([]));

    harness.stop().await;
}

#[tokio::test]
async fn a_dead_letter_is_listed_read_and_replayed_over_json() {
    let harness = Harness::start_with_scopes("grpc-facade-admin-dlq", operator()).await;
    let id = seed_dead_letter(&harness.storage, "charge");

    let listed = harness.get("/v1/admin/deadLetters?pageSize=10").await;
    assert_eq!(listed.status, StatusCode::OK, "body: {}", listed.body);
    let entry = &listed.body["deadLetters"][0];
    assert_eq!(entry["id"], Value::from(id.as_str()));
    assert_eq!(entry["taskName"], Value::from("charge"));
    assert!(entry.get("payload").is_none(), "a listing carries no blob");

    let read = harness
        .get(&format!("/v1/admin/deadLetters/{id}?includePayload=true"))
        .await;
    assert_eq!(read.status, StatusCode::OK, "body: {}", read.body);
    assert!(read.body["deadLetter"]["payload"].is_string());

    let replayed = harness
        .post(&format!("/v1/admin/deadLetters/{id}:replay"), json!({}))
        .await;
    assert_eq!(replayed.status, StatusCode::OK, "body: {}", replayed.body);
    assert_eq!(replayed.body["job"]["taskName"], Value::from("charge"));
    assert_eq!(
        replayed.body["job"]["status"],
        Value::from("JOB_STATUS_PENDING")
    );

    let emptied = harness.get("/v1/admin/deadLetters").await;
    assert_eq!(emptied.body["deadLetters"], json!([]));

    // A oneof is one arm; two is a body only this door can see.
    let both = harness
        .post(
            "/v1/admin/deadLetters:purge",
            json!({"failedBefore": "2025-09-03T12:26:40Z", "taskName": "charge"}),
        )
        .await;
    assert_eq!(both.status, StatusCode::BAD_REQUEST);
    assert_eq!(both.reason(), "INVALID_REQUEST");

    seed_dead_letter(&harness.storage, "charge");
    let purged = harness
        .post("/v1/admin/deadLetters:purge", json!({"taskName": "charge"}))
        .await;
    assert_eq!(purged.status, StatusCode::OK, "body: {}", purged.body);
    assert_eq!(purged.body["purged"], Value::from("1"));

    harness.stop().await;
}

#[tokio::test]
async fn a_periodic_task_is_put_with_structured_arguments_and_triggered() {
    let harness = Harness::start_with_scopes("grpc-facade-admin-periodic", operator()).await;

    let put = harness
        .post(
            "/v1/admin/periodicTasks",
            json!({
                "name": "nightly",
                "taskName": "report",
                "cron": "0 0 3 * * *",
                "structured": {"args": ["a@b.c"]}
            }),
        )
        .await;
    assert_eq!(put.status, StatusCode::OK, "body: {}", put.body);
    assert_eq!(put.body["periodicTask"]["name"], Value::from("nightly"));
    assert_eq!(put.body["periodicTask"]["enabled"], Value::from(true));
    assert!(put.body["periodicTask"].get("payload").is_none());

    let read = harness
        .get("/v1/admin/periodicTasks/nightly?includePayload=true")
        .await;
    assert_eq!(read.status, StatusCode::OK, "body: {}", read.body);
    assert!(read.body["periodicTask"]["payload"].is_string());

    let triggered = harness
        .post("/v1/admin/periodicTasks/nightly:trigger", json!({}))
        .await;
    assert_eq!(triggered.status, StatusCode::OK, "body: {}", triggered.body);
    let job_id = triggered.body["job"]["id"]
        .as_str()
        .expect("a trigger answers with its job");
    let stored = harness
        .storage
        .get_job(job_id, Some(NAMESPACE))
        .expect("read")
        .expect("the triggered job exists");
    assert_eq!(stored.task_name, "report");
    assert_eq!(
        stored.payload,
        CALL_ENVELOPE.to_vec(),
        "structured arguments are encoded as an enqueue's are"
    );

    let listed = harness.get("/v1/admin/periodicTasks").await;
    assert_eq!(
        listed.body["periodicTasks"]
            .as_array()
            .expect("an array")
            .len(),
        1
    );

    let deleted = harness
        .post("/v1/admin/periodicTasks/nightly:delete", json!({}))
        .await;
    assert_eq!(deleted.status, StatusCode::OK, "body: {}", deleted.body);
    assert_eq!(deleted.body, json!({}));

    harness.stop().await;
}

#[tokio::test]
async fn an_override_is_the_request_body_and_reads_back() {
    let harness = Harness::start_with_scopes("grpc-facade-admin-overrides", operator()).await;

    let set = harness
        .post("/v1/admin/tasks/send/override", json!({"timeout": "30s"}))
        .await;
    assert_eq!(set.status, StatusCode::OK, "body: {}", set.body);
    assert_eq!(set.body["taskOverride"]["timeout"], Value::from("30s"));

    let queue = harness
        .post(
            "/v1/admin/queues/emails/override",
            json!({"rateLimit": "10/s"}),
        )
        .await;
    assert_eq!(queue.status, StatusCode::OK, "body: {}", queue.body);
    assert_eq!(
        queue.body["queueOverride"]["rateLimit"],
        Value::from("10/s")
    );

    let listed = harness.get("/v1/admin/overrides").await;
    assert_eq!(listed.status, StatusCode::OK, "body: {}", listed.body);
    assert_eq!(listed.body["tasks"]["send"]["timeout"], Value::from("30s"));
    assert_eq!(
        listed.body["queues"]["emails"]["rateLimit"],
        Value::from("10/s")
    );

    // A field the message does not have is refused by name.
    let typo = harness
        .post("/v1/admin/tasks/send/override", json!({"timeuot": "30s"}))
        .await;
    assert_eq!(typo.status, StatusCode::BAD_REQUEST);
    assert_eq!(typo.reason(), "MALFORMED_PAYLOAD");

    let cleared = harness
        .post("/v1/admin/tasks/send/override:clear", json!({}))
        .await;
    assert_eq!(cleared.status, StatusCode::OK, "body: {}", cleared.body);
    let after = harness.get("/v1/admin/overrides").await;
    assert!(after.body["tasks"].get("send").is_none(), "{}", after.body);

    harness.stop().await;
}

#[tokio::test]
async fn an_operator_drains_a_worker_over_json() {
    let harness = Harness::start_with_scopes("grpc-facade-admin-drain", operator()).await;
    harness
        .storage
        .register_worker(&WorkerRegistration::new("w-mine", "emails", 2).namespace(Some(NAMESPACE)))
        .expect("register");
    harness
        .storage
        .register_worker(
            &WorkerRegistration::new("w-theirs", "emails", 1).namespace(Some("grpc-facade-other")),
        )
        .expect("register");

    let drained = harness
        .post("/v1/admin/workers/w-mine:drain", json!({}))
        .await;
    assert_eq!(drained.status, StatusCode::OK, "body: {}", drained.body);
    assert_eq!(drained.body["worker"]["workerId"], Value::from("w-mine"));
    assert_eq!(
        drained.body["worker"]["status"],
        Value::from("WORKER_STATUS_DRAINING")
    );
    let listed = harness.get("/v1/admin/workers").await;
    assert_eq!(
        listed.body["workers"][0]["status"],
        Value::from("WORKER_STATUS_DRAINING")
    );

    for path in [
        "/v1/admin/workers/w-theirs:drain",
        "/v1/admin/workers/w-nobody:drain",
    ] {
        let absent = harness.post(path, json!({})).await;
        assert_eq!(absent.status, StatusCode::NOT_FOUND, "path: {path}");
        assert_eq!(absent.code(), "NOT_FOUND", "path: {path}");
        assert_eq!(absent.reason(), "WORKER_NOT_FOUND", "path: {path}");
    }
    let theirs = harness
        .storage
        .list_workers(Some("grpc-facade-other"))
        .expect("list");
    assert_eq!(
        theirs[0].status, "active",
        "another tenant's worker drained"
    );

    harness.stop().await;
}

/// A verb nobody implements on an operator resource is an address with no RPC
/// at it, exactly as on a job — not a queue name, and not a `405`.
#[tokio::test]
async fn an_unknown_admin_verb_is_no_such_method() {
    let harness = Harness::start_with_scopes("grpc-facade-admin-verb", operator()).await;

    for path in [
        "/v1/admin/queues/emails:drain",
        "/v1/admin/queues/emails",
        "/v1/admin/deadLetters/abc:bogus",
        "/v1/admin/deadLetters/abc",
        "/v1/admin/periodicTasks/nightly:explode",
        "/v1/admin/workers/w-1:bogus",
        "/v1/admin/workers/w-1",
    ] {
        let answer = harness.post(path, json!({})).await;
        assert_eq!(answer.status, StatusCode::NOT_IMPLEMENTED, "path: {path}");
        assert_eq!(answer.reason(), "NO_SUCH_METHOD", "path: {path}");
    }

    harness.stop().await;
}

/// The facade asks what the gRPC door asks: `inspect` to read, `admin` to
/// write, and a producer's token for neither.
#[tokio::test]
async fn each_scope_reaches_its_half_of_the_operator_paths() {
    let producer =
        Harness::start_with_scopes("grpc-facade-admin-produce", ScopeSet::of(&[Scope::Produce]))
            .await;
    let refused = producer.get("/v1/admin/queues").await;
    assert_eq!(refused.status, StatusCode::FORBIDDEN);
    assert_eq!(refused.reason(), "SCOPE_DENIED");
    assert_eq!(
        refused.body["error"]["details"][0]["metadata"]["scope"],
        Value::from("inspect")
    );
    producer.stop().await;

    let inspector =
        Harness::start_with_scopes("grpc-facade-admin-inspect", ScopeSet::of(&[Scope::Inspect]))
            .await;
    let read = inspector.get("/v1/admin/queues").await;
    assert_eq!(read.status, StatusCode::OK, "body: {}", read.body);
    let refused = inspector
        .post("/v1/admin/queues/emails:pause", json!({}))
        .await;
    assert_eq!(refused.status, StatusCode::FORBIDDEN);
    assert_eq!(refused.reason(), "SCOPE_DENIED");
    assert_eq!(
        refused.body["error"]["details"][0]["metadata"]["scope"],
        Value::from("admin")
    );
    // A drain is a write, whatever an `inspect` token can see of the worker.
    inspector
        .storage
        .register_worker(&WorkerRegistration::new("w-1", "emails", 1).namespace(Some(NAMESPACE)))
        .expect("register");
    let refused = inspector
        .post("/v1/admin/workers/w-1:drain", json!({}))
        .await;
    assert_eq!(refused.status, StatusCode::FORBIDDEN);
    assert_eq!(refused.reason(), "SCOPE_DENIED");
    assert_eq!(
        refused.body["error"]["details"][0]["metadata"]["scope"],
        Value::from("admin")
    );
    inspector.stop().await;
}
