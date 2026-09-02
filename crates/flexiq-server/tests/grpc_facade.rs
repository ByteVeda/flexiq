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

use support::{mint_token, temp_storage, Bearer, TempStorage};

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
        let listener = Listener::bind(&GrpcConfig {
            listen: ListenAddress::Tcp("127.0.0.1:0".parse().expect("valid address")),
            namespace: NAMESPACE.to_string(),
        })
        .await
        .expect("bind");
        let addr = listener
            .local_addr()
            .expect("a TCP listener knows what it bound");
        let served = tokio::spawn(listener.serve((*storage).clone(), shutdown.clone()));

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
