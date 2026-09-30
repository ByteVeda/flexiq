//! End to end: `WatchJobs` as Server-Sent Events on the JSON facade.
//!
//! The client is `reqwest` reading raw bytes, because what is under test is the
//! framing a `curl` or an `EventSource` sees: which events arrive, what their
//! `id` is, and how the stream ends. What each item *means* is pinned over gRPC
//! in `grpc_watch.rs`; both doors call the same handler.
#![cfg(feature = "grpc")]

mod support;

use std::time::Duration;

use flexiq_server::config::grpc::GrpcConfig;
use flexiq_server::config::listen::ListenAddress;
use flexiq_server::config::watch::WatchConfig;
use flexiq_server::grpc::status::reason;
use flexiq_server::grpc::Listener;
use flexiq_server::runtime::shutdown::Shutdown;
use flexiq_server::tokens::ScopeSet;
use reqwest::StatusCode;
use serde_json::{json, Value};

use support::{mint_token, temp_storage, temp_workflows, TempStorage};

const NAMESPACE: &str = "grpc-watch-sse-tests";

/// How long any one expectation may take. A failure deadline, not a delay.
const WAIT: Duration = Duration::from_secs(20);

struct Harness {
    base: String,
    token: String,
    client: reqwest::Client,
    _storage: TempStorage,
    shutdown: Shutdown,
    served: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl Harness {
    async fn start(label: &str, watch: WatchConfig) -> Self {
        let storage = temp_storage(label);
        let token = mint_token(&storage, NAMESPACE, ScopeSet::ALL);
        let shutdown = Shutdown::default();
        let listener = Listener::bind(&GrpcConfig {
            watch,
            ..GrpcConfig::new(
                ListenAddress::Tcp("127.0.0.1:0".parse().expect("valid address")),
                NAMESPACE,
            )
        })
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
        Self {
            base: format!("http://{addr}"),
            token,
            client: reqwest::Client::new(),
            _storage: storage,
            shutdown,
            served,
        }
    }

    async fn post(&self, path: &str, body: Value) -> Value {
        let response = self
            .client
            .post(format!("{}{path}", self.base))
            .bearer_auth(&self.token)
            .json(&body)
            .send()
            .await
            .expect("the listener answers");
        assert_eq!(response.status(), StatusCode::OK, "{path}");
        response.json().await.expect("a JSON body")
    }

    async fn enqueue(&self, queue: &str) -> String {
        let answer = self
            .post(
                "/v1/jobs",
                json!({"taskName": "t", "raw": "", "options": {"queue": queue}}),
            )
            .await;
        answer["job"]["id"].as_str().expect("a job id").to_string()
    }

    async fn cancel(&self, id: &str) {
        self.post(&format!("/v1/jobs/{id}:cancel"), json!({})).await;
    }

    /// Open a watch, with `last_event_id` as an `EventSource` would resend it.
    async fn open(&self, query: &str, last_event_id: Option<&str>) -> reqwest::Response {
        let mut request = self
            .client
            .get(format!("{}/v1/jobs:watch?{query}", self.base))
            .bearer_auth(&self.token)
            .header("accept", "text/event-stream");
        if let Some(id) = last_event_id {
            request = request.header("last-event-id", id);
        }
        request.send().await.expect("the listener answers")
    }

    async fn watch(&self, query: &str, last_event_id: Option<&str>) -> Events {
        let response = self.open(query, last_event_id).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["content-type"], "text/event-stream");
        Events {
            response,
            buffer: String::new(),
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

/// One parsed event: its `event` name (empty for a plain message), `id` and
/// `data`.
#[derive(Debug, Default)]
struct Event {
    name: String,
    id: Option<String>,
    data: Value,
}

/// An open stream, read one event at a time. Comments are skipped.
struct Events {
    response: reqwest::Response,
    buffer: String,
}

impl Events {
    /// The next event, or `None` once the server closes the stream.
    async fn next(&mut self) -> Option<Event> {
        loop {
            if let Some(end) = self.buffer.find("\n\n") {
                let block: String = self.buffer.drain(..end + 2).collect();
                if let Some(event) = parse(&block) {
                    return Some(event);
                }
                continue;
            }
            let chunk = tokio::time::timeout(WAIT, self.response.chunk())
                .await
                .expect("the stream stalled")
                .expect("the stream reads");
            match chunk {
                Some(bytes) => self
                    .buffer
                    .push_str(std::str::from_utf8(&bytes).expect("UTF-8")),
                None => {
                    assert!(self.buffer.trim().is_empty(), "a torn event");
                    return None;
                }
            }
        }
    }

    async fn expect(&mut self) -> Event {
        self.next().await.expect("an event, not the end")
    }
}

/// An event block, or `None` for a comment-only one.
fn parse(block: &str) -> Option<Event> {
    let mut event = Event::default();
    let mut data = None;
    for line in block.lines() {
        if let Some(value) = line.strip_prefix("data: ") {
            data = Some(serde_json::from_str(value).expect("data is JSON"));
        } else if let Some(value) = line.strip_prefix("id: ") {
            event.id = Some(value.to_string());
        } else if let Some(value) = line.strip_prefix("event: ") {
            event.name = value.to_string();
        }
    }
    event.data = data?;
    Some(event)
}

#[tokio::test]
async fn a_watch_on_a_terminal_job_sends_it_then_ends() {
    let harness = Harness::start("sse-terminal", WatchConfig::default()).await;
    let id = harness.enqueue("q").await;
    harness.cancel(&id).await;

    let mut events = harness.watch(&format!("jobIds={id}"), None).await;
    let snapshot = events.expect().await;
    assert_eq!(snapshot.name, "");
    assert_eq!(snapshot.id, None, "an id watch hands out no cursor");
    let transition = &snapshot.data["transition"];
    assert_eq!(transition["jobId"], Value::from(id.as_str()));
    assert_eq!(transition["kind"], "JOB_TRANSITION_KIND_SNAPSHOT");
    assert_eq!(transition["status"], "JOB_STATUS_CANCELLED");
    assert_eq!(transition["terminal"], true);

    let end = events.expect().await;
    assert_eq!(end.name, "end");
    assert!(
        events.next().await.is_none(),
        "the stream closes after `end`"
    );
    harness.stop().await;
}

#[tokio::test]
async fn a_transition_on_this_server_reaches_an_open_watch() {
    let harness = Harness::start("sse-live", WatchConfig::default()).await;
    let a = harness.enqueue("q").await;
    let b = harness.enqueue("q").await;

    let mut events = harness.watch(&format!("jobIds={a}&jobIds={b}"), None).await;
    for _ in 0..2 {
        let snapshot = events.expect().await;
        assert_eq!(snapshot.data["transition"]["terminal"], false);
    }
    harness.cancel(&a).await;
    harness.cancel(&b).await;
    for _ in 0..2 {
        let cancelled = events.expect().await;
        assert_eq!(
            cancelled.data["transition"]["kind"],
            "JOB_TRANSITION_KIND_CANCELLED"
        );
    }
    assert_eq!(events.expect().await.name, "end");
    harness.stop().await;
}

/// The cursor is the event `id`, so a reconnect that sends it back as
/// `Last-Event-ID` resumes where the first stream stopped.
#[tokio::test]
async fn a_queue_watch_resumes_from_last_event_id() {
    let harness = Harness::start("sse-resume", WatchConfig::default()).await;

    let mut first = harness.watch("queue=orders", None).await;
    let checkpoint = first.expect().await;
    let cursor = checkpoint.id.expect("a queue watch opens with a cursor");
    assert_eq!(checkpoint.data, json!({"cursor": cursor}));
    drop(first);

    let id = harness.enqueue("orders").await;

    // The query still carries a stale cursor, as a reconnecting `EventSource`
    // resends its original URL; the header wins.
    let mut resumed = harness
        .watch("queue=orders&resumeCursor=stale", Some(&cursor))
        .await;
    let reopened = resumed.expect().await;
    assert_eq!(reopened.id.as_deref(), Some(cursor.as_str()));
    let enqueued = resumed.expect().await;
    assert!(enqueued.id.is_some_and(|id| id != cursor));
    assert_eq!(
        enqueued.data["transition"]["jobId"],
        Value::from(id.as_str())
    );
    assert_eq!(
        enqueued.data["transition"]["kind"],
        "JOB_TRANSITION_KIND_ENQUEUED"
    );
    harness.stop().await;
}

/// Refused before the stream opens: the facade's ordinary JSON error.
#[tokio::test]
async fn a_refusal_before_the_stream_is_a_json_error() {
    let harness = Harness::start("sse-refusal", WatchConfig::default()).await;
    for (query, status, why) in [
        ("", StatusCode::BAD_REQUEST, "no target"),
        ("jobIds=a&queue=q", StatusCode::BAD_REQUEST, "both targets"),
        (
            "jobIds=a&bogus=1",
            StatusCode::BAD_REQUEST,
            "an unknown key",
        ),
        (
            "queue=q&resumeCursor=nope",
            StatusCode::BAD_REQUEST,
            "a foreign cursor",
        ),
    ] {
        let response = harness.open(query, None).await;
        assert_eq!(response.status(), status, "{why}");
        assert_eq!(response.headers()["content-type"], "application/json");
        let body: Value = response.json().await.expect("a JSON body");
        assert_eq!(body["error"]["status"], "INVALID_ARGUMENT", "{why}");
    }

    let unauthenticated = harness
        .client
        .get(format!("{}/v1/jobs:watch?queue=q", harness.base))
        .send()
        .await
        .expect("the listener answers");
    assert_eq!(unauthenticated.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        unauthenticated.headers()["content-type"],
        "application/json"
    );
    harness.stop().await;
}

#[tokio::test]
async fn a_credential_over_its_cap_is_refused_before_the_stream() {
    let watch = WatchConfig {
        max_per_credential: 1,
        ..WatchConfig::default()
    };
    let harness = Harness::start("sse-quota", watch).await;
    let mut held = harness.watch("queue=q", None).await;
    held.expect().await;

    let refused = harness.open("queue=q", None).await;
    assert_eq!(refused.status(), StatusCode::TOO_MANY_REQUESTS);
    let body: Value = refused.json().await.expect("a JSON body");
    assert_eq!(body["error"]["details"][0]["reason"], reason::WATCH_LIMIT);
    harness.stop().await;
}

/// Failed after the stream opened: a final `error` event, then the close.
#[tokio::test]
async fn a_shutdown_ends_an_open_watch_with_an_error_event() {
    let harness = Harness::start("sse-shutdown", WatchConfig::default()).await;
    let mut events = harness.watch("queue=q", None).await;
    events.expect().await;

    harness.shutdown.trigger();
    let error = events.expect().await;
    assert_eq!(error.name, "error");
    assert_eq!(error.data["error"]["status"], "UNAVAILABLE");
    assert_eq!(
        error.data["error"]["details"][0]["reason"],
        reason::SHUTTING_DOWN
    );
    assert!(
        events.next().await.is_none(),
        "the stream closes after `error`"
    );
    harness.stop().await;
}
