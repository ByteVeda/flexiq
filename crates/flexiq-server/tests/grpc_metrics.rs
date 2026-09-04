//! End to end: `/metrics` on the gRPC listener.
//!
//! The reason this door exists is a deployment with `grpc.enabled=true` and
//! `dashboard.enabled=false` — a combination the Helm chart offers and CI
//! renders — which had no scrape target at all. So the harness here starts the
//! gRPC listener and nothing else, exactly as that release does, and asserts a
//! scraper can reach it and that the numbers move.
#![cfg(feature = "grpc")]

mod support;

use flexiq_core::job::{now_millis, NewJob};
use flexiq_core::Storage;
use flexiq_server::config::grpc::GrpcConfig;
use flexiq_server::config::listen::ListenAddress;
use flexiq_server::grpc::Listener;
use flexiq_server::runtime::shutdown::Shutdown;
use flexiq_server::tokens::{Scope, ScopeSet};
use reqwest::StatusCode;

use support::{mint_token, temp_storage, temp_workflows, TempStorage};

/// The one namespace this door serves.
const NAMESPACE: &str = "grpc-metrics-tests";

/// A running listener with a scrape client pointed at it.
struct Harness {
    base: String,
    token: String,
    client: reqwest::Client,
    storage: TempStorage,
    shutdown: Shutdown,
    served: tokio::task::JoinHandle<anyhow::Result<()>>,
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
        // No dashboard, no attach listener, no executor door: the gRPC-only
        // release this route was added for.
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

    /// Scrape with the harness token.
    async fn scrape(&self) -> (StatusCode, String) {
        self.send(
            self.client
                .get(format!("{}/metrics", self.base))
                .bearer_auth(&self.token),
        )
        .await
    }

    async fn send(&self, request: reqwest::RequestBuilder) -> (StatusCode, String) {
        let response = request.send().await.expect("the listener must answer");
        let status = response.status();
        (status, response.text().await.expect("a body"))
    }

    async fn stop(self) {
        self.shutdown.trigger();
        self.served
            .await
            .expect("the serve task must not panic")
            .expect("a shutdown is not an error");
    }
}

/// One `flexiq_grpc_requests_total` series' value, or zero when it is absent.
fn counter(body: &str, method: &str, door: &str, code: &str) -> u64 {
    let needle = format!(
        "flexiq_grpc_requests_total{{method=\"{method}\",door=\"{door}\",code=\"{code}\"}} "
    );
    body.lines()
        .find_map(|line| line.strip_prefix(&needle))
        .map_or(0, |value| value.trim().parse().expect("a counter value"))
}

#[tokio::test]
async fn a_grpc_only_listener_serves_a_scrapeable_exposition() {
    let harness = Harness::start("metrics-scrape").await;

    harness
        .storage
        .enqueue(NewJob {
            queue: "emails".to_string(),
            task_name: "send".to_string(),
            payload: vec![],
            priority: 0,
            scheduled_at: now_millis(),
            max_retries: 0,
            timeout_ms: 1_000,
            unique_key: None,
            metadata: None,
            notes: None,
            depends_on: vec![],
            expires_at: None,
            result_ttl_ms: None,
            namespace: Some(NAMESPACE.to_string()),
            debounce_key: None,
        })
        .expect("enqueue");

    let (status, body) = harness.scrape().await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body.contains("flexiq_jobs{queue=\"emails\",status=\"pending\"} 1"),
        "unexpected exposition:\n{body}"
    );
    assert!(body.contains("# TYPE flexiq_workers gauge"), "{body}");
    assert!(
        body.contains("# TYPE flexiq_grpc_requests_total counter"),
        "{body}"
    );
    // Nothing attaches executors here, so those gauges say nothing rather than
    // reporting a zero that reads as "none attached".
    assert!(!body.contains("flexiq_executors"), "{body}");

    harness.stop().await;
}

#[tokio::test]
async fn the_scrape_is_credentialled() {
    let harness = Harness::start("metrics-credential").await;

    let (status, body) = harness
        .send(harness.client.get(format!("{}/metrics", harness.base)))
        .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    // Rendered for the door it arrived at: a scraper gets the JSON error shape,
    // not gRPC trailers.
    assert!(body.contains("UNAUTHENTICATED"), "unexpected body: {body}");

    harness.stop().await;
}

/// A scrape is not a produce and not an execute, so either scope opens it.
#[tokio::test]
async fn either_scope_can_scrape() {
    for scope in [Scope::Produce, Scope::Execute] {
        let harness =
            Harness::start_with_scopes(&format!("metrics-scope-{scope}"), ScopeSet::of(&[scope]))
                .await;
        let (status, _) = harness.scrape().await;
        assert_eq!(status, StatusCode::OK, "scope {scope} could not scrape");
        harness.stop().await;
    }
}

/// The facade and the gRPC door are one service, so a call over either is
/// counted under the RPC it reached — and a job id never becomes a series.
#[tokio::test]
async fn a_facade_call_is_counted_under_its_rpc() {
    let harness = Harness::start("metrics-facade-call").await;

    let (status, _) = harness
        .send(
            harness
                .client
                .get(format!("{}/v1/jobs/does-not-exist", harness.base))
                .bearer_auth(&harness.token),
        )
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (_, body) = harness.scrape().await;
    assert_eq!(
        counter(
            &body,
            "flexiq.v1.ProducerService/GetJob",
            "http",
            "NOT_FOUND"
        ),
        1,
        "unexpected exposition:\n{body}"
    );
    assert!(
        !body.contains("does-not-exist"),
        "a job id must not reach a label:\n{body}"
    );
    assert!(
        body.contains(
            "flexiq_grpc_request_duration_seconds_count{method=\"flexiq.v1.ProducerService/\
             GetJob\",door=\"http\"} 1"
        ),
        "unexpected exposition:\n{body}"
    );

    harness.stop().await;
}

/// The layer sits outside the auth layer, which is the only way a refusal is
/// counted at all — and a client failing every call for want of a credential is
/// exactly what an operator scrapes to find out.
#[tokio::test]
async fn a_refused_call_is_counted() {
    let harness = Harness::start("metrics-refusal").await;

    let (status, _) = harness
        .send(harness.client.get(format!("{}/v1/jobs", harness.base)))
        .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    let (_, body) = harness.scrape().await;
    assert_eq!(
        counter(
            &body,
            "flexiq.v1.ProducerService/ListJobs",
            "http",
            "UNAUTHENTICATED"
        ),
        1,
        "unexpected exposition:\n{body}"
    );

    harness.stop().await;
}

/// An unrouted path is answered, and counted, without minting a series for
/// whatever the caller typed.
#[tokio::test]
async fn an_unrouted_path_cannot_mint_a_series() {
    let harness = Harness::start("metrics-unrouted").await;

    let (status, _) = harness
        .send(
            harness
                .client
                .get(format!("{}/nope/whatever", harness.base))
                .bearer_auth(&harness.token),
        )
        .await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);

    let (_, body) = harness.scrape().await;
    assert_eq!(
        counter(&body, "other", "http", "UNIMPLEMENTED"),
        1,
        "{body}"
    );
    assert!(!body.contains("whatever"), "{body}");

    harness.stop().await;
}
