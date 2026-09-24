//! End to end: the gRPC producer door's job events, delivered to a loopback
//! HTTP sink, and the hub's counters on the same listener's `/metrics`.
//!
//! The door owns two events — `job.enqueued` for a row it wrote, and
//! `job.cancelled` for a pending job it cancelled. Everything later is the
//! scheduler's, and is covered where the scheduler is.
#![cfg(all(feature = "grpc", feature = "events-http"))]

mod support;

use std::sync::Arc;
use std::time::Duration;

use flexiq_core::EventHub;
use flexiq_server::config::grpc::GrpcConfig;
use flexiq_server::config::listen::ListenAddress;
use flexiq_server::grpc::pb::producer_service_client::ProducerServiceClient;
use flexiq_server::grpc::pb::{enqueue_request, CancelJobRequest, EnqueueOptions, EnqueueRequest};
use flexiq_server::grpc::Listener;
use flexiq_server::runtime::shutdown::Shutdown;
use flexiq_server::tokens::ScopeSet;
use serde_json::Value;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;

use support::webhook_receiver::{Received, WebhookReceiver};
use support::{mint_token, temp_storage, temp_workflows, Bearer, TempStorage};

/// The one namespace this door serves.
const NAMESPACE: &str = "grpc-events-tests";

/// The sink's name, which is its metrics label.
const SINK: &str = "loopback";

/// A listener whose doors emit into a hub that posts to `receiver`.
struct Harness {
    client: ProducerServiceClient<InterceptedService<Channel, Bearer>>,
    receiver: WebhookReceiver,
    hub: Arc<EventHub>,
    base: String,
    token: String,
    _storage: TempStorage,
    shutdown: Shutdown,
    served: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl Harness {
    async fn start(label: &str) -> Self {
        let receiver = WebhookReceiver::start().await;
        // One event per request, so each POST body is exactly one event, and
        // payloads on, so the test can see the door attach the one it holds.
        let document = serde_json::json!({
            "sinks": [{
                "kind": "http", "name": SINK, "url": receiver.url,
                "allow": ["127.0.0.1"], "allow_loopback": true,
                "include_payload": true,
                "delivery": {"max_batch": 1}
            }]
        });
        let hub = Arc::new(EventHub::from_json(&document.to_string()).expect("the hub starts"));

        let storage = temp_storage(label);
        let token = mint_token(&storage, NAMESPACE, ScopeSet::ALL);
        let shutdown = Shutdown::default();
        let listener = Listener::bind(&GrpcConfig::new(
            ListenAddress::Tcp("127.0.0.1:0".parse().expect("valid address")),
            NAMESPACE,
        ))
        .await
        .expect("bind")
        .events(Arc::clone(&hub));
        let addr = listener
            .local_addr()
            .expect("a TCP listener knows what it bound");
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
            client: ProducerServiceClient::with_interceptor(channel, Bearer::new(&token)),
            receiver,
            hub,
            base: format!("http://{addr}"),
            token,
            _storage: storage,
            shutdown,
            served,
        }
    }

    async fn enqueue(&mut self, task: &str, options: EnqueueOptions) -> (String, bool) {
        let response = self
            .client
            .enqueue(EnqueueRequest {
                task_name: task.to_string(),
                body: Some(enqueue_request::Body::Raw(vec![7, 8, 9])),
                options: Some(options),
            })
            .await
            .expect("enqueue")
            .into_inner();
        let job = response.job.expect("the enqueued job");
        (job.id, response.deduplicated)
    }

    /// Wait until the receiver holds `count` events, and return them.
    async fn events(&self, count: usize) -> Vec<Value> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let received: Vec<Value> = self
                .receiver
                .received()
                .iter()
                .filter_map(|r: &Received| r.body.clone())
                .collect();
            if received.len() >= count {
                return received;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "expected {count} events, got {received:?}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Scrape until `series` appears, or give up and return the last body.
    ///
    /// Polled because a delivery counter moves once the sink reads the reply,
    /// a moment after the receiver has recorded the request.
    async fn scrape_until(&self, series: &str) -> String {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let body = reqwest::Client::new()
                .get(format!("{}/metrics", self.base))
                .bearer_auth(&self.token)
                .send()
                .await
                .expect("the listener must answer")
                .text()
                .await
                .expect("a body");
            if body.contains(series) || tokio::time::Instant::now() >= deadline {
                return body;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn stop(self) {
        self.shutdown.trigger();
        self.served
            .await
            .expect("the serve task must not panic")
            .expect("a shutdown is not an error");
        self.hub.shutdown(Duration::from_secs(1));
    }
}

fn keyed(key: &str) -> EnqueueOptions {
    EnqueueOptions {
        queue: "emails".to_string(),
        unique_key: Some(key.to_string()),
        ..Default::default()
    }
}

#[tokio::test]
async fn an_enqueue_arrives_as_a_cloudevent() {
    let mut harness = Harness::start("events-enqueued").await;
    let (job_id, _) = harness
        .enqueue(
            "send_email",
            EnqueueOptions {
                queue: "emails".to_string(),
                ..Default::default()
            },
        )
        .await;

    let events = harness.events(1).await;
    let event = &events[0];
    assert_eq!(event["specversion"], "1.0");
    assert_eq!(event["type"], "org.byteveda.flexiq.job.enqueued");
    assert_eq!(event["source"], "/flexiq");
    assert_eq!(event["id"], format!("{job_id}:0:-:job.enqueued"));
    assert_eq!(event["subject"], job_id.as_str());
    assert_eq!(event["flexiqnamespace"], NAMESPACE);
    assert_eq!(event["flexiqqueue"], "emails");
    assert_eq!(event["flexiqtask"], "send_email");
    assert_eq!(event["data"]["attempt"], 0);
    // [7, 8, 9], base64: the door attached the payload it already held.
    assert_eq!(event["data"]["payload_base64"], "BwgJ");
    let content_type = harness.receiver.received()[0]
        .header("content-type")
        .map(str::to_string);
    assert_eq!(
        content_type.as_deref(),
        Some("application/cloudevents+json")
    );

    harness.stop().await;
}

#[tokio::test]
async fn a_deduplicated_enqueue_emits_nothing() {
    let mut harness = Harness::start("events-dedupe").await;
    let (first, deduplicated) = harness.enqueue("send_email", keyed("once")).await;
    assert!(!deduplicated);
    let (second, deduplicated) = harness.enqueue("send_email", keyed("once")).await;
    assert!(deduplicated);
    assert_eq!(first, second);

    // A marker enqueue after the duplicate: once it has arrived, anything the
    // duplicate emitted would have arrived before it, on the one sink thread.
    let (marker, _) = harness.enqueue("marker", keyed("marker")).await;
    let events = harness.events(2).await;
    assert_eq!(events.len(), 2, "{events:?}");
    assert_eq!(events[0]["subject"], first.as_str());
    assert_eq!(events[1]["subject"], marker.as_str());

    harness.stop().await;
}

#[tokio::test]
async fn cancelling_a_pending_job_emits_cancelled_once() {
    let mut harness = Harness::start("events-cancelled").await;
    let (job_id, _) = harness.enqueue("send_email", keyed("to-cancel")).await;
    harness
        .client
        .cancel_job(CancelJobRequest {
            job_id: job_id.clone(),
        })
        .await
        .expect("cancel");
    // A second cancel finds the job terminal, so it emits nothing more.
    harness
        .client
        .cancel_job(CancelJobRequest {
            job_id: job_id.clone(),
        })
        .await
        .expect("cancel again");

    let (marker, _) = harness.enqueue("marker", keyed("marker")).await;

    let events = harness.events(3).await;
    assert_eq!(events.len(), 3, "{events:?}");
    assert_eq!(events[1]["type"], "org.byteveda.flexiq.job.cancelled");
    assert_eq!(events[1]["id"], format!("{job_id}:0:-:job.cancelled"));
    assert_eq!(events[2]["subject"], marker.as_str());

    harness.stop().await;
}

#[tokio::test]
async fn metrics_count_what_the_sink_delivered() {
    let mut harness = Harness::start("events-metrics").await;
    harness.enqueue("send_email", keyed("one")).await;
    harness.enqueue("send_email", keyed("two")).await;
    harness.events(2).await;

    let delivered = format!("flexiq_events_delivered_total{{sink=\"{SINK}\"}} 2");
    let body = harness.scrape_until(&delivered).await;
    assert!(body.contains(&delivered), "{body}");
    for reason in ["buffer_full", "rejected", "failed", "shutdown"] {
        let dropped =
            format!("flexiq_events_dropped_total{{sink=\"{SINK}\",reason=\"{reason}\"}} 0");
        assert!(body.contains(&dropped), "{body}");
    }
    assert!(body.contains("# TYPE flexiq_events_queued gauge"), "{body}");

    harness.stop().await;
}
