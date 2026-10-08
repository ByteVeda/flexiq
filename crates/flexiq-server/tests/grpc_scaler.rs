//! End-to-end: KEDA's external scaler on the gRPC listener (#850).
//!
//! KEDA sends no `authorization` header, so these dial the service the way it
//! does — token in `scalerMetadata` — and check the in-band credential holds
//! to what the header check would: scope, narrowed grants, namespace.
#![cfg(feature = "grpc")]

mod support;

use std::collections::HashMap;

use flexiq_core::storage::Storage;
use flexiq_core::{now_millis, NewJob};
use flexiq_server::config::grpc::GrpcConfig;
use flexiq_server::config::listen::ListenAddress;
use flexiq_server::grpc::pb::externalscaler::external_scaler_client::ExternalScalerClient;
use flexiq_server::grpc::pb::externalscaler::{GetMetricsRequest, ScaledObjectRef};
use flexiq_server::grpc::status::reason;
use flexiq_server::grpc::Listener;
use flexiq_server::runtime::shutdown::Shutdown;
use flexiq_server::tokens::{store, Grants, ScopeSet};
use tonic::transport::Channel;
use tonic::{Code, Request, Status};
use tonic_types::StatusExt;

use support::{mint_token, temp_storage, temp_workflows, TempStorage};

const NAMESPACE: &str = "grpc-scaler-tests";

struct Harness {
    channel: Channel,
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

    /// A whole `inspect` token.
    fn inspect(&self) -> String {
        self.token(&["inspect"])
    }

    /// A client sending no header, as KEDA does.
    fn client(&self) -> ExternalScalerClient<Channel> {
        ExternalScalerClient::new(self.channel.clone())
    }

    /// Seed one pending job on `queue` in `namespace`.
    fn enqueue(&self, queue: &str, namespace: &str) {
        self.storage
            .enqueue(job(queue, namespace))
            .expect("enqueue");
    }

    async fn stop(self) {
        self.shutdown.trigger();
        self.served
            .await
            .expect("the serve task must not panic")
            .expect("a shutdown is not an error");
    }
}

fn job(queue: &str, namespace: &str) -> NewJob {
    NewJob {
        queue: queue.to_string(),
        task_name: "send_email".to_string(),
        payload: vec![1],
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
        namespace: Some(namespace.to_string()),
        debounce_key: None,
        enqueued_by: None,
    }
}

/// A scaled object whose trigger metadata is `pairs`.
fn object(pairs: &[(&str, &str)]) -> ScaledObjectRef {
    ScaledObjectRef {
        name: "worker".into(),
        namespace: "default".into(),
        scaler_metadata: pairs
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect::<HashMap<_, _>>(),
    }
}

fn metrics_request(object: ScaledObjectRef) -> GetMetricsRequest {
    GetMetricsRequest {
        metric_name: "ignored".into(),
        scaled_object_ref: Some(object),
    }
}

fn refusal_reason(status: &Status) -> String {
    status
        .get_error_details()
        .error_info()
        .map(|info| info.reason.clone())
        .unwrap_or_default()
}

#[tokio::test]
async fn a_metadata_token_reads_the_metric() {
    let harness = Harness::start("grpc-scaler-token").await;
    let token = harness.inspect();
    harness.enqueue("emails", NAMESPACE);
    harness.enqueue("emails", NAMESPACE);

    let values = harness
        .client()
        .get_metrics(metrics_request(object(&[
            ("token", &token),
            ("queue", "emails"),
        ])))
        .await
        .expect("an inspect token reads the metric")
        .into_inner()
        .metric_values;
    assert_eq!(values.len(), 1);
    assert_eq!(values[0].metric_name, "flexiq-emails");
    assert_eq!(values[0].metric_value, 2);
    assert_eq!(values[0].metric_value_float, 2.0);
    harness.stop().await;
}

/// KEDA resolves `tokenFromEnv` and sends the value under that same key.
#[tokio::test]
async fn a_token_from_env_is_accepted() {
    let harness = Harness::start("grpc-scaler-from-env").await;
    let token = harness.inspect();
    let active = harness
        .client()
        .is_active(object(&[("tokenFromEnv", &token)]))
        .await
        .expect("a resolved tokenFromEnv authenticates")
        .into_inner();
    assert!(!active.result);
    harness.stop().await;
}

/// A caller that can send a header may, and the header wins over metadata.
#[tokio::test]
async fn a_header_token_is_accepted_and_wins() {
    let harness = Harness::start("grpc-scaler-header").await;
    let token = harness.inspect();

    let mut request = Request::new(object(&[("token", "not-a-token")]));
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {token}").parse().expect("ASCII"),
    );
    harness
        .client()
        .is_active(request)
        .await
        .expect("the header token is the one checked");

    let mut request = Request::new(object(&[("token", &token)]));
    request
        .metadata_mut()
        .insert("authorization", "Bearer wrong".parse().expect("ASCII"));
    let status = harness
        .client()
        .is_active(request)
        .await
        .expect_err("a bad header is not rescued by good metadata");
    assert_eq!(status.code(), Code::Unauthenticated);
    harness.stop().await;
}

#[tokio::test]
async fn no_token_is_unauthenticated() {
    let harness = Harness::start("grpc-scaler-none").await;
    for metadata in [vec![], vec![("token", "")], vec![("token", "garbage")]] {
        let status = harness
            .client()
            .get_metric_spec(object(&metadata))
            .await
            .expect_err("no usable token");
        assert_eq!(status.code(), Code::Unauthenticated, "{metadata:?}");
        assert_eq!(refusal_reason(&status), reason::UNAUTHENTICATED);
    }
    harness.stop().await;
}

#[tokio::test]
async fn a_revoked_token_is_refused() {
    let harness = Harness::start("grpc-scaler-revoked").await;
    let request = flexiq_server::tokens::NewToken::new(
        "keda",
        ScopeSet::of(&[flexiq_server::tokens::Scope::Inspect]),
        NAMESPACE,
        None,
        None,
    )
    .expect("a valid mint request");
    let (minted, token) = store::create(&*harness.storage, request).expect("mint");
    let ask = || object(&[("token", token.as_str())]);
    harness.client().is_active(ask()).await.expect("live token");

    assert!(store::revoke(&*harness.storage, &minted.id, Some(NAMESPACE)).expect("revoke"));
    let status = harness
        .client()
        .is_active(ask())
        .await
        .expect_err("revoked");
    assert_eq!(status.code(), Code::Unauthenticated);
    harness.stop().await;
}

#[tokio::test]
async fn a_token_without_inspect_is_denied() {
    let harness = Harness::start("grpc-scaler-scope").await;
    let token = harness.token(&["produce", "read"]);
    let status = harness
        .client()
        .is_active(object(&[("token", &token)]))
        .await
        .expect_err("produce and read do not open the scaler");
    assert_eq!(status.code(), Code::PermissionDenied);
    assert_eq!(refusal_reason(&status), reason::SCOPE_DENIED);
    harness.stop().await;
}

#[tokio::test]
async fn a_narrowed_grant_reaches_only_its_queues() {
    let harness = Harness::start("grpc-scaler-narrowed").await;
    let token = harness.token(&["inspect:queue=emails"]);

    harness
        .client()
        .is_active(object(&[("token", &token), ("queue", "emails")]))
        .await
        .expect("its own queue");

    for metadata in [
        vec![("token", token.as_str()), ("queue", "billing")],
        // The whole namespace spans queues the grant does not reach.
        vec![("token", token.as_str())],
    ] {
        let status = harness
            .client()
            .get_metrics(metrics_request(object(&metadata)))
            .await
            .expect_err("beyond the grant");
        assert_eq!(status.code(), Code::PermissionDenied, "{metadata:?}");
        assert_eq!(refusal_reason(&status), reason::SCOPE_DENIED);
    }

    // Counts span every task, so a task-narrowed grant reaches no queue whole.
    let task_only = harness.token(&["inspect:queue=emails,task=send_email"]);
    let status = harness
        .client()
        .is_active(object(&[("token", &task_only), ("queue", "emails")]))
        .await
        .expect_err("a task-narrowed grant");
    assert_eq!(status.code(), Code::PermissionDenied);
    harness.stop().await;
}

#[tokio::test]
async fn is_active_follows_pending_and_running_jobs() {
    let harness = Harness::start("grpc-scaler-active").await;
    let token = harness.inspect();
    let ask = |extra: &[(&str, &str)]| {
        let mut pairs = vec![("token", token.as_str()), ("queue", "emails")];
        pairs.extend_from_slice(extra);
        object(&pairs)
    };
    let active = |object| {
        let mut client = harness.client();
        async move {
            client
                .is_active(object)
                .await
                .expect("is_active")
                .into_inner()
                .result
        }
    };

    assert!(!active(ask(&[])).await, "an empty queue is idle");
    harness.enqueue("emails", NAMESPACE);
    assert!(active(ask(&[])).await, "a pending job activates");
    assert!(
        !active(ask(&[("activationQueueDepth", "1")])).await,
        "at the activation depth it stays idle"
    );

    // Claimed: nothing pending, one running — still active, so KEDA does not
    // scale away the worker running it.
    harness
        .storage
        .dequeue("emails", now_millis(), Some(NAMESPACE))
        .expect("dequeue")
        .expect("a job to claim");
    assert!(active(ask(&[])).await, "a running job keeps it active");
    let value = harness
        .client()
        .get_metrics(metrics_request(ask(&[])))
        .await
        .expect("metrics")
        .into_inner()
        .metric_values[0]
        .metric_value;
    assert_eq!(value, 0, "the metric is pending jobs only");
    harness.stop().await;
}

#[tokio::test]
async fn the_metric_spec_carries_the_target() {
    let harness = Harness::start("grpc-scaler-spec").await;
    let token = harness.inspect();

    let spec = harness
        .client()
        .get_metric_spec(object(&[("token", &token)]))
        .await
        .expect("spec")
        .into_inner()
        .metric_specs;
    assert_eq!(spec.len(), 1);
    assert_eq!(spec[0].metric_name, "flexiq-all");
    assert_eq!(spec[0].target_size, 10);
    assert_eq!(spec[0].target_size_float, 10.0);

    let spec = harness
        .client()
        .get_metric_spec(object(&[
            ("token", &token),
            ("queue", "Emails_EU"),
            ("targetQueueDepth", "25"),
        ]))
        .await
        .expect("spec")
        .into_inner()
        .metric_specs;
    assert_eq!(spec[0].metric_name, "flexiq-emails-eu");
    assert_eq!(spec[0].target_size, 25);
    harness.stop().await;
}

#[tokio::test]
async fn a_bad_target_is_an_invalid_argument() {
    let harness = Harness::start("grpc-scaler-invalid").await;
    let token = harness.inspect();
    let status = harness
        .client()
        .get_metric_spec(object(&[("token", &token), ("targetQueueDepth", "0")]))
        .await
        .expect_err("a zero target");
    assert_eq!(status.code(), Code::InvalidArgument);
    assert!(status.message().contains("targetQueueDepth"), "{status:?}");
    assert_eq!(refusal_reason(&status), reason::INVALID_REQUEST);
    harness.stop().await;
}

/// The namespace is the token's; another tenant's jobs are not counted.
#[tokio::test]
async fn another_namespace_is_not_counted() {
    let harness = Harness::start("grpc-scaler-namespace").await;
    let token = harness.inspect();
    harness.enqueue("emails", "someone-else");
    harness.enqueue("emails", NAMESPACE);

    for metadata in [
        vec![("token", token.as_str()), ("queue", "emails")],
        vec![("token", token.as_str())],
    ] {
        let value = harness
            .client()
            .get_metrics(metrics_request(object(&metadata)))
            .await
            .expect("metrics")
            .into_inner()
            .metric_values[0]
            .metric_value;
        assert_eq!(value, 1, "{metadata:?}");
    }
    harness.stop().await;
}

/// The stream is not served yet; KEDA falls back to polling.
#[tokio::test]
async fn the_streams_are_unimplemented() {
    let harness = Harness::start("grpc-scaler-streams").await;
    let token = harness.inspect();
    let status = harness
        .client()
        .stream_metric_spec(object(&[("token", &token)]))
        .await
        .expect_err("not served");
    assert_eq!(status.code(), Code::Unimplemented);
    harness.stop().await;
}
