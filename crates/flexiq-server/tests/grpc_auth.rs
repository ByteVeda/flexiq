//! End-to-end: the gRPC door's credential, over a real socket.
//!
//! What these pin is the property the design doc asks #716 for — the check is
//! *one interceptor*, not a check per RPC. So the assertions are deliberately
//! not "the six producer RPCs each refuse an anonymous caller": they are that a
//! path the router does not implement is refused too, that reflection is
//! refused, and that health is not. A per-RPC check cannot produce those
//! answers, so a regression to one fails here rather than at the first RPC
//! somebody adds.
#![cfg(feature = "grpc")]

mod support;

use flexiq_core::storage::Storage;
use flexiq_core::Secret;
use flexiq_server::config::grpc::GrpcConfig;
use flexiq_server::config::listen::ListenAddress;
use flexiq_server::grpc::pb::producer_service_client::ProducerServiceClient;
use flexiq_server::grpc::pb::{enqueue_request, EnqueueOptions, EnqueueRequest};
use flexiq_server::grpc::status::reason;
use flexiq_server::grpc::Listener;
use flexiq_server::runtime::shutdown::Shutdown;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tonic::{Code, Request, Status};
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_client::HealthClient;
use tonic_health::pb::HealthCheckRequest;
use tonic_reflection::pb::v1::server_reflection_client::ServerReflectionClient;
use tonic_reflection::pb::v1::server_reflection_request::MessageRequest;
use tonic_reflection::pb::v1::ServerReflectionRequest;
use tonic_types::StatusExt;

use support::{temp_storage, TempStorage};

/// The one namespace this door serves.
const NAMESPACE: &str = "grpc-auth-tests";
/// The configured secret. Sixteen characters is the floor `config::grpc`
/// enforces, so a shorter one would never reach a listener.
const TOKEN: &str = "0123456789abcdef";

/// A running listener and the channel to reach it.
struct Harness {
    channel: Channel,
    storage: TempStorage,
    shutdown: Shutdown,
    served: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl Harness {
    /// Start a door with `token` as its credential — `None` for the loopback
    /// door that asks for none.
    async fn start(label: &str, token: Option<&str>) -> Self {
        let storage = temp_storage(label);
        let shutdown = Shutdown::default();
        let listener = Listener::bind(&GrpcConfig {
            listen: ListenAddress::Tcp("127.0.0.1:0".parse().expect("valid address")),
            namespace: NAMESPACE.to_string(),
            token: token.map(Secret::new),
        })
        .await
        .expect("bind");
        let url = format!(
            "http://{}",
            listener
                .local_addr()
                .expect("a TCP listener knows what it bound")
        );

        let served = tokio::spawn(listener.serve((*storage).clone(), shutdown.clone()));

        let channel = Channel::from_shared(url)
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

    fn producer(&self) -> ProducerServiceClient<Channel> {
        ProducerServiceClient::new(self.channel.clone())
    }

    async fn stop(self) {
        self.shutdown.trigger();
        self.served
            .await
            .expect("the serve task must not panic")
            .expect("a shutdown is not an error");
    }
}

/// A minimal enqueue, optionally carrying a credential.
fn enqueue(credential: Option<&str>) -> Request<EnqueueRequest> {
    let mut request = Request::new(EnqueueRequest {
        task_name: "send_email".to_string(),
        body: Some(enqueue_request::Body::Raw(vec![1, 2, 3])),
        options: Some(EnqueueOptions {
            queue: "emails".to_string(),
            ..Default::default()
        }),
    });
    if let Some(credential) = credential {
        request.metadata_mut().insert(
            "authorization",
            MetadataValue::try_from(format!("Bearer {credential}")).expect("an ASCII header"),
        );
    }
    request
}

/// The `ErrorInfo.reason` a refusal carries, which is what a client branches on.
fn refusal_reason(status: &Status) -> String {
    status
        .get_error_details()
        .error_info()
        .expect("every error this door produces carries an ErrorInfo")
        .reason
        .clone()
}

#[tokio::test]
async fn a_call_with_no_credential_is_refused() {
    let harness = Harness::start("grpc-auth-missing", Some(TOKEN)).await;

    let status = harness
        .producer()
        .enqueue(enqueue(None))
        .await
        .expect_err("an anonymous enqueue must be refused");
    assert_eq!(status.code(), Code::Unauthenticated);
    assert_eq!(refusal_reason(&status), reason::UNAUTHENTICATED);

    // And nothing was written on the way to being refused.
    assert_eq!(
        harness
            .storage
            .stats(Some(NAMESPACE))
            .expect("stats")
            .pending,
        0
    );

    harness.stop().await;
}

/// The acceptance criterion: a wrong token and a missing one are one answer.
#[tokio::test]
async fn a_wrong_credential_is_indistinguishable_from_none() {
    let harness = Harness::start("grpc-auth-wrong", Some(TOKEN)).await;

    let missing = harness
        .producer()
        .enqueue(enqueue(None))
        .await
        .expect_err("refused");
    let wrong = harness
        .producer()
        .enqueue(enqueue(Some("0000000000000000")))
        .await
        .expect_err("refused");
    // A prefix of the real token, which a short-circuiting compare would leak
    // the length of through timing and a sloppy one might accept outright.
    let prefix = harness
        .producer()
        .enqueue(enqueue(Some("0123456789abcde")))
        .await
        .expect_err("refused");

    for status in [&wrong, &prefix] {
        assert_eq!(status.code(), missing.code());
        assert_eq!(status.message(), missing.message());
        assert_eq!(refusal_reason(status), refusal_reason(&missing));
    }

    harness.stop().await;
}

#[tokio::test]
async fn the_configured_credential_is_accepted_and_scopes_the_write() {
    let harness = Harness::start("grpc-auth-accepted", Some(TOKEN)).await;

    let job = harness
        .producer()
        .enqueue(enqueue(Some(TOKEN)))
        .await
        .expect("the right credential must be accepted")
        .into_inner()
        .job
        .expect("a response carries its job");
    // The namespace came off the principal, not off a request field: nothing in
    // the request above names one.
    assert_eq!(job.namespace, NAMESPACE);
    assert!(harness
        .storage
        .get_job(&job.id, Some(NAMESPACE))
        .expect("read")
        .is_some());

    harness.stop().await;
}

/// Health is the one public path, because a kubelet `grpc:` probe cannot carry
/// a credential and gating it would cost the chart its readiness probe.
#[tokio::test]
async fn health_answers_without_a_credential() {
    let harness = Harness::start("grpc-auth-health", Some(TOKEN)).await;

    let status = HealthClient::new(harness.channel.clone())
        .check(HealthCheckRequest {
            service: String::new(),
        })
        .await
        .expect("health must answer an anonymous probe")
        .into_inner()
        .status;
    assert_eq!(status, ServingStatus::Serving as i32);

    harness.stop().await;
}

/// Reflection describes the door, so it sits behind the credential: the
/// allowlist is "public iff an unauthenticated caller must reach it", and
/// nothing must reach reflection.
#[tokio::test]
async fn reflection_is_behind_the_credential() {
    let harness = Harness::start("grpc-auth-reflection", Some(TOKEN)).await;

    let outbound = tokio_stream::iter(vec![ServerReflectionRequest {
        host: String::new(),
        message_request: Some(MessageRequest::ListServices(String::new())),
    }]);
    // A streaming call may open before the server answers, so the refusal
    // arrives either at the call or on the first read of the stream.
    let status = match ServerReflectionClient::new(harness.channel.clone())
        .server_reflection_info(outbound)
        .await
    {
        Err(status) => status,
        Ok(response) => response
            .into_inner()
            .message()
            .await
            .expect_err("an anonymous reflection call must be refused"),
    };
    assert_eq!(status.code(), Code::Unauthenticated);
    assert_eq!(refusal_reason(&status), reason::UNAUTHENTICATED);

    harness.stop().await;
}

/// The proof that the check is one interceptor and not one per RPC: a path the
/// router does not implement is refused *before* it is routed. A per-handler
/// check would answer `UNIMPLEMENTED` here and tell an anonymous caller which
/// services this build carries.
#[tokio::test]
async fn an_unrouted_path_is_refused_rather_than_reported_missing() {
    let harness = Harness::start("grpc-auth-unrouted", Some(TOKEN)).await;

    let mut client = tonic::client::Grpc::new(harness.channel.clone());
    client.ready().await.expect("the channel must be ready");
    let status = client
        .unary::<EnqueueRequest, EnqueueRequest, _>(
            enqueue(None),
            http::uri::PathAndQuery::from_static("/flexiq.v1.NoSuchService/NoSuchMethod"),
            tonic_prost::ProstCodec::default(),
        )
        .await
        .expect_err("an anonymous call must be refused whatever it names");
    assert_eq!(status.code(), Code::Unauthenticated);
    assert_eq!(refusal_reason(&status), reason::UNAUTHENTICATED);

    // With the credential the same path reaches the router and is answered for
    // what it actually is — so the refusal above was the gate, not the route.
    let mut client = tonic::client::Grpc::new(harness.channel.clone());
    client.ready().await.expect("the channel must be ready");
    let status = client
        .unary::<EnqueueRequest, EnqueueRequest, _>(
            enqueue(Some(TOKEN)),
            http::uri::PathAndQuery::from_static("/flexiq.v1.NoSuchService/NoSuchMethod"),
            tonic_prost::ProstCodec::default(),
        )
        .await
        .expect_err("there is no such method");
    assert_eq!(status.code(), Code::Unimplemented);

    harness.stop().await;
}

/// The loopback door with no `FLEXIQ_GRPC_TOKEN`: still one principal, still
/// one namespace, still every storage call scoped. The boundary is the network
/// stack rather than a credential, and nothing above the authenticator knows
/// the difference.
#[tokio::test]
async fn an_unconfigured_door_still_scopes_every_call() {
    let harness = Harness::start("grpc-auth-anonymous", None).await;

    let job = harness
        .producer()
        .enqueue(enqueue(None))
        .await
        .expect("a door with no credential configured accepts an anonymous call")
        .into_inner()
        .job
        .expect("a response carries its job");
    assert_eq!(job.namespace, NAMESPACE);

    // A credential nobody asked for is ignored rather than refused: there is
    // nothing to compare it against.
    assert!(harness
        .producer()
        .enqueue(enqueue(Some("whatever")))
        .await
        .is_ok());

    harness.stop().await;
}

/// The listener's own guard, not the config's: a bind that got past
/// `config::grpc` with no token must still not serve a reachable address.
#[tokio::test]
async fn a_non_loopback_bind_with_no_token_refuses_to_serve() {
    let refused = Listener::bind(&GrpcConfig {
        listen: ListenAddress::Tcp("0.0.0.0:0".parse().expect("valid address")),
        namespace: NAMESPACE.to_string(),
        token: None,
    })
    .await;
    // `Listener` is not `Debug` — a bound socket has nothing worth printing —
    // so the error comes out by hand rather than through `expect_err`.
    let Err(error) = refused else {
        panic!("an unauthenticated public bind must be refused");
    };
    let message = error.to_string();
    assert!(
        message.contains("FLEXIQ_GRPC_TOKEN"),
        "the message must name the variable that fixes it: {message}"
    );
}

/// And the other half: with a token, the same bind is allowed.
#[tokio::test]
async fn a_non_loopback_bind_with_a_token_is_allowed() {
    let storage = temp_storage("grpc-auth-public-bind");
    let shutdown = Shutdown::default();
    let listener = Listener::bind(&GrpcConfig {
        listen: ListenAddress::Tcp("0.0.0.0:0".parse().expect("valid address")),
        namespace: NAMESPACE.to_string(),
        token: Some(Secret::new(TOKEN)),
    })
    .await
    .expect("a credentialled public bind is allowed");
    let addr = listener
        .local_addr()
        .expect("a TCP listener knows what it bound");
    let served = tokio::spawn(listener.serve((*storage).clone(), shutdown.clone()));

    let channel = Channel::from_shared(format!("http://127.0.0.1:{}", addr.port()))
        .expect("a valid endpoint")
        .connect()
        .await
        .expect("the listener must accept a connection");
    assert!(ProducerServiceClient::new(channel)
        .enqueue(enqueue(Some(TOKEN)))
        .await
        .is_ok());

    shutdown.trigger();
    served
        .await
        .expect("the serve task must not panic")
        .expect("a shutdown is not an error");
}
