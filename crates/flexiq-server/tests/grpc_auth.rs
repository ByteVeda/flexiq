//! End-to-end: the gRPC door's credential, over a real socket.
//!
//! Two things are pinned here that a unit test cannot pin.
//!
//! The first is that the check is *one interceptor*, not a check per RPC. So
//! the assertions are deliberately not "the six producer RPCs each refuse an
//! anonymous caller": they are that a path the router does not implement is
//! refused too, that reflection is refused, and that health is not. A per-RPC
//! check cannot produce those answers, so a regression to one fails here rather
//! than at the first RPC somebody adds.
//!
//! The second is #717's three acceptance criteria, which are about a credential
//! changing underneath a *running* server: a revoked token stops working with no
//! restart, a `produce` token cannot reach the executor package, and a token
//! bound to one namespace is not believed by a listener serving another.
#![cfg(feature = "grpc")]

mod support;

use flexiq_core::storage::Storage;
use flexiq_server::config::grpc::GrpcConfig;
use flexiq_server::config::listen::ListenAddress;
use flexiq_server::grpc::pb::producer_service_client::ProducerServiceClient;
use flexiq_server::grpc::pb::{enqueue_request, EnqueueOptions, EnqueueRequest};
use flexiq_server::grpc::status::reason;
use flexiq_server::grpc::Listener;
use flexiq_server::runtime::shutdown::Shutdown;
use flexiq_server::tokens::{store, Scope, ScopeSet};
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

/// A running listener and the channel to reach it.
struct Harness {
    channel: Channel,
    storage: TempStorage,
    shutdown: Shutdown,
    served: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl Harness {
    /// Start a door on loopback with an empty token store.
    async fn start(label: &str) -> Self {
        Self::start_on(label, "127.0.0.1:0").await
    }

    /// Start a door bound to `spec`.
    async fn start_on(label: &str, spec: &str) -> Self {
        let storage = temp_storage(label);
        let shutdown = Shutdown::default();
        let listener = Listener::bind(&GrpcConfig {
            listen: ListenAddress::Tcp(spec.parse().expect("valid address")),
            namespace: NAMESPACE.to_string(),
        })
        .await
        .expect("bind");
        let addr = listener
            .local_addr()
            .expect("a TCP listener knows what it bound");

        let served = tokio::spawn(listener.serve((*storage).clone(), shutdown.clone()));

        // Dial loopback even for a wildcard bind: the port is what matters.
        let channel = Channel::from_shared(format!("http://127.0.0.1:{}", addr.port()))
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

    /// Mint a token into the store this door reads, returning `(id, token)`.
    fn mint(&self, namespace: &str, scopes: ScopeSet) -> (String, String) {
        let request = flexiq_server::tokens::NewToken::new("client", scopes, namespace, None, None)
            .expect("a valid mint request");
        let (row, plaintext) = store::create(&*self.storage, request).expect("mint");
        (row.id, plaintext)
    }

    /// A token good for everything this door serves.
    fn mint_default(&self) -> String {
        self.mint(NAMESPACE, ScopeSet::ALL).1
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

/// Call a path the router does not implement, so the answer is the gate's and
/// not a handler's.
async fn call_path(harness: &Harness, path: &'static str, credential: Option<&str>) -> Status {
    let mut client = tonic::client::Grpc::new(harness.channel.clone());
    client.ready().await.expect("the channel must be ready");
    client
        .unary::<EnqueueRequest, EnqueueRequest, _>(
            enqueue(credential),
            http::uri::PathAndQuery::from_static(path),
            tonic_prost::ProstCodec::default(),
        )
        .await
        .expect_err("no such method exists; the interesting part is which refusal")
}

#[tokio::test]
async fn a_call_with_no_credential_is_refused() {
    let harness = Harness::start("grpc-auth-missing").await;
    harness.mint_default();

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

/// A door with nothing minted serves nobody — it does not fall back to letting
/// callers through. #716 could not assert this, because it had an anonymous
/// authenticator behind the missing secret.
#[tokio::test]
async fn a_door_with_no_tokens_authenticates_nobody() {
    let harness = Harness::start("grpc-auth-empty").await;

    for credential in [None, Some("fqt_0123456789abcdef.anything")] {
        let status = harness
            .producer()
            .enqueue(enqueue(credential))
            .await
            .expect_err("an unprovisioned door serves nothing");
        assert_eq!(status.code(), Code::Unauthenticated);
    }

    harness.stop().await;
}

/// A wrong token, a well-formed token for an id that does not exist, and no
/// token at all are one answer. Each distinction is a step in guessing.
#[tokio::test]
async fn every_wrong_credential_is_indistinguishable_from_none() {
    let harness = Harness::start("grpc-auth-wrong").await;
    let (id, token) = harness.mint(NAMESPACE, ScopeSet::ALL);
    let secret = token.split_once('.').expect("separated").1;

    let missing = harness
        .producer()
        .enqueue(enqueue(None))
        .await
        .expect_err("refused");

    // Labelled rather than interpolated: two of these are built from the live
    // secret, and an assert message ends up in a CI log.
    for (label, wrong) in [
        (
            "a well-formed token for an id that was never minted",
            "fqt_ffffffffffffffff.whatever".to_string(),
        ),
        ("the right id, the wrong secret", format!("fqt_{id}.wrong")),
        (
            // A prefix of the real secret, which a short-circuiting compare
            // would leak the length of through timing and a sloppy one might
            // accept.
            "a prefix of the real secret",
            format!("fqt_{id}.{}", &secret[..secret.len() - 1]),
        ),
        (
            "the right secret under an id that is not its own",
            format!("fqt_ffffffffffffffff.{secret}"),
        ),
        (
            "not one of our tokens at all",
            "some-other-credential".to_string(),
        ),
    ] {
        let status = harness
            .producer()
            .enqueue(enqueue(Some(&wrong)))
            .await
            .expect_err("refused");
        assert_eq!(status.code(), missing.code(), "{label}");
        assert_eq!(status.message(), missing.message(), "{label}");
        assert_eq!(refusal_reason(&status), refusal_reason(&missing));
    }

    harness.stop().await;
}

#[tokio::test]
async fn a_stored_token_is_accepted_and_scopes_the_write() {
    let harness = Harness::start("grpc-auth-accepted").await;
    let token = harness.mint_default();

    let job = harness
        .producer()
        .enqueue(enqueue(Some(&token)))
        .await
        .expect("a minted credential must be accepted")
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

/// #717's first acceptance criterion, and the reason there is no cache: the
/// revocation is a write by another process, and the *same* channel to the
/// *same* running server must honour it on the next call.
#[tokio::test]
async fn a_revoked_token_fails_on_the_next_rpc_with_no_restart() {
    let harness = Harness::start("grpc-auth-revoked").await;
    let (id, token) = harness.mint(NAMESPACE, ScopeSet::ALL);

    assert!(
        harness
            .producer()
            .enqueue(enqueue(Some(&token)))
            .await
            .is_ok(),
        "the token works before it is revoked"
    );

    assert!(store::revoke(&*harness.storage, &id, Some(NAMESPACE)).expect("revoke"));

    let status = harness
        .producer()
        .enqueue(enqueue(Some(&token)))
        .await
        .expect_err("the next call on the same channel must be refused");
    assert_eq!(status.code(), Code::Unauthenticated);
    assert_eq!(refusal_reason(&status), reason::UNAUTHENTICATED);

    // Exactly one job was written: the call before the revoke.
    assert_eq!(
        harness
            .storage
            .stats(Some(NAMESPACE))
            .expect("stats")
            .pending,
        1
    );

    harness.stop().await;
}

/// #717's second acceptance criterion. `ExecutorService` is #720's, so the call
/// goes to a literal path — which is the honest test anyway: the scope is
/// checked before the router is consulted, so it holds for every RPC that
/// package will ever carry, including the ones not written yet.
#[tokio::test]
async fn a_produce_token_cannot_open_an_executor_stream() {
    let harness = Harness::start("grpc-auth-scope").await;
    let produce_only = harness.mint(NAMESPACE, ScopeSet::of(&[Scope::Produce])).1;

    let status = call_path(
        &harness,
        "/flexiq.executor.v1.ExecutorService/Dispatch",
        Some(&produce_only),
    )
    .await;
    assert_eq!(status.code(), Code::PermissionDenied);
    assert_eq!(refusal_reason(&status), reason::SCOPE_DENIED);
    let all = status.get_error_details();
    let details = all.error_info().expect("a refusal carries an ErrorInfo");
    assert_eq!(
        details.metadata.get(reason::KEY_SCOPE).map(String::as_str),
        Some("execute"),
        "the refusal must say which scope was missing"
    );

    // The same token reaches the producer package it *was* minted for.
    assert!(harness
        .producer()
        .enqueue(enqueue(Some(&produce_only)))
        .await
        .is_ok());

    // And a token that carries the scope gets past the gate — the refusal above
    // was the scope, not the package being unimplemented.
    let both = harness.mint_default();
    let status = call_path(
        &harness,
        "/flexiq.executor.v1.ExecutorService/Dispatch",
        Some(&both),
    )
    .await;
    assert_eq!(status.code(), Code::Unimplemented);

    harness.stop().await;
}

/// #717's third acceptance criterion. One database can carry two listeners, and
/// the token store is a single keyspace, so the namespace on the credential is
/// checked against the one this listener serves.
#[tokio::test]
async fn a_token_for_another_namespace_cannot_read_this_one() {
    let harness = Harness::start("grpc-auth-namespace").await;
    let elsewhere = harness.mint("some-other-tenant", ScopeSet::ALL).1;

    let status = harness
        .producer()
        .enqueue(enqueue(Some(&elsewhere)))
        .await
        .expect_err("a credential for another namespace must not be believed");
    assert_eq!(
        status.code(),
        Code::Unauthenticated,
        "not PermissionDenied, which would confirm the token exists"
    );
    assert_eq!(refusal_reason(&status), reason::UNAUTHENTICATED);
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

/// Health is the one public path, because a kubelet `grpc:` probe cannot carry
/// a credential and gating it would cost the chart its readiness probe.
#[tokio::test]
async fn health_answers_without_a_credential() {
    let harness = Harness::start("grpc-auth-health").await;

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
    let harness = Harness::start("grpc-auth-reflection").await;
    harness.mint_default();

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
    let harness = Harness::start("grpc-auth-unrouted").await;
    let token = harness.mint_default();

    let status = call_path(&harness, "/flexiq.v1.NoSuchService/NoSuchMethod", None).await;
    assert_eq!(status.code(), Code::Unauthenticated);
    assert_eq!(refusal_reason(&status), reason::UNAUTHENTICATED);

    // With the credential the same path reaches the router and is answered for
    // what it actually is — so the refusal above was the gate, not the route.
    let status = call_path(
        &harness,
        "/flexiq.v1.NoSuchService/NoSuchMethod",
        Some(&token),
    )
    .await;
    assert_eq!(status.code(), Code::Unimplemented);

    harness.stop().await;
}

/// #716 refused a non-loopback bind without `FLEXIQ_GRPC_TOKEN`. There is no
/// such variable now, and no bind that skips the credential — so the wildcard
/// bind starts, and it is the token store rather than the address that keeps an
/// anonymous caller out.
#[tokio::test]
async fn a_wildcard_bind_starts_and_still_refuses_an_anonymous_caller() {
    let harness = Harness::start_on("grpc-auth-public-bind", "0.0.0.0:0").await;
    let token = harness.mint_default();

    assert_eq!(
        harness
            .producer()
            .enqueue(enqueue(None))
            .await
            .expect_err("still refused, on a public bind as on loopback")
            .code(),
        Code::Unauthenticated
    );
    assert!(harness
        .producer()
        .enqueue(enqueue(Some(&token)))
        .await
        .is_ok());

    harness.stop().await;
}

/// A token records when it was last used, which is the audit trail a shared
/// secret could not have.
#[tokio::test]
async fn using_a_token_records_that_it_was_used() {
    let harness = Harness::start("grpc-auth-last-used").await;
    let (id, token) = harness.mint(NAMESPACE, ScopeSet::ALL);
    assert!(store::get(&*harness.storage, &id)
        .expect("read")
        .expect("present")
        .last_used_at
        .is_none());

    harness
        .producer()
        .enqueue(enqueue(Some(&token)))
        .await
        .expect("accepted");

    assert!(store::get(&*harness.storage, &id)
        .expect("read")
        .expect("present")
        .last_used_at
        .is_some());

    harness.stop().await;
}
