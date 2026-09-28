//! End-to-end: the gRPC door terminating TLS and mTLS itself (#838).
//!
//! What is pinned here is what a unit test of the config cannot reach: that a
//! real client negotiates with the listener, that a plaintext client is not
//! served beside it, that mTLS refuses a connection before any credential is
//! read — and that the bearer token is still demanded once it has not.
#![cfg(feature = "grpc")]

mod support;

use std::path::PathBuf;

use flexiq_cli::connect::ClientTls;
use flexiq_server::config::grpc::GrpcConfig;
use flexiq_server::config::listen::ListenAddress;
use flexiq_server::grpc::pb::producer_service_client::ProducerServiceClient;
use flexiq_server::grpc::pb::{enqueue_request, EnqueueOptions, EnqueueRequest};
use flexiq_server::grpc::Listener;
use flexiq_server::runtime::shutdown::Shutdown;
use flexiq_server::tls::TlsFiles;
use flexiq_server::tokens::{store, NewToken, ScopeSet};
use tonic::metadata::MetadataValue;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Identity};
use tonic::{Code, Request};
use tonic_health::pb::health_client::HealthClient;
use tonic_health::pb::HealthCheckRequest;

use support::{temp_storage, temp_workflows, TempStorage};

const NAMESPACE: &str = "grpc-tls-tests";

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../flexiq-core/tests/fixtures/tls")
        .join(name)
}

fn pem(name: &str) -> Vec<u8> {
    std::fs::read(fixture(name)).expect("fixture")
}

/// A running TLS door.
struct Harness {
    port: u16,
    storage: TempStorage,
    shutdown: Shutdown,
    served: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl Harness {
    /// Start a door presenting the fixture server certificate, demanding a
    /// client certificate from the fixture CA when `mtls`.
    async fn start(label: &str, mtls: bool) -> Self {
        let storage = temp_storage(label);
        let shutdown = Shutdown::default();
        let config = GrpcConfig {
            tls: Some(TlsFiles {
                cert: fixture("server.pem"),
                key: fixture("server-key.pem"),
                client_ca: mtls.then(|| fixture("ca.pem")),
            }),
            ..GrpcConfig::new(
                ListenAddress::Tcp("127.0.0.1:0".parse().expect("address")),
                NAMESPACE,
            )
        };
        let listener = Listener::bind(&config).await.expect("bind");
        let port = listener.local_addr().expect("tcp").port();
        let served = tokio::spawn(listener.serve(
            (*storage).clone(),
            temp_workflows(&storage),
            None,
            shutdown.clone(),
        ));
        Self {
            port,
            storage,
            shutdown,
            served,
        }
    }

    /// A channel trusting the fixture CA, presenting `identity` if given.
    async fn channel(
        &self,
        identity: Option<(&str, &str)>,
    ) -> Result<Channel, tonic::transport::Error> {
        let mut tls = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(pem("ca.pem")))
            .domain_name("localhost");
        if let Some((cert, key)) = identity {
            tls = tls.identity(Identity::from_pem(pem(cert), pem(key)));
        }
        Channel::from_shared(format!("https://127.0.0.1:{}", self.port))
            .expect("endpoint")
            .tls_config(tls)?
            .connect()
            .await
    }

    fn mint(&self) -> String {
        let request = NewToken::new("client", ScopeSet::ALL, NAMESPACE, None, None).expect("mint");
        store::create(&*self.storage, request).expect("mint").1
    }

    async fn stop(self) {
        self.shutdown.trigger();
        self.served
            .await
            .expect("the serve task must not panic")
            .expect("a shutdown is not an error");
    }
}

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
            MetadataValue::try_from(format!("Bearer {credential}")).expect("ASCII"),
        );
    }
    request
}

async fn healthy(channel: Channel) -> Result<(), tonic::Status> {
    HealthClient::new(channel)
        .check(HealthCheckRequest {
            service: String::new(),
        })
        .await
        .map(|_| ())
}

#[tokio::test]
async fn a_tls_client_is_served_and_still_needs_its_token() {
    let harness = Harness::start("tls-served", false).await;
    let channel = harness.channel(None).await.expect("TLS connect");
    healthy(channel.clone()).await.expect("health over TLS");

    let mut producer = ProducerServiceClient::new(channel);
    let refused = producer
        .enqueue(enqueue(None))
        .await
        .expect_err("TLS is not a credential");
    assert_eq!(refused.code(), Code::Unauthenticated);

    let token = harness.mint();
    producer
        .enqueue(enqueue(Some(&token)))
        .await
        .expect("a token over TLS is honoured");
    harness.stop().await;
}

#[tokio::test]
async fn a_plaintext_client_is_not_served_beside_tls() {
    let harness = Harness::start("tls-plaintext", false).await;
    let outcome = async {
        let channel = Channel::from_shared(format!("http://127.0.0.1:{}", harness.port))
            .expect("endpoint")
            .connect()
            .await
            .map_err(|error| error.to_string())?;
        healthy(channel).await.map_err(|status| status.to_string())
    }
    .await;
    assert!(outcome.is_err(), "a cleartext call must not be answered");
    harness.stop().await;
}

#[tokio::test]
async fn the_json_facade_answers_http1_over_tls() {
    let harness = Harness::start("tls-facade", false).await;
    let client = reqwest::Client::builder()
        .add_root_certificate(reqwest::Certificate::from_pem(&pem("ca.pem")).expect("ca"))
        .http1_only()
        .build()
        .expect("client");
    let response = client
        .get(format!("https://localhost:{}/v1/jobs", harness.port))
        .send()
        .await
        .expect("an HTTP/1.1 request over TLS");
    // Refused for want of a token — which is an answer, over TLS, from the door.
    assert_eq!(response.status(), reqwest::StatusCode::UNAUTHORIZED);
    harness.stop().await;
}

#[tokio::test]
async fn mtls_serves_a_client_with_a_trusted_certificate() {
    let harness = Harness::start("mtls-trusted", true).await;
    let channel = harness
        .channel(Some(("client.pem", "client-key.pem")))
        .await
        .expect("mTLS connect");
    healthy(channel.clone()).await.expect("health over mTLS");

    // The certificate lets the connection exist; it does not name a principal.
    let refused = ProducerServiceClient::new(channel)
        .enqueue(enqueue(None))
        .await
        .expect_err("a client certificate is not a token");
    assert_eq!(refused.code(), Code::Unauthenticated);
    harness.stop().await;
}

#[tokio::test]
async fn mtls_refuses_a_client_without_a_certificate() {
    let harness = Harness::start("mtls-anonymous", true).await;
    let outcome = match harness.channel(None).await {
        Ok(channel) => healthy(channel).await.map_err(|status| status.to_string()),
        Err(error) => Err(error.to_string()),
    };
    assert!(outcome.is_err(), "no certificate, no connection");
    harness.stop().await;
}

#[tokio::test]
async fn mtls_refuses_a_certificate_from_another_ca() {
    let harness = Harness::start("mtls-rogue", true).await;
    let outcome = match harness
        .channel(Some(("rogue-client.pem", "rogue-client-key.pem")))
        .await
    {
        Ok(channel) => healthy(channel).await.map_err(|status| status.to_string()),
        Err(error) => Err(error.to_string()),
    };
    assert!(
        outcome.is_err(),
        "an untrusted certificate must not connect"
    );
    harness.stop().await;
}

/// `fq`'s own dialler, with its `--tls-ca/--tls-cert/--tls-key` files, against
/// an mTLS door.
#[tokio::test]
async fn fq_dials_an_mtls_door_with_its_certificate() {
    let harness = Harness::start("mtls-fq", true).await;
    let endpoint = format!("https://localhost:{}", harness.port);
    let token = harness.mint();
    let tls = ClientTls {
        ca: Some(fixture("ca.pem")),
        cert: Some(fixture("client.pem")),
        key: Some(fixture("client-key.pem")),
    };
    let mut client = flexiq_cli::connect::connect(&endpoint, &token, &tls)
        .await
        .expect("fq connects over mTLS");
    client
        .enqueue(flexiq_cli::pb::EnqueueRequest {
            task_name: "send_email".to_string(),
            body: Some(flexiq_cli::pb::enqueue_request::Body::Raw(vec![1])),
            options: Some(flexiq_cli::pb::EnqueueOptions {
                queue: "emails".to_string(),
                ..Default::default()
            }),
        })
        .await
        .expect("an enqueue over mTLS");

    // Without its certificate the same call is refused before the token is read.
    let anonymous = ClientTls {
        ca: Some(fixture("ca.pem")),
        ..ClientTls::default()
    };
    let refused = match flexiq_cli::connect::connect(&endpoint, &token, &anonymous).await {
        Ok(mut client) => client
            .enqueue(flexiq_cli::pb::EnqueueRequest::default())
            .await
            .map(|_| ())
            .map_err(|status| status.to_string()),
        Err(error) => Err(error.to_string()),
    };
    assert!(refused.is_err(), "no certificate, no call");
    harness.stop().await;
}

#[tokio::test]
async fn fq_refuses_tls_files_beside_a_plaintext_endpoint() {
    let tls = ClientTls {
        ca: Some(fixture("ca.pem")),
        ..ClientTls::default()
    };
    // Matched rather than `expect_err`: the client is not `Debug`, because the
    // credential it carries must never be printed.
    let Err(error) = flexiq_cli::connect::connect("http://127.0.0.1:1", "token", &tls).await else {
        panic!("must refuse rather than send in the clear");
    };
    assert!(error.to_string().contains("https://"), "{error}");
}
