//! End-to-end: the gRPC role binds, answers health and reflection, and stops
//! on the process-wide shutdown signal.
//!
//! The client here is a real gRPC client rather than a socket probe, because
//! the acceptance is that `grpcurl` works against a bare image — that the
//! committed descriptor is served, not merely that something is listening.
#![cfg(feature = "grpc")]

mod support;

use std::time::Duration;

use flexiq_server::config::grpc::GrpcConfig;
use flexiq_server::config::listen::ListenAddress;
use flexiq_server::grpc::Listener;
use flexiq_server::runtime::shutdown::Shutdown;
use tonic::transport::Channel;
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_client::HealthClient;
use tonic_health::pb::HealthCheckRequest;
use tonic_reflection::pb::v1::server_reflection_client::ServerReflectionClient;
use tonic_reflection::pb::v1::server_reflection_request::MessageRequest;
use tonic_reflection::pb::v1::server_reflection_response::MessageResponse;
use tonic_reflection::pb::v1::ServerReflectionRequest;

use support::temp_storage;

/// The role serves exactly one namespace, and refuses to start without one.
const NAMESPACE: &str = "grpc-tests";

fn config(listen: ListenAddress) -> GrpcConfig {
    GrpcConfig {
        listen,
        namespace: NAMESPACE.to_string(),
        // Loopback with no credential: the shape a developer runs, and the one
        // `config::grpc` allows without a token. Authentication has its own
        // suite in `grpc_auth.rs`.
        token: None,
    }
}

/// An ephemeral loopback port, so tests can run concurrently.
fn loopback() -> ListenAddress {
    ListenAddress::Tcp("127.0.0.1:0".parse().expect("valid address"))
}

/// Dial a listener. The generated clients ship without a `connect` of their
/// own — tonic only emits one when codegen ran with the channel feature — so
/// the channel is built here and handed to them.
async fn dial(url: &str) -> Channel {
    Channel::from_shared(url.to_string())
        .expect("a valid endpoint")
        .connect()
        .await
        .expect("the listener must accept a connection")
}

/// One reflection round trip, opened and closed per request.
async fn reflect(url: &str, request: MessageRequest) -> MessageResponse {
    let mut client = ServerReflectionClient::new(dial(url).await);
    let outbound = tokio_stream::iter(vec![ServerReflectionRequest {
        host: String::new(),
        message_request: Some(request),
    }]);
    client
        .server_reflection_info(outbound)
        .await
        .expect("open the reflection stream")
        .into_inner()
        .message()
        .await
        .expect("read the reflection response")
        .expect("the server must answer before closing the stream")
        .message_response
        .expect("a reflection response carries one of the response arms")
}

#[tokio::test]
async fn health_reports_serving_once_storage_has_answered() {
    let storage = temp_storage("grpc-health");
    let shutdown = Shutdown::default();
    let listener = Listener::bind(&config(loopback())).await.expect("bind");
    let addr = listener
        .local_addr()
        .expect("a TCP listener knows what it bound");
    let served = tokio::spawn(listener.serve((*storage).clone(), shutdown.clone()));

    let mut health = HealthClient::new(dial(&format!("http://{addr}")).await);
    let status = health
        .check(HealthCheckRequest {
            // The empty service name is overall server health.
            service: String::new(),
        })
        .await
        .expect("the health check must answer")
        .into_inner()
        .status;
    assert_eq!(status, ServingStatus::Serving as i32);

    shutdown.trigger();
    served
        .await
        .expect("the serve task must not panic")
        .expect("a shutdown is not an error");
}

#[tokio::test]
async fn reflection_lists_the_health_service_and_resolves_the_committed_contract() {
    let storage = temp_storage("grpc-reflection");
    let shutdown = Shutdown::default();
    let listener = Listener::bind(&config(loopback())).await.expect("bind");
    let url = format!(
        "http://{}",
        listener
            .local_addr()
            .expect("a TCP listener knows what it bound")
    );
    let served = tokio::spawn(listener.serve((*storage).clone(), shutdown.clone()));

    // What `grpcurl list` asks for.
    let listed = reflect(&url, MessageRequest::ListServices(String::new())).await;
    let MessageResponse::ListServicesResponse(services) = listed else {
        panic!("ListServices must answer with a service list, got {listed:?}");
    };
    let names: Vec<&str> = services
        .service
        .iter()
        .map(|service| service.name.as_str())
        .collect();
    assert!(
        names.contains(&"grpc.health.v1.Health"),
        "a reflecting client must be able to find the health service: {names:?}"
    );
    // The producer door is discoverable the same way, which is what makes
    // `grpcurl` against a bare image the acceptance rather than a demo.
    assert!(
        names.contains(&"flexiq.v1.ProducerService"),
        "a reflecting client must be able to find the producer service: {names:?}"
    );

    // And the reason the descriptor is embedded: the wire contract travels with
    // the binary, so a client needs no `.proto` on hand.
    let symbol = reflect(
        &url,
        MessageRequest::FileContainingSymbol("flexiq.v1.JobStatus".to_string()),
    )
    .await;
    let MessageResponse::FileDescriptorResponse(files) = symbol else {
        panic!("a symbol lookup must answer with file descriptors, got {symbol:?}");
    };
    assert!(
        !files.file_descriptor_proto.is_empty(),
        "flexiq.v1.JobStatus must resolve out of contracts/descriptor.binpb"
    );

    shutdown.trigger();
    served
        .await
        .expect("the serve task must not panic")
        .expect("a shutdown is not an error");
}

/// The Unix path reuses the attach listener's hardened bind, so the two
/// properties worth pinning are the mode it lands at and that the path does not
/// outlive the listener.
#[cfg(unix)]
#[tokio::test]
async fn a_unix_socket_lands_narrow_and_is_cleaned_up() {
    use std::os::unix::fs::PermissionsExt;

    let storage = temp_storage("grpc-unix");
    let path = std::env::temp_dir().join(format!("flexiq-grpc-{}.sock", std::process::id()));
    let _ = std::fs::remove_file(&path);

    let shutdown = Shutdown::default();
    let listener = Listener::bind(&config(ListenAddress::Unix(path.clone())))
        .await
        .expect("bind the Unix socket");
    assert!(
        listener.local_addr().is_none(),
        "a Unix listener has no socket address to report"
    );
    let served = tokio::spawn(listener.serve((*storage).clone(), shutdown.clone()));

    // Owner and group, and nobody else — whatever the umask was.
    let mode = std::fs::metadata(&path)
        .expect("the socket must exist while the listener runs")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o660);
    tokio::net::UnixStream::connect(&path)
        .await
        .expect("the socket must be accepting");

    shutdown.trigger();
    tokio::time::timeout(Duration::from_secs(10), served)
        .await
        .expect("the listener must stop on the shutdown signal")
        .expect("the serve task must not panic")
        .expect("a shutdown is not an error");
    assert!(
        !path.exists(),
        "the socket file must not outlive the listener"
    );
}
