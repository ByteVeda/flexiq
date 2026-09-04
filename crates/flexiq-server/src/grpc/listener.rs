//! Bind the gRPC port and serve until shutdown.
//!
//! Binding is separate from serving for the same reason it is in the attach
//! listener: a `:0` bind resolves to a port only the listener knows, and a bind
//! failure should be an error the caller gets rather than a task that quietly
//! never accepts.

#[cfg(unix)]
use std::path::PathBuf;

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use flexiq_core::StorageBackend;
use flexiq_workflows::WorkflowStorageBackend;
use tokio::net::TcpListener;
#[cfg(unix)]
use tokio::net::UnixListener;
use tokio_stream::wrappers::TcpListenerStream;
#[cfg(unix)]
use tokio_stream::wrappers::UnixListenerStream;
use tonic::service::Routes;
use tonic::transport::Server;

use crate::config::grpc::GrpcConfig;
use crate::config::listen::ListenAddress;
use crate::grpc::auth::{self, AuthLayer};
use crate::grpc::executor::ExecutorDoor;
use crate::grpc::limits::PRODUCER_MAX_MESSAGE_BYTES;
use crate::grpc::producer::Producer;
use crate::grpc::{facade, health, metrics, reflection};
use crate::runtime::shutdown::Shutdown;

/// How long the listener waits for open connections after shutdown before it
/// stops waiting.
///
/// tonic's graceful shutdown ends when every connection has closed, and an
/// HTTP/2 stream is only closed once *both* halves are. An executor whose
/// attach stream the scheduler has already ended, but which has not closed its
/// own half — because it crashed, or froze, or is simply impolite — would
/// otherwise hold this process open until the orchestrator gave up and
/// `SIGKILL`ed it, turning a clean drain into an abrupt one.
///
/// Long enough that an in-flight producer call finishes normally; short enough
/// to fit inside every default termination grace period.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(15);

/// Run `served` to completion, or abandon it [`SHUTDOWN_GRACE`] after shutdown.
async fn within_grace<F>(served: F, shutdown: Shutdown) -> Result<(), tonic::transport::Error>
where
    F: Future<Output = Result<(), tonic::transport::Error>>,
{
    tokio::pin!(served);
    tokio::select! {
        result = &mut served => result,
        () = async { shutdown.wait().await; tokio::time::sleep(SHUTDOWN_GRACE).await } => {
            log::warn!(
                "[flexiq] the gRPC listener still had open connections {SHUTDOWN_GRACE:?} after \
                 shutdown; abandoning them"
            );
            Ok(())
        }
    }
}

/// A bound, not yet serving, gRPC listener.
pub struct Listener {
    config: GrpcConfig,
    incoming: Incoming,
}

/// The bound socket, in whichever shape the address asked for.
enum Incoming {
    Tcp(TcpListener),
    #[cfg(unix)]
    Unix(UnixListener, PathBuf),
}

impl Listener {
    /// Bind the address in `config`.
    pub async fn bind(config: &GrpcConfig) -> Result<Self> {
        let incoming = match &config.listen {
            ListenAddress::Tcp(addr) => {
                let listener = TcpListener::bind(addr)
                    .await
                    .with_context(|| format!("failed to bind the gRPC listener on {addr}"))?;
                // Report what was bound, not what was asked for: port 0
                // resolves to an ephemeral port only the listener knows.
                let bound = listener.local_addr().unwrap_or(*addr);
                log::info!("[flexiq] gRPC listener on tcp://{bound}");
                Incoming::Tcp(listener)
            }
            #[cfg(unix)]
            ListenAddress::Unix(path) => {
                // The attach role's hardened bind: the socket is created inside
                // a private directory, narrowed to 0660, and only then renamed
                // into place, so it never accepts at a umask-derived mode.
                let listener = crate::runtime::listener::bind_unix(path)?;
                listener
                    .set_nonblocking(true)
                    .context("failed to make the gRPC socket non-blocking")?;
                let listener = UnixListener::from_std(listener)
                    .context("failed to register the gRPC socket with the runtime")?;
                log::info!("[flexiq] gRPC listener on unix:{}", path.display());
                Incoming::Unix(listener, path.clone())
            }
        };
        Ok(Self {
            config: config.clone(),
            incoming,
        })
    }

    /// Address actually bound, for a TCP listener.
    pub fn local_addr(&self) -> Option<std::net::SocketAddr> {
        match &self.incoming {
            Incoming::Tcp(listener) => listener.local_addr().ok(),
            #[cfg(unix)]
            Incoming::Unix(..) => None,
        }
    }

    /// Serve the producer service, the JSON facade, health and reflection until
    /// `shutdown` fires.
    ///
    /// `executor` is the `flexiq.executor.v1` door, present whenever this
    /// process has a dispatcher to attach executors to. It is registered on the
    /// same listener rather than a second one: the two packages differ in
    /// audience and credential, not in address, and a token scoped to one
    /// cannot reach the other.
    pub async fn serve(
        self,
        storage: StorageBackend,
        workflows: WorkflowStorageBackend,
        executor: Option<ExecutorDoor>,
        shutdown: Shutdown,
    ) -> Result<()> {
        let producer = Producer::new(storage.clone(), workflows);
        let health = health::serve(
            storage.clone(),
            self.config.namespace.clone(),
            shutdown.clone(),
        )
        .await;

        let rpc_metrics = metrics::RpcMetrics::new();

        // `/metrics` is merged into the facade's router rather than the other
        // way round: `facade::router` owns the fallback that keeps an unrouted
        // answer in the caller's shape, and `merge` tolerates exactly one.
        let http = metrics::router(
            storage.clone(),
            self.config.namespace.clone(),
            executor.clone(),
            Arc::clone(&rpc_metrics),
        )
        .merge(facade::router(producer.clone()));

        // That router is what the gRPC services are then added to, rather than
        // a second listener or a service behind a proxy: an HTTP request
        // reaches the same `Producer` through the same layer, in this process,
        // with no loopback hop.
        let mut routes = Routes::from(http)
            .add_service(producer.into_service())
            .add_service(health.max_decoding_message_size(PRODUCER_MAX_MESSAGE_BYTES))
            .add_service(reflection::v1()?.max_decoding_message_size(PRODUCER_MAX_MESSAGE_BYTES))
            .add_service(
                reflection::v1alpha()?.max_decoding_message_size(PRODUCER_MAX_MESSAGE_BYTES),
            );

        // Its own cap, deliberately larger: the executor door carries the
        // payloads the local frame protocol already allows, and a message limit
        // equal to the payload limit would reject the largest legal one.
        if let Some(executor) = executor {
            routes = routes.add_service(executor.into_service());
        }

        let mut builder = Server::builder()
            // `curl` speaks HTTP/1.1, and a facade only reachable over h2c
            // prior knowledge would not be a facade. HTTP/2 is still detected
            // by its preface, so a gRPC client notices nothing.
            .accept_http1(true)
            // An HTTP/2 ping, not a TCP keepalive: `tcp_keepalive` is applied
            // to a socket tonic binds, and this listener hands it one it bound
            // itself so it could resolve a `:0` port. Setting the TCP knob here
            // would be accepted and ignored, which is the failure mode this
            // whole module refuses elsewhere.
            .http2_keepalive_interval(
                (!self.config.keepalive_interval.is_zero())
                    .then_some(self.config.keepalive_interval),
            )
            .http2_keepalive_timeout(self.config.keepalive_timeout());

        // Both of these race the response *future*, not the response body, and
        // release on the same event: an `Attach` stream returns its response as
        // soon as it has spawned its workers, so neither one bounds how long it
        // then lives. The same holds for `Health/Watch` and reflection.
        if !self.config.request_timeout.is_zero() {
            builder = builder.timeout(self.config.request_timeout);
        }
        if self.config.max_concurrent_requests > 0 {
            builder = builder.concurrency_limit_per_connection(self.config.max_concurrent_requests);
        }

        // One layer over the whole router, not one per service: `Server::layer`
        // takes `Layer<Routes>`, so every service registered above — and every
        // route added in future — is gated by this line and by nothing else. It
        // is also what supplies the namespace: the producer holds none of its
        // own and reads it off the request's principal.
        // Order matters, and it is the reverse of how it reads: `Server::layer`
        // makes each new layer the *inner* one, so the metrics layer is applied
        // first to end up outermost. It has to be outermost, or a call refused
        // for want of a credential would never reach it — and a refusal missing
        // from the metrics is the one an operator most needs to see.
        let mut server = builder
            .layer(metrics::MetricsLayer::new(rpc_metrics))
            .layer(AuthLayer::new(Arc::new(auth::TokenStore::new(
                storage.clone(),
                self.config.namespace.as_str(),
            ))));
        let router = server.add_routes(routes);

        let listen = self.config.listen.clone();
        let result = match self.incoming {
            Incoming::Tcp(listener) => {
                let signal = shutdown.clone();
                within_grace(
                    router.serve_with_incoming_shutdown(
                        TcpListenerStream::new(listener),
                        async move { signal.wait().await },
                    ),
                    shutdown.clone(),
                )
                .await
            }
            #[cfg(unix)]
            Incoming::Unix(listener, path) => {
                let signal = shutdown.clone();
                let served = within_grace(
                    router.serve_with_incoming_shutdown(
                        UnixListenerStream::new(listener),
                        async move { signal.wait().await },
                    ),
                    shutdown.clone(),
                )
                .await;
                // The socket file outlives the listener otherwise, and the next
                // start would find a path with nothing behind it.
                let _ = std::fs::remove_file(&path);
                served
            }
        };
        result.with_context(|| format!("the gRPC listener on {listen} stopped"))?;
        Ok(())
    }
}
