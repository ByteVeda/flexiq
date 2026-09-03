//! Bind the gRPC port and serve until shutdown.
//!
//! Binding is separate from serving for the same reason it is in the attach
//! listener: a `:0` bind resolves to a port only the listener knows, and a bind
//! failure should be an error the caller gets rather than a task that quietly
//! never accepts.

#[cfg(unix)]
use std::path::PathBuf;

use std::sync::Arc;

use anyhow::{Context, Result};
use flexiq_core::StorageBackend;
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
use crate::grpc::{facade, health, reflection};
use crate::runtime::shutdown::Shutdown;

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
        executor: Option<ExecutorDoor>,
        shutdown: Shutdown,
    ) -> Result<()> {
        let producer = Producer::new(storage.clone());
        let health = health::serve(
            storage.clone(),
            self.config.namespace.clone(),
            shutdown.clone(),
        )
        .await;

        // The facade's routes are the router the gRPC services are then added
        // to, rather than a second listener or a service behind a proxy: an
        // HTTP request reaches the same `Producer` through the same layer, in
        // this process, with no loopback hop. It also owns the fallback, so an
        // unrouted path is answered in the shape the caller asked in.
        let mut routes = Routes::from(facade::router(producer.clone()))
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

        // One layer over the whole router, not one per service: `Server::layer`
        // takes `Layer<Routes>`, so every service registered above — and every
        // route added in future — is gated by this line and by nothing else. It
        // is also what supplies the namespace: the producer holds none of its
        // own and reads it off the request's principal.
        let mut server = Server::builder()
            // `curl` speaks HTTP/1.1, and a facade only reachable over h2c
            // prior knowledge would not be a facade. HTTP/2 is still detected
            // by its preface, so a gRPC client notices nothing.
            .accept_http1(true)
            .layer(AuthLayer::new(Arc::new(auth::TokenStore::new(
                storage.clone(),
                self.config.namespace.as_str(),
            ))));
        let router = server.add_routes(routes);

        let listen = self.config.listen.clone();
        let result = match self.incoming {
            Incoming::Tcp(listener) => {
                let signal = shutdown.clone();
                router
                    .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                        signal.wait().await
                    })
                    .await
            }
            #[cfg(unix)]
            Incoming::Unix(listener, path) => {
                let signal = shutdown.clone();
                let served = router
                    .serve_with_incoming_shutdown(UnixListenerStream::new(listener), async move {
                        signal.wait().await
                    })
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
