//! The gRPC door: the fourth role this process can play.
//!
//! It is deliberately the same shape as the other three — one environment
//! variable, one listener, the one shared `StorageBackend`, and the same
//! shutdown signal — so a deployment enables it the way it enables the
//! dashboard or the webhook, and `SIGTERM` drains it alongside them.
//!
//! What it serves is `grpc.health.v1`, server reflection, the `flexiq.v1`
//! [`ProducerService`](producer::Producer) and — over ordinary HTTP, on the
//! same port — that same service's [JSON facade](facade). The
//! `flexiq.executor.v1` stream lands on this listener when it arrives; the
//! contract all of them must keep lives in
//! `tasks/specs/2026-09-01-flexiq-v1-proto-design.md`.
//!
//! The two rules that shape everything above it: **this door serves exactly
//! one namespace**, the process's own, and refuses to start without one; and
//! **every call on it is authenticated in one place**, [`auth::AuthLayer`],
//! which wraps the whole router so a new RPC cannot land outside the check.
//! The two `grpc.health.v1` RPCs are the one exception, because a kubelet probe
//! carries no credential.
//! See [`crate::config::grpc`] and [`auth`].

pub mod auth;
pub mod blocking;
pub mod facade;
pub mod health;
pub mod limits;
pub mod listener;
pub mod pb;
pub mod producer;
pub mod reflection;
pub mod status;

use anyhow::Result;
use flexiq_core::StorageBackend;

use crate::config::grpc::GrpcConfig;
use crate::runtime::shutdown::Shutdown;

pub use listener::Listener;

/// Bind and serve until `shutdown` fires.
///
/// The two halves are separate on [`Listener`] for tests and for a caller that
/// needs the port a `:0` bind chose; a deployment only ever wants both.
pub async fn serve(config: GrpcConfig, storage: StorageBackend, shutdown: Shutdown) -> Result<()> {
    Listener::bind(&config)
        .await?
        .serve(storage, shutdown)
        .await
}
