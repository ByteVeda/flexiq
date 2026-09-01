//! The gRPC door: the fourth role this process can play.
//!
//! It is deliberately the same shape as the other three — one environment
//! variable, one listener, the one shared `StorageBackend`, and the same
//! shutdown signal — so a deployment enables it the way it enables the
//! dashboard or the webhook, and `SIGTERM` drains it alongside them.
//!
//! What it serves today is `grpc.health.v1` and server reflection. The
//! `flexiq.v1` producer service and the `flexiq.executor.v1` stream land on
//! this listener as they arrive; the contract they must keep lives in
//! `tasks/specs/2026-09-01-flexiq-v1-proto-design.md`.
//!
//! The one rule that shapes everything above it: **this door serves exactly one
//! namespace**, the process's own, and refuses to start without one. See
//! [`crate::config::grpc`].

pub mod health;
pub mod limits;
pub mod listener;
pub mod pb;
pub mod reflection;

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
