//! The trigger listener: one catch-all route, dispatched by path.
//!
//! A catch-all rather than one axum route per trigger, because a trigger path
//! is operator data and axum route syntax would give `{` and `*` in it a
//! meaning. The handler looks the path up in a map instead.

use std::sync::Arc;

use anyhow::{Context, Result};
use axum::Router;
use flexiq_core::StorageBackend;

use crate::config::trigger::TriggerConfig;
use crate::runtime::shutdown::Shutdown;
use crate::trigger::handler::{receive, Role};

/// The listener's routes, for `serve` and for a test that binds its own port.
pub fn router(config: &TriggerConfig, storage: StorageBackend) -> Router {
    let role = Arc::new(Role::new(
        storage,
        config.namespace.clone(),
        config.triggers.clone(),
    ));
    Router::new().fallback(receive).with_state(role)
}

/// Serve until `shutdown` fires.
pub async fn serve(
    config: TriggerConfig,
    storage: StorageBackend,
    shutdown: Shutdown,
) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(config.bind)
        .await
        .with_context(|| format!("the trigger listener could not bind {}", config.bind))?;
    log::info!(
        "[flexiq] triggers on http://{} — {} from {}, into namespace {}",
        config.bind,
        config.triggers.len(),
        config.file.display(),
        config.namespace
    );
    axum::serve(listener, router(&config, storage))
        .with_graceful_shutdown(async move { shutdown.wait().await })
        .await
        .with_context(|| format!("the trigger listener on {} stopped", config.bind))?;
    Ok(())
}
