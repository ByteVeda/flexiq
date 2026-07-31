//! The dashboard HTTP server: SPA delivery plus the JSON API the SPA calls.

pub mod auth;
pub mod blocking;
pub mod dto;
pub mod error;
pub mod probes;
pub mod query;
pub mod routes;
pub mod security;
pub mod state;
pub mod static_assets;
pub mod stores;
pub mod webhook_sender;

use axum::extract::State;
use axum::http::{StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;

use crate::dashboard::error::ApiError;
use crate::dashboard::state::SharedState;
use crate::dashboard::static_assets::{content_type_for, IMMUTABLE_PREFIX, MISSING_ASSETS_HTML};
use crate::runtime::shutdown::Shutdown;

/// Build the router. Exposed so tests can drive it without binding a port.
pub fn router(state: SharedState) -> Router {
    Router::new()
        .route("/health", get(probes::health))
        .route("/readiness", get(probes::readiness))
        .route("/metrics", get(probes::metrics))
        .merge(routes::router())
        .merge(auth::router())
        .fallback(serve_spa)
        // Order matters: the gate runs before every route, and the security
        // headers wrap even the responses the gate itself returns.
        .layer(axum::middleware::from_fn({
            let state = state.clone();
            move |request, next| auth::middleware::gate_request(state.clone(), request, next)
        }))
        .layer(axum::middleware::from_fn(security::headers))
        .with_state(state)
}

/// Bind and serve until `shutdown` fires.
pub async fn serve(state: SharedState, shutdown: Shutdown) -> anyhow::Result<()> {
    let bind = state.config.bind;
    let listener = tokio::net::TcpListener::bind(bind).await?;
    log::info!("[taskito] dashboard on http://{bind}");
    axum::serve(listener, router(state))
        .with_graceful_shutdown(async move { shutdown.wait().await })
        .await?;
    Ok(())
}

/// Serve a bundled asset, or the SPA entry point for a client-routed path.
///
/// Unmatched `/api/` paths must 404 as JSON: the SPA parses every API response
/// as JSON, and handing it `index.html` turns a typo into a parse error.
async fn serve_spa(State(state): State<SharedState>, uri: Uri) -> Response {
    let path = uri.path();
    if path.starts_with("/api/") {
        return ApiError::not_found().into_response();
    }
    if !state.assets.available() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            [("content-type", "text/html; charset=utf-8")],
            MISSING_ASSETS_HTML,
        )
            .into_response();
    }

    if let Some(bytes) = state.assets.resolve(path) {
        // Vite content-hashes everything under /assets/, so those may be
        // cached forever; index.html must not be, or a deploy is invisible.
        let cache = if path.starts_with(IMMUTABLE_PREFIX) {
            "public, max-age=31536000, immutable"
        } else {
            "no-cache"
        };
        return (
            [
                ("content-type", content_type_for(path)),
                ("cache-control", cache),
            ],
            bytes.into_owned(),
        )
            .into_response();
    }

    // A miss under the hashed-asset prefix is a genuine 404, never the SPA
    // shell — otherwise a stale bundle reference returns HTML with a JS
    // content type.
    if path.starts_with(IMMUTABLE_PREFIX) {
        return ApiError::not_found().into_response();
    }

    match state.assets.index() {
        Some(index) => (
            [
                ("content-type", "text/html; charset=utf-8"),
                ("cache-control", "no-cache"),
            ],
            index.into_owned(),
        )
            .into_response(),
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            [("content-type", "text/html; charset=utf-8")],
            MISSING_ASSETS_HTML,
        )
            .into_response(),
    }
}
