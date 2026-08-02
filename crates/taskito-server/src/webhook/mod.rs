//! The mutating admission webhook that injects executor sidecars.
//!
//! An attach deployment is otherwise two edits to every workload that wants
//! one: a sidecar container and its environment. This turns both into
//! annotations, the way OpenTelemetry auto-instrumentation does, by answering
//! the API server's admission call with a patch that adds the container.
//!
//! It is the one role in this binary that touches no storage — pods in, patch
//! out — so it runs without a DSN and can be deployed on its own.

pub mod admission;
pub mod annotations;
pub mod inject;

use std::sync::Arc;

use anyhow::{Context, Result};
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use axum_server::tls_rustls::RustlsConfig;

use crate::config::webhook::WebhookConfig;
use crate::runtime::shutdown::Shutdown;
use crate::webhook::admission::{AdmissionReview, AdmissionReviewResponse};

/// Path the `MutatingWebhookConfiguration` points at.
pub const MUTATE_PATH: &str = "/mutate";

/// Build the router. Exposed so tests can drive it without binding a port.
pub fn router(config: Arc<WebhookConfig>) -> Router {
    Router::new()
        .route(MUTATE_PATH, post(mutate))
        // The webhook pod needs its own probes: it may be the only role running,
        // in which case the dashboard's are not there to borrow.
        .route(
            "/health",
            get(|| async { Json(serde_json::json!({ "status": "ok" })) }),
        )
        .with_state(config)
}

/// How often the mounted key pair is checked for a renewal. A kubelet takes up
/// to a minute to project an updated Secret anyway, so polling faster would
/// only burn syscalls.
const CERT_POLL: std::time::Duration = std::time::Duration::from_secs(30);

/// Serve until `shutdown` fires.
pub async fn serve(config: WebhookConfig, shutdown: Shutdown) -> Result<()> {
    // rustls 0.23 refuses to build a config until a provider is installed, and
    // nothing else in this process does it. An `Err` means another crate got
    // there first, which is exactly as good.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let tls = RustlsConfig::from_pem_file(&config.cert, &config.key)
        .await
        .with_context(|| {
            format!(
                "failed to load the webhook certificate from {} and {}",
                config.cert.display(),
                config.key.display()
            )
        })?;

    let bind = config.bind;
    log::info!("[taskito] admission webhook on https://{bind}{MUTATE_PATH}");

    tokio::spawn(watch_certificate(
        tls.clone(),
        config.cert.clone(),
        config.key.clone(),
        shutdown.clone(),
    ));

    let handle = axum_server::Handle::new();
    tokio::spawn({
        let handle = handle.clone();
        let shutdown = shutdown.clone();
        async move {
            shutdown.wait().await;
            // Let in-flight admissions finish: the API server is blocking a pod
            // creation on each one, and a dropped connection there surfaces as a
            // failed deploy rather than a retry.
            handle.graceful_shutdown(Some(std::time::Duration::from_secs(5)));
        }
    });

    axum_server::bind_rustls(bind, tls)
        .handle(handle)
        .serve(router(Arc::new(config)).into_make_service())
        .await
        .with_context(|| format!("the admission webhook on {bind} stopped"))?;
    Ok(())
}

/// Re-read the key pair whenever it changes on disk.
///
/// cert-manager renews a Secret in place. The kubelet swaps the projected files
/// under a running pod, so without this the server would keep presenting the
/// certificate it read at boot until something restarted it — and the API
/// server would start refusing the connection the moment the old one expired.
///
/// Content, not mtime: a projected Secret is a symlink dance whose timestamps
/// move for reasons unrelated to the bytes, and reloading rustls needlessly is
/// worse than reading two small files.
async fn watch_certificate(
    tls: RustlsConfig,
    cert: std::path::PathBuf,
    key: std::path::PathBuf,
    shutdown: Shutdown,
) {
    let mut current = read_pair(&cert, &key).await;
    loop {
        tokio::select! {
            _ = shutdown.wait() => return,
            _ = tokio::time::sleep(CERT_POLL) => {}
        }

        let latest = read_pair(&cert, &key).await;
        // `None` is a read failure — mid-swap, most likely. Keep serving what
        // we have and look again next tick.
        if latest.is_none() || latest == current {
            continue;
        }

        match tls.reload_from_pem_file(&cert, &key).await {
            Ok(()) => {
                log::info!("[taskito] reloaded the webhook certificate");
                current = latest;
            }
            // Left on the previous pair, which still works until it expires.
            Err(error) => log::error!(
                "the webhook certificate at {} changed but could not be loaded: {error}",
                cert.display()
            ),
        }
    }
}

/// The two files' bytes, or `None` if either could not be read.
async fn read_pair(cert: &std::path::Path, key: &std::path::Path) -> Option<(Vec<u8>, Vec<u8>)> {
    let cert = tokio::fs::read(cert).await.ok()?;
    let key = tokio::fs::read(key).await.ok()?;
    Some((cert, key))
}

/// Answer one `AdmissionReview`.
async fn mutate(State(_config): State<Arc<WebhookConfig>>, body: String) -> Response {
    let review: AdmissionReview = match serde_json::from_str(&body) {
        Ok(review) => review,
        // No uid to answer with, so there is no valid AdmissionReview to send
        // back — only a plain 400 the API server logs.
        Err(error) => {
            log::warn!("admission request was not an AdmissionReview: {error}");
            return (StatusCode::BAD_REQUEST, "not an AdmissionReview").into_response();
        }
    };

    let Some(request) = &review.request else {
        log::warn!("admission review carried no request");
        return (StatusCode::BAD_REQUEST, "no request in the review").into_response();
    };
    let uid = request.uid.clone();
    let namespace = request.namespace.as_deref().unwrap_or("<none>");

    match inject::patch_for(&request.object) {
        Ok(Some(patch)) => match AdmissionReviewResponse::patch(&review, uid.clone(), &patch) {
            Ok(response) => {
                log::info!("[taskito] injected an executor sidecar into a pod in {namespace}");
                Json(response).into_response()
            }
            // Serialising our own patch cannot realistically fail, but admitting
            // the pod unchanged beats failing a deploy over it.
            Err(error) => {
                log::error!("failed to encode the injection patch: {error}");
                Json(AdmissionReviewResponse::allow(&review, uid)).into_response()
            }
        },
        Ok(None) => Json(AdmissionReviewResponse::allow(&review, uid)).into_response(),
        // The pod asked to be injected and described it wrongly. Denying is the
        // point: admitting it would produce a pod that looks healthy and never
        // runs a job.
        Err(error) => {
            log::warn!("refusing a pod in {namespace}: {error:#}");
            Json(AdmissionReviewResponse::deny(
                &review,
                uid,
                format!("taskito sidecar injection: {error:#}"),
            ))
            .into_response()
        }
    }
}
