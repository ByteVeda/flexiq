//! Shared helpers for the integration tests.
//!
//! Each test binary compiles this module separately, so helpers used by only
//! one of them read as dead code in the others.
#![allow(dead_code)]

pub mod oidc_issuer;

use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::http::{HeaderMap, Request, StatusCode};
use serde_json::Value;
use taskito_core::{SqliteStorage, StorageBackend};
use taskito_server::config::dashboard::{AuthMode, DashboardConfig};
use taskito_server::dashboard::auth::oauth::config::OAuthConfig;
use taskito_server::dashboard::auth::oauth::providers::OAuthRuntime;
use taskito_server::dashboard::router;
use taskito_server::dashboard::state::{AppState, SharedState};
use taskito_server::dashboard::static_assets::StaticAssets;
use taskito_workflows::{WorkflowSqliteStorage, WorkflowStorageBackend};
use tower::ServiceExt;

/// Distinguishes databases created within one test binary run.
static COUNTER: AtomicU32 = AtomicU32::new(0);

/// A file-backed SQLite database in the temp directory, deleted on drop.
///
/// File-backed rather than `:memory:` because the scheduler opens its own
/// pooled connections; an in-memory database would give each of them a private,
/// empty schema.
pub struct TempStorage {
    backend: StorageBackend,
    path: PathBuf,
}

impl std::ops::Deref for TempStorage {
    type Target = StorageBackend;

    fn deref(&self) -> &Self::Target {
        &self.backend
    }
}

impl Drop for TempStorage {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
        // SQLite's WAL companions are only present on some configurations.
        for suffix in ["-wal", "-shm"] {
            let mut companion = self.path.clone().into_os_string();
            companion.push(suffix);
            let _ = std::fs::remove_file(PathBuf::from(companion));
        }
    }
}

/// Open a throwaway SQLite backend labelled with `label`.
pub fn temp_storage(label: &str) -> TempStorage {
    let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!(
        "taskito-server-{label}-{}-{unique}.db",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&path);
    let storage = SqliteStorage::new(path.to_string_lossy().as_ref()).expect("open temp SQLite");
    TempStorage {
        backend: StorageBackend::Sqlite(storage),
        path,
    }
}

/// Build dashboard state over `storage`, in the given auth mode.
///
/// Cookies are marked insecure so a test client does not have to speak TLS,
/// and no SPA bundle is attached — the API surface is what these tests drive.
pub fn dashboard_state(storage: &StorageBackend, auth: AuthMode) -> SharedState {
    dashboard_state_with_assets(storage, auth, StaticAssets::new(None))
}

/// [`dashboard_state`] with an explicit SPA source, so asset tests do not
/// depend on whether a bundle happened to be built into this binary.
pub fn dashboard_state_with_assets(
    storage: &StorageBackend,
    auth: AuthMode,
    assets: StaticAssets,
) -> SharedState {
    let workflows = WorkflowStorageBackend::Sqlite(
        WorkflowSqliteStorage::new(match storage {
            StorageBackend::Sqlite(sqlite) => sqlite.clone(),
            #[allow(unreachable_patterns)]
            _ => panic!("this constructor only builds SQLite state; use dashboard_state_for"),
        })
        .expect("workflow tables"),
    );
    dashboard_state_for(storage.clone(), workflows, auth, assets)
}

/// Dashboard state over an already-opened backend pair — the only shape that
/// works for Postgres and Redis, whose workflow stores are their own types.
pub fn dashboard_state_for(
    storage: StorageBackend,
    workflows: WorkflowStorageBackend,
    auth: AuthMode,
    assets: StaticAssets,
) -> SharedState {
    Arc::new(AppState {
        storage,
        workflows,
        dispatcher: None,
        assets,
        config: DashboardConfig {
            bind: "127.0.0.1:0".parse().expect("valid address"),
            auth,
            assets_dir: None,
            metrics_token: None,
            secure_cookies: false,
            admin_bootstrap: None,
            oauth: None,
        },
        oauth: None,
        namespace: None,
        queues: vec!["default".to_string()],
        maintenance: true,
    })
}

/// [`dashboard_state`] in session mode with provider login configured.
pub fn dashboard_state_with_oauth(storage: &StorageBackend, oauth: OAuthConfig) -> SharedState {
    let mut state = dashboard_state(storage, AuthMode::Session);
    let shared = Arc::get_mut(&mut state).expect("the state is not shared yet");
    shared.oauth = Some(Arc::new(OAuthRuntime::new(oauth)));
    state
}

/// A directory holding a throwaway SPA bundle, deleted on drop.
pub struct TempAssets {
    pub path: PathBuf,
}

impl Drop for TempAssets {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// Create an asset directory; `files` are written relative to its root.
pub fn temp_assets(label: &str, files: &[(&str, &str)]) -> TempAssets {
    let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!(
        "taskito-server-{label}-{}-{unique}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&path);
    std::fs::create_dir_all(&path).expect("create the asset directory");
    for (relative, contents) in files {
        let file = path.join(relative);
        if let Some(parent) = file.parent() {
            std::fs::create_dir_all(parent).expect("create the asset subdirectory");
        }
        std::fs::write(file, contents).expect("write an asset");
    }
    TempAssets { path }
}

/// Send one request through the full router — middleware included.
pub async fn call(state: &SharedState, request: Request<Body>) -> (StatusCode, HeaderMap, Value) {
    let response = router(state.clone())
        .oneshot(request)
        .await
        .expect("the router is infallible");
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = axum::body::to_bytes(response.into_body(), 1 << 20)
        .await
        .expect("read the response body");
    // Not every route answers with JSON (the SPA fallback, /metrics); those
    // tests assert on the status and headers instead.
    let body = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, headers, body)
}

/// A GET with no credentials.
pub fn get(path: &str) -> Request<Body> {
    Request::builder()
        .uri(path)
        .body(Body::empty())
        .expect("valid request")
}

/// A request with a JSON body.
pub fn json_request(method: &str, path: &str, body: Value) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(path)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .expect("valid request")
}

/// Poll `condition` until it holds or `timeout` elapses.
///
/// The scheduler is asynchronous by nature — dispatch, execution, and the
/// result drain are separate threads — so tests assert on convergence rather
/// than on a rendezvous that would flake.
pub fn poll_until(timeout: Duration, mut condition: impl FnMut() -> bool) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if condition() {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(25));
    }
    Err(format!("condition did not hold within {timeout:?}"))
}
