//! Shared state every route handler is given.

use std::sync::Arc;

use taskito_core::{RemoteDispatcher, StorageBackend};
use taskito_workflows::WorkflowStorageBackend;

use crate::config::dashboard::DashboardConfig;
use crate::dashboard::auth::oauth::providers::OAuthRuntime;
use crate::dashboard::auth::throttle::LoginThrottle;
use crate::dashboard::static_assets::StaticAssets;

/// Everything the dashboard needs to answer a request.
pub struct AppState {
    /// Job storage — the source of truth for every view.
    pub storage: StorageBackend,
    /// Workflow storage, for the workflow run views.
    pub workflows: WorkflowStorageBackend,
    /// Attached-executor registry. `None` when the attach listener is off.
    pub dispatcher: Option<RemoteDispatcher>,
    /// SPA bundle.
    pub assets: StaticAssets,
    /// Bind address, auth mode, cookie policy.
    pub config: DashboardConfig,
    /// Provider login, when any provider is configured.
    pub oauth: Option<Arc<OAuthRuntime>>,
    /// Failed-login counters, so password guessing is not free.
    pub login_throttle: LoginThrottle,
    /// Tenant namespace listings are scoped to, matching the scheduler.
    pub namespace: Option<String>,
    /// Queues the scheduler is configured for, so they appear before they
    /// have ever had a job.
    pub queues: Vec<String>,
    /// Whether this process runs retention, which the retention views report.
    pub maintenance: bool,
}

/// The state handle axum clones per request.
pub type SharedState = Arc<AppState>;
