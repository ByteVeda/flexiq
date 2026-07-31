//! Session authentication: users, sessions, CSRF, and role checks.
//!
//! Everything persists under `auth:` keys in the settings store, in the same
//! layout every SDK dashboard uses — a session created by one is accepted by
//! any of them against the same backend.

pub mod bootstrap;
pub mod context;
pub mod cookies;
pub mod gate;
pub mod middleware;
pub mod model;
pub mod oauth;
pub mod password;
pub mod routes;
pub mod store;
pub mod throttle;

use axum::routing::{get, post};
use axum::Router;

use crate::dashboard::state::SharedState;

/// The session endpoints.
pub fn router() -> Router<SharedState> {
    Router::new()
        .route("/api/auth/status", get(routes::status))
        .route("/api/auth/setup", post(routes::setup))
        .route("/api/auth/login", post(routes::login))
        .route("/api/auth/logout", post(routes::logout))
        .route("/api/auth/whoami", get(routes::whoami))
        .route("/api/auth/change-password", post(routes::change_password))
        .merge(oauth::router())
}
