//! A stub GitHub: token exchange plus the three API endpoints the flow reads.
//!
//! GitHub publishes no discovery document, so the only way to exercise its
//! exchange is to point the provider's endpoints somewhere controllable.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{json, Value};

/// How the stub should answer the next flow.
#[derive(Clone, Debug)]
pub struct Scenario {
    /// Body of the token exchange.
    pub token_response: Value,
    /// Body of `GET /user`.
    pub user: Value,
    /// Body of `GET /user/emails`.
    pub emails: Value,
    /// Status for `GET /orgs/{org}/members/{login}`: 204 member, 404 not.
    pub membership_status: u16,
}

impl Default for Scenario {
    /// A member of every org, with a verified primary email.
    fn default() -> Self {
        Self {
            token_response: json!({
                "access_token": "stub-access-token",
                "token_type": "bearer",
                "scope": "read:user,user:email",
            }),
            user: json!({
                "id": 4_242,
                "login": "octocat",
                "name": "Octo Cat",
                "avatar_url": "https://example.com/avatar.png",
            }),
            emails: json!([
                { "email": "secondary@example.com", "primary": false, "verified": true },
                { "email": "ops@example.com", "primary": true, "verified": true },
            ]),
            membership_status: 204,
        }
    }
}

impl Scenario {
    /// Primary email present but unverified — never an identity claim.
    pub fn with_unverified_email(mut self) -> Self {
        self.emails = json!([{ "email": "ops@example.com", "primary": true, "verified": false }]);
        self
    }

    /// GitHub's way of reporting a rejected exchange: 200 with an error body.
    pub fn with_token_error(mut self) -> Self {
        self.token_response = json!({
            "error": "bad_verification_code",
            "error_description": "The code passed is incorrect or expired.",
        });
        self
    }

    /// A token response carrying no token at all.
    pub fn without_access_token(mut self) -> Self {
        self.token_response = json!({ "token_type": "bearer" });
        self
    }

    /// `/user` missing the fields the identity is built from.
    pub fn with_unusable_user(mut self) -> Self {
        self.user = json!({ "login": "octocat" });
        self
    }

    /// Not a member of any allowed org.
    pub fn with_membership_status(mut self, status: u16) -> Self {
        self.membership_status = status;
        self
    }
}

struct Inner {
    scenario: Mutex<Scenario>,
    membership_checks: Mutex<Vec<String>>,
}

/// A running stub GitHub. Dropping it stops the server.
pub struct GitHubStub {
    inner: Arc<Inner>,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    /// Base the provider's endpoints should be rooted at.
    pub base_url: String,
}

impl GitHubStub {
    /// Bind an ephemeral port and start serving `scenario`.
    pub async fn start(scenario: Scenario) -> Self {
        let listener = tokio::net::TcpListener::bind::<SocketAddr>("127.0.0.1:0".parse().unwrap())
            .await
            .expect("bind the GitHub stub");
        let base_url = format!("http://{}", listener.local_addr().expect("bound address"));

        let inner = Arc::new(Inner {
            scenario: Mutex::new(scenario),
            membership_checks: Mutex::new(Vec::new()),
        });
        let app = Router::new()
            .route("/login/oauth/access_token", post(token))
            .route("/user", get(user))
            .route("/user/emails", get(emails))
            .route("/orgs/{org}/members/{login}", get(membership))
            .with_state(inner.clone());

        let (shutdown, stopped) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = stopped.await;
                })
                .await;
        });

        Self {
            inner,
            shutdown: Some(shutdown),
            base_url,
        }
    }

    /// Org membership lookups the flow performed, as `org/login`.
    pub fn membership_checks(&self) -> Vec<String> {
        self.inner
            .membership_checks
            .lock()
            .expect("stub lock")
            .clone()
    }
}

impl Drop for GitHubStub {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
    }
}

async fn token(State(inner): State<Arc<Inner>>) -> Json<Value> {
    Json(
        inner
            .scenario
            .lock()
            .expect("stub lock")
            .token_response
            .clone(),
    )
}

async fn user(State(inner): State<Arc<Inner>>) -> Json<Value> {
    Json(inner.scenario.lock().expect("stub lock").user.clone())
}

async fn emails(State(inner): State<Arc<Inner>>) -> Json<Value> {
    Json(inner.scenario.lock().expect("stub lock").emails.clone())
}

async fn membership(
    State(inner): State<Arc<Inner>>,
    Path((org, login)): Path<(String, String)>,
) -> Response {
    inner
        .membership_checks
        .lock()
        .expect("stub lock")
        .push(format!("{org}/{login}"));
    let status = inner.scenario.lock().expect("stub lock").membership_status;
    StatusCode::from_u16(status)
        .unwrap_or(StatusCode::NOT_FOUND)
        .into_response()
}
