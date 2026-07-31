//! A stub OpenID provider: discovery, JWKS, and a token endpoint that mints
//! `id_token`s signed with a fixture RSA key.
//!
//! Everything the real flow depends on but unit tests cannot reach —
//! signature verification, `iss`/`aud`/`exp` enforcement, and the nonce
//! binding — only runs against a live issuer. This is that issuer, without
//! the network.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use axum::extract::State;
use axum::routing::{get, post};
use axum::{Json, Router};
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use serde_json::{json, Value};

/// Signing key the stub uses. Test fixture only — never a real credential.
const SIGNING_KEY_PEM: &[u8] = include_bytes!("../fixtures/oidc_test_key.pem");
/// Its public half, in the shape a provider publishes.
const JWKS: &str = include_str!("../fixtures/oidc_test_jwks.json");
/// `kid` of the key in that set.
pub const KEY_ID: &str = "test-key";

/// What the next token exchange should hand back.
#[derive(Clone)]
pub struct NextToken {
    /// Claims to sign. The test mutates these to forge each failure case.
    pub claims: Value,
    /// `kid` written into the JWT header.
    pub key_id: Option<String>,
    /// Corrupt the signature after signing, to prove verification is real.
    pub tamper: bool,
}

impl NextToken {
    /// A well-formed token for `issuer`/`audience` that expires in an hour.
    pub fn valid(issuer: &str, audience: &str, nonce: &str) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("the clock is after 1970")
            .as_secs();
        Self {
            claims: json!({
                "iss": issuer,
                "aud": audience,
                "sub": "provider-subject-1",
                "exp": now + 3_600,
                "iat": now,
                "nonce": nonce,
                "email": "ops@example.com",
                "email_verified": true,
                "name": "Ops Person",
            }),
            key_id: Some(KEY_ID.to_string()),
            tamper: false,
        }
    }

    /// Overwrite one claim, for the negative cases.
    pub fn with_claim(mut self, name: &str, value: Value) -> Self {
        self.claims[name] = value;
        self
    }

    /// Sign with a `kid` the published set does not contain.
    pub fn with_unknown_key_id(mut self) -> Self {
        self.key_id = Some("rotated-away".to_string());
        self
    }

    /// Return a token whose signature does not match its payload.
    pub fn tampered(mut self) -> Self {
        self.tamper = true;
        self
    }
}

struct Issuer {
    base_url: String,
    next: Mutex<Option<NextToken>>,
    exchanges: std::sync::atomic::AtomicUsize,
}

/// A running stub issuer. Dropping it stops the server.
pub struct StubIssuer {
    issuer: Arc<Issuer>,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    /// Where the provider is reachable, e.g. `http://127.0.0.1:41234`.
    pub base_url: String,
}

impl StubIssuer {
    /// Bind an ephemeral port and start serving.
    pub async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind::<SocketAddr>("127.0.0.1:0".parse().unwrap())
            .await
            .expect("bind the stub issuer");
        let base_url = format!(
            "http://{}",
            listener.local_addr().expect("the bound address")
        );

        let issuer = Arc::new(Issuer {
            base_url: base_url.clone(),
            next: Mutex::new(None),
            exchanges: std::sync::atomic::AtomicUsize::new(0),
        });
        let app = Router::new()
            .route("/.well-known/openid-configuration", get(discovery))
            .route("/jwks", get(jwks))
            .route("/token", post(token))
            .with_state(issuer.clone());

        let (shutdown, stopped) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = stopped.await;
                })
                .await;
        });

        Self {
            issuer,
            shutdown: Some(shutdown),
            base_url,
        }
    }

    /// URL of the discovery document, for `ProviderConfig::discovery_url`.
    pub fn discovery_url(&self) -> String {
        format!("{}/.well-known/openid-configuration", self.base_url)
    }

    /// Value the flow must see in `iss`.
    pub fn issuer_url(&self) -> String {
        self.base_url.clone()
    }

    /// Arm the next token exchange.
    pub fn expect_exchange(&self, token: NextToken) {
        *self.issuer.next.lock().expect("stub lock") = Some(token);
    }

    /// How many token exchanges actually reached the stub.
    ///
    /// A negative test that never gets here proves nothing about verification —
    /// it would only prove the flow died earlier.
    pub fn exchanges(&self) -> usize {
        self.issuer
            .exchanges
            .load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl Drop for StubIssuer {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
    }
}

async fn discovery(State(issuer): State<Arc<Issuer>>) -> Json<Value> {
    Json(json!({
        "issuer": issuer.base_url,
        "authorization_endpoint": format!("{}/authorize", issuer.base_url),
        "token_endpoint": format!("{}/token", issuer.base_url),
        "jwks_uri": format!("{}/jwks", issuer.base_url),
    }))
}

async fn jwks() -> Json<Value> {
    Json(serde_json::from_str(JWKS).expect("the fixture JWKS parses"))
}

async fn token(State(issuer): State<Arc<Issuer>>) -> Json<Value> {
    issuer
        .exchanges
        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let armed = issuer
        .next
        .lock()
        .expect("stub lock")
        .take()
        .expect("a test armed the stub before triggering the exchange");

    let mut header = Header::new(Algorithm::RS256);
    header.kid = armed.key_id.clone();
    let key = EncodingKey::from_rsa_pem(SIGNING_KEY_PEM).expect("the fixture key parses");
    let mut id_token = jsonwebtoken::encode(&header, &armed.claims, &key).expect("sign the token");

    if armed.tamper {
        // Flip one byte of the signature; header and payload stay intact, so
        // only signature verification can catch this.
        let signature_start = id_token.rfind('.').expect("a JWT has three parts") + 1;
        let flipped = match id_token.as_bytes()[signature_start] {
            b'A' => 'B',
            _ => 'A',
        };
        id_token.replace_range(signature_start..signature_start + 1, &flipped.to_string());
    }

    Json(json!({
        "access_token": "stub-access-token",
        "token_type": "Bearer",
        "expires_in": 3_600,
        "id_token": id_token,
    }))
}
