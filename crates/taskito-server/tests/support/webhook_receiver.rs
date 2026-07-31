//! A stub webhook endpoint that records what it was sent.
//!
//! The delivery path — signing, headers, the body on the wire — is only
//! observable from the receiving end, so tests need one.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, Mutex};

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::Router;
use serde_json::Value;

/// One request the stub received.
#[derive(Clone, Debug)]
pub struct Received {
    /// Headers as sent, lowercased names.
    pub headers: Vec<(String, String)>,
    /// Raw body bytes, before parsing.
    pub raw_body: String,
    /// Parsed body, when it was JSON.
    pub body: Option<Value>,
}

impl Received {
    /// One header's value.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }
}

struct Inner {
    received: Mutex<Vec<Received>>,
    status: AtomicU16,
}

/// A running stub endpoint. Dropping it stops the server.
pub struct WebhookReceiver {
    inner: Arc<Inner>,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    /// URL to configure a subscription with.
    pub url: String,
}

impl WebhookReceiver {
    /// Bind an ephemeral port and start accepting deliveries.
    pub async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind::<SocketAddr>("127.0.0.1:0".parse().unwrap())
            .await
            .expect("bind the webhook receiver");
        let url = format!(
            "http://{}/hook",
            listener.local_addr().expect("the bound address")
        );

        let inner = Arc::new(Inner {
            received: Mutex::new(Vec::new()),
            status: AtomicU16::new(200),
        });
        let app = Router::new()
            .route("/hook", post(receive))
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
            url,
        }
    }

    /// Answer subsequent deliveries with `status`.
    pub fn respond_with(&self, status: u16) {
        self.inner.status.store(status, Ordering::SeqCst);
    }

    /// Everything received so far, oldest first.
    pub fn received(&self) -> Vec<Received> {
        self.inner.received.lock().expect("receiver lock").clone()
    }
}

impl Drop for WebhookReceiver {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
    }
}

async fn receive(State(inner): State<Arc<Inner>>, headers: HeaderMap, body: String) -> StatusCode {
    inner
        .received
        .lock()
        .expect("receiver lock")
        .push(Received {
            headers: headers
                .iter()
                .map(|(name, value)| {
                    (
                        name.as_str().to_ascii_lowercase(),
                        value.to_str().unwrap_or_default().to_string(),
                    )
                })
                .collect(),
            body: serde_json::from_str(&body).ok(),
            raw_body: body,
        });
    StatusCode::from_u16(inner.status.load(Ordering::SeqCst)).unwrap_or(StatusCode::OK)
}
