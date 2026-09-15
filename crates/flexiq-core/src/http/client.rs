//! The client every operator-configured push target is dialled through.

use std::sync::Arc;
use std::time::Duration;

use super::egress::EgressPolicy;
use super::resolver::PinnedResolver;
use crate::worker::http_target::HttpTargetError;

/// The client every operator-configured URL is dialled through.
///
/// There is no `Default` and no second constructor: a policy is not optional,
/// and a client that could be built without one is a client someone will
/// build without one.
///
/// Ignores `HTTP_PROXY`/`HTTPS_PROXY`/`ALL_PROXY` even though reqwest reads
/// them by default: a proxy resolves the target itself, which moves the
/// lookup outside the pinned resolver above and defeats the pinning this
/// type exists for. A deployment behind a mandatory egress proxy cannot use
/// push dispatch through this client until a configured, allowlisted proxy
/// is a designed feature — a clear connection failure beats a silent bypass.
pub struct DispatchClient {
    client: reqwest::Client,
}

impl DispatchClient {
    /// Build a client that resolves through `policy` and gives up on
    /// connecting after `connect_timeout`.
    pub fn new(
        policy: Arc<EgressPolicy>,
        connect_timeout: Duration,
    ) -> Result<Self, HttpTargetError> {
        let client = reqwest::Client::builder()
            .dns_resolver(Arc::new(PinnedResolver::new(policy)))
            // A 3xx could carry a signed body to a host that never passed the guard.
            .redirect(reqwest::redirect::Policy::none())
            // reqwest retries a protocol NACK twice by default. Those never
            // reached the application, but a second retry loop inside the
            // client is a second retry policy, and this crate already has one.
            .retry(reqwest::retry::never())
            // A proxy resolves the target itself, which moves the lookup
            // outside the resolver above and defeats the pinning. Honouring
            // one would let an environment variable silently disable an
            // SSRF control.
            .no_proxy()
            .connect_timeout(connect_timeout)
            // No client-wide timeout: the budget is per request and is
            // derived from the job's own execution timeout, so it cannot live
            // here.
            .build()
            .map_err(|error| HttpTargetError::Client(error.to_string()))?;
        Ok(Self { client })
    }

    /// The underlying client, for the dispatcher that builds requests on it.
    // No caller yet: the dispatcher that builds requests on this client
    // arrives in a later commit.
    #[allow(dead_code)]
    pub(crate) fn inner(&self) -> &reqwest::Client {
        &self.client
    }
}

/// Read at most `cap` bytes of a response, reporting whether it was truncated.
///
/// `bytes()` would buffer whatever the endpoint chose to send before any cap
/// applied. Streaming stops as soon as the budget is spent.
// No caller yet: the dispatcher that reads a target's response arrives in a
// later commit. Covered directly by this file's tests in the meantime.
#[allow(dead_code)]
pub(crate) async fn read_bounded(response: reqwest::Response, cap: usize) -> (Vec<u8>, bool) {
    let mut response = response;
    let mut buffered: Vec<u8> = Vec::new();
    let mut truncated = false;
    loop {
        match response.chunk().await {
            Ok(Some(chunk)) => {
                let room = cap.saturating_sub(buffered.len());
                if chunk.len() > room {
                    truncated = true;
                }
                buffered.extend_from_slice(&chunk[..chunk.len().min(room)]);
                // Checked against the length just written, not the `room`
                // computed above it: a chunk that exactly fills the
                // remaining budget must stop here, not await one more
                // `chunk()` call to discover there is no room left.
                if buffered.len() == cap {
                    break;
                }
            }
            Ok(None) => break,
            // Whatever arrived is still useful (mirrors `flexiq-server`'s
            // `webhook_sender::read_bounded`), but unlike a clean end of
            // body this is not the complete response — the caller cannot
            // tell the difference unless `truncated` says so.
            Err(_) => {
                truncated = true;
                break;
            }
        }
    }
    (buffered, truncated)
}

#[cfg(test)]
mod tests {
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;

    use super::*;
    use crate::net::Allowlist;

    #[test]
    fn a_client_builds_from_a_policy() {
        let policy = Arc::new(EgressPolicy::new(
            Allowlist::parse("93.184.216.34").expect("test allowlist parses"),
            false,
        ));

        let client = DispatchClient::new(policy, Duration::from_secs(5));

        assert!(
            client.is_ok(),
            "a policy and a timeout are enough to build a client"
        );
    }

    /// Answers one HTTP/1.1 request on loopback with `body` and a matching
    /// `Content-Length`, then closes. Loopback and a kernel-assigned port, so
    /// this needs no external network and cannot collide with another test.
    async fn serve_once(body: Vec<u8>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("loopback listener binds");
        let addr = listener.local_addr().expect("listener has a local address");
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("test client connects");
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            socket
                .write_all(header.as_bytes())
                .await
                .expect("header writes");
            socket.write_all(&body).await.expect("body writes");
            socket.shutdown().await.expect("socket shuts down cleanly");
        });
        format!("http://{addr}/")
    }

    #[tokio::test]
    async fn read_bounded_stops_at_the_cap_and_reports_truncation() {
        let url = serve_once(vec![b'x'; 100]).await;
        let response = reqwest::Client::new()
            .get(url)
            .send()
            .await
            .expect("the local server answers");

        let (bytes, truncated) = read_bounded(response, 10).await;

        assert_eq!(bytes.len(), 10);
        assert!(truncated);
    }

    #[tokio::test]
    async fn read_bounded_reports_no_truncation_when_the_body_fits() {
        let body = b"short body".to_vec();
        let url = serve_once(body.clone()).await;
        let response = reqwest::Client::new()
            .get(url)
            .send()
            .await
            .expect("the local server answers");

        let (bytes, truncated) = read_bounded(response, 1024).await;

        assert_eq!(bytes, body);
        assert!(!truncated);
    }
}
