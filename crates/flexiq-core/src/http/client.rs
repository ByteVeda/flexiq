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

    /// The underlying client, for a caller that must dial through the same
    /// guard without building a fresh `reqwest::Client` of its own.
    ///
    /// Two callers: `oidc::oauth2`'s OAuth2 token fetch, which clones the
    /// client out rather than holding a whole `DispatchClient` because the
    /// operator's token URL needs exactly the same egress guard the dispatch
    /// target does (see `oidc/oauth2.rs`'s module doc); and
    /// `worker::http_target::attempt`, which builds the push dispatch itself
    /// on it.
    pub(crate) fn inner(&self) -> &reqwest::Client {
        &self.client
    }
}

/// Read at most `cap` bytes of a response, reporting whether it was truncated.
///
/// `bytes()` would buffer whatever the endpoint chose to send before any cap
/// applied. Streaming stops as soon as the budget is spent.
///
/// # Two constraints the loop has to satisfy at once
///
/// 1. **Never buffer past `cap`.** The cap is the memory bound; a loop that
///    appended a whole chunk and trimmed afterwards would have already held
///    `cap + chunk` bytes at once.
/// 2. **Never report `truncated: false` without having seen the end of the
///    body.** A body that exactly fills `cap` is indistinguishable from the
///    first `cap` bytes of a longer one until something further is read, and
///    the caller turns this flag into a refusal, so guessing is not available.
///
/// They pull opposite ways, and the resolution is that the loop reads *one
/// byte* past the cap and keeps none of it: a chunk longer than the remaining
/// room is appended only up to that room, and its existence is what sets
/// `truncated`. So a body of exactly `cap` costs one extra `chunk()` await
/// that comes back `Ok(None)` and reports `false`, and a body of `cap + 1`
/// reports `true` having buffered exactly `cap`.
pub(crate) async fn read_bounded(response: reqwest::Response, cap: usize) -> (Vec<u8>, bool) {
    let mut response = response;
    let mut buffered: Vec<u8> = Vec::new();
    let mut truncated = false;
    loop {
        match response.chunk().await {
            Ok(Some(chunk)) => {
                let room = cap.saturating_sub(buffered.len());
                if chunk.len() > room {
                    // Keep what fits and drop the rest unexamined: the
                    // overflow is evidence that the body continues past the
                    // cap, and evidence is the only use it has. `room` is `0`
                    // once the budget is already spent, so this appends
                    // nothing and the slice is still in bounds.
                    buffered.extend_from_slice(&chunk[..room]);
                    truncated = true;
                    break;
                }
                buffered.extend_from_slice(&chunk);
            }
            // The one answer that proves nothing follows, and so the only one
            // that may leave `truncated` false.
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

    /// Regression: the loop used to stop the moment `buffered.len() == cap`,
    /// so a body that exactly filled the cap reported `truncated: false`
    /// whether or not more followed. The caller turns that flag into a
    /// refusal, so the two cases below must not be reported the same way.
    #[tokio::test]
    async fn read_bounded_distinguishes_a_body_that_exactly_fills_the_cap() {
        let exact = serve_once(vec![b'x'; 10]).await;
        let response = reqwest::Client::new()
            .get(exact)
            .send()
            .await
            .expect("the local server answers");

        let (bytes, truncated) = read_bounded(response, 10).await;

        assert_eq!(bytes.len(), 10);
        assert!(
            !truncated,
            "the body ended at the cap, which is a complete answer"
        );

        let one_more = serve_once(vec![b'x'; 11]).await;
        let response = reqwest::Client::new()
            .get(one_more)
            .send()
            .await
            .expect("the local server answers");

        let (bytes, truncated) = read_bounded(response, 10).await;

        assert_eq!(
            bytes.len(),
            10,
            "the overflow byte is evidence, never something to buffer"
        );
        assert!(truncated, "one byte past the cap is still past the cap");
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
