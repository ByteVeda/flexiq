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
    /// The same policy [`PinnedResolver`] resolves through, kept so a caller
    /// that has to vet a URL *before* dialling it can reach the one this
    /// client was actually built with rather than being handed a second copy
    /// to keep in step. The resolver closes the rebinding window for a name;
    /// it is never reached at all for an IP literal, which is why a
    /// construction-time check needs this. See [`Self::policy`].
    policy: Arc<EgressPolicy>,
}

impl DispatchClient {
    /// Build a client that resolves through `policy` and gives up on
    /// connecting after `connect_timeout`.
    pub fn new(
        policy: Arc<EgressPolicy>,
        connect_timeout: Duration,
    ) -> Result<Self, HttpTargetError> {
        let client = reqwest::Client::builder()
            .dns_resolver(Arc::new(PinnedResolver::new(Arc::clone(&policy))))
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
        Ok(Self { client, policy })
    }

    /// The egress policy this client resolves through.
    ///
    /// For a caller that must vet an operator-supplied URL at *construction*,
    /// which the resolver cannot do for it: an IP-literal host never reaches a
    /// `Resolve` impl — the connector dials it directly — so a literal would
    /// otherwise pass no allowlist check at all. `worker::http_target`'s
    /// `validate_target_url` uses `EgressPolicy::permits_host` for exactly
    /// this on the dispatch URL; `oidc::oauth2` reaches it through here for
    /// the OAuth2 token URL, the other operator-supplied host in this
    /// subsystem.
    pub(crate) fn policy(&self) -> &EgressPolicy {
        &self.policy
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

/// How a bounded body read ended.
///
/// Three outcomes, not two, because the caller's response to each differs and
/// two of them are not failures of the same kind. Collapsing the last two into
/// one boolean is what made a broken connection settle as
/// `Refusal::ResponseTooLarge` — permanently, under a reason that was also
/// untrue.
pub(crate) enum BodyRead {
    /// The body ended inside the cap. These are all of it.
    Complete(Vec<u8>),
    /// More body followed the cap. The bytes read are deliberately not carried:
    /// a partial body is not what the target said, and the caller refuses
    /// rather than storing half an answer.
    Truncated,
    /// The connection broke part-way through the body. Carries reqwest's
    /// message, already stripped of the URL it was dialling.
    Broken(String),
}

/// Read at most `cap` bytes of a response.
///
/// `bytes()` would buffer whatever the endpoint chose to send before any cap
/// applied. Streaming stops as soon as the budget is spent.
///
/// # Two constraints the loop has to satisfy at once
///
/// 1. **Never buffer past `cap`.** The cap is the memory bound; a loop that
///    appended a whole chunk and trimmed afterwards would have already held
///    `cap + chunk` bytes at once.
/// 2. **Never answer [`BodyRead::Complete`] without having seen the end of the
///    body.** A body that exactly fills `cap` is indistinguishable from the
///    first `cap` bytes of a longer one until something further is read, and
///    the caller turns this answer into a job result, so guessing is not
///    available.
///
/// They pull opposite ways, and the resolution is that the loop reads *one
/// byte* past the cap and keeps none of it: a chunk longer than the remaining
/// room is appended only up to that room, and its existence is what proves the
/// body continues. So a body of exactly `cap` costs one extra `chunk()` await
/// that comes back `Ok(None)` and answers `Complete`, and a body of `cap + 1`
/// answers `Truncated`.
pub(crate) async fn read_bounded(response: reqwest::Response, cap: usize) -> BodyRead {
    let mut response = response;
    let mut buffered: Vec<u8> = Vec::new();
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
                    return BodyRead::Truncated;
                }
                buffered.extend_from_slice(&chunk);
            }
            // The one answer that proves nothing follows, and so the only one
            // that may report a complete body.
            Ok(None) => return BodyRead::Complete(buffered),
            // Reported apart from `Truncated`, not folded into it: the body did
            // not exceed anything, the connection went away, and that is worth
            // another attempt where an oversized body is not. `without_url`
            // for the reason `Refusal::Transport`'s own call site gives — the
            // URL reqwest interpolates is where an operator's query-string
            // credential would be.
            Err(error) => return BodyRead::Broken(error.without_url().to_string()),
        }
    }
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
        serve_once_declaring(body, 0).await
    }

    /// As [`serve_once`], but advertises `missing` bytes more than it sends,
    /// so the peer sees the body stop before its `Content-Length` and reports
    /// a read error rather than a clean end of body.
    async fn serve_once_declaring(body: Vec<u8>, missing: usize) -> String {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("loopback listener binds");
        let addr = listener.local_addr().expect("listener has a local address");
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("test client connects");
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len() + missing
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

    /// The bytes of a `Complete` read, or a panic naming what came back
    /// instead. `BodyRead` carries no `Debug` — one variant holds a response
    /// body — so `assert!(matches!(..))` is the idiom everywhere else here.
    fn complete(read: BodyRead) -> Vec<u8> {
        match read {
            BodyRead::Complete(bytes) => bytes,
            BodyRead::Truncated => panic!("expected a complete body, got Truncated"),
            BodyRead::Broken(error) => panic!("expected a complete body, got Broken({error})"),
        }
    }

    #[tokio::test]
    async fn read_bounded_stops_at_the_cap_and_reports_truncation() {
        let url = serve_once(vec![b'x'; 100]).await;
        let response = reqwest::Client::new()
            .get(url)
            .send()
            .await
            .expect("the local server answers");

        assert!(matches!(
            read_bounded(response, 10).await,
            BodyRead::Truncated
        ));
    }

    /// Regression: the loop used to stop the moment `buffered.len() == cap`,
    /// so a body that exactly filled the cap reported `truncated: false`
    /// whether or not more followed. The caller turns that answer into a job
    /// result, so the two cases below must not be reported the same way.
    #[tokio::test]
    async fn read_bounded_distinguishes_a_body_that_exactly_fills_the_cap() {
        let exact = serve_once(vec![b'x'; 10]).await;
        let response = reqwest::Client::new()
            .get(exact)
            .send()
            .await
            .expect("the local server answers");

        let bytes = complete(read_bounded(response, 10).await);
        assert_eq!(
            bytes.len(),
            10,
            "the body ended at the cap, which is a complete answer"
        );

        let one_more = serve_once(vec![b'x'; 11]).await;
        let response = reqwest::Client::new()
            .get(one_more)
            .send()
            .await
            .expect("the local server answers");

        assert!(
            matches!(read_bounded(response, 10).await, BodyRead::Truncated),
            "one byte past the cap is still past the cap"
        );
    }

    /// Regression: a body that broke mid-read used to be reported as
    /// truncation, which `attempt.rs` settles as `Refusal::ResponseTooLarge`
    /// — non-retryable, and untrue. The body exceeded nothing; the connection
    /// went away.
    #[tokio::test]
    async fn read_bounded_reports_a_broken_body_apart_from_truncation() {
        // Well inside the cap, so `Truncated` is not even a candidate: the
        // only reason this is not `Complete` is the connection.
        let url = serve_once_declaring(b"half a bo".to_vec(), 64).await;
        let response = reqwest::Client::new()
            .get(url)
            .send()
            .await
            .expect("the local server answers");

        assert!(matches!(
            read_bounded(response, 1024).await,
            BodyRead::Broken(_)
        ));
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

        assert_eq!(complete(read_bounded(response, 1024).await), body);
    }
}
