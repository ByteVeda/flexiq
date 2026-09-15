//! A scriptable, minimal HTTP/1.1 stub for this module's own tests — the
//! delivery path (headers, query, body, status) is only observable from the
//! receiving end, and this crate has no other server to be that end.
//!
//! Deliberately not a real server. It reads one request, answers it, and
//! closes the connection: no chunked transfer encoding, no keep-alive
//! negotiation, no pipelining, no HTTP/2. `flexiq-core` carries no `axum` and
//! gets none for a `#[cfg(test)]`-only stub — see
//! `crates/flexiq-server/tests/support/webhook_receiver.rs` for the
//! equivalent built on one, in a crate that already depends on it. Do not
//! reach for this module to test anything that needs more than "receive a
//! request, answer with a scripted status and body."

use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// One request the stub received.
#[derive(Clone)]
pub(crate) struct Received {
    pub(crate) method: String,
    /// Path plus query, exactly as sent on the request line.
    pub(crate) target: String,
    /// Header names lowercased; values as sent.
    pub(crate) headers: Vec<(String, String)>,
    pub(crate) body: Vec<u8>,
}

/// What the stub answers with, for the next request.
enum Script {
    /// Every request gets the same answer.
    Fixed(u16, String),
    /// Requests are answered in order; once the list is exhausted, the last
    /// entry repeats.
    Sequence(Vec<(u16, String)>),
}

impl Script {
    fn response_for(&self, index: usize) -> (u16, String) {
        match self {
            Script::Fixed(status, body) => (*status, body.clone()),
            Script::Sequence(responses) => {
                let bounded = index.min(responses.len() - 1);
                responses[bounded].clone()
            }
        }
    }
}

struct Inner {
    script: Script,
    /// The response index and this list's length must agree: under
    /// concurrent connections, a response chosen from a separately
    /// incremented counter could pair with the wrong recorded request.
    /// Deriving the index from `received.len()` while holding this same
    /// lock is what keeps the two atomic with each other.
    received: Mutex<Vec<Received>>,
}

/// A scriptable HTTP/1.1 server on an ephemeral loopback port.
pub(crate) struct StubServer {
    inner: Arc<Inner>,
    base_url: String,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
}

impl StubServer {
    /// Bind `127.0.0.1:0` and answer every request with `status` and `body`.
    pub(crate) async fn start(status: u16, body: impl Into<String>) -> Self {
        Self::start_with(Script::Fixed(status, body.into())).await
    }

    /// Answer each request from the queue in turn, then repeat the last.
    pub(crate) async fn start_scripted(responses: Vec<(u16, String)>) -> Self {
        assert!(
            !responses.is_empty(),
            "a scripted stub needs at least one response"
        );
        Self::start_with(Script::Sequence(responses)).await
    }

    async fn start_with(script: Script) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("stub binds an ephemeral loopback port");
        let base_url = format!(
            "http://{}",
            listener
                .local_addr()
                .expect("bound listener has a local address")
        );

        let inner = Arc::new(Inner {
            script,
            received: Mutex::new(Vec::new()),
        });

        let (shutdown, mut stopped) = tokio::sync::oneshot::channel();
        let accept_inner = Arc::clone(&inner);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = &mut stopped => break,
                    accepted = listener.accept() => {
                        let Ok((socket, _)) = accepted else { break };
                        tokio::spawn(serve_one(socket, Arc::clone(&accept_inner)));
                    }
                }
            }
        });

        Self {
            inner,
            base_url,
            shutdown: Some(shutdown),
        }
    }

    /// `http://127.0.0.1:<port>`.
    pub(crate) fn base_url(&self) -> String {
        self.base_url.clone()
    }

    /// Everything received so far, oldest first.
    pub(crate) fn received(&self) -> Vec<Received> {
        self.inner.received.lock().expect("stub lock").clone()
    }

    /// How many requests have arrived, without cloning every body received
    /// so far the way [`Self::received`] does.
    pub(crate) fn request_count(&self) -> usize {
        self.inner.received.lock().expect("stub lock").len()
    }
}

impl Drop for StubServer {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
    }
}

/// A header block larger than this is not a request any test here sends; a
/// bound exists so a malformed request cannot hang the accept loop forever.
const MAX_HEAD_BYTES: usize = 64 * 1024;

async fn serve_one(mut socket: TcpStream, inner: Arc<Inner>) {
    let Some((request_line, headers, mut body)) = read_head(&mut socket).await else {
        return;
    };
    let Some((method, target)) = parse_request_line(&request_line) else {
        return;
    };

    let content_length = headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, value)| value.trim().parse::<usize>().ok())
        .unwrap_or(0);

    let mut chunk = [0u8; 4096];
    while body.len() < content_length {
        match socket.read(&mut chunk).await {
            Ok(0) | Err(_) => break,
            Ok(n) => body.extend_from_slice(&chunk[..n]),
        }
    }
    body.truncate(content_length);

    let received = Received {
        method,
        target,
        headers: headers
            .into_iter()
            .map(|(name, value)| (name.to_ascii_lowercase(), value))
            .collect(),
        body,
    };

    // The index the script answers from and this request's position in
    // `received` are read and written under the same lock, so the two can
    // never disagree about which request a scripted response belongs to.
    let (status, response_body) = {
        let mut guard = inner.received.lock().expect("stub lock");
        let index = guard.len();
        let response = inner.script.response_for(index);
        guard.push(received);
        response
    };

    let head = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Length: {len}\r\nConnection: close\r\n\r\n",
        reason = reason_phrase(status),
        len = response_body.len(),
    );
    let _ = socket.write_all(head.as_bytes()).await;
    let _ = socket.write_all(response_body.as_bytes()).await;
    let _ = socket.shutdown().await;
}

/// Reads up to and including the blank line ending the header block, then
/// returns the request line, the parsed headers, and whatever body bytes
/// were already buffered past that point (there is no framing between the
/// header read and the body: TCP does not deliver them separately).
async fn read_head(socket: &mut TcpStream) -> Option<(String, Vec<(String, String)>, Vec<u8>)> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    let header_end = loop {
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break pos;
        }
        if buf.len() > MAX_HEAD_BYTES {
            return None;
        }
        match socket.read(&mut chunk).await {
            Ok(0) | Err(_) => return None,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
        }
    };

    let head = String::from_utf8_lossy(&buf[..header_end]).into_owned();
    let body = buf[header_end + 4..].to_vec();

    let mut lines = head.split("\r\n");
    let request_line = lines.next()?.to_string();
    let headers = lines
        .filter(|line| !line.is_empty())
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.trim().to_string(), value.trim().to_string()))
        .collect();

    Some((request_line, headers, body))
}

fn parse_request_line(line: &str) -> Option<(String, String)> {
    let mut parts = line.split(' ');
    let method = parts.next()?.to_string();
    let target = parts.next()?.to_string();
    Some((method, target))
}

/// Reason phrases for the statuses this crate's tests actually script.
/// HTTP/1.1 does not require this text to mean anything to a client — every
/// caller here reads the status code, not the phrase — so the fallback for
/// anything else is a placeholder rather than an exhaustive table.
fn reason_phrase(status: u16) -> &'static str {
    match status {
        200 => "OK",
        201 => "Created",
        204 => "No Content",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        410 => "Gone",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        503 => "Service Unavailable",
        _ => "Stub",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn the_stub_records_method_target_headers_and_body_faithfully() {
        let stub = StubServer::start(200, "ok").await;

        let response = reqwest::Client::new()
            .post(format!("{}/widgets?x=1", stub.base_url()))
            .header("x-test-header", "value-1")
            .body("hello-body")
            .send()
            .await
            .expect("the stub answers");
        assert_eq!(response.status().as_u16(), 200);
        assert_eq!(response.text().await.expect("body reads"), "ok");

        assert_eq!(stub.request_count(), 1);
        let received = stub.received();
        let request = &received[0];
        assert_eq!(request.method, "POST");
        assert_eq!(request.target, "/widgets?x=1");
        assert!(request
            .headers
            .iter()
            .any(|(name, value)| name == "x-test-header" && value == "value-1"));
        assert_eq!(request.body, b"hello-body");
    }
}
