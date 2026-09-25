//! CloudEvents over HTTP, through push dispatch's egress guard.
//!
//! One `POST` per batch. With `max_batch` 1 the body is one event in
//! structured mode (`application/cloudevents+json`); above 1 it is always a
//! JSON array (`application/cloudevents-batch+json`), even of one, so a
//! receiver sees a single content type per sink.
//!
//! # Signing
//!
//! With `hmac_secret_env` set, each request carries
//! `x-flexiq-event-timestamp: <unix ms>` and
//! `x-flexiq-event-signature: v1=<hex HMAC-SHA256(secret, "<timestamp>.<body>")>`.
//! Deliberately neither the webhook `X-Flexiq-Signature` nor the dispatch
//! `x-flexiq-dispatch-*` scheme: different signed bytes under a shared name
//! would let one verifier accept the other's messages.

use std::error::Error as _;
use std::sync::Arc;
use std::time::Duration;

use reqwest::header::{HeaderMap, HeaderName, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use serde_json::Value;
use tokio::runtime::{Builder, Runtime};

use super::{DeliveryResult, SinkBackend};
use crate::events::config::{EventsConfigError, HttpSinkConfig};
use crate::events::event::JobEvent;
use crate::http::auth::digest::hmac_sha256_hex;
use crate::http::{DispatchClient, EgressPolicy};
use crate::job::now_millis;
use crate::net::Allowlist;
use crate::worker::http_target::{validate_target_url, HttpTargetError};
use crate::worker::Secret;

const STRUCTURED: &str = "application/cloudevents+json";
const BATCHED: &str = "application/cloudevents-batch+json";
const TIMESTAMP_HEADER: HeaderName = HeaderName::from_static("x-flexiq-event-timestamp");
const SIGNATURE_HEADER: HeaderName = HeaderName::from_static("x-flexiq-event-signature");

/// The `http` sink kind.
pub(crate) struct HttpSink {
    source: String,
    url: url::Url,
    client: DispatchClient,
    bearer: Option<Secret>,
    hmac: Option<Secret>,
    timeout: Duration,
    include_payload: bool,
    batched: bool,
    /// Built on the sink thread at first delivery, not here: a runtime
    /// dropped inside an async context panics, and a hub can be started, and
    /// fail part-way, from one.
    runtime: Option<Runtime>,
}

impl HttpSink {
    /// Build from config, reading secrets from the process environment.
    pub(crate) fn new(config: &HttpSinkConfig, source: &str) -> Result<Self, EventsConfigError> {
        Self::with_env(config, source, |name| std::env::var(name).ok())
    }

    /// Build from config, reading secrets through `env`.
    fn with_env(
        config: &HttpSinkConfig,
        source: &str,
        env: impl Fn(&str) -> Option<String>,
    ) -> Result<Self, EventsConfigError> {
        let fail = |message: String| EventsConfigError::Sink {
            sink: config.name.clone(),
            message,
        };
        let allow =
            Allowlist::parse(&config.allow.join(",")).map_err(|e| fail(format!("allow: {e}")))?;
        let policy = EgressPolicy::new(allow, config.allow_loopback);
        let client = DispatchClient::new(
            Arc::new(policy),
            Duration::from_millis(config.connect_timeout_ms),
        )
        .map_err(|e| fail(target_message(e)))?;
        let url = validate_target_url(&config.url, client.policy())
            .map_err(|e| fail(target_message(e)))?;
        let bearer =
            secret(&env, "bearer_token_env", config.bearer_token_env.as_deref()).map_err(&fail)?;
        if let Some(token) = &bearer {
            // Checked here so a token no header can carry fails at start.
            bearer_header(token)
                .map_err(|_| fail("bearer token is not a valid header value".into()))?;
        }
        let hmac =
            secret(&env, "hmac_secret_env", config.hmac_secret_env.as_deref()).map_err(&fail)?;
        Ok(Self {
            source: source.to_string(),
            url,
            client,
            bearer,
            hmac,
            timeout: Duration::from_millis(config.timeout_ms),
            include_payload: config.include_payload,
            batched: config.delivery.max_batch > 1,
            runtime: None,
        })
    }

    /// The request body and its content type.
    fn encode(&self, batch: &[Arc<JobEvent>]) -> Result<(String, &'static str), DeliveryResult> {
        // The hub already stripped payloads for this sink; passing the flag
        // again keeps the encoder from depending on that.
        let encode =
            |event: &Arc<JobEvent>| event.to_cloudevent(&self.source, self.include_payload);
        if self.batched {
            let events = batch.iter().map(encode).collect();
            return Ok((Value::Array(events).to_string(), BATCHED));
        }
        match batch {
            [event] => Ok((encode(event).to_string(), STRUCTURED)),
            _ => Err(DeliveryResult::Reject(format!(
                "structured mode sends one event per request, got {}",
                batch.len()
            ))),
        }
    }

    fn headers(&self, body: &str, content_type: &'static str) -> Result<HeaderMap, DeliveryResult> {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
        if let Some(token) = &self.bearer {
            let value = bearer_header(token)
                .map_err(|_| DeliveryResult::Reject("bearer token is not a valid header".into()))?;
            headers.insert(AUTHORIZATION, value);
        }
        if let Some(secret) = &self.hmac {
            let timestamp = now_millis();
            let digest = hmac_sha256_hex(secret.expose_secret(), &format!("{timestamp}.{body}"))
                .map_err(|()| DeliveryResult::Reject("could not sign the request".into()))?;
            let signature = HeaderValue::from_str(&format!("v1={digest}"))
                .map_err(|_| DeliveryResult::Reject("could not sign the request".into()))?;
            headers.insert(TIMESTAMP_HEADER, HeaderValue::from(timestamp));
            headers.insert(SIGNATURE_HEADER, signature);
        }
        Ok(headers)
    }

    fn send(&mut self, body: String, headers: HeaderMap) -> DeliveryResult {
        let request = self
            .client
            .inner()
            .post(self.url.clone())
            .headers(headers)
            .timeout(self.timeout)
            .body(body);
        let runtime = match self.runtime.take() {
            Some(runtime) => runtime,
            None => match Builder::new_current_thread().enable_all().build() {
                Ok(runtime) => runtime,
                Err(e) => return DeliveryResult::Retry(format!("could not start a runtime: {e}")),
            },
        };
        // Only the status is read; the response drops unread inside the
        // runtime that owns its connection.
        let outcome = runtime.block_on(async { request.send().await.map(|r| r.status().as_u16()) });
        self.runtime = Some(runtime);
        match outcome {
            Ok(status) => by_status(status),
            // Connect, timeout, I/O and an egress refusal at resolve time.
            Err(error) => DeliveryResult::Retry(transport_message(error)),
        }
    }
}

impl SinkBackend for HttpSink {
    fn deliver(&mut self, batch: &[Arc<JobEvent>]) -> DeliveryResult {
        let prepared = self
            .encode(batch)
            .and_then(|(body, content_type)| Ok((self.headers(&body, content_type)?, body)));
        match prepared {
            Ok((headers, body)) => self.send(body, headers),
            Err(result) => result,
        }
    }
}

/// 2xx delivered; 408, 429 and 5xx worth another try; anything else final.
fn by_status(status: u16) -> DeliveryResult {
    match status {
        200..=299 => DeliveryResult::Delivered,
        408 | 429 | 500..=599 => DeliveryResult::Retry(format!("HTTP {status}")),
        _ => DeliveryResult::Reject(format!("HTTP {status}")),
    }
}

/// The error and its causes, without the URL: a query string can carry a
/// credential. The causes name an egress refusal's host and address.
fn transport_message(error: reqwest::Error) -> String {
    let error = error.without_url();
    let mut message = error.to_string();
    let mut cause = error.source();
    while let Some(inner) = cause {
        message.push_str(": ");
        message.push_str(&inner.to_string());
        cause = inner.source();
    }
    message
}

/// The shared target errors say "push target"; this sink's field is `url`.
fn target_message(error: HttpTargetError) -> String {
    match error {
        HttpTargetError::MissingUrl => "url is empty".into(),
        HttpTargetError::Url(why) => format!("url is not usable: {why}"),
        HttpTargetError::Scheme(scheme) => {
            format!("url scheme must be http or https, got '{scheme}'")
        }
        HttpTargetError::NoHost => "url must include a hostname".into(),
        HttpTargetError::Userinfo => "url must not carry userinfo".into(),
        HttpTargetError::HostRefused(host) => format!("url host '{host}' is not on the allowlist"),
        HttpTargetError::InsecureTransport(host) => format!(
            "url host '{host}' must be reached over https; cleartext http is allowed only to a \
             loopback host with allow_loopback"
        ),
        HttpTargetError::Client(why) => format!("HTTP client could not be built: {why}"),
        // Neither is produced by URL validation or client construction.
        HttpTargetError::ZeroCapacity | HttpTargetError::Auth(_) => {
            "HTTP client could not be configured".into()
        }
    }
}

/// The secret an `*_env` field names; unset or empty refuses the sink.
fn secret(
    env: &impl Fn(&str) -> Option<String>,
    field: &str,
    name: Option<&str>,
) -> Result<Option<Secret>, String> {
    let Some(name) = name else {
        return Ok(None);
    };
    match env(name) {
        Some(value) if !value.is_empty() => Ok(Some(Secret::new(value))),
        _ => Err(format!("{field} names '{name}', which is unset or empty")),
    }
}

fn bearer_header(token: &Secret) -> Result<HeaderValue, reqwest::header::InvalidHeaderValue> {
    let mut value = b"Bearer ".to_vec();
    value.extend_from_slice(token.expose_secret());
    let mut header = HeaderValue::from_bytes(&value)?;
    header.set_sensitive(true);
    Ok(header)
}

#[cfg(test)]
mod tests {
    use std::net::TcpListener;

    use super::*;
    use crate::events::config::{EventsConfig, SinkConfig};
    use crate::events::event::EventType;
    use crate::events::EventHub;
    use crate::http::testing::StubServer;

    const WAIT: Duration = Duration::from_secs(10);

    /// A background runtime for the stub; the sink blocks on its own.
    fn stub(responses: Vec<(u16, String)>) -> (Runtime, StubServer) {
        let runtime = Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let server = runtime.block_on(StubServer::start_scripted(responses));
        (runtime, server)
    }

    fn document(url: &str, extra: &str) -> String {
        format!(
            r#"{{"source":"/test","sinks":[{{"kind":"http","name":"web","url":"{url}",
            "allow":["127.0.0.1"],"allow_loopback":true{extra}}}]}}"#
        )
    }

    fn config(url: &str, extra: &str) -> HttpSinkConfig {
        match EventsConfig::parse(&document(url, extra))
            .unwrap()
            .sinks
            .remove(0)
        {
            SinkConfig::Http(config) => config,
            other => panic!("not an http sink: {other:?}"),
        }
    }

    fn no_env(_: &str) -> Option<String> {
        None
    }

    fn sink(url: &str, extra: &str) -> HttpSink {
        HttpSink::with_env(&config(url, extra), "/test", no_env).unwrap()
    }

    fn refusal(url: &str, extra: &str) -> String {
        match HttpSink::with_env(&config(url, extra), "/test", no_env) {
            Err(EventsConfigError::Sink { sink, message }) => {
                assert_eq!(sink, "web");
                message
            }
            Err(other) => panic!("unexpected error: {other}"),
            Ok(_) => panic!("{url} was accepted"),
        }
    }

    fn event(job_id: &str) -> Arc<JobEvent> {
        let mut event = JobEvent::new(EventType::JobDead, job_id, None, "q", "t");
        event.payload = Some(vec![1, 2]);
        Arc::new(event)
    }

    fn header<'a>(received: &'a crate::http::testing::Received, name: &str) -> Option<&'a str> {
        received
            .headers
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.as_str())
    }

    fn body(received: &crate::http::testing::Received) -> Value {
        serde_json::from_slice(&received.body).unwrap()
    }

    #[test]
    fn structured_mode_posts_one_cloudevent() {
        let (_rt, server) = stub(vec![(200, String::new())]);
        let mut sink = sink(&format!("{}/in", server.base_url()), "");
        assert_eq!(sink.deliver(&[event("j1")]), DeliveryResult::Delivered);
        let received = &server.received()[0];
        assert_eq!(
            (received.method.as_str(), received.target.as_str()),
            ("POST", "/in")
        );
        assert_eq!(header(received, "content-type"), Some(STRUCTURED));
        assert_eq!(header(received, "authorization"), None);
        assert_eq!(header(received, "x-flexiq-event-signature"), None);
        let ce = body(received);
        assert_eq!(ce["specversion"], "1.0");
        assert_eq!(ce["id"], "j1:-:-:job.dead");
        assert_eq!(ce["source"], "/test");
        assert_eq!(ce["type"], "org.byteveda.flexiq.job.dead");
        assert_eq!(ce["subject"], "j1");
    }

    #[test]
    fn batched_mode_posts_an_array_even_of_one() {
        let (_rt, server) = stub(vec![(200, String::new())]);
        let url = format!("{}/in", server.base_url());
        let mut sink = sink(&url, r#","delivery":{"max_batch":10}"#);
        assert_eq!(
            sink.deliver(&[event("a"), event("b")]),
            DeliveryResult::Delivered
        );
        assert_eq!(sink.deliver(&[event("c")]), DeliveryResult::Delivered);
        let received = server.received();
        assert_eq!(header(&received[0], "content-type"), Some(BATCHED));
        let ids: Vec<Value> = body(&received[0])
            .as_array()
            .unwrap()
            .iter()
            .map(|e| e["subject"].clone())
            .collect();
        assert_eq!(ids, ["a", "b"]);
        assert_eq!(body(&received[1]).as_array().unwrap().len(), 1);
    }

    #[test]
    fn bearer_token_rides_the_authorization_header() {
        let (_rt, server) = stub(vec![(200, String::new())]);
        let config = config(
            &format!("{}/in", server.base_url()),
            r#","bearer_token_env":"TOK""#,
        );
        let env = |name: &str| (name == "TOK").then(|| "s3cret".to_string());
        let mut sink = HttpSink::with_env(&config, "/test", env).unwrap();
        assert_eq!(sink.deliver(&[event("j")]), DeliveryResult::Delivered);
        assert_eq!(
            header(&server.received()[0], "authorization"),
            Some("Bearer s3cret")
        );
    }

    #[test]
    fn hmac_signature_verifies_over_timestamp_and_body() {
        let (_rt, server) = stub(vec![(200, String::new())]);
        let config = config(
            &format!("{}/in", server.base_url()),
            r#","hmac_secret_env":"KEY""#,
        );
        let env = |name: &str| (name == "KEY").then(|| "k3y".to_string());
        let mut sink = HttpSink::with_env(&config, "/test", env).unwrap();
        assert_eq!(sink.deliver(&[event("j")]), DeliveryResult::Delivered);
        let received = &server.received()[0];
        let timestamp = header(received, "x-flexiq-event-timestamp").unwrap();
        timestamp.parse::<i64>().unwrap();
        let body = std::str::from_utf8(&received.body).unwrap();
        let expected = hmac_sha256_hex(b"k3y", &format!("{timestamp}.{body}")).unwrap();
        assert_eq!(
            header(received, "x-flexiq-event-signature"),
            Some(format!("v1={expected}").as_str())
        );
    }

    #[test]
    fn payload_is_sent_only_with_include_payload() {
        let (_rt, server) = stub(vec![(200, String::new())]);
        let url = format!("{}/in", server.base_url());
        // Handed the payload directly: the sink must not rely on the hub.
        assert_eq!(
            sink(&url, "").deliver(&[event("j")]),
            DeliveryResult::Delivered
        );
        let mut opted_in = sink(&url, r#","include_payload":true"#);
        assert_eq!(opted_in.deliver(&[event("j")]), DeliveryResult::Delivered);
        let received = server.received();
        assert!(body(&received[0])["data"].get("payload_base64").is_none());
        assert_eq!(body(&received[1])["data"]["payload_base64"], "AQI=");
    }

    #[test]
    fn status_decides_delivered_retry_or_reject() {
        assert_eq!(by_status(204), DeliveryResult::Delivered);
        for status in [408, 429, 500, 503] {
            assert!(
                matches!(by_status(status), DeliveryResult::Retry(_)),
                "{status}"
            );
        }
        for status in [301, 400, 401, 404, 413] {
            assert!(
                matches!(by_status(status), DeliveryResult::Reject(_)),
                "{status}"
            );
        }
    }

    #[test]
    fn a_refused_connection_is_retried() {
        let port = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let mut sink = sink(&format!("http://127.0.0.1:{port}/in?token=q"), "");
        match sink.deliver(&[event("j")]) {
            DeliveryResult::Retry(why) => assert!(!why.contains("token=q"), "{why}"),
            other => panic!("expected a retry, got {other:?}"),
        }
    }

    #[test]
    fn a_503_then_200_delivers_once_through_the_hub() {
        let (_rt, server) = stub(vec![(503, String::new()), (200, String::new())]);
        let hub = EventHub::from_json(&document(&format!("{}/in", server.base_url()), "")).unwrap();
        hub.emit((*event("j")).clone());
        hub.shutdown(WAIT);
        assert_eq!(server.request_count(), 2);
        let stats = &hub.stats()[0];
        assert_eq!((stats.delivered, stats.dropped_failed), (1, 0));
    }

    #[test]
    fn a_400_is_rejected_without_a_retry() {
        let (_rt, server) = stub(vec![(400, String::new())]);
        let hub = EventHub::from_json(&document(&format!("{}/in", server.base_url()), "")).unwrap();
        hub.emit((*event("j")).clone());
        hub.shutdown(WAIT);
        assert_eq!(server.request_count(), 1);
        let stats = &hub.stats()[0];
        assert_eq!((stats.delivered, stats.dropped_rejected), (0, 1));
    }

    #[test]
    fn a_url_off_the_allowlist_is_refused_at_start() {
        let message = refusal("https://events.example.com/in", "");
        assert_eq!(
            message,
            "url host 'events.example.com' is not on the allowlist"
        );
    }

    #[test]
    fn cleartext_http_to_a_public_host_is_refused_at_start() {
        let doc = r#"{"sinks":[{"kind":"http","name":"web","url":"http://93.184.216.34/in",
            "allow":["93.184.216.34"]}]}"#;
        let error = EventHub::from_json(doc).unwrap_err().to_string();
        assert!(error.contains("must be reached over https"), "{error}");
        assert!(!error.contains("push target"), "{error}");
    }

    #[test]
    fn a_missing_or_empty_secret_is_refused_at_start() {
        let url = "http://127.0.0.1:1/in";
        let message = refusal(url, r#","bearer_token_env":"NOPE""#);
        assert_eq!(
            message,
            "bearer_token_env names 'NOPE', which is unset or empty"
        );
        let config = config(url, r#","hmac_secret_env":"EMPTY""#);
        let empty = |_: &str| Some(String::new());
        assert!(matches!(
            HttpSink::with_env(&config, "/test", empty),
            Err(EventsConfigError::Sink { message, .. }) if message.contains("hmac_secret_env")
        ));
    }
}
