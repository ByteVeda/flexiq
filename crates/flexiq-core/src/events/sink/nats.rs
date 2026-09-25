//! CloudEvents published to NATS, in the NATS protocol binding's structured
//! mode.
//!
//! Each event is one message on the subject its template renders, carrying
//! the structured CloudEvent, a `content-type` header and the CloudEvents id
//! as `Nats-Msg-Id`. In `jetstream` mode the sink waits for every message's
//! acknowledgement, and the stream drops a retried duplicate inside its
//! dedupe window by that id. In `core` mode it publishes, then flushes.
//!
//! The client is dialled lazily, on first delivery, and then kept: it
//! reconnects by itself, and a batch sent while it is reconnecting times out
//! and is retried. Its runtime has one worker thread of its own, so the
//! connection answers the server's pings between deliveries instead of being
//! dropped as stale.

use std::sync::Arc;
use std::time::Duration;

use async_nats::jetstream::context::PublishErrorKind as JetStreamErrorKind;
use async_nats::{
    Client, ConnectErrorKind, ConnectOptions, HeaderMap, PublishErrorKind, ServerAddr,
};
use tokio::runtime::{Builder, Runtime};

use super::{tls, DeliveryResult, SinkBackend};
use crate::events::config::{EventsConfigError, NatsMode, NatsSinkConfig};
use crate::events::event::JobEvent;
use crate::events::subject::SubjectTemplate;

const CONTENT_TYPE: &str = "application/cloudevents+json";
const CLIENT_NAME: &str = "flexiq-events";

/// The `nats` sink kind.
pub(crate) struct NatsSink {
    /// Each server in the URL list. They can carry a password; never
    /// formatted.
    servers: Vec<String>,
    /// A `.creds` file's contents; never formatted.
    credentials: Option<String>,
    tls: rustls::ClientConfig,
    subject: SubjectTemplate,
    mode: NatsMode,
    timeout: Duration,
    include_payload: bool,
    source: String,
    client: Option<Client>,
    /// Built on the sink thread at first delivery; see the HTTP sink.
    runtime: Option<Runtime>,
}

impl NatsSink {
    /// Build from config, reading secrets from the process environment.
    pub(crate) fn new(config: &NatsSinkConfig, source: &str) -> Result<Self, EventsConfigError> {
        Self::with_env(config, source, |name| std::env::var(name).ok())
    }

    /// Build from config, reading secrets through `env`.
    fn with_env(
        config: &NatsSinkConfig,
        source: &str,
        env: impl Fn(&str) -> Option<String>,
    ) -> Result<Self, EventsConfigError> {
        let fail = |message: String| EventsConfigError::Sink {
            sink: config.name.clone(),
            message,
        };
        let url = match env(&config.url_env) {
            Some(value) if !value.trim().is_empty() => value,
            _ => {
                return Err(fail(format!(
                    "url_env names '{}', which is unset or empty",
                    config.url_env
                )))
            }
        };
        let servers: Vec<String> = url.split(',').map(|s| s.trim().to_string()).collect();
        // Never the parse error: it can echo the URL, which carries the
        // password.
        if servers.iter().any(|s| s.parse::<ServerAddr>().is_err()) {
            return Err(fail(
                "url is not a usable NATS server URL or comma-separated list".into(),
            ));
        }
        let credentials = match config.credentials_env.as_deref() {
            None => None,
            Some(name) => match env(name) {
                Some(value) if !value.trim().is_empty() => Some(value),
                _ => {
                    return Err(fail(format!(
                        "credentials_env names '{name}', which is unset or empty"
                    )))
                }
            },
        };
        if let Some(creds) = &credentials {
            // Parsed here so a malformed file fails at start, not at first
            // delivery.
            ConnectOptions::new()
                .credentials(creds)
                .map_err(|_| fail("credentials are not a valid .creds file".into()))?;
        }
        let subject = SubjectTemplate::parse(&config.subject).map_err(fail)?;
        Ok(Self {
            servers,
            credentials,
            tls: tls::client_config(config.ca_file.as_deref()).map_err(fail)?,
            subject,
            mode: config.mode,
            timeout: Duration::from_millis(config.timeout_ms),
            include_payload: config.include_payload,
            source: source.to_string(),
            client: None,
            runtime: None,
        })
    }

    async fn connect(&self) -> Result<Client, DeliveryResult> {
        let mut options = ConnectOptions::new()
            .name(CLIENT_NAME)
            .connection_timeout(self.timeout)
            .tls_client_config(self.tls.clone());
        if let Some(creds) = &self.credentials {
            options = options
                .credentials(creds)
                .map_err(|_| DeliveryResult::Reject("credentials are not valid".into()))?;
        }
        options
            .connect(self.servers.clone())
            .await
            .map_err(|e| classify_connect(e.kind()))
    }

    async fn send(&mut self, batch: &[Arc<JobEvent>]) -> Result<(), DeliveryResult> {
        let client = match &self.client {
            Some(client) => client.clone(),
            None => {
                let client = self.connect().await?;
                self.client = Some(client.clone());
                client
            }
        };
        let messages = batch.iter().map(|event| {
            let (headers, body) = message(event, &self.source, self.include_payload);
            (self.subject.render(event), headers, body)
        });
        match self.mode {
            NatsMode::Core => {
                for (subject, headers, body) in messages {
                    client
                        .publish_with_headers(subject, headers, body.into())
                        .await
                        .map_err(|e| classify_publish(e.kind()))?;
                }
                client
                    .flush()
                    .await
                    .map_err(|_| DeliveryResult::Retry("nats flush failed".into()))
            }
            NatsMode::Jetstream => {
                let mut jetstream = async_nats::jetstream::new(client);
                jetstream.set_timeout(self.timeout);
                // Every message goes out before any ack is awaited, so a
                // batch costs one round trip, not one per event.
                let mut acks = Vec::with_capacity(batch.len());
                for (subject, headers, body) in messages {
                    let ack = jetstream
                        .publish_with_headers(subject, headers, body.into())
                        .await
                        .map_err(|e| classify_jetstream(e.kind()))?;
                    acks.push(ack);
                }
                for ack in acks {
                    ack.await.map_err(|e| classify_jetstream(e.kind()))?;
                }
                Ok(())
            }
        }
    }
}

impl SinkBackend for NatsSink {
    fn deliver(&mut self, batch: &[Arc<JobEvent>]) -> DeliveryResult {
        let runtime = match self.runtime.take() {
            Some(runtime) => runtime,
            None => match Builder::new_multi_thread()
                .worker_threads(1)
                .thread_name("flexiq-events-nats")
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(e) => return DeliveryResult::Retry(format!("could not start a runtime: {e}")),
            },
        };
        let timeout = self.timeout;
        // The timer is built inside `block_on`: outside a runtime it panics.
        let outcome =
            runtime.block_on(async { tokio::time::timeout(timeout, self.send(batch)).await });
        self.runtime = Some(runtime);
        match outcome {
            Ok(Ok(())) => DeliveryResult::Delivered,
            Ok(Err(result)) => result,
            Err(_) => DeliveryResult::Retry(format!(
                "nats did not answer within {} ms",
                timeout.as_millis()
            )),
        }
    }
}

/// One event's headers and body.
fn message(event: &JobEvent, source: &str, include_payload: bool) -> (HeaderMap, String) {
    let mut headers = HeaderMap::new();
    headers.insert("content-type", CONTENT_TYPE);
    headers.insert(async_nats::header::NATS_MESSAGE_ID, event.id());
    let body = event.to_cloudevent(source, include_payload).to_string();
    (headers, body)
}

/// Authentication, authorization, a bad address and a TLS setup the server
/// refuses are final; the network and timeouts retry.
fn classify_connect(kind: ConnectErrorKind) -> DeliveryResult {
    match kind {
        ConnectErrorKind::ServerParse
        | ConnectErrorKind::Authentication
        | ConnectErrorKind::AuthorizationViolation
        | ConnectErrorKind::Tls => {
            DeliveryResult::Reject(format!("nats refused to connect: {kind}"))
        }
        _ => DeliveryResult::Retry(format!("nats unreachable: {kind}")),
    }
}

/// Only a failed send retries; an oversized message or a bad subject never
/// gets better.
fn classify_publish(kind: PublishErrorKind) -> DeliveryResult {
    match kind {
        PublishErrorKind::Send => DeliveryResult::Retry(format!("nats publish failed: {kind}")),
        _ => DeliveryResult::Reject(format!("nats refused the message: {kind}")),
    }
}

/// A missing ack (timeout, dropped connection, too many in flight) retries;
/// no stream for the subject, an oversized message, or any other refusal
/// from the stream is final.
fn classify_jetstream(kind: JetStreamErrorKind) -> DeliveryResult {
    match kind {
        JetStreamErrorKind::TimedOut
        | JetStreamErrorKind::BrokenPipe
        | JetStreamErrorKind::MaxAckPending => {
            DeliveryResult::Retry(format!("jetstream publish failed: {kind}"))
        }
        _ => DeliveryResult::Reject(format!("jetstream refused the message: {kind}")),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::*;
    use crate::events::config::{EventsConfig, SinkConfig};
    use crate::events::event::EventType;

    fn event(job_id: &str) -> Arc<JobEvent> {
        let mut event = JobEvent::new(EventType::JobDead, job_id, None, "emails", "send");
        event.attempt = Some(1);
        event.epoch = Some(2);
        event.payload = Some(vec![1, 2]);
        Arc::new(event)
    }

    fn config(extra: &str) -> NatsSinkConfig {
        config_for("flexiq.{namespace}.{queue}.{type}", extra)
    }

    fn config_for(subject: &str, extra: &str) -> NatsSinkConfig {
        let doc = format!(
            r#"{{"sinks":[{{"kind":"nats","name":"n","url_env":"URL",
            "subject":"{subject}"{extra}}}]}}"#
        );
        match EventsConfig::parse(&doc).unwrap().sinks.remove(0) {
            SinkConfig::Nats(config) => config,
            other => panic!("not a nats sink: {other:?}"),
        }
    }

    fn url(value: &'static str) -> impl Fn(&str) -> Option<String> {
        move |name| (name == "URL").then(|| value.to_string())
    }

    #[test]
    fn a_message_is_the_structured_cloudevent_with_its_id() {
        let (headers, body) = message(&event("j1"), "/test", false);
        assert_eq!(
            headers.get("Nats-Msg-Id").unwrap().as_str(),
            "j1:1:2:job.dead"
        );
        assert_eq!(headers.get("content-type").unwrap().as_str(), CONTENT_TYPE);
        let ce: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(ce["id"], "j1:1:2:job.dead");
        assert!(ce["data"].get("payload_base64").is_none());

        let (_, body) = message(&event("j1"), "/test", true);
        let ce: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(ce["data"]["payload_base64"], "AQI=");
    }

    #[test]
    fn a_missing_or_malformed_url_is_refused_without_echoing_it() {
        let config = config("");
        assert!(matches!(
            NatsSink::with_env(&config, "/test", |_| None),
            Err(EventsConfigError::Sink { message, .. }) if message.contains("url_env names 'URL'")
        ));
        match NatsSink::with_env(&config, "/test", url("nats://u:hunter2@[::1")) {
            Err(EventsConfigError::Sink { message, .. }) => {
                assert!(!message.contains("hunter2"), "{message}");
            }
            Err(other) => panic!("unexpected error: {other:?}"),
            Ok(_) => panic!("a malformed url was accepted"),
        }
        assert!(NatsSink::with_env(&config, "/test", url("nats://a:4222,tls://b:4222")).is_ok());
    }

    #[test]
    fn unset_or_malformed_credentials_are_refused_at_start() {
        let config = config(r#","credentials_env":"CREDS""#);
        assert!(matches!(
            NatsSink::with_env(&config, "/test", url("nats://localhost:4222")),
            Err(EventsConfigError::Sink { message, .. }) if message.contains("credentials_env")
        ));
        let garbage = |name: &str| match name {
            "URL" => Some("nats://localhost:4222".to_string()),
            _ => Some("not a creds file".to_string()),
        };
        assert!(matches!(
            NatsSink::with_env(&config, "/test", garbage),
            Err(EventsConfigError::Sink { message, .. }) if message.contains(".creds")
        ));
    }

    #[test]
    fn connect_failures_split_on_whether_waiting_helps() {
        for kind in [
            ConnectErrorKind::Dns,
            ConnectErrorKind::TimedOut,
            ConnectErrorKind::Io,
            ConnectErrorKind::MaxReconnects,
        ] {
            assert!(
                matches!(classify_connect(kind), DeliveryResult::Retry(_)),
                "{kind:?}"
            );
        }
        for kind in [
            ConnectErrorKind::ServerParse,
            ConnectErrorKind::Authentication,
            ConnectErrorKind::AuthorizationViolation,
            ConnectErrorKind::Tls,
        ] {
            assert!(
                matches!(classify_connect(kind), DeliveryResult::Reject(_)),
                "{kind:?}"
            );
        }
    }

    #[test]
    fn publish_failures_split_on_whether_waiting_helps() {
        assert!(matches!(
            classify_publish(PublishErrorKind::Send),
            DeliveryResult::Retry(_)
        ));
        for kind in [
            PublishErrorKind::MaxPayloadExceeded,
            PublishErrorKind::InvalidSubject,
        ] {
            assert!(
                matches!(classify_publish(kind), DeliveryResult::Reject(_)),
                "{kind:?}"
            );
        }
        for kind in [
            JetStreamErrorKind::TimedOut,
            JetStreamErrorKind::BrokenPipe,
            JetStreamErrorKind::MaxAckPending,
        ] {
            assert!(
                matches!(classify_jetstream(kind), DeliveryResult::Retry(_)),
                "{kind:?}"
            );
        }
        for kind in [
            JetStreamErrorKind::StreamNotFound,
            JetStreamErrorKind::MaxPayloadExceeded,
            JetStreamErrorKind::Other,
        ] {
            assert!(
                matches!(classify_jetstream(kind), DeliveryResult::Reject(_)),
                "{kind:?}"
            );
        }
    }

    #[test]
    fn an_unreachable_server_retries_within_the_budget() {
        let config = config(r#","timeout_ms":300"#);
        let mut sink = NatsSink::with_env(&config, "/test", url("nats://127.0.0.1:1")).unwrap();
        let started = std::time::Instant::now();
        let result = sink.deliver(&[event("j1")]);
        assert!(matches!(result, DeliveryResult::Retry(_)), "{result:?}");
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    /// Only runs against a real server; skips cleanly when unset. Core mode
    /// needs no stream; JetStream mode then proves a subject no stream
    /// captures is refused, not retried.
    #[test]
    fn live_delivery_publishes_and_an_uncaptured_subject_is_refused() {
        let Ok(server) = std::env::var("FLEXIQ_NATS_TEST_URL") else {
            eprintln!("Skipping: FLEXIQ_NATS_TEST_URL unset");
            return;
        };
        let env = |name: &str| (name == "URL").then(|| server.clone());

        let mut core = NatsSink::with_env(&config(r#","mode":"core""#), "/test", env).unwrap();
        assert_eq!(core.deliver(&[event("live")]), DeliveryResult::Delivered);
        // The held client is reused.
        assert_eq!(core.deliver(&[event("live")]), DeliveryResult::Delivered);

        let uncaptured = config_for("flexiq-test-uncaptured.{queue}", "");
        let mut jetstream = NatsSink::with_env(&uncaptured, "/test", env).unwrap();
        let result = jetstream.deliver(&[event("live")]);
        assert!(matches!(result, DeliveryResult::Reject(_)), "{result:?}");
    }
}
