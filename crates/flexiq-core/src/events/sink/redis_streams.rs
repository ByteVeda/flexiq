//! CloudEvents fields written to a Redis stream via `XADD`.
//!
//! One pipelined `XADD <stream> MAXLEN ~ <max_len> *` per event in the batch,
//! carrying the routing attributes as individual fields plus the whole
//! structured CloudEvent as one `event` field, so a consumer can filter on
//! the cheap fields without parsing JSON first.
//!
//! The connection is dialled lazily, on first delivery, and is never reused
//! after an error: a failure may have left the reply stream out of step (a
//! partial pipeline write, a desync), so the next attempt dials fresh rather
//! than risk a wrong read landing on the next caller.

use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;

use super::{DeliveryResult, SinkBackend};
use crate::events::config::{EventsConfigError, RedisSinkConfig};
use crate::events::event::JobEvent;

/// Connect, read and write budget: a dead Redis must not hold the sink
/// thread forever.
const TIMEOUT: Duration = Duration::from_secs(5);

/// The `redis_streams` sink kind.
pub(crate) struct RedisStreamsSink {
    client: redis::Client,
    stream: String,
    max_len: u64,
    include_payload: bool,
    source: String,
    /// Held between deliveries; `None` after any error, so the next call
    /// dials fresh.
    conn: Option<redis::Connection>,
}

impl RedisStreamsSink {
    /// Build from config, reading the URL from the process environment.
    pub(crate) fn new(config: &RedisSinkConfig, source: &str) -> Result<Self, EventsConfigError> {
        Self::with_env(config, source, |name| std::env::var(name).ok())
    }

    /// Build from config, reading the URL through `env`.
    fn with_env(
        config: &RedisSinkConfig,
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
        // Never the Display of an open error: a malformed URL's parse detail
        // can echo it back, and the URL carries the password.
        let client = redis::Client::open(url).map_err(|e| {
            fail(format!(
                "url is not a usable redis:// URL ({})",
                e.category()
            ))
        })?;
        Ok(Self {
            client,
            stream: config.stream.clone(),
            max_len: config.max_len,
            include_payload: config.include_payload,
            source: source.to_string(),
            conn: None,
        })
    }

    /// The held connection, or a freshly dialled one with every timeout set.
    fn take_connection(&mut self) -> redis::RedisResult<redis::Connection> {
        match self.conn.take() {
            Some(conn) => Ok(conn),
            None => {
                let conn = self.client.get_connection_with_timeout(TIMEOUT)?;
                conn.set_read_timeout(Some(TIMEOUT))?;
                conn.set_write_timeout(Some(TIMEOUT))?;
                Ok(conn)
            }
        }
    }
}

impl SinkBackend for RedisStreamsSink {
    fn deliver(&mut self, batch: &[Arc<JobEvent>]) -> DeliveryResult {
        let mut pipe = redis::pipe();
        for event in batch {
            append(
                &mut pipe,
                event,
                &self.source,
                &self.stream,
                self.max_len,
                self.include_payload,
            );
        }
        let mut conn = match self.take_connection() {
            Ok(conn) => conn,
            Err(e) => return classify(&e),
        };
        match pipe.query::<()>(&mut conn) {
            Ok(()) => {
                self.conn = Some(conn);
                DeliveryResult::Delivered
            }
            Err(e) => classify(&e),
        }
    }
}

/// One `XADD` for `event`, appended to `pipe`.
fn append(
    pipe: &mut redis::Pipeline,
    event: &JobEvent,
    source: &str,
    stream: &str,
    max_len: u64,
    include_payload: bool,
) {
    pipe.cmd("XADD")
        .arg(stream)
        .arg("MAXLEN")
        .arg("~")
        .arg(max_len)
        .arg("*");
    for (field, value) in fields(event, source, include_payload) {
        pipe.arg(field).arg(value);
    }
}

/// The `(field, value)` pairs one event contributes to its `XADD`: cheap
/// routing attributes plus the whole structured CloudEvent, built from the
/// same encoding the HTTP sink uses so the two agree byte for byte.
fn fields(event: &JobEvent, source: &str, include_payload: bool) -> Vec<(&'static str, String)> {
    let ce = event.to_cloudevent(source, include_payload);
    let text = |key: &str| {
        ce.get(key)
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string()
    };
    vec![
        ("id", event.id()),
        ("type", text("type")),
        ("source", source.to_string()),
        ("time", text("time")),
        ("namespace", event.namespace_label().to_string()),
        ("queue", event.queue.clone()),
        ("task", event.task_name.clone()),
        ("event", ce.to_string()),
    ]
}

/// Defers to the client's own `retry_method`, so the replies a restart or
/// failover produces (LOADING, TRYAGAIN, MASTERDOWN, CLUSTERDOWN, READONLY)
/// retry alongside connection trouble; only `NoRetry` (WRONGTYPE, NOPERM,
/// an unknown code such as BUSY) is final. Built from the error's category,
/// never its `Display`: a connection failure's message can carry the URL.
fn classify(error: &redis::RedisError) -> DeliveryResult {
    match error.retry_method() {
        redis::RetryMethod::NoRetry => {
            DeliveryResult::Reject(format!("redis refused the batch: {}", error.category()))
        }
        _ => DeliveryResult::Retry(format!("redis unavailable: {}", error.category())),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

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

    fn config(url_env: &str, stream: &str) -> RedisSinkConfig {
        let doc = format!(
            r#"{{"sinks":[{{"kind":"redis_streams","name":"stream","url_env":"{url_env}",
            "stream":"{stream}"}}]}}"#
        );
        match EventsConfig::parse(&doc).unwrap().sinks.remove(0) {
            SinkConfig::RedisStreams(config) => config,
            other => panic!("not a redis sink: {other:?}"),
        }
    }

    #[test]
    fn field_list_carries_routing_attributes_and_the_whole_cloudevent() {
        let pairs = fields(&event("j1"), "/test", false);
        let map: HashMap<_, _> = pairs.into_iter().collect();
        assert_eq!(map["id"], "j1:1:2:job.dead");
        assert_eq!(map["type"], "org.byteveda.flexiq.job.dead");
        assert_eq!(map["source"], "/test");
        assert_eq!(map["namespace"], "default");
        assert_eq!(map["queue"], "emails");
        assert_eq!(map["task"], "send");
        assert!(!map["time"].is_empty());
        let ce: Value = serde_json::from_str(&map["event"]).unwrap();
        assert_eq!(ce["id"], "j1:1:2:job.dead");
        assert!(ce["data"].get("payload_base64").is_none());
    }

    #[test]
    fn field_list_carries_the_payload_only_when_opted_in() {
        let pairs = fields(&event("j1"), "/test", true);
        let map: HashMap<_, _> = pairs.into_iter().collect();
        let ce: Value = serde_json::from_str(&map["event"]).unwrap();
        assert_eq!(ce["data"]["payload_base64"], "AQI=");
    }

    #[test]
    fn a_missing_or_empty_url_is_refused_at_start() {
        let config = config("NOPE", "flexiq:events");
        match RedisStreamsSink::with_env(&config, "/test", |_| None) {
            Err(EventsConfigError::Sink { sink, message }) => {
                assert_eq!(sink, "stream");
                assert!(message.contains("url_env"), "{message}");
            }
            Err(other) => panic!("unexpected error: {other:?}"),
            Ok(_) => panic!("empty url_env was accepted"),
        }
        let empty = |_: &str| Some(String::new());
        assert!(matches!(
            RedisStreamsSink::with_env(&config, "/test", empty),
            Err(EventsConfigError::Sink { message, .. }) if message.contains("url_env")
        ));
    }

    #[test]
    fn a_malformed_url_is_refused_without_echoing_it() {
        let config = config("URL", "flexiq:events");
        let env = |name: &str| (name == "URL").then(|| "not a redis url ::::".to_string());
        match RedisStreamsSink::with_env(&config, "/test", env) {
            Err(EventsConfigError::Sink { message, .. }) => {
                assert!(!message.contains("not a redis url"), "{message}");
            }
            Err(other) => panic!("unexpected error: {other:?}"),
            Ok(_) => panic!("a malformed url was accepted"),
        }
    }

    fn server_error(kind: redis::ServerErrorKind) -> redis::RedisError {
        (redis::ErrorKind::Server(kind), "test reply").into()
    }

    #[test]
    fn restart_and_failover_replies_retry() {
        for kind in [
            redis::ServerErrorKind::BusyLoading,
            redis::ServerErrorKind::TryAgain,
            redis::ServerErrorKind::MasterDown,
            redis::ServerErrorKind::ClusterDown,
            redis::ServerErrorKind::ReadOnly,
        ] {
            let result = classify(&server_error(kind));
            assert!(
                matches!(result, DeliveryResult::Retry(_)),
                "{kind:?}: {result:?}"
            );
        }
        let io = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "refused");
        assert!(matches!(classify(&io.into()), DeliveryResult::Retry(_)));
        let timeout = std::io::Error::new(std::io::ErrorKind::TimedOut, "slow");
        assert!(matches!(
            classify(&timeout.into()),
            DeliveryResult::Retry(_)
        ));
    }

    #[test]
    fn replies_retrying_cannot_fix_are_rejected() {
        for kind in [
            redis::ServerErrorKind::ResponseError,
            redis::ServerErrorKind::NoPerm,
            redis::ServerErrorKind::CrossSlot,
        ] {
            let result = classify(&server_error(kind));
            assert!(
                matches!(result, DeliveryResult::Reject(_)),
                "{kind:?}: {result:?}"
            );
        }
        // BUSY has no known kind, so the client files it as an extension.
        let busy = redis::make_extension_error("BUSY".to_string(), None);
        assert!(matches!(classify(&busy), DeliveryResult::Reject(_)));
    }

    /// Only runs against a real Redis, the way the rest of this crate's
    /// Redis-backed tests do; skips cleanly when that URL is not set.
    #[test]
    fn live_delivery_appends_one_entry_to_the_stream() {
        let Ok(url) = std::env::var("FLEXIQ_REDIS_TEST_URL") else {
            eprintln!("Skipping: FLEXIQ_REDIS_TEST_URL unset");
            return;
        };
        let stream = format!("flexiq:events:test:{}", crate::job::now_millis());
        let config = config("URL", &stream);
        let env = |name: &str| (name == "URL").then(|| url.clone());
        let mut sink = RedisStreamsSink::with_env(&config, "/test", env).unwrap();

        assert_eq!(sink.deliver(&[event("live")]), DeliveryResult::Delivered);

        let client = redis::Client::open(url).unwrap();
        let mut conn = client.get_connection().unwrap();
        let len: i64 = redis::cmd("XLEN").arg(&stream).query(&mut conn).unwrap();
        assert_eq!(len, 1);
        let _: i64 = redis::cmd("DEL").arg(&stream).query(&mut conn).unwrap();
    }
}
