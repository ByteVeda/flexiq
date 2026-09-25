//! CloudEvents produced to a Kafka topic, in the Kafka protocol binding's
//! structured mode.
//!
//! Each event is one record: the key is the job id, the value the structured
//! CloudEvent, and a `content-type` header says so. The partition is Kafka's
//! own default for a keyed record — `positive(murmur2(key)) % partitions` —
//! so a job's events share one partition, in the order this sink sent them,
//! and land where a Java producer would have put them. That is not the order
//! they happened in: emits race, and a retried batch repeats. Records are
//! acknowledged by every in-sync replica (`acks=all`).
//!
//! The client is connected lazily, on first delivery, and dropped after any
//! error, so the next attempt re-reads the cluster metadata: a leader move
//! or a partition count change is picked up there.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use rskafka::client::error::{Error, ProtocolError, RequestError};
use rskafka::client::partition::{Compression, PartitionClient, UnknownTopicHandling};
use rskafka::client::{ClientBuilder, Credentials, SaslConfig};
use rskafka::record::Record;
use rskafka::{BackoffConfig, ConnectionError, SaslError};
use tokio::runtime::{Builder, Runtime};

use super::{tls, DeliveryResult, SinkBackend};
use crate::events::config::{EventsConfigError, KafkaSaslMechanism, KafkaSinkConfig};
use crate::events::event::JobEvent;

const CONTENT_TYPE: &str = "application/cloudevents+json";
const CLIENT_ID: &str = "flexiq-events";

/// The `kafka` sink kind.
pub(crate) struct KafkaSink {
    brokers: Vec<String>,
    topic: String,
    tls: Option<Arc<rustls::ClientConfig>>,
    /// Carries the password; never formatted.
    sasl: Option<SaslConfig>,
    timeout: Duration,
    include_payload: bool,
    source: String,
    /// One client per partition, indexed by position in the sorted partition
    /// ids. `None` until the first delivery and after any error.
    partitions: Option<Vec<PartitionClient>>,
    /// Built on the sink thread at first delivery; see the HTTP sink.
    runtime: Option<Runtime>,
}

impl KafkaSink {
    /// Build from config, reading credentials from the process environment.
    pub(crate) fn new(config: &KafkaSinkConfig, source: &str) -> Result<Self, EventsConfigError> {
        Self::with_env(config, source, |name| std::env::var(name).ok())
    }

    /// Build from config, reading credentials through `env`.
    fn with_env(
        config: &KafkaSinkConfig,
        source: &str,
        env: impl Fn(&str) -> Option<String>,
    ) -> Result<Self, EventsConfigError> {
        let fail = |message: String| EventsConfigError::Sink {
            sink: config.name.clone(),
            message,
        };
        let tls = match config.tls {
            true => Some(Arc::new(
                tls::client_config(config.ca_file.as_deref()).map_err(fail)?,
            )),
            false => None,
        };
        let sasl = match &config.sasl {
            None => None,
            Some(sasl) => {
                let read = |field: &str, name: &str| match env(name) {
                    Some(value) if !value.is_empty() => Ok(value),
                    _ => Err(fail(format!(
                        "sasl.{field} names '{name}', which is unset or empty"
                    ))),
                };
                let credentials = Credentials::new(
                    read("username_env", &sasl.username_env)?,
                    read("password_env", &sasl.password_env)?,
                );
                Some(match sasl.mechanism {
                    KafkaSaslMechanism::Plain => SaslConfig::Plain(credentials),
                    KafkaSaslMechanism::ScramSha256 => SaslConfig::ScramSha256(credentials),
                    KafkaSaslMechanism::ScramSha512 => SaslConfig::ScramSha512(credentials),
                })
            }
        };
        Ok(Self {
            brokers: config.brokers.clone(),
            topic: config.topic.clone(),
            tls,
            sasl,
            timeout: Duration::from_millis(config.timeout_ms),
            include_payload: config.include_payload,
            source: source.to_string(),
            partitions: None,
            runtime: None,
        })
    }

    /// A client per partition of the topic, from fresh cluster metadata.
    async fn connect(&self) -> Result<Vec<PartitionClient>, DeliveryResult> {
        // The hub owns retries: the client's own backoff is capped at the
        // batch budget, so it gives up and says why instead of looping.
        let backoff = BackoffConfig {
            init_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(1),
            base: 3.0,
            deadline: Some(self.timeout),
        };
        let mut builder = ClientBuilder::new(self.brokers.clone())
            .client_id(CLIENT_ID)
            .backoff_config(backoff);
        if let Some(tls) = &self.tls {
            builder = builder.tls_config(Arc::clone(tls));
        }
        if let Some(sasl) = &self.sasl {
            builder = builder.sasl_config(sasl.clone());
        }
        let client = builder.build().await.map_err(|e| classify(&e))?;
        let ids: Vec<i32> = client
            .list_topics()
            .await
            .map_err(|e| classify(&e))?
            .into_iter()
            .find(|topic| topic.name == self.topic)
            .map(|topic| topic.partitions.into_iter().collect())
            .unwrap_or_default();
        if ids.is_empty() {
            // Kafka files an unknown topic as retriable: it may be being
            // created. Each batch spends its attempts, then drops.
            return Err(DeliveryResult::Retry(format!(
                "kafka topic '{}' is not in the cluster metadata",
                self.topic
            )));
        }
        let mut partitions = Vec::with_capacity(ids.len());
        for id in ids {
            let partition = client
                .partition_client(self.topic.clone(), id, UnknownTopicHandling::Error)
                .await
                .map_err(|e| classify(&e))?;
            partitions.push(partition);
        }
        Ok(partitions)
    }

    async fn send(&mut self, batch: &[Arc<JobEvent>]) -> Result<(), DeliveryResult> {
        // Taken, not borrowed: a batch that fails or times out part-way
        // leaves `None` behind, and the next attempt connects fresh.
        let partitions = match self.partitions.take() {
            Some(partitions) => partitions,
            None => self.connect().await?,
        };
        let mut by_partition: BTreeMap<usize, Vec<Record>> = BTreeMap::new();
        for event in batch {
            let index = partition_for(event.job_id.as_bytes(), partitions.len());
            by_partition.entry(index).or_default().push(record(
                event,
                &self.source,
                self.include_payload,
            ));
        }
        for (index, records) in by_partition {
            partitions[index]
                .produce(records, Compression::NoCompression)
                .await
                .map_err(|e| classify(&e))?;
        }
        self.partitions = Some(partitions);
        Ok(())
    }
}

impl SinkBackend for KafkaSink {
    fn deliver(&mut self, batch: &[Arc<JobEvent>]) -> DeliveryResult {
        let runtime = match self.runtime.take() {
            Some(runtime) => runtime,
            None => match Builder::new_current_thread().enable_all().build() {
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
                "kafka did not answer within {} ms",
                timeout.as_millis()
            )),
        }
    }
}

/// One event as one record.
fn record(event: &JobEvent, source: &str, include_payload: bool) -> Record {
    let value = event.to_cloudevent(source, include_payload).to_string();
    Record {
        key: Some(event.job_id.as_bytes().to_vec()),
        value: Some(value.into_bytes()),
        headers: BTreeMap::from([("content-type".to_string(), CONTENT_TYPE.as_bytes().to_vec())]),
        timestamp: DateTime::from_timestamp_millis(event.time_ms).unwrap_or_else(Utc::now),
    }
}

/// Kafka's default partitioner for a keyed record: `toPositive(murmur2(key))
/// % partitions`, where `toPositive` masks the sign bit rather than negating.
fn partition_for(key: &[u8], partitions: usize) -> usize {
    (murmur2(key) & 0x7fff_ffff) as usize % partitions
}

/// Kafka's `Utils.murmur2`: MurmurHash2, 32-bit, seed `0x9747b28c`, reading
/// little-endian words.
fn murmur2(data: &[u8]) -> u32 {
    const SEED: u32 = 0x9747_b28c;
    const M: u32 = 0x5bd1_e995;
    const R: u32 = 24;

    // Kafka hashes the length as a Java `int`; a key over 2 GiB is not a
    // job id.
    let mut h = SEED ^ data.len() as u32;
    let mut words = data.chunks_exact(4);
    for word in &mut words {
        let mut k = u32::from_le_bytes([word[0], word[1], word[2], word[3]]);
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        h = h.wrapping_mul(M);
        h ^= k;
    }
    let tail = words.remainder();
    if !tail.is_empty() {
        for (i, byte) in tail.iter().enumerate().rev() {
            h ^= u32::from(*byte) << (8 * i);
        }
        h = h.wrapping_mul(M);
    }
    h ^= h >> 13;
    h = h.wrapping_mul(M);
    h ^= h >> 15;
    h
}

/// Follows Kafka's own taxonomy: a protocol error retries when Kafka files it
/// under `RetriableException`, and transport trouble retries; anything else is
/// final. Built from the error's variant and protocol code, never a broker's
/// free-text message.
fn classify(error: &Error) -> DeliveryResult {
    match error {
        Error::Connection(ConnectionError::SaslFailed(SaslError::RequestError(_))) => {
            DeliveryResult::Retry("kafka connection broke during the SASL handshake".into())
        }
        Error::Connection(ConnectionError::SaslFailed(_)) => {
            DeliveryResult::Reject("kafka refused the SASL credentials".into())
        }
        Error::Connection(_) => DeliveryResult::Retry("kafka brokers unreachable".into()),
        Error::Request(RequestError::IO(_) | RequestError::Poisoned(_)) => {
            DeliveryResult::Retry("kafka connection broke".into())
        }
        Error::Request(_) => {
            DeliveryResult::Reject("kafka request could not be encoded or its reply read".into())
        }
        Error::InvalidResponse(_) => {
            DeliveryResult::Retry("kafka sent a response that did not fit the request".into())
        }
        Error::ServerError { protocol_error, .. } if retriable(*protocol_error) => {
            DeliveryResult::Retry(format!("kafka error: {protocol_error:?}"))
        }
        Error::ServerError { protocol_error, .. } => {
            DeliveryResult::Reject(format!("kafka refused the batch: {protocol_error:?}"))
        }
        Error::RetryFailed(_) => {
            DeliveryResult::Retry("kafka stayed unavailable for the whole batch budget".into())
        }
        Error::Timeout => DeliveryResult::Retry("kafka timed out".into()),
        _ => DeliveryResult::Reject("kafka failed in a way this build does not know".into()),
    }
}

/// The protocol errors Kafka's Java client files under `RetriableException`.
fn retriable(error: ProtocolError) -> bool {
    matches!(
        error,
        ProtocolError::CorruptMessage
            | ProtocolError::UnknownTopicOrPartition
            | ProtocolError::LeaderNotAvailable
            | ProtocolError::NotLeaderOrFollower
            | ProtocolError::RequestTimedOut
            | ProtocolError::ReplicaNotAvailable
            | ProtocolError::NetworkException
            | ProtocolError::CoordinatorLoadInProgress
            | ProtocolError::CoordinatorNotAvailable
            | ProtocolError::NotCoordinator
            | ProtocolError::NotEnoughReplicas
            | ProtocolError::NotEnoughReplicasAfterAppend
            | ProtocolError::NotController
            | ProtocolError::KafkaStorageError
            | ProtocolError::FetchSessionIdNotFound
            | ProtocolError::InvalidFetchSessionEpoch
            | ProtocolError::FencedLeaderEpoch
            | ProtocolError::UnknownLeaderEpoch
            | ProtocolError::OffsetNotAvailable
            | ProtocolError::PreferredLeaderNotAvailable
            | ProtocolError::EligibleLeadersNotAvailable
            | ProtocolError::ConcurrentTransactions
            | ProtocolError::UnstableOffsetCommit
            | ProtocolError::ThrottlingQuotaExceeded
            | ProtocolError::UnknownTopicId
    )
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

    fn config(extra: &str) -> KafkaSinkConfig {
        let doc = format!(
            r#"{{"sinks":[{{"kind":"kafka","name":"k","brokers":["localhost:9092"],
            "topic":"flexiq.events"{extra}}}]}}"#
        );
        match EventsConfig::parse(&doc).unwrap().sinks.remove(0) {
            SinkConfig::Kafka(config) => config,
            other => panic!("not a kafka sink: {other:?}"),
        }
    }

    /// Kafka's own `UtilsTest.testMurmur2` vectors, as the Java `int`s it
    /// asserts.
    #[test]
    fn murmur2_matches_kafka() {
        for (key, expected) in [
            (&b"21"[..], -973_932_308),
            (b"foobar", -790_332_482),
            (b"a-little-bit-long-string", -985_981_536),
            (b"a-little-bit-longer-string", -1_486_304_829),
            (
                b"lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8",
                -58_897_971,
            ),
            (b"abc", 479_470_107),
        ] {
            assert_eq!(
                murmur2(key) as i32,
                expected,
                "{:?}",
                String::from_utf8_lossy(key)
            );
        }
    }

    #[test]
    fn a_job_always_maps_to_one_partition_in_range() {
        for partitions in 1..=12 {
            let first = partition_for(b"job-1", partitions);
            assert!(first < partitions);
            assert_eq!(first, partition_for(b"job-1", partitions));
        }
        // The sign bit is masked, not negated: murmur2("21") is negative.
        assert_eq!(
            partition_for(b"21", 7),
            ((-973_932_308_i32 & 0x7fff_ffff) % 7) as usize
        );
    }

    #[test]
    fn a_record_is_the_structured_cloudevent_keyed_by_job_id() {
        let e = event("j1");
        let record = record(&e, "/test", false);
        assert_eq!(record.key.as_deref(), Some(&b"j1"[..]));
        assert_eq!(record.headers["content-type"], CONTENT_TYPE.as_bytes());
        assert_eq!(record.timestamp.timestamp_millis(), e.time_ms);
        let ce: Value = serde_json::from_slice(record.value.as_deref().unwrap()).unwrap();
        assert_eq!(ce["id"], "j1:1:2:job.dead");
        assert_eq!(ce["source"], "/test");
        assert!(ce["data"].get("payload_base64").is_none());

        let with_payload = super::record(&e, "/test", true);
        let ce: Value = serde_json::from_slice(with_payload.value.as_deref().unwrap()).unwrap();
        assert_eq!(ce["data"]["payload_base64"], "AQI=");
    }

    #[test]
    fn unset_sasl_credentials_are_refused_at_start() {
        let config = config(
            r#","sasl":{"mechanism":"scram-sha-512","username_env":"KU","password_env":"KP"}"#,
        );
        let only_user = |name: &str| (name == "KU").then(|| "user".to_string());
        match KafkaSink::with_env(&config, "/test", only_user) {
            Err(EventsConfigError::Sink { sink, message }) => {
                assert_eq!(sink, "k");
                assert!(
                    message.contains("sasl.password_env names 'KP'"),
                    "{message}"
                );
            }
            Err(other) => panic!("unexpected error: {other:?}"),
            Ok(_) => panic!("an unset password was accepted"),
        }
        let both = |_: &str| Some("x".to_string());
        assert!(KafkaSink::with_env(&config, "/test", both).is_ok());
    }

    #[test]
    fn an_unreadable_ca_file_is_refused_at_start() {
        let config = config(r#","tls":true,"ca_file":"/nonexistent/ca.pem""#);
        assert!(matches!(
            KafkaSink::with_env(&config, "/test", |_| None),
            Err(EventsConfigError::Sink { message, .. }) if message.contains("ca_file")
        ));
    }

    fn server_error(protocol_error: ProtocolError) -> Error {
        Error::ServerError {
            protocol_error,
            error_message: Some("broker text".into()),
            request: rskafka::client::error::RequestContext::Topic("t".into()),
            response: None,
            is_virtual: false,
        }
    }

    #[test]
    fn leader_and_replica_trouble_retries() {
        for code in [
            ProtocolError::NotLeaderOrFollower,
            ProtocolError::LeaderNotAvailable,
            ProtocolError::NotEnoughReplicas,
            ProtocolError::RequestTimedOut,
            ProtocolError::UnknownTopicOrPartition,
            ProtocolError::KafkaStorageError,
        ] {
            let result = classify(&server_error(code));
            assert!(
                matches!(result, DeliveryResult::Retry(_)),
                "{code:?}: {result:?}"
            );
        }
        assert!(matches!(
            classify(&Error::Timeout),
            DeliveryResult::Retry(_)
        ));
        let io = std::io::Error::new(std::io::ErrorKind::ConnectionReset, "reset");
        assert!(matches!(
            classify(&Error::Request(RequestError::IO(io))),
            DeliveryResult::Retry(_)
        ));
    }

    #[test]
    fn errors_retrying_cannot_fix_are_rejected() {
        for code in [
            ProtocolError::TopicAuthorizationFailed,
            ProtocolError::MessageTooLarge,
            ProtocolError::RecordListTooLarge,
            ProtocolError::InvalidRecord,
            ProtocolError::SaslAuthenticationFailed,
            ProtocolError::Unknown(-2),
        ] {
            let result = classify(&server_error(code));
            assert!(
                matches!(result, DeliveryResult::Reject(_)),
                "{code:?}: {result:?}"
            );
        }
    }

    #[test]
    fn a_classification_never_carries_the_broker_message() {
        let result = classify(&server_error(ProtocolError::MessageTooLarge));
        let (DeliveryResult::Retry(message) | DeliveryResult::Reject(message)) = result else {
            panic!("delivered");
        };
        assert!(!message.contains("broker text"), "{message}");
    }

    #[test]
    fn an_unreachable_broker_retries_within_the_budget() {
        // Port 1 on loopback: refused at once, so the client's own backoff
        // runs until the batch budget ends it.
        let mut config = config(r#","timeout_ms":300"#);
        config.brokers = vec!["127.0.0.1:1".into()];
        let mut sink = KafkaSink::with_env(&config, "/test", |_| None).unwrap();
        let started = std::time::Instant::now();
        let result = sink.deliver(&[event("j1")]);
        assert!(matches!(result, DeliveryResult::Retry(_)), "{result:?}");
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    /// Only runs against a real broker; skips cleanly when unset. The topic
    /// must exist (or the broker auto-create it) with any partition count.
    #[test]
    fn live_delivery_produces_one_record() {
        let Ok(brokers) = std::env::var("FLEXIQ_KAFKA_TEST_BROKERS") else {
            eprintln!("Skipping: FLEXIQ_KAFKA_TEST_BROKERS unset");
            return;
        };
        let topic = std::env::var("FLEXIQ_KAFKA_TEST_TOPIC").unwrap_or("flexiq-events-test".into());
        let list: Vec<String> = brokers.split(',').map(|b| format!("{b:?}")).collect();
        let doc = format!(
            r#"{{"sinks":[{{"kind":"kafka","name":"k","brokers":[{}],"topic":"{topic}"}}]}}"#,
            list.join(",")
        );
        let SinkConfig::Kafka(config) = EventsConfig::parse(&doc).unwrap().sinks.remove(0) else {
            panic!("not a kafka sink");
        };
        let mut sink = KafkaSink::with_env(&config, "/test", |_| None).unwrap();
        let batch = [event("live-1"), event("live-2")];
        assert_eq!(sink.deliver(&batch), DeliveryResult::Delivered);
        // The held connection is reused.
        assert_eq!(sink.deliver(&batch[..1]), DeliveryResult::Delivered);
    }
}
