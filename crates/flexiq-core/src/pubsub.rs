//! Topic pub/sub: fan-out (one job per active subscription) plus opt-in log
//! mode (one durable message per publish, pulled via a per-subscription cursor).
//!
//! Lives in the core (not the language shells) so every binding shares the
//! same semantics — in particular the idempotency-key salting, which silently
//! drops deliveries if a shell gets it wrong.

use crate::error::{QueueError, Result};
use crate::job::{now_millis, Job, NewJob};
use crate::storage::records::{Subscription, SubscriptionMode};
use crate::storage::Storage;

/// Validate a topic declaration before a backend persists it: only `"log"`
/// topics are declarable, and a retention window must be non-negative (a
/// negative one would expire messages immediately or overflow `now + retention`).
pub(crate) fn validate_topic_declaration(
    mode: SubscriptionMode,
    retention_ms: Option<i64>,
) -> Result<()> {
    if !mode.is_log() {
        return Err(QueueError::Config(format!(
            "only \"log\" topics are declarable, got {mode:?}"
        )));
    }
    if retention_ms.is_some_and(|ms| ms < 0) {
        return Err(QueueError::Config(
            "topic retention_ms must be non-negative".to_string(),
        ));
    }
    Ok(())
}

/// Queue-level fallback delivery settings, used when neither the publish call
/// nor the subscription row specifies a value.
#[derive(Clone, Copy)]
pub struct DeliveryDefaults {
    /// Default dispatch priority for deliveries.
    pub priority: i32,
    /// Default retry cap for deliveries.
    pub max_retries: i32,
    /// Default execution timeout in milliseconds.
    pub timeout_ms: i64,
}

/// Everything a publish shares across the jobs it fans out to. Per-subscriber
/// routing (task name, queue) and delivery settings come from the subscription
/// registry; delivery settings resolve per field as explicit publish override,
/// then the subscription row's persisted setting, then the queue default.
pub struct PublishRequest {
    /// Topic to fan out on.
    pub topic: String,
    /// Wire-envelope payload bytes; every subscriber receives the same body.
    pub payload: Vec<u8>,
    /// Salted per subscription identity (topic + name, length-prefixed so
    /// delimiter characters in either part cannot collide) before becoming a
    /// job `unique_key`: the jobs table's unique index is global, so reusing
    /// the raw key across the fan-out would dedup away all but one delivery.
    pub idempotency_key: Option<String>,
    /// Pre-encoded JSON of free-form caller metadata, copied to every delivery.
    pub metadata: Option<String>,
    /// Pre-validated canonical notes JSON object (see `Job::notes`); the
    /// topic and subscription name are stamped in per delivery.
    pub notes: Option<String>,
    /// Priority override for every delivery. `None` = subscription, then default.
    pub priority: Option<i32>,
    /// Unix-millisecond time the deliveries become eligible to run.
    pub scheduled_at: i64,
    /// Retry-cap override for every delivery. `None` = subscription, then default.
    pub max_retries: Option<i32>,
    /// Timeout override in milliseconds. `None` = subscription, then default.
    pub timeout_ms: Option<i64>,
    /// Unix-millisecond expiry for still-pending deliveries.
    pub expires_at: Option<i64>,
    /// How long each delivery's archived result is kept, in milliseconds.
    pub result_ttl_ms: Option<i64>,
    /// Tenant namespace the deliveries are scoped to. `None` = default namespace.
    pub namespace: Option<String>,
    /// Queue-level fallback delivery settings.
    pub queue_defaults: DeliveryDefaults,
}

/// Fan a message out to every active subscription of `topic` as ordinary
/// jobs. Returns the created jobs — empty when the topic has no active
/// subscriptions (a valid pub/sub no-op, not an error).
///
/// Keyed publishes go through `enqueue_unique_batch` (one transaction) so a
/// re-published event dedups per subscriber instead of failing the whole batch
/// on the unique index; unkeyed publishes use one batch insert.
pub fn publish_to_topic<S: Storage>(storage: &S, request: &PublishRequest) -> Result<Vec<Job>> {
    let subscriptions = storage.list_subscriptions_for_topic(&request.topic)?;
    let has_log_sub = subscriptions.iter().any(|s| s.mode.is_log());

    // A log topic stores one durable message that consumers pull via cursor;
    // fan-out subscribers still get one job each. A topic may mix both modes.
    // Decide whether to write the log row:
    //   - a live log subscriber always gets one (retention via min-cursor);
    //   - otherwise a *declared* log topic still retains the publish (removing
    //     the late-join boundary), with `retention_ms` as an expiry TTL so the
    //     sweep can reclaim it. The registry lookup only runs when there is no
    //     log subscriber, so the log-subscriber hot path adds no extra query.
    let log_expires = if has_log_sub {
        Some(request.expires_at)
    } else {
        match storage.get_topic(&request.topic)? {
            Some(topic) if topic.is_log() => Some(
                request
                    .expires_at
                    .or_else(|| topic.retention_ms.map(|ms| now_millis() + ms)),
            ),
            _ => None,
        }
    };
    if let Some(expires_at) = log_expires {
        storage.publish_message(
            &request.topic,
            &request.payload,
            request.metadata.as_deref(),
            request.notes.as_deref(),
            expires_at,
        )?;
    }

    let jobs: Vec<NewJob> = subscriptions
        .iter()
        .filter(|sub| !sub.mode.is_log())
        .map(|sub| delivery_job(request, sub))
        .collect();
    if jobs.is_empty() {
        return Ok(Vec::new());
    }
    if request.idempotency_key.is_none() {
        return storage.enqueue_batch(jobs);
    }
    // Keyed fan-out: one transaction dedupe-inserting every delivery, instead
    // of one write transaction per subscriber (N database-wide write locks).
    storage.enqueue_unique_batch(jobs)
}

/// `key::<topic_len>:<name_len>:<topic><name>` — length prefixes make the
/// encoding injective, so distinct (topic, name) pairs can never produce the
/// same salted key even when the parts contain the delimiter characters.
fn salted_unique_key(key: &str, topic: &str, subscription_name: &str) -> String {
    format!(
        "{key}::{}:{}:{topic}{subscription_name}",
        topic.len(),
        subscription_name.len()
    )
}

fn delivery_job(request: &PublishRequest, sub: &Subscription) -> NewJob {
    // Resolve each field independently: explicit publish override, then the
    // subscription's persisted setting, then the queue default. Persisting on
    // the row is what lets a producer-only process apply a subscriber's own
    // retry/timeout/priority without ever loading its task code.
    let defaults = request.queue_defaults;
    NewJob {
        queue: sub.queue.clone(),
        task_name: sub.task_name.clone(),
        payload: request.payload.clone(),
        priority: request
            .priority
            .or(sub.priority)
            .unwrap_or(defaults.priority),
        scheduled_at: request.scheduled_at,
        max_retries: request
            .max_retries
            .or(sub.max_retries)
            .unwrap_or(defaults.max_retries),
        timeout_ms: request
            .timeout_ms
            .or(sub.timeout_ms)
            .unwrap_or(defaults.timeout_ms),
        unique_key: request
            .idempotency_key
            .as_ref()
            .map(|key| salted_unique_key(key, &request.topic, &sub.subscription_name)),
        metadata: request.metadata.clone(),
        notes: Some(delivery_notes(request, sub)),
        depends_on: Vec::new(),
        expires_at: request.expires_at,
        result_ttl_ms: request.result_ttl_ms,
        namespace: request.namespace.clone(),
        debounce_key: None,
    }
}

/// Inverse of `delivery_notes`: pull `topic`/`subscription` back out of a
/// job's notes JSON when both are present. Feeds the indexed
/// `jobs.topic`/`jobs.subscription_name` columns (and their `dead_letter`
/// mirrors) at insert time, so backlog/lag aggregation runs off an index
/// instead of a JSON scan — without adding pub/sub-only fields to `NewJob`.
pub fn extract_topic_subscription(notes: Option<&str>) -> Option<(String, String)> {
    let obj: serde_json::Map<String, serde_json::Value> = serde_json::from_str(notes?).ok()?;
    let topic = obj.get("topic")?.as_str()?.to_string();
    let subscription = obj.get("subscription")?.as_str()?.to_string();
    Some((topic, subscription))
}

/// Stamp `topic` and `subscription` into the caller's notes object so every
/// delivery is filterable per subscriber without a schema change.
fn delivery_notes(request: &PublishRequest, sub: &Subscription) -> String {
    let mut notes = request
        .notes
        .as_deref()
        .and_then(|raw| {
            serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(raw).ok()
        })
        .unwrap_or_default();
    notes.insert(
        "topic".to_string(),
        serde_json::Value::String(request.topic.clone()),
    );
    notes.insert(
        "subscription".to_string(),
        serde_json::Value::String(sub.subscription_name.clone()),
    );
    serde_json::Value::Object(notes).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::job::now_millis;
    use crate::storage::records::NewSubscription;
    use crate::SqliteStorage;

    fn request(topic: &str, idempotency_key: Option<&str>) -> PublishRequest {
        PublishRequest {
            topic: topic.to_string(),
            payload: vec![0x02, 0xf5],
            idempotency_key: idempotency_key.map(str::to_string),
            metadata: None,
            notes: None,
            priority: None,
            scheduled_at: now_millis(),
            max_retries: None,
            timeout_ms: None,
            expires_at: None,
            result_ttl_ms: None,
            namespace: None,
            queue_defaults: DeliveryDefaults {
                priority: 0,
                max_retries: 3,
                timeout_ms: 300_000,
            },
        }
    }

    fn subscribe(storage: &SqliteStorage, topic: &str, name: &str, task: &str) {
        subscribe_with(storage, topic, name, task, None, None, None);
    }

    #[allow(clippy::too_many_arguments)]
    fn subscribe_with(
        storage: &SqliteStorage,
        topic: &str,
        name: &str,
        task: &str,
        priority: Option<i32>,
        max_retries: Option<i32>,
        timeout_ms: Option<i64>,
    ) {
        storage
            .register_subscription(&NewSubscription {
                topic: topic.to_string(),
                subscription_name: name.to_string(),
                task_name: task.to_string(),
                queue: "default".to_string(),
                active: true,
                durable: true,
                owner_worker_id: None,
                created_at: now_millis(),
                priority,
                max_retries,
                timeout_ms,
                mode: SubscriptionMode::Fanout,
            })
            .unwrap();
    }

    #[test]
    fn publish_with_no_subscriptions_is_a_no_op() {
        let storage = SqliteStorage::in_memory().unwrap();
        let jobs = publish_to_topic(&storage, &request("orders", None)).unwrap();
        assert!(jobs.is_empty());
    }

    fn subscribe_log(storage: &SqliteStorage, topic: &str, name: &str) {
        storage
            .register_subscription(&NewSubscription {
                topic: topic.to_string(),
                subscription_name: name.to_string(),
                task_name: String::new(),
                queue: "default".to_string(),
                active: true,
                durable: true,
                owner_worker_id: None,
                created_at: now_millis(),
                priority: None,
                max_retries: None,
                timeout_ms: None,
                mode: SubscriptionMode::Log,
            })
            .unwrap();
    }

    #[test]
    fn log_topic_writes_one_message_and_no_jobs() {
        let storage = SqliteStorage::in_memory().unwrap();
        subscribe_log(&storage, "events", "analytics");

        // Two publishes → two log rows, zero fan-out jobs, regardless of readers.
        assert!(publish_to_topic(&storage, &request("events", None))
            .unwrap()
            .is_empty());
        assert!(publish_to_topic(&storage, &request("events", None))
            .unwrap()
            .is_empty());
        let msgs = storage
            .read_topic_messages("events", "analytics", 10)
            .unwrap();
        assert_eq!(msgs.len(), 2);
    }

    #[test]
    fn mixed_topic_logs_once_and_fans_out_to_fanout_subs() {
        let storage = SqliteStorage::in_memory().unwrap();
        subscribe(&storage, "events", "email", "send_email");
        subscribe_log(&storage, "events", "analytics");

        let jobs = publish_to_topic(&storage, &request("events", None)).unwrap();
        // One fan-out job (email), one log message (analytics).
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].task_name, "send_email");
        assert_eq!(
            storage
                .read_topic_messages("events", "analytics", 10)
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn publish_fans_out_one_job_per_subscription() {
        let storage = SqliteStorage::in_memory().unwrap();
        subscribe(&storage, "orders", "email", "send_email");
        subscribe(&storage, "orders", "analytics", "track_order");

        let jobs = publish_to_topic(&storage, &request("orders", None)).unwrap();

        assert_eq!(jobs.len(), 2);
        let mut tasks: Vec<_> = jobs.iter().map(|j| j.task_name.as_str()).collect();
        tasks.sort_unstable();
        assert_eq!(tasks, ["send_email", "track_order"]);
        assert!(jobs.iter().all(|j| j.payload == vec![0x02, 0xf5]));
    }

    #[test]
    fn idempotency_key_is_salted_per_subscription() {
        // The jobs unique index is global: an unsalted key would let only the
        // first fan-out insert win and silently drop the other deliveries.
        let storage = SqliteStorage::in_memory().unwrap();
        subscribe(&storage, "orders", "email", "send_email");
        subscribe(&storage, "orders", "analytics", "track_order");

        let first = publish_to_topic(&storage, &request("orders", Some("evt-42"))).unwrap();
        assert_eq!(first.len(), 2);
        let mut keys: Vec<_> = first.iter().filter_map(|j| j.unique_key.clone()).collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            ["evt-42::6:5:ordersemail", "evt-42::6:9:ordersanalytics"]
        );

        // Same event published twice: every subscriber still has exactly one
        // delivery (per-subscriber dedup), not zero and not two.
        let second = publish_to_topic(&storage, &request("orders", Some("evt-42"))).unwrap();
        assert_eq!(second.len(), 2);
        assert_eq!(
            first.iter().map(|j| &j.id).collect::<Vec<_>>(),
            second.iter().map(|j| &j.id).collect::<Vec<_>>(),
        );
    }

    #[test]
    fn delivery_settings_resolve_override_then_subscription_then_queue() {
        use std::collections::HashMap;
        let storage = SqliteStorage::in_memory().unwrap();
        // send_email persisted its own settings on the subscription row;
        // audit_log has none → queue defaults.
        subscribe_with(
            &storage,
            "orders",
            "email",
            "send_email",
            Some(5),
            Some(0),
            Some(60_000),
        );
        subscribe(&storage, "orders", "audit", "audit_log");

        let jobs = publish_to_topic(&storage, &request("orders", None)).unwrap();
        let by_task: HashMap<_, _> = jobs.iter().map(|j| (j.task_name.as_str(), j)).collect();
        assert_eq!(by_task["send_email"].max_retries, 0);
        assert_eq!(by_task["send_email"].priority, 5);
        assert_eq!(by_task["send_email"].timeout_ms, 60_000);
        assert_eq!(by_task["audit_log"].max_retries, 3);

        // An explicit publish-level override beats both the row and the default.
        let mut req = request("orders", None);
        req.max_retries = Some(9);
        let jobs = publish_to_topic(&storage, &req).unwrap();
        assert!(jobs.iter().all(|j| j.max_retries == 9));
    }

    #[test]
    fn paused_subscriptions_are_skipped() {
        let storage = SqliteStorage::in_memory().unwrap();
        subscribe(&storage, "orders", "email", "send_email");
        subscribe(&storage, "orders", "analytics", "track_order");
        storage
            .set_subscription_active("orders", "analytics", false)
            .unwrap();

        let jobs = publish_to_topic(&storage, &request("orders", None)).unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].task_name, "send_email");
    }

    #[test]
    fn deliveries_carry_topic_and_subscription_notes() {
        let storage = SqliteStorage::in_memory().unwrap();
        subscribe(&storage, "orders", "email", "send_email");

        let mut req = request("orders", None);
        req.notes = Some(r#"{"tenant":"acme"}"#.to_string());
        let jobs = publish_to_topic(&storage, &req).unwrap();

        let notes: serde_json::Value =
            serde_json::from_str(jobs[0].notes.as_deref().unwrap()).unwrap();
        assert_eq!(notes["topic"], "orders");
        assert_eq!(notes["subscription"], "email");
        assert_eq!(notes["tenant"], "acme");
    }

    #[test]
    fn declared_log_topic_retains_with_zero_subscribers() {
        let storage = SqliteStorage::in_memory().unwrap();
        storage
            .declare_topic("events", SubscriptionMode::Log, None)
            .unwrap();

        // No subscriber at publish time, but the topic is declared → retained.
        assert!(publish_to_topic(&storage, &request("events", None))
            .unwrap()
            .is_empty());

        // A log subscriber that joins later still sees the earlier publish.
        subscribe_log(&storage, "events", "analytics");
        let msgs = storage
            .read_topic_messages("events", "analytics", 10)
            .unwrap();
        assert_eq!(msgs.len(), 1);
    }

    #[test]
    fn undeclared_topic_with_no_log_sub_stores_nothing() {
        let storage = SqliteStorage::in_memory().unwrap();
        // Neither declared nor any log subscriber → no log row (unchanged behavior).
        publish_to_topic(&storage, &request("events", None)).unwrap();
        subscribe_log(&storage, "events", "late");
        assert!(storage
            .read_topic_messages("events", "late", 10)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn declared_topic_retention_sets_message_expiry() {
        let storage = SqliteStorage::in_memory().unwrap();
        storage
            .declare_topic("events", SubscriptionMode::Log, Some(60_000))
            .unwrap();
        publish_to_topic(&storage, &request("events", None)).unwrap();

        subscribe_log(&storage, "events", "c");
        let msgs = storage.read_topic_messages("events", "c", 10).unwrap();
        assert_eq!(msgs.len(), 1);
        assert!(msgs[0].expires_at.is_some());
    }

    #[test]
    fn declare_topic_is_idempotent_and_preserves_created_at() {
        let storage = SqliteStorage::in_memory().unwrap();
        assert!(storage.get_topic("events").unwrap().is_none());

        storage
            .declare_topic("events", SubscriptionMode::Log, Some(1000))
            .unwrap();
        let first = storage.get_topic("events").unwrap().unwrap();
        assert_eq!(first.name, "events");
        assert!(first.is_log());
        assert_eq!(first.retention_ms, Some(1000));

        // Re-declaring updates retention but keeps the original created_at.
        // Sleep so the re-declare lands in a later millisecond — otherwise a
        // regression that overwrote created_at could still pass this assertion.
        std::thread::sleep(std::time::Duration::from_millis(2));
        storage
            .declare_topic("events", SubscriptionMode::Log, Some(2000))
            .unwrap();
        let second = storage.get_topic("events").unwrap().unwrap();
        assert_eq!(second.retention_ms, Some(2000));
        assert_eq!(second.created_at, first.created_at);
        assert_eq!(storage.list_declared_topics().unwrap().len(), 1);
    }

    #[test]
    fn declare_topic_rejects_bad_mode_and_negative_retention() {
        let storage = SqliteStorage::in_memory().unwrap();
        assert!(storage
            .declare_topic("events", SubscriptionMode::Fanout, None)
            .is_err());
        assert!(storage
            .declare_topic("events", SubscriptionMode::Log, Some(-1))
            .is_err());
        // Nothing was persisted on rejection.
        assert!(storage.get_topic("events").unwrap().is_none());
    }
}
