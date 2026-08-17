//! Topic pub/sub methods on `JsQueue`. Fan-out and registry semantics live in
//! the core (`flexiq_core::pubsub`); every method touches storage, so each is
//! async and offloads the blocking I/O to the blocking pool.

use flexiq_core::job::now_millis;
use flexiq_core::pubsub::{publish_to_topic, DeliveryDefaults, PublishRequest};
use flexiq_core::storage::records::{NewSubscription, SubscriptionMode};
use flexiq_core::Storage;
use napi::bindgen_prelude::{spawn_blocking, Buffer, Result};
use napi_derive::napi;

use super::JsQueue;
use crate::config::PublishOptions;
use crate::convert::{
    job_to_js, subscription_to_js, topic_log_stat_to_js, topic_message_to_js, topic_stat_to_js,
    topic_to_js, JsJob, JsSubscription, JsTopic, JsTopicLogStat, JsTopicMessage, JsTopicStat,
    DEFAULT_MAX_RETRIES, DEFAULT_PRIORITY, DEFAULT_TIMEOUT_MS,
};
use crate::error::{invalid_arg, join_to_napi_err, non_negative, to_napi_err};

#[napi]
impl JsQueue {
    /// Insert or update a topic subscription (idempotent on topic + name).
    /// Ephemeral subscriptions (`durable: false`) carry the owning worker's id
    /// so they can be reaped once that worker is gone.
    #[napi]
    #[allow(clippy::too_many_arguments)]
    pub async fn register_subscription(
        &self,
        topic: String,
        subscription_name: String,
        task_name: String,
        queue: String,
        durable: bool,
        owner_worker_id: Option<String>,
        priority: Option<i32>,
        max_retries: Option<i32>,
        timeout_ms: Option<i64>,
        mode: Option<String>,
    ) -> Result<()> {
        // An owner-less ephemeral row could never be reaped — reject it before
        // it reaches storage.
        if !durable && owner_worker_id.is_none() {
            return Err(invalid_arg(
                "an ephemeral subscription (durable=false) requires ownerWorkerId",
            ));
        }
        let mode = match mode {
            Some(m) => SubscriptionMode::parse(&m).ok_or_else(|| {
                invalid_arg(format!(
                    "unknown subscription mode '{m}' (expected 'fanout' or 'log')"
                ))
            })?,
            None => SubscriptionMode::default(),
        };
        let storage = self.storage.clone();
        spawn_blocking(move || {
            let row = NewSubscription {
                topic,
                subscription_name,
                task_name,
                queue,
                active: true,
                durable,
                owner_worker_id,
                created_at: now_millis(),
                priority,
                max_retries,
                timeout_ms,
                mode,
            };
            storage.register_subscription(&row).map_err(to_napi_err)
        })
        .await
        .map_err(join_to_napi_err)?
    }

    /// Messages after a log subscription's cursor, oldest first, `<= limit`.
    /// Cursor resolved server-side; the read is exclusive of it.
    #[napi]
    pub async fn read_topic_messages(
        &self,
        topic: String,
        subscription_name: String,
        limit: i64,
    ) -> Result<Vec<JsTopicMessage>> {
        let storage = self.storage.clone();
        spawn_blocking(move || {
            let messages = storage
                .read_topic_messages(&topic, &subscription_name, limit)
                .map_err(to_napi_err)?;
            Ok(messages.into_iter().map(topic_message_to_js).collect())
        })
        .await
        .map_err(join_to_napi_err)?
    }

    /// Advance a log subscription's cursor (monotonic). Returns false when
    /// nothing advanced.
    #[napi]
    pub async fn ack_topic_cursor(
        &self,
        topic: String,
        subscription_name: String,
        cursor: String,
    ) -> Result<bool> {
        let storage = self.storage.clone();
        spawn_blocking(move || {
            storage
                .ack_topic_cursor(&topic, &subscription_name, &cursor)
                .map_err(to_napi_err)
        })
        .await
        .map_err(join_to_napi_err)?
    }

    /// Lease up to `limit` available messages for `visibility_ms`, tracking
    /// per-message state so a nack or lease timeout redelivers just that message.
    /// `now` is computed here so the SDK never passes a clock.
    #[napi]
    pub async fn lease_topic_messages(
        &self,
        topic: String,
        subscription_name: String,
        limit: i64,
        visibility_ms: i64,
    ) -> Result<Vec<JsTopicMessage>> {
        let storage = self.storage.clone();
        spawn_blocking(move || {
            let messages = storage
                .lease_topic_messages(
                    &topic,
                    &subscription_name,
                    limit,
                    visibility_ms,
                    now_millis(),
                )
                .map_err(to_napi_err)?;
            Ok(messages.into_iter().map(topic_message_to_js).collect())
        })
        .await
        .map_err(join_to_napi_err)?
    }

    /// Ack one leased message — done, never redelivered. False if nothing to ack.
    #[napi]
    pub async fn ack_message(
        &self,
        topic: String,
        subscription_name: String,
        message_id: String,
    ) -> Result<bool> {
        let storage = self.storage.clone();
        spawn_blocking(move || {
            storage
                .ack_message(&topic, &subscription_name, &message_id)
                .map_err(to_napi_err)
        })
        .await
        .map_err(join_to_napi_err)?
    }

    /// Nack one leased message — redeliver it now. False if nothing to nack.
    #[napi]
    pub async fn nack_message(
        &self,
        topic: String,
        subscription_name: String,
        message_id: String,
    ) -> Result<bool> {
        let storage = self.storage.clone();
        spawn_blocking(move || {
            storage
                .nack_message(&topic, &subscription_name, &message_id)
                .map_err(to_napi_err)
        })
        .await
        .map_err(join_to_napi_err)?
    }

    /// Lag snapshot per log subscription.
    #[napi]
    pub async fn topic_log_stats(&self) -> Result<Vec<JsTopicLogStat>> {
        let storage = self.storage.clone();
        spawn_blocking(move || {
            let stats = storage.topic_log_stats().map_err(to_napi_err)?;
            Ok(stats.into_iter().map(topic_log_stat_to_js).collect())
        })
        .await
        .map_err(join_to_napi_err)?
    }

    /// Declare a topic (idempotent). A declared log topic retains its publishes
    /// even with no subscriber; `retentionMs` bounds a sub-less backlog.
    #[napi]
    pub async fn declare_topic(
        &self,
        name: String,
        mode: String,
        retention_ms: Option<i64>,
    ) -> Result<()> {
        let parsed = SubscriptionMode::parse(&mode).ok_or_else(|| {
            invalid_arg(format!(
                "unknown subscription mode '{mode}' (expected 'fanout' or 'log')"
            ))
        })?;
        let storage = self.storage.clone();
        spawn_blocking(move || {
            storage
                .declare_topic(&name, parsed, retention_ms)
                .map_err(to_napi_err)
        })
        .await
        .map_err(join_to_napi_err)?
    }

    /// Every declared topic in the registry.
    #[napi]
    pub async fn list_declared_topics(&self) -> Result<Vec<JsTopic>> {
        let storage = self.storage.clone();
        spawn_blocking(move || {
            let topics = storage.list_declared_topics().map_err(to_napi_err)?;
            Ok(topics.into_iter().map(topic_to_js).collect())
        })
        .await
        .map_err(join_to_napi_err)?
    }

    /// List subscriptions — all of them, or only a topic's active ones.
    #[napi]
    pub async fn list_subscriptions(&self, topic: Option<String>) -> Result<Vec<JsSubscription>> {
        let storage = self.storage.clone();
        spawn_blocking(move || {
            let rows = match topic.as_deref() {
                Some(t) => storage.list_subscriptions_for_topic(t),
                None => storage.list_subscriptions(),
            }
            .map_err(to_napi_err)?;
            Ok(rows.into_iter().map(subscription_to_js).collect())
        })
        .await
        .map_err(join_to_napi_err)?
    }

    /// Remove a subscription. Returns false if none matched.
    #[napi]
    pub async fn unsubscribe(&self, topic: String, subscription_name: String) -> Result<bool> {
        let storage = self.storage.clone();
        spawn_blocking(move || {
            storage
                .unsubscribe(&topic, &subscription_name)
                .map_err(to_napi_err)
        })
        .await
        .map_err(join_to_napi_err)?
    }

    /// Pause (false) or resume (true) a subscription without unregistering it.
    /// Returns false if none matched.
    #[napi]
    pub async fn set_subscription_active(
        &self,
        topic: String,
        subscription_name: String,
        active: bool,
    ) -> Result<bool> {
        let storage = self.storage.clone();
        spawn_blocking(move || {
            storage
                .set_subscription_active(&topic, &subscription_name, active)
                .map_err(to_napi_err)
        })
        .await
        .map_err(join_to_napi_err)?
    }

    /// Backlog snapshot per registered subscription — one entry each, even at
    /// zero backlog. Counts are aggregated live off the delivery-attribution
    /// indexes, so they can never drift the way a maintained counter would.
    #[napi]
    pub async fn topic_backlog_stats(&self) -> Result<Vec<JsTopicStat>> {
        let storage = self.storage.clone();
        spawn_blocking(move || {
            let stats = storage.topic_backlog_stats().map_err(to_napi_err)?;
            Ok(stats.into_iter().map(topic_stat_to_js).collect())
        })
        .await
        .map_err(join_to_napi_err)?
    }

    /// Drop ephemeral subscriptions whose owning worker is gone. Runs on the
    /// heartbeat cadence. Returns the number of subscriptions removed.
    ///
    /// Gated behind the same reaper election as the dead-worker reap when a
    /// `workerId` is given: only the leader sweeps, so the scan runs once per
    /// cluster. Without one (a manual call) it runs unconditionally. The live
    /// set is filtered by heartbeat, so a stale worker is excluded whether or
    /// not its registry row has been reaped yet.
    #[napi]
    pub async fn reap_ephemeral_subscriptions(&self, worker_id: Option<String>) -> Result<i64> {
        let storage = self.storage.clone();
        spawn_blocking(move || {
            flexiq_core::storage::sweep_ephemeral_subscriptions(&storage, worker_id.as_deref())
                .map(|n| n as i64)
                .map_err(to_napi_err)
        })
        .await
        .map_err(join_to_napi_err)?
    }

    /// Publish an opaque payload to a topic: one job per active subscription.
    /// Returns the created jobs — empty when nothing is subscribed (a valid
    /// pub/sub no-op, not an error).
    #[napi]
    pub async fn publish(
        &self,
        topic: String,
        payload: Buffer,
        options: Option<PublishOptions>,
    ) -> Result<Vec<JsJob>> {
        let request = build_publish_request(
            topic,
            payload.to_vec(),
            options.unwrap_or_default(),
            self.namespace.clone(),
        )?;
        let storage = self.storage.clone();
        spawn_blocking(move || {
            let jobs = publish_to_topic(&storage, &request).map_err(to_napi_err)?;
            Ok(jobs.into_iter().map(job_to_js).collect())
        })
        .await
        .map_err(join_to_napi_err)?
    }
}

/// Build the core [`PublishRequest`]: validate signed JS inputs (like
/// `build_new_job` does for enqueue) and leave omitted settings `None` so the
/// core's per-subscriber resolution applies.
fn build_publish_request(
    topic: String,
    payload: Vec<u8>,
    opts: PublishOptions,
    namespace: Option<String>,
) -> Result<PublishRequest> {
    let now = now_millis();
    let delay = non_negative(opts.delay_ms.unwrap_or(0), "delayMs")?;
    let max_retries = match opts.max_retries {
        Some(n) => Some(non_negative(n as i64, "maxRetries")? as i32),
        None => None,
    };
    let timeout_ms = match opts.timeout_ms {
        Some(n) => Some(non_negative(n, "timeoutMs")?),
        None => None,
    };
    let expires_at = match opts.expires_ms {
        Some(n) => Some(now.saturating_add(non_negative(n, "expiresMs")?)),
        None => None,
    };
    let result_ttl_ms = match opts.result_ttl_ms {
        Some(n) => Some(non_negative(n, "resultTtlMs")?),
        None => None,
    };
    let queue_defaults = DeliveryDefaults {
        priority: DEFAULT_PRIORITY,
        max_retries: DEFAULT_MAX_RETRIES,
        timeout_ms: DEFAULT_TIMEOUT_MS,
    };
    Ok(PublishRequest {
        topic,
        payload,
        idempotency_key: opts.idempotency_key,
        metadata: opts.metadata,
        notes: opts.notes,
        priority: opts.priority,
        // Saturate so an extreme delay can't overflow into a past schedule.
        scheduled_at: now.saturating_add(delay),
        max_retries,
        timeout_ms,
        expires_at,
        result_ttl_ms,
        namespace,
        queue_defaults,
    })
}
