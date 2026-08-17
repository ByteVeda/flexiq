use pyo3::prelude::*;

use flexiq_core::job::now_millis;
use flexiq_core::pubsub::{publish_to_topic, DeliveryDefaults, PublishRequest};
use flexiq_core::storage::records::{NewSubscription, SubscriptionMode};
use flexiq_core::storage::Storage;

/// Strictly parse a caller-supplied subscription mode. Unlike the lenient reader
/// used for persisted rows, a typo here is a caller error, not a legacy value.
fn parse_mode(mode: &str) -> PyResult<SubscriptionMode> {
    SubscriptionMode::parse(mode).ok_or_else(|| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "unknown subscription mode '{mode}' (expected 'fanout' or 'log')"
        ))
    })
}

use super::PyQueue;
use crate::py_job::PyJob;

/// A subscription row surfaced to Python:
/// `(topic, subscription_name, task_name, queue, active, durable)`.
type SubscriptionTuple = (String, String, String, String, bool, bool);

/// Per-subscription backlog snapshot surfaced to Python:
/// `(topic, subscription, task_name, queue, active, durable, pending, running,
/// dead, oldest_pending_age_ms)`.
type SubscriptionBacklogTuple = (
    String,
    String,
    String,
    String,
    bool,
    bool,
    i64,
    i64,
    i64,
    Option<i64>,
);

/// A log message surfaced to Python:
/// `(id, payload, metadata, notes, created_at)`.
type TopicMessageTuple = (String, Vec<u8>, Option<String>, Option<String>, i64);

/// Per-log-subscription lag surfaced to Python:
/// `(topic, subscription, cursor, lag, oldest_unacked_age_ms)`.
type TopicLogStatsTuple = (String, String, Option<String>, i64, Option<i64>);

/// A declared topic surfaced to Python: `(name, mode, retention_ms, created_at)`.
type TopicTuple = (String, String, Option<i64>, i64);

#[pymethods]
impl PyQueue {
    /// Insert or update a topic subscription (idempotent on topic + name).
    ///
    /// `priority`/`max_retries`/`timeout_ms` (already in milliseconds) persist
    /// the subscriber task's own delivery settings so a producer-only process
    /// can apply them without loading the task.
    #[pyo3(signature = (topic, subscription_name, task_name, queue="default", durable=true, owner_worker_id=None, priority=None, max_retries=None, timeout_ms=None, mode="fanout"))]
    #[allow(clippy::too_many_arguments)]
    pub fn register_subscription(
        &self,
        topic: &str,
        subscription_name: &str,
        task_name: &str,
        queue: &str,
        durable: bool,
        owner_worker_id: Option<&str>,
        priority: Option<i32>,
        max_retries: Option<i32>,
        timeout_ms: Option<i64>,
        mode: &str,
    ) -> PyResult<()> {
        // An unowned ephemeral row could never be reaped (cleanup keys off
        // live worker ids), so it would stay active forever.
        if !durable && owner_worker_id.is_none() {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "an ephemeral subscription (durable=false) requires owner_worker_id",
            ));
        }
        let row = NewSubscription {
            topic: topic.to_string(),
            subscription_name: subscription_name.to_string(),
            task_name: task_name.to_string(),
            queue: queue.to_string(),
            active: true,
            durable,
            owner_worker_id: owner_worker_id.map(str::to_string),
            created_at: now_millis(),
            priority,
            max_retries,
            timeout_ms,
            mode: parse_mode(mode)?,
        };
        self.storage
            .register_subscription(&row)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// List subscriptions — all of them, or only a topic's active ones.
    #[pyo3(signature = (topic=None))]
    pub fn list_subscriptions(&self, topic: Option<&str>) -> PyResult<Vec<SubscriptionTuple>> {
        let rows = match topic {
            Some(t) => self.storage.list_subscriptions_for_topic(t),
            None => self.storage.list_subscriptions(),
        }
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(rows
            .into_iter()
            .map(|row| {
                (
                    row.topic,
                    row.subscription_name,
                    row.task_name,
                    row.queue,
                    row.active,
                    row.durable,
                )
            })
            .collect())
    }

    /// Remove a subscription. Returns false if none matched.
    pub fn unsubscribe(&self, topic: &str, subscription_name: &str) -> PyResult<bool> {
        self.storage
            .unsubscribe(topic, subscription_name)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Pause (false) or resume (true) a subscription without unregistering it.
    pub fn set_subscription_active(
        &self,
        topic: &str,
        subscription_name: &str,
        active: bool,
    ) -> PyResult<bool> {
        self.storage
            .set_subscription_active(topic, subscription_name, active)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Backlog/lag snapshot per subscription:
    /// `(topic, subscription, task_name, queue, active, durable, pending,
    /// running, dead, oldest_pending_age_ms)`.
    pub fn topic_backlog_stats(&self, py: Python<'_>) -> PyResult<Vec<SubscriptionBacklogTuple>> {
        let storage = &self.storage;
        // Release the GIL: aggregation groups over every pub/sub delivery.
        py.detach(|| storage.topic_backlog_stats())
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?
            .into_iter()
            .map(|s| {
                Ok((
                    s.topic,
                    s.subscription_name,
                    s.task_name,
                    s.queue,
                    s.active,
                    s.durable,
                    s.pending,
                    s.running,
                    s.dead,
                    s.oldest_pending_age_ms,
                ))
            })
            .collect()
    }

    /// Messages after a log subscription's cursor, oldest first, `<= limit`:
    /// `(id, payload, metadata, notes, created_at)`. Cursor resolved server-side.
    pub fn read_topic_messages(
        &self,
        py: Python<'_>,
        topic: &str,
        subscription_name: &str,
        limit: i64,
    ) -> PyResult<Vec<TopicMessageTuple>> {
        let storage = &self.storage;
        Ok(py
            .detach(|| storage.read_topic_messages(topic, subscription_name, limit))
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?
            .into_iter()
            .map(|m| (m.id, m.payload, m.metadata, m.notes, m.created_at))
            .collect())
    }

    /// Advance a log subscription's cursor to `cursor` (monotonic). Returns
    /// false when nothing advanced.
    pub fn ack_topic_cursor(
        &self,
        py: Python<'_>,
        topic: &str,
        subscription_name: &str,
        cursor: &str,
    ) -> PyResult<bool> {
        let storage = &self.storage;
        py.detach(|| storage.ack_topic_cursor(topic, subscription_name, cursor))
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Lease up to `limit` available messages for `visibility_ms`, tracking
    /// per-message state so a nack or lease timeout redelivers just that message.
    /// Returns `(id, payload, metadata, notes, created_at)` tuples.
    pub fn lease_topic_messages(
        &self,
        py: Python<'_>,
        topic: &str,
        subscription_name: &str,
        limit: i64,
        visibility_ms: i64,
    ) -> PyResult<Vec<TopicMessageTuple>> {
        let storage = &self.storage;
        Ok(py
            .detach(|| {
                storage.lease_topic_messages(
                    topic,
                    subscription_name,
                    limit,
                    visibility_ms,
                    now_millis(),
                )
            })
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?
            .into_iter()
            .map(|m| (m.id, m.payload, m.metadata, m.notes, m.created_at))
            .collect())
    }

    /// Ack one leased message — done, never redelivered. False if nothing to ack.
    pub fn ack_message(
        &self,
        py: Python<'_>,
        topic: &str,
        subscription_name: &str,
        message_id: &str,
    ) -> PyResult<bool> {
        let storage = &self.storage;
        py.detach(|| storage.ack_message(topic, subscription_name, message_id))
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Nack one leased message — redeliver it now. False if nothing to nack.
    pub fn nack_message(
        &self,
        py: Python<'_>,
        topic: &str,
        subscription_name: &str,
        message_id: &str,
    ) -> PyResult<bool> {
        let storage = &self.storage;
        py.detach(|| storage.nack_message(topic, subscription_name, message_id))
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Per-log-subscription lag snapshot:
    /// `(topic, subscription, cursor, lag, oldest_unacked_age_ms)`.
    pub fn topic_log_stats(&self, py: Python<'_>) -> PyResult<Vec<TopicLogStatsTuple>> {
        let storage = &self.storage;
        Ok(py
            .detach(|| storage.topic_log_stats())
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?
            .into_iter()
            .map(|s| {
                (
                    s.topic,
                    s.subscription_name,
                    s.cursor,
                    s.lag,
                    s.oldest_unacked_age_ms,
                )
            })
            .collect())
    }

    /// Declare a topic (idempotent). A declared log topic retains its publishes
    /// even with no subscriber; `retention_ms` bounds a sub-less backlog.
    #[pyo3(signature = (name, mode, retention_ms=None))]
    pub fn declare_topic(
        &self,
        py: Python<'_>,
        name: &str,
        mode: &str,
        retention_ms: Option<i64>,
    ) -> PyResult<()> {
        let storage = &self.storage;
        let mode = parse_mode(mode)?;
        py.detach(|| storage.declare_topic(name, mode, retention_ms))
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Every declared topic: `(name, mode, retention_ms, created_at)`.
    pub fn list_declared_topics(&self, py: Python<'_>) -> PyResult<Vec<TopicTuple>> {
        let storage = &self.storage;
        Ok(py
            .detach(|| storage.list_declared_topics())
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?
            .into_iter()
            .map(|t| {
                (
                    t.name,
                    t.mode.as_str().to_string(),
                    t.retention_ms,
                    t.created_at,
                )
            })
            .collect())
    }

    /// Drop ephemeral subscriptions whose owning worker is gone. Runs on the
    /// heartbeat cadence, after `reap_dead_workers` has pruned the registry.
    ///
    /// Gated behind the same reaper election as the dead-worker reap: only the
    /// leader sweeps, so the scan runs once per cluster rather than once per
    /// worker. A non-leader returns 0. `worker_id` is the election owner.
    #[pyo3(signature = (worker_id=None))]
    pub fn reap_ephemeral_subscriptions(
        &self,
        py: Python<'_>,
        worker_id: Option<&str>,
    ) -> PyResult<u64> {
        let storage = &self.storage;
        // Release the GIL: the reap scans every worker + subscription, which
        // must not freeze other Python threads while it runs.
        py.detach(|| {
            // An explicit owner elects a single reaper; without one (a manual
            // admin call) the sweep runs unconditionally.
            flexiq_core::storage::sweep_ephemeral_subscriptions(storage, worker_id)
        })
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Publish a payload to a topic: one job per active subscription.
    /// Returns the created jobs — empty when nothing is subscribed.
    #[pyo3(signature = (topic, payload, idempotency_key=None, metadata=None, notes=None, priority=None, delay_seconds=None, max_retries=None, timeout=None, expires=None, result_ttl=None))]
    #[allow(clippy::too_many_arguments)]
    pub fn publish(
        &self,
        py: Python<'_>,
        topic: &str,
        payload: Vec<u8>,
        idempotency_key: Option<String>,
        metadata: Option<String>,
        notes: Option<String>,
        priority: Option<i32>,
        delay_seconds: Option<f64>,
        max_retries: Option<i32>,
        timeout: Option<i64>,
        expires: Option<f64>,
        result_ttl: Option<i64>,
    ) -> PyResult<Vec<PyJob>> {
        let now = now_millis();
        let scheduled_at = match delay_seconds {
            Some(d) => {
                if !d.is_finite() || d < 0.0 {
                    return Err(pyo3::exceptions::PyValueError::new_err(
                        "delay_seconds must be a finite non-negative number",
                    ));
                }
                now.checked_add((d * 1000.0) as i64).ok_or_else(|| {
                    pyo3::exceptions::PyValueError::new_err(
                        "delay_seconds too large, would overflow",
                    )
                })?
            }
            None => now,
        };
        let expires_at = match expires {
            Some(e) => {
                if !e.is_finite() || e < 0.0 {
                    return Err(pyo3::exceptions::PyValueError::new_err(
                        "expires must be a finite non-negative number",
                    ));
                }
                Some(now.checked_add((e * 1000.0) as i64).ok_or_else(|| {
                    pyo3::exceptions::PyValueError::new_err("expires too large, would overflow")
                })?)
            }
            None => None,
        };
        let result_ttl_ms = match result_ttl {
            Some(s) => Some(s.checked_mul(1000).ok_or_else(|| {
                pyo3::exceptions::PyValueError::new_err("result_ttl too large, would overflow")
            })?),
            None => None,
        };
        let timeout_ms = match timeout {
            Some(t) => Some(t.checked_mul(1000).ok_or_else(|| {
                pyo3::exceptions::PyValueError::new_err("timeout too large, would overflow")
            })?),
            None => None,
        };

        let request = PublishRequest {
            topic: topic.to_string(),
            payload,
            idempotency_key,
            metadata,
            notes,
            priority,
            scheduled_at,
            max_retries,
            timeout_ms,
            expires_at,
            result_ttl_ms,
            namespace: self.namespace.clone(),
            queue_defaults: DeliveryDefaults {
                priority: self.default_priority,
                max_retries: self.default_retry,
                timeout_ms: self.default_timeout.saturating_mul(1000),
            },
        };
        let storage = &self.storage;
        // Release the GIL: fan-out can create one job per subscription, which
        // must not block every other Python thread for the duration.
        let jobs = py
            .detach(|| publish_to_topic(storage, &request))
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(jobs.into_iter().map(PyJob::from).collect())
    }
}
