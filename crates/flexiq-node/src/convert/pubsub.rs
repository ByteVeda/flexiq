//! JS-facing shapes for topic subscriptions and log messages.

use flexiq_core::storage::records::{Subscription, Topic, TopicLogStats, TopicMessage};
use flexiq_core::storage::SubscriptionBacklogStats;
use napi::bindgen_prelude::Buffer;
use napi_derive::napi;

/// A topic subscription: routes messages published to `topic` to `taskName`
/// jobs on `queue`, one delivery per active subscription.
#[napi(object)]
pub struct JsSubscription {
    pub topic: String,
    pub subscription_name: String,
    pub task_name: String,
    pub queue: String,
    pub active: bool,
    pub durable: bool,
}

/// Convert a core [`Subscription`] into its JS-facing shape.
pub fn subscription_to_js(row: Subscription) -> JsSubscription {
    JsSubscription {
        topic: row.topic,
        subscription_name: row.subscription_name,
        task_name: row.task_name,
        queue: row.queue,
        active: row.active,
        durable: row.durable,
    }
}

/// Backlog snapshot for one topic subscription. One entry per *registered*
/// subscription — durable or ephemeral, active or paused — even at zero
/// backlog, so a caller renders the full subscriber list from a single call.
#[napi(object)]
pub struct JsTopicStat {
    pub topic: String,
    pub subscription: String,
    pub task_name: String,
    pub queue: String,
    pub active: bool,
    pub durable: bool,
    /// Deliveries waiting to run.
    pub pending: i64,
    /// Deliveries currently executing.
    pub running: i64,
    /// Deliveries in the dead-letter queue.
    pub dead: i64,
    /// Age (ms) of the oldest still-pending delivery, absent at zero backlog.
    pub oldest_pending_age_ms: Option<i64>,
}

/// Convert a core [`SubscriptionBacklogStats`] into its JS-facing shape.
pub fn topic_stat_to_js(stat: SubscriptionBacklogStats) -> JsTopicStat {
    JsTopicStat {
        topic: stat.topic,
        subscription: stat.subscription_name,
        task_name: stat.task_name,
        queue: stat.queue,
        active: stat.active,
        durable: stat.durable,
        pending: stat.pending,
        running: stat.running,
        dead: stat.dead,
        oldest_pending_age_ms: stat.oldest_pending_age_ms,
    }
}

/// A message pulled from a log topic. `id` is the cursor token to pass to
/// `ackTopic`; `payload` is the opaque published bytes.
#[napi(object)]
pub struct JsTopicMessage {
    pub id: String,
    pub payload: Buffer,
    pub metadata: Option<String>,
    pub notes: Option<String>,
    pub created_at: i64,
}

/// Convert a core [`TopicMessage`] into its JS-facing shape.
pub fn topic_message_to_js(msg: TopicMessage) -> JsTopicMessage {
    JsTopicMessage {
        id: msg.id,
        payload: msg.payload.into(),
        metadata: msg.metadata,
        notes: msg.notes,
        created_at: msg.created_at,
    }
}

/// Lag snapshot for one log subscription.
#[napi(object)]
pub struct JsTopicLogStat {
    pub topic: String,
    pub subscription: String,
    pub cursor: Option<String>,
    pub lag: i64,
    pub oldest_unacked_age_ms: Option<i64>,
}

/// Convert a core [`TopicLogStats`] into its JS-facing shape.
pub fn topic_log_stat_to_js(stat: TopicLogStats) -> JsTopicLogStat {
    JsTopicLogStat {
        topic: stat.topic,
        subscription: stat.subscription_name,
        cursor: stat.cursor,
        lag: stat.lag,
        oldest_unacked_age_ms: stat.oldest_unacked_age_ms,
    }
}

/// A declared topic in the registry. A declared log topic retains its publishes
/// even with no subscriber; `retentionMs` bounds a sub-less backlog (absent =
/// kept until consumed). `createdAt` is Unix milliseconds.
#[napi(object)]
pub struct JsTopic {
    pub name: String,
    pub mode: String,
    pub retention_ms: Option<i64>,
    pub created_at: i64,
}

/// Convert a core [`Topic`] into its JS-facing shape.
pub fn topic_to_js(topic: Topic) -> JsTopic {
    JsTopic {
        name: topic.name,
        mode: topic.mode.as_str().to_string(),
        retention_ms: topic.retention_ms,
        created_at: topic.created_at,
    }
}
