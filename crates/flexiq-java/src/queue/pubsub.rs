//! Pub/sub JNI entry points: the topic-subscription registry and `publish`.
//!
//! Fan-out semantics (per-subscriber idempotency-key salting, notes stamping,
//! delivery-settings resolution) live in the core's `publish_to_topic`; this
//! module only marshals arguments and views.

use flexiq_core::job::now_millis;
use flexiq_core::pubsub::publish_to_topic;
use flexiq_core::storage::records::{NewSubscription, SubscriptionMode};
use flexiq_core::Storage;
use jni::objects::{JByteArray, JClass, JString};
use jni::sys::{jboolean, jint, jlong, jstring, JNI_FALSE};
use jni::JNIEnv;

use crate::backend;
use crate::convert::{
    build_publish_request, parse_json, to_json, JobView, PublishOptions, SubscriptionView,
    TopicLogStatsView, TopicMessageView, TopicStatsView, TopicView,
};
use crate::ffi::{guard, new_string, read_bytes, read_optional_string, read_string};

use super::{borrow_queue, to_jboolean};

/// `void registerSubscription(long handle, String topic, String subscriptionName,
/// String taskName, String queue, boolean durable, String ownerWorkerIdOrNull,
/// int priority, int maxRetries, long timeoutMs, String mode)` — insert or update
/// a subscription (idempotent on topic + name). The three delivery-setting
/// numerics use `i32::MIN` / `i64::MIN` as the "unset → queue default" sentinel,
/// since JNI carries primitives, not boxed nullables. `mode` is `"fanout"`
/// (one job per publish) or `"log"` (append-once + cursor).
#[no_mangle]
#[allow(clippy::too_many_arguments)]
pub extern "system" fn Java_org_byteveda_flexiq_internal_NativeQueue_registerSubscription<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    topic: JString<'local>,
    subscription_name: JString<'local>,
    task_name: JString<'local>,
    queue: JString<'local>,
    durable: jboolean,
    owner_worker_id: JString<'local>,
    priority: jint,
    max_retries: jint,
    timeout_ms: jlong,
    mode: JString<'local>,
) {
    guard(&mut env, (), |env| {
        let queue_handle = unsafe { borrow_queue(handle) };
        let topic = read_string(env, &topic)?;
        let subscription_name = read_string(env, &subscription_name)?;
        let task_name = read_string(env, &task_name)?;
        let queue = read_string(env, &queue)?;
        let owner_worker_id = read_optional_string(env, &owner_worker_id)?;
        let mode_wire = read_string(env, &mode)?;
        let mode = SubscriptionMode::parse(&mode_wire).ok_or_else(|| {
            crate::error::BindingError::new(format!(
                "unknown subscription mode '{mode_wire}' (expected 'fanout' or 'log')"
            ))
        })?;
        // An ownerless ephemeral row would never be reaped (the reaper matches
        // on owner) yet keeps receiving deliveries — reject it up front.
        if durable == 0 && owner_worker_id.is_none() {
            return Err(crate::error::BindingError::new(
                "an ephemeral subscription (durable=false) requires ownerWorkerId",
            ));
        }
        let row = NewSubscription {
            topic,
            subscription_name,
            task_name,
            queue,
            // Insert default only: the core upsert preserves an existing row's
            // `active` (and `created_at`), so re-declaring never resumes a pause.
            active: true,
            durable: durable != 0,
            owner_worker_id,
            created_at: now_millis(),
            priority: (priority != jint::MIN).then_some(priority),
            max_retries: (max_retries != jint::MIN).then_some(max_retries),
            timeout_ms: (timeout_ms != jlong::MIN).then_some(timeout_ms),
            mode,
        };
        queue_handle.storage.register_subscription(&row)?;
        Ok(())
    })
}

/// `String listSubscriptions(long handle, String topicOrNull)` — a JSON array
/// of subscriptions: all of them, or only a topic's active ones.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_flexiq_internal_NativeQueue_listSubscriptions<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    topic: JString<'local>,
) -> jstring {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let rows = match read_optional_string(env, &topic)? {
            Some(topic) => queue.storage.list_subscriptions_for_topic(&topic)?,
            None => queue.storage.list_subscriptions()?,
        };
        let views: Vec<SubscriptionView> = rows.iter().map(SubscriptionView::from).collect();
        new_string(env, to_json(&views)?)
    })
}

/// `boolean unsubscribe(long handle, String topic, String subscriptionName)` —
/// remove a subscription; false if none matched.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_flexiq_internal_NativeQueue_unsubscribe<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    topic: JString<'local>,
    subscription_name: JString<'local>,
) -> jboolean {
    guard(&mut env, JNI_FALSE, |env| {
        let queue = unsafe { borrow_queue(handle) };
        let topic = read_string(env, &topic)?;
        let subscription_name = read_string(env, &subscription_name)?;
        Ok(to_jboolean(
            queue.storage.unsubscribe(&topic, &subscription_name)?,
        ))
    })
}

/// `boolean setSubscriptionActive(long handle, String topic, String subscriptionName,
/// boolean active)` — pause (false) or resume (true) without unregistering;
/// false if none matched.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_flexiq_internal_NativeQueue_setSubscriptionActive<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    topic: JString<'local>,
    subscription_name: JString<'local>,
    active: jboolean,
) -> jboolean {
    guard(&mut env, JNI_FALSE, |env| {
        let queue = unsafe { borrow_queue(handle) };
        let topic = read_string(env, &topic)?;
        let subscription_name = read_string(env, &subscription_name)?;
        Ok(to_jboolean(queue.storage.set_subscription_active(
            &topic,
            &subscription_name,
            active != 0,
        )?))
    })
}

/// `String topicBacklogStats(long handle)` — a JSON array of `TopicStatsView`,
/// one backlog snapshot per registered subscription (even at zero backlog).
/// Counts are aggregated live off the delivery-attribution indexes, so they can
/// never drift the way a maintained counter would.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_flexiq_internal_NativeQueue_topicBacklogStats<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jstring {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let stats = queue.storage.topic_backlog_stats()?;
        let views: Vec<TopicStatsView> = stats.iter().map(TopicStatsView::from).collect();
        new_string(env, to_json(&views)?)
    })
}

/// `long reapEphemeralSubscriptions(long handle)` — drop ephemeral subscriptions
/// whose owning worker is gone; returns the count removed.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_flexiq_internal_NativeQueue_reapEphemeralSubscriptions(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
) -> jlong {
    guard(&mut env, 0, |_env| {
        let queue = unsafe { borrow_queue(handle) };
        Ok(backend::reap_ephemeral_subscriptions(&queue.storage)? as jlong)
    })
}

/// `String publish(long handle, String topic, byte[] payload, String optionsJson)`
/// — fan a payload out to every active subscription of the topic. Returns the
/// created jobs as a JSON array — empty when nothing is subscribed.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_flexiq_internal_NativeQueue_publish<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    topic: JString<'local>,
    payload: JByteArray<'local>,
    options_json: JString<'local>,
) -> jstring {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let topic = read_string(env, &topic)?;
        let payload = read_bytes(env, &payload)?;
        let raw_opts = read_string(env, &options_json)?;
        let options: PublishOptions = parse_json(&raw_opts, "publish options")?;
        let request = build_publish_request(topic, payload, options, queue.namespace.as_deref());
        let jobs = publish_to_topic(&queue.storage, &request)?;
        let views: Vec<JobView> = jobs.iter().map(JobView::from).collect();
        new_string(env, to_json(&views)?)
    })
}

/// `String readTopicMessages(long handle, String topic, String subscriptionName,
/// long limit)` — pull up to `limit` messages after a log subscription's cursor,
/// oldest first and exclusive of it, as a JSON array of `TopicMessageView`.
/// At-least-once: reading without acking re-delivers.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_flexiq_internal_NativeQueue_readTopicMessages<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    topic: JString<'local>,
    subscription_name: JString<'local>,
    limit: jlong,
) -> jstring {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let topic = read_string(env, &topic)?;
        let subscription_name = read_string(env, &subscription_name)?;
        let messages = queue
            .storage
            .read_topic_messages(&topic, &subscription_name, limit)?;
        let views: Vec<TopicMessageView> = messages.iter().map(TopicMessageView::from).collect();
        new_string(env, to_json(&views)?)
    })
}

/// `boolean ackTopicCursor(long handle, String topic, String subscriptionName,
/// String cursor)` — advance the subscription's cursor to `cursor` (a message
/// id). A monotonic high-water mark: acking an id acks everything up to it, and
/// acking an older id is a no-op. Returns false when nothing moved.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_flexiq_internal_NativeQueue_ackTopicCursor<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    topic: JString<'local>,
    subscription_name: JString<'local>,
    cursor: JString<'local>,
) -> jboolean {
    guard(&mut env, JNI_FALSE, |env| {
        let queue = unsafe { borrow_queue(handle) };
        let topic = read_string(env, &topic)?;
        let subscription_name = read_string(env, &subscription_name)?;
        let cursor = read_string(env, &cursor)?;
        Ok(to_jboolean(queue.storage.ack_topic_cursor(
            &topic,
            &subscription_name,
            &cursor,
        )?))
    })
}

/// `String leaseTopicMessages(long handle, String topic, String subscriptionName,
/// long limit, long visibilityMs)` — lease up to `limit` available messages for
/// `visibilityMs`, tracking per-message state, as a JSON array of `TopicMessageView`.
/// A nack or an expired lease redelivers just that message. `now` is taken here so
/// the SDK never passes a clock.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_flexiq_internal_NativeQueue_leaseTopicMessages<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    topic: JString<'local>,
    subscription_name: JString<'local>,
    limit: jlong,
    visibility_ms: jlong,
) -> jstring {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let topic = read_string(env, &topic)?;
        let subscription_name = read_string(env, &subscription_name)?;
        let messages = queue.storage.lease_topic_messages(
            &topic,
            &subscription_name,
            limit,
            visibility_ms,
            now_millis(),
        )?;
        let views: Vec<TopicMessageView> = messages.iter().map(TopicMessageView::from).collect();
        new_string(env, to_json(&views)?)
    })
}

/// `boolean ackMessage(long handle, String topic, String subscriptionName,
/// String messageId)` — ack one leased message; it is done and never redelivered.
/// Returns false when there was no un-acked delivery to ack.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_flexiq_internal_NativeQueue_ackMessage<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    topic: JString<'local>,
    subscription_name: JString<'local>,
    message_id: JString<'local>,
) -> jboolean {
    guard(&mut env, JNI_FALSE, |env| {
        let queue = unsafe { borrow_queue(handle) };
        let topic = read_string(env, &topic)?;
        let subscription_name = read_string(env, &subscription_name)?;
        let message_id = read_string(env, &message_id)?;
        Ok(to_jboolean(queue.storage.ack_message(
            &topic,
            &subscription_name,
            &message_id,
        )?))
    })
}

/// `boolean nackMessage(long handle, String topic, String subscriptionName,
/// String messageId)` — nack one leased message; make it available for redelivery
/// now. Returns false when there was no un-acked delivery to nack.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_flexiq_internal_NativeQueue_nackMessage<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    topic: JString<'local>,
    subscription_name: JString<'local>,
    message_id: JString<'local>,
) -> jboolean {
    guard(&mut env, JNI_FALSE, |env| {
        let queue = unsafe { borrow_queue(handle) };
        let topic = read_string(env, &topic)?;
        let subscription_name = read_string(env, &subscription_name)?;
        let message_id = read_string(env, &message_id)?;
        Ok(to_jboolean(queue.storage.nack_message(
            &topic,
            &subscription_name,
            &message_id,
        )?))
    })
}

/// `String topicLogStats(long handle)` — a JSON array of `TopicLogStatsView`, one
/// lag snapshot per log subscription.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_flexiq_internal_NativeQueue_topicLogStats<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jstring {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let stats = queue.storage.topic_log_stats()?;
        let views: Vec<TopicLogStatsView> = stats.iter().map(TopicLogStatsView::from).collect();
        new_string(env, to_json(&views)?)
    })
}

/// `void declareTopic(long handle, String name, long retentionMs)` — declare a
/// log topic (idempotent) so its publishes are retained even with no subscriber.
/// `retentionMs` bounds a sub-less backlog; `i64::MIN` is the "unbounded → keep
/// until consumed" sentinel, since JNI carries a primitive `long`, not a boxed
/// nullable. Mode is always `"log"` — the only declarable mode today.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_flexiq_internal_NativeQueue_declareTopic<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    name: JString<'local>,
    retention_ms: jlong,
) {
    guard(&mut env, (), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let name = read_string(env, &name)?;
        let retention = (retention_ms != jlong::MIN).then_some(retention_ms);
        queue
            .storage
            .declare_topic(&name, SubscriptionMode::Log, retention)?;
        Ok(())
    })
}

/// `String listDeclaredTopics(long handle)` — a JSON array of declared topics
/// (`TopicView`).
#[no_mangle]
pub extern "system" fn Java_org_byteveda_flexiq_internal_NativeQueue_listDeclaredTopics<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jstring {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let topics = queue.storage.list_declared_topics()?;
        let views: Vec<TopicView> = topics.iter().map(TopicView::from).collect();
        new_string(env, to_json(&views)?)
    })
}
