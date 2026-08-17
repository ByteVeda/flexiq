use std::collections::HashSet;

use redis::streams::{StreamId, StreamRangeReply};
use redis::Commands;
use serde::{Deserialize, Serialize};

use super::{map_err, RedisStorage};
use crate::error::{QueueError, Result};
use crate::job::{Job, JobStatus};
use crate::storage::records::{
    NewSubscription, Subscription, SubscriptionMode, Topic, TopicLogStats, TopicMessage,
};
use crate::storage::SubscriptionBacklogStats;

/// JSON blob stored at `sub:<topic>:<subscription_name>`. Mirrors
/// [`Subscription`] so the CRUD path needs no Lua — index sets alone keep
/// lookups and the reaper off a keyspace `SCAN`.
#[derive(Serialize, Deserialize)]
struct SubEntry {
    topic: String,
    subscription_name: String,
    task_name: String,
    queue: String,
    active: bool,
    durable: bool,
    owner_worker_id: Option<String>,
    created_at: i64,
    // Older blobs (registered before per-subscription settings) lack these;
    // default to None so they resolve to queue defaults, matching prior behavior.
    #[serde(default)]
    priority: Option<i32>,
    #[serde(default)]
    max_retries: Option<i32>,
    #[serde(default)]
    timeout_ms: Option<i64>,
    // S28 log topics. Older blobs default to fan-out with no cursor.
    #[serde(default = "default_mode")]
    mode: String,
    #[serde(default)]
    cursor: Option<String>,
}

fn default_mode() -> String {
    SubscriptionMode::Fanout.as_str().to_string()
}

/// JSON blob stored in the `topics` hash, field = topic name (so the name is
/// not duplicated in the value). Mirrors the declarable columns of [`Topic`].
#[derive(Serialize, Deserialize)]
struct TopicEntry {
    mode: String,
    retention_ms: Option<i64>,
    created_at: i64,
}

impl TopicEntry {
    fn into_topic(self, name: String) -> Topic {
        Topic {
            name,
            mode: SubscriptionMode::from_wire(&self.mode),
            retention_ms: self.retention_ms,
            created_at: self.created_at,
        }
    }
}

impl From<SubEntry> for Subscription {
    fn from(e: SubEntry) -> Self {
        Self {
            topic: e.topic,
            subscription_name: e.subscription_name,
            task_name: e.task_name,
            queue: e.queue,
            active: e.active,
            durable: e.durable,
            owner_worker_id: e.owner_worker_id,
            created_at: e.created_at,
            priority: e.priority,
            max_retries: e.max_retries,
            timeout_ms: e.timeout_ms,
            mode: SubscriptionMode::from_wire(&e.mode),
            cursor: e.cursor,
        }
    }
}

impl RedisStorage {
    /// The `topic|subscription_name` member string used in the index sets.
    fn sub_composite(topic: &str, subscription_name: &str) -> String {
        format!("{topic}|{subscription_name}")
    }

    /// Load the entry a composite `topic|name` member points at, or `None` when
    /// the blob is gone (a stale index member). Splits on the first `|`, matching
    /// how [`Self::sub_composite`] builds it.
    fn load_sub_by_composite(
        &self,
        conn: &mut redis::Connection,
        composite: &str,
    ) -> Result<Option<SubEntry>> {
        let Some((topic, name)) = composite.split_once('|') else {
            return Ok(None);
        };
        let blob_key = self.key(&["sub", topic, name]);
        let data: Option<String> = conn.get(&blob_key).map_err(map_err)?;
        match data {
            Some(d) => {
                let entry: SubEntry = serde_json::from_str(&d)?;
                Ok(Some(entry))
            }
            None => Ok(None),
        }
    }

    /// Pipeline-load the entries for many `(topic, name)` pairs in a single
    /// round trip. Stale index members whose blob is gone are dropped (their
    /// `GET` returns nil). Replaces the per-member `GET` loop that made every
    /// list/reap/stats call O(N) round trips against Redis.
    fn load_subs_pipelined(
        &self,
        conn: &mut redis::Connection,
        pairs: &[(String, String)],
    ) -> Result<Vec<SubEntry>> {
        if pairs.is_empty() {
            return Ok(Vec::new());
        }
        let mut pipe = redis::pipe();
        for (topic, name) in pairs {
            pipe.get(self.key(&["sub", topic, name]));
        }
        let blobs: Vec<Option<String>> = pipe.query(conn).map_err(map_err)?;
        blobs
            .into_iter()
            .flatten()
            .map(|d| serde_json::from_str(&d).map_err(QueueError::from))
            .collect()
    }

    /// Insert or update a subscription. Idempotent on (topic, subscription_name).
    ///
    /// Maintains three index sets — `subs:by_topic:<topic>`, `subs:all`, and
    /// `subs:by_owner:<worker_id>` (ephemeral rows only). When a re-registration
    /// changes the owner (including clearing it to durable), the stale
    /// `by_owner` membership is dropped so the reaper's view stays exact.
    pub fn register_subscription(&self, sub: &NewSubscription) -> Result<()> {
        let mut conn = self.conn()?;

        let entry = SubEntry {
            topic: sub.topic.clone(),
            subscription_name: sub.subscription_name.clone(),
            task_name: sub.task_name.clone(),
            queue: sub.queue.clone(),
            active: sub.active,
            durable: sub.durable,
            owner_worker_id: sub.owner_worker_id.clone(),
            created_at: sub.created_at,
            priority: sub.priority,
            max_retries: sub.max_retries,
            timeout_ms: sub.timeout_ms,
            mode: sub.mode.as_str().to_string(),
            cursor: None,
        };
        let blob_key = self.key(&["sub", &sub.topic, &sub.subscription_name]);
        let by_topic = self.key(&["subs", "by_topic", &sub.topic]);
        let all = self.key(&["subs", "all"]);
        let composite = Self::sub_composite(&sub.topic, &sub.subscription_name);

        // A prior row may have carried a different (or NULL) owner; drop its
        // stale by_owner entry so ownership never lingers across re-registration.
        // Re-declaring also must not resume a paused subscription or reset its
        // registration time, so both survive from the prior entry.
        let prior = self.load_sub_by_composite(&mut conn, &composite)?;
        let prior_owner = prior.as_ref().and_then(|e| e.owner_worker_id.clone());
        let mut entry = entry;
        if let Some(prior) = &prior {
            entry.active = prior.active;
            entry.created_at = prior.created_at;
            entry.cursor = prior.cursor.clone();
        }
        let json = serde_json::to_string(&entry)?;

        let pipe = redis::pipe().atomic().to_owned();
        let mut pipe = pipe;
        pipe.set(&blob_key, &json);
        pipe.sadd(&by_topic, &sub.subscription_name);
        pipe.sadd(&all, &composite);
        if let Some(prev) = prior_owner.as_deref() {
            if Some(prev) != sub.owner_worker_id.as_deref() {
                pipe.srem(self.key(&["subs", "by_owner", prev]), &composite);
            }
        }
        if let Some(owner) = sub.owner_worker_id.as_deref() {
            pipe.sadd(self.key(&["subs", "by_owner", owner]), &composite);
        }
        pipe.query::<()>(&mut conn).map_err(map_err)?;

        Ok(())
    }

    /// Active subscriptions for a topic, in registration order (`created_at`
    /// then `subscription_name`, matching the Diesel backends).
    pub fn list_subscriptions_for_topic(&self, topic: &str) -> Result<Vec<Subscription>> {
        let mut conn = self.conn()?;
        let by_topic = self.key(&["subs", "by_topic", topic]);

        let names: Vec<String> = conn.smembers(&by_topic).map_err(map_err)?;
        let pairs: Vec<(String, String)> = names
            .into_iter()
            .map(|name| (topic.to_string(), name))
            .collect();

        let mut rows: Vec<Subscription> = self
            .load_subs_pipelined(&mut conn, &pairs)?
            .into_iter()
            .filter(|entry| entry.active)
            .map(Subscription::from)
            .collect();

        rows.sort_by(|a, b| {
            a.created_at
                .cmp(&b.created_at)
                .then_with(|| a.subscription_name.cmp(&b.subscription_name))
        });

        Ok(rows)
    }

    /// Every registered subscription, active or paused, across all topics.
    pub fn list_subscriptions(&self) -> Result<Vec<Subscription>> {
        let mut conn = self.conn()?;
        let all = self.key(&["subs", "all"]);

        let composites: Vec<String> = conn.smembers(&all).map_err(map_err)?;
        let pairs = Self::composites_to_pairs(&composites);

        let rows = self
            .load_subs_pipelined(&mut conn, &pairs)?
            .into_iter()
            .map(Subscription::from)
            .collect();

        Ok(rows)
    }

    /// Split `topic|name` composite index members into `(topic, name)` pairs,
    /// dropping any malformed member (matching `load_sub_by_composite`).
    fn composites_to_pairs(composites: &[String]) -> Vec<(String, String)> {
        composites
            .iter()
            .filter_map(|c| {
                c.split_once('|')
                    .map(|(t, n)| (t.to_string(), n.to_string()))
            })
            .collect()
    }

    /// Remove a subscription. Returns false if none matched.
    pub fn unsubscribe(&self, topic: &str, subscription_name: &str) -> Result<bool> {
        let mut conn = self.conn()?;
        let blob_key = self.key(&["sub", topic, subscription_name]);

        // Read the row first so its owner index entry can be dropped too.
        let data: Option<String> = conn.get(&blob_key).map_err(map_err)?;
        let Some(d) = data else {
            return Ok(false);
        };
        let entry: SubEntry = serde_json::from_str(&d)?;

        let by_topic = self.key(&["subs", "by_topic", topic]);
        let all = self.key(&["subs", "all"]);
        let composite = Self::sub_composite(topic, subscription_name);

        let mut pipe = redis::pipe().atomic().to_owned();
        pipe.del(&blob_key);
        pipe.srem(&by_topic, subscription_name);
        pipe.srem(&all, &composite);
        if let Some(owner) = entry.owner_worker_id.as_deref() {
            pipe.srem(self.key(&["subs", "by_owner", owner]), &composite);
        }
        pipe.query::<()>(&mut conn).map_err(map_err)?;

        Ok(true)
    }

    /// Pause (false) or resume (true) a subscription without removing its
    /// registration. Returns false if none matched.
    pub fn set_subscription_active(
        &self,
        topic: &str,
        subscription_name: &str,
        active: bool,
    ) -> Result<bool> {
        let mut conn = self.conn()?;
        let blob_key = self.key(&["sub", topic, subscription_name]);

        let data: Option<String> = conn.get(&blob_key).map_err(map_err)?;
        let Some(d) = data else {
            return Ok(false);
        };

        let mut entry: SubEntry = serde_json::from_str(&d)?;
        entry.active = active;

        let json = serde_json::to_string(&entry)?;
        conn.set::<_, _, ()>(&blob_key, &json).map_err(map_err)?;

        Ok(true)
    }

    /// Remove ephemeral subscriptions whose owner is not in `live_worker_ids`.
    /// Durable rows (owner NULL) are never touched, and rows younger than the
    /// registration grace window survive (a starting worker inserts its
    /// ephemeral subscriptions before its first heartbeat lands).
    /// Returns the count removed.
    ///
    /// Iterates the `subs:all` index set rather than a keyspace `SCAN`; each
    /// removed row is also pulled from its `by_topic` and `by_owner` sets.
    pub fn reap_ephemeral_subscriptions(&self, live_worker_ids: &[String]) -> Result<u64> {
        let mut conn = self.conn()?;
        let all = self.key(&["subs", "all"]);
        let cutoff = crate::job::now_millis() - crate::storage::EPHEMERAL_SUBSCRIPTION_GRACE_MS;

        let composites: Vec<String> = conn.smembers(&all).map_err(map_err)?;
        let pairs = Self::composites_to_pairs(&composites);
        let entries = self.load_subs_pipelined(&mut conn, &pairs)?;
        let live: HashSet<&str> = live_worker_ids.iter().map(String::as_str).collect();

        // Decide in memory, then remove every doomed row in one atomic pipe
        // rather than a MULTI/EXEC round trip per row.
        let mut pipe = redis::pipe().atomic().to_owned();
        let mut removed = 0u64;
        for entry in &entries {
            // Durable rows (owner NULL) and live-owned rows survive.
            let Some(owner) = entry.owner_worker_id.as_deref() else {
                continue;
            };
            if live.contains(owner) || entry.created_at >= cutoff {
                continue;
            }

            let composite = Self::sub_composite(&entry.topic, &entry.subscription_name);
            pipe.del(self.key(&["sub", &entry.topic, &entry.subscription_name]));
            pipe.srem(
                self.key(&["subs", "by_topic", &entry.topic]),
                &entry.subscription_name,
            );
            pipe.srem(&all, &composite);
            pipe.srem(self.key(&["subs", "by_owner", owner]), &composite);
            removed += 1;
        }

        if removed > 0 {
            pipe.query::<()>(&mut conn).map_err(map_err)?;
        }

        Ok(removed)
    }

    // ── Per-subscription backlog/lag indices ───────────────────────────────
    //
    // Three index keys per subscription mirror the live delivery backlog so
    // `topic_backlog_stats` costs O(subscriptions), never a job scan:
    //   sub:pending:<topic>:<name> — ZSET of job ids scored by `created_at` (ms)
    //   sub:running:<topic>:<name> — SET of job ids
    //   sub:dead:<topic>:<name>    — SET of (original) job ids
    // Every write is gated on `extract_topic_subscription`, so an ordinary job
    // never touches them: the whole feature stays zero-cost off the pub/sub path.

    /// Key for a subscription's per-status backlog index. `kind` is
    /// `"pending"`, `"running"`, or `"dead"`.
    fn sub_index_key(&self, kind: &str, topic: &str, subscription_name: &str) -> String {
        self.key(&["sub", kind, topic, subscription_name])
    }

    /// Append the backlog-index ops for `job` landing in status `to` onto
    /// `pipe`, if (and only if) `job` is a pub/sub delivery. Keyed on the
    /// *destination* status alone: every transition additionally scrubs the job
    /// from the indices it must not be in, so the indices self-heal any drift a
    /// best-effort claim reindex (see `reindex_pubsub_best_effort`) may leave,
    /// on the very next state change. All ops are `.ignore()`d so the helper is
    /// safe to fold into either a plain or a MULTI/EXEC pipeline without
    /// disturbing the caller's result type.
    pub(in crate::storage::redis_backend) fn push_pubsub_transition(
        &self,
        pipe: &mut redis::Pipeline,
        job: &Job,
        to: JobStatus,
    ) {
        let Some((topic, name)) = crate::pubsub::extract_topic_subscription(job.notes.as_deref())
        else {
            return;
        };
        let pending = self.sub_index_key("pending", &topic, &name);
        let running = self.sub_index_key("running", &topic, &name);
        let dead = self.sub_index_key("dead", &topic, &name);
        match to {
            JobStatus::Pending => {
                pipe.srem(&running, &job.id).ignore();
                pipe.zadd(&pending, &job.id, job.created_at as f64).ignore();
            }
            JobStatus::Running => {
                pipe.zrem(&pending, &job.id).ignore();
                pipe.sadd(&running, &job.id).ignore();
            }
            JobStatus::Dead => {
                pipe.zrem(&pending, &job.id).ignore();
                pipe.srem(&running, &job.id).ignore();
                pipe.sadd(&dead, &job.id).ignore();
            }
            // Complete / Failed / Cancelled: the delivery leaves the backlog.
            _ => {
                pipe.zrem(&pending, &job.id).ignore();
                pipe.srem(&running, &job.id).ignore();
            }
        }
    }

    /// Append a `sub:dead` removal onto `pipe` for a dead-letter row whose
    /// attribution comes from its persisted `notes` (retry/purge/delete paths
    /// hold a `DeadJobEntry`, not a live `Job`). `member` is the original job id
    /// — the value `move_to_dlq` added via [`Self::push_pubsub_transition`].
    /// No-op for non-pub/sub rows. Keeps the `dead` count from drifting upward
    /// after a dead-letter entry is retried or purged.
    pub(in crate::storage::redis_backend) fn push_pubsub_dead_remove(
        &self,
        pipe: &mut redis::Pipeline,
        notes: Option<&str>,
        member: &str,
    ) {
        if let Some((topic, name)) = crate::pubsub::extract_topic_subscription(notes) {
            pipe.srem(self.sub_index_key("dead", &topic, &name), member)
                .ignore();
        }
    }

    /// Best-effort backlog reindex for a status change committed by a bespoke,
    /// correctness-critical Lua script we deliberately keep minimal (the
    /// Pending→Running claim and the requeue-stuck rescue). The index move runs
    /// in a follow-up pipe: a crash between the script and this pipe can leave a
    /// delivery in the wrong backlog set, but that only skews a dashboard metric
    /// — never job state — and the next transition scrubs it. A Redis error is
    /// logged and swallowed so it cannot fail an operation that already
    /// succeeded. No-op (and no round trip) for ordinary jobs.
    pub(in crate::storage::redis_backend) fn reindex_pubsub_best_effort(
        &self,
        conn: &mut redis::Connection,
        job: &Job,
        to: JobStatus,
    ) {
        if crate::pubsub::extract_topic_subscription(job.notes.as_deref()).is_none() {
            return;
        }
        let mut pipe = redis::pipe();
        self.push_pubsub_transition(&mut pipe, job, to);
        if let Err(e) = pipe.query::<()>(conn) {
            log::warn!("pub/sub backlog reindex failed (metrics only): {e}");
        }
    }

    /// Backlog/lag snapshot per registered subscription. One bounded round trip
    /// per subscription (never a job scan): the three index cardinalities plus
    /// the oldest pending score. `oldest_pending_age_ms` is `now - score`
    /// floored at 0, or `None` when the subscription has no pending backlog.
    /// Mirrors the Diesel backends' shape; counts are a direct read of the live
    /// indices, so they cannot drift the way a maintained counter would.
    pub fn topic_backlog_stats(&self) -> Result<Vec<SubscriptionBacklogStats>> {
        let subs = self.list_subscriptions()?;
        if subs.is_empty() {
            return Ok(Vec::new());
        }
        let mut conn = self.conn()?;
        let now = crate::job::now_millis();

        // Batch every subscription's index reads instead of one MULTI/EXEC per
        // subscription. Two homogeneous pipes keep the reply types clean: the
        // three cardinalities (i64) in one, the oldest-pending score (a
        // heterogeneous ZRANGE reply) in another. Both are O(1) round trips.
        let mut counts_pipe = redis::pipe();
        let mut oldest_pipe = redis::pipe();
        for sub in &subs {
            let pending_key = self.sub_index_key("pending", &sub.topic, &sub.subscription_name);
            let running_key = self.sub_index_key("running", &sub.topic, &sub.subscription_name);
            let dead_key = self.sub_index_key("dead", &sub.topic, &sub.subscription_name);
            counts_pipe
                .zcard(&pending_key)
                .scard(&running_key)
                .scard(&dead_key);
            // ZRANGE 0 0 WITHSCORES yields at most the single oldest pending job.
            oldest_pipe.zrange_withscores(&pending_key, 0, 0);
        }
        let counts: Vec<i64> = counts_pipe.query(&mut conn).map_err(map_err)?;
        let oldest: Vec<Vec<(String, f64)>> = oldest_pipe.query(&mut conn).map_err(map_err)?;

        let out = subs
            .into_iter()
            .enumerate()
            .map(|(i, sub)| {
                let oldest_pending_age_ms = oldest
                    .get(i)
                    .and_then(|v| v.first())
                    .map(|(_, score)| (now - *score as i64).max(0));
                SubscriptionBacklogStats {
                    topic: sub.topic,
                    subscription_name: sub.subscription_name,
                    task_name: sub.task_name,
                    queue: sub.queue,
                    active: sub.active,
                    durable: sub.durable,
                    pending: counts[i * 3],
                    running: counts[i * 3 + 1],
                    dead: counts[i * 3 + 2],
                    oldest_pending_age_ms,
                }
            })
            .collect();
        Ok(out)
    }

    // ── Log topics (Redis Streams) ──────────────────────────────────
    //
    // A log topic is a stream at `topic:<topic>`; a publish is one `XADD` and
    // the returned stream id is the message id / cursor token. Per-subscription
    // cursors live in the `SubEntry` blob. This is the Redis fork of the Diesel
    // `topic_messages` table (see `diesel_common/pubsub.rs`).

    /// Stream key for a topic's log.
    fn topic_stream_key(&self, topic: &str) -> String {
        self.key(&["topic", topic])
    }

    /// Parse a `<ms>-<seq>` stream id into comparable parts (invalid → `(0, 0)`).
    fn stream_id_parts(id: &str) -> (u64, u64) {
        let mut parts = id.splitn(2, '-');
        let ms = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0);
        let seq = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0);
        (ms, seq)
    }

    /// The next stream id after `id`, used as an exclusive `XTRIM MINID` floor.
    fn next_stream_id(id: &str) -> String {
        let (ms, seq) = Self::stream_id_parts(id);
        match seq.checked_add(1) {
            Some(next) => format!("{ms}-{next}"),
            None => format!("{}-0", ms + 1),
        }
    }

    /// Build a [`TopicMessage`] from a stream entry.
    fn stream_id_to_message(topic: &str, entry: StreamId) -> TopicMessage {
        TopicMessage {
            id: entry.id.clone(),
            topic: topic.to_string(),
            payload: entry.get::<Vec<u8>>("payload").unwrap_or_default(),
            metadata: entry.get::<String>("metadata"),
            notes: entry.get::<String>("notes"),
            created_at: entry.get::<i64>("created_at").unwrap_or_default(),
            expires_at: entry.get::<i64>("expires_at"),
        }
    }

    /// Append one message to a log topic (`XADD`); the stream id is the message
    /// id. O(1), independent of subscriber count.
    pub fn publish_message(
        &self,
        topic: &str,
        payload: &[u8],
        metadata: Option<&str>,
        notes: Option<&str>,
        expires_at: Option<i64>,
    ) -> Result<TopicMessage> {
        let mut conn = self.conn()?;
        let created_at = crate::job::now_millis();
        let stream_key = self.topic_stream_key(topic);

        let mut cmd = redis::cmd("XADD");
        cmd.arg(&stream_key).arg("*");
        cmd.arg("payload").arg(payload);
        cmd.arg("created_at").arg(created_at);
        if let Some(m) = metadata {
            cmd.arg("metadata").arg(m);
        }
        if let Some(n) = notes {
            cmd.arg("notes").arg(n);
        }
        if let Some(e) = expires_at {
            cmd.arg("expires_at").arg(e);
        }
        let id: String = cmd.query(&mut conn).map_err(map_err)?;

        Ok(TopicMessage {
            id,
            topic: topic.to_string(),
            payload: payload.to_vec(),
            metadata: metadata.map(str::to_string),
            notes: notes.map(str::to_string),
            created_at,
            expires_at,
        })
    }

    /// Messages after a log subscription's cursor, oldest first, `<= limit`.
    /// Cursor resolved from the `SubEntry`; `XRANGE (cursor +` is exclusive.
    pub fn read_topic_messages(
        &self,
        topic: &str,
        subscription_name: &str,
        limit: i64,
    ) -> Result<Vec<TopicMessage>> {
        if limit <= 0 {
            return Ok(Vec::new());
        }
        let mut conn = self.conn()?;
        let blob_key = self.key(&["sub", topic, subscription_name]);
        let Some(data): Option<String> = conn.get(&blob_key).map_err(map_err)? else {
            return Ok(Vec::new());
        };
        let entry: SubEntry = serde_json::from_str(&data)?;
        // A fan-out sub must never read a mixed topic's log.
        if !SubscriptionMode::from_wire(&entry.mode).is_log() {
            return Ok(Vec::new());
        }

        let start = match &entry.cursor {
            Some(cursor) => format!("({cursor}"),
            None => "-".to_string(),
        };
        let reply: StreamRangeReply = redis::cmd("XRANGE")
            .arg(self.topic_stream_key(topic))
            .arg(&start)
            .arg("+")
            .arg("COUNT")
            .arg(limit)
            .query(&mut conn)
            .map_err(map_err)?;

        Ok(reply
            .ids
            .into_iter()
            .map(|entry| Self::stream_id_to_message(topic, entry))
            .collect())
    }

    /// Advance a log subscription's cursor. Monotonic (never rewinds by stream-id
    /// order) and idempotent. Read-modify-write on the `SubEntry` blob — a single
    /// puller per subscription is assumed, matching the pull-consumer model.
    pub fn ack_topic_cursor(
        &self,
        topic: &str,
        subscription_name: &str,
        cursor: &str,
    ) -> Result<bool> {
        let mut conn = self.conn()?;
        let blob_key = self.key(&["sub", topic, subscription_name]);
        let Some(data): Option<String> = conn.get(&blob_key).map_err(map_err)? else {
            return Ok(false);
        };
        let mut entry: SubEntry = serde_json::from_str(&data)?;
        // Only a log subscription has a cursor to advance.
        if !SubscriptionMode::from_wire(&entry.mode).is_log() {
            return Ok(false);
        }

        let advance = match &entry.cursor {
            None => true,
            Some(current) => Self::stream_id_parts(cursor) > Self::stream_id_parts(current),
        };
        if !advance {
            return Ok(false);
        }
        entry.cursor = Some(cursor.to_string());
        let json = serde_json::to_string(&entry)?;
        conn.set::<_, _, ()>(&blob_key, json).map_err(map_err)?;
        Ok(true)
    }

    /// Per-log-subscription lag + oldest un-acked age. One `XRANGE (cursor +` per
    /// log subscription (O(unacked)); fan-out subscriptions are excluded.
    pub fn topic_log_stats(&self) -> Result<Vec<TopicLogStats>> {
        let subs = self.list_subscriptions()?;
        let mut conn = self.conn()?;
        let now = crate::job::now_millis();

        let mut out = Vec::new();
        for sub in subs.into_iter().filter(|s| s.mode.is_log()) {
            let start = match &sub.cursor {
                Some(cursor) => format!("({cursor}"),
                None => "-".to_string(),
            };
            let reply: StreamRangeReply = redis::cmd("XRANGE")
                .arg(self.topic_stream_key(&sub.topic))
                .arg(&start)
                .arg("+")
                .query(&mut conn)
                .map_err(map_err)?;
            let oldest = reply
                .ids
                .first()
                .and_then(|entry| entry.get::<i64>("created_at"))
                .map(|created| (now - created).max(0));
            out.push(TopicLogStats {
                topic: sub.topic,
                subscription_name: sub.subscription_name,
                cursor: sub.cursor,
                lag: reply.ids.len() as i64,
                oldest_unacked_age_ms: oldest,
            });
        }
        Ok(out)
    }

    /// Compact each log topic: `XTRIM MINID` to the min cursor across its subs,
    /// then `XDEL` any entry past `expires_at` (the TTL safety net, so a NULL or
    /// stalled cursor can't block reclamation), then — on a topic consumed purely
    /// per-message — `XDEL` any entry every per-message subscriber has acked.
    /// Matches the Diesel backend. The scans are bounded by `limit` per topic.
    pub fn purge_topic_messages(&self, now: i64, limit: i64) -> Result<u64> {
        use std::collections::HashMap;
        // Group each topic's log subs so cursor floor and per-message compaction
        // both see the full set (a mixed cursor+per-message topic must be spotted).
        let mut by_topic: HashMap<String, Vec<Subscription>> = HashMap::new();
        for sub in self
            .list_subscriptions()?
            .into_iter()
            .filter(|s| s.mode.is_log())
        {
            by_topic.entry(sub.topic.clone()).or_default().push(sub);
        }

        let mut conn = self.conn()?;
        let mut removed = 0u64;
        for (topic, subs) in &by_topic {
            let stream_key = self.topic_stream_key(topic);

            // Min cursor across the topic's subs; `None` once any read nothing.
            let mut floor: Option<String> = None;
            let mut floor_poisoned = false;
            for sub in subs {
                match &sub.cursor {
                    None => {
                        floor_poisoned = true;
                        break;
                    }
                    Some(cursor) => {
                        let take = floor.as_ref().is_none_or(|cur| {
                            Self::stream_id_parts(cursor) < Self::stream_id_parts(cur)
                        });
                        if take {
                            floor = Some(cursor.clone());
                        }
                    }
                }
            }
            if let Some(floor) = floor.filter(|_| !floor_poisoned) {
                let trimmed: u64 = redis::cmd("XTRIM")
                    .arg(&stream_key)
                    .arg("MINID")
                    .arg(Self::next_stream_id(&floor))
                    .query(&mut conn)
                    .map_err(map_err)?;
                removed += trimmed;
            }
            removed += self.xdel_expired(&mut conn, &stream_key, now, limit)?;
            removed += self.xdel_per_message_acked(&mut conn, topic, subs, limit)?;
        }
        Ok(removed)
    }

    /// On a topic consumed purely per-message (every log sub has a delivery hash),
    /// `XDEL` up to `limit` entries every per-message subscriber has acked, and
    /// drop their delivery fields so per-message state can't outlive the log. A
    /// topic mixing in a cursor reader (a log sub with no deliveries) is left to
    /// the cursor/expiry paths, matching the Diesel backend.
    fn xdel_per_message_acked(
        &self,
        conn: &mut redis::Connection,
        topic: &str,
        subs: &[Subscription],
        limit: i64,
    ) -> Result<u64> {
        use std::collections::HashMap;
        // Delivery state per sub; a sub with an empty hash is a cursor reader.
        let mut deliveries: Vec<(String, HashMap<String, String>)> = Vec::with_capacity(subs.len());
        for sub in subs {
            let key = self.pm_deliveries_key(topic, &sub.subscription_name);
            let map: HashMap<String, String> = conn.hgetall(&key).map_err(map_err)?;
            deliveries.push((key, map));
        }
        // Purely per-message only: skip if any log sub has no delivery hash.
        if deliveries.iter().any(|(_, map)| map.is_empty()) {
            return Ok(0);
        }

        let reply: StreamRangeReply = redis::cmd("XRANGE")
            .arg(self.topic_stream_key(topic))
            .arg("-")
            .arg("+")
            .arg("COUNT")
            .arg(limit)
            .query(conn)
            .map_err(map_err)?;
        let acked: Vec<String> = reply
            .ids
            .into_iter()
            .filter(|entry| {
                deliveries
                    .iter()
                    .all(|(_, map)| map.get(&entry.id).is_some_and(|s| s == "acked"))
            })
            .map(|entry| entry.id)
            .collect();
        if acked.is_empty() {
            return Ok(0);
        }

        let deleted: u64 = redis::cmd("XDEL")
            .arg(self.topic_stream_key(topic))
            .arg(&acked)
            .query(conn)
            .map_err(map_err)?;
        for (key, _) in &deliveries {
            conn.hdel::<_, _, ()>(key, &acked).map_err(map_err)?;
        }
        Ok(deleted)
    }

    /// `XDEL` up to `limit` entries in `stream_key` whose `expires_at` field is
    /// at or before `now`, regardless of any subscription cursor.
    fn xdel_expired(
        &self,
        conn: &mut redis::Connection,
        stream_key: &str,
        now: i64,
        limit: i64,
    ) -> Result<u64> {
        let reply: StreamRangeReply = redis::cmd("XRANGE")
            .arg(stream_key)
            .arg("-")
            .arg("+")
            .arg("COUNT")
            .arg(limit)
            .query(conn)
            .map_err(map_err)?;
        let expired: Vec<String> = reply
            .ids
            .into_iter()
            .filter(|entry| entry.get::<i64>("expires_at").is_some_and(|at| at <= now))
            .map(|entry| entry.id)
            .collect();
        if expired.is_empty() {
            return Ok(0);
        }
        let deleted: u64 = redis::cmd("XDEL")
            .arg(stream_key)
            .arg(&expired)
            .query(conn)
            .map_err(map_err)?;
        Ok(deleted)
    }

    /// Declare a topic (idempotent). Stored as a field in the `topics` hash;
    /// re-declaring preserves the original `created_at`.
    pub fn declare_topic(
        &self,
        name: &str,
        mode: SubscriptionMode,
        retention_ms: Option<i64>,
    ) -> Result<()> {
        crate::pubsub::validate_topic_declaration(mode, retention_ms)?;
        let mut conn = self.conn()?;
        let key = self.key(&["topics"]);
        let created_at = match conn
            .hget::<_, _, Option<String>>(&key, name)
            .map_err(map_err)?
        {
            Some(existing) => serde_json::from_str::<TopicEntry>(&existing)
                .map(|e| e.created_at)
                .unwrap_or_else(|_| crate::job::now_millis()),
            None => crate::job::now_millis(),
        };
        let entry = TopicEntry {
            mode: mode.as_str().to_string(),
            retention_ms,
            created_at,
        };
        conn.hset::<_, _, _, ()>(&key, name, serde_json::to_string(&entry)?)
            .map_err(map_err)?;
        Ok(())
    }

    /// Fetch a declared topic by name, or `None` if never declared.
    pub fn get_topic(&self, name: &str) -> Result<Option<Topic>> {
        let mut conn = self.conn()?;
        let Some(json): Option<String> = conn.hget(self.key(&["topics"]), name).map_err(map_err)?
        else {
            return Ok(None);
        };
        let entry: TopicEntry = serde_json::from_str(&json)?;
        Ok(Some(entry.into_topic(name.to_string())))
    }

    /// Every declared topic in the registry.
    pub fn list_declared_topics(&self) -> Result<Vec<Topic>> {
        let mut conn = self.conn()?;
        let map: std::collections::HashMap<String, String> =
            conn.hgetall(self.key(&["topics"])).map_err(map_err)?;
        map.into_iter()
            .map(|(name, json)| Ok(serde_json::from_str::<TopicEntry>(&json)?.into_topic(name)))
            .collect()
    }

    /// Per-message delivery state for a subscription: a hash at
    /// `pmdeliv:<topic>:<subscription>`, field = message id, value = `"acked"`
    /// or the lease's `expires_at` (`0` = available for redelivery, e.g. nacked).
    /// This mirrors the Diesel `topic_deliveries` table so the semantics match.
    fn pm_deliveries_key(&self, topic: &str, subscription_name: &str) -> String {
        self.key(&["pmdeliv", topic, subscription_name])
    }

    /// Lease up to `limit` available messages to `subscription_name` for
    /// `visibility_ms`. Available = never delivered, or a lease that expired /
    /// was nacked and never acked. See the `lease_topic_messages` trait method.
    /// Requires a registered `log` subscription (like the cursor read). The
    /// stream is scanned in `COUNT`-bounded pages (not loaded whole) until
    /// `limit` are collected or it ends. Delivery is **at-least-once**: the
    /// `HGETALL` availability snapshot and the per-message `HSET` lease aren't a
    /// single atomic step, so two concurrent leasers can briefly claim the same
    /// message — handlers must be idempotent, as with a lease-timeout redeliver.
    pub fn lease_topic_messages(
        &self,
        topic: &str,
        subscription_name: &str,
        limit: i64,
        visibility_ms: i64,
        now: i64,
    ) -> Result<Vec<TopicMessage>> {
        if limit <= 0 {
            return Ok(Vec::new());
        }
        let mut conn = self.conn()?;
        // Only a registered log subscription may lease — a fan-out or unknown
        // name must never claim from the log (mirrors read/ack).
        let blob_key = self.key(&["sub", topic, subscription_name]);
        let Some(data): Option<String> = conn.get(&blob_key).map_err(map_err)? else {
            return Ok(Vec::new());
        };
        if !SubscriptionMode::from_wire(&serde_json::from_str::<SubEntry>(&data)?.mode).is_log() {
            return Ok(Vec::new());
        }

        let deliv_key = self.pm_deliveries_key(topic, subscription_name);
        let deliveries: std::collections::HashMap<String, String> =
            conn.hgetall(&deliv_key).map_err(map_err)?;
        let stream_key = self.topic_stream_key(topic);

        let lease_until = now + visibility_ms;
        let mut out = Vec::new();
        let mut start = "-".to_string();
        while (out.len() as i64) < limit {
            let page: StreamRangeReply = redis::cmd("XRANGE")
                .arg(&stream_key)
                .arg(&start)
                .arg("+")
                .arg("COUNT")
                .arg(limit)
                .query(&mut conn)
                .map_err(map_err)?;
            if page.ids.is_empty() {
                break;
            }
            let exhausted = (page.ids.len() as i64) < limit;
            // Resume the next page exclusively after this page's last id.
            let next_start = page.ids.last().map(|entry| format!("({}", entry.id));
            for entry in page.ids {
                if (out.len() as i64) >= limit {
                    break;
                }
                let available = match deliveries.get(&entry.id) {
                    None => true,
                    Some(state) if state == "acked" => false,
                    // A leased entry: available once its lease expired (nack = 0).
                    Some(state) => state.parse::<i64>().map(|exp| exp <= now).unwrap_or(true),
                };
                if !available {
                    continue;
                }
                conn.hset::<_, _, _, ()>(&deliv_key, &entry.id, lease_until)
                    .map_err(map_err)?;
                out.push(Self::stream_id_to_message(topic, entry));
            }
            match next_start {
                Some(next) if !exhausted => start = next,
                _ => break,
            }
        }
        Ok(out)
    }

    /// Ack one leased message (delivery done). Returns false when there was no
    /// un-acked delivery to ack.
    pub fn ack_message(
        &self,
        topic: &str,
        subscription_name: &str,
        message_id: &str,
    ) -> Result<bool> {
        self.set_delivery_state(topic, subscription_name, message_id, "acked")
    }

    /// Nack one leased message — make it immediately available (`0`). Returns
    /// false when there was no un-acked delivery to nack.
    pub fn nack_message(
        &self,
        topic: &str,
        subscription_name: &str,
        message_id: &str,
    ) -> Result<bool> {
        self.set_delivery_state(topic, subscription_name, message_id, "0")
    }

    /// Set a delivery's state, but only when it exists and is not already acked
    /// (ack/nack are no-ops on an unknown or already-acked delivery).
    fn set_delivery_state(
        &self,
        topic: &str,
        subscription_name: &str,
        message_id: &str,
        state: &str,
    ) -> Result<bool> {
        let mut conn = self.conn()?;
        let deliv_key = self.pm_deliveries_key(topic, subscription_name);
        let current: Option<String> = conn.hget(&deliv_key, message_id).map_err(map_err)?;
        match current {
            Some(v) if v != "acked" => {
                conn.hset::<_, _, _, ()>(&deliv_key, message_id, state)
                    .map_err(map_err)?;
                Ok(true)
            }
            _ => Ok(false),
        }
    }
}
