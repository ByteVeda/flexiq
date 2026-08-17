/// Generates shared topic-subscription operation methods for Diesel-backed
/// storage backends.
///
/// `register_subscription` differs between SQLite (`replace_into`) and Postgres
/// (`on_conflict...do_update`), so it stays backend-specific. The remaining five
/// read/update/reap methods are identical and live here.
macro_rules! impl_diesel_pubsub_ops {
    ($storage_type:ty) => {
        impl $storage_type {
            /// Active subscriptions for a topic, in registration order.
            ///
            /// Ordering is `created_at` then `subscription_name` so every backend
            /// agrees on a stable, registration-time order.
            pub fn list_subscriptions_for_topic(
                &self,
                topic: &str,
            ) -> Result<Vec<$crate::storage::records::Subscription>> {
                let mut conn = self.conn()?;

                let rows = topic_subscriptions::table
                    .filter(topic_subscriptions::topic.eq(topic))
                    .filter(topic_subscriptions::active.eq(true))
                    .order((
                        topic_subscriptions::created_at.asc(),
                        topic_subscriptions::subscription_name.asc(),
                    ))
                    .select(SubscriptionRow::as_select())
                    .load::<SubscriptionRow>(&mut conn)?;

                Ok(rows.into_iter().map(Into::into).collect())
            }

            /// Every registered subscription, active or paused, across all topics.
            pub fn list_subscriptions(
                &self,
            ) -> Result<Vec<$crate::storage::records::Subscription>> {
                let mut conn = self.conn()?;

                let rows = topic_subscriptions::table
                    .select(SubscriptionRow::as_select())
                    .load::<SubscriptionRow>(&mut conn)?;

                Ok(rows.into_iter().map(Into::into).collect())
            }

            /// Remove a subscription. Returns false if none matched.
            pub fn unsubscribe(&self, topic: &str, subscription_name: &str) -> Result<bool> {
                let mut conn = self.conn()?;

                let affected =
                    diesel::delete(topic_subscriptions::table.find((topic, subscription_name)))
                        .execute(&mut conn)?;

                Ok(affected > 0)
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

                let affected =
                    diesel::update(topic_subscriptions::table.find((topic, subscription_name)))
                        .set(topic_subscriptions::active.eq(active))
                        .execute(&mut conn)?;

                Ok(affected > 0)
            }

            /// Remove ephemeral subscriptions whose owner is not in
            /// `live_worker_ids`. Durable rows (owner NULL) are excluded by the
            /// `is_not_null` filter and never touched. Rows younger than the
            /// registration grace window survive: a starting worker inserts its
            /// ephemeral subscriptions before its first heartbeat lands, and
            /// another worker's reap tick must not race that gap.
            /// Returns the count removed.
            pub fn reap_ephemeral_subscriptions(&self, live_worker_ids: &[String]) -> Result<u64> {
                let mut conn = self.conn()?;
                let cutoff =
                    $crate::job::now_millis() - $crate::storage::EPHEMERAL_SUBSCRIPTION_GRACE_MS;

                let affected = diesel::delete(
                    topic_subscriptions::table
                        .filter(topic_subscriptions::owner_worker_id.is_not_null())
                        .filter(topic_subscriptions::owner_worker_id.ne_all(live_worker_ids))
                        .filter(topic_subscriptions::created_at.lt(cutoff)),
                )
                .execute(&mut conn)?;

                Ok(affected as u64)
            }

            /// Backlog/lag snapshot per registered subscription. Four bounded,
            /// index-backed queries (subscription skeleton + pending/running
            /// counts + oldest-pending age + dead counts) merged in Rust — the
            /// same live+aggregate shape as `stats_all_queues`, never a full
            /// table scan. Counts are always a direct read of real state, so
            /// they cannot drift the way a maintained counter would.
            pub fn topic_backlog_stats(
                &self,
            ) -> Result<Vec<$crate::storage::SubscriptionBacklogStats>> {
                use $crate::job::JobStatus;
                use $crate::storage::schema::{dead_letter, jobs};
                let mut conn = self.conn()?;

                let subs = topic_subscriptions::table
                    .select($crate::storage::models::SubscriptionRow::as_select())
                    .load::<$crate::storage::models::SubscriptionRow>(&mut conn)?;

                // Pending + running counts, scoped to pub/sub-tagged rows only.
                let counts: Vec<(String, String, i32, i64)> = jobs::table
                    .filter(jobs::subscription_name.is_not_null())
                    .filter(
                        jobs::status.eq_any([JobStatus::Pending as i32, JobStatus::Running as i32]),
                    )
                    .group_by((jobs::topic, jobs::subscription_name, jobs::status))
                    .select((
                        jobs::topic.assume_not_null(),
                        jobs::subscription_name.assume_not_null(),
                        jobs::status,
                        diesel::dsl::count(jobs::id),
                    ))
                    .load(&mut conn)?;

                let oldest: Vec<(String, String, Option<i64>)> = jobs::table
                    .filter(jobs::subscription_name.is_not_null())
                    .filter(jobs::status.eq(JobStatus::Pending as i32))
                    .group_by((jobs::topic, jobs::subscription_name))
                    .select((
                        jobs::topic.assume_not_null(),
                        jobs::subscription_name.assume_not_null(),
                        diesel::dsl::min(jobs::created_at),
                    ))
                    .load(&mut conn)?;

                let dead: Vec<(String, String, i64)> = dead_letter::table
                    .filter(dead_letter::subscription_name.is_not_null())
                    .group_by((dead_letter::topic, dead_letter::subscription_name))
                    .select((
                        dead_letter::topic.assume_not_null(),
                        dead_letter::subscription_name.assume_not_null(),
                        diesel::dsl::count(dead_letter::id),
                    ))
                    .load(&mut conn)?;

                let subs = subs.into_iter().map(Into::into).collect();
                Ok($crate::storage::merge_backlog_stats(
                    subs, counts, oldest, dead,
                ))
            }

            /// Append one message to a log topic (id = UUIDv7, so the id is the
            /// time-ordered read cursor). O(1), independent of subscriber count.
            pub fn publish_message(
                &self,
                topic: &str,
                payload: &[u8],
                metadata: Option<&str>,
                notes: Option<&str>,
                expires_at: Option<i64>,
            ) -> Result<$crate::storage::records::TopicMessage> {
                let mut conn = self.conn()?;
                let id = uuid::Uuid::now_v7().to_string();
                let created_at = $crate::job::now_millis();
                let row = NewTopicMessageRow {
                    id: &id,
                    topic,
                    payload,
                    metadata,
                    notes,
                    created_at,
                    expires_at,
                };
                diesel::insert_into(topic_messages::table)
                    .values(&row)
                    .execute(&mut conn)?;
                Ok($crate::storage::records::TopicMessage {
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
            /// Cursor resolved server-side; read is exclusive. Unknown subscription
            /// or non-positive limit → empty.
            pub fn read_topic_messages(
                &self,
                topic: &str,
                subscription_name: &str,
                limit: i64,
            ) -> Result<Vec<$crate::storage::records::TopicMessage>> {
                if limit <= 0 {
                    return Ok(Vec::new());
                }
                let mut conn = self.conn()?;
                // Require a log subscription: a fan-out sub must never read the
                // log of a mixed topic.
                let cursor: Option<Option<String>> = topic_subscriptions::table
                    .filter(topic_subscriptions::topic.eq(topic))
                    .filter(topic_subscriptions::subscription_name.eq(subscription_name))
                    .filter(
                        topic_subscriptions::mode
                            .eq($crate::storage::records::SubscriptionMode::Log.as_str()),
                    )
                    .select(topic_subscriptions::cursor)
                    .first(&mut conn)
                    .optional()?;
                let Some(cursor) = cursor else {
                    return Ok(Vec::new());
                };

                let mut query = topic_messages::table
                    .filter(topic_messages::topic.eq(topic))
                    .into_boxed();
                if let Some(after) = cursor {
                    query = query.filter(topic_messages::id.gt(after));
                }
                let rows = query
                    .order(topic_messages::id.asc())
                    .limit(limit)
                    .select(TopicMessageRow::as_select())
                    .load::<TopicMessageRow>(&mut conn)?;
                Ok(rows.into_iter().map(Into::into).collect())
            }

            /// Advance a log subscription's cursor. Monotonic (never rewinds) and
            /// idempotent. Returns false when nothing advanced.
            pub fn ack_topic_cursor(
                &self,
                topic: &str,
                subscription_name: &str,
                cursor: &str,
            ) -> Result<bool> {
                let mut conn = self.conn()?;
                // Only a log subscription has a cursor; the mode filter also stops
                // an ack on a fan-out subscription from writing one.
                let affected = diesel::update(
                    topic_subscriptions::table
                        .filter(topic_subscriptions::topic.eq(topic))
                        .filter(topic_subscriptions::subscription_name.eq(subscription_name))
                        .filter(
                            topic_subscriptions::mode
                                .eq($crate::storage::records::SubscriptionMode::Log.as_str()),
                        )
                        .filter(
                            topic_subscriptions::cursor
                                .is_null()
                                .or(topic_subscriptions::cursor.lt(cursor)),
                        ),
                )
                .set(topic_subscriptions::cursor.eq(cursor))
                .execute(&mut conn)?;
                Ok(affected > 0)
            }

            /// Per-log-subscription lag (messages after the cursor) + oldest
            /// un-acked age. One aggregate per log subscription; fan-out excluded.
            pub fn topic_log_stats(&self) -> Result<Vec<$crate::storage::records::TopicLogStats>> {
                let mut conn = self.conn()?;
                let now = $crate::job::now_millis();
                let subs: Vec<(String, String, Option<String>)> = topic_subscriptions::table
                    .filter(
                        topic_subscriptions::mode
                            .eq($crate::storage::records::SubscriptionMode::Log.as_str()),
                    )
                    .select((
                        topic_subscriptions::topic,
                        topic_subscriptions::subscription_name,
                        topic_subscriptions::cursor,
                    ))
                    .load(&mut conn)?;

                let mut out = Vec::with_capacity(subs.len());
                for (topic, subscription_name, cursor) in subs {
                    let mut query = topic_messages::table
                        .filter(topic_messages::topic.eq(&topic))
                        .into_boxed();
                    if let Some(ref after) = cursor {
                        query = query.filter(topic_messages::id.gt(after.clone()));
                    }
                    let (lag, oldest): (i64, Option<i64>) = query
                        .select((
                            diesel::dsl::count_star(),
                            diesel::dsl::min(topic_messages::created_at),
                        ))
                        .first(&mut conn)?;
                    out.push($crate::storage::records::TopicLogStats {
                        topic,
                        subscription_name,
                        cursor,
                        lag,
                        oldest_unacked_age_ms: oldest.map(|c| (now - c).max(0)),
                    });
                }
                Ok(out)
            }

            /// Drop log messages every subscriber has acked past (id `<=` the min
            /// cursor across a topic's log subs) plus any past `expires_at`.
            /// Bounded by `limit`. A topic with an un-consumed (NULL-cursor) sub
            /// keeps all its messages except expired ones.
            pub fn purge_topic_messages(&self, now: i64, limit: i64) -> Result<u64> {
                use std::collections::HashMap;
                if limit <= 0 {
                    return Ok(0);
                }
                let mut conn = self.conn()?;

                // Per-topic delete floor: the min cursor across its log subs.
                // A topic maps to `None` once any of its subs has read nothing
                // (NULL cursor), which excludes it from cursor-based purging.
                let subs: Vec<(String, Option<String>)> = topic_subscriptions::table
                    .filter(
                        topic_subscriptions::mode
                            .eq($crate::storage::records::SubscriptionMode::Log.as_str()),
                    )
                    .select((topic_subscriptions::topic, topic_subscriptions::cursor))
                    .load(&mut conn)?;
                let mut floors: HashMap<String, Option<String>> = HashMap::new();
                for (topic, cursor) in subs {
                    match floors.entry(topic) {
                        std::collections::hash_map::Entry::Vacant(slot) => {
                            slot.insert(cursor);
                        }
                        std::collections::hash_map::Entry::Occupied(mut slot) => {
                            let merged = match (slot.get().clone(), cursor) {
                                (Some(current), Some(c)) => Some(current.min(c)),
                                _ => None,
                            };
                            slot.insert(merged);
                        }
                    }
                }

                let mut ids: Vec<String> = Vec::new();
                // Expired messages first (TTL safety net, ignores cursors).
                let expired: Vec<String> = topic_messages::table
                    .filter(topic_messages::expires_at.is_not_null())
                    .filter(topic_messages::expires_at.le(now))
                    .select(topic_messages::id)
                    .limit(limit)
                    .load(&mut conn)?;
                ids.extend(expired);

                // Then cursor-fully-acked messages per topic, within the budget.
                for (topic, floor) in floors {
                    let Some(floor) = floor else { continue };
                    let remaining = limit - ids.len() as i64;
                    if remaining <= 0 {
                        break;
                    }
                    let acked: Vec<String> = topic_messages::table
                        .filter(topic_messages::topic.eq(&topic))
                        .filter(topic_messages::id.le(&floor))
                        .select(topic_messages::id)
                        .limit(remaining)
                        .load(&mut conn)?;
                    ids.extend(acked);
                }

                // Per-message compaction: on a topic consumed purely per-message
                // (every log sub has delivery rows — mixing with a cursor sub
                // falls back to expiry-only cleanup), drop the oldest messages
                // every per-message subscriber has acked. Inlined so it reuses
                // `conn` — a second pooled connection would deadlock a size-1 pool.
                let pm_budget = limit - ids.len() as i64;
                if pm_budget > 0 {
                    use diesel::dsl::count_star;
                    use std::collections::HashSet;
                    // Per-message subscribers per topic (those with delivery rows).
                    let pm_pairs: Vec<(String, String)> = topic_deliveries::table
                        .select((topic_deliveries::topic, topic_deliveries::subscription_name))
                        .distinct()
                        .load(&mut conn)?;
                    let mut pm_subs: HashMap<String, HashSet<String>> = HashMap::new();
                    for (topic, name) in pm_pairs {
                        pm_subs.entry(topic).or_default().insert(name);
                    }
                    // Log subscribers per topic — skip a topic that mixes in a
                    // cursor reader (a log sub with no delivery rows).
                    let mut log_by_topic: HashMap<String, Vec<String>> = HashMap::new();
                    if !pm_subs.is_empty() {
                        let log_subs: Vec<(String, String)> = topic_subscriptions::table
                            .filter(
                                topic_subscriptions::mode
                                    .eq($crate::storage::records::SubscriptionMode::Log.as_str()),
                            )
                            .select((
                                topic_subscriptions::topic,
                                topic_subscriptions::subscription_name,
                            ))
                            .load(&mut conn)?;
                        for (topic, name) in log_subs {
                            log_by_topic.entry(topic).or_default().push(name);
                        }
                    }
                    for (topic, names) in &pm_subs {
                        let remaining = limit - ids.len() as i64;
                        if remaining <= 0 {
                            break;
                        }
                        if let Some(logs) = log_by_topic.get(topic) {
                            if logs.iter().any(|n| !names.contains(n)) {
                                continue;
                            }
                        }
                        let sub_count = names.len() as i64;
                        let candidates: Vec<String> = topic_messages::table
                            .filter(topic_messages::topic.eq(topic))
                            .order(topic_messages::id.asc())
                            .select(topic_messages::id)
                            .limit(remaining)
                            .load(&mut conn)?;
                        if candidates.is_empty() {
                            continue;
                        }
                        let acked_counts: Vec<(String, i64)> = topic_deliveries::table
                            .filter(topic_deliveries::topic.eq(topic))
                            .filter(topic_deliveries::message_id.eq_any(&candidates))
                            .filter(topic_deliveries::acked.eq(true))
                            .group_by(topic_deliveries::message_id)
                            .select((topic_deliveries::message_id, count_star()))
                            .load(&mut conn)?;
                        let counts: HashMap<String, i64> = acked_counts.into_iter().collect();
                        for id in candidates {
                            if counts.get(&id).copied().unwrap_or(0) == sub_count {
                                ids.push(id);
                            }
                        }
                    }
                }

                if ids.is_empty() {
                    return Ok(0);
                }
                ids.sort_unstable();
                ids.dedup();
                let removed =
                    diesel::delete(topic_messages::table.filter(topic_messages::id.eq_any(&ids)))
                        .execute(&mut conn)?;
                // Drop the delivery rows of the purged messages so per-message
                // state can't outlive the log it tracks.
                diesel::delete(
                    topic_deliveries::table.filter(topic_deliveries::message_id.eq_any(&ids)),
                )
                .execute(&mut conn)?;
                Ok(removed as u64)
            }

            /// Fetch a declared topic by name, or `None` if never declared.
            pub fn get_topic(&self, name: &str) -> Result<Option<$crate::storage::records::Topic>> {
                let mut conn = self.conn()?;
                let row = topics::table
                    .find(name)
                    .select(TopicRow::as_select())
                    .first::<TopicRow>(&mut conn)
                    .optional()?;
                Ok(row.map(Into::into))
            }

            /// Every declared topic in the registry.
            pub fn list_declared_topics(&self) -> Result<Vec<$crate::storage::records::Topic>> {
                let mut conn = self.conn()?;
                let rows = topics::table
                    .select(TopicRow::as_select())
                    .load::<TopicRow>(&mut conn)?;
                Ok(rows.into_iter().map(Into::into).collect())
            }

            /// Lease up to `limit` available messages to `subscription_name` for
            /// `visibility_ms` (see the `lease_topic_messages` trait method).
            /// Requires a registered `log` subscription (like the cursor read).
            /// One bounded anti-join finds the available messages, then a lease
            /// row is upserted per message in one transaction. Delivery is
            /// **at-least-once**: the availability window isn't row-locked, so two
            /// concurrent leasers can briefly claim the same message — handlers
            /// must be idempotent, the same guarantee as a lease timeout redeliver.
            pub fn lease_topic_messages(
                &self,
                topic: &str,
                subscription_name: &str,
                limit: i64,
                visibility_ms: i64,
                now: i64,
            ) -> Result<Vec<$crate::storage::records::TopicMessage>> {
                if limit <= 0 {
                    return Ok(Vec::new());
                }
                let mut conn = self.conn()?;
                // Only a registered log subscription may lease — a fan-out or
                // unknown name must never claim from the log (mirrors read/ack).
                let is_log_sub = topic_subscriptions::table
                    .filter(topic_subscriptions::topic.eq(topic))
                    .filter(topic_subscriptions::subscription_name.eq(subscription_name))
                    .filter(
                        topic_subscriptions::mode
                            .eq($crate::storage::records::SubscriptionMode::Log.as_str()),
                    )
                    .select(topic_subscriptions::subscription_name)
                    .first::<String>(&mut conn)
                    .optional()?;
                if is_log_sub.is_none() {
                    return Ok(Vec::new());
                }
                conn.transaction(|conn| {
                    // Available = no delivery row (never delivered), or a prior
                    // lease that expired and was never acked.
                    let rows: Vec<TopicMessageRow> = topic_messages::table
                        .left_join(
                            topic_deliveries::table.on(topic_deliveries::topic
                                .eq(topic_messages::topic)
                                .and(topic_deliveries::subscription_name.eq(subscription_name))
                                .and(topic_deliveries::message_id.eq(topic_messages::id))),
                        )
                        .filter(topic_messages::topic.eq(topic))
                        .filter(
                            topic_deliveries::message_id
                                .is_null()
                                .or(topic_deliveries::acked
                                    .eq(false)
                                    .and(topic_deliveries::lease_expires_at.le(now))),
                        )
                        .order(topic_messages::id.asc())
                        .limit(limit)
                        .select(TopicMessageRow::as_select())
                        .load(conn)?;

                    let lease_until = now + visibility_ms;
                    for row in &rows {
                        let lease = NewTopicDeliveryRow {
                            topic,
                            subscription_name,
                            message_id: &row.id,
                            acked: false,
                            attempts: 1,
                            lease_expires_at: lease_until,
                            delivered_at: now,
                        };
                        diesel::insert_into(topic_deliveries::table)
                            .values(&lease)
                            .on_conflict((
                                topic_deliveries::topic,
                                topic_deliveries::subscription_name,
                                topic_deliveries::message_id,
                            ))
                            .do_update()
                            .set((
                                topic_deliveries::acked.eq(false),
                                topic_deliveries::attempts.eq(topic_deliveries::attempts + 1),
                                topic_deliveries::lease_expires_at.eq(lease_until),
                                topic_deliveries::delivered_at.eq(now),
                            ))
                            .execute(conn)?;
                    }
                    Ok(rows.into_iter().map(Into::into).collect())
                })
            }

            /// Ack one leased message — the delivery is done. Returns false when
            /// there was no un-acked delivery row to ack.
            pub fn ack_message(
                &self,
                topic: &str,
                subscription_name: &str,
                message_id: &str,
            ) -> Result<bool> {
                let mut conn = self.conn()?;
                let affected = diesel::update(
                    topic_deliveries::table
                        .find((topic, subscription_name, message_id))
                        .filter(topic_deliveries::acked.eq(false)),
                )
                .set(topic_deliveries::acked.eq(true))
                .execute(&mut conn)?;
                Ok(affected > 0)
            }

            /// Nack one leased message — make it immediately available for
            /// redelivery (`lease_expires_at = 0`). Returns false when there was
            /// no un-acked delivery row to nack.
            pub fn nack_message(
                &self,
                topic: &str,
                subscription_name: &str,
                message_id: &str,
            ) -> Result<bool> {
                let mut conn = self.conn()?;
                let affected = diesel::update(
                    topic_deliveries::table
                        .find((topic, subscription_name, message_id))
                        .filter(topic_deliveries::acked.eq(false)),
                )
                .set(topic_deliveries::lease_expires_at.eq(0))
                .execute(&mut conn)?;
                Ok(affected > 0)
            }
        }
    };
}

pub(crate) use impl_diesel_pubsub_ops;
