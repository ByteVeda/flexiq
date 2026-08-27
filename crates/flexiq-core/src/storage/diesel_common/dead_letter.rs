/// Generates shared dead-letter operation methods for Diesel-backed storage backends.
///
/// All four methods (`move_to_dlq`, `list_dead`, `retry_dead`, `purge_dead`) are
/// identical between SQLite and Postgres, so the entire file is macro-generated.
macro_rules! impl_diesel_dead_letter_ops {
    ($storage_type:ty) => {
        impl $storage_type {
            /// Move a job to the dead letter queue and cascade-cancel dependents.
            pub fn move_to_dlq(
                &self,
                job: &Job,
                error: &str,
                metadata: Option<&str>,
            ) -> Result<()> {
                self.dead_letter(job, error, metadata, false)
            }

            /// Dead-letter a job the scheduler shed, flagging the entry so the
            /// auto-retry sweep's query skips it.
            pub fn shed_to_dlq(
                &self,
                job: &Job,
                error: &str,
                metadata: Option<&str>,
            ) -> Result<()> {
                self.dead_letter(job, error, metadata, true)
            }

            /// Shared body of `move_to_dlq`/`shed_to_dlq`; `shed` is the only
            /// difference between them.
            fn dead_letter(
                &self,
                job: &Job,
                error: &str,
                metadata: Option<&str>,
                shed: bool,
            ) -> Result<()> {
                let now = now_millis();
                let dlq_id = uuid::Uuid::now_v7().to_string();
                let job_id = job.id.clone();

                // Write-priority transaction: this reads the job row then writes
                // it to `archived_jobs`, which would deadlock under SQLite's
                // deferred lock-upgrade. See `write_transaction`.
                let dlq_retry_count = job
                    .metadata
                    .as_deref()
                    .and_then(|m| serde_json::from_str::<serde_json::Value>(m).ok())
                    .and_then(|v| v.get("__dlq_retry_count")?.as_i64())
                    .unwrap_or(0) as i32;
                // Carry the delivery's subscription attribution into the DLQ so
                // per-subscription dead-letter depth is countable.
                let (topic, subscription_name) =
                    $crate::pubsub::extract_topic_subscription(job.notes.as_deref())
                        .map_or((None, None), |(t, s)| (Some(t), Some(s)));
                // The run this job belongs to, recorded on a column of its own:
                // the `metadata` argument replaces the job's blob wholesale, and
                // a bare-string replacement has nothing to merge an origin into.
                // See `m0014_dead_letter_origin`.
                let origin_job_id = $crate::step::run_key(job);

                self.write_transaction(|conn| {
                    let dlq_row = NewDeadLetterRow {
                        id: &dlq_id,
                        original_job_id: &job.id,
                        queue: &job.queue,
                        task_name: &job.task_name,
                        payload: &job.payload,
                        error: Some(error),
                        retry_count: job.retry_count,
                        failed_at: now,
                        // Preserve the job's own metadata so it survives the
                        // round trip; an explicit `metadata` arg overrides it.
                        metadata: metadata.or(job.metadata.as_deref()),
                        notes: job.notes.as_deref(),
                        priority: job.priority,
                        max_retries: job.max_retries,
                        timeout_ms: job.timeout_ms,
                        result_ttl_ms: job.result_ttl_ms,
                        namespace: job.namespace.as_deref(),
                        dlq_retry_count,
                        topic: topic.as_deref(),
                        subscription_name: subscription_name.as_deref(),
                        shed,
                        origin_job_id: Some(&origin_job_id),
                    };

                    diesel::insert_into(dead_letter::table)
                        .values(&dlq_row)
                        .execute(conn)?;

                    // Archive the now-Dead job: move it out of the live `jobs`
                    // table into `archived_jobs` so the dequeue index and stats
                    // scans no longer see it. The row may already be absent if a
                    // prior terminal transition archived it; only archive when
                    // it is still live.
                    if let Some(mut row) = jobs::table
                        .find(&job.id)
                        .select(JobRow::as_select())
                        .first(conn)
                        .optional()?
                    {
                        row.status = JobStatus::Dead as i32;
                        row.error = Some(error.to_string());
                        row.completed_at = Some(now);
                        <$storage_type>::archive_job_row(conn, &row)?;
                    }

                    Ok::<(), QueueError>(())
                })?;

                // Cascade cancel dependents (opens its own connection, so the
                // archive transaction above must already be committed).
                self.cascade_cancel(&job_id, "dependency failed", job.namespace.as_deref())?;

                Ok(())
            }

            /// List dead letter entries. `namespace` of `None` returns every
            /// namespace, matching `list_jobs`.
            pub fn list_dead(
                &self,
                limit: i64,
                offset: i64,
                namespace: Option<&str>,
            ) -> Result<Vec<DeadJob>> {
                let mut conn = self.conn()?;

                // Narrow projection: DLQ listings never render the arg blob.
                let mut query = dead_letter::table
                    .into_boxed()
                    .order(dead_letter::failed_at.desc());
                if let Some(ns) = namespace {
                    query = query.filter(dead_letter::namespace.eq(ns));
                }

                let rows: Vec<NarrowDeadLetterRow> = query
                    .limit(limit)
                    .offset(offset)
                    .select(NarrowDeadLetterRow::as_select())
                    .load(&mut conn)?;

                Ok(rows.into_iter().map(DeadJob::from_narrow).collect())
            }

            /// Keyset-paginated `list_dead`, ordered by `(failed_at, id)`
            /// descending with a `(failed_at, id) < cursor` bound.
            pub fn list_dead_after(
                &self,
                limit: i64,
                after: Option<(i64, &str)>,
                namespace: Option<&str>,
            ) -> Result<Vec<DeadJob>> {
                if limit <= 0 {
                    return Ok(Vec::new());
                }
                let mut conn = self.conn()?;

                let mut query = dead_letter::table
                    .into_boxed()
                    .order((dead_letter::failed_at.desc(), dead_letter::id.desc()));

                if let Some((cursor_failed_at, cursor_id)) = after {
                    let cursor_id = cursor_id.to_string();
                    query = query.filter(
                        dead_letter::failed_at
                            .lt(cursor_failed_at)
                            .or(dead_letter::failed_at
                                .eq(cursor_failed_at)
                                .and(dead_letter::id.lt(cursor_id))),
                    );
                }
                if let Some(ns) = namespace {
                    query = query.filter(dead_letter::namespace.eq(ns));
                }

                let rows: Vec<NarrowDeadLetterRow> = query
                    .limit(limit)
                    .select(NarrowDeadLetterRow::as_select())
                    .load(&mut conn)?;

                Ok(rows.into_iter().map(DeadJob::from_narrow).collect())
            }

            /// List dead letter entries for a single task, newest first.
            pub fn list_dead_by_task(
                &self,
                task_name: &str,
                limit: i64,
                offset: i64,
                namespace: Option<&str>,
            ) -> Result<Vec<DeadJob>> {
                // Normalize pagination so every backend agrees: a non-positive
                // limit yields no page (matches Redis), and offset never goes
                // negative.
                if limit <= 0 {
                    return Ok(Vec::new());
                }
                let offset = offset.max(0);

                let mut conn = self.conn()?;

                let mut query = dead_letter::table
                    .into_boxed()
                    .filter(dead_letter::task_name.eq(task_name))
                    .order(dead_letter::failed_at.desc());
                if let Some(ns) = namespace {
                    query = query.filter(dead_letter::namespace.eq(ns));
                }

                let rows: Vec<NarrowDeadLetterRow> = query
                    .limit(limit)
                    .offset(offset)
                    .select(NarrowDeadLetterRow::as_select())
                    .load(&mut conn)?;

                Ok(rows.into_iter().map(DeadJob::from_narrow).collect())
            }

            /// Delete every dead letter entry for a task. Returns the count removed.
            pub fn purge_dead_by_task(&self, task_name: &str) -> Result<u64> {
                let mut conn = self.conn()?;

                let affected =
                    diesel::delete(dead_letter::table.filter(dead_letter::task_name.eq(task_name)))
                        .execute(&mut conn)?;

                Ok(affected as u64)
            }

            /// Re-enqueue a dead letter job. Returns the new job ID.
            ///
            /// Returns `JobNotFound` only when the dead-letter row is absent or
            /// is removed concurrently by another retry; other Diesel errors
            /// propagate as `Storage` so real DB failures aren't masked. The
            /// delete inside the transaction asserts a row was actually removed,
            /// preventing two concurrent retries from both committing a fresh
            /// enqueue against the same dead-letter id.
            ///
            /// An entry in another namespace reports `JobNotFound`: a caller
            /// scoped to one tenant must not be able to tell an id it may not
            /// touch from one that does not exist.
            pub fn retry_dead(&self, dead_id: &str, namespace: Option<&str>) -> Result<String> {
                let mut conn = self.conn()?;

                let dead_row: DeadLetterRow = match dead_letter::table
                    .find(dead_id)
                    .select(DeadLetterRow::as_select())
                    .first(&mut conn)
                {
                    Ok(row) => row,
                    Err(diesel::result::Error::NotFound) => {
                        return Err(QueueError::JobNotFound(dead_id.to_string()));
                    }
                    Err(err) => return Err(err.into()),
                };

                if !Self::in_namespace(dead_row.namespace.as_deref(), namespace) {
                    return Err(QueueError::JobNotFound(dead_id.to_string()));
                }

                let retry_metadata = {
                    let next_count = dead_row.dlq_retry_count + 1;
                    let mut obj = dead_row
                        .metadata
                        .as_deref()
                        .and_then(|m| {
                            serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(m)
                                .ok()
                        })
                        .unwrap_or_default();
                    obj.insert(
                        "__dlq_retry_count".to_string(),
                        serde_json::Value::from(next_count),
                    );
                    // The one path that changes a run's job id, so the one
                    // that records what it was: an operator's DLQ retry must
                    // send the step keys the first attempt sent, not fresh ones.
                    // The column outranks the blob, which a caller's replacement
                    // metadata may have emptied; a row older than the column
                    // still resolves from the blob.
                    $crate::step::stamp_origin_job_id(
                        &mut obj,
                        dead_row.origin_job_id.as_deref(),
                        &dead_row.original_job_id,
                    );
                    Some(serde_json::to_string(&serde_json::Value::Object(obj)).unwrap_or_default())
                };

                let new_job = NewJob {
                    queue: dead_row.queue,
                    task_name: dead_row.task_name,
                    payload: dead_row.payload,
                    priority: dead_row.priority,
                    scheduled_at: now_millis(),
                    max_retries: dead_row.max_retries,
                    timeout_ms: dead_row.timeout_ms,
                    unique_key: None,
                    metadata: retry_metadata,
                    notes: dead_row.notes,
                    depends_on: vec![],
                    expires_at: None,
                    result_ttl_ms: dead_row.result_ttl_ms,
                    namespace: dead_row.namespace,
                    // Cleared for the same reason as `unique_key`: an operator
                    // retry must run, not be coalesced into someone's window.
                    debounce_key: None,
                };

                let job = new_job.into_job();
                // Re-attribute the resurrected delivery so it counts against the
                // subscription's backlog again, not just its DLQ depth.
                let attribution = $crate::storage::diesel_common::JobAttribution::of(&job);

                let result = conn.transaction(|conn| {
                    let row = $crate::storage::diesel_common::new_job_row(&job, &attribution);

                    diesel::insert_into(jobs::table)
                        .values(&row)
                        .execute(conn)?;

                    let deleted = diesel::delete(dead_letter::table.find(dead_id)).execute(conn)?;
                    if deleted == 0 {
                        // A concurrent retry won the race. Roll back the
                        // freshly-inserted job by returning an error from the
                        // transaction closure — Diesel aborts the txn.
                        return Err(diesel::result::Error::NotFound);
                    }

                    Ok::<(), diesel::result::Error>(())
                });

                match result {
                    Ok(()) => Ok(job.id),
                    Err(diesel::result::Error::NotFound) => {
                        Err(QueueError::JobNotFound(dead_id.to_string()))
                    }
                    Err(err) => Err(err.into()),
                }
            }

            /// Purge dead letter entries older than the given timestamp.
            ///
            /// Deletes in bounded batches, each its own txn — see
            /// `diesel_common::purge`.
            pub fn purge_dead(&self, older_than_ms: i64) -> Result<u64> {
                $crate::storage::diesel_common::purge::drain_batches(|| {
                    self.write_transaction(|conn| {
                        let ids: Vec<String> = dead_letter::table
                            .filter(dead_letter::failed_at.lt(older_than_ms))
                            .select(dead_letter::id)
                            .limit($crate::storage::diesel_common::purge::PURGE_BATCH)
                            .load(conn)?;
                        let affected =
                            diesel::delete(dead_letter::table.filter(dead_letter::id.eq_any(&ids)))
                                .execute(conn)?;
                        Ok(affected as u64)
                    })
                })
            }

            /// Delete a single dead letter entry. Returns true if it existed.
            ///
            /// An entry in another namespace reports `false`, the same answer an
            /// unknown id gets.
            pub fn delete_dead(&self, dead_id: &str, namespace: Option<&str>) -> Result<bool> {
                let mut conn = self.conn()?;
                let mut delete = diesel::delete(dead_letter::table.find(dead_id)).into_boxed();
                if let Some(ns) = namespace {
                    delete = delete.filter(dead_letter::namespace.eq(ns));
                }
                Ok(delete.execute(&mut conn)? > 0)
            }

            /// Whether a row's namespace is visible to a caller scoped to
            /// `caller`. An unscoped caller sees every namespace.
            fn in_namespace(row: Option<&str>, caller: Option<&str>) -> bool {
                caller.is_none_or(|scope| row == Some(scope))
            }

            /// Purge dead letter entries respecting per-entry TTL overrides.
            ///
            /// Deletes in bounded batches, each its own txn — see
            /// `diesel_common::purge`. A `None` cutoff runs only the per-entry
            /// sweep — an entry can carry its own TTL even when the queue keeps
            /// everything.
            pub fn purge_dead_with_ttl(&self, global_cutoff_ms: Option<i64>) -> Result<u64> {
                let now = now_millis();

                // Entries with no per-entry TTL fall back to the global cutoff.
                let global = match global_cutoff_ms {
                    Some(cutoff) => $crate::storage::diesel_common::purge::drain_batches(|| {
                        self.write_transaction(|conn| {
                            let ids: Vec<String> = dead_letter::table
                                .filter(dead_letter::result_ttl_ms.is_null())
                                .filter(dead_letter::failed_at.lt(cutoff))
                                .select(dead_letter::id)
                                .limit($crate::storage::diesel_common::purge::PURGE_BATCH)
                                .load(conn)?;
                            let affected = diesel::delete(
                                dead_letter::table.filter(dead_letter::id.eq_any(&ids)),
                            )
                            .execute(conn)?;
                            Ok(affected as u64)
                        })
                    })?,
                    None => 0,
                };

                // Entries with a per-entry TTL: `failed_at + result_ttl_ms < now`.
                // Served by the partial `idx_dead_letter_ttl` — no ordinary index
                // covers an expression over two columns.
                let per_entry = $crate::storage::diesel_common::purge::drain_batches(|| {
                    self.write_transaction(|conn| {
                        let ids: Vec<String> = dead_letter::table
                            .filter(dead_letter::result_ttl_ms.is_not_null())
                            .filter(
                                (dead_letter::failed_at
                                    + dead_letter::result_ttl_ms.assume_not_null())
                                .lt(now),
                            )
                            .select(dead_letter::id)
                            .limit($crate::storage::diesel_common::purge::PURGE_BATCH)
                            .load(conn)?;
                        let affected =
                            diesel::delete(dead_letter::table.filter(dead_letter::id.eq_any(&ids)))
                                .execute(conn)?;
                        Ok(affected as u64)
                    })
                })?;

                Ok(global + per_entry)
            }

            /// List DLQ entries eligible for auto-retry.
            pub fn list_dead_for_retry(
                &self,
                cutoff_ms: i64,
                max_retries: i32,
                namespace: Option<&str>,
                queues: &[String],
                limit: i64,
            ) -> Result<Vec<DeadJob>> {
                let mut conn = self.conn()?;

                // Scope to the worker's own namespace + served queues, mirroring
                // the poller's dequeue scoping, so auto-retry never resurrects
                // entries belonging to other namespaces/queues.
                // `shed` is filtered here rather than by the caller: the limit
                // is applied by the query, so a page of shed entries would be
                // truncated *before* the caller ever saw the failures behind
                // them. A shed entry is never retried, so its `dlq_retry_count`
                // stays 0 and it would own the head of this ordering forever.
                let mut query = dead_letter::table
                    .filter(dead_letter::failed_at.le(cutoff_ms))
                    .filter(dead_letter::dlq_retry_count.lt(max_retries))
                    .filter(dead_letter::queue.eq_any(queues))
                    .filter(dead_letter::shed.eq(false))
                    .into_boxed();
                match namespace {
                    Some(ns) => query = query.filter(dead_letter::namespace.eq(ns)),
                    None => query = query.filter(dead_letter::namespace.is_null()),
                }

                // Narrow projection: the auto-retry loop reads only `id`/
                // `dlq_retry_count`; `retry_dead(id)` re-reads the full row to
                // rebuild the payload, so listing never needs the arg blob.
                let rows: Vec<NarrowDeadLetterRow> = query
                    .order(dead_letter::failed_at.asc())
                    .limit(limit)
                    .select(NarrowDeadLetterRow::as_select())
                    .load(&mut conn)?;

                Ok(rows.into_iter().map(DeadJob::from_narrow).collect())
            }
        }
    };
}

pub(crate) use impl_diesel_dead_letter_ops;
