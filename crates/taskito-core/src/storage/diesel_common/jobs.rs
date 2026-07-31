/// Generates shared job operation methods for Diesel-backed storage backends.
///
/// Both SQLite and PostgreSQL implementations are identical for these methods.
/// Only dequeue locking semantics and a few upsert strategies differ between backends.
macro_rules! impl_diesel_job_ops {
    ($storage_type:ty, $conn_type:ty) => {
        impl $storage_type {
            /// Max archived rows deleted per txn in the batched purge loops.
            /// Shared with every other purge — see `diesel_common::purge`.
            const PURGE_BATCH: i64 = $crate::storage::diesel_common::purge::PURGE_BATCH;
            /// Max pending rows archived per txn in the batched cancel/expire
            /// loops — same lock-hold bound for the mass-mutation paths.
            const MASS_ARCHIVE_BATCH: i64 = 500;

            /// Delete a job's dependency edges and replay history — the
            /// relational children with no retention window, always removed with
            /// the job.
            fn delete_job_relations(
                conn: &mut $conn_type,
                job_ids: &[String],
            ) -> diesel::result::QueryResult<()> {
                let chunk_size = $crate::storage::diesel_common::purge::DELETE_ID_CHUNK;
                for chunk in job_ids.chunks(chunk_size) {
                    diesel::delete(
                        job_dependencies::table.filter(
                            job_dependencies::job_id
                                .eq_any(chunk)
                                .or(job_dependencies::depends_on_job_id.eq_any(chunk)),
                        ),
                    )
                    .execute(conn)?;
                    diesel::delete(
                        replay_history::table.filter(replay_history::original_job_id.eq_any(chunk)),
                    )
                    .execute(conn)?;
                }
                Ok(())
            }

            /// Delete a job's retention-managed side data — errors, logs, and
            /// metrics. Each has its own retention window, so the retention
            /// archive purge leaves these to their own sweep; only a full purge
            /// (`purge_completed`) removes them alongside the job.
            fn delete_job_diagnostics(
                conn: &mut $conn_type,
                job_ids: &[String],
            ) -> diesel::result::QueryResult<()> {
                let chunk_size = $crate::storage::diesel_common::purge::DELETE_ID_CHUNK;
                for chunk in job_ids.chunks(chunk_size) {
                    diesel::delete(job_errors::table.filter(job_errors::job_id.eq_any(chunk)))
                        .execute(conn)?;
                    diesel::delete(task_logs::table.filter(task_logs::job_id.eq_any(chunk)))
                        .execute(conn)?;
                    diesel::delete(task_metrics::table.filter(task_metrics::job_id.eq_any(chunk)))
                        .execute(conn)?;
                }
                Ok(())
            }

            /// Move an already-loaded job row into `archived_jobs` and delete it
            /// from `jobs`, inside the caller's transaction. Terminal jobs live
            /// in `archived_jobs` so the live `jobs` table only ever holds
            /// pending/running rows that the dequeue index must scan.
            pub(crate) fn archive_job_row(
                conn: &mut $conn_type,
                row: &JobRow,
            ) -> diesel::result::QueryResult<()> {
                let archived = NewArchivedJobRow {
                    id: &row.id,
                    queue: &row.queue,
                    task_name: &row.task_name,
                    payload: &row.payload,
                    status: row.status,
                    priority: row.priority,
                    created_at: row.created_at,
                    scheduled_at: row.scheduled_at,
                    started_at: row.started_at,
                    completed_at: row.completed_at,
                    retry_count: row.retry_count,
                    max_retries: row.max_retries,
                    result: row.result.as_deref(),
                    error: row.error.as_deref(),
                    timeout_ms: row.timeout_ms,
                    unique_key: row.unique_key.as_deref(),
                    progress: row.progress,
                    metadata: row.metadata.as_deref(),
                    notes: row.notes.as_deref(),
                    cancel_requested: row.cancel_requested,
                    expires_at: row.expires_at,
                    result_ttl_ms: row.result_ttl_ms,
                    namespace: row.namespace.as_deref(),
                };

                diesel::insert_into(archived_jobs::table)
                    .values(&archived)
                    .execute(conn)?;
                // Archived jobs keep their blobs inline in `archived_jobs`'s own
                // payload/result columns, copied above from the live row.
                diesel::delete(jobs::table.filter(jobs::id.eq(&row.id))).execute(conn)?;
                Ok(())
            }

            fn deps_satisfied(
                conn: &mut $conn_type,
                job_id: &str,
            ) -> diesel::result::QueryResult<bool> {
                let dep_job_ids: Vec<String> = job_dependencies::table
                    .filter(job_dependencies::job_id.eq(job_id))
                    .select(job_dependencies::depends_on_job_id)
                    .load(conn)?;

                if dep_job_ids.is_empty() {
                    return Ok(true);
                }

                // A dependency is incomplete if it is non-Complete in the live
                // `jobs` table OR non-Complete in `archived_jobs` (a terminal
                // parent that was cancelled/failed/dead has been archived and
                // must still block its dependents; an archived-Complete parent
                // is counted as satisfied).
                let live_incomplete: i64 = jobs::table
                    .filter(jobs::id.eq_any(&dep_job_ids))
                    .filter(jobs::status.ne(JobStatus::Complete as i32))
                    .count()
                    .get_result(conn)?;

                let archived_incomplete: i64 = archived_jobs::table
                    .filter(archived_jobs::id.eq_any(&dep_job_ids))
                    .filter(archived_jobs::status.ne(JobStatus::Complete as i32))
                    .count()
                    .get_result(conn)?;

                Ok(live_incomplete + archived_incomplete == 0)
            }

            /// Validate that a dependency exists and is in an acceptable state.
            /// A live (pending/running) or archived-complete dependency is
            /// accepted; a dead/cancelled/failed dependency or a missing one is
            /// rejected via `RollbackTransaction`. Terminal deps live in
            /// `archived_jobs`, so a missing live row falls back to the archive.
            fn validate_dependency(
                conn: &mut $conn_type,
                dep_id: &str,
            ) -> diesel::result::QueryResult<()> {
                let dep: Option<JobRow> = jobs::table
                    .find(dep_id)
                    .select(JobRow::as_select())
                    .first(conn)
                    .optional()?;

                match dep {
                    Some(d)
                        if d.status == JobStatus::Dead as i32
                            || d.status == JobStatus::Cancelled as i32 =>
                    {
                        Err(diesel::result::Error::RollbackTransaction)
                    }
                    Some(_) => Ok(()),
                    None => {
                        let archived: Option<ArchivedJobRow> = archived_jobs::table
                            .find(dep_id)
                            .select(ArchivedJobRow::as_select())
                            .first(conn)
                            .optional()?;

                        match archived {
                            Some(a) if a.status == JobStatus::Complete as i32 => Ok(()),
                            _ => Err(diesel::result::Error::RollbackTransaction),
                        }
                    }
                }
            }

            /// Insert a new job into the queue. Returns the job.
            pub fn enqueue(&self, new_job: NewJob) -> Result<Job> {
                let depends_on = new_job.depends_on.clone();
                let job = new_job.into_job();

                self.write_transaction(|conn| {
                    // Validate dependencies exist and aren't dead/cancelled.
                    // Terminal deps live in `archived_jobs`, so a missing live
                    // row falls back to the archive.
                    for dep_id in &depends_on {
                        Self::validate_dependency(conn, dep_id)?;
                    }

                    // Attribute pub/sub deliveries to their subscription so
                    // backlog stats index by it; `None` for ordinary jobs.
                    let (topic, subscription_name) =
                        $crate::pubsub::extract_topic_subscription(job.notes.as_deref())
                            .map_or((None, None), |(t, s)| (Some(t), Some(s)));
                    let row = NewJobRow {
                        id: &job.id,
                        queue: &job.queue,
                        task_name: &job.task_name,
                        payload: &job.payload,
                        status: job.status as i32,
                        priority: job.priority,
                        created_at: job.created_at,
                        scheduled_at: job.scheduled_at,
                        retry_count: job.retry_count,
                        max_retries: job.max_retries,
                        timeout_ms: job.timeout_ms,
                        unique_key: job.unique_key.as_deref(),
                        metadata: job.metadata.as_deref(),
                        notes: job.notes.as_deref(),
                        cancel_requested: 0,
                        expires_at: job.expires_at,
                        result_ttl_ms: job.result_ttl_ms,
                        namespace: job.namespace.as_deref(),
                        has_deps: job.has_deps,
                        topic: topic.as_deref(),
                        subscription_name: subscription_name.as_deref(),
                    };

                    diesel::insert_into(jobs::table)
                        .values(&row)
                        .execute(conn)?;

                    // Insert dependency rows
                    for dep_id in &depends_on {
                        let dep_row = NewJobDependencyRow {
                            id: &uuid::Uuid::now_v7().to_string(),
                            job_id: &job.id,
                            depends_on_job_id: dep_id,
                        };
                        diesel::insert_into(job_dependencies::table)
                            .values(&dep_row)
                            .execute(conn)?;
                    }

                    Ok(())
                })
                .map_err(|e| match e {
                    QueueError::Storage(diesel::result::Error::RollbackTransaction) => {
                        QueueError::DependencyNotFound(
                            "dependency not found or already dead/cancelled".to_string(),
                        )
                    }
                    other => other,
                })?;

                Ok(job)
            }

            /// Enqueue multiple jobs in a single transaction.
            pub fn enqueue_batch(&self, new_jobs: Vec<NewJob>) -> Result<Vec<Job>> {
                // Bound rows-per-INSERT so the bound-parameter count stays
                // under SQLite's 999 limit (NewJobRow has ~19 columns;
                // 50 * 19 < 999). Postgres tolerates far more, but one shared
                // chunk size keeps the macro-generated code identical.
                const BATCH_INSERT_CHUNK: usize = 50;

                let jobs: Vec<Job> = new_jobs.into_iter().map(|nj| nj.into_job()).collect();

                // Pre-compute subscription attribution so the owned strings
                // outlive the borrowing rows built below.
                let attribution: Vec<(Option<String>, Option<String>)> = jobs
                    .iter()
                    .map(|job| {
                        $crate::pubsub::extract_topic_subscription(job.notes.as_deref())
                            .map_or((None, None), |(t, s)| (Some(t), Some(s)))
                    })
                    .collect();

                self.write_transaction(|conn| {
                    let rows: Vec<NewJobRow> = jobs
                        .iter()
                        .zip(&attribution)
                        .map(|(job, (topic, subscription_name))| NewJobRow {
                            id: &job.id,
                            queue: &job.queue,
                            task_name: &job.task_name,
                            payload: &job.payload,
                            status: job.status as i32,
                            priority: job.priority,
                            created_at: job.created_at,
                            scheduled_at: job.scheduled_at,
                            retry_count: job.retry_count,
                            max_retries: job.max_retries,
                            timeout_ms: job.timeout_ms,
                            unique_key: job.unique_key.as_deref(),
                            metadata: job.metadata.as_deref(),
                            notes: job.notes.as_deref(),
                            cancel_requested: 0,
                            expires_at: job.expires_at,
                            result_ttl_ms: job.result_ttl_ms,
                            namespace: job.namespace.as_deref(),
                            has_deps: job.has_deps,
                            topic: topic.as_deref(),
                            subscription_name: subscription_name.as_deref(),
                        })
                        .collect();

                    // One multi-row INSERT per chunk instead of N single-row
                    // INSERTs — far fewer round trips / statement executions.
                    for chunk in rows.chunks(BATCH_INSERT_CHUNK) {
                        diesel::insert_into(jobs::table)
                            .values(chunk)
                            .execute(conn)?;
                    }
                    Ok(jobs)
                })
            }

            /// Enqueue with unique_key deduplication. Returns the existing active
            /// job on a duplicate, validates dependencies exactly like `enqueue`,
            /// and never returns a job whose insert was rolled back.
            pub fn enqueue_unique(&self, new_job: NewJob) -> Result<Job> {
                let depends_on = new_job.depends_on.clone();
                let job = new_job.into_job();

                // A UniqueViolation means a concurrent insert won the unique slot.
                // If that job is still active we return it; if it has since gone
                // terminal — freeing the partial unique index — we retry the
                // insert. Bound the attempts so persistent contention surfaces as
                // an error instead of a phantom job that was never persisted.
                const MAX_ENQUEUE_ATTEMPTS: usize = 3;
                for _ in 0..MAX_ENQUEUE_ATTEMPTS {
                    let result = self.write_transaction(|conn| {
                        // Return any existing active job with the same unique_key.
                        if let Some(ref uk) = job.unique_key {
                            let existing: Option<JobRow> =
                                jobs::table
                                    .filter(jobs::unique_key.eq(uk))
                                    .filter(jobs::status.eq_any([
                                        JobStatus::Pending as i32,
                                        JobStatus::Running as i32,
                                    ]))
                                    .select(JobRow::as_select())
                                    .first(conn)
                                    .optional()?;
                            if let Some(row) = existing {
                                return Ok(Job::from(row));
                            }
                        }

                        // Validate dependencies exist and aren't dead/cancelled,
                        // matching `enqueue` (RollbackTransaction → DependencyNotFound).
                        for dep_id in &depends_on {
                            Self::validate_dependency(conn, dep_id)?;
                        }

                        let (topic, subscription_name) =
                            $crate::pubsub::extract_topic_subscription(job.notes.as_deref())
                                .map_or((None, None), |(t, s)| (Some(t), Some(s)));
                        let row = NewJobRow {
                            id: &job.id,
                            queue: &job.queue,
                            task_name: &job.task_name,
                            payload: &job.payload,
                            status: job.status as i32,
                            priority: job.priority,
                            created_at: job.created_at,
                            scheduled_at: job.scheduled_at,
                            retry_count: job.retry_count,
                            max_retries: job.max_retries,
                            timeout_ms: job.timeout_ms,
                            unique_key: job.unique_key.as_deref(),
                            metadata: job.metadata.as_deref(),
                            notes: job.notes.as_deref(),
                            cancel_requested: 0,
                            expires_at: job.expires_at,
                            result_ttl_ms: job.result_ttl_ms,
                            namespace: job.namespace.as_deref(),
                            has_deps: job.has_deps,
                            topic: topic.as_deref(),
                            subscription_name: subscription_name.as_deref(),
                        };

                        diesel::insert_into(jobs::table)
                            .values(&row)
                            .execute(conn)?;

                        for dep_id in &depends_on {
                            let dep_row = NewJobDependencyRow {
                                id: &uuid::Uuid::now_v7().to_string(),
                                job_id: &job.id,
                                depends_on_job_id: dep_id,
                            };
                            diesel::insert_into(job_dependencies::table)
                                .values(&dep_row)
                                .execute(conn)?;
                        }

                        Ok(job.clone())
                    });

                    match result {
                        Ok(j) => return Ok(j),
                        Err(QueueError::Storage(diesel::result::Error::DatabaseError(
                            diesel::result::DatabaseErrorKind::UniqueViolation,
                            _,
                        ))) => {
                            // Concurrent winner: return it if still active, else the
                            // slot was freed by a terminal transition — retry insert.
                            // Acquire the connection here (not before the loop) so
                            // it never overlaps `write_transaction`'s own checkout —
                            // the in-memory test pool has a single connection.
                            if let Some(ref uk) = job.unique_key {
                                let mut conn = self.conn()?;
                                let existing: Option<JobRow> = jobs::table
                                    .filter(jobs::unique_key.eq(uk))
                                    .filter(jobs::status.eq_any([
                                        JobStatus::Pending as i32,
                                        JobStatus::Running as i32,
                                    ]))
                                    .select(JobRow::as_select())
                                    .first(&mut conn)
                                    .optional()?;
                                if let Some(row) = existing {
                                    return Ok(Job::from(row));
                                }
                            }
                            continue;
                        }
                        Err(QueueError::Storage(diesel::result::Error::RollbackTransaction)) => {
                            return Err(QueueError::DependencyNotFound(
                                "dependency not found or already dead/cancelled".to_string(),
                            ));
                        }
                        Err(e) => return Err(e),
                    }
                }

                Err(QueueError::Other(
                    "enqueue_unique: unique key contended across retries".to_string(),
                ))
            }

            /// Batch variant of `enqueue_unique`: dedupe-insert many jobs in a
            /// single transaction instead of one transaction per job. Used by
            /// pub/sub keyed fan-out, where a publish creates one uniquely-keyed
            /// job per subscriber — previously N separate write transactions
            /// (N database-wide write locks on SQLite). Within one call the
            /// salted keys are all distinct, so a `UniqueViolation` can only come
            /// from a concurrent publish; on one, the whole batch retries
            /// (bounded) and any concurrent winner is returned in place.
            pub fn enqueue_unique_batch(&self, new_jobs: Vec<NewJob>) -> Result<Vec<Job>> {
                const MAX_ENQUEUE_ATTEMPTS: usize = 3;

                // Precompute owned jobs, dependency lists, and pub/sub attribution
                // once so the per-attempt closure only borrows.
                type Prepared = (Job, Vec<String>, Option<String>, Option<String>);
                let prepared: Vec<Prepared> = new_jobs
                    .into_iter()
                    .map(|nj| {
                        let depends_on = nj.depends_on.clone();
                        let job = nj.into_job();
                        let (topic, subscription_name) =
                            $crate::pubsub::extract_topic_subscription(job.notes.as_deref())
                                .map_or((None, None), |(t, s)| (Some(t), Some(s)));
                        (job, depends_on, topic, subscription_name)
                    })
                    .collect();

                for _ in 0..MAX_ENQUEUE_ATTEMPTS {
                    let result = self.write_transaction(|conn| {
                        let mut out = Vec::with_capacity(prepared.len());
                        for (job, depends_on, topic, subscription_name) in &prepared {
                            // Return any existing active job with the same key.
                            if let Some(ref uk) = job.unique_key {
                                let existing: Option<JobRow> = jobs::table
                                    .filter(jobs::unique_key.eq(uk))
                                    .filter(jobs::status.eq_any([
                                        JobStatus::Pending as i32,
                                        JobStatus::Running as i32,
                                    ]))
                                    .select(JobRow::as_select())
                                    .first(conn)
                                    .optional()?;
                                if let Some(row) = existing {
                                    out.push(Job::from(row));
                                    continue;
                                }
                            }

                            for dep_id in depends_on {
                                Self::validate_dependency(conn, dep_id)?;
                            }

                            let row = NewJobRow {
                                id: &job.id,
                                queue: &job.queue,
                                task_name: &job.task_name,
                                payload: &job.payload,
                                status: job.status as i32,
                                priority: job.priority,
                                created_at: job.created_at,
                                scheduled_at: job.scheduled_at,
                                retry_count: job.retry_count,
                                max_retries: job.max_retries,
                                timeout_ms: job.timeout_ms,
                                unique_key: job.unique_key.as_deref(),
                                metadata: job.metadata.as_deref(),
                                notes: job.notes.as_deref(),
                                cancel_requested: 0,
                                expires_at: job.expires_at,
                                result_ttl_ms: job.result_ttl_ms,
                                namespace: job.namespace.as_deref(),
                                has_deps: job.has_deps,
                                topic: topic.as_deref(),
                                subscription_name: subscription_name.as_deref(),
                            };
                            diesel::insert_into(jobs::table)
                                .values(&row)
                                .execute(conn)?;

                            for dep_id in depends_on {
                                let dep_row = NewJobDependencyRow {
                                    id: &uuid::Uuid::now_v7().to_string(),
                                    job_id: &job.id,
                                    depends_on_job_id: dep_id,
                                };
                                diesel::insert_into(job_dependencies::table)
                                    .values(&dep_row)
                                    .execute(conn)?;
                            }

                            out.push(job.clone());
                        }
                        Ok(out)
                    });

                    match result {
                        Ok(v) => return Ok(v),
                        Err(QueueError::Storage(diesel::result::Error::DatabaseError(
                            diesel::result::DatabaseErrorKind::UniqueViolation,
                            _,
                        ))) => continue,
                        Err(QueueError::Storage(diesel::result::Error::RollbackTransaction)) => {
                            return Err(QueueError::DependencyNotFound(
                                "dependency not found or already dead/cancelled".to_string(),
                            ));
                        }
                        Err(e) => return Err(e),
                    }
                }

                Err(QueueError::Other(
                    "enqueue_unique_batch: unique key contended across retries".to_string(),
                ))
            }

            /// Atomically dequeue the highest-priority ready job from the given queue.
            /// Skips expired jobs. When `namespace` is `Some`, only jobs in that
            /// namespace are considered; when `None`, only jobs with no namespace.
            pub fn dequeue(
                &self,
                queue_name: &str,
                now: i64,
                namespace: Option<&str>,
            ) -> Result<Option<Job>> {
                self.dequeue_ordered(
                    queue_name,
                    now,
                    namespace,
                    $crate::storage::DispatchOrder::Fifo,
                )
            }

            /// [`dequeue`](Self::dequeue) with an explicit dispatch order — the
            /// per-queue path used by [`dequeue_from`](Self::dequeue_from).
            pub fn dequeue_ordered(
                &self,
                queue_name: &str,
                now: i64,
                namespace: Option<&str>,
                order: $crate::storage::DispatchOrder,
            ) -> Result<Option<Job>> {
                self.write_transaction(|conn| {
                    // Narrow candidate scan (no payload/result blobs). Postgres
                    // applies FOR UPDATE SKIP LOCKED so concurrent workers claim
                    // disjoint rows; SQLite serializes writers via BEGIN IMMEDIATE.
                    let candidates: Vec<NarrowJobRow> = Self::scan_dequeue_candidates(
                        conn, queue_name, now, namespace, 100, order,
                    )?;

                    for row in candidates {
                        // Skip expired jobs — archive them as cancelled. The
                        // archived row keeps the payload, so load the full row
                        // (only for this one expired candidate) before archiving.
                        if let Some(expires_at) = row.expires_at {
                            if now > expires_at {
                                let mut full: JobRow = jobs::table
                                    .find(&row.id)
                                    .select(JobRow::as_select())
                                    .first(conn)?;
                                full.status = JobStatus::Cancelled as i32;
                                full.completed_at = Some(now);
                                full.error = Some("expired before execution".to_string());
                                Self::archive_job_row(conn, &full)?;
                                continue;
                            }
                        }

                        // Common case: jobs with no dependencies skip the
                        // job_dependencies lookup entirely.
                        if row.has_deps && !Self::deps_satisfied(conn, &row.id)? {
                            continue;
                        }

                        // Claim guarded by the affected-row count: if another
                        // worker already moved this row out of `Pending`, the
                        // update touches zero rows — skip it rather than
                        // handing out a job we don't own.
                        let claimed = diesel::update(jobs::table)
                            .filter(jobs::id.eq(&row.id))
                            .filter(jobs::status.eq(JobStatus::Pending as i32))
                            .set((
                                jobs::status.eq(JobStatus::Running as i32),
                                jobs::started_at.eq(now),
                            ))
                            .execute(conn)?;

                        if claimed == 0 {
                            continue;
                        }

                        // Load the full winning row (with blobs) inline and
                        // assemble the Job. Only the one claimed row reads blobs.
                        let updated: JobRow = jobs::table
                            .find(&row.id)
                            .select(JobRow::as_select())
                            .first(conn)?;

                        return Ok(Some(Job::from(updated)));
                    }

                    Ok(None)
                })
            }

            /// Dequeue from multiple queues, checking each in order. Each queue
            /// is scanned with its own dispatch order (`orders`, default Fifo).
            pub fn dequeue_from(
                &self,
                queues: &[String],
                now: i64,
                namespace: Option<&str>,
                orders: &std::collections::HashMap<String, $crate::storage::DispatchOrder>,
            ) -> Result<Option<Job>> {
                for queue_name in queues {
                    let order = $crate::storage::order_for(orders, queue_name);
                    if let Some(job) = self.dequeue_ordered(queue_name, now, namespace, order)? {
                        return Ok(Some(job));
                    }
                }
                Ok(None)
            }

            /// Atomically claim up to `max` ready jobs from a single queue in
            /// one transaction. Generalizes `dequeue` to the batch case: scans
            /// a bounded candidate set and claims eligible rows until the
            /// budget is met or candidates are exhausted.
            ///
            /// Each claim uses an `UPDATE ... WHERE status = Pending` guarded
            /// by the affected-row count: if another worker already moved the
            /// row out of `Pending`, the update affects zero rows and the
            /// candidate is skipped — avoiding a double-claim race.
            pub fn dequeue_batch(
                &self,
                queue_name: &str,
                now: i64,
                namespace: Option<&str>,
                max: usize,
            ) -> Result<Vec<Job>> {
                self.dequeue_batch_ordered(
                    queue_name,
                    now,
                    namespace,
                    max,
                    $crate::storage::DispatchOrder::Fifo,
                )
            }

            /// [`dequeue_batch`](Self::dequeue_batch) with an explicit dispatch
            /// order — the per-queue path used by
            /// [`dequeue_batch_from`](Self::dequeue_batch_from).
            pub fn dequeue_batch_ordered(
                &self,
                queue_name: &str,
                now: i64,
                namespace: Option<&str>,
                max: usize,
                order: $crate::storage::DispatchOrder,
            ) -> Result<Vec<Job>> {
                if max == 0 {
                    return Ok(Vec::new());
                }

                // Scan more candidates than `max` so dependency/expiry skips
                // still leave enough eligible rows to fill the batch, bounded
                // to keep the loaded set small.
                let scan_limit = (max.saturating_mul(4)).min(400) as i64;

                self.write_transaction(|conn| {
                    // Narrow candidate scan, identical to `dequeue` (no blobs;
                    // Postgres applies FOR UPDATE SKIP LOCKED). Only claimed
                    // winners load their payload inline below.
                    let candidates: Vec<NarrowJobRow> = Self::scan_dequeue_candidates(
                        conn, queue_name, now, namespace, scan_limit, order,
                    )?;

                    let mut claimed: Vec<Job> = Vec::with_capacity(max.min(candidates.len()));

                    for row in candidates {
                        if claimed.len() == max {
                            break;
                        }

                        // Skip expired jobs — archive them as cancelled so they
                        // leave the live `jobs` table (matching `dequeue`).
                        if let Some(expires_at) = row.expires_at {
                            if now > expires_at {
                                let mut full: JobRow = jobs::table
                                    .find(&row.id)
                                    .select(JobRow::as_select())
                                    .first(conn)?;
                                full.status = JobStatus::Cancelled as i32;
                                full.completed_at = Some(now);
                                full.error = Some("expired before execution".to_string());
                                Self::archive_job_row(conn, &full)?;
                                continue;
                            }
                        }

                        // Common case: jobs with no dependencies skip the
                        // job_dependencies lookup entirely.
                        if row.has_deps && !Self::deps_satisfied(conn, &row.id)? {
                            continue;
                        }

                        // Claim guarded by the affected-row count: if another
                        // worker already moved this row out of `Pending`, the
                        // update touches zero rows — skip it rather than
                        // claiming a job we don't own.
                        let affected = diesel::update(jobs::table)
                            .filter(jobs::id.eq(&row.id))
                            .filter(jobs::status.eq(JobStatus::Pending as i32))
                            .set((
                                jobs::status.eq(JobStatus::Running as i32),
                                jobs::started_at.eq(now),
                            ))
                            .execute(conn)?;

                        if affected == 0 {
                            continue;
                        }

                        let updated: JobRow = jobs::table
                            .find(&row.id)
                            .select(JobRow::as_select())
                            .first(conn)?;

                        claimed.push(Job::from(updated));
                    }

                    Ok(claimed)
                })
            }

            /// Claim up to `max` ready jobs across the given queues, checking
            /// each in order until the budget is exhausted. Each queue uses its
            /// own dispatch order (`orders`, default Fifo).
            pub fn dequeue_batch_from(
                &self,
                queues: &[String],
                now: i64,
                namespace: Option<&str>,
                max: usize,
                orders: &std::collections::HashMap<String, $crate::storage::DispatchOrder>,
            ) -> Result<Vec<Job>> {
                let mut claimed: Vec<Job> = Vec::new();
                for queue_name in queues {
                    if claimed.len() >= max {
                        break;
                    }
                    let remaining = max - claimed.len();
                    let order = $crate::storage::order_for(orders, queue_name);
                    let mut batch =
                        self.dequeue_batch_ordered(queue_name, now, namespace, remaining, order)?;
                    claimed.append(&mut batch);
                }
                Ok(claimed)
            }

            /// Mark a job as complete with the given result. The job moves from
            /// `jobs` into `archived_jobs` in a single transaction.
            pub fn complete(&self, id: &str, result_bytes: Option<Vec<u8>>) -> Result<()> {
                let now = now_millis();

                self.write_transaction(|conn| {
                    let mut row: JobRow = match jobs::table
                        .find(id)
                        .filter(jobs::status.eq(JobStatus::Running as i32))
                        .select(JobRow::as_select())
                        .first(conn)
                        .optional()?
                    {
                        Some(row) => row,
                        None => return Err(QueueError::JobNotFound(id.to_string())),
                    };

                    row.status = JobStatus::Complete as i32;
                    row.completed_at = Some(now);
                    row.result = result_bytes;
                    Self::archive_job_row(conn, &row)?;
                    Ok(())
                })
            }

            /// Persist many successful completions in one transaction. Per job
            /// this does exactly what the success path did one-at-a-time —
            /// archive the completed row, clear its execution claim, record its
            /// metric — but coalesces what were three writes × N jobs across N
            /// transactions into a single commit (one fsync). If any job is not
            /// `Running` the whole batch rolls back with `JobNotFound`, so the
            /// caller can fall back to per-job handling.
            pub fn complete_batch(&self, completions: &[$crate::job::JobCompletion]) -> Result<()> {
                if completions.is_empty() {
                    return Ok(());
                }
                let now = now_millis();

                self.write_transaction(|conn| {
                    for c in completions {
                        let mut row: JobRow = match jobs::table
                            .find(&c.job_id)
                            .filter(jobs::status.eq(JobStatus::Running as i32))
                            .select(JobRow::as_select())
                            .first(conn)
                            .optional()?
                        {
                            Some(row) => row,
                            None => return Err(QueueError::JobNotFound(c.job_id.clone())),
                        };

                        row.status = JobStatus::Complete as i32;
                        row.completed_at = Some(now);
                        row.result = c.result.clone();
                        Self::archive_job_row(conn, &row)?;

                        diesel::delete(
                            execution_claims::table.filter(execution_claims::job_id.eq(&c.job_id)),
                        )
                        .execute(conn)?;

                        let metric_id = uuid::Uuid::now_v7().to_string();
                        diesel::insert_into(task_metrics::table)
                            .values(&NewTaskMetricRow {
                                id: &metric_id,
                                task_name: &c.task_name,
                                job_id: &c.job_id,
                                wall_time_ns: c.wall_time_ns,
                                memory_bytes: 0,
                                succeeded: true,
                                recorded_at: now,
                                // Taken from the job itself, which is the only
                                // place this batch knows a namespace from.
                                namespace: row.namespace.as_deref(),
                            })
                            .execute(conn)?;
                    }
                    Ok(())
                })
            }

            /// Mark a job as failed with the given error message. The job moves
            /// from `jobs` into `archived_jobs` in a single transaction.
            pub fn fail(&self, id: &str, error: &str) -> Result<()> {
                let now = now_millis();

                self.write_transaction(|conn| {
                    let mut row: JobRow = match jobs::table
                        .find(id)
                        .filter(jobs::status.eq(JobStatus::Running as i32))
                        .select(JobRow::as_select())
                        .first(conn)
                        .optional()?
                    {
                        Some(row) => row,
                        None => return Err(QueueError::JobNotFound(id.to_string())),
                    };

                    row.status = JobStatus::Failed as i32;
                    row.completed_at = Some(now);
                    row.error = Some(error.to_string());
                    Self::archive_job_row(conn, &row)?;
                    Ok(())
                })
            }

            /// Re-schedule a job for retry.
            pub fn retry(&self, id: &str, next_scheduled_at: i64) -> Result<()> {
                let mut conn = self.conn()?;

                let affected = diesel::update(jobs::table)
                    .filter(jobs::id.eq(id))
                    .set((
                        jobs::status.eq(JobStatus::Pending as i32),
                        jobs::scheduled_at.eq(next_scheduled_at),
                        jobs::retry_count.eq(jobs::retry_count + 1),
                        jobs::started_at.eq(None::<i64>),
                        jobs::completed_at.eq(None::<i64>),
                        jobs::error.eq(None::<String>),
                    ))
                    .execute(&mut conn)?;

                if affected == 0 {
                    return Err(QueueError::JobNotFound(id.to_string()));
                }
                Ok(())
            }

            /// Re-schedule a job back to `Pending` without touching
            /// `retry_count`. Mirrors [`retry`](Self::retry) for soft-gate
            /// reschedules (rate limit, circuit breaker, concurrency cap,
            /// backpressure) where the job never executed, so its retry
            /// budget must be preserved.
            pub fn reschedule(&self, id: &str, next_scheduled_at: i64) -> Result<()> {
                let mut conn = self.conn()?;

                let affected = diesel::update(jobs::table)
                    .filter(jobs::id.eq(id))
                    .set((
                        jobs::status.eq(JobStatus::Pending as i32),
                        jobs::scheduled_at.eq(next_scheduled_at),
                        jobs::started_at.eq(None::<i64>),
                        jobs::completed_at.eq(None::<i64>),
                        jobs::error.eq(None::<String>),
                    ))
                    .execute(&mut conn)?;

                if affected == 0 {
                    return Err(QueueError::JobNotFound(id.to_string()));
                }
                Ok(())
            }

            /// Force a `Running` job back to `Pending` and delete its
            /// execution claim in one transaction. The status filter gates
            /// the operation (missing / not-Running rows update nothing);
            /// the claim must be deleted, not transferred, because
            /// `claim_execution` is insert-only and a leftover row would
            /// block the next worker's claim. Clearing `cancel_requested`
            /// keeps a stale cancel request from killing the fresh attempt.
            pub fn requeue_stuck(&self, id: &str, now: i64) -> Result<bool> {
                self.write_transaction(|conn| {
                    let affected = diesel::update(jobs::table)
                        .filter(jobs::id.eq(id))
                        .filter(jobs::status.eq(JobStatus::Running as i32))
                        .set((
                            jobs::status.eq(JobStatus::Pending as i32),
                            jobs::scheduled_at.eq(now),
                            jobs::started_at.eq(None::<i64>),
                            jobs::completed_at.eq(None::<i64>),
                            jobs::error.eq(None::<String>),
                            jobs::cancel_requested.eq(0),
                        ))
                        .execute(conn)?;
                    if affected == 0 {
                        return Ok(false);
                    }
                    diesel::delete(execution_claims::table.filter(execution_claims::job_id.eq(id)))
                        .execute(conn)?;
                    Ok(true)
                })
            }

            /// Cancel a pending job and cascade-cancel its dependents. The
            /// cancelled job moves from `jobs` into `archived_jobs`.
            ///
            /// A job in another namespace reports `false`, the same answer an
            /// unknown or already-terminal id gets: a caller scoped to one
            /// tenant learns nothing about ids outside it.
            pub fn cancel_job(&self, id: &str, namespace: Option<&str>) -> Result<bool> {
                let now = now_millis();

                let archived = self.write_transaction(|conn| {
                    let mut row: JobRow = match jobs::table
                        .find(id)
                        .filter(jobs::status.eq(JobStatus::Pending as i32))
                        .select(JobRow::as_select())
                        .first(conn)
                        .optional()?
                    {
                        Some(row) => row,
                        None => return Ok(false),
                    };

                    if namespace.is_some_and(|scope| row.namespace.as_deref() != Some(scope)) {
                        return Ok(false);
                    }

                    row.status = JobStatus::Cancelled as i32;
                    row.completed_at = Some(now);
                    Self::archive_job_row(conn, &row)?;
                    Ok(true)
                })?;

                if archived {
                    self.cascade_cancel(id, "dependency cancelled")?;
                }

                Ok(archived)
            }

            /// Request cancellation of a running job. The task must check for this.
            pub fn request_cancel(&self, id: &str) -> Result<bool> {
                let mut conn = self.conn()?;

                let affected = diesel::update(jobs::table)
                    .filter(jobs::id.eq(id))
                    .filter(jobs::status.eq(JobStatus::Running as i32))
                    .set(jobs::cancel_requested.eq(1))
                    .execute(&mut conn)?;

                Ok(affected > 0)
            }

            /// Check if cancellation has been requested for a job.
            pub fn is_cancel_requested(&self, id: &str) -> Result<bool> {
                let mut conn = self.conn()?;

                let val: Option<i32> = jobs::table
                    .find(id)
                    .select(jobs::cancel_requested)
                    .first(&mut conn)
                    .optional()?;

                Ok(val.unwrap_or(0) != 0)
            }

            /// Mark a job as cancelled (used when a running job detects
            /// cancellation). The job moves from `jobs` into `archived_jobs`.
            pub fn mark_cancelled(&self, id: &str) -> Result<()> {
                let now = now_millis();

                self.write_transaction(|conn| {
                    let mut row: JobRow = match jobs::table
                        .find(id)
                        .select(JobRow::as_select())
                        .first(conn)
                        .optional()?
                    {
                        Some(row) => row,
                        None => return Ok(()),
                    };

                    row.status = JobStatus::Cancelled as i32;
                    row.completed_at = Some(now);
                    row.error = Some("cancelled by request".to_string());
                    Self::archive_job_row(conn, &row)?;
                    Ok(())
                })
            }

            /// Cascade-cancel all pending jobs that depend (directly or transitively)
            /// on the given job. Uses BFS to handle deep chains.
            pub fn cascade_cancel(&self, failed_job_id: &str, reason: &str) -> Result<()> {
                let mut conn = self.conn()?;
                let now = now_millis();

                let mut queue: Vec<String> = vec![failed_job_id.to_string()];
                let mut visited = std::collections::HashSet::new();
                visited.insert(failed_job_id.to_string());
                let mut idx = 0;

                while idx < queue.len() {
                    let current_id = queue[idx].clone();
                    idx += 1;

                    let dependents: Vec<String> = job_dependencies::table
                        .filter(job_dependencies::depends_on_job_id.eq(&current_id))
                        .select(job_dependencies::job_id)
                        .load(&mut conn)?;

                    for dep_id in dependents {
                        if visited.insert(dep_id.clone()) {
                            queue.push(dep_id);
                        }
                    }
                }

                // Remove the original job from the list (only cancel dependents)
                if !queue.is_empty() {
                    queue.remove(0);
                }
                // Release the BFS connection before the write transaction grabs
                // its own (single-connection pools would otherwise deadlock).
                drop(conn);

                if !queue.is_empty() {
                    let error_msg = format!("{reason}: {failed_job_id}");
                    self.write_transaction(|conn| {
                        let rows: Vec<JobRow> = jobs::table
                            .filter(jobs::id.eq_any(&queue))
                            .filter(jobs::status.eq(JobStatus::Pending as i32))
                            .select(JobRow::as_select())
                            .load(conn)?;

                        for mut row in rows {
                            row.status = JobStatus::Cancelled as i32;
                            row.completed_at = Some(now);
                            row.error = Some(error_msg.clone());
                            Self::archive_job_row(conn, &row)?;
                        }
                        Ok(())
                    })?;
                }

                Ok(())
            }

            /// Get the IDs of jobs that a given job depends on.
            pub fn get_dependencies(&self, job_id: &str) -> Result<Vec<String>> {
                let mut conn = self.conn()?;
                let ids: Vec<String> = job_dependencies::table
                    .filter(job_dependencies::job_id.eq(job_id))
                    .select(job_dependencies::depends_on_job_id)
                    .load(&mut conn)?;
                Ok(ids)
            }

            /// Get the IDs of jobs that depend on a given job.
            pub fn get_dependents(&self, job_id: &str) -> Result<Vec<String>> {
                let mut conn = self.conn()?;
                let ids: Vec<String> = job_dependencies::table
                    .filter(job_dependencies::depends_on_job_id.eq(job_id))
                    .select(job_dependencies::job_id)
                    .load(&mut conn)?;
                Ok(ids)
            }

            /// Update progress for a running job (0-100).
            pub fn update_progress(&self, id: &str, progress: i32) -> Result<()> {
                if !(0..=100).contains(&progress) {
                    return Err(QueueError::Other(
                        "progress must be between 0 and 100".into(),
                    ));
                }
                let mut conn = self.conn()?;

                let affected = diesel::update(jobs::table)
                    .filter(jobs::id.eq(id))
                    .set(jobs::progress.eq(progress))
                    .execute(&mut conn)?;

                if affected == 0 {
                    return Err(QueueError::JobNotFound(id.to_string()));
                }
                Ok(())
            }

            /// List jobs with optional filters and pagination.
            /// When `namespace` is `Some`, only jobs in that namespace are returned.
            /// When `None`, all jobs are returned regardless of namespace.
            pub fn list_jobs(
                &self,
                status: Option<i32>,
                queue_name: Option<&str>,
                task_name: Option<&str>,
                limit: i64,
                offset: i64,
                namespace: Option<&str>,
            ) -> Result<Vec<Job>> {
                self.list_jobs_filtered(
                    status, queue_name, task_name, None, None, None, None, limit, offset, namespace,
                )
            }

            /// Keyset-paginated `list_jobs` — see `list_jobs_filtered_after`.
            pub fn list_jobs_after(
                &self,
                status: Option<i32>,
                queue_name: Option<&str>,
                task_name: Option<&str>,
                limit: i64,
                after: Option<(i64, &str)>,
                namespace: Option<&str>,
            ) -> Result<Vec<Job>> {
                self.list_jobs_filtered_after(
                    status, queue_name, task_name, None, None, None, None, limit, after, namespace,
                )
            }

            /// True when `status` is a terminal status whose rows now live in
            /// `archived_jobs` rather than the live `jobs` table.
            fn is_terminal_status(status: i32) -> bool {
                matches!(
                    JobStatus::from_i32(status),
                    Some(JobStatus::Complete)
                        | Some(JobStatus::Failed)
                        | Some(JobStatus::Dead)
                        | Some(JobStatus::Cancelled)
                )
            }

            /// Get a job by ID. Checks the live `jobs` table first, then falls
            /// back to `archived_jobs` for terminal jobs.
            pub fn get_job(&self, id: &str) -> Result<Option<Job>> {
                let mut conn = self.conn()?;

                let row: Option<JobRow> = jobs::table
                    .find(id)
                    .select(JobRow::as_select())
                    .first(&mut conn)
                    .optional()?;

                if let Some(jobrow) = row {
                    return Ok(Some(Job::from(jobrow)));
                }

                let archived: Option<ArchivedJobRow> = archived_jobs::table
                    .find(id)
                    .select(ArchivedJobRow::as_select())
                    .first(&mut conn)
                    .optional()?;

                Ok(archived.map(Job::from))
            }

            /// Get queue statistics. Pending/Running counts come from the live
            /// `jobs` table; terminal counts come from `archived_jobs`.
            /// `namespace` of `None` counts every namespace, matching
            /// `list_jobs`.
            pub fn stats(&self, namespace: Option<&str>) -> Result<QueueStats> {
                let mut conn = self.conn()?;

                // Written out per branch rather than boxed: Diesel's boxed
                // queries have no `GROUP BY`, and the unscoped shape must stay
                // exactly the single grouped scan it has always been.
                let live_rows: Vec<(i32, i64)> = match namespace {
                    Some(ns) => jobs::table
                        .filter(jobs::namespace.eq(ns))
                        .group_by(jobs::status)
                        .select((jobs::status, diesel::dsl::count(jobs::id)))
                        .load(&mut conn)?,
                    None => jobs::table
                        .group_by(jobs::status)
                        .select((jobs::status, diesel::dsl::count(jobs::id)))
                        .load(&mut conn)?,
                };

                let archived_rows: Vec<(i32, i64)> = match namespace {
                    Some(ns) => archived_jobs::table
                        .filter(archived_jobs::namespace.eq(ns))
                        .group_by(archived_jobs::status)
                        .select((archived_jobs::status, diesel::dsl::count(archived_jobs::id)))
                        .load(&mut conn)?,
                    None => archived_jobs::table
                        .group_by(archived_jobs::status)
                        .select((archived_jobs::status, diesel::dsl::count(archived_jobs::id)))
                        .load(&mut conn)?,
                };

                let mut stats = QueueStats::default();
                Self::apply_status_count(&mut stats, live_rows);
                Self::apply_status_count(&mut stats, archived_rows);
                Ok(stats)
            }

            /// Get queue statistics for a specific queue. Pending/Running counts
            /// come from `jobs`; terminal counts come from `archived_jobs`.
            pub fn stats_by_queue(
                &self,
                queue_name: &str,
                namespace: Option<&str>,
            ) -> Result<QueueStats> {
                let mut conn = self.conn()?;

                let live_rows: Vec<(i32, i64)> = match namespace {
                    Some(ns) => jobs::table
                        .filter(jobs::queue.eq(queue_name))
                        .filter(jobs::namespace.eq(ns))
                        .group_by(jobs::status)
                        .select((jobs::status, diesel::dsl::count(jobs::id)))
                        .load(&mut conn)?,
                    None => jobs::table
                        .filter(jobs::queue.eq(queue_name))
                        .group_by(jobs::status)
                        .select((jobs::status, diesel::dsl::count(jobs::id)))
                        .load(&mut conn)?,
                };

                let archived_rows: Vec<(i32, i64)> = match namespace {
                    Some(ns) => archived_jobs::table
                        .filter(archived_jobs::queue.eq(queue_name))
                        .filter(archived_jobs::namespace.eq(ns))
                        .group_by(archived_jobs::status)
                        .select((archived_jobs::status, diesel::dsl::count(archived_jobs::id)))
                        .load(&mut conn)?,
                    None => archived_jobs::table
                        .filter(archived_jobs::queue.eq(queue_name))
                        .group_by(archived_jobs::status)
                        .select((archived_jobs::status, diesel::dsl::count(archived_jobs::id)))
                        .load(&mut conn)?,
                };

                let mut stats = QueueStats::default();
                Self::apply_status_count(&mut stats, live_rows);
                Self::apply_status_count(&mut stats, archived_rows);
                Ok(stats)
            }

            /// Get queue statistics broken down by queue name. Pending/Running
            /// counts come from `jobs`; terminal counts from `archived_jobs`.
            pub fn stats_all_queues(
                &self,
                namespace: Option<&str>,
            ) -> Result<std::collections::HashMap<String, QueueStats>> {
                let mut conn = self.conn()?;

                let live_rows: Vec<(String, i32, i64)> = match namespace {
                    Some(ns) => jobs::table
                        .filter(jobs::namespace.eq(ns))
                        .group_by((jobs::queue, jobs::status))
                        .select((jobs::queue, jobs::status, diesel::dsl::count(jobs::id)))
                        .load(&mut conn)?,
                    None => jobs::table
                        .group_by((jobs::queue, jobs::status))
                        .select((jobs::queue, jobs::status, diesel::dsl::count(jobs::id)))
                        .load(&mut conn)?,
                };

                let archived_rows: Vec<(String, i32, i64)> = match namespace {
                    Some(ns) => archived_jobs::table
                        .filter(archived_jobs::namespace.eq(ns))
                        .group_by((archived_jobs::queue, archived_jobs::status))
                        .select((
                            archived_jobs::queue,
                            archived_jobs::status,
                            diesel::dsl::count(archived_jobs::id),
                        ))
                        .load(&mut conn)?,
                    None => archived_jobs::table
                        .group_by((archived_jobs::queue, archived_jobs::status))
                        .select((
                            archived_jobs::queue,
                            archived_jobs::status,
                            diesel::dsl::count(archived_jobs::id),
                        ))
                        .load(&mut conn)?,
                };

                let mut map = std::collections::HashMap::<String, QueueStats>::new();
                for (queue, status, count) in live_rows.into_iter().chain(archived_rows) {
                    let stats = map.entry(queue).or_default();
                    Self::set_status_count(stats, status, count);
                }

                Ok(map)
            }

            /// Merge a `(status, count)` GROUP BY result into a `QueueStats`.
            fn apply_status_count(stats: &mut QueueStats, rows: Vec<(i32, i64)>) {
                for (status, count) in rows {
                    Self::set_status_count(stats, status, count);
                }
            }

            /// Assign a per-status count into the matching `QueueStats` field.
            fn set_status_count(stats: &mut QueueStats, status: i32, count: i64) {
                match JobStatus::from_i32(status) {
                    Some(JobStatus::Pending) => stats.pending = count,
                    Some(JobStatus::Running) => stats.running = count,
                    Some(JobStatus::Complete) => stats.completed = count,
                    Some(JobStatus::Failed) => stats.failed = count,
                    Some(JobStatus::Dead) => stats.dead = count,
                    Some(JobStatus::Cancelled) => stats.cancelled = count,
                    None => {}
                }
            }

            /// List jobs with extended filters.
            /// When `namespace` is `Some`, only jobs in that namespace are returned.
            /// When `None`, all jobs are returned regardless of namespace.
            #[allow(clippy::too_many_arguments)]
            pub fn list_jobs_filtered(
                &self,
                status: Option<i32>,
                queue_name: Option<&str>,
                task_name: Option<&str>,
                metadata_like: Option<&str>,
                error_like: Option<&str>,
                created_after: Option<i64>,
                created_before: Option<i64>,
                limit: i64,
                offset: i64,
                namespace: Option<&str>,
            ) -> Result<Vec<Job>> {
                // Terminal statuses now live in `archived_jobs`; live statuses in
                // `jobs`. With no status filter, both tables are merged.
                match status {
                    Some(s) if Self::is_terminal_status(s) => self.list_archived_filtered(
                        Some(s),
                        queue_name,
                        task_name,
                        metadata_like,
                        error_like,
                        created_after,
                        created_before,
                        limit,
                        offset,
                        namespace,
                    ),
                    Some(_) => self.list_live_filtered(
                        status,
                        queue_name,
                        task_name,
                        metadata_like,
                        error_like,
                        created_after,
                        created_before,
                        limit,
                        offset,
                        namespace,
                    ),
                    None => {
                        // Fetch enough from each table to satisfy limit+offset,
                        // then merge by created_at desc and paginate in memory.
                        let take = limit.saturating_add(offset).max(0);
                        let mut live = self.list_live_filtered(
                            None,
                            queue_name,
                            task_name,
                            metadata_like,
                            error_like,
                            created_after,
                            created_before,
                            take,
                            0,
                            namespace,
                        )?;
                        let archived = self.list_archived_filtered(
                            None,
                            queue_name,
                            task_name,
                            metadata_like,
                            error_like,
                            created_after,
                            created_before,
                            take,
                            0,
                            namespace,
                        )?;
                        live.extend(archived);
                        live.sort_by_key(|j| std::cmp::Reverse(j.created_at));

                        let start = (offset.max(0) as usize).min(live.len());
                        let end = start.saturating_add(limit.max(0) as usize).min(live.len());
                        Ok(live[start..end].to_vec())
                    }
                }
            }

            /// Keyset-paginated `list_jobs_filtered`, ordered by
            /// `(created_at, id)` descending. Additive twin of the offset form.
            #[allow(clippy::too_many_arguments)]
            pub fn list_jobs_filtered_after(
                &self,
                status: Option<i32>,
                queue_name: Option<&str>,
                task_name: Option<&str>,
                metadata_like: Option<&str>,
                error_like: Option<&str>,
                created_after: Option<i64>,
                created_before: Option<i64>,
                limit: i64,
                after: Option<(i64, &str)>,
                namespace: Option<&str>,
            ) -> Result<Vec<Job>> {
                // A non-positive limit yields no page on every backend. SQLite
                // reads a negative LIMIT as unbounded, which would turn a paged
                // call into a full scan.
                if limit <= 0 {
                    return Ok(Vec::new());
                }
                match status {
                    Some(s) if Self::is_terminal_status(s) => self.list_archived_filtered_after(
                        Some(s),
                        queue_name,
                        task_name,
                        metadata_like,
                        error_like,
                        created_after,
                        created_before,
                        limit,
                        after,
                        namespace,
                    ),
                    Some(_) => self.list_live_filtered_after(
                        status,
                        queue_name,
                        task_name,
                        metadata_like,
                        error_like,
                        created_after,
                        created_before,
                        limit,
                        after,
                        namespace,
                    ),
                    None => {
                        // Ask each table for its own top `limit` under the SAME
                        // cursor. Any row in the true merged top-`limit` has at
                        // most `limit-1` rows from its own table ahead of it, so
                        // it is already in that table's top-`limit` — no offset
                        // compensation is needed.
                        let mut live = self.list_live_filtered_after(
                            None,
                            queue_name,
                            task_name,
                            metadata_like,
                            error_like,
                            created_after,
                            created_before,
                            limit,
                            after,
                            namespace,
                        )?;
                        let archived = self.list_archived_filtered_after(
                            None,
                            queue_name,
                            task_name,
                            metadata_like,
                            error_like,
                            created_after,
                            created_before,
                            limit,
                            after,
                            namespace,
                        )?;
                        live.extend(archived);
                        // Same `(created_at, id)` descending order the SQL uses,
                        // so the merged cursor advances monotonically.
                        live.sort_by(|a, b| (b.created_at, &b.id).cmp(&(a.created_at, &a.id)));
                        live.truncate(limit.max(0) as usize);
                        Ok(live)
                    }
                }
            }

            /// Query the live `jobs` table with the shared filter set.
            #[allow(clippy::too_many_arguments)]
            fn list_live_filtered(
                &self,
                status: Option<i32>,
                queue_name: Option<&str>,
                task_name: Option<&str>,
                metadata_like: Option<&str>,
                error_like: Option<&str>,
                created_after: Option<i64>,
                created_before: Option<i64>,
                limit: i64,
                offset: i64,
                namespace: Option<&str>,
            ) -> Result<Vec<Job>> {
                let mut conn = self.conn()?;

                let mut query = jobs::table.into_boxed().order(jobs::created_at.desc());

                if let Some(s) = status {
                    query = query.filter(jobs::status.eq(s));
                }
                if let Some(q) = queue_name {
                    query = query.filter(jobs::queue.eq(q));
                }
                if let Some(t) = task_name {
                    query = query.filter(jobs::task_name.eq(t));
                }
                if let Some(m) = metadata_like {
                    query = query.filter(jobs::metadata.like(format!("%{m}%")));
                }
                if let Some(e) = error_like {
                    query = query.filter(jobs::error.like(format!("%{e}%")));
                }
                if let Some(after) = created_after {
                    query = query.filter(jobs::created_at.ge(after));
                }
                if let Some(before) = created_before {
                    query = query.filter(jobs::created_at.le(before));
                }
                if let Some(ns) = namespace {
                    query = query.filter(jobs::namespace.eq(ns));
                }

                // Listings never render the arg/result blobs, so select the
                // narrow projection: the blobs stay on SQLite overflow pages /
                // Postgres TOAST and are read only by a `get_job` detail lookup.
                let rows: Vec<NarrowJobRow> = query
                    .limit(limit)
                    .offset(offset)
                    .select(NarrowJobRow::as_select())
                    .load(&mut conn)?;

                Ok(rows
                    .into_iter()
                    .map(|r| Job::from_narrow(r, Vec::new(), None))
                    .collect())
            }

            /// Keyset twin of `list_live_filtered`, ordered by
            /// `(created_at, id)` descending with a `(created_at, id) < cursor`
            /// bound instead of an offset.
            #[allow(clippy::too_many_arguments)]
            fn list_live_filtered_after(
                &self,
                status: Option<i32>,
                queue_name: Option<&str>,
                task_name: Option<&str>,
                metadata_like: Option<&str>,
                error_like: Option<&str>,
                created_after: Option<i64>,
                created_before: Option<i64>,
                limit: i64,
                after: Option<(i64, &str)>,
                namespace: Option<&str>,
            ) -> Result<Vec<Job>> {
                let mut conn = self.conn()?;

                let mut query = jobs::table
                    .into_boxed()
                    .order((jobs::created_at.desc(), jobs::id.desc()));

                if let Some(s) = status {
                    query = query.filter(jobs::status.eq(s));
                }
                if let Some(q) = queue_name {
                    query = query.filter(jobs::queue.eq(q));
                }
                if let Some(t) = task_name {
                    query = query.filter(jobs::task_name.eq(t));
                }
                if let Some(m) = metadata_like {
                    query = query.filter(jobs::metadata.like(format!("%{m}%")));
                }
                if let Some(e) = error_like {
                    query = query.filter(jobs::error.like(format!("%{e}%")));
                }
                if let Some(after) = created_after {
                    query = query.filter(jobs::created_at.ge(after));
                }
                if let Some(before) = created_before {
                    query = query.filter(jobs::created_at.le(before));
                }
                if let Some(ns) = namespace {
                    query = query.filter(jobs::namespace.eq(ns));
                }
                if let Some((cursor_created_at, cursor_id)) = after {
                    let cursor_id = cursor_id.to_string();
                    query = query.filter(
                        jobs::created_at.lt(cursor_created_at).or(jobs::created_at
                            .eq(cursor_created_at)
                            .and(jobs::id.lt(cursor_id))),
                    );
                }

                let rows: Vec<NarrowJobRow> = query
                    .limit(limit)
                    .select(NarrowJobRow::as_select())
                    .load(&mut conn)?;

                Ok(rows
                    .into_iter()
                    .map(|r| Job::from_narrow(r, Vec::new(), None))
                    .collect())
            }

            /// Query the `archived_jobs` table with the shared filter set.
            #[allow(clippy::too_many_arguments)]
            fn list_archived_filtered(
                &self,
                status: Option<i32>,
                queue_name: Option<&str>,
                task_name: Option<&str>,
                metadata_like: Option<&str>,
                error_like: Option<&str>,
                created_after: Option<i64>,
                created_before: Option<i64>,
                limit: i64,
                offset: i64,
                namespace: Option<&str>,
            ) -> Result<Vec<Job>> {
                let mut conn = self.conn()?;

                let mut query = archived_jobs::table
                    .into_boxed()
                    .order(archived_jobs::created_at.desc());

                if let Some(s) = status {
                    query = query.filter(archived_jobs::status.eq(s));
                }
                if let Some(q) = queue_name {
                    query = query.filter(archived_jobs::queue.eq(q));
                }
                if let Some(t) = task_name {
                    query = query.filter(archived_jobs::task_name.eq(t));
                }
                if let Some(m) = metadata_like {
                    query = query.filter(archived_jobs::metadata.like(format!("%{m}%")));
                }
                if let Some(e) = error_like {
                    query = query.filter(archived_jobs::error.like(format!("%{e}%")));
                }
                if let Some(after) = created_after {
                    query = query.filter(archived_jobs::created_at.ge(after));
                }
                if let Some(before) = created_before {
                    query = query.filter(archived_jobs::created_at.le(before));
                }
                if let Some(ns) = namespace {
                    query = query.filter(archived_jobs::namespace.eq(ns));
                }

                // Narrow projection: terminal listings never render blobs, so
                // leave `payload`/`result` on TOAST/overflow pages.
                let rows: Vec<NarrowArchivedJobRow> = query
                    .limit(limit)
                    .offset(offset)
                    .select(NarrowArchivedJobRow::as_select())
                    .load(&mut conn)?;

                Ok(rows.into_iter().map(Job::from_narrow_archived).collect())
            }

            /// Keyset twin of `list_archived_filtered`, ordered by
            /// `(created_at, id)` descending. Used by the status=None merge, so
            /// it must page on `created_at` (matching the live side), NOT on
            /// `completed_at` (which `list_archived_after` uses).
            #[allow(clippy::too_many_arguments)]
            fn list_archived_filtered_after(
                &self,
                status: Option<i32>,
                queue_name: Option<&str>,
                task_name: Option<&str>,
                metadata_like: Option<&str>,
                error_like: Option<&str>,
                created_after: Option<i64>,
                created_before: Option<i64>,
                limit: i64,
                after: Option<(i64, &str)>,
                namespace: Option<&str>,
            ) -> Result<Vec<Job>> {
                let mut conn = self.conn()?;

                let mut query = archived_jobs::table
                    .into_boxed()
                    .order((archived_jobs::created_at.desc(), archived_jobs::id.desc()));

                if let Some(s) = status {
                    query = query.filter(archived_jobs::status.eq(s));
                }
                if let Some(q) = queue_name {
                    query = query.filter(archived_jobs::queue.eq(q));
                }
                if let Some(t) = task_name {
                    query = query.filter(archived_jobs::task_name.eq(t));
                }
                if let Some(m) = metadata_like {
                    query = query.filter(archived_jobs::metadata.like(format!("%{m}%")));
                }
                if let Some(e) = error_like {
                    query = query.filter(archived_jobs::error.like(format!("%{e}%")));
                }
                if let Some(a) = created_after {
                    query = query.filter(archived_jobs::created_at.ge(a));
                }
                if let Some(before) = created_before {
                    query = query.filter(archived_jobs::created_at.le(before));
                }
                if let Some(ns) = namespace {
                    query = query.filter(archived_jobs::namespace.eq(ns));
                }
                if let Some((cursor_created_at, cursor_id)) = after {
                    let cursor_id = cursor_id.to_string();
                    query = query.filter(
                        archived_jobs::created_at.lt(cursor_created_at).or(
                            archived_jobs::created_at
                                .eq(cursor_created_at)
                                .and(archived_jobs::id.lt(cursor_id)),
                        ),
                    );
                }

                let rows: Vec<NarrowArchivedJobRow> = query
                    .limit(limit)
                    .select(NarrowArchivedJobRow::as_select())
                    .load(&mut conn)?;

                Ok(rows.into_iter().map(Job::from_narrow_archived).collect())
            }

            /// Delete one already-selected batch of archived jobs and their
            /// children inside the caller's txn. Relations (deps, replay) always
            /// go; the retention-managed diagnostics (errors/logs/metrics) go
            /// only when `cascade_diagnostics` is set — the per-table retention
            /// purge leaves them to their own window instead.
            fn purge_archived_id_batch(
                conn: &mut $conn_type,
                ids: &[String],
                cascade_diagnostics: bool,
            ) -> diesel::result::QueryResult<u64> {
                if ids.is_empty() {
                    return Ok(0);
                }
                Self::delete_job_relations(conn, ids)?;
                if cascade_diagnostics {
                    Self::delete_job_diagnostics(conn, ids)?;
                }
                let affected =
                    diesel::delete(archived_jobs::table.filter(archived_jobs::id.eq_any(ids)))
                        .execute(conn)?;
                Ok(affected as u64)
            }

            /// Purge completed jobs older than the given timestamp. Terminal
            /// jobs live in `archived_jobs`, so the purge targets that table.
            ///
            /// Deletes in bounded batches, each its own `BEGIN IMMEDIATE` txn,
            /// so a purge of millions of rows never holds the single writer lock
            /// for the whole sweep (SQLite) and never builds an unbounded
            /// `IN (...)` list.
            pub fn purge_completed(&self, older_than_ms: i64) -> Result<u64> {
                $crate::storage::diesel_common::purge::drain_batches(|| {
                    self.write_transaction(|conn| {
                        let ids: Vec<String> = archived_jobs::table
                            .filter(archived_jobs::status.eq(JobStatus::Complete as i32))
                            .filter(archived_jobs::completed_at.lt(older_than_ms))
                            .select(archived_jobs::id)
                            .limit(Self::PURGE_BATCH)
                            .load(conn)?;
                        Ok(Self::purge_archived_id_batch(conn, &ids, true)?)
                    })
                })
            }

            /// Purge archived jobs respecting per-job result_ttl_ms. Covers
            /// **every terminal status** — a failing queue's Failed/Cancelled
            /// rows grow the archive just as much as successes, so retention must
            /// bound them too. Batched like [`Self::purge_completed`]; the
            /// global-TTL and per-job-TTL rows are swept in two independent
            /// bounded loops. A `None` cutoff runs only the per-entry sweep — a
            /// job can carry its own TTL even when the queue keeps everything.
            pub fn purge_completed_with_ttl(&self, global_cutoff_ms: Option<i64>) -> Result<u64> {
                let now = now_millis();

                // Rows with no per-job TTL fall back to the global cutoff.
                let global = match global_cutoff_ms {
                    Some(cutoff) => $crate::storage::diesel_common::purge::drain_batches(|| {
                        self.write_transaction(|conn| {
                            let ids: Vec<String> = archived_jobs::table
                                .filter(archived_jobs::result_ttl_ms.is_null())
                                .filter(archived_jobs::completed_at.lt(cutoff))
                                .select(archived_jobs::id)
                                .limit(Self::PURGE_BATCH)
                                .load(conn)?;
                            Ok(Self::purge_archived_id_batch(conn, &ids, false)?)
                        })
                    })?,
                    None => 0,
                };

                // Rows with a per-job TTL: `completed_at + result_ttl_ms < now`.
                // The check is pushed into SQL, selecting only the id so no
                // payload/result blob is loaded just to filter it.
                let per_entry = $crate::storage::diesel_common::purge::drain_batches(|| {
                    self.write_transaction(|conn| {
                        let ids: Vec<String> = archived_jobs::table
                            .filter(archived_jobs::result_ttl_ms.is_not_null())
                            .filter(archived_jobs::completed_at.is_not_null())
                            .filter(
                                (archived_jobs::completed_at.assume_not_null()
                                    + archived_jobs::result_ttl_ms.assume_not_null())
                                .lt(now),
                            )
                            .select(archived_jobs::id)
                            .limit(Self::PURGE_BATCH)
                            .load(conn)?;
                        Ok(Self::purge_archived_id_batch(conn, &ids, false)?)
                    })
                })?;

                Ok(global + per_entry)
            }

            /// Find stale running jobs that exceeded their timeout.
            pub fn reap_stale_jobs(&self, now: i64) -> Result<Vec<Job>> {
                let mut conn = self.conn()?;

                // Push the `started_at + timeout_ms < now` deadline into SQL so
                // only genuinely-stale rows are read, instead of every running
                // job. The narrow row skips the payload/result blobs entirely —
                // reaping only needs the timeout arithmetic plus id/task/queue,
                // so the assembled Job carries an empty payload.
                let rows: Vec<NarrowJobRow> = jobs::table
                    .filter(jobs::status.eq(JobStatus::Running as i32))
                    .filter(jobs::started_at.is_not_null())
                    .filter((jobs::started_at.assume_not_null() + jobs::timeout_ms).lt(now))
                    .select(NarrowJobRow::as_select())
                    .load(&mut conn)?;

                Ok(rows
                    .into_iter()
                    .map(|narrow| Job::from_narrow(narrow, Vec::new(), None))
                    .collect())
            }

            /// Running jobs whose execution-claim owner is no longer alive (the
            /// worker that claimed them died). Read-only, like `reap_stale_jobs`:
            /// the scheduler atomically reclaims and requeues each one. Two
            /// indexed queries rather than a join, since `jobs` and
            /// `execution_claims` are not declared joinable.
            pub fn reap_orphaned_jobs(
                &self,
                live_owner_ids: &[String],
                _now: i64,
            ) -> Result<Vec<(Job, String)>> {
                // Defensive: never treat every claim as orphaned. The caller
                // always includes its own owner, so this is unreachable in
                // practice but guards against a `NOT IN (empty)` sweep.
                if live_owner_ids.is_empty() {
                    return Ok(Vec::new());
                }

                let mut conn = self.conn()?;

                // Claims owned by a worker not in the live set.
                let orphan_claims: Vec<(String, String)> = execution_claims::table
                    .filter(diesel::dsl::not(
                        execution_claims::worker_id.eq_any(live_owner_ids),
                    ))
                    .select((execution_claims::job_id, execution_claims::worker_id))
                    .load(&mut conn)?;
                if orphan_claims.is_empty() {
                    return Ok(Vec::new());
                }

                let job_ids: Vec<String> = orphan_claims.iter().map(|(id, _)| id.clone()).collect();
                let owner_by_job: std::collections::HashMap<String, String> =
                    orphan_claims.into_iter().collect();

                // Of those, the ones still Running (blob-free narrow row).
                let rows: Vec<NarrowJobRow> = jobs::table
                    .filter(jobs::id.eq_any(&job_ids))
                    .filter(jobs::status.eq(JobStatus::Running as i32))
                    .select(NarrowJobRow::as_select())
                    .load(&mut conn)?;

                Ok(rows
                    .into_iter()
                    .map(|narrow| {
                        let owner = owner_by_job.get(&narrow.id).cloned().unwrap_or_default();
                        (Job::from_narrow(narrow, Vec::new(), None), owner)
                    })
                    .collect())
            }

            /// Record an error for a job attempt.
            pub fn record_error(&self, job_id: &str, attempt: i32, error: &str) -> Result<()> {
                let mut conn = self.conn()?;
                let id = uuid::Uuid::now_v7().to_string();
                let now = now_millis();

                let row = NewJobErrorRow {
                    id: &id,
                    job_id,
                    attempt,
                    error,
                    failed_at: now,
                };

                diesel::insert_into(job_errors::table)
                    .values(&row)
                    .execute(&mut conn)?;

                Ok(())
            }

            /// Get all errors for a job, ordered by attempt.
            pub fn get_job_errors(
                &self,
                job_id: &str,
            ) -> Result<Vec<$crate::storage::records::JobError>> {
                let mut conn = self.conn()?;

                let rows = job_errors::table
                    .filter(job_errors::job_id.eq(job_id))
                    .order(job_errors::attempt.asc())
                    .select(JobErrorRow::as_select())
                    .load::<JobErrorRow>(&mut conn)?;

                Ok(rows.into_iter().map(Into::into).collect())
            }

            /// Archive a set of pending job rows as cancelled with the given
            /// error, moving each from `jobs` to `archived_jobs`.
            fn archive_pending_rows(
                conn: &mut $conn_type,
                rows: Vec<JobRow>,
                now: i64,
                error: &str,
            ) -> diesel::result::QueryResult<u64> {
                let count = rows.len() as u64;
                for mut row in rows {
                    row.status = JobStatus::Cancelled as i32;
                    row.completed_at = Some(now);
                    row.error = Some(error.to_string());
                    Self::archive_job_row(conn, &row)?;
                }
                Ok(count)
            }

            /// Archive matching pending jobs in bounded batches. `select_batch`
            /// loads up to `limit` pending rows to cancel; each batch runs in
            /// its own txn, so cancelling a huge pending backlog never holds the
            /// SQLite writer lock (or the full row set in memory) at once. The
            /// archive removes each row from `jobs`, so the same filter drains
            /// toward empty across iterations.
            fn archive_pending_in_batches<S>(
                &self,
                now: i64,
                error: &str,
                select_batch: S,
            ) -> Result<u64>
            where
                S: Fn(&mut $conn_type, i64) -> diesel::result::QueryResult<Vec<JobRow>>,
            {
                let mut total = 0u64;
                loop {
                    let archived = self.write_transaction(|conn| {
                        let rows = select_batch(conn, Self::MASS_ARCHIVE_BATCH)?;
                        Ok(Self::archive_pending_rows(conn, rows, now, error)?)
                    })?;
                    total += archived;
                    if archived < Self::MASS_ARCHIVE_BATCH as u64 {
                        break;
                    }
                }
                Ok(total)
            }

            /// Expire pending jobs that have passed their expires_at.
            pub fn expire_pending_jobs(&self, now: i64) -> Result<u64> {
                self.archive_pending_in_batches(now, "expired", |conn, limit| {
                    jobs::table
                        .filter(jobs::status.eq(JobStatus::Pending as i32))
                        .filter(jobs::expires_at.is_not_null())
                        .filter(jobs::expires_at.lt(now))
                        .select(JobRow::as_select())
                        .limit(limit)
                        .load(conn)
                })
            }

            /// Cancel all pending jobs in a specific queue.
            pub fn cancel_pending_by_queue(&self, queue: &str) -> Result<u64> {
                let now = now_millis();
                self.archive_pending_in_batches(now, "purged", |conn, limit| {
                    jobs::table
                        .filter(jobs::status.eq(JobStatus::Pending as i32))
                        .filter(jobs::queue.eq(queue))
                        .select(JobRow::as_select())
                        .limit(limit)
                        .load(conn)
                })
            }

            /// Cancel all pending jobs for a specific task name.
            pub fn cancel_pending_by_task(&self, task_name: &str) -> Result<u64> {
                let now = now_millis();
                self.archive_pending_in_batches(now, "revoked", |conn, limit| {
                    jobs::table
                        .filter(jobs::status.eq(JobStatus::Pending as i32))
                        .filter(jobs::task_name.eq(task_name))
                        .select(JobRow::as_select())
                        .limit(limit)
                        .load(conn)
                })
            }

            /// Count running jobs for a specific task name (for per-task concurrency limiting).
            pub fn count_running_by_task(&self, task_name: &str) -> Result<i64> {
                let mut conn = self.conn()?;

                let count: i64 = jobs::table
                    .filter(jobs::task_name.eq(task_name))
                    .filter(jobs::status.eq(JobStatus::Running as i32))
                    .count()
                    .get_result(&mut conn)?;

                Ok(count)
            }

            /// Count pending jobs on a queue (for the `max_pending` admission cap).
            pub fn count_pending_by_queue(&self, queue_name: &str) -> Result<i64> {
                let mut conn = self.conn()?;

                let count: i64 = jobs::table
                    .filter(jobs::queue.eq(queue_name))
                    .filter(jobs::status.eq(JobStatus::Pending as i32))
                    .count()
                    .get_result(&mut conn)?;

                Ok(count)
            }

            /// Purge job errors older than the given timestamp.
            ///
            /// Deletes in bounded batches, each its own txn — see
            /// `diesel_common::purge`.
            pub fn purge_job_errors(&self, older_than_ms: i64) -> Result<u64> {
                $crate::storage::diesel_common::purge::drain_batches(|| {
                    self.write_transaction(|conn| {
                        let ids: Vec<String> = job_errors::table
                            .filter(job_errors::failed_at.lt(older_than_ms))
                            .select(job_errors::id)
                            .limit(Self::PURGE_BATCH)
                            .load(conn)?;
                        let affected =
                            diesel::delete(job_errors::table.filter(job_errors::id.eq_any(&ids)))
                                .execute(conn)?;
                        Ok(affected as u64)
                    })
                })
            }
        }
    };
}

pub(crate) use impl_diesel_job_ops;
