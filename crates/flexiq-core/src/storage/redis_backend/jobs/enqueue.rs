//! Enqueue, batch enqueue, and unique-keyed enqueue.

use std::sync::LazyLock;

use redis::Commands;

use super::dequeue_score;
use crate::error::{QueueError, Result};
use crate::job::{now_millis, Job, JobStatus, NewJob};
use crate::storage::records::DebounceOptions;
use crate::storage::redis_backend::{map_err, RedisStorage};
use crate::storage::DEBOUNCE_CANDIDATE_SCAN;

/// Lua: find the pending, unclaimed job a debounce write may slide, pruning the
/// index as it goes. Shared verbatim by [`DEBOUNCE_RESOLVE`] and
/// [`DEBOUNCE_INSERT`] — the insert must re-run it under the same script call
/// that writes, or two first-of-a-burst enqueues both find nothing and both
/// insert, which is the exact case debounce exists to collapse.
///
/// Candidates come out of the index oldest-first, so a burst always coalesces
/// onto the same job and its `created_at` is a stable `first_seen` for the
/// `max_wait` cap — the Diesel scan's ordering.
///
/// Membership is a hint, never an authority: an entry whose job is gone or has
/// left `Pending` is dropped here. A job holding an execution claim is skipped
/// but left indexed — `claim_execution` writes its row without touching
/// `status`, so a Pending job can carry a live claim, and a job a worker
/// already holds must never be pulled back to a later deadline.
///
/// The status comparison uses `JobStatus::wire_name()` passed through ARGV
/// rather than a discriminant baked into the Lua, keeping the wire-format
/// contract single-sourced (see `wire_name_matches_serde_output`).
///
/// KEYS[1]: debounce index. ARGV[1-4]: job key prefix, claim key prefix,
/// pending wire name, scan limit. Falls through when nothing matches.
const DEBOUNCE_SCAN: &str = r#"
    local debounce_index = KEYS[1]
    local job_key_prefix = ARGV[1]
    local claim_key_prefix = ARGV[2]
    local pending_status = ARGV[3]
    local scan_limit = tonumber(ARGV[4])

    local candidates = redis.call('ZRANGE', debounce_index, 0, scan_limit - 1)
    for i = 1, #candidates do
        local candidate_id = candidates[i]
        local candidate = redis.call('GET', job_key_prefix .. candidate_id)
        if not candidate then
            redis.call('ZREM', debounce_index, candidate_id)
        elseif cjson.decode(candidate).status ~= pending_status then
            redis.call('ZREM', debounce_index, candidate_id)
        elseif redis.call('EXISTS', claim_key_prefix .. candidate_id) == 0 then
            return candidate
        end
    end
"#;

/// Lua: [`DEBOUNCE_SCAN`] as a lookup — the slide target's document, or nil when
/// the key has no window open.
///
/// Composed at first use rather than as a `const`: the two scripts share the
/// scan verbatim, and a `Script` built once keeps its SHA so every call after
/// the first is an `EVALSHA`.
static DEBOUNCE_RESOLVE: LazyLock<redis::Script> =
    LazyLock::new(|| redis::Script::new(&format!("{DEBOUNCE_SCAN}\n    return nil\n")));

/// Lua: open a fresh window — insert the job with every index a plain `enqueue`
/// writes, plus its own debounce entry. Re-runs [`DEBOUNCE_SCAN`] first and
/// hands back the document instead if a concurrent caller opened the window in
/// the meantime, so the caller slides that one rather than inserting a second
/// job.
///
/// Three replies, told apart by type: nil for an insert, a bulk string for the
/// document to slide instead, and a one-element array holding the pending count
/// when the queue is at its admission cap. The array is what keeps the cap
/// check inside the write — the count is taken here, after the scan has proven
/// no window is open, so nothing is refused for a burst that inserts nothing.
///
/// KEYS (after the index): job, status set, queue zset, by_queue, by_task, all,
/// and — only for a pub/sub delivery — its subscription's pending backlog.
/// ARGV (after the scan's four): job id, job JSON, queue score, `created_at`,
/// dependency count, the admission cap (negative = uncapped), then one
/// `(depends_on_key, dep_id, dependents_key)` triple per dependency.
static DEBOUNCE_INSERT: LazyLock<redis::Script> =
    LazyLock::new(|| redis::Script::new(&format!("{DEBOUNCE_SCAN}{DEBOUNCE_INSERT_BODY}")));

/// The write half of [`DEBOUNCE_INSERT`], appended to [`DEBOUNCE_SCAN`].
const DEBOUNCE_INSERT_BODY: &str = r#"
    local job_id = ARGV[5]
    local job_json = ARGV[6]
    local score = tonumber(ARGV[7])
    local created_at = tonumber(ARGV[8])
    local num_deps = tonumber(ARGV[9])
    local max_pending = tonumber(ARGV[10])
    local dep_args_base = 11

    -- KEYS[3] is the status set of the job being inserted, which is always
    -- Pending, so this is `count_pending_by_queue` computed server-side.
    if max_pending >= 0 then
        local pending = redis.call('SINTERCARD', 2, KEYS[5], KEYS[3])
        if pending + 1 > max_pending then
            return { pending }
        end
    end

    redis.call('SET', KEYS[2], job_json)
    redis.call('SADD', KEYS[3], job_id)
    redis.call('ZADD', KEYS[4], score, job_id)
    redis.call('SADD', KEYS[5], job_id)
    redis.call('SADD', KEYS[6], job_id)
    redis.call('ZADD', KEYS[7], -created_at, job_id)
    redis.call('ZADD', debounce_index, created_at, job_id)

    if KEYS[8] then
        redis.call('ZADD', KEYS[8], created_at, job_id)
    end

    for i = 1, num_deps do
        local offset = dep_args_base + (i - 1) * 3
        redis.call('SADD', ARGV[offset], ARGV[offset + 1])
        redis.call('SADD', ARGV[offset + 2], job_id)
    end

    return nil
"#;

/// Lua: commit a slid deadline as a compare-and-swap on the whole document.
///
/// Comparing the document the scan read is what makes the read-modify-write
/// safe without a transaction: a job archived, retried, or otherwise rewritten
/// in between no longer matches, so the write is refused rather than
/// resurrecting or clobbering it. The claim is re-checked because a claim
/// appears beside the document without changing it. Returns 1 on write, 0 to
/// retry from the scan.
///
/// KEYS: job, queue zset, execution claim. ARGV: job id, document as read,
/// document to write, new queue score.
const DEBOUNCE_SLIDE: &str = r#"
    if redis.call('GET', KEYS[1]) ~= ARGV[2] then return 0 end
    if redis.call('EXISTS', KEYS[3]) == 1 then return 0 end
    redis.call('SET', KEYS[1], ARGV[3])
    redis.call('ZADD', KEYS[2], ARGV[4], ARGV[1])
    return 1
"#;

/// How many times a debounced enqueue re-runs the scan before giving up. Each
/// retry means another caller took the slot mid-flight; persistent contention
/// surfaces as an error rather than a second job or a phantom one. Mirrors
/// `MAX_ENQUEUE_ATTEMPTS` in [`RedisStorage::enqueue_unique`].
const MAX_DEBOUNCE_ATTEMPTS: usize = 3;

/// What [`DEBOUNCE_INSERT`] wrote. Refusing the insert for the admission cap is
/// not a variant here: it is the error the caller returns either way, and the
/// script is the only thing that knows the count behind it.
enum DebounceInsert {
    /// The window was opened by this job.
    Opened,
    /// A concurrent caller opened it first; this is its document to slide.
    Raced(String),
}

impl RedisStorage {
    /// Validate that each `dep_id` references a job that exists, shares
    /// `namespace` with the job that depends on it, and isn't in `Dead` /
    /// `Cancelled` state.
    ///
    /// A dependency across the namespace boundary is rejected rather than
    /// filtered: the edge would let one tenant's failure cascade into
    /// another's queue, and it can only ever be half-honoured —
    /// `cascade_cancel` already refuses to cross. It reads as an ordinary
    /// missing dependency so a scoped caller learns nothing about ids outside
    /// its own namespace.
    ///
    /// `batch` short-circuits intra-batch dependencies for `enqueue_batch`,
    /// where some dep ids point at jobs being created in the same call. It maps
    /// each id to its namespace rather than being a bare id set: skipping the
    /// not-yet-written row must not also skip the boundary check.
    fn validate_dep_ids(
        &self,
        conn: &mut redis::Connection,
        dep_ids: &[String],
        namespace: Option<&str>,
        batch: Option<&std::collections::HashMap<&str, Option<&str>>>,
    ) -> Result<()> {
        const DEP_MISSING: &str = "dependency not found or already dead/cancelled";
        for dep_id in dep_ids {
            if let Some(&dep_ns) = batch.and_then(|b| b.get(dep_id.as_str())) {
                if dep_ns != namespace {
                    return Err(QueueError::DependencyNotFound(DEP_MISSING.to_string()));
                }
                continue;
            }
            // A live dep is read from `job:<id>`; a terminal dep has been
            // archived, so a missing live row falls back to `archived:<id>`.
            // A completed archived dep is valid; any other state is rejected.
            let dep_key = self.key(&["job", dep_id]);
            let data: Option<String> = conn.get(&dep_key).map_err(map_err)?;
            let dep_job: Job = match data {
                Some(d) => serde_json::from_str(&d)?,
                None => match self.load_archived_job(conn, dep_id)? {
                    Some(archived)
                        if archived.status == JobStatus::Complete
                            && archived.namespace.as_deref() == namespace =>
                    {
                        continue
                    }
                    _ => return Err(QueueError::DependencyNotFound(DEP_MISSING.to_string())),
                },
            };
            if dep_job.status == JobStatus::Dead
                || dep_job.status == JobStatus::Cancelled
                || dep_job.namespace.as_deref() != namespace
            {
                return Err(QueueError::DependencyNotFound(DEP_MISSING.to_string()));
            }
        }
        Ok(())
    }

    /// Insert a new job and return it, writing its JSON plus the status,
    /// queue, and task index entries.
    pub fn enqueue(&self, new_job: NewJob) -> Result<Job> {
        let depends_on = new_job.depends_on.clone();
        let job = new_job.into_job();
        let mut conn = self.conn()?;

        self.validate_dep_ids(&mut conn, &depends_on, job.namespace.as_deref(), None)?;

        let job_json = serde_json::to_string(&job)?;
        let job_key = self.key(&["job", &job.id]);
        let status_key = self.key(&["jobs", "status", &(job.status as i32).to_string()]);
        let queue_key = self.key(&["queue", &job.queue, "pending"]);
        let by_queue_key = self.key(&["jobs", "by_queue", &job.queue]);
        let by_task_key = self.key(&["jobs", "by_task", &job.task_name]);
        let all_key = self.key(&["jobs", "all"]);
        let score = dequeue_score(job.priority, job.scheduled_at);

        let pipe = &mut redis::pipe();
        pipe.set(&job_key, &job_json);
        pipe.sadd(&status_key, &job.id);
        pipe.zadd(&queue_key, &job.id, score);
        pipe.sadd(&by_queue_key, &job.id);
        pipe.sadd(&by_task_key, &job.id);
        pipe.zadd(&all_key, &job.id, -(job.created_at as f64));

        // A pending job carrying a debounce key is a valid slide target however
        // it was enqueued, so it enters the key's index here too — the Diesel
        // partial index covers every insert, not only the debounced path.
        if let Some(debounce_key) = self.job_debounce_index_key(&job) {
            pipe.zadd(&debounce_key, &job.id, job.created_at as f64);
        }

        // Store dependencies
        for dep_id in &depends_on {
            let depends_on_key = self.key(&["job", &job.id, "depends_on"]);
            let dependents_key = self.key(&["job", dep_id, "dependents"]);
            pipe.sadd(&depends_on_key, dep_id);
            pipe.sadd(&dependents_key, &job.id);
        }

        // A pub/sub delivery enters its subscription's pending backlog index
        // (no-op for ordinary jobs). Same pipe as the job's own indices.
        self.push_pubsub_transition(pipe, &job, JobStatus::Pending);

        pipe.query::<()>(&mut conn).map_err(map_err)?;

        Ok(job)
    }

    /// Batch [`enqueue`](Self::enqueue): insert multiple jobs at once.
    pub fn enqueue_batch(&self, new_jobs: Vec<NewJob>) -> Result<Vec<Job>> {
        // Collect dependency lists before consuming new_jobs
        let dep_lists: Vec<Vec<String>> = new_jobs.iter().map(|nj| nj.depends_on.clone()).collect();
        let jobs: Vec<Job> = new_jobs.into_iter().map(|nj| nj.into_job()).collect();
        let mut conn = self.conn()?;

        // Namespace per batch job, for intra-batch dependency resolution.
        let batch_ns: std::collections::HashMap<&str, Option<&str>> = jobs
            .iter()
            .map(|j| (j.id.as_str(), j.namespace.as_deref()))
            .collect();

        for (job, depends_on) in jobs.iter().zip(&dep_lists) {
            self.validate_dep_ids(
                &mut conn,
                depends_on,
                job.namespace.as_deref(),
                Some(&batch_ns),
            )?;
        }

        let pipe = &mut redis::pipe();
        for (i, job) in jobs.iter().enumerate() {
            let job_json = serde_json::to_string(job)?;
            let job_key = self.key(&["job", &job.id]);
            let status_key = self.key(&["jobs", "status", &(job.status as i32).to_string()]);
            let queue_key = self.key(&["queue", &job.queue, "pending"]);
            let by_queue_key = self.key(&["jobs", "by_queue", &job.queue]);
            let by_task_key = self.key(&["jobs", "by_task", &job.task_name]);
            let all_key = self.key(&["jobs", "all"]);
            let score = dequeue_score(job.priority, job.scheduled_at);

            pipe.set(&job_key, &job_json);
            pipe.sadd(&status_key, &job.id);
            pipe.zadd(&queue_key, &job.id, score);
            pipe.sadd(&by_queue_key, &job.id);
            pipe.sadd(&by_task_key, &job.id);
            pipe.zadd(&all_key, &job.id, -(job.created_at as f64));

            // Debounce index, as in the single-job `enqueue`.
            if let Some(debounce_key) = self.job_debounce_index_key(job) {
                pipe.zadd(&debounce_key, &job.id, job.created_at as f64);
            }

            // Store dependencies
            for dep_id in &dep_lists[i] {
                let depends_on_key = self.key(&["job", &job.id, "depends_on"]);
                let dependents_key = self.key(&["job", dep_id, "dependents"]);
                pipe.sadd(&depends_on_key, dep_id);
                pipe.sadd(&dependents_key, &job.id);
            }

            // Pub/sub deliveries enter their subscription's pending backlog
            // index (no-op for ordinary jobs), atomically with the batch insert.
            self.push_pubsub_transition(pipe, job, JobStatus::Pending);
        }

        pipe.query::<()>(&mut conn).map_err(map_err)?;
        Ok(jobs)
    }

    /// Batch variant of `enqueue_unique`. Redis has no database-wide write
    /// lock (each op is atomic and cheap), so looping the single-delivery path
    /// is correct and avoids a bespoke multi-delivery Lua script — the batch
    /// API's real win is on the Diesel backends. Salted keys are distinct
    /// within one publish, so deliveries never dedupe against each other.
    pub fn enqueue_unique_batch(&self, new_jobs: Vec<NewJob>) -> Result<Vec<Job>> {
        new_jobs
            .into_iter()
            .map(|job| self.enqueue_unique(job))
            .collect()
    }

    /// [`enqueue_unique_batch`](Self::enqueue_unique_batch), reporting per item
    /// whether it deduped — see
    /// [`enqueue_unique_reporting`](Self::enqueue_unique_reporting).
    pub fn enqueue_unique_batch_reporting(
        &self,
        new_jobs: Vec<NewJob>,
    ) -> Result<Vec<(Job, bool)>> {
        new_jobs
            .into_iter()
            .map(|job| self.enqueue_unique_reporting(job))
            .collect()
    }

    /// The document of the pending, unclaimed job this key may slide, or `None`
    /// when its window is closed. Prunes stale index entries as it scans.
    fn resolve_debounce_target(
        &self,
        conn: &mut redis::Connection,
        index_key: &str,
    ) -> Result<Option<String>> {
        let mut invocation = DEBOUNCE_RESOLVE.prepare_invoke();
        invocation.key(index_key);
        self.push_debounce_scan_args(&mut invocation);
        invocation.invoke(conn).map_err(map_err)
    }

    /// The four arguments [`DEBOUNCE_SCAN`] reads, in order. Shared so the
    /// resolve and insert calls can never drift out of agreement on them.
    fn push_debounce_scan_args(&self, invocation: &mut redis::ScriptInvocation<'_>) {
        invocation
            .arg(self.key(&["job", ""]))
            .arg(self.key(&["exec_claim", ""]))
            .arg(JobStatus::Pending.wire_name())
            .arg(DEBOUNCE_CANDIDATE_SCAN);
    }

    /// Slide `target_json`'s deadline and commit it, or `None` when the job
    /// changed under us and the caller should rescan.
    ///
    /// The document is patched here rather than in Lua: `payload` is a
    /// `Vec<u8>`, so an empty one serializes as `[]`, and a `cjson` decode /
    /// encode round trip rewrites that as `{}`, which no longer deserializes.
    /// Keeping serde the only writer of the document costs one extra round trip
    /// on the slide and none on the insert.
    fn slide_debounce_target(
        &self,
        conn: &mut redis::Connection,
        target_json: &str,
        job: &Job,
        options: &DebounceOptions,
    ) -> Result<Option<Job>> {
        let mut target: Job = serde_json::from_str(target_json)?;
        // The cap is measured from when the window opened, so a caller holding
        // the button down cannot starve the job. Saturating like `job
        // .scheduled_at` above: an absurd `max_wait_ms` would otherwise wrap
        // negative and dispatch immediately, the opposite of what was asked.
        target.scheduled_at = std::cmp::min(
            job.scheduled_at,
            target.created_at.saturating_add(options.max_wait_ms),
        );
        if options.replace_payload {
            target.payload.clone_from(&job.payload);
        }

        let new_json = serde_json::to_string(&target)?;
        let applied: i32 = redis::Script::new(DEBOUNCE_SLIDE)
            .key(self.key(&["job", &target.id]))
            .key(self.key(&["queue", &target.queue, "pending"]))
            .key(self.key(&["exec_claim", &target.id]))
            .arg(&target.id)
            .arg(target_json)
            .arg(&new_json)
            .arg(dequeue_score(target.priority, target.scheduled_at))
            .invoke(conn)
            .map_err(map_err)?;

        Ok((applied == 1).then_some(target))
    }

    /// Open a window with `job`. [`DebounceInsert::Raced`] means a concurrent
    /// caller opened it first and that job is the one to slide.
    fn insert_debounced(
        &self,
        conn: &mut redis::Connection,
        index_key: &str,
        job: &Job,
        depends_on: &[String],
        max_pending: Option<i64>,
    ) -> Result<DebounceInsert> {
        let mut invocation = DEBOUNCE_INSERT.prepare_invoke();
        invocation
            .key(index_key)
            .key(self.key(&["job", &job.id]))
            .key(self.key(&["jobs", "status", &(job.status as i32).to_string()]))
            .key(self.key(&["queue", &job.queue, "pending"]))
            .key(self.key(&["jobs", "by_queue", &job.queue]))
            .key(self.key(&["jobs", "by_task", &job.task_name]))
            .key(self.key(&["jobs", "all"]));
        // A pub/sub delivery also enters its subscription's pending backlog
        // index, in the same script so the two cannot desync on a crash.
        if let Some((topic, name)) = crate::pubsub::extract_topic_subscription(job.notes.as_deref())
        {
            invocation.key(self.key(&["sub", "pending", &topic, &name]));
        }

        self.push_debounce_scan_args(&mut invocation);
        invocation
            .arg(&job.id)
            .arg(serde_json::to_string(job)?)
            .arg(dequeue_score(job.priority, job.scheduled_at))
            .arg(job.created_at)
            .arg(depends_on.len())
            // An uncapped queue is a negative cap rather than a nil argument:
            // Lua reads ARGV positionally, and a missing one would shift every
            // dependency triple after it.
            .arg(max_pending.unwrap_or(-1));
        for dep_id in depends_on {
            invocation
                .arg(self.key(&["job", &job.id, "depends_on"]))
                .arg(dep_id)
                .arg(self.key(&["job", dep_id, "dependents"]));
        }

        match (max_pending, invocation.invoke(conn).map_err(map_err)?) {
            (_, redis::Value::Nil) => Ok(DebounceInsert::Opened),
            // A one-element array is the cap refusal carrying its count. The
            // script only counts when it was given a cap, so the same shape
            // with no cap behind it is the script disagreeing with this
            // function, and falls through as an unreadable document below.
            (Some(cap), redis::Value::Array(reply)) => match reply.first() {
                Some(&redis::Value::Int(pending)) => Err(QueueError::QueueFull {
                    queue: job.queue.clone(),
                    pending,
                    cap,
                }),
                _ => Err(QueueError::Other(format!(
                    "unreadable debounced cap refusal: {reply:?}"
                ))),
            },
            (_, document) => Ok(DebounceInsert::Raced(
                redis::from_redis_value(document).map_err(|e| {
                    QueueError::Other(format!("unreadable debounced insert reply: {e}"))
                })?,
            )),
        }
    }

    /// Enqueue under a debounce window. See
    /// [`Storage::enqueue_debounced`](crate::storage::Storage::enqueue_debounced).
    ///
    /// Redis has no transaction to hang the read-modify-write on, so the
    /// slide-or-insert decision is a Lua script over the key's debounce index
    /// and the slide commits as a document-level compare-and-swap. Losing
    /// either race rescans rather than writing, so a burst can never leave two
    /// jobs behind.
    pub fn enqueue_debounced(&self, new_job: NewJob, options: DebounceOptions) -> Result<Job> {
        let debounce_key = crate::storage::validated_debounce_key(&new_job, &options)?;

        let depends_on = new_job.depends_on.clone();
        let mut job = new_job.into_job();
        let now = now_millis();
        // The window decides when a debounced job runs, not the caller.
        // `created_at` is pinned to the same instant because it doubles as
        // `first_seen` for the `max_wait` cap: leaving `into_job`'s own clock
        // reading there lets the two drift a millisecond apart, which is enough
        // to slide a deadline *backwards* when `max_wait_ms == window_ms`.
        job.created_at = now;
        job.scheduled_at = now.saturating_add(options.window_ms);

        let mut conn = self.conn()?;
        let index_key = self.debounce_index_key(job.namespace.as_deref(), &debounce_key);

        for _ in 0..MAX_DEBOUNCE_ATTEMPTS {
            let target = match self.resolve_debounce_target(&mut conn, &index_key)? {
                Some(target) => target,
                None => {
                    // No open window: insert, validating dependencies exactly as
                    // `enqueue` does. A coalescing call never reaches this, which
                    // matches the Diesel backends — a vote to run again soon does
                    // not redefine the run, so its dependencies are discarded
                    // unread.
                    self.validate_dep_ids(&mut conn, &depends_on, job.namespace.as_deref(), None)?;
                    match self.insert_debounced(
                        &mut conn,
                        &index_key,
                        &job,
                        &depends_on,
                        options.max_pending,
                    )? {
                        DebounceInsert::Opened => return Ok(job),
                        DebounceInsert::Raced(raced) => raced,
                    }
                }
            };

            if let Some(slid) = self.slide_debounce_target(&mut conn, &target, &job, &options)? {
                return Ok(slid);
            }
        }

        Err(QueueError::Other(format!(
            "debounced enqueue for {debounce_key} lost its slide target {MAX_DEBOUNCE_ATTEMPTS} times"
        )))
    }

    /// Enqueue with `unique_key` deduplication: a Lua script atomically returns
    /// the existing active job when a duplicate is found instead of inserting.
    pub fn enqueue_unique(&self, new_job: NewJob) -> Result<Job> {
        Ok(self.enqueue_unique_reporting(new_job)?.0)
    }

    /// [`enqueue_unique`](Self::enqueue_unique), also reporting whether the job
    /// came back from the unique slot instead of being inserted.
    ///
    /// Only this function can answer that: the id is generated here, so a
    /// caller comparing what it got against what it sent has nothing to
    /// compare. The flag is what `EnqueueResponse.deduplicated` carries on the
    /// wire.
    pub fn enqueue_unique_reporting(&self, new_job: NewJob) -> Result<(Job, bool)> {
        let mut conn = self.conn()?;

        if let Some(uk) = new_job.unique_key.clone() {
            let unique_key = self.key(&["jobs", "unique", &uk]);

            // Active-status comparison values are sourced from Rust via ARGV
            // rather than hardcoded in Lua, keeping the wire-format contract
            // single-sourced in `JobStatus::wire_name()`.
            let active_pending = JobStatus::Pending.wire_name();
            let active_running = JobStatus::Running.wire_name();

            // Atomically: check unique key → validate referenced job → decide
            let script = redis::Script::new(
                r#"
                local unique_key = KEYS[1]
                local job_key_prefix = ARGV[1]
                local active_pending = ARGV[2]
                local active_running = ARGV[3]

                local existing_id = redis.call('GET', unique_key)
                if existing_id then
                    local job_data = redis.call('GET', job_key_prefix .. existing_id)
                    if job_data then
                        local job = cjson.decode(job_data)
                        if job.status == active_pending or job.status == active_running then
                            return job_data
                        end
                    end
                    -- Referenced job is gone or terminal — drop the stale pointer.
                    redis.call('DEL', unique_key)
                end

                return nil
                "#,
            );

            let job_key_prefix = self.key(&["job", ""]);
            let result: Option<String> = script
                .key(&unique_key)
                .arg(&job_key_prefix)
                .arg(active_pending)
                .arg(active_running)
                .invoke(&mut conn)
                .map_err(map_err)?;

            if let Some(job_data) = result {
                let job: Job = serde_json::from_str(&job_data)?;
                return Ok((job, true));
            }

            // No active duplicate — enqueue normally
            let depends_on = new_job.depends_on.clone();
            let job = new_job.into_job();
            let job_json = serde_json::to_string(&job)?;

            self.validate_dep_ids(&mut conn, &depends_on, job.namespace.as_deref(), None)?;

            // Store everything atomically via Lua. Active-status names are
            // passed via ARGV (positions 7-8) for the same reason as above —
            // single-sourced in `JobStatus::wire_name()`. ARGV[9] names the
            // KEYS slot holding the debounce index, and dependency triples
            // start at ARGV[10].
            let store_script = redis::Script::new(
                r#"
                local unique_key = KEYS[1]
                local job_key = KEYS[2]
                local status_key = KEYS[3]
                local queue_key = KEYS[4]
                local by_queue_key = KEYS[5]
                local by_task_key = KEYS[6]
                local all_key = KEYS[7]

                local job_id = ARGV[1]
                local job_json = ARGV[2]
                local score = tonumber(ARGV[3])
                local created_at = tonumber(ARGV[4])
                local num_deps = tonumber(ARGV[5])
                local job_key_prefix = ARGV[6]
                local active_pending = ARGV[7]
                local active_running = ARGV[8]
                local debounce_slot = tonumber(ARGV[9])
                local dep_args_base = 10

                -- Re-check unique key (race guard against a concurrent enqueue).
                local existing = redis.call('GET', unique_key)
                if existing then
                    local ej_data = redis.call('GET', job_key_prefix .. existing)
                    if ej_data then
                        local ej = cjson.decode(ej_data)
                        if ej.status == active_pending or ej.status == active_running then
                            return ej_data
                        end
                    end
                    redis.call('DEL', unique_key)
                end

                -- Store job and queue indices.
                redis.call('SET', job_key, job_json)
                redis.call('SADD', status_key, job_id)
                redis.call('ZADD', queue_key, score, job_id)
                redis.call('SADD', by_queue_key, job_id)
                redis.call('SADD', by_task_key, job_id)
                redis.call('ZADD', all_key, -created_at, job_id)
                redis.call('SET', unique_key, job_id)

                -- Pub/sub delivery: mirror into its subscription's pending
                -- backlog index (KEYS[8], present only for pub/sub deliveries),
                -- scored by created_at. Folded into this atomic store so the
                -- backlog index cannot desync from the job on a crash.
                if KEYS[8] then
                    redis.call('ZADD', KEYS[8], created_at, job_id)
                end

                -- A debounce key makes the job a slide target while it stays
                -- pending. Its index sits at the KEYS slot ARGV[9] names,
                -- because both optional keys can be absent independently.
                if debounce_slot > 0 then
                    redis.call('ZADD', KEYS[debounce_slot], created_at, job_id)
                end

                -- Store dependencies (3 ARGVs per dep: dep_on_key, dep_id, dependents_key).
                for i = 1, num_deps do
                    local offset = dep_args_base + (i - 1) * 3
                    local dep_on_key = ARGV[offset]
                    local dep_id = ARGV[offset + 1]
                    local dependents_key = ARGV[offset + 2]
                    redis.call('SADD', dep_on_key, dep_id)
                    redis.call('SADD', dependents_key, job_id)
                end

                return nil
                "#,
            );

            let job_key = self.key(&["job", &job.id]);
            let status_key = self.key(&["jobs", "status", &(job.status as i32).to_string()]);
            let queue_key = self.key(&["queue", &job.queue, "pending"]);
            let by_queue_key = self.key(&["jobs", "by_queue", &job.queue]);
            let by_task_key = self.key(&["jobs", "by_task", &job.task_name]);
            let all_key = self.key(&["jobs", "all"]);
            let score = dequeue_score(job.priority, job.scheduled_at);
            let job_key_prefix = self.key(&["job", ""]);

            // Build keys and args vectors to avoid temporary lifetime issues.
            // KEYS[8] (the subscription's pending backlog index) is appended
            // only for pub/sub deliveries, so ordinary jobs pass 7 keys and the
            // Lua's `if KEYS[8]` guard is false.
            let mut keys = vec![
                unique_key.clone(),
                job_key,
                status_key,
                queue_key,
                by_queue_key,
                by_task_key,
                all_key,
            ];
            if let Some((topic, name)) =
                crate::pubsub::extract_topic_subscription(job.notes.as_deref())
            {
                keys.push(self.key(&["sub", "pending", &topic, &name]));
            }
            // The debounce index goes last, and the Lua reads its slot from
            // ARGV: it and the pub/sub backlog key are independently optional,
            // so neither can own a fixed position.
            let debounce_slot = match self.job_debounce_index_key(&job) {
                Some(debounce_key) => {
                    keys.push(debounce_key);
                    keys.len()
                }
                None => 0,
            };
            let mut args: Vec<String> = vec![
                job.id.clone(),
                job_json.clone(),
                score.to_string(),
                job.created_at.to_string(),
                depends_on.len().to_string(),
                job_key_prefix,
                active_pending.to_string(),
                active_running.to_string(),
                debounce_slot.to_string(),
            ];

            for dep_id in &depends_on {
                args.push(self.key(&["job", &job.id, "depends_on"]));
                args.push(dep_id.clone());
                args.push(self.key(&["job", dep_id, "dependents"]));
            }

            let mut invocation = store_script.prepare_invoke();
            for k in &keys {
                invocation.key(k);
            }
            for a in &args {
                invocation.arg(a);
            }
            let result: Option<String> = invocation.invoke(&mut conn).map_err(map_err)?;

            if let Some(existing_data) = result {
                // Lost the race — another caller created a job first
                let existing_job: Job = serde_json::from_str(&existing_data)?;
                return Ok((existing_job, true));
            }

            Ok((job, false))
        } else {
            // No key, so nothing to dedupe against: an insert every time.
            self.enqueue(new_job).map(|job| (job, false))
        }
    }
}
