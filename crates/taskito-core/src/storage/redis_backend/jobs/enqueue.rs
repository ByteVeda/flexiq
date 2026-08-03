//! Enqueue, batch enqueue, and unique-keyed enqueue.

use redis::Commands;

use super::dequeue_score;
use crate::error::{QueueError, Result};
use crate::job::{Job, JobStatus, NewJob};
use crate::storage::redis_backend::{map_err, RedisStorage};

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

    /// Enqueue with `unique_key` deduplication: a Lua script atomically returns
    /// the existing active job when a duplicate is found instead of inserting.
    pub fn enqueue_unique(&self, new_job: NewJob) -> Result<Job> {
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
                return Ok(job);
            }

            // No active duplicate — enqueue normally
            let depends_on = new_job.depends_on.clone();
            let job = new_job.into_job();
            let job_json = serde_json::to_string(&job)?;

            self.validate_dep_ids(&mut conn, &depends_on, job.namespace.as_deref(), None)?;

            // Store everything atomically via Lua. Active-status names are
            // passed via ARGV (positions 7-8) for the same reason as above —
            // single-sourced in `JobStatus::wire_name()`. Dependency triples
            // start at ARGV[9].
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
                local dep_args_base = 9

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
            let mut args: Vec<String> = vec![
                job.id.clone(),
                job_json.clone(),
                score.to_string(),
                job.created_at.to_string(),
                depends_on.len().to_string(),
                job_key_prefix,
                active_pending.to_string(),
                active_running.to_string(),
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
                return Ok(existing_job);
            }

            Ok(job)
        } else {
            self.enqueue(new_job)
        }
    }
}
