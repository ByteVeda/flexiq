//! Dequeue jobs from one or more queues.

use std::sync::LazyLock;

use crate::error::Result;
use crate::job::{Job, JobStatus};
use crate::storage::redis_backend::{map_err, RedisConnection, RedisStorage};

/// Lua: select and claim up to `max` ready jobs from one queue in a single
/// round trip. Candidates are read in score order; a ready one is flipped
/// Pending→Running under the same `SISMEMBER jobs:status:0` guard as
/// [`CLAIM_JOB_SCRIPT`], so a job a concurrent cancel/expire already archived
/// is never resurrected, and Redis's atomic script execution rules out a
/// double claim between schedulers.
///
/// A candidate is decoded only once raw-token checks say it could be claimed
/// (Pending, due, this namespace), so a scan past future, finished or
/// foreign-namespace jobs costs string searches, not full JSON decodes.
/// The document is decoded but never re-encoded — `lua-cjson` rewrites an empty
/// `[]` payload as `{}` — so the claim patches it by swapping the exact
/// `status` and `started_at` tokens, each of which must occur exactly once.
/// A job with dependencies is claimed in its score-order turn once every dep
/// is `Complete` (live or archived), and skipped otherwise, so a ready
/// dependent is never starved by plain jobs behind it. What the script cannot
/// settle on its own is handed back to Rust: expired jobs (archived with
/// serde) and an unpatchable document (claimed through [`CLAIM_JOB_SCRIPT`]).
///
/// KEYS: queue pending zset, pending status set, running status set.
/// ARGV: job key prefix, archived job key prefix, debounce index key prefix,
///       pending / running / complete wire names, now, namespace mode (`1` =
///       only `ARGV[9]`, `0` = only jobs without a namespace), namespace, max,
///       scan limit.
/// Returns `{claimed_docs, expired_ids, deferred_docs}`.
const SELECT_AND_CLAIM_BODY: &str = r#"
    local function swap_once(doc, from, to)
        local s, e = string.find(doc, from, 1, true)
        if not s or string.find(doc, from, e + 1, true) then return nil end
        return string.sub(doc, 1, s - 1) .. to .. string.sub(doc, e + 1)
    end
    local function present(v)
        return v ~= nil and v ~= cjson.null
    end

    local job_key_prefix = ARGV[1]
    local archived_key_prefix = ARGV[2]
    local debounce_key_prefix = ARGV[3]
    local pending_status = ARGV[4]
    local running_status = ARGV[5]
    local complete_status = ARGV[6]
    local now_arg = ARGV[7]
    local now = tonumber(now_arg)
    local want_namespace = ARGV[8] == '1'
    local namespace = ARGV[9]
    local max = tonumber(ARGV[10])

    -- A completed dep has been archived out of the live keys, so the archive
    -- is the fallback before a dep counts as unsatisfied.
    local function deps_complete(id)
        local deps = redis.call('SMEMBERS', job_key_prefix .. id .. ':depends_on')
        for _, dep in ipairs(deps) do
            local dep_doc = redis.call('GET', job_key_prefix .. dep)
            if not dep_doc then dep_doc = redis.call('GET', archived_key_prefix .. dep) end
            if not dep_doc or cjson.decode(dep_doc).status ~= complete_status then
                return false
            end
        end
        return true
    end

    -- Raw-token prefilter, so a candidate that cannot be claimed (not
    -- Pending, not yet due, another namespace) is skipped without decoding its
    -- whole document on the Redis thread. The `Job` document is one flat serde
    -- object, and serde escapes every `"` inside a string, so an unescaped
    -- `"key":` is always a top-level key, never text inside a value. It only
    -- ever skips; the decoded checks below stay the authority.
    local pending_token = '"status":"' .. pending_status .. '"'
    local function worth_decoding(doc)
        if not string.find(doc, pending_token, 1, true) then return false end
        local scheduled_at = tonumber(string.match(doc, '"scheduled_at":(%-?%d+)'))
        if scheduled_at and scheduled_at > now then return false end
        -- A namespaced scheduler never claims a default-namespace job, and the
        -- default one never claims a namespaced job. A missing key falls
        -- through to the decode, which reads it as no namespace.
        if want_namespace then
            return not string.find(doc, '"namespace":null', 1, true)
        end
        return not string.find(doc, '"namespace":"', 1, true)
    end

    local claimed, expired, deferred = {}, {}, {}
    local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', '+inf', 'LIMIT', 0, ARGV[11])
    for _, id in ipairs(ids) do
        -- Deferred docs spend the budget too: Rust claims them from what is
        -- left, so a head-of-queue deferred job is never starved by plain ones.
        if #claimed + #deferred >= max then break end
        local doc = redis.call('GET', job_key_prefix .. id)
        if not doc then
            -- Stale entry: the job is gone, so drop it from the queue.
            redis.call('ZREM', KEYS[1], id)
        elseif worth_decoding(doc) then
            local job = cjson.decode(doc)
            local job_ns = job.namespace
            if not present(job_ns) then job_ns = nil end
            local ns_ok
            if want_namespace then ns_ok = job_ns == namespace else ns_ok = job_ns == nil end

            if job.status == pending_status and job.scheduled_at <= now and ns_ok then
                if present(job.expires_at) and now > job.expires_at then
                    table.insert(expired, id)
                elseif (job.has_deps ~= true or deps_complete(id))
                    and redis.call('SISMEMBER', KEYS[2], id) == 1 then
                    local patched = swap_once(doc,
                        '"status":"' .. pending_status .. '"',
                        '"status":"' .. running_status .. '"')
                    if patched then
                        patched = swap_once(patched, '"started_at":null', '"started_at":' .. now_arg)
                    end
                    if not patched then
                        table.insert(deferred, doc)
                    else
                        redis.call('SET', job_key_prefix .. id, patched)
                        redis.call('SREM', KEYS[2], id)
                        redis.call('SADD', KEYS[3], id)
                        redis.call('ZREM', KEYS[1], id)
                        -- A claimed job has left its debounce window; the key
                        -- mirrors `namespace_segment` byte for byte.
                        if present(job.debounce_key) then
                            local segment = '-'
                            if job_ns then segment = #job_ns .. ':' .. job_ns end
                            redis.call('ZREM',
                                debounce_key_prefix .. segment .. ':' .. job.debounce_key, id)
                        end
                        table.insert(claimed, patched)
                    end
                end
            end
        end
    end
    return {claimed, expired, deferred}
"#;

/// [`SELECT_AND_CLAIM_BODY`] built once, so its SHA is computed once and every
/// call after the first is an `EVALSHA`.
static SELECT_AND_CLAIM_SCRIPT: LazyLock<redis::Script> =
    LazyLock::new(|| redis::Script::new(SELECT_AND_CLAIM_BODY));

/// Lua: claim a candidate by flipping it Pending→Running, but only if it is
/// still a member of the pending status set (`jobs:status:0`). This is the
/// Redis equivalent of the Diesel `UPDATE ... WHERE status = Pending`
/// affected-row guard: a concurrent cancel/expire that already archived the
/// job has removed it from `jobs:status:0`, so the claim is refused (returns 0)
/// instead of resurrecting the job as a Running orphan. Redis runs the script
/// atomically, which also closes the double-claim window between schedulers.
///
/// KEYS: job_key, pending_status_set, running_status_set, pending_zset,
///       [debounce_zset] (present only for a job carrying a debounce key)
/// ARGV: job_id, running_json
const CLAIM_JOB_SCRIPT: &str = r#"
    if redis.call('SISMEMBER', KEYS[2], ARGV[1]) == 0 then return 0 end
    redis.call('SET', KEYS[1], ARGV[2])
    redis.call('SREM', KEYS[2], ARGV[1])
    redis.call('SADD', KEYS[3], ARGV[1])
    redis.call('ZREM', KEYS[4], ARGV[1])
    if KEYS[5] then
        redis.call('ZREM', KEYS[5], ARGV[1])
    end
    return 1
"#;

/// Candidates `dequeue_batch` hands back to Rust, as `(claimed_docs,
/// expired_ids, deferred_docs)`.
type SelectAndClaimReply = (Vec<String>, Vec<String>, Vec<String>);

impl RedisStorage {
    /// Atomically claim a candidate job (Pending→Running) via [`CLAIM_JOB_SCRIPT`].
    /// The caller must have already set `job.status = Running` and `started_at`.
    /// Returns `false` if the job was concurrently cancelled/expired/claimed, so
    /// the dequeue scan should skip it.
    fn claim_pending(
        &self,
        conn: &mut RedisConnection,
        job: &Job,
        queue_key: &str,
    ) -> Result<bool> {
        let job_json = serde_json::to_string(job)?;
        let job_key = self.key(&["job", &job.id]);
        let pending_status =
            self.key(&["jobs", "status", &(JobStatus::Pending as i32).to_string()]);
        let running_status =
            self.key(&["jobs", "status", &(JobStatus::Running as i32).to_string()]);
        let claim_script = redis::Script::new(CLAIM_JOB_SCRIPT);
        let mut invocation = claim_script.prepare_invoke();
        invocation
            .key(&job_key)
            .key(&pending_status)
            .key(&running_status)
            .key(queue_key);
        // A claimed job has left the debounce window it opened: a later
        // debounced enqueue must insert a fresh job rather than slide this one
        // out from under the worker now holding it. KEYS[5] is absent (and the
        // script's guard false) for a job without a debounce key.
        if let Some(debounce_key) = self.job_debounce_index_key(job) {
            invocation.key(debounce_key);
        }
        let claimed: i32 = invocation
            .arg(&job.id)
            .arg(&job_json)
            .invoke(conn)
            .map_err(map_err)?;
        Ok(claimed == 1)
    }

    /// Archive a job the claim script found expired as cancelled, exactly as
    /// both Diesel paths do. A job that left `Pending` since the script ran is
    /// someone else's to settle.
    fn archive_expired(&self, conn: &mut RedisConnection, job_id: &str, now: i64) -> Result<()> {
        let Some(mut job) = self.load_job(conn, job_id)? else {
            return Ok(());
        };
        if job.status != JobStatus::Pending {
            return Ok(());
        }
        job.status = JobStatus::Cancelled;
        job.completed_at = Some(now);
        job.error = Some("expired before execution".to_string());
        self.archive_job_immediately(conn, &job, JobStatus::Pending)
    }

    /// Atomically claim the highest-priority ready job, moving it to `Running`.
    /// One claim through [`dequeue_batch`](Self::dequeue_batch).
    pub fn dequeue(
        &self,
        queue_name: &str,
        now: i64,
        namespace: Option<&str>,
    ) -> Result<Option<Job>> {
        Ok(self
            .dequeue_batch(queue_name, now, namespace, 1)?
            .into_iter()
            .next())
    }

    /// Dequeue across queues. `orders` is accepted for cross-backend signature
    /// parity but **ignored**: Redis packs priority and `scheduled_at` into a
    /// single ZSET score, so per-priority LIFO would need a second score-inverted
    /// sorted set (a backfill migration of every pending row). Documented as an
    /// exception, like the `list_jobs_after` Redis note; Redis stays FIFO.
    pub fn dequeue_from(
        &self,
        queues: &[String],
        now: i64,
        namespace: Option<&str>,
        _orders: &std::collections::HashMap<String, crate::storage::DispatchOrder>,
    ) -> Result<Option<Job>> {
        for queue_name in queues {
            if let Some(job) = self.dequeue(queue_name, now, namespace)? {
                return Ok(Some(job));
            }
        }
        Ok(None)
    }

    /// Claim up to `max` ready jobs from a single queue. Candidate selection
    /// and the Pending→Running claim run together in one script
    /// (`SELECT_AND_CLAIM_BODY`), so the common case is one round trip and no
    /// candidate can change between being read and being claimed.
    ///
    /// Dependencies are checked inside the script, so jobs come back in score
    /// order. Only a document the script cannot patch falls back to
    /// `CLAIM_JOB_SCRIPT`, with whatever budget is left after the script's
    /// claims.
    pub fn dequeue_batch(
        &self,
        queue_name: &str,
        now: i64,
        namespace: Option<&str>,
        max: usize,
    ) -> Result<Vec<Job>> {
        if max == 0 {
            return Ok(Vec::new());
        }

        let mut conn = self.conn()?;
        let queue_key = self.key(&["queue", queue_name, "pending"]);

        // Scan more candidates than `max` so dependency/expiry skips still
        // leave enough eligible rows to fill the batch, bounded to keep the
        // scripted scan short. The floor keeps single-job `dequeue`'s window.
        let scan_limit = max.saturating_mul(4).clamp(100, 400);

        let mut invocation = SELECT_AND_CLAIM_SCRIPT.prepare_invoke();
        invocation
            .key(&queue_key)
            .key(self.key(&["jobs", "status", &(JobStatus::Pending as i32).to_string()]))
            .key(self.key(&["jobs", "status", &(JobStatus::Running as i32).to_string()]))
            .arg(self.key(&["job", ""]))
            .arg(self.key(&["archived", ""]))
            .arg(self.key(&["jobs", "debounce", ""]))
            .arg(JobStatus::Pending.wire_name())
            .arg(JobStatus::Running.wire_name())
            .arg(JobStatus::Complete.wire_name())
            .arg(now)
            .arg(if namespace.is_some() { "1" } else { "0" })
            .arg(namespace.unwrap_or(""))
            .arg(max)
            .arg(scan_limit);
        let (claimed_docs, expired_ids, deferred_docs): SelectAndClaimReply =
            invocation.invoke(&mut conn).map_err(map_err)?;

        let mut claimed: Vec<Job> = Vec::with_capacity(max);
        for doc in &claimed_docs {
            let job: Job = serde_json::from_str(doc)?;
            // Best-effort pub/sub backlog reindex Pending→Running; a follow-up
            // rather than folded into the claim script (see
            // `reindex_pubsub_best_effort`).
            self.reindex_pubsub_best_effort(&mut conn, &job, JobStatus::Running);
            claimed.push(job);
        }

        for job_id in &expired_ids {
            self.archive_expired(&mut conn, job_id, now)?;
        }

        for doc in &deferred_docs {
            if claimed.len() == max {
                break;
            }
            let mut job: Job = serde_json::from_str(doc)?;
            // Skip candidates lost to a concurrent cancel / expire / claim
            // instead of resurrecting them.
            job.status = JobStatus::Running;
            job.started_at = Some(now);
            if self.claim_pending(&mut conn, &job, &queue_key)? {
                self.reindex_pubsub_best_effort(&mut conn, &job, JobStatus::Running);
                claimed.push(job);
            }
        }

        Ok(claimed)
    }

    /// Claim up to `max` ready jobs across the given queues, checking each in
    /// order until the budget is exhausted. `orders` is accepted but ignored —
    /// see [`dequeue_from`](Self::dequeue_from) for why Redis stays FIFO.
    pub fn dequeue_batch_from(
        &self,
        queues: &[String],
        now: i64,
        namespace: Option<&str>,
        max: usize,
        _orders: &std::collections::HashMap<String, crate::storage::DispatchOrder>,
    ) -> Result<Vec<Job>> {
        let mut claimed: Vec<Job> = Vec::new();
        for queue_name in queues {
            if claimed.len() >= max {
                break;
            }
            let remaining = max - claimed.len();
            let mut batch = self.dequeue_batch(queue_name, now, namespace, remaining)?;
            claimed.append(&mut batch);
        }
        Ok(claimed)
    }
}
