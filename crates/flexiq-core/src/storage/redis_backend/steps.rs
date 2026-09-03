//! Durable inline steps on Redis.
//!
//! One hash per job under the usual prefix — `{prefix}job_steps:{job_id}` —
//! holding a JSON document per position, a `k:<step_key>` uniqueness index, and
//! the reserved bookkeeping fields:
//!
//! ```text
//! <seq>          → "{created_at}\n{json}"   -- decimal seq
//! k:<step_key>   → <seq>                    -- what UNIQUE(job_id, step_key) gives Diesel
//! __total        → running result_len sum
//! __wake         → latest sleep deadline, for the TTL
//! __ns           → the owning job's namespace, denormalised like the Diesel column
//! ```
//!
//! A hash rather than the list `job_errors` uses, because a step lookup is by
//! position. The `k:` fields carry what the Diesel side gets from its second
//! unique index: `HSETNX` on `<seq>` alone would happily accept the same
//! explicit key at two positions, which is exactly the collision an unordered
//! loop's `key=` exists to reject.
//!
//! **Every write is one Lua script, never `HSETNX` plus `MULTI`.** `MULTI` is
//! not conditional: it cannot check a cap and abandon the write when the check
//! fails, so a rejected commit would still have moved `__total`, and two
//! concurrent commits could each read a total under the cap and both write.
//! Lua's single-threaded execution gives the conditional the transaction cannot.
//!
//! The scripts **decode** JSON but never re-encode it: `lua-cjson` turns an
//! empty `[]` into `{}`, so a document round-tripped through Lua would come back
//! undecodable in Rust. The `{created_at}\n` prefix exists so an identical
//! re-commit can be recognised by comparing the document as an opaque string —
//! only the timestamp differs between a commit and its retransmission.

use redis::Commands;
use serde::{Deserialize, Serialize};

use crate::error::{QueueError, Result};
use crate::job::{now_millis, JobStatus};
use crate::step::StepLimits;
use crate::storage::records::{
    AttemptFence, JobStep, NewJobStep, SleepOutcome, StepCommit, StepKind,
};
use crate::storage::redis_backend::{map_err, RedisStorage};

use super::jobs::dequeue_score;

/// Running total of committed `result_len`, so the per-job cap is one `HGET`.
const TOTAL_FIELD: &str = "__total";
/// Latest sleep deadline in the hash — what sizes the TTL.
const WAKE_FIELD: &str = "__wake";
/// The owning job's namespace, denormalised so a scoped read stays one key.
const NAMESPACE_FIELD: &str = "__ns";

/// How long a step hash outlives its last write.
///
/// The TTL is a backstop for the crash window between a terminal write and its
/// `DEL` that Lua cannot close; it is *not* a retention policy, which is why it
/// is sized from the job's own sleep rather than from a constant. A flat seven
/// days refreshed only on commit would expire the snapshot of a job sleeping for
/// thirty, and the wake attempt would then silently re-run every committed step
/// — the exact failure the feature exists to prevent, arriving only for long
/// sleeps. Both arms carry the same grace, so a job that never sleeps is
/// unaffected.
const TTL_GRACE_MS: i64 = 7 * 24 * 60 * 60 * 1000;

/// The stored half of a step row. `created_at` is deliberately outside it: it is
/// the one field that legitimately differs between a commit and its
/// retransmission, and keeping it out is what makes the identical-re-commit
/// check a plain string comparison in Lua.
///
/// `result` is **base64**, not the byte array `serde_json` would write for a
/// `Vec<u8>`. JSON has no binary type, so the natural encoding is `[1,2,3,…]` —
/// three to four bytes of storage per byte of payload. The caps are measured on
/// the payload, so a 256 KiB step would quietly occupy near a megabyte here and
/// mean something different from the same step on a Diesel backend, where the
/// column is a BLOB. Base64 costs 4 bytes per 3 and keeps one meaning of "256
/// KiB" across all three backends.
#[derive(Serialize, Deserialize)]
struct StepDoc {
    step_key: String,
    kind: String,
    #[serde(with = "base64_bytes")]
    result: Option<Vec<u8>>,
    wake_at: Option<i64>,
}

/// `Option<Vec<u8>>` as a base64 string (or JSON null), for [`StepDoc::result`].
mod base64_bytes {
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine as _;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub(super) fn serialize<S: Serializer>(
        bytes: &Option<Vec<u8>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        bytes
            .as_ref()
            .map(|raw| STANDARD.encode(raw))
            .serialize(serializer)
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Vec<u8>>, D::Error> {
        let encoded = Option::<String>::deserialize(deserializer)?;
        encoded
            .map(|text| STANDARD.decode(text).map_err(serde::de::Error::custom))
            .transpose()
    }
}

/// The fence of §1.4, shared by every script that needs it.
///
/// `KEYS[1]` job document · `KEYS[2]` claim key · `KEYS[3]` claim time index.
/// `ARGV[1]` job id · `ARGV[2]` owner · `ARGV[3]` attempt · `ARGV[4]` now ·
/// `ARGV[5]` the wire name of `Running` · `ARGV[6]` the namespace scope, empty
/// for unscoped.
///
/// `reassert` is what separates a write from a check, and it appends a clause
/// rather than selecting between two copies of the fence — two copies is
/// exactly how the backends would come to disagree about what the fence is.
/// A step write puts an age-swept claim back, because the writes after it need
/// one to fence against. A read-only check must not: it would leave a claim its
/// caller never asked for.
///
/// `epoch_argv` is the position of the caller's epoch, which every script
/// appends as its **last** `ARGV`. Interpolated rather than fixed because the
/// fence is a prefix: pinning it to a low index would renumber every
/// script-specific argument that follows, and those numbers are load-bearing —
/// `sleep_job_script` even passes *KEYS positions* as `ARGV` values.
fn fence(reassert: bool, epoch_argv: usize) -> String {
    let mut lua = FENCE.replace("ARGV[EPOCH]", &format!("ARGV[{epoch_argv}]"));
    if reassert {
        lua.push_str(&FENCE_REASSERT.replace("ARGV[EPOCH]", &format!("ARGV[{epoch_argv}]")));
    }
    lua
}

/// The four-case resolution. Reads only; `claim` stays in scope for the clause
/// below, which is the whole reason the owner check is not written as an
/// if/else.
const FENCE: &str = r#"
    local jobdoc = redis.call('GET', KEYS[1])
    if not jobdoc then return {'claim_lost'} end
    local job = cjson.decode(jobdoc)
    if job.status ~= ARGV[5] then return {'claim_lost'} end
    if tonumber(job.retry_count) ~= tonumber(ARGV[3]) then return {'claim_lost'} end
    local job_ns = job.namespace
    if job_ns == cjson.null then job_ns = '' end
    if ARGV[6] ~= '' and job_ns ~= ARGV[6] then return {'claim_lost'} end
    local claim = redis.call('GET', KEYS[2])
    if claim then
        -- Owner is everything before the LAST ':' (the rest of the value is a
        -- numeric suffix); the owner itself may contain ':' (e.g. "host:pid").
        local owner = string.match(claim, '^(.*):') or claim
        if owner ~= ARGV[2] then return {'claim_lost'} end
        -- The epoch separates two claims one owner won at one attempt, which
        -- `requeue_stuck` produces every time an operator unsticks a job an
        -- executor is still running. Compared only when both sides have one:
        -- a claim written before the epoch existed, and a caller that was
        -- never handed a lease, are each an absence rather than a mismatch.
        local claim_epoch = string.match(claim, ':%d+%.(%d+)$')
        if claim_epoch and ARGV[EPOCH] ~= '' and claim_epoch ~= ARGV[EPOCH] then
            return {'claim_lost'}
        end
    end
"#;

/// Put an age-swept claim back before a write proceeds. Claims are swept by
/// age, so a job that legitimately outruns the cutoff finds its own claim gone
/// while still being the only thing executing — abandoning a live attempt there
/// would be a stall caused entirely by housekeeping. Safe because `SET NX`
/// semantics are unnecessary here: the fence above already established that
/// nothing else holds it, inside the same single-threaded script.
///
/// Re-asserted under the caller's own epoch, not a fresh one: this puts back
/// the claim the age sweep removed from a still-running attempt, so it has to
/// come back as the same claim the lease already names.
const FENCE_REASSERT: &str = r#"
    if not claim then
        local value = ARGV[2] .. ':' .. ARGV[4]
        if ARGV[EPOCH] ~= '' then value = value .. '.' .. ARGV[EPOCH] end
        redis.call('SET', KEYS[2], value, 'PX', 86400000)
        redis.call('ZADD', KEYS[3], ARGV[4], ARGV[1])
    end
"#;

/// Render a caller's claim epoch for the fence's `ARGV`.
///
/// The empty string is "I hold no lease", which the fence reads as an absence
/// rather than a mismatch. `redis::Script` has no null argument, so the absence
/// has to be a value the script can recognise, and an epoch is always digits.
fn epoch_arg(epoch: Option<i64>) -> String {
    epoch.map(|e| e.to_string()).unwrap_or_default()
}

/// The fence on its own, for a result the scheduler is about to act on.
fn authorize_attempt_script() -> String {
    format!("{}\n    return {{'ok'}}\n", fence(false, 7))
}

/// Commit one step.
///
/// `KEYS[4]` steps hash. `ARGV[7]` seq · `ARGV[8]` step key · `ARGV[9]` the
/// document · `ARGV[10]` result_len · `ARGV[11]` max_steps · `ARGV[12]`
/// max_total_bytes · `ARGV[13]` kind · `ARGV[14]` created_at · `ARGV[15]` the
/// caller's claim epoch, empty when it has none.
fn record_step_script() -> String {
    let fence = fence(true, 15);
    format!(
        r#"{fence}
    local steps = KEYS[4]
    local seq = ARGV[7]
    local stored = redis.call('HGET', steps, seq)
    if stored then
        local doc = string.match(stored, '^%d+\n(.*)$')
        if doc == ARGV[9] then return {{'already'}} end
        local d = cjson.decode(doc)
        if d.step_key ~= ARGV[8] or d.kind ~= ARGV[13] then
            return {{'diverged', d.kind, d.step_key}}
        end
        return {{'diverged_result', d.kind, d.step_key}}
    end
    local taken = redis.call('HGET', steps, 'k:' .. ARGV[8])
    if taken then return {{'key_taken', taken}} end
    local n = tonumber(seq)
    if n > 0 and redis.call('HEXISTS', steps, tostring(n - 1)) == 0 then
        return {{'gap', tostring(n)}}
    end
    if n + 1 > tonumber(ARGV[11]) then return {{'too_many', tostring(n + 1)}} end
    local total = tonumber(redis.call('HGET', steps, '{TOTAL_FIELD}')) or 0
    local grown = total + tonumber(ARGV[10])
    if grown > tonumber(ARGV[12]) then return {{'too_big', tostring(grown)}} end
    redis.call('HSET', steps, seq, ARGV[14] .. '\n' .. ARGV[9])
    redis.call('HSET', steps, 'k:' .. ARGV[8], seq)
    redis.call('HSET', steps, '{TOTAL_FIELD}', tostring(grown))
    redis.call('HSETNX', steps, '{NAMESPACE_FIELD}', job_ns)
    local wake = tonumber(redis.call('HGET', steps, '{WAKE_FIELD}')) or 0
    local deadline = tonumber(ARGV[4])
    if wake > deadline then deadline = wake end
    redis.call('PEXPIREAT', steps, deadline + {TTL_GRACE_MS})
    return {{'ok'}}
"#
    )
}

/// End the attempt in a sleep: commit the row, release the claim, and put the
/// job back to `Pending` at its deadline — one script, so no crash can leave the
/// job `Running` with an unreached deadline for the stale reaper to find.
///
/// `KEYS[4]` steps hash · `KEYS[5]` running status set · `KEYS[6]` pending
/// status set · `KEYS[7]` per-queue pending zset, then any optional index keys
/// the caller appended. `ARGV[7]` seq · `ARGV[8]` step key · `ARGV[9]` document
/// · `ARGV[10]` max_steps · `ARGV[11]` created_at · `ARGV[12]` whether a row is
/// expected at this position · `ARGV[13]` the patched job JSON · `ARGV[14]`
/// dequeue score · `ARGV[15]` the deadline · `ARGV[16]` job `created_at` ·
/// `ARGV[17..19]` the KEYS index of the debounce, sub-pending and sub-running
/// indices, or `0` · `ARGV[20]` the caller's claim epoch, empty when it has
/// none.
fn sleep_job_script() -> String {
    let fence = fence(true, 20);
    format!(
        r#"{fence}
    local steps = KEYS[4]
    local seq = ARGV[7]
    local stored = redis.call('HGET', steps, seq)
    if ARGV[12] == '1' then
        -- The caller read this row to learn the deadline it must reschedule to;
        -- if it changed underneath, its JSON is stale and it must read again.
        if not stored then return {{'retry'}} end
        local d = cjson.decode(string.match(stored, '^%d+\n(.*)$'))
        if d.step_key ~= ARGV[8] or d.kind ~= 'sleep' then
            return {{'diverged', d.kind, d.step_key}}
        end
        if d.wake_at ~= tonumber(ARGV[15]) then return {{'retry'}} end
    else
        if stored then return {{'retry'}} end
        local taken = redis.call('HGET', steps, 'k:' .. ARGV[8])
        if taken then return {{'key_taken', taken}} end
        local n = tonumber(seq)
        if n > 0 and redis.call('HEXISTS', steps, tostring(n - 1)) == 0 then
            return {{'gap', tostring(n)}}
        end
        if n + 1 > tonumber(ARGV[10]) then return {{'too_many', tostring(n + 1)}} end
        redis.call('HSET', steps, seq, ARGV[11] .. '\n' .. ARGV[9])
        redis.call('HSET', steps, 'k:' .. ARGV[8], seq)
        redis.call('HSETNX', steps, '{NAMESPACE_FIELD}', job_ns)
        local wake = tonumber(redis.call('HGET', steps, '{WAKE_FIELD}')) or 0
        if tonumber(ARGV[15]) > wake then
            redis.call('HSET', steps, '{WAKE_FIELD}', ARGV[15])
        end
    end
    local wake = tonumber(redis.call('HGET', steps, '{WAKE_FIELD}')) or 0
    local deadline = tonumber(ARGV[4])
    if wake > deadline then deadline = wake end
    redis.call('PEXPIREAT', steps, deadline + {TTL_GRACE_MS})

    redis.call('SET', KEYS[1], ARGV[13])
    redis.call('SREM', KEYS[5], ARGV[1])
    redis.call('SADD', KEYS[6], ARGV[1])
    redis.call('ZADD', KEYS[7], ARGV[14], ARGV[1])
    redis.call('DEL', KEYS[2])
    redis.call('ZREM', KEYS[3], ARGV[1])
    local debounce = tonumber(ARGV[17])
    if debounce > 0 then redis.call('ZADD', KEYS[debounce], ARGV[16], ARGV[1]) end
    local sub_pending = tonumber(ARGV[18])
    if sub_pending > 0 then redis.call('ZADD', KEYS[sub_pending], ARGV[16], ARGV[1]) end
    local sub_running = tonumber(ARGV[19])
    if sub_running > 0 then redis.call('SREM', KEYS[sub_running], ARGV[1]) end
    return {{'ok'}}
"#
    )
}

/// Namespace check, count and delete in one pass.
///
/// Three round trips would let a commit land between the count and the `DEL`
/// — deleted but not counted — or between the namespace check and the `DEL`,
/// under a check that no longer describes the hash. Every other write here is
/// one script for the same reason.
const DELETE_STEPS: &str = r#"
    if ARGV[1] ~= '' and redis.call('HGET', KEYS[1], '__ns') ~= ARGV[1] then return 0 end
    local committed = 0
    for _, field in ipairs(redis.call('HKEYS', KEYS[1])) do
        if string.match(field, '^%d+$') then committed = committed + 1 end
    end
    redis.call('DEL', KEYS[1])
    return committed
"#;

impl RedisStorage {
    /// The hash holding one job's committed steps.
    pub(in crate::storage::redis_backend) fn job_steps_key(&self, job_id: &str) -> String {
        self.key(&["job_steps", job_id])
    }

    /// Whether this backend implements the step store.
    pub fn supports_steps(&self) -> bool {
        true
    }

    /// Every committed step for a job, ordered by `seq`.
    pub fn get_job_steps(&self, job_id: &str, namespace: Option<&str>) -> Result<Vec<JobStep>> {
        let mut conn = self.conn()?;
        let fields: std::collections::HashMap<String, String> =
            conn.hgetall(self.job_steps_key(job_id)).map_err(map_err)?;
        if fields.is_empty() {
            return Ok(Vec::new());
        }
        if let Some(scope) = namespace {
            if fields.get(NAMESPACE_FIELD).map(String::as_str) != Some(scope) {
                return Ok(Vec::new());
            }
        }

        let mut steps = Vec::new();
        for (field, value) in &fields {
            let Ok(seq) = field.parse::<i32>() else {
                continue;
            };
            steps.push(decode_step(job_id, seq, value)?);
        }
        steps.sort_by_key(|step: &JobStep| step.seq);
        Ok(steps)
    }

    /// Commit one step, fenced on the execution claim.
    pub fn record_step_result(
        &self,
        step: &NewJobStep<'_>,
        owner: &str,
        attempt: i32,
        epoch: Option<i64>,
        limits: &StepLimits,
        namespace: Option<&str>,
    ) -> Result<StepCommit> {
        let limits = limits.clamped();
        let payload = step.result.unwrap_or(&[]);
        // Measured on the encoded bytes — post serializer, post codec — because
        // that is what is stored.
        if payload.len() > limits.max_step_bytes {
            return Err(QueueError::StepLimitExceeded {
                step_key: step.step_key.to_string(),
                limit: "step bytes".to_string(),
                actual: payload.len() as u64,
                allowed: limits.max_step_bytes as u64,
            });
        }

        let mut conn = self.conn()?;
        let now = now_millis();
        let doc = encode_doc(step, None)?;
        let reply: Vec<String> = redis::Script::new(&record_step_script())
            .key(self.key(&["job", step.job_id]))
            .key(self.key(&["exec_claim", step.job_id]))
            .key(self.key(&["exec_claims", "by_time"]))
            .key(self.job_steps_key(step.job_id))
            .arg(step.job_id)
            .arg(owner)
            .arg(attempt)
            .arg(now)
            .arg(JobStatus::Running.wire_name())
            .arg(namespace.unwrap_or(""))
            .arg(step.seq)
            .arg(step.step_key)
            .arg(&doc)
            .arg(payload.len() as i64)
            .arg(limits.max_steps as i64)
            .arg(limits.max_total_bytes as i64)
            .arg(step.kind.as_str())
            .arg(now)
            .arg(epoch_arg(epoch))
            .invoke(&mut conn)
            .map_err(map_err)?;

        match reply.first().map(String::as_str) {
            Some("ok") => Ok(StepCommit::Committed),
            Some("already") => Ok(StepCommit::AlreadyCommitted),
            _ => Err(step_reply_error(step, &limits, &reply)),
        }
    }

    /// End the attempt in a sleep.
    ///
    /// Two round trips, not one, and deliberately: the job document the script
    /// writes has to carry the deadline the job is actually rescheduled to, and
    /// on a replay that is the *stored* one. Lua cannot patch the document
    /// itself — re-encoding through `lua-cjson` corrupts an empty payload — so
    /// the deadline is read first and the script re-checks what was read.
    /// A committed sleep row is immutable, so the check can only fail once.
    #[allow(clippy::too_many_arguments)]
    pub fn sleep_job(
        &self,
        step: &NewJobStep<'_>,
        owner: &str,
        attempt: i32,
        epoch: Option<i64>,
        wake_at: i64,
        limits: &StepLimits,
        namespace: Option<&str>,
    ) -> Result<SleepOutcome> {
        let limits = limits.clamped();
        let script = redis::Script::new(&sleep_job_script());

        for _ in 0..3 {
            let mut conn = self.conn()?;
            let stored = self.stored_sleep(&mut conn, step)?;
            let deadline = stored.unwrap_or(wake_at);

            let mut job = self.get_job_required_in(step.job_id, namespace)?;
            let old_status = job.status;
            job.status = JobStatus::Pending;
            job.scheduled_at = deadline;
            job.started_at = None;
            job.completed_at = None;
            job.error = None;
            let job_json = serde_json::to_string(&job)?;

            let mut invocation = script.prepare_invoke();
            invocation
                .key(self.key(&["job", step.job_id]))
                .key(self.key(&["exec_claim", step.job_id]))
                .key(self.key(&["exec_claims", "by_time"]))
                .key(self.job_steps_key(step.job_id))
                .key(self.key(&["jobs", "status", &(old_status as i32).to_string()]))
                .key(self.key(&["jobs", "status", &(JobStatus::Pending as i32).to_string()]))
                .key(self.key(&["queue", &job.queue, "pending"]));

            // Optional index keys are appended and addressed by their position,
            // so an absent one costs nothing and the script needs no placeholder.
            let mut optional = Vec::new();
            let debounce_slot = push_slot(&mut optional, self.job_debounce_index_key(&job));
            let (sub_pending, sub_running) = self.sub_backlog_keys(&job);
            let sub_pending_slot = push_slot(&mut optional, sub_pending);
            let sub_running_slot = push_slot(&mut optional, sub_running);
            for key in &optional {
                invocation.key(key);
            }

            let reply: Vec<String> = invocation
                .arg(step.job_id)
                .arg(owner)
                .arg(attempt)
                .arg(now_millis())
                .arg(JobStatus::Running.wire_name())
                .arg(namespace.unwrap_or(""))
                .arg(step.seq)
                .arg(step.step_key)
                .arg(encode_doc(step, Some(deadline))?)
                .arg(limits.max_steps as i64)
                .arg(now_millis())
                .arg(i32::from(stored.is_some()))
                .arg(&job_json)
                .arg(dequeue_score(job.priority, deadline))
                .arg(deadline)
                .arg(job.created_at)
                .arg(debounce_slot)
                .arg(sub_pending_slot)
                .arg(sub_running_slot)
                .arg(epoch_arg(epoch))
                .invoke(&mut conn)
                .map_err(map_err)?;

            match reply.first().map(String::as_str) {
                Some("ok") if stored.is_some() => {
                    return Ok(SleepOutcome::AlreadySleeping { wake_at: deadline })
                }
                Some("ok") => return Ok(SleepOutcome::Slept { wake_at: deadline }),
                Some("retry") => continue,
                _ => return Err(step_reply_error(step, &limits, &reply)),
            }
        }

        Err(QueueError::Other(format!(
            "sleep for job {} could not settle on a deadline",
            step.job_id
        )))
    }

    /// Whether a result carrying `(owner, attempt, epoch)` still speaks for
    /// this job.
    pub fn authorize_attempt(
        &self,
        job_id: &str,
        owner: &str,
        attempt: i32,
        epoch: Option<i64>,
        namespace: Option<&str>,
    ) -> Result<AttemptFence> {
        let mut conn = self.conn()?;
        let reply: Vec<String> = redis::Script::new(&authorize_attempt_script())
            .key(self.key(&["job", job_id]))
            .key(self.key(&["exec_claim", job_id]))
            .key(self.key(&["exec_claims", "by_time"]))
            .arg(job_id)
            .arg(owner)
            .arg(attempt)
            .arg(now_millis())
            .arg(JobStatus::Running.wire_name())
            .arg(namespace.unwrap_or(""))
            .arg(epoch_arg(epoch))
            .invoke(&mut conn)
            .map_err(map_err)?;

        Ok(match reply.first().map(String::as_str) {
            Some("ok") => AttemptFence::Authorized,
            _ => AttemptFence::Superseded,
        })
    }

    /// Drop every step row for a job. The explicit admin entry point.
    pub fn delete_job_steps(&self, job_id: &str, namespace: Option<&str>) -> Result<u64> {
        let mut conn = self.conn()?;
        // One `DEL` removes every position at once, so the count comes from the
        // field names rather than from the reply — and reading the names costs
        // nothing next to decoding every blob.
        let committed: i64 = redis::Script::new(DELETE_STEPS)
            .key(self.job_steps_key(job_id))
            .arg(namespace.unwrap_or(""))
            .invoke(&mut conn)
            .map_err(map_err)?;
        Ok(committed.max(0) as u64)
    }

    /// The deadline already committed at this position, if any.
    fn stored_sleep(
        &self,
        conn: &mut redis::Connection,
        step: &NewJobStep<'_>,
    ) -> Result<Option<i64>> {
        let stored: Option<String> = conn
            .hget(self.job_steps_key(step.job_id), step.seq.to_string())
            .map_err(map_err)?;
        let Some(stored) = stored else {
            return Ok(None);
        };
        let committed = decode_step(step.job_id, step.seq, &stored)?;
        if committed.kind != StepKind::Sleep || committed.step_key != step.step_key {
            return Err(QueueError::StepDiverged {
                job_id: step.job_id.to_string(),
                seq: step.seq,
                expected: format!(
                    "a {} step '{}'",
                    committed.kind.as_str(),
                    committed.step_key
                ),
                found: format!("a sleep step '{}'", step.step_key),
            });
        }
        committed
            .wake_at
            .ok_or_else(|| QueueError::StepDiverged {
                job_id: step.job_id.to_string(),
                seq: step.seq,
                expected: "a sleep step with a deadline".to_string(),
                found: format!("'{}' with none", committed.step_key),
            })
            .map(Some)
    }
}

/// Where the first optional index key lands in `KEYS`.
const FIXED_SLEEP_KEYS: usize = 7;

/// Append an optional index key and return the `KEYS` slot it took, or `0` when
/// there is nothing to append.
fn push_slot(keys: &mut Vec<String>, key: Option<String>) -> usize {
    match key {
        Some(key) => {
            keys.push(key);
            FIXED_SLEEP_KEYS + keys.len()
        }
        None => 0,
    }
}

/// The `{created_at}\n{json}` value a step field holds.
fn decode_step(job_id: &str, seq: i32, stored: &str) -> Result<JobStep> {
    let (created_at, json) = stored.split_once('\n').ok_or_else(|| {
        QueueError::Serialization(format!("malformed step row for job {job_id} at {seq}"))
    })?;
    let created_at: i64 = created_at.parse().map_err(|_| {
        QueueError::Serialization(format!(
            "malformed step timestamp for job {job_id} at {seq}"
        ))
    })?;
    let doc: StepDoc = serde_json::from_str(json)?;
    Ok(JobStep {
        job_id: job_id.to_string(),
        seq,
        step_key: doc.step_key,
        kind: StepKind::from_wire(&doc.kind),
        result: doc.result,
        wake_at: doc.wake_at,
        created_at,
    })
}

/// The comparable half of a step row — everything but its timestamp.
fn encode_doc(step: &NewJobStep<'_>, wake_at: Option<i64>) -> Result<String> {
    Ok(serde_json::to_string(&StepDoc {
        step_key: step.step_key.to_string(),
        kind: step.kind.as_str().to_string(),
        result: step.result.map(<[u8]>::to_vec),
        wake_at,
    })?)
}

/// Turn a script's refusal into the error the trait promises.
fn step_reply_error(step: &NewJobStep<'_>, limits: &StepLimits, reply: &[String]) -> QueueError {
    let detail = |index: usize| reply.get(index).cloned().unwrap_or_default();
    match reply.first().map(String::as_str) {
        Some("claim_lost") => QueueError::ClaimLost(step.job_id.to_string()),
        Some("diverged") => QueueError::StepDiverged {
            job_id: step.job_id.to_string(),
            seq: step.seq,
            expected: format!("a {} step '{}'", detail(1), detail(2)),
            found: format!("a {} step '{}'", step.kind.as_str(), step.step_key),
        },
        Some("diverged_result") => QueueError::StepDiverged {
            job_id: step.job_id.to_string(),
            seq: step.seq,
            expected: format!("the stored result of '{}'", detail(2)),
            found: "a different result for the same step".to_string(),
        },
        Some("key_taken") => QueueError::StepDiverged {
            job_id: step.job_id.to_string(),
            seq: step.seq,
            expected: "an unused step key".to_string(),
            found: format!(
                "'{}', already committed at position {}",
                step.step_key,
                detail(1)
            ),
        },
        Some("gap") => QueueError::StepDiverged {
            job_id: step.job_id.to_string(),
            seq: step.seq,
            expected: "the next unused position".to_string(),
            found: format!("position {}", step.seq),
        },
        Some("too_many") => QueueError::StepLimitExceeded {
            step_key: step.step_key.to_string(),
            limit: "step count".to_string(),
            actual: detail(1).parse().unwrap_or_default(),
            allowed: limits.max_steps as u64,
        },
        Some("too_big") => QueueError::StepLimitExceeded {
            step_key: step.step_key.to_string(),
            limit: "total bytes".to_string(),
            actual: detail(1).parse().unwrap_or_default(),
            allowed: limits.max_total_bytes as u64,
        },
        other => QueueError::Other(format!(
            "unexpected step reply for job {}: {}",
            step.job_id,
            other.unwrap_or("<empty>")
        )),
    }
}
