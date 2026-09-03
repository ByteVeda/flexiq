use redis::Commands;

use super::{map_err, RedisStorage};
use crate::error::Result;
use crate::job::now_millis;
use crate::lease::mint_claim_epoch;
use crate::storage::records::LockInfo;

/// Render the value an execution claim is stored under.
///
/// `"{owner}:{claimed_at}.{epoch}"`, and `"{owner}:{claimed_at}"` when there is
/// no epoch — which is what a claim written before this column existed looks
/// like, and what it keeps looking like until its 24-hour expiry.
///
/// The epoch rides the timestamp field rather than being appended as a fourth
/// one because every reader of this value — the reclaim script, the step fence,
/// `reap_orphaned_jobs` — takes the owner as *everything before the last* `:`.
/// A new `:` would move that boundary and silently truncate every owner that
/// contains one (`"host:pid"`, pinned by the contract suite). A `.` inside the
/// final field moves nothing, and the two forms stay distinguishable because a
/// legacy value's last field is digits with no dot.
pub(super) fn claim_value(worker_id: &str, claimed_at: i64, epoch: Option<i64>) -> String {
    match epoch {
        Some(epoch) => format!("{worker_id}:{claimed_at}.{epoch}"),
        None => format!("{worker_id}:{claimed_at}"),
    }
}

/// Lua script: atomically transfer an execution claim from `expected_owner`
/// (ARGV[2]) to `new_owner` (ARGV[3]). The claim value is
/// "{owner}:{ts}.{epoch}"; returns 1 only if the current owner matches.
/// KEYS[1] = claim key, KEYS[2] = the by-time index; ARGV[1] = job_id,
/// ARGV[4] = now, ARGV[5] = the epoch the transfer mints.
const RECLAIM_CLAIM_SCRIPT: &str = r#"
    local cur = redis.call('GET', KEYS[1])
    if not cur then return 0 end
    -- Owner is everything before the LAST ':' (the rest of the value is a
    -- numeric suffix); the owner itself may contain ':' (e.g. "host:pid").
    local owner = string.match(cur, '^(.*):') or cur
    if owner ~= ARGV[2] then return 0 end
    redis.call('SET', KEYS[1], ARGV[3] .. ':' .. ARGV[4] .. '.' .. ARGV[5], 'PX', 86400000)
    redis.call('ZADD', KEYS[2], ARGV[4], ARGV[1])
    return 1
"#;

/// Lua script: release lock only if owner matches.
const RELEASE_LOCK_SCRIPT: &str = r#"
    local key = KEYS[1]
    local owner = ARGV[1]
    local current = redis.call('HGET', key, 'owner_id')
    if current == owner then
        redis.call('DEL', key)
        return 1
    end
    return 0
"#;

/// Lua script: extend lock TTL only if owner matches.
const EXTEND_LOCK_SCRIPT: &str = r#"
    local key = KEYS[1]
    local owner = ARGV[1]
    local new_expires = ARGV[2]
    local current = redis.call('HGET', key, 'owner_id')
    if current == owner then
        redis.call('HSET', key, 'expires_at', new_expires)
        redis.call('PEXPIREAT', key, tonumber(new_expires))
        return 1
    end
    return 0
"#;

/// Lua script: acquire lock atomically (SET NX equivalent with hash).
const ACQUIRE_LOCK_SCRIPT: &str = r#"
    local key = KEYS[1]
    local owner = ARGV[1]
    local acquired_at = ARGV[2]
    local expires_at = ARGV[3]
    local now = ARGV[4]
    local existing_expires = redis.call('HGET', key, 'expires_at')
    if existing_expires and tonumber(existing_expires) > tonumber(now) then
        return 0
    end
    redis.call('HSET', key, 'lock_name', KEYS[2], 'owner_id', owner,
               'acquired_at', acquired_at, 'expires_at', expires_at)
    redis.call('PEXPIREAT', key, tonumber(expires_at))
    return 1
"#;

/// Lua script: delete a lock only if it is still expired at delete time.
/// Re-checking inside the script closes the TOCTOU window where the SCAN-driven
/// reaper would HGET an expired lock, another client re-acquires it, and the
/// reaper then DELs the now-valid lock.
const REAP_LOCK_SCRIPT: &str = r#"
    local exp = redis.call('HGET', KEYS[1], 'expires_at')
    if exp and tonumber(exp) <= tonumber(ARGV[1]) then
        redis.call('DEL', KEYS[1])
        return 1
    end
    return 0
"#;

impl RedisStorage {
    /// Try to take a distributed lock for `ttl_ms` (milliseconds) via a
    /// Lua-atomic check-and-set. Returns `false` while another holder's lock is unexpired.
    pub fn acquire_lock(&self, lock_name: &str, owner_id: &str, ttl_ms: i64) -> Result<bool> {
        let mut conn = self.conn()?;
        let now = now_millis();
        let expires_at = now + ttl_ms;
        let lkey = self.key(&["lock", lock_name]);

        let result: i32 = redis::Script::new(ACQUIRE_LOCK_SCRIPT)
            .key(&lkey)
            .key(lock_name)
            .arg(owner_id)
            .arg(now)
            .arg(expires_at)
            .arg(now)
            .invoke(&mut conn)
            .map_err(map_err)?;

        Ok(result == 1)
    }

    /// Release a lock. Lua-checked: returns `true` only if `owner_id` held it.
    pub fn release_lock(&self, lock_name: &str, owner_id: &str) -> Result<bool> {
        let mut conn = self.conn()?;
        let lkey = self.key(&["lock", lock_name]);

        let result: i32 = redis::Script::new(RELEASE_LOCK_SCRIPT)
            .key(&lkey)
            .arg(owner_id)
            .invoke(&mut conn)
            .map_err(map_err)?;

        Ok(result == 1)
    }

    /// Reset a lock's expiry to `ttl_ms` milliseconds from now (not additive).
    /// Returns `true` only if `owner_id` held it.
    pub fn extend_lock(&self, lock_name: &str, owner_id: &str, ttl_ms: i64) -> Result<bool> {
        let mut conn = self.conn()?;
        let now = now_millis();
        let new_expires = now + ttl_ms;
        let lkey = self.key(&["lock", lock_name]);

        let result: i32 = redis::Script::new(EXTEND_LOCK_SCRIPT)
            .key(&lkey)
            .arg(owner_id)
            .arg(new_expires)
            .invoke(&mut conn)
            .map_err(map_err)?;

        Ok(result == 1)
    }

    /// Holder and expiry of a lock, if it exists.
    pub fn get_lock_info(&self, lock_name: &str) -> Result<Option<LockInfo>> {
        let mut conn = self.conn()?;
        let lkey = self.key(&["lock", lock_name]);

        let data: std::collections::HashMap<String, String> =
            conn.hgetall(&lkey).map_err(map_err)?;

        if data.is_empty() {
            return Ok(None);
        }

        Ok(Some(LockInfo {
            lock_name: data
                .get("lock_name")
                .cloned()
                .unwrap_or_else(|| lock_name.to_string()),
            owner_id: data.get("owner_id").cloned().unwrap_or_default(),
            acquired_at: data
                .get("acquired_at")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0),
            expires_at: data
                .get("expires_at")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0),
        }))
    }

    /// Remove locks expired before `now` (Unix milliseconds); a Lua re-check
    /// closes the scan-then-delete race. Returns the count removed.
    pub fn reap_expired_locks(&self, now: i64) -> Result<u64> {
        let mut conn = self.conn()?;
        let pattern = self.key(&["lock", "*"]);

        let mut count = 0u64;
        let mut cursor: u64 = 0;
        loop {
            let (next_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(100)
                .query(&mut conn)
                .map_err(map_err)?;

            let reap = redis::Script::new(REAP_LOCK_SCRIPT);
            for key in keys {
                let deleted: i64 = reap.key(&key).arg(now).invoke(&mut conn).map_err(map_err)?;
                count += deleted as u64;
            }

            cursor = next_cursor;
            if cursor == 0 {
                break;
            }
        }

        Ok(count)
    }

    /// Claim exclusive execution of a job via `SET NX` with a 24-hour expiry.
    /// Returns the epoch the claim was won under, or `None` when a claim
    /// already exists.
    pub fn claim_execution(&self, job_id: &str, worker_id: &str) -> Result<Option<i64>> {
        let mut conn = self.conn()?;
        let now = now_millis();
        let epoch = mint_claim_epoch();
        let ckey = self.key(&["exec_claim", job_id]);
        let index_key = self.key(&["exec_claims", "by_time"]);

        // NX: set only if not exists. PX: auto-expire after 24 hours so
        // orphaned claims from dead workers don't block re-execution forever.
        let acquired: bool = redis::cmd("SET")
            .arg(&ckey)
            .arg(claim_value(worker_id, now, Some(epoch)))
            .arg("NX")
            .arg("PX")
            .arg(86_400_000i64) // 24 hours in milliseconds
            .query(&mut conn)
            .map_err(map_err)?;

        if acquired {
            // Mirror the claim into a time-indexed sorted set so the
            // scheduler's maintenance loop can purge stale claims with an
            // O(log n) range query.
            conn.zadd::<_, _, _, ()>(&index_key, job_id, now as f64)
                .map_err(map_err)?;
        }

        Ok(acquired.then_some(epoch))
    }

    /// Batch variant of `claim_execution`. Issues all NX sets in one pipeline,
    /// then mirrors only the won claims into the by-time index in a second
    /// pipeline — two round trips regardless of batch size. Returns one flag per
    /// input id, in order: the epoch if this worker won the claim.
    pub fn claim_execution_batch(
        &self,
        job_ids: &[&str],
        worker_id: &str,
    ) -> Result<Vec<Option<i64>>> {
        if job_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut conn = self.conn()?;
        let now = now_millis();
        let index_key = self.key(&["exec_claims", "by_time"]);

        // One epoch per row, not per batch: the epoch is the identity of a
        // claim, and two jobs claimed together are still two claims.
        let epochs: Vec<i64> = job_ids.iter().map(|_| mint_claim_epoch()).collect();

        // One pipeline of NX sets; each reply is the "OK" status on a win or Nil
        // when a claim already existed. Decoding as Option maps that to Some/None.
        let mut set_pipe = redis::pipe();
        for (job_id, epoch) in job_ids.iter().zip(&epochs) {
            let ckey = self.key(&["exec_claim", job_id]);
            set_pipe
                .cmd("SET")
                .arg(ckey)
                .arg(claim_value(worker_id, now, Some(*epoch)))
                .arg("NX")
                .arg("PX")
                .arg(86_400_000i64); // 24 hours in milliseconds
        }
        let outcomes: Vec<Option<String>> = set_pipe.query(&mut conn).map_err(map_err)?;
        let claimed: Vec<Option<i64>> = outcomes
            .iter()
            .zip(&epochs)
            .map(|(won, epoch)| won.is_some().then_some(*epoch))
            .collect();

        // Mirror the won claims into the time-indexed set so the maintenance
        // loop can range-purge stale claims, exactly as `claim_execution` does.
        let mut index_pipe = redis::pipe();
        let mut any_won = false;
        for (job_id, won) in job_ids.iter().zip(&claimed) {
            if won.is_some() {
                any_won = true;
                index_pipe.zadd(&index_key, *job_id, now as f64);
            }
        }
        if any_won {
            index_pipe.query::<()>(&mut conn).map_err(map_err)?;
        }

        Ok(claimed)
    }

    /// Remove the execution claim of a finished job.
    ///
    /// The claim carries no namespace of its own, so the scope comes from the
    /// claimed job. A claim on a job in another namespace is left in place —
    /// releasing it would hand that tenant's job back to this one's poller.
    pub fn complete_execution(&self, job_id: &str, namespace: Option<&str>) -> Result<()> {
        if namespace.is_some() && self.get_job(job_id, namespace)?.is_none() {
            return Ok(());
        }

        let mut conn = self.conn()?;
        let ckey = self.key(&["exec_claim", job_id]);
        let index_key = self.key(&["exec_claims", "by_time"]);

        let pipe = &mut redis::pipe();
        pipe.del(&ckey);
        pipe.zrem(&index_key, job_id);
        pipe.query::<()>(&mut conn).map_err(map_err)?;

        Ok(())
    }

    /// Atomically transfer a claim from `expected_owner` to `new_owner`. Returns
    /// the new epoch only if the claim was held by `expected_owner` — the single
    /// GET/SET in the Lua script serializes concurrent rescuers so exactly one
    /// wins.
    ///
    /// The transfer mints a **new epoch**, so the rescued job's next dispatch is
    /// a different claim: the owner alone would leave the rescuer able to
    /// authorize a result the dead owner's executor is still on its way to
    /// sending.
    pub fn reclaim_execution(
        &self,
        job_id: &str,
        expected_owner: &str,
        new_owner: &str,
    ) -> Result<Option<i64>> {
        let mut conn = self.conn()?;
        let now = now_millis();
        let epoch = mint_claim_epoch();
        let ckey = self.key(&["exec_claim", job_id]);
        let index_key = self.key(&["exec_claims", "by_time"]);

        let result: i32 = redis::Script::new(RECLAIM_CLAIM_SCRIPT)
            .key(&ckey)
            .key(&index_key)
            .arg(job_id)
            .arg(expected_owner)
            .arg(new_owner)
            .arg(now)
            .arg(epoch)
            .invoke(&mut conn)
            .map_err(map_err)?;

        Ok((result == 1).then_some(epoch))
    }

    /// Purge execution claims older than the cutoff via the time-indexed sorted
    /// set. Returns the count removed.
    pub fn purge_execution_claims(&self, older_than_ms: i64) -> Result<u64> {
        let mut conn = self.conn()?;
        let index_key = self.key(&["exec_claims", "by_time"]);

        // Find all claims with `claimed_at <= older_than_ms`.
        let expired_ids: Vec<String> = conn
            .zrangebyscore(&index_key, "-inf", older_than_ms as f64)
            .map_err(map_err)?;

        if expired_ids.is_empty() {
            return Ok(0);
        }

        let pipe = &mut redis::pipe();
        for id in &expired_ids {
            let ckey = self.key(&["exec_claim", id]);
            pipe.del(&ckey);
            pipe.zrem(&index_key, id);
        }
        pipe.query::<()>(&mut conn).map_err(map_err)?;

        Ok(expired_ids.len() as u64)
    }
}
