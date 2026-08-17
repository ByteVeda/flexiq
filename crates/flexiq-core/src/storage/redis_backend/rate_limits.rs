use redis::Commands;

use super::{map_err, RedisStorage};
use crate::error::Result;
use crate::job::now_millis;
use crate::storage::records::RateLimitState;

impl RedisStorage {
    /// Token-bucket state for a rate-limit key, if one exists.
    pub fn get_rate_limit(&self, key: &str) -> Result<Option<RateLimitState>> {
        let mut conn = self.conn()?;
        let rkey = self.key(&["rate_limit", key]);

        let data: Option<String> = conn.get(&rkey).map_err(map_err)?;
        match data {
            Some(d) => {
                let row: RateLimitState = serde_json::from_str(&d)?;
                Ok(Some(row))
            }
            None => Ok(None),
        }
    }

    /// Insert or replace a token-bucket state entry.
    pub fn upsert_rate_limit(&self, row: &RateLimitState) -> Result<()> {
        let mut conn = self.conn()?;
        let rkey = self.key(&["rate_limit", &row.key]);
        let json = serde_json::to_string(row)?;
        conn.set::<_, _, ()>(&rkey, &json).map_err(map_err)?;
        Ok(())
    }

    /// Atomically refill and consume one token via a Lua script. Returns
    /// `false` when the bucket is empty.
    pub fn try_acquire_token(&self, key: &str, max_tokens: f64, refill_rate: f64) -> Result<bool> {
        let mut conn = self.conn()?;
        let now = now_millis();

        // Use a Lua script for atomicity
        let script = redis::Script::new(
            r#"
            local key = KEYS[1]
            local now = tonumber(ARGV[1])
            local max_tokens = tonumber(ARGV[2])
            local refill_rate = tonumber(ARGV[3])

            local data = redis.call('GET', key)
            local tokens, last_refill

            if data then
                local t = cjson.decode(data)
                tokens = tonumber(t.tokens)
                last_refill = tonumber(t.last_refill)
            else
                tokens = max_tokens
                last_refill = now
            end

            -- Refill
            local elapsed_ms = math.max(0, now - last_refill)
            local elapsed_sec = elapsed_ms / 1000.0
            tokens = math.min(max_tokens, tokens + elapsed_sec * refill_rate)
            last_refill = now

            -- Try consume
            local acquired = 0
            if tokens >= 1.0 then
                tokens = tokens - 1.0
                acquired = 1
            end

            local new_data = cjson.encode({
                key = ARGV[4],
                tokens = tokens,
                max_tokens = max_tokens,
                refill_rate = refill_rate,
                last_refill = last_refill
            })
            redis.call('SET', key, new_data)

            return acquired
            "#,
        );

        let rkey = self.key(&["rate_limit", key]);
        let result: i32 = script
            .key(&rkey)
            .arg(now)
            .arg(max_tokens)
            .arg(refill_rate)
            .arg(key)
            .invoke(&mut conn)
            .map_err(map_err)?;

        Ok(result == 1)
    }
}
