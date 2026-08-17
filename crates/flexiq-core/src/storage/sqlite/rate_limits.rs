use diesel::prelude::*;

use super::super::models::RateLimitRow;
use super::super::records::RateLimitState;
use super::super::schema::rate_limits;
use super::SqliteStorage;
use crate::error::Result;
use crate::job::now_millis;

impl SqliteStorage {
    /// Token-bucket state for a rate-limit key, if one exists.
    pub fn get_rate_limit(&self, key: &str) -> Result<Option<RateLimitState>> {
        let mut conn = self.conn()?;

        let row: Option<RateLimitRow> = rate_limits::table
            .find(key)
            .select(RateLimitRow::as_select())
            .first(&mut conn)
            .optional()?;

        Ok(row.map(Into::into))
    }

    /// Insert or replace a token-bucket state row.
    pub fn upsert_rate_limit(&self, state: &RateLimitState) -> Result<()> {
        let mut conn = self.conn()?;

        let row = RateLimitRow::from(state);
        diesel::replace_into(rate_limits::table)
            .values(&row)
            .execute(&mut conn)?;

        Ok(())
    }

    /// Atomically try to acquire a rate limit token.
    /// Does the read-refill-consume-write in a single write transaction to
    /// prevent race conditions between concurrent workers. Uses
    /// `SqliteStorage::write_transaction` (BEGIN IMMEDIATE) so the read-then-
    /// write can't hit the deferred-lock-upgrade `SQLITE_BUSY` deadlock.
    pub fn try_acquire_token(&self, key: &str, max_tokens: f64, refill_rate: f64) -> Result<bool> {
        let now = now_millis();

        self.write_transaction(|conn| {
            let existing: Option<RateLimitRow> = rate_limits::table
                .find(key)
                .select(RateLimitRow::as_select())
                .first(conn)
                .optional()?;

            let mut row = match existing {
                Some(r) => r,
                None => RateLimitRow {
                    key: key.to_string(),
                    tokens: max_tokens,
                    max_tokens,
                    refill_rate,
                    last_refill: now,
                },
            };

            // Refill tokens based on elapsed time
            let elapsed_ms = (now - row.last_refill).max(0) as f64;
            let elapsed_sec = elapsed_ms / 1000.0;
            let refilled = row.tokens + elapsed_sec * refill_rate;
            row.tokens = refilled.min(max_tokens);
            row.last_refill = now;

            // Try to consume one token
            let acquired = if row.tokens >= 1.0 {
                row.tokens -= 1.0;
                true
            } else {
                false
            };

            diesel::replace_into(rate_limits::table)
                .values(&row)
                .execute(conn)?;

            Ok(acquired)
        })
    }
}
