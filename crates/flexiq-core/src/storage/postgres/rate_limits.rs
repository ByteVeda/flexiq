use diesel::prelude::*;

use super::super::models::RateLimitRow;
use super::super::records::RateLimitState;
use super::super::schema::rate_limits;
use super::PostgresStorage;
use crate::error::Result;
use crate::job::now_millis;

impl PostgresStorage {
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
        diesel::insert_into(rate_limits::table)
            .values(&row)
            .on_conflict(rate_limits::key)
            .do_update()
            .set(&row)
            .execute(&mut conn)?;

        Ok(())
    }

    /// Atomically try to acquire a rate limit token.
    /// Does the read-refill-consume-write in a single transaction to prevent
    /// race conditions between concurrent workers.
    ///
    /// The bucket row is seeded with `INSERT ... ON CONFLICT DO NOTHING` and then
    /// read `FOR UPDATE`. A bare `SELECT ... FOR UPDATE` locks nothing when the
    /// row is absent, so concurrent first-writers would each start from a full
    /// bucket and over-admit; seeding first guarantees the row exists exactly once
    /// (the unique-index conflict serializes the first writers) so the subsequent
    /// `FOR UPDATE` always locks a committed row and the read-modify-write is
    /// serialized. Mirrors the `FOR UPDATE` pattern in `acquire_lock`.
    pub fn try_acquire_token(&self, key: &str, max_tokens: f64, refill_rate: f64) -> Result<bool> {
        let mut conn = self.conn()?;
        let now = now_millis();

        conn.transaction(|conn| {
            diesel::sql_query(
                "INSERT INTO rate_limits (key, tokens, max_tokens, refill_rate, last_refill) \
                 VALUES ($1, $2, $2, $3, $4) ON CONFLICT (key) DO NOTHING",
            )
            .bind::<diesel::sql_types::Text, _>(key)
            .bind::<diesel::sql_types::Double, _>(max_tokens)
            .bind::<diesel::sql_types::Double, _>(refill_rate)
            .bind::<diesel::sql_types::BigInt, _>(now)
            .execute(conn)?;

            let mut row: RateLimitRow = diesel::sql_query(
                "SELECT key, tokens, max_tokens, refill_rate, last_refill \
                 FROM rate_limits WHERE key = $1 FOR UPDATE",
            )
            .bind::<diesel::sql_types::Text, _>(key)
            .get_result(conn)?;

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

            diesel::insert_into(rate_limits::table)
                .values(&row)
                .on_conflict(rate_limits::key)
                .do_update()
                .set(&row)
                .execute(conn)?;

            Ok(acquired)
        })
    }
}
