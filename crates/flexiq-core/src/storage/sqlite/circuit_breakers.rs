use diesel::prelude::*;

use super::super::models::CircuitBreakerRow;
use super::super::records::CircuitBreakerState;
use super::super::schema::circuit_breakers;
use super::SqliteStorage;
use crate::error::Result;

impl SqliteStorage {
    /// Get or create a circuit breaker state for a task.
    pub fn get_circuit_breaker(&self, task_name: &str) -> Result<Option<CircuitBreakerState>> {
        let mut conn = self.conn()?;
        let row = circuit_breakers::table
            .find(task_name)
            .select(CircuitBreakerRow::as_select())
            .first::<CircuitBreakerRow>(&mut conn)
            .optional()?;
        Ok(row.map(Into::into))
    }

    /// Upsert circuit breaker state.
    pub fn upsert_circuit_breaker(&self, state: &CircuitBreakerState) -> Result<()> {
        let mut conn = self.conn()?;
        let row = CircuitBreakerRow::from(state);
        diesel::replace_into(circuit_breakers::table)
            .values(&row)
            .execute(&mut conn)?;
        Ok(())
    }

    /// Get all circuit breaker states.
    pub fn list_circuit_breakers(&self) -> Result<Vec<CircuitBreakerState>> {
        let mut conn = self.conn()?;
        let rows = circuit_breakers::table
            .select(CircuitBreakerRow::as_select())
            .load::<CircuitBreakerRow>(&mut conn)?;
        Ok(rows.into_iter().map(Into::into).collect())
    }
}
