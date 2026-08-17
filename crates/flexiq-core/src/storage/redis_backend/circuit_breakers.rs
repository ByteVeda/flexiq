use redis::Commands;

use super::{map_err, RedisStorage};
use crate::error::Result;
use crate::storage::records::CircuitBreakerState;

impl RedisStorage {
    /// Persisted circuit-breaker state for a task, if one exists.
    pub fn get_circuit_breaker(&self, task_name: &str) -> Result<Option<CircuitBreakerState>> {
        let mut conn = self.conn()?;
        let cb_key = self.key(&["cb", task_name]);

        let data: Option<String> = conn.get(&cb_key).map_err(map_err)?;
        match data {
            Some(d) => {
                let row: CircuitBreakerState = serde_json::from_str(&d)?;
                Ok(Some(row))
            }
            None => Ok(None),
        }
    }

    /// Insert or replace a task's circuit-breaker state.
    pub fn upsert_circuit_breaker(&self, row: &CircuitBreakerState) -> Result<()> {
        let mut conn = self.conn()?;
        let cb_key = self.key(&["cb", &row.task_name]);
        let cb_all = self.key(&["cb", "all"]);

        let json = serde_json::to_string(row)?;

        let pipe = &mut redis::pipe();
        pipe.set(&cb_key, &json);
        pipe.sadd(&cb_all, &row.task_name);
        pipe.query::<()>(&mut conn).map_err(map_err)?;

        Ok(())
    }

    /// Every persisted circuit-breaker state.
    pub fn list_circuit_breakers(&self) -> Result<Vec<CircuitBreakerState>> {
        let mut conn = self.conn()?;
        let cb_all = self.key(&["cb", "all"]);

        let names: Vec<String> = conn.smembers(&cb_all).map_err(map_err)?;
        let mut rows = Vec::new();
        for name in names {
            let cb_key = self.key(&["cb", &name]);
            let data: Option<String> = conn.get(&cb_key).map_err(map_err)?;
            if let Some(d) = data {
                let row: CircuitBreakerState = serde_json::from_str(&d)?;
                rows.push(row);
            }
        }

        Ok(rows)
    }
}
