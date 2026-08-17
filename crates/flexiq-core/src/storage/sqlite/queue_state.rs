use diesel::prelude::*;

use super::super::models::QueueStateRow;
use super::super::schema::queue_state;
use super::SqliteStorage;
use crate::error::Result;
use crate::job::now_millis;

impl SqliteStorage {
    /// Pause a queue so no new jobs are dispatched from it.
    pub fn pause_queue(&self, queue_name: &str) -> Result<()> {
        let mut conn = self.conn()?;
        let now = now_millis();

        let row = QueueStateRow {
            queue_name: queue_name.to_string(),
            paused: true,
            paused_at: Some(now),
        };

        diesel::replace_into(queue_state::table)
            .values(&row)
            .execute(&mut conn)?;

        Ok(())
    }

    /// Resume a paused queue.
    pub fn resume_queue(&self, queue_name: &str) -> Result<()> {
        let mut conn = self.conn()?;

        diesel::delete(queue_state::table.filter(queue_state::queue_name.eq(queue_name)))
            .execute(&mut conn)?;

        Ok(())
    }

    /// List all currently paused queues.
    pub fn list_paused_queues(&self) -> Result<Vec<String>> {
        let mut conn = self.conn()?;

        let names: Vec<String> = queue_state::table
            .filter(queue_state::paused.eq(true))
            .select(queue_state::queue_name)
            .load(&mut conn)?;

        Ok(names)
    }
}
