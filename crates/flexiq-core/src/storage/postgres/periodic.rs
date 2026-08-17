use diesel::prelude::*;

use super::super::models::*;
use super::super::records::{NewPeriodicTask, PeriodicTask};
use super::super::schema::periodic_tasks;
use super::PostgresStorage;
use crate::error::Result;

impl PostgresStorage {
    /// Register or update a periodic task.
    pub fn register_periodic(&self, task: &NewPeriodicTask) -> Result<()> {
        let mut conn = self.conn()?;
        let row = NewPeriodicTaskRow {
            name: &task.name,
            task_name: &task.task_name,
            cron_expr: &task.cron_expr,
            args: task.args.as_deref(),
            kwargs: task.kwargs.as_deref(),
            queue: &task.queue,
            enabled: task.enabled,
            next_run: task.next_run,
            timezone: task.timezone.as_deref(),
        };

        diesel::insert_into(periodic_tasks::table)
            .values(&row)
            .on_conflict(periodic_tasks::name)
            .do_update()
            .set(&row)
            .execute(&mut conn)?;

        Ok(())
    }

    /// Get all periodic tasks that are due to run.
    pub fn get_due_periodic(&self, now: i64) -> Result<Vec<PeriodicTask>> {
        let mut conn = self.conn()?;

        let rows = periodic_tasks::table
            .filter(periodic_tasks::enabled.eq(true))
            .filter(periodic_tasks::next_run.le(now))
            .select(PeriodicTaskRow::as_select())
            .load::<PeriodicTaskRow>(&mut conn)?;

        Ok(rows.into_iter().map(Into::into).collect())
    }

    /// Update a periodic task's schedule after execution.
    pub fn update_periodic_schedule(&self, name: &str, last_run: i64, next_run: i64) -> Result<()> {
        let mut conn = self.conn()?;

        diesel::update(periodic_tasks::table.find(name))
            .set((
                periodic_tasks::last_run.eq(last_run),
                periodic_tasks::next_run.eq(next_run),
            ))
            .execute(&mut conn)?;

        Ok(())
    }

    /// List all registered periodic tasks, enabled or paused.
    pub fn list_periodic(&self) -> Result<Vec<PeriodicTask>> {
        let mut conn = self.conn()?;

        let rows = periodic_tasks::table
            .select(PeriodicTaskRow::as_select())
            .load::<PeriodicTaskRow>(&mut conn)?;

        Ok(rows.into_iter().map(Into::into).collect())
    }

    /// Remove a periodic task. Returns false if no task had that name.
    pub fn delete_periodic(&self, name: &str) -> Result<bool> {
        let mut conn = self.conn()?;

        let affected = diesel::delete(periodic_tasks::table.find(name)).execute(&mut conn)?;

        Ok(affected > 0)
    }

    /// Pause (false) or resume (true) a periodic task by toggling `enabled`.
    /// Returns false if no task had that name.
    pub fn set_periodic_enabled(&self, name: &str, enabled: bool) -> Result<bool> {
        let mut conn = self.conn()?;

        let affected = diesel::update(periodic_tasks::table.find(name))
            .set(periodic_tasks::enabled.eq(enabled))
            .execute(&mut conn)?;

        Ok(affected > 0)
    }
}
