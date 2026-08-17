//! Periodic (cron) task registration + management. The worker's maintenance
//! loop enqueues due tasks; this binds registration plus list / delete / pause
//! / resume. Re-registering an existing name replaces it (upsert).

use flexiq_core::job::now_millis;
use flexiq_core::periodic::{next_cron_time, next_cron_time_tz};
use flexiq_core::storage::records::NewPeriodicTask;
use flexiq_core::Storage;
use napi::bindgen_prelude::{Buffer, Result};
use napi_derive::napi;

use super::JsQueue;
use crate::convert::{periodic_to_js, JsPeriodicTask};
use crate::error::to_napi_err;

const DEFAULT_QUEUE: &str = "default";

#[napi]
impl JsQueue {
    /// Register (or replace) a cron-scheduled task. `args` is the opaque,
    /// already-serialized payload handed to the task when it fires. Returns the
    /// next fire time (Unix ms); the worker maintenance loop enqueues the job
    /// once due. Rejects an invalid cron expression.
    #[napi]
    #[allow(clippy::too_many_arguments)]
    pub fn register_periodic(
        &self,
        name: String,
        task_name: String,
        cron_expr: String,
        args: Option<Buffer>,
        queue: Option<String>,
        timezone: Option<String>,
        enabled: Option<bool>,
    ) -> Result<i64> {
        let now = now_millis();
        let next_run = match &timezone {
            Some(tz) => next_cron_time_tz(&cron_expr, now, tz),
            None => next_cron_time(&cron_expr, now),
        }
        .map_err(to_napi_err)?;

        let args_bytes = args.map(|b| b.to_vec());
        let row = NewPeriodicTask {
            name: name.clone(),
            task_name,
            cron_expr,
            args: args_bytes,
            kwargs: None,
            queue: queue.unwrap_or_else(|| DEFAULT_QUEUE.to_string()),
            enabled: enabled.unwrap_or(true),
            next_run,
            timezone,
        };
        self.storage.register_periodic(&row).map_err(to_napi_err)?;
        Ok(next_run)
    }

    /// Every registered periodic task, enabled or paused.
    #[napi]
    pub fn list_periodic(&self) -> Result<Vec<JsPeriodicTask>> {
        let tasks = self.storage.list_periodic().map_err(to_napi_err)?;
        Ok(tasks.into_iter().map(periodic_to_js).collect())
    }

    /// Unschedule a periodic task. Returns false if none had that name.
    #[napi]
    pub fn delete_periodic(&self, name: String) -> Result<bool> {
        self.storage.delete_periodic(&name).map_err(to_napi_err)
    }

    /// Pause (false) or resume (true) a periodic task by toggling its enabled
    /// flag. Returns false if none had that name.
    #[napi]
    pub fn set_periodic_enabled(&self, name: String, enabled: bool) -> Result<bool> {
        self.storage
            .set_periodic_enabled(&name, enabled)
            .map_err(to_napi_err)
    }
}
