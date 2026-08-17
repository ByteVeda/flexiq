//! JS-facing shape for a registered periodic task. Timestamps are Unix milliseconds.

use flexiq_core::storage::records::PeriodicTask;
use napi_derive::napi;

/// JS-facing view of a periodic task (omits the opaque args/kwargs payloads).
#[napi(object)]
pub struct JsPeriodicTask {
    pub name: String,
    pub task_name: String,
    pub cron_expr: String,
    pub queue: String,
    pub enabled: bool,
    /// Last fire time (Unix ms), or `null` if it has not run yet.
    pub last_run: Option<i64>,
    pub next_run: i64,
    /// IANA timezone the cron is evaluated in, or `null` for UTC.
    pub timezone: Option<String>,
}

pub fn periodic_to_js(row: PeriodicTask) -> JsPeriodicTask {
    JsPeriodicTask {
        name: row.name,
        task_name: row.task_name,
        cron_expr: row.cron_expr,
        queue: row.queue,
        enabled: row.enabled,
        last_run: row.last_run,
        next_run: row.next_run,
        timezone: row.timezone,
    }
}
