//! Task-log methods on `JsQueue`: the storage behind published partial results
//! (`currentJob().publish()`) and the stream consumer (`queue.stream()`).

use napi::bindgen_prelude::Result;
use napi_derive::napi;
use taskito_core::Storage;

use super::JsQueue;
use crate::convert::{log_to_js, JsTaskLog};
use crate::error::to_napi_err;

#[napi]
impl JsQueue {
    /// Append a task-log entry for a job. A published partial result uses
    /// `level = "result"` with the value JSON-encoded in `extra`.
    #[napi]
    pub fn write_task_log(
        &self,
        job_id: String,
        task_name: String,
        level: String,
        message: String,
        extra: Option<String>,
    ) -> Result<()> {
        self.storage
            .write_task_log(
                &job_id,
                &task_name,
                &level,
                &message,
                extra.as_deref(),
                self.namespace.as_deref(),
            )
            .map_err(to_napi_err)
    }

    /// All task-log entries for a job, oldest first.
    #[napi]
    pub fn get_task_logs(&self, job_id: String) -> Result<Vec<JsTaskLog>> {
        let rows = self.storage.get_task_logs(&job_id).map_err(to_napi_err)?;
        Ok(rows.into_iter().map(log_to_js).collect())
    }

    /// Task-log entries for a job with id after `afterId` (UUIDv7 ids are
    /// time-ordered, so the last seen id is the stream cursor).
    #[napi]
    pub fn get_task_logs_after(
        &self,
        job_id: String,
        after_id: Option<String>,
    ) -> Result<Vec<JsTaskLog>> {
        let rows = self
            .storage
            .get_task_logs_after(&job_id, after_id.as_deref())
            .map_err(to_napi_err)?;
        Ok(rows.into_iter().map(log_to_js).collect())
    }
}
