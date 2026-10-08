//! Recording what the dashboard changes in the audit trail (#1020).
//!
//! The dashboard calls these from its request handler; the record shape and
//! the off-path write are `flexiq_core::audit`'s, so a row reads the same as
//! one `flexiq-server`'s dashboard writes.

use std::time::Duration;

use napi::bindgen_prelude::{spawn_blocking, Result};
use napi_derive::napi;

use super::JsQueue;
use crate::error::{invalid_arg, join_to_napi_err};

#[napi]
impl JsQueue {
    /// Start recording dashboard actions into this queue's namespace, pruning
    /// records older than `retentionDays`. A second call while recording is a
    /// no-op.
    #[napi]
    pub fn start_dashboard_audit(&self, retention_days: u32) -> Result<()> {
        if retention_days == 0 {
            return Err(invalid_arg("audit retention must be at least 1 day"));
        }
        let window = Duration::from_secs(u64::from(retention_days) * 86_400);
        self.dashboard_audit
            .start(
                self.storage.clone(),
                self.namespace.as_deref(),
                Some(window),
            )
            .map_err(|e| napi::Error::from_reason(e.to_string()))
    }

    /// Record one answered dashboard request. `username` is the signed-in
    /// user, absent for a dashboard with auth off. Anything but a
    /// state-changing route records nothing; never blocks on storage.
    #[napi]
    pub fn record_dashboard_action(
        &self,
        method: String,
        path: String,
        status: u32,
        username: Option<String>,
    ) {
        let status = u16::try_from(status).unwrap_or(0);
        self.dashboard_audit
            .record(&method, &path, status, username.as_deref());
    }

    /// Stop recording and flush what is buffered, waiting at most ten
    /// seconds — off the event loop.
    #[napi]
    pub async fn close_dashboard_audit(&self) -> Result<()> {
        let audit = self.dashboard_audit.clone();
        spawn_blocking(move || audit.close())
            .await
            .map_err(join_to_napi_err)
    }
}
