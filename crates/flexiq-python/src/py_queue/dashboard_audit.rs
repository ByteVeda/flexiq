//! Recording what the dashboard changes in the audit trail (#1020).
//!
//! The dashboard calls these from its request handler; the record shape and
//! the off-path write are `flexiq_core::audit`'s, so a row reads the same as
//! one `flexiq-server`'s dashboard writes.

use std::time::Duration;

use pyo3::prelude::*;

use super::PyQueue;

#[pymethods]
impl PyQueue {
    /// Start recording dashboard actions into this queue's namespace,
    /// pruning records older than `retention_days`. A second call while
    /// recording is a no-op.
    pub fn start_dashboard_audit(&self, py: Python<'_>, retention_days: u64) -> PyResult<()> {
        if retention_days == 0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "audit retention must be at least 1 day",
            ));
        }
        let window = Duration::from_secs(retention_days.saturating_mul(86_400));
        // Opening the writer may prune at once; keep that off the GIL.
        py.detach(|| {
            self.dashboard_audit.start(
                self.storage.clone(),
                self.namespace.as_deref(),
                Some(window),
            )
        })
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Record one answered dashboard request. `username` is the signed-in
    /// user, `None` a dashboard with auth off. Anything but a state-changing
    /// route records nothing; never blocks on storage.
    #[pyo3(signature = (method, path, status, username=None))]
    pub fn record_dashboard_action(
        &self,
        method: &str,
        path: &str,
        status: u16,
        username: Option<&str>,
    ) {
        self.dashboard_audit.record(method, path, status, username);
    }

    /// Stop recording and flush what is buffered, waiting at most ten
    /// seconds.
    pub fn close_dashboard_audit(&self, py: Python<'_>) {
        py.detach(|| self.dashboard_audit.close());
    }
}
