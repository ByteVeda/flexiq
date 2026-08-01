//! `taskito executor` — attach to a detached scheduler and run its jobs here.
//!
//! Jobs run on the same [`PreforkPool`] the in-process worker uses, so the
//! timeout watchdog, least-loaded dispatch, restart-on-crash and `SIGKILL`
//! after the drain budget all come from shipped code. `taskito.prefork.child`
//! is untouched: it already speaks these frames over stdio, so attaching is a
//! second hop of the same protocol rather than a new one.
//!
//! The lifecycle is split into `wait`/`stop` rather than one blocking `run`
//! because Python signal handlers only run when the main thread holds the GIL.
//! A single blocking call with the GIL released would make the process
//! unkillable by `SIGTERM`, which is exactly the signal a container gets.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;

use taskito_core::worker::{
    AttachAddress, ExecutorClient, ExecutorConfig, ExecutorError, ExecutorHandle,
};

use crate::prefork::PreforkPool;

/// How long to wait for the TCP connect before giving up on the scheduler.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// A running attachment to a scheduler.
///
/// Constructed already connected: the handshake happens in `__new__`, so a bad
/// token or an unreachable scheduler raises here rather than after the pool has
/// been built.
#[pyclass(name = "Executor", module = "taskito._taskito")]
pub struct PyExecutor {
    /// Taken by `shutdown`, which consumes the handle to join its threads.
    handle: Mutex<Option<ExecutorHandle>>,
    scheduler_id: String,
    executor_id: String,
    peer: String,
}

#[pymethods]
impl PyExecutor {
    /// Attach to `address` and start running `tasks` from `app_path`.
    ///
    /// `slots` is the number of prefork children, so it is also the number of
    /// jobs that can run at once.
    #[new]
    #[pyo3(signature = (address, app_path, tasks, slots, token=None, executor_id=None))]
    fn new(
        py: Python<'_>,
        address: &str,
        app_path: &str,
        tasks: Vec<String>,
        slots: u32,
        token: Option<String>,
        executor_id: Option<String>,
    ) -> PyResult<Self> {
        if slots == 0 {
            return Err(PyValueError::new_err("slots must be at least 1"));
        }
        if tasks.is_empty() {
            // The scheduler only sends tasks an executor advertises, so this
            // would attach successfully and then sit idle forever.
            return Err(PyValueError::new_err(
                "no tasks are registered on this app, so the executor would never \
                 be sent any work",
            ));
        }

        let mut config = ExecutorConfig {
            tasks,
            slots,
            token: token.map(taskito_core::Secret::new),
            ..ExecutorConfig::new("python", env!("CARGO_PKG_VERSION"))
        };
        if let Some(id) = executor_id {
            config.executor_id = id;
        }

        // Kept typed as well as erased: the side-channel handle only exists
        // once the handshake has completed, so it is installed after `spawn`.
        let pool = Arc::new(PreforkPool::new(slots as usize, app_path.to_string()));

        // Dialling and the handshake both block on the network; holding the GIL
        // across them would freeze every other Python thread in the process.
        let client = py
            .detach(|| -> Result<ExecutorClient, String> {
                let target = AttachAddress::parse(address).map_err(|error| error.to_string())?;
                let transport = target.connect(CONNECT_TIMEOUT).map_err(|error| {
                    format!("could not reach the scheduler at {target}: {error}")
                })?;
                ExecutorClient::connect(transport, config).map_err(|error| match error {
                    // Named so a wrong token reads as a refusal rather than as
                    // a network fault.
                    ExecutorError::Refused => error.to_string(),
                    other => format!("could not attach to {target}: {other}"),
                })
            })
            .map_err(PyRuntimeError::new_err)?;

        let scheduler_id = client.scheduler_id().to_string();
        let peer = client.peer().to_string();
        let handle = client.spawn(pool.clone() as Arc<dyn taskito_core::WorkerDispatcher>);
        // A child's progress and task logs reach storage only through the
        // scheduler, and this is the relay they travel the second hop on.
        pool.set_side_channel(handle.side_channel());

        Ok(Self {
            executor_id: handle.executor_id().to_string(),
            handle: Mutex::new(Some(handle)),
            scheduler_id,
            peer,
        })
    }

    /// Identity the scheduler announced when it accepted this attach.
    #[getter]
    fn scheduler_id(&self) -> &str {
        &self.scheduler_id
    }

    /// Identity this executor attached under.
    #[getter]
    fn executor_id(&self) -> &str {
        &self.executor_id
    }

    /// Peer label of the scheduler connection.
    #[getter]
    fn peer(&self) -> &str {
        &self.peer
    }

    /// Whether the scheduler session is still open.
    fn is_running(&self) -> bool {
        self.with_handle(|handle| handle.is_running())
            .unwrap_or(false)
    }

    /// Block for at most `timeout_ms`, returning whether the session has ended.
    ///
    /// The caller loops on this so that each return hands the GIL back, which
    /// is the only moment a pending Python signal handler can run.
    fn wait(&self, py: Python<'_>, timeout_ms: u64) -> bool {
        let waited = self.with_handle(|handle| {
            py.detach(|| handle.wait_timeout(Duration::from_millis(timeout_ms)))
        });
        // A shut-down executor has no handle left, and is certainly finished.
        waited.unwrap_or(true)
    }

    /// Ask the scheduler to stop sending work and finish what is in flight.
    ///
    /// Returns immediately when it can take the handle lock, which is what
    /// makes it safe on the signal-handling path CPython actually uses: the
    /// handler runs on the main thread only between `wait` calls, when the lock
    /// is free. A caller on any *other* thread can instead block for up to the
    /// current `wait`'s `timeout_ms`, since `wait` holds the lock across it.
    fn stop(&self) {
        self.with_handle(ExecutorHandle::stop);
    }

    /// Drain in-flight work, disconnect, and join. Idempotent.
    fn shutdown(&self, py: Python<'_>) {
        let handle = self
            .handle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        if let Some(handle) = handle {
            py.detach(|| handle.shutdown());
        }
    }
}

impl PyExecutor {
    /// Run `body` against the live handle, or `None` once shut down.
    fn with_handle<T>(&self, body: impl FnOnce(&ExecutorHandle) -> T) -> Option<T> {
        self.handle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .map(body)
    }
}
