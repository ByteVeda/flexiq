use pyo3::prelude::*;

#[cfg(not(feature = "native-async"))]
mod async_worker;
mod executor;
#[cfg(feature = "native-async")]
mod native_async;
mod prefork;
mod py_attached_steps;
mod py_config;
mod py_job;
mod py_queue;
mod py_step;
pub mod py_worker;
mod py_worker_steps;
#[cfg(feature = "workflows")]
mod py_workflow;

use py_config::PyTaskConfig;
use py_job::PyJob;
use py_queue::PyQueue;

/// Activate the Rust → Python logging bridge.
///
/// Called explicitly from `flexiq.log_config.configure()` rather than from
/// module init so that cold imports (which can run while a connection pool
/// is blocking the GIL on retries) don't trip pyo3-log's flush path.
#[pyfunction]
fn _init_rust_logging() {
    let _ = pyo3_log::try_init();
}

/// Settings-key prefixes the dashboard's generic KV surface must hide. Sourced
/// from the core so every shell hides the same keys.
#[pyfunction]
fn reserved_setting_prefixes() -> Vec<String> {
    flexiq_core::RESERVED_SETTING_PREFIXES
        .iter()
        .map(|prefix| (*prefix).to_string())
        .collect()
}

// `gil_used = true`: this extension relies on the GIL for its shared mutable
// state (scheduler, workflow tracker). Until that state is audited for the
// free-threaded build, advertise GIL dependence so 3.13t/3.14t fall back safely.
#[pymodule(gil_used = true)]
fn _flexiq(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(_init_rust_logging, m)?)?;
    m.add_function(wrap_pyfunction!(reserved_setting_prefixes, m)?)?;
    // Sourced from the core so the executor side never mirrors the literal.
    m.add(
        "WORKER_PROTOCOL_VERSION",
        flexiq_core::worker::protocol::PROTOCOL_VERSION,
    )?;
    // Same reason: a capability a child spells out by hand is one that drifts
    // out of agreement with the pool negotiating against it.
    m.add("CAP_STEPS", flexiq_core::worker::protocol::CAP_STEPS)?;
    m.add_class::<PyQueue>()?;
    m.add_class::<PyJob>()?;
    m.add_class::<PyTaskConfig>()?;
    m.add_class::<py_step::PyStepSession>()?;
    m.add_class::<py_step::PyStepDecision>()?;
    m.add_class::<py_step::PyStepSleep>()?;
    m.add_class::<py_worker_steps::PyWorkerSteps>()?;
    m.add_class::<py_attached_steps::PyAttachedSteps>()?;
    m.add_function(wrap_pyfunction!(py_step::derive_step_key, m)?)?;
    m.add_class::<executor::PyExecutor>()?;
    #[cfg(feature = "native-async")]
    {
        m.add_class::<native_async::PyResultSender>()?;
        m.add_class::<native_async::PyJobPermit>()?;
    }
    #[cfg(feature = "workflows")]
    {
        m.add_class::<py_workflow::PyWorkflowBuilder>()?;
        m.add_class::<py_workflow::PyWorkflowHandle>()?;
        m.add_class::<py_workflow::PyWorkflowRunStatus>()?;
        m.add_class::<py_workflow::PyWorkflowRun>()?;
        m.add_class::<py_workflow::PyWorkflowRunNode>()?;
    }
    Ok(())
}
