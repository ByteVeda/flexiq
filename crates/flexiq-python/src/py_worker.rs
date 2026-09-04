use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyTuple};

use flexiq_core::job::Job;
use flexiq_core::scheduler::JobResult;

use crate::py_worker_steps::PyWorkerSteps;

/// Call the Python function registered for `job`, returning its serialized
/// result — `None` when the task returned `None`.
///
/// `flexiq.context` is installed before the call and cleared after it whether
/// the task returned or raised, so a failed attempt cannot leak its job id or
/// step handle into the next one.
pub fn execute_task(
    py: Python<'_>,
    task_registry: &Py<PyAny>,
    worker_steps: &Py<PyWorkerSteps>,
    job: &Job,
) -> PyResult<Option<Vec<u8>>> {
    let cloudpickle = py.import("cloudpickle")?;
    let registry = task_registry.bind(py);

    // Look up the task function
    let registry_dict: &Bound<'_, PyDict> = registry.cast()?;
    let task_fn = registry_dict
        .get_item(&job.task_name)?
        .or_else(|| {
            // Fallback: if task_name starts with "__main__.", try matching by suffix
            if job.task_name.starts_with("__main__.") {
                let suffix = &job.task_name["__main__".len()..]; // ".process_user"
                registry_dict.iter().find_map(|(key, val)| {
                    let key_str = key.extract::<String>().ok()?;
                    if key_str.ends_with(suffix) {
                        Some(val)
                    } else {
                        None
                    }
                })
            } else {
                None
            }
        })
        .ok_or_else(|| {
            pyo3::exceptions::PyKeyError::new_err(format!(
                "task '{}' not registered",
                job.task_name
            ))
        })?;

    // Set job context before execution. The step handle rides with it because
    // it is *this* worker's: the claim a step is fenced on belongs to the
    // worker that won it, not to the queue handle the worker was started from.
    let context_mod = py.import("flexiq.context")?;
    context_mod.call_method1(
        "_set_context",
        (
            &job.id,
            &job.task_name,
            job.retry_count,
            &job.queue,
            job.namespace.as_deref(),
            worker_steps.clone_ref(py),
        ),
    )?;

    // Wrap deserialization + call so _clear_context is always called
    let result = (|| -> PyResult<Bound<'_, pyo3::PyAny>> {
        // Deserialize arguments using per-task or queue-level serializer
        let payload_bytes = PyBytes::new(py, &job.payload);
        let queue_ref = context_mod.getattr("_queue_ref")?;
        let unpickled = if !queue_ref.is_none() {
            queue_ref.call_method1("_deserialize_payload", (&job.task_name, &payload_bytes))?
        } else {
            cloudpickle.call_method1("loads", (&payload_bytes,))?
        };
        let args_tuple: Bound<'_, PyTuple> = unpickled.cast_into()?;

        if args_tuple.len() != 2 {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "expected payload to be a 2-tuple (args, kwargs), got {}-tuple",
                args_tuple.len()
            )));
        }

        let args = args_tuple.get_item(0)?;
        let kwargs = args_tuple.get_item(1)?;

        // Call the task function
        if kwargs.is_none() {
            let args_tuple_inner: Bound<'_, PyTuple> = args.cast_into()?;
            task_fn.call(args_tuple_inner, None)
        } else {
            let kwargs_dict: Bound<'_, PyDict> = kwargs.cast_into()?;
            let args_tuple_inner: Bound<'_, PyTuple> = args.cast_into()?;
            task_fn.call(args_tuple_inner, Some(&kwargs_dict))
        }
    })();

    // Clear context after execution (whether success or failure)
    let _ = context_mod.call_method0("_clear_context");

    let result = result?;

    // Serialize result using the queue-level serializer (with any codec
    // chain); fall back to raw cloudpickle when no queue is registered.
    if result.is_none() {
        Ok(None)
    } else {
        let queue_ref = context_mod.getattr("_queue_ref")?;
        let serialized = if !queue_ref.is_none() {
            queue_ref.call_method1("_serialize_result", (&job.task_name, result))?
        } else {
            cloudpickle.call_method1("dumps", (result,))?
        };
        let bytes: Vec<u8> = serialized.extract()?;
        Ok(Some(bytes))
    }
}

/// Render a raised Python exception as the error string stored on the job.
///
/// `flexiq.task_errors` owns the canonical JSON encoding; a failure to import
/// or encode falls back to plain traceback text, which readers treat as legacy.
pub fn format_python_error(py: Python<'_>, e: &PyErr) -> String {
    // Canonical structured error (BINDING_CONTRACT.md "Task errors") — the
    // Python module owns the encoding so every worker path emits one format.
    if let Ok(errors_mod) = py.import("flexiq.task_errors") {
        if let Ok(encoded) = errors_mod.call_method1(
            "encode_from_parts",
            (e.get_type(py), e.value(py), e.traceback(py)),
        ) {
            if let Ok(json) = encoded.extract::<String>() {
                return json;
            }
        }
    }
    // Fallback: plain traceback text (readers treat non-JSON as legacy).
    if let Ok(tb_mod) = py.import("traceback") {
        if let Ok(formatted) = tb_mod.call_method1(
            "format_exception",
            (e.get_type(py), e.value(py), e.traceback(py)),
        ) {
            if let Ok(lines) = formatted.extract::<Vec<String>>() {
                return lines.join("");
            }
        }
    }
    format!("{e}")
}

/// How an attempt that raised ended, before it is turned into a `JobResult`.
enum Ending {
    /// `step.sleep` committed its row and released the claim.
    Slept { wake_at: i64 },
    /// The task observed its cancel request.
    Cancelled,
    /// Anything else.
    Failed { error: String, should_retry: bool },
}

/// The result a failed attempt reports, classified once for every worker path.
///
/// One function rather than one per pool: the three pools had already drifted
/// on how many times they took the GIL to answer this, and a sleep — which is
/// neither a success nor a failure — is exactly the kind of ending a fourth
/// copy would miss.
pub fn job_result_from_error(
    e: &PyErr,
    retry_filters: &Py<PyAny>,
    job: &Job,
    wall_time_ns: i64,
) -> JobResult {
    let job_id = job.id.clone();
    let task_name = job.task_name.clone();

    // A single GIL acquisition: every question below is asked of the same
    // exception object, and taking the GIL per question is pure overhead on
    // the failure path.
    let ending = Python::attach(|py| {
        if let Some(wake_at) = sleep_wake_at(py, e) {
            return Ending::Slept { wake_at };
        }
        if is_cancelled_error(py, e) {
            return Ending::Cancelled;
        }
        let class_name = get_exception_class_name(py, e);
        Ending::Failed {
            error: format_python_error(py, e),
            should_retry: check_should_retry(py, retry_filters, &task_name, &class_name, e),
        }
    });

    match ending {
        Ending::Slept { wake_at } => {
            log::info!("[flexiq] Task {task_name}[{job_id}] sleeping until {wake_at}");
            JobResult::Slept {
                job_id,
                task_name,
                wake_at,
                wall_time_ns,
            }
        }
        Ending::Cancelled => JobResult::Cancelled {
            job_id,
            task_name,
            wall_time_ns,
        },
        Ending::Failed {
            error,
            should_retry,
        } => {
            log::error!("[flexiq] Task {task_name}[{job_id}] failed: {error}");
            JobResult::Failure {
                job_id,
                error,
                retry_count: job.retry_count,
                max_retries: job.max_retries,
                task_name,
                wall_time_ns,
                should_retry,
                timed_out: false,
            }
        }
    }
}

/// The deadline of a `step.sleep` that ended this attempt, if that is what
/// happened.
///
/// A slept attempt is neither a success nor a failure: the sleep row is already
/// committed, the claim already released and the job already `Pending` at the
/// deadline, so the only thing left is to tell the scheduler where it went.
pub fn sleep_wake_at(py: Python<'_>, e: &PyErr) -> Option<i64> {
    let signal = py
        .import("flexiq.steps.errors")
        .ok()?
        .getattr("StepSleepSignal")
        .ok()?;
    if !e.get_type(py).is_subclass(&signal).unwrap_or(false) {
        return None;
    }
    e.value(py).getattr("wake_at").ok()?.extract().ok()
}

/// The core's retry decision for a step failure, or `None` for anything else.
///
/// A divergence or a cap will be just as wrong on the next attempt, and an
/// unreachable step store may not be. The task's `retry_on` / `dont_retry_on`
/// filters have no opinion worth overriding that with, so this is consulted
/// first.
fn step_retry_decision(py: Python<'_>, e: &PyErr) -> Option<bool> {
    e.value(py)
        .getattr(crate::py_step::SHOULD_RETRY_ATTR)
        .ok()?
        .extract()
        .ok()
}

/// Check if the Python exception is a TaskCancelledError.
pub fn is_cancelled_error(py: Python<'_>, e: &PyErr) -> bool {
    if let Ok(exceptions_mod) = py.import("flexiq.exceptions") {
        if let Ok(cancelled_cls) = exceptions_mod.getattr("TaskCancelledError") {
            return e.get_type(py).is_subclass(&cancelled_cls).unwrap_or(false);
        }
    }
    false
}

/// Get the fully-qualified class name of a Python exception.
pub fn get_exception_class_name(py: Python<'_>, e: &PyErr) -> String {
    let type_obj = e.get_type(py);
    let module = type_obj
        .getattr("__module__")
        .and_then(|m| m.extract::<String>())
        .unwrap_or_default();
    let qualname = type_obj
        .getattr("__qualname__")
        .and_then(|q| q.extract::<String>())
        .unwrap_or_else(|_| "Exception".to_string());

    if module.is_empty() || module == "builtins" {
        qualname
    } else {
        format!("{module}.{qualname}")
    }
}

/// Check the retry_filters dict to determine if an exception should be retried.
/// Returns true by default (retry everything unless filtered).
pub fn check_should_retry(
    py: Python<'_>,
    retry_filters: &Py<PyAny>,
    task_name: &str,
    _exc_class_name: &str,
    exc: &PyErr,
) -> bool {
    if let Some(decision) = step_retry_decision(py, exc) {
        return decision;
    }

    let filters = retry_filters.bind(py);
    let filters_dict: &Bound<'_, PyDict> = match filters.cast() {
        Ok(d) => d,
        Err(_) => return true,
    };

    let task_filters = match filters_dict.get_item(task_name) {
        Ok(Some(f)) => f,
        _ => return true, // No filters for this task
    };

    let task_dict: &Bound<'_, PyDict> = match task_filters.cast() {
        Ok(d) => d,
        Err(_) => return true,
    };

    // Check dont_retry_on first
    if let Ok(Some(dont_retry)) = task_dict.get_item("dont_retry_on") {
        if let Ok(list) = dont_retry.cast::<PyList>() {
            for cls in list.iter() {
                if exc.get_type(py).is_subclass(&cls).unwrap_or(false) {
                    return false;
                }
            }
        }
    }

    // Check retry_on (if set, only retry for these exceptions)
    if let Ok(Some(retry_on)) = task_dict.get_item("retry_on") {
        if let Ok(list) = retry_on.cast::<PyList>() {
            if !list.is_empty() {
                for cls in list.iter() {
                    if exc.get_type(py).is_subclass(&cls).unwrap_or(false) {
                        return true;
                    }
                }
                return false; // retry_on specified but exception doesn't match
            }
        }
    }

    true // Default: retry
}
