use crossbeam_channel::Sender;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyTuple};

use flexiq_core::job::Job;
use flexiq_core::scheduler::JobResult;

use crate::py_worker::job_result_from_error;
use crate::py_worker_steps::PyWorkerSteps;

/// Execute a sync task on the current thread (called inside `spawn_blocking`).
///
/// Acquires the GIL, deserializes the payload via cloudpickle, calls the task
/// wrapper from the registry, serializes the result, and sends a `JobResult`
/// to the scheduler via `result_tx`.
pub fn execute_sync_task(
    task_registry: &Py<PyAny>,
    retry_filters: &Py<PyAny>,
    worker_steps: &Py<PyWorkerSteps>,
    job: &Job,
    result_tx: &Sender<JobResult>,
) {
    let job_id = job.id.clone();
    let task_name = job.task_name.clone();

    let start = std::time::Instant::now();
    log::info!("[flexiq] Task {task_name}[{job_id}] received");

    let result = Python::attach(|py| -> PyResult<Option<Vec<u8>>> {
        run_task(py, task_registry, worker_steps, job)
    });

    let wall_time_ns: i64 = start.elapsed().as_nanos().try_into().unwrap_or(i64::MAX);

    let job_result = match result {
        Ok(result_bytes) => {
            let secs = start.elapsed().as_secs_f64();
            log::info!("[flexiq] Task {task_name}[{job_id}] succeeded in {secs:.3}s");
            JobResult::Success {
                job_id,
                result: result_bytes,
                task_name,
                wall_time_ns,
            }
        }
        Err(e) => job_result_from_error(&e, retry_filters, job, wall_time_ns),
    };

    let _ = result_tx.send(job_result);
}

/// Inner task execution: deserialize payload, look up and call the task function,
/// serialize the return value.
fn run_task(
    py: Python<'_>,
    task_registry: &Py<PyAny>,
    worker_steps: &Py<PyWorkerSteps>,
    job: &Job,
) -> PyResult<Option<Vec<u8>>> {
    let cloudpickle = py.import("cloudpickle")?;
    let registry = task_registry.bind(py);

    let registry_dict: &Bound<'_, PyDict> = registry.cast()?;
    let task_fn = registry_dict
        .get_item(&job.task_name)?
        .or_else(|| {
            if job.task_name.starts_with("__main__.") {
                let suffix = &job.task_name["__main__".len()..];
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

    // Set job context before execution, step handle included: the claim a
    // durable step is fenced on belongs to this worker, not to the queue handle.
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

        if kwargs.is_none() {
            let args_tuple_inner: Bound<'_, PyTuple> = args.cast_into()?;
            task_fn.call(args_tuple_inner, None)
        } else {
            let kwargs_dict: Bound<'_, PyDict> = kwargs.cast_into()?;
            let args_tuple_inner: Bound<'_, PyTuple> = args.cast_into()?;
            task_fn.call(args_tuple_inner, Some(&kwargs_dict))
        }
    })();

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
