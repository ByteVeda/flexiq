//! Map the core's [`ResultOutcome`] to a JS-facing shape for the events layer.

use flexiq_core::scheduler::ResultOutcome;
use napi_derive::napi;

/// The outcome of a finished job, delivered to the worker's outcome callback.
/// `kind` is `"success" | "retry" | "dead" | "cancelled"`.
#[napi(object)]
pub struct JsOutcome {
    pub kind: String,
    pub job_id: String,
    pub task_name: String,
    pub queue: Option<String>,
    pub error: Option<String>,
    pub retry_count: Option<i32>,
    pub timed_out: Option<bool>,
    /// Execution time in milliseconds; absent when the run wasn't measured.
    pub duration_ms: Option<i64>,
    /// The workflow node the job ran for; absent for a non-workflow job.
    pub workflow: Option<JsWorkflowLink>,
}

/// The workflow node a job runs for, read from its metadata at dispatch.
#[napi(object)]
#[derive(Clone)]
pub struct JsWorkflowLink {
    /// The workflow run.
    pub run_id: String,
    /// The node within the run.
    pub node_name: String,
    /// A saga rollback job, which advances the compensation, not the forward run.
    pub compensation: bool,
}

/// Nanoseconds to whole milliseconds, dropping the core's "not measured" 0 so a
/// consumer never reads an unmeasured run as an instant one.
fn duration_ms(wall_time_ns: i64) -> Option<i64> {
    (wall_time_ns > 0).then_some(wall_time_ns / 1_000_000)
}

/// `None` for a superseded result: nothing was written, so the shell emits no
/// event and runs no middleware for it — exactly as if it had never arrived.
pub fn outcome_to_js(outcome: &ResultOutcome) -> Option<JsOutcome> {
    let mapped = match outcome {
        ResultOutcome::Success {
            job_id,
            task_name,
            wall_time_ns,
        } => JsOutcome {
            kind: "success".to_string(),
            job_id: job_id.clone(),
            task_name: task_name.clone(),
            queue: None,
            error: None,
            retry_count: None,
            timed_out: None,
            duration_ms: duration_ms(*wall_time_ns),
            workflow: None,
        },
        ResultOutcome::Retry {
            job_id,
            task_name,
            queue,
            error,
            retry_count,
            timed_out,
            wall_time_ns,
        } => JsOutcome {
            kind: "retry".to_string(),
            job_id: job_id.clone(),
            task_name: task_name.clone(),
            queue: Some(queue.clone()),
            error: Some(error.clone()),
            retry_count: Some(*retry_count),
            timed_out: Some(*timed_out),
            duration_ms: duration_ms(*wall_time_ns),
            workflow: None,
        },
        ResultOutcome::DeadLettered {
            job_id,
            task_name,
            queue,
            error,
            timed_out,
            wall_time_ns,
        } => JsOutcome {
            kind: "dead".to_string(),
            job_id: job_id.clone(),
            task_name: task_name.clone(),
            queue: Some(queue.clone()),
            error: Some(error.clone()),
            retry_count: None,
            timed_out: Some(*timed_out),
            duration_ms: duration_ms(*wall_time_ns),
            workflow: None,
        },
        ResultOutcome::Cancelled {
            job_id,
            task_name,
            queue,
            wall_time_ns,
        } => JsOutcome {
            kind: "cancelled".to_string(),
            job_id: job_id.clone(),
            task_name: task_name.clone(),
            queue: Some(queue.clone()),
            error: None,
            retry_count: None,
            timed_out: None,
            duration_ms: duration_ms(*wall_time_ns),
            workflow: None,
        },
        ResultOutcome::Superseded { .. } => return None,
        // Handled where the attempt ended, not here: the task callback pairs
        // the sleep with `onSleep` and emits `job.sleeping` while the job
        // context is still live, the same way it owns the success event.
        // Doing it again here would double both.
        ResultOutcome::Slept { .. } => return None,
        // The taxonomy is `#[non_exhaustive]`; an outcome this build does not
        // know is one it cannot name correctly, so it emits nothing.
        _ => return None,
    };
    Some(mapped)
}
