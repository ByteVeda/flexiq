//! Marshalling between core job types and JS-facing shapes. Kept out of the
//! logic modules so `queue`/`worker` read as intent, not plumbing.

use flexiq_core::job::now_millis;
use flexiq_core::storage::records::DebounceOptions;
use flexiq_core::{Job, NewJob};
use napi::bindgen_prelude::{Buffer, Result};
use napi_derive::napi;

use crate::config::EnqueueOptions;
use crate::error::{invalid_arg, non_negative};

const DEFAULT_QUEUE: &str = "default";
// Shared with publish fan-out (queue-level delivery defaults).
pub(crate) const DEFAULT_PRIORITY: i32 = 0;
pub(crate) const DEFAULT_MAX_RETRIES: i32 = 3;
pub(crate) const DEFAULT_TIMEOUT_MS: i64 = 300_000;

/// Build a [`NewJob`] from a task name, opaque payload bytes, and JS options.
/// The core never interprets `payload` — the shell owns (de)serialization.
/// `queue_namespace` is the queue-level default applied when the enqueue call
/// doesn't override it.
pub fn build_new_job(
    task_name: String,
    payload: Vec<u8>,
    opts: EnqueueOptions,
    queue_namespace: Option<&str>,
) -> Result<NewJob> {
    // Signed types reach us from JS; reject the negatives that would silently
    // corrupt scheduling (premature timeout, retries disabled, past schedule).
    let delay = non_negative(opts.delay_ms.unwrap_or(0), "delayMs")?;
    let max_retries = match opts.max_retries {
        Some(n) => non_negative(n as i64, "maxRetries")? as i32,
        None => DEFAULT_MAX_RETRIES,
    };
    let timeout_ms = match opts.timeout_ms {
        Some(n) => non_negative(n, "timeoutMs")?,
        None => DEFAULT_TIMEOUT_MS,
    };
    Ok(NewJob {
        queue: opts.queue.unwrap_or_else(|| DEFAULT_QUEUE.to_string()),
        task_name,
        payload,
        priority: opts.priority.unwrap_or(DEFAULT_PRIORITY),
        // Saturate so an extreme delay can't overflow into a past schedule.
        scheduled_at: now_millis().saturating_add(delay),
        max_retries,
        timeout_ms,
        unique_key: opts.unique_key,
        metadata: opts.metadata,
        notes: opts.notes,
        depends_on: Vec::new(),
        expires_at: None,
        result_ttl_ms: None,
        namespace: opts
            .namespace
            .or_else(|| queue_namespace.map(str::to_string)),
        debounce_key: opts.debounce_key,
    })
}

/// Read the debounce quartet off the JS options. `None` means an ordinary
/// enqueue; `Some` routes the call through `Storage::enqueue_debounced`.
///
/// `debounceWindowMs` is the field that switches the path on, so every other
/// debounce field is rejected without it: a `debounceKey` on a plainly enqueued
/// job is worse than ignored, since the partial index makes any pending row
/// carrying one a slide target for a later debounced enqueue. The window and
/// max-wait *relationship* is left to the core's `validated_debounce_key`, the
/// one place that owns it.
pub fn debounce_options(opts: &EnqueueOptions) -> Result<Option<DebounceOptions>> {
    let Some(window_ms) = opts.debounce_window_ms else {
        if opts.debounce_key.is_some()
            || opts.debounce_max_wait_ms.is_some()
            || opts.debounce_replace_payload.is_some()
        {
            return Err(invalid_arg(
                "debounceKey/debounceMaxWaitMs/debounceReplacePayload require debounceWindowMs",
            ));
        }
        return Ok(None);
    };
    if opts.debounce_key.as_deref().unwrap_or("").is_empty() {
        return Err(invalid_arg(
            "debounceWindowMs requires a non-empty debounceKey",
        ));
    }
    let Some(max_wait_ms) = opts.debounce_max_wait_ms else {
        return Err(invalid_arg("debounceWindowMs requires debounceMaxWaitMs"));
    };
    Ok(Some(DebounceOptions {
        window_ms: non_negative(window_ms, "debounceWindowMs")?,
        max_wait_ms: non_negative(max_wait_ms, "debounceMaxWaitMs")?,
        replace_payload: opts.debounce_replace_payload.unwrap_or(false),
    }))
}

/// Passed to the JS task callback for each dispatched job. `payload` is the
/// opaque arg blob the shell deserializes before running the task.
#[napi(object)]
pub struct JsTaskInvocation {
    pub id: String,
    pub task_name: String,
    pub payload: Buffer,
    /// The job's `retry_count` as dispatched — the attempt half of the
    /// `(owner, attempt)` fence durable steps commit under. Not a claim: the
    /// owner never rides a frame (see `JsQueue::openStepSession`), and this is
    /// re-read from the job row before a step session is handed out.
    pub attempt: i32,
}

/// What the JS task callback resolves with: either a result or an error, never
/// both. Resolving (rather than rejecting) is what lets the shell say *how* a
/// failure should be treated — a rejection carries nothing but a string.
#[napi(object)]
pub struct JsTaskOutcome {
    /// The serialized task result. Set only on success.
    pub result: Option<Buffer>,
    /// The failure, as the canonical cross-SDK `{errtype,message,traceback}`
    /// JSON. Its presence is what marks the outcome a failure.
    pub error: Option<String>,
    /// Whether the failure may be retried; absent means yes. `false` sends the
    /// job straight to the dead-letter queue, whatever budget is left.
    pub retryable: Option<bool>,
    /// Set when the attempt ended in `ctx.step.sleep`: the deadline the job was
    /// rescheduled to, in Unix milliseconds. Neither a result nor a failure —
    /// the sleep row is already committed and the claim already released, so it
    /// outranks both fields above.
    pub slept_until: Option<i64>,
}

/// JS-facing view of a stored [`Job`]. `result` is the opaque result blob (or
/// `null`); the shell deserializes it.
#[napi(object)]
pub struct JsJob {
    pub id: String,
    pub queue: String,
    pub task_name: String,
    pub status: String,
    pub priority: i32,
    pub progress: Option<i32>,
    pub retry_count: i32,
    pub max_retries: i32,
    pub result: Option<Buffer>,
    pub error: Option<String>,
    pub created_at: i64,
    pub scheduled_at: i64,
    pub started_at: Option<i64>,
    pub completed_at: Option<i64>,
    pub timeout_ms: i64,
    pub unique_key: Option<String>,
    pub metadata: Option<String>,
    pub notes: Option<String>,
    pub namespace: Option<String>,
}

/// Convert a core [`Job`] into its JS-facing shape.
pub fn job_to_js(job: Job) -> JsJob {
    JsJob {
        id: job.id,
        queue: job.queue,
        task_name: job.task_name,
        status: job.status.as_str().to_string(),
        priority: job.priority,
        progress: job.progress,
        retry_count: job.retry_count,
        max_retries: job.max_retries,
        result: job.result.map(Buffer::from),
        error: job.error,
        created_at: job.created_at,
        scheduled_at: job.scheduled_at,
        started_at: job.started_at,
        completed_at: job.completed_at,
        timeout_ms: job.timeout_ms,
        unique_key: job.unique_key,
        metadata: job.metadata,
        notes: job.notes,
        namespace: job.namespace,
    }
}

/// One page of jobs plus the cursor for the next. napi has no generics, so each
/// item type needs its own page struct; the TS shell declares a `Page<T>` they
/// all satisfy structurally.
///
/// `next_cursor` is `null` on the last page — a short page means the listing is
/// exhausted — so callers can loop until it is null.
#[napi(object)]
pub struct JsJobPage {
    pub items: Vec<JsJob>,
    pub next_cursor: Option<String>,
}
