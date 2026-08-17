//! `JsQueue` — the producer/inspection surface over the core storage.

use flexiq_core::{Storage, StorageBackend};
use napi::bindgen_prelude::{Buffer, Promise, Result, Status, Unknown};
use napi::threadsafe_function::ThreadsafeFunction;
use napi_derive::napi;

use crate::config::{EnqueueJob, EnqueueOptions, OpenOptions, WorkerOptions};
use crate::convert::{build_new_job, job_to_js, JsJob, JsOutcome, JsTaskInvocation, JsTaskOutcome};
use crate::error::to_napi_err;
use crate::worker::{start_worker, JsWorker};

/// Outcome callback registered from JS: `(outcome) => void`. Its return value is
/// ignored, hence `Unknown`. See [`crate::dispatcher::TaskCallback`] for the
/// meaning of the trailing `false`.
pub type OutcomeCallback =
    ThreadsafeFunction<JsOutcome, Unknown<'static>, JsOutcome, Status, false>;

mod admin;
mod inspect;
mod locks;
mod logs;
mod periodic;
mod pubsub;
#[cfg(feature = "workflows")]
mod workflows;

/// A FlexiQ queue handle (SQLite/Postgres/Redis) exposed to JavaScript.
#[napi]
pub struct JsQueue {
    storage: StorageBackend,
    namespace: Option<String>,
    /// Whether opening applies schema changes. When false the workflow store is
    /// built unmigrated too, so no path applies DDL until `migrate()` runs.
    #[cfg(feature = "workflows")]
    pub(crate) auto_migrate: bool,
    /// Workflow storage, lazily initialized on first workflow call so the
    /// workflow migrations only run when workflows are actually used.
    #[cfg(feature = "workflows")]
    workflow_storage: std::sync::OnceLock<flexiq_workflows::WorkflowStorageBackend>,
}

#[napi]
impl JsQueue {
    /// Open (creating if needed) a queue's storage backend.
    #[napi(factory)]
    pub fn open(options: OpenOptions) -> Result<Self> {
        let namespace = options.namespace.clone();
        let auto_migrate = options.auto_migrate.unwrap_or(true);
        let storage = crate::backend::open(&options)?;
        // Every process that joins a deployment passes through this one open,
        // so the contract floor is checked here rather than in the SDK. The one
        // storage that cannot answer is one whose schema was never applied —
        // there is no floor recorded and nothing to read it from — and that
        // storage is unusable until `migrate()` runs, which checks it then.
        if auto_migrate || storage.is_migrated().map_err(to_napi_err)? {
            flexiq_core::ensure_contract_supported(&storage).map_err(to_napi_err)?;
        }
        Ok(Self {
            storage,
            namespace,
            #[cfg(feature = "workflows")]
            auto_migrate,
            #[cfg(feature = "workflows")]
            workflow_storage: std::sync::OnceLock::new(),
        })
    }

    /// Enqueue `task_name` with an opaque serialized `payload`. Returns the job
    /// id. When `options.uniqueKey` is set, a duplicate enqueue is a no-op while
    /// the first job is pending/running (idempotency).
    #[napi]
    pub fn enqueue(
        &self,
        task_name: String,
        payload: Buffer,
        options: Option<EnqueueOptions>,
    ) -> Result<String> {
        let opts = options.unwrap_or_default();
        let unique = opts.unique_key.is_some();
        let new_job = build_new_job(task_name, payload.to_vec(), opts, self.namespace.as_deref())?;
        let job = if unique {
            self.storage.enqueue_unique(new_job)
        } else {
            self.storage.enqueue(new_job)
        }
        .map_err(to_napi_err)?;
        Ok(job.id)
    }

    /// Enqueue a batch of jobs for one `task_name` in a single storage call.
    /// Each entry carries its own payload and options. Returns the job ids
    /// in input order. Entries with a `uniqueKey` get the same dedup as
    /// `enqueue` — a duplicate yields the active job's id instead of a new row.
    #[napi]
    pub fn enqueue_many(&self, task_name: String, jobs: Vec<EnqueueJob>) -> Result<Vec<String>> {
        let namespace = self.namespace.as_deref();
        let new_jobs = jobs
            .into_iter()
            .map(|job| {
                let opts = job.options.unwrap_or_default();
                build_new_job(task_name.clone(), job.payload.to_vec(), opts, namespace)
            })
            .collect::<Result<Vec<_>>>()?;
        let created = flexiq_core::storage::enqueue_batch_dedup(&self.storage, new_jobs)
            .map_err(to_napi_err)?;
        Ok(created.into_iter().map(|job| job.id).collect())
    }

    /// Count pending jobs on a queue — the lean primitive behind the
    /// `maxPending` admission cap. Sync so the producer can gate a sync
    /// `enqueue`/`enqueueMany` without a round trip to the event loop.
    #[napi]
    pub fn count_pending_by_queue(&self, queue: String) -> Result<i64> {
        self.storage
            .count_pending_by_queue(&queue)
            .map_err(to_napi_err)
    }

    /// Fetch a job by id, or `null` if no such job exists.
    #[napi]
    pub fn get_job(&self, id: String) -> Result<Option<JsJob>> {
        let job = self
            .storage
            .get_job(&id, self.namespace.as_deref())
            .map_err(to_napi_err)?;
        Ok(job.map(job_to_js))
    }

    /// Cancel a pending job immediately. Returns false if it was not pending.
    #[napi]
    pub fn cancel_job(&self, id: String) -> Result<bool> {
        self.storage
            .cancel_job(&id, self.namespace.as_deref())
            .map_err(to_napi_err)
    }

    /// Request cancellation of a running job (cooperative). Returns false if
    /// there is no such running job.
    #[napi]
    pub fn request_cancel(&self, id: String) -> Result<bool> {
        self.storage
            .request_cancel(&id, self.namespace.as_deref())
            .map_err(to_napi_err)
    }

    /// Whether cancellation has been requested for a job.
    #[napi]
    pub fn is_cancel_requested(&self, id: String) -> Result<bool> {
        self.storage
            .is_cancel_requested(&id, self.namespace.as_deref())
            .map_err(to_napi_err)
    }

    /// Update a running job's progress (0–100), for observability.
    #[napi]
    pub fn update_progress(&self, id: String, progress: i32) -> Result<()> {
        self.storage
            .update_progress(&id, progress.clamp(0, 100), self.namespace.as_deref())
            .map_err(to_napi_err)
    }

    /// Start a worker that runs `callback` for each dequeued job. Returns a
    /// [`JsWorker`] handle — call `stop()` on it to shut the worker down.
    #[napi]
    pub fn run_worker(
        &self,
        // Spelled out rather than written as the `TaskCallback` / `OutcomeCallback`
        // aliases: napi-derive resolves these generics syntactically, and an alias
        // reaches the generated `index.d.ts` as an undefined type name.
        callback: ThreadsafeFunction<
            JsTaskInvocation,
            Promise<JsTaskOutcome>,
            JsTaskInvocation,
            Status,
            false,
        >,
        outcome_callback: ThreadsafeFunction<JsOutcome, Unknown<'static>, JsOutcome, Status, false>,
        options: Option<WorkerOptions>,
    ) -> Result<JsWorker> {
        start_worker(
            self.storage.clone(),
            self.namespace.clone(),
            options.unwrap_or_default(),
            callback,
            outcome_callback,
        )
    }
}
