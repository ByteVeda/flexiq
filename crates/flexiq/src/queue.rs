//! The handle a producer holds.

use flexiq_core::{
    ensure_contract_supported, Job, QueueError, QueueStats, Result, SqliteStorage, Storage,
    StorageBackend,
};
use sha2::{Digest, Sha256};

use crate::{Task, TaskCall};

/// An embedded FlexiQ: one storage backend, and the operations a producer needs.
///
/// Named for the product rather than `Queue`, because a *queue* here is already
/// a named string every job carries. A type called `Queue` that is not one of
/// those would be the more confusing of the two options.
#[derive(Clone)]
pub struct FlexiQ {
    storage: StorageBackend,
    namespace: Option<String>,
}

impl FlexiQ {
    /// Open, or create, a SQLite database at `path`.
    pub fn open(path: &str) -> Result<Self> {
        Self::from_storage(StorageBackend::Sqlite(SqliteStorage::new(path)?))
    }

    /// Open a private in-memory database. Useful in tests; the data dies with
    /// the handle.
    pub fn in_memory() -> Result<Self> {
        Self::from_storage(StorageBackend::Sqlite(SqliteStorage::in_memory()?))
    }

    /// Wrap a backend that is already open — a Postgres or Redis one, or a
    /// SQLite handle sharing a pool with something else.
    ///
    /// This is where the contract floor is checked. `BINDING_CONTRACT.md`
    /// requires every shell to call `ensure_contract_supported` once at storage
    /// open, and a gate that runs anywhere else is a gate a caller can skip by
    /// choosing a different constructor.
    pub fn from_storage(storage: StorageBackend) -> Result<Self> {
        ensure_contract_supported(&storage)?;
        Ok(Self {
            storage,
            namespace: None,
        })
    }

    /// Scope every operation on this handle to a tenant namespace.
    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    /// The namespace this handle is scoped to, if any.
    pub fn namespace(&self) -> Option<&str> {
        self.namespace.as_deref()
    }

    /// The backend underneath, for an operation this shell does not wrap.
    pub fn storage(&self) -> &StorageBackend {
        &self.storage
    }

    /// Enqueue one call.
    ///
    /// Three writes hide behind this, and which one runs is decided by the
    /// call's own options: a debounce window routes to `enqueue_debounced`, a
    /// dedup key to `enqueue_unique`, and everything else to the plain insert.
    pub fn enqueue<T: Task>(&self, call: TaskCall<T>) -> Result<Job> {
        let (new_job, debounce, unique) = self.prepare::<T>(call)?;

        match (debounce, unique) {
            (Some(window), _) => self.storage.enqueue_debounced(new_job, window),
            (None, true) => self.storage.enqueue_unique(new_job),
            (None, false) => self.storage.enqueue(new_job),
        }
    }

    /// Enqueue several calls of the same task in one write.
    ///
    /// A debounced call is refused rather than split out: storage has no
    /// batched debounce, and looping it item by item would cost the Diesel
    /// backends the atomicity that is the reason to send a batch at all.
    pub fn enqueue_batch<T: Task>(&self, calls: Vec<TaskCall<T>>) -> Result<Vec<Job>> {
        let mut rows = Vec::with_capacity(calls.len());
        let mut any_unique = false;

        for call in calls {
            let (new_job, debounce, unique) = self.prepare::<T>(call)?;
            if debounce.is_some() {
                return Err(QueueError::Other(format!(
                    "a debounce window cannot ride a batch: enqueue `{}` on its own",
                    T::NAME
                )));
            }
            any_unique |= unique;
            rows.push(new_job);
        }

        if any_unique {
            self.storage.enqueue_unique_batch(rows)
        } else {
            self.storage.enqueue_batch(rows)
        }
    }

    /// Turn a call into the row storage takes, and say which write it wants.
    fn prepare<T: Task>(
        &self,
        call: TaskCall<T>,
    ) -> Result<(
        flexiq_core::NewJob,
        Option<flexiq_core::storage::records::DebounceOptions>,
        bool,
    )> {
        let TaskCall {
            payload,
            mut options,
            ..
        } = call;

        // Raised here rather than at `call(..)`, which mirrors the task's own
        // signature. A `Serialize` value can still have no representation in
        // the envelope — a `u64` past `i64::MAX` is the ordinary case — and a
        // producer should see an error rather than a panic.
        let payload = payload.map_err(|e| {
            QueueError::Other(format!(
                "task `{}` could not encode its arguments: {e}",
                T::NAME
            ))
        })?;

        match (self.namespace.as_deref(), options.namespace.as_deref()) {
            // A scoped handle is a boundary, not a default. `TaskCall::namespace`
            // is public, so without this check a caller holding a handle for one
            // tenant could name another on the call and write into it.
            (Some(handle), Some(named)) if handle != named => {
                return Err(QueueError::Other(format!(
                    "this handle is scoped to namespace `{handle}`, and the call names \
                     `{named}`: a scoped handle cannot enqueue outside its own namespace"
                )))
            }
            (Some(_), None) => options.namespace.clone_from(&self.namespace),
            // An unscoped handle naming a namespace per call is how a caller
            // targets one without holding a scoped handle, and stays allowed.
            _ => {}
        }

        // An explicit key wins: a caller who names an identity has said
        // something the payload's bytes cannot.
        if options.idempotent && options.unique_key.is_none() {
            options.unique_key = Some(auto_unique_key(T::NAME, &payload));
        }

        let unique = options.unique_key.is_some();
        let window = options.debounce.as_ref().map(|d| d.options());
        Ok((options.into_new_job(T::NAME, payload), window, unique))
    }

    /// Start building a worker over this backend.
    pub fn worker(&self) -> crate::WorkerBuilder {
        crate::WorkerBuilder::new(self.storage.clone(), self.namespace.clone())
    }

    /// Cancel a pending job. `false` when there was nothing to cancel.
    pub fn cancel(&self, job_id: &str) -> Result<bool> {
        self.storage.cancel_job(job_id, self.namespace.as_deref())
    }

    /// Ask a *running* job to stop. The task has to poll for it; nothing
    /// interrupts a handler mid-call.
    pub fn request_cancel(&self, job_id: &str) -> Result<bool> {
        self.storage
            .request_cancel(job_id, self.namespace.as_deref())
    }

    /// Read one job back.
    pub fn get_job(&self, job_id: &str) -> Result<Option<Job>> {
        self.storage.get_job(job_id, self.namespace.as_deref())
    }

    /// The most recent jobs in this namespace, newest first.
    pub fn list_jobs(&self, limit: i64, offset: i64) -> Result<Vec<Job>> {
        self.storage
            .list_jobs(None, None, None, limit, offset, self.namespace.as_deref())
    }

    /// Counts by lifecycle state.
    pub fn stats(&self) -> Result<QueueStats> {
        self.storage.stats(self.namespace.as_deref())
    }

    /// Every registered periodic task.
    ///
    /// **Refused on a namespaced handle.** Periodic rows carry no namespace and
    /// every backend keys the table by name alone, so a namespaced listing would
    /// return other namespaces' schedules. The same holds for
    /// [`delete_periodic`](Self::delete_periodic),
    /// [`pause_periodic`](Self::pause_periodic) and
    /// [`resume_periodic`](Self::resume_periodic), and for registering one on a
    /// namespaced worker.
    pub fn list_periodic(&self) -> Result<Vec<flexiq_core::PeriodicTask>> {
        self.refuse_periodic_in_namespace("list_periodic")?;
        self.storage.list_periodic()
    }

    /// Remove a periodic. `false` when there was none by that name.
    ///
    /// A worker that still has the task registered writes it back at its next
    /// startup: the schedule is declared in code, and this removes the row
    /// rather than the declaration.
    pub fn delete_periodic(&self, name: &str) -> Result<bool> {
        self.refuse_periodic_in_namespace("delete_periodic")?;
        self.storage.delete_periodic(name)
    }

    /// Stop a periodic firing, without forgetting it.
    pub fn pause_periodic(&self, name: &str) -> Result<bool> {
        self.refuse_periodic_in_namespace("pause_periodic")?;
        self.storage.set_periodic_enabled(name, false)
    }

    /// Let a paused periodic fire again.
    pub fn resume_periodic(&self, name: &str) -> Result<bool> {
        self.refuse_periodic_in_namespace("resume_periodic")?;
        self.storage.set_periodic_enabled(name, true)
    }

    /// The periodic table is keyed by name alone, so a namespaced handle has no
    /// safe way to touch it.
    fn refuse_periodic_in_namespace(&self, operation: &str) -> Result<()> {
        match self.namespace {
            Some(_) => Err(crate::cron::unsupported_in_namespace(operation)),
            None => Ok(()),
        }
    }
}

/// The dedup key `idempotent` derives: `auto:` and the first 32 hex characters
/// of `sha256(task_name || 0x00 || payload)`.
///
/// The separator is a NUL byte and the digest is truncated to 32 characters.
/// Both are wire-visible: this key is how the same call sent from another
/// language deduplicates against this one, so a divergence does not fail
/// anything here — it silently stops deduping over there.
fn auto_unique_key(task_name: &str, payload: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(task_name.as_bytes());
    hasher.update([0x00]);
    hasher.update(payload);
    let digest = hex::encode(hasher.finalize());
    format!("auto:{}", &digest[..32])
}
