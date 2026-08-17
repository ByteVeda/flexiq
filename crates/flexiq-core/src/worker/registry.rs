//! Task registry: maps a job's `task_name` to the Rust handler that runs it.
//!
//! The language bindings keep their own registries (a Python dict, a JS map);
//! this one exists so a pure-Rust consumer has a first-class way to say
//! "`send_email` means this function".

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::job::Job;

/// What a handler produced: optional result bytes (stored on the job and
/// readable via `get_job`), or a [`TaskError`].
pub type TaskResult = std::result::Result<Option<Vec<u8>>, TaskError>;

/// A boxed future returned by async handlers.
pub type TaskFuture = Pin<Box<dyn Future<Output = TaskResult> + Send + 'static>>;

/// A failed task execution. `retryable` feeds the scheduler's retry decision:
/// a retryable failure follows the job's retry policy; a fatal one goes
/// straight to the dead-letter queue.
#[derive(Debug, Clone)]
pub struct TaskError {
    /// Human-readable failure message.
    pub message: String,
    /// Whether the scheduler may retry the job.
    pub retryable: bool,
}

impl TaskError {
    /// A failure the scheduler may retry (subject to the job's retry policy).
    pub fn retryable(message: impl Into<String>) -> Self {
        TaskError {
            message: message.into(),
            retryable: true,
        }
    }

    /// A failure that must not be retried — the job dead-letters immediately.
    pub fn fatal(message: impl Into<String>) -> Self {
        TaskError {
            message: message.into(),
            retryable: false,
        }
    }
}

impl std::fmt::Display for TaskError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for TaskError {}

/// A registered handler: blocking (run on a blocking thread) or async
/// (spawned on the worker's tokio runtime).
#[derive(Clone)]
pub enum TaskHandler {
    /// Blocking handler, run on a blocking thread.
    Sync(Arc<dyn Fn(&Job) -> TaskResult + Send + Sync>),
    /// Async handler, spawned on the worker's tokio runtime.
    Async(Arc<dyn Fn(Job) -> TaskFuture + Send + Sync>),
}

/// Name → handler map consulted by the dispatcher for every dequeued job.
#[derive(Clone, Default)]
pub struct TaskRegistry {
    handlers: HashMap<String, TaskHandler>,
}

impl TaskRegistry {
    /// An empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a blocking handler for `task_name`. It runs on a dedicated
    /// blocking thread, so it may block freely (I/O, CPU work).
    pub fn register(
        &mut self,
        task_name: impl Into<String>,
        handler: impl Fn(&Job) -> TaskResult + Send + Sync + 'static,
    ) {
        self.handlers
            .insert(task_name.into(), TaskHandler::Sync(Arc::new(handler)));
    }

    /// Register an async handler for `task_name`. The future is spawned on
    /// the worker's runtime, so it must not block the executor.
    pub fn register_async<F, Fut>(&mut self, task_name: impl Into<String>, handler: F)
    where
        F: Fn(Job) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = TaskResult> + Send + 'static,
    {
        self.handlers.insert(
            task_name.into(),
            TaskHandler::Async(Arc::new(move |job| Box::pin(handler(job)))),
        );
    }

    /// The handler registered for `task_name`, if any.
    pub fn get(&self, task_name: &str) -> Option<&TaskHandler> {
        self.handlers.get(task_name)
    }

    /// Names of every registered task.
    pub fn task_names(&self) -> impl Iterator<Item = &str> {
        self.handlers.keys().map(String::as_str)
    }

    /// True when no handlers are registered.
    pub fn is_empty(&self) -> bool {
        self.handlers.is_empty()
    }
}
