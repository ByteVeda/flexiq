pub mod auth;
pub mod cancel;
pub mod dial;
pub mod dispatcher;
pub mod executor;
pub mod protocol;
pub mod registry;
pub mod remote;
pub mod runner;
pub mod transport;

pub use auth::Secret;
pub use cancel::CancelSignals;
pub use dial::AttachAddress;
pub use dispatcher::NativeDispatcher;
pub use executor::{
    ExecutorClient, ExecutorConfig, ExecutorError, ExecutorHandle, ExecutorSession,
};
pub use protocol::{ExecutorMessage, ProtocolError, SchedulerMessage, PROTOCOL_VERSION};
pub use registry::{TaskError, TaskHandler, TaskRegistry, TaskResult};
pub use remote::{AttachError, AttachedExecutor, Capacity, RemoteConfig, RemoteDispatcher};
pub use runner::{Worker, WorkerHandle};
#[cfg(unix)]
pub use transport::UnixTransport;
pub use transport::{MemoryTransport, TcpTransport, Transport};

use async_trait::async_trait;
use crossbeam_channel::Sender;

use crate::job::Job;
use crate::scheduler::JobResult;

/// Abstraction for worker pool implementations.
/// Core defines the interface; the [`NativeDispatcher`] runs registered Rust
/// handlers, and the language bindings provide their own pools.
#[async_trait]
pub trait WorkerDispatcher: Send + Sync {
    /// Start dispatching jobs from the receiver. Runs until channel closes.
    async fn run(&self, job_rx: tokio::sync::mpsc::Receiver<Job>, result_tx: Sender<JobResult>);

    /// Signal the pool to stop accepting new work.
    fn shutdown(&self);

    /// Notify the pool that a running job should be cancelled.
    ///
    /// Pools that run tasks in-process (e.g. the thread pool) can rely on the
    /// storage cancel flag and provide a no-op. Pools that execute tasks in a
    /// separate process (e.g. the prefork pool) must use this hook to deliver
    /// a side-channel signal so the worker observes the cancel without polling
    /// storage.
    fn notify_cancel(&self, _job_id: &str) {}
}

#[cfg(test)]
mod executor_tests;
#[cfg(test)]
mod remote_tests;
#[cfg(test)]
mod tests;
