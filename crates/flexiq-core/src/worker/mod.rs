pub mod auth;
pub mod cancel;
pub mod dial;
pub mod dispatcher;
pub mod executor;
// Public because the sibling SDK crates fingerprint their own registries before
// handing one to `register_worker`, and because the value now lands in a storage
// column an executor written outside this workspace has to agree with — see
// `BINDING_CONTRACT.md`, which pins the algorithm and its vectors.
pub mod fingerprint;
pub mod protocol;
pub mod registry;
pub mod remote;
pub mod runner;
pub mod side_channel;
mod step_pump;
pub mod transport;

pub use auth::Secret;
pub use cancel::CancelSignals;
pub use dial::AttachAddress;
pub use dispatcher::NativeDispatcher;
pub use executor::{
    ExecutorClient, ExecutorConfig, ExecutorError, ExecutorHandle, ExecutorSession,
    ExecutorSideChannel,
};
pub use fingerprint::registry_fingerprint;
pub use protocol::{
    decode_step_snapshot, encode_step_snapshot, Dispatch, ExecutorMessage, HelloBuilder, Incoming,
    ProtocolError, SchedulerMessage, CAP_SIDE_CHANNEL, CAP_STEPS, PROTOCOL_VERSION,
};
pub use registry::{TaskError, TaskHandler, TaskRegistry, TaskResult};
pub use remote::{AttachError, AttachedExecutor, Capacity, RemoteConfig, RemoteDispatcher};
pub use runner::{Worker, WorkerHandle};
pub use side_channel::{SideChannel, StorageSideChannel};
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

    /// Tell the pool which worker id the scheduler wins its execution claims
    /// under.
    ///
    /// Only a pool that performs a *fenced* write on the scheduler's behalf
    /// needs it, which today means the [`RemoteDispatcher`]: an attached
    /// executor cannot supply the owner for its own step commits, because an
    /// owner it supplies is an owner it can forge. Everyone else runs where the
    /// claim was won and ignores this.
    fn set_claim_owner(&self, _owner: &str) {}
}
