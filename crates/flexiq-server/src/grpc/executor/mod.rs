//! `flexiq.executor.v1`: the executor door.
//!
//! One service, two RPCs, and no state of its own beyond the streams that are
//! currently attached. The scheduler side is
//! [`RemoteDispatcher`](flexiq_core::RemoteDispatcher) — the same one the TCP
//! and Unix attach listener feeds — so this module's whole job is to be a
//! fourth transport for a protocol that already exists.
//!
//! * [`frames`] is the only place the proto and the frame protocol meet.
//! * [`session`] answers "which stream" for the one RPC that is not on a
//!   stream.
//! * [`service`] is the plumbing, and the stream's bounded lifetime.
//!
//! The credential is the scoped API token every call on this listener carries;
//! `/flexiq.executor.v1.` requires `Scope::Execute`, classified once in
//! [`crate::grpc::auth::gate`] and inherited by every RPC in the package.

pub mod frames;
pub mod service;
pub mod session;

pub use service::{ExecutorDoor, Rotation, SESSION_METADATA};
