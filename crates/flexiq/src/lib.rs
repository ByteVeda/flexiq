#![doc = include_str!("../README.md")]
#![deny(missing_docs)]

// The whole crate is a re-export: `flexiq::X` and `flexiq_core::X` are the
// same item, so there is exactly one definition and one set of docs to keep
// current. Anything added to the core root appears here without an edit.
pub use flexiq_core::*;

// Also re-exported under its own name, so code that already spells out
// `flexiq_core::` keeps compiling when it depends only on this crate.
pub use flexiq_core;

// Module names here must not collide with a `pub mod` on the core root: a
// private module of the same name shadows the glob re-export above, so
// `flexiq::error` would stop resolving to `flexiq_core::error`. Core owns
// `contract, error, job, lease, periodic, pubsub, resilience, scheduler,
// settings, step, storage, wire, worker` — the shell's names steer clear.
mod encode;
mod outcome;

pub use outcome::{Abort, Outcome};

/// The seam `#[flexiq::task]` expands against. Not a stable API.
///
/// A macro expands in the *caller's* crate, so everything its output names has
/// to be reachable from outside this one. Collecting those items here rather
/// than exporting them at the root keeps the crate's real surface readable, and
/// keeps a caller from building against something that exists only to be
/// generated.
#[doc(hidden)]
pub mod __private {
    pub use crate::encode::{encode_args, to_wire, EncodeError};
    pub use flexiq_core::wire::WireValue;
}

/// DAG workflows. Enable the `workflows` feature.
///
/// Re-export of [`flexiq_workflows`]; `flexiq::workflows::WorkflowRun` and
/// `flexiq_workflows::WorkflowRun` are the same type.
#[cfg(feature = "workflows")]
pub use flexiq_workflows as workflows;
#[cfg(feature = "workflows")]
pub use flexiq_workflows;

/// Decentralized mesh scheduling. Enable the `mesh` feature.
///
/// Re-export of [`flexiq_mesh`]; `flexiq::mesh::MeshNode` and
/// `flexiq_mesh::MeshNode` are the same type.
#[cfg(feature = "mesh")]
pub use flexiq_mesh as mesh;
#[cfg(feature = "mesh")]
pub use flexiq_mesh;
