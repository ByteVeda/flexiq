#![doc = include_str!("../README.md")]
#![deny(missing_docs)]

// The whole crate is a re-export: `flexiq::X` and `flexiq_core::X` are the
// same item, so there is exactly one definition and one set of docs to keep
// current. Anything added to the core root appears here without an edit.
pub use flexiq_core::*;

// Also re-exported under its own name, so code that already spells out
// `flexiq_core::` keeps compiling when it depends only on this crate.
pub use flexiq_core;

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
