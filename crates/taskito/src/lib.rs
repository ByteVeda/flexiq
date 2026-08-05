#![doc = include_str!("../README.md")]

// The whole crate is a re-export: `taskito::X` and `taskito_core::X` are the
// same item, so there is exactly one definition and one set of docs to keep
// current. Anything added to the core root appears here without an edit.
pub use taskito_core::*;

// Also re-exported under its own name, so code that already spells out
// `taskito_core::` keeps compiling when it depends only on this crate.
pub use taskito_core;

/// DAG workflows. Enable the `workflows` feature.
///
/// Re-export of [`taskito_workflows`]; `taskito::workflows::WorkflowRun` and
/// `taskito_workflows::WorkflowRun` are the same type.
#[cfg(feature = "workflows")]
pub use taskito_workflows as workflows;
#[cfg(feature = "workflows")]
pub use taskito_workflows;

/// Decentralized mesh scheduling. Enable the `mesh` feature.
///
/// Re-export of [`taskito_mesh`]; `taskito::mesh::MeshNode` and
/// `taskito_mesh::MeshNode` are the same type.
#[cfg(feature = "mesh")]
pub use taskito_mesh as mesh;
#[cfg(feature = "mesh")]
pub use taskito_mesh;
