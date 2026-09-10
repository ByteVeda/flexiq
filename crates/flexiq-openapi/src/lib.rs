//! The OpenAPI document for the FlexiQ JSON facade, generated from the
//! committed wire contract.
//!
//! `crates/flexiq-server`'s facade serves the `flexiq.v1` producer RPCs as JSON
//! over plain HTTP. This crate describes that door in a form the generator
//! ecosystem can read, and it does so from one artifact —
//! `contracts/descriptor.binpb` — so the description cannot drift from the
//! contract it describes.
//!
//! ## Where the paths come from
//!
//! Not from a table in here. Every RPC carries a `google.api.http` option
//! naming the path and verb the facade answers, and [`binding::bindings`] reads
//! it back out. The server's route table is held to the same annotations by a
//! test, so there are three things — the annotation, the router and this
//! document — and only one of them can be edited without the other two failing.
//!
//! ## What is generated, and what is written by hand
//!
//! Message schemas, enum values, parameter names and every description come off
//! the descriptor. The error envelope does not: the facade renders
//! `google.rpc.Status` into a shape of its own, so [`schema::ERROR`] spells
//! that shape out and the generated schemas reference it.

#![deny(missing_docs)]

pub mod binding;
pub mod descriptor;
pub mod document;
pub mod schema;

pub use binding::{bindings, Binding, Verb};
pub use descriptor::Contract;
pub use document::{document, PRODUCER_PACKAGE};

/// Anything that stops the document being generated.
///
/// Every arm names the contract element at fault, because the only way to
/// resolve one of these is to edit a `.proto` and every one of them is reached
/// through a `scripts/proto-check.sh` run rather than at runtime.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The bytes handed in are not a `FileDescriptorSet`.
    #[error("the descriptor is not a FileDescriptorSet: {0}")]
    Descriptor(#[from] prost::DecodeError),

    /// A message or enum the contract references is not in the descriptor.
    #[error("{0} is referenced by the contract but is not in the descriptor")]
    Undeclared(String),

    /// An RPC's `google.api.http` option names no method, or names two.
    #[error("{0}: the google.api.http option must name exactly one HTTP method")]
    Pattern(String),

    /// A shape the generator deliberately does not describe rather than
    /// describing wrongly.
    #[error("{element}: {reason}")]
    Unsupported {
        /// The contract element that reached it.
        element: String,
        /// What about it is not describable.
        reason: String,
    },
}
