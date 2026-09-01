//! Writing the cross-SDK wire envelope from Rust.
//!
//! `Job.payload` is opaque to this crate everywhere else: the shells serialize
//! a call before it crosses into Rust and deserialize it in the worker. This
//! module is the one place Rust *produces* one, for the two callers that have
//! no language runtime to do it for them — a Rust producer using the `flexiq`
//! crate directly, and the gRPC door's structured-arguments arm, which takes a
//! call as protobuf values and has to turn it into the same bytes an SDK would
//! have sent.
//!
//! The format is specified in `crates/flexiq-core/BINDING_CONTRACT.md` and
//! pinned byte-for-byte by `contracts/wire-vectors.json`, which every SDK
//! asserts in its own suite. This is the Rust implementation of that one
//! format, not a second definition of it: a payload written here is
//! indistinguishable from one written by any shell.

mod cbor;
mod envelope;
mod value;

pub use envelope::{encode_call, encode_result, TAG_CBOR};
pub use value::WireValue;
