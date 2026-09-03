//! `flexiq.v1` as proto3 JSON, in both directions.
//!
//! Hand-written, and that is a decision rather than an omission. The crates
//! that generate serde implementations for prost types need
//! `google.protobuf.*` to be *their* copies of the well-known types, and none
//! of them can serialise `google.rpc.Status` at all — so adopting one would
//! mean rewriting the conversions the producer service already has and
//! generating a second `google.rpc.Status` inside a process that deliberately
//! has one. The mapping is a few hundred lines; a second spelling of the error
//! model is permanent.
//!
//! Three modules, split by direction:
//!
//! * [`wkt`] — the well-known types, whose JSON spellings are not guessable
//!   from the protobuf encoding and belong in one place.
//! * [`request`] — serde structs, so a misspelled field is named in the answer.
//! * [`response`] — hand-built objects, so presence is a decision per field.
//!
//! What keeps all three honest is not the compiler: it is the test in
//! [`response`] that reads `contracts/descriptor.binpb` and asserts a fully
//! populated message emits exactly the JSON names the contract gives it. Field
//! *names* are frozen alongside the numbers for this door's sake (design doc
//! D4) — a rename is invisible to binary protobuf and fatal to a client that
//! has only the JSON.

pub mod request;
pub mod response;
pub mod wkt;
