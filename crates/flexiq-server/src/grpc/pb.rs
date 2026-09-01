//! The generated `flexiq.v1` types.
//!
//! `build.rs` compiles them from `contracts/descriptor.binpb`, the same
//! `FileDescriptorSet` buf lints, `buf breaking` gates and
//! [`super::reflection`] serves. Nothing in this module is hand-written, and
//! nothing hand-written belongs in it: the conversions between these types and
//! `flexiq_core` live in [`super::producer::convert`].

// Lints against generated code, which no edit here can satisfy:
//
// * `missing_docs` — the generator carries the .proto comments through onto the
//   items, but writes none on the modules it wraps them in.
// * `doc_lazy_continuation` — a wrapped bullet in a .proto comment arrives
//   without the indentation rustdoc wants, and reflowing the .proto to please a
//   Rust lint would be the tail wagging the contract.
// * `large_enum_variant` — the `body` and `outcome` oneofs hold a payload and a
//   `Job` beside much smaller arms, which is what the wire says they are.
#![allow(
    missing_docs,
    clippy::doc_lazy_continuation,
    clippy::large_enum_variant
)]

include!(concat!(env!("OUT_DIR"), "/flexiq.v1.rs"));
