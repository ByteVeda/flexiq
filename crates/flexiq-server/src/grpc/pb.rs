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

/// The committed `FileDescriptorSet` these types were generated from.
///
/// Built by `scripts/proto-check.sh`, verified byte-for-byte in CI, and read
/// here by everything that needs the contract at runtime rather than as Rust
/// types: [`super::reflection`] serves it, and the JSON facade's drift tests
/// ask it which RPCs exist and what their fields are called. One artifact, so
/// nothing can be checked against a copy.
pub const FILE_DESCRIPTOR_SET: &[u8] = include_bytes!("../../../../contracts/descriptor.binpb");

include!(concat!(env!("OUT_DIR"), "/flexiq.v1.rs"));

/// The generated `flexiq.executor.v1` types.
///
/// A separate module because it is a separate package: the two doors have
/// different audiences, different credentials and different message limits, and
/// nothing in one may import the other. Compiled from the same descriptor, so
/// there is still one artifact and one gate.
pub mod executor {
    include!(concat!(env!("OUT_DIR"), "/flexiq.executor.v1.rs"));
}
