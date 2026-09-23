//! The generated `flexiq.v1` client and message types.
//!
//! `build.rs` compiles them from `contracts/descriptor.binpb`. Nothing here is
//! hand-written and nothing hand-written belongs here; the mapping between
//! these types and what an operator types lives in [`crate::args`],
//! [`crate::output`] and [`crate::commands`].
//!
//! `flexiq.v1` and `flexiq.admin.v1` are included. `flexiq.executor.v1` is a
//! different door with a different credential scope, and this binary never
//! opens it.

// Lints against generated code, which no edit here can satisfy:
//
// * `missing_docs` — the generator carries the .proto comments onto the items
//   but writes none on the modules it wraps them in.
// * `doc_lazy_continuation` — a wrapped bullet in a .proto comment arrives
//   without the indentation rustdoc wants.
// * `large_enum_variant` — the `body` and `outcome` oneofs hold a payload and a
//   `Job` beside much smaller arms, which is what the wire says they are.
#![allow(
    missing_docs,
    clippy::doc_lazy_continuation,
    clippy::large_enum_variant
)]

include!(concat!(env!("OUT_DIR"), "/flexiq.v1.rs"));

/// The generated `flexiq.admin.v1` types, as [`admin`].
///
/// The generator names the `flexiq.v1` types this package imports by package
/// path — `super::super::v1::Job` from inside `flexiq.admin.v1` — so the
/// include sits at that depth, beside a `v1` that re-exports this module's.
mod flexiq {
    pub mod v1 {
        pub use crate::pb::{Job, StructuredArgs};
    }

    pub mod admin {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/flexiq.admin.v1.rs"));
        }
    }
}

pub use flexiq::admin::v1 as admin;

#[cfg(test)]
mod tests {
    /// The generated client is what every command dials with. If codegen
    /// silently produced messages and no service, this stops naming a type.
    #[test]
    fn the_producer_client_is_generated() {
        fn assert_nameable<T>() {}
        assert_nameable::<
            super::producer_service_client::ProducerServiceClient<tonic::transport::Channel>,
        >();
    }

    /// The operator door's client, generated from the same descriptor.
    #[test]
    fn the_admin_client_is_generated() {
        fn assert_nameable<T>() {}
        assert_nameable::<
            super::admin::admin_service_client::AdminServiceClient<tonic::transport::Channel>,
        >();
    }
}
