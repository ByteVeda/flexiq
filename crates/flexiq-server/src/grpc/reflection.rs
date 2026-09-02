//! Server reflection, so a client can describe this door without a `.proto`.
//!
//! The descriptor served is `contracts/descriptor.binpb` — the artifact the buf
//! gate already builds and commits at a pinned version. Embedding it is what
//! makes `grpcurl -plaintext host:port list` work against a bare image: the
//! wire contract travels with the binary rather than with a checkout, and it
//! cannot drift from the `.proto` files, because CI fails when the two
//! disagree.
//!
//! `grpc.health.v1` is registered beside it for the same reason: a reflecting
//! client that cannot see the health service has to be told about it out of
//! band, which is exactly what reflection exists to avoid.

use anyhow::{Context, Result};
use tonic_reflection::server::v1::{ServerReflection, ServerReflectionServer};
use tonic_reflection::server::v1alpha::{
    ServerReflection as ServerReflectionAlpha,
    ServerReflectionServer as ServerReflectionServerAlpha,
};
use tonic_reflection::server::Builder;

use crate::grpc::pb::FILE_DESCRIPTOR_SET as FLEXIQ_DESCRIPTOR;

/// Every descriptor set this server reflects over.
fn configured() -> Builder<'static> {
    Builder::configure()
        .register_encoded_file_descriptor_set(FLEXIQ_DESCRIPTOR)
        .register_encoded_file_descriptor_set(tonic_health::pb::FILE_DESCRIPTOR_SET)
}

/// The `grpc.reflection.v1` service.
pub fn v1() -> Result<ServerReflectionServer<impl ServerReflection>> {
    configured()
        .build_v1()
        .context("failed to build the gRPC reflection service from contracts/descriptor.binpb")
}

/// The `grpc.reflection.v1alpha` service.
///
/// v1 has been the stable name since 2023, but the tools an operator reaches
/// for outlive their protocol versions and several still ask for v1alpha
/// first. Serving both costs one registration and removes a failure whose only
/// symptom is an empty `list`.
pub fn v1alpha() -> Result<ServerReflectionServerAlpha<impl ServerReflectionAlpha>> {
    configured()
        .build_v1alpha()
        .context("failed to build the gRPC reflection service from contracts/descriptor.binpb")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The descriptor is a build-time artifact of another tool, so the thing
    /// worth pinning is that it still decodes into something reflection can
    /// serve — a truncated or half-regenerated file fails here rather than at
    /// the first client that asks.
    #[test]
    fn the_committed_descriptor_builds_both_reflection_services() {
        assert!(!FLEXIQ_DESCRIPTOR.is_empty());
        v1().expect("v1 reflection");
        v1alpha().expect("v1alpha reflection");
    }
}
