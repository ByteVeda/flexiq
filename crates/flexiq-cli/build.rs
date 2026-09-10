//! Generate the `flexiq.v1` client from the committed descriptor.
//!
//! Compiling the `FileDescriptorSet` rather than the `.proto` files keeps
//! `protoc` off the build path, and it makes the CLI's types derive from the
//! exact artifact `scripts/proto-check.sh` gates, `buf breaking` protects and
//! the server serves over reflection — so client and server cannot describe
//! different contracts.

use std::path::{Path, PathBuf};
use std::{env, fs};

use prost::Message;

/// The `FileDescriptorSet` buf builds from `contracts/proto`.
const DESCRIPTOR: &str = "contracts/descriptor.binpb";

fn main() {
    let descriptor = workspace_root().join(DESCRIPTOR);
    println!("cargo:rerun-if-changed={}", descriptor.display());

    let bytes = fs::read(&descriptor).unwrap_or_else(|error| {
        panic!(
            "failed to read {}: {error}. Build it with scripts/proto-check.sh --fix",
            descriptor.display()
        )
    });
    let set = prost_types::FileDescriptorSet::decode(&bytes[..])
        .expect("contracts/descriptor.binpb is not a FileDescriptorSet");

    tonic_prost_build::configure()
        // A CLI dials a door; it never answers one.
        .build_server(false)
        .build_client(true)
        // google.rpc.Status is generated once, in tonic-types — the crate whose
        // StatusExt reads the ErrorInfo details this CLI prints.
        .extern_path(".google.rpc", "::tonic_types::pb")
        // Encoded into a CBOR map server-side, whose key order is part of the
        // bytes, so the ordering is a property of the type rather than of
        // remembering to sort at the call site.
        .btree_map(".flexiq.v1.StructuredArgs.kwargs")
        .compile_fds(set)
        .expect("failed to generate the flexiq.v1 client");
}

/// `crates/flexiq-cli` → the repository root.
fn workspace_root() -> PathBuf {
    let manifest = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("cargo sets this"));
    manifest
        .parent()
        .and_then(Path::parent)
        .map(Path::to_path_buf)
        .unwrap_or(manifest)
}
