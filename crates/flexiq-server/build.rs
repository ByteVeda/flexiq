//! Two generated files: the embedded dashboard SPA, and — behind the `grpc`
//! feature — the Rust types for the `flexiq.v1` wire contract.
//!
//! The SPA is a pnpm/vite build that lives outside the cargo tree, so it may
//! simply not exist — `cargo check` in CI never runs pnpm. A missing bundle
//! generates an empty table instead of failing the build; the server then
//! serves the "assets not bundled" page, exactly as the SDK dashboards do.

use std::env;
use std::fs;
use std::path::{Path, PathBuf};

/// Directories searched for a built SPA, first hit wins. `FLEXIQ_DASHBOARD_ASSETS_DIR`
/// overrides all of them (the deploy image sets it explicitly).
const CANDIDATE_DIRS: [&str; 2] = ["dashboard/dist", "sdks/python/flexiq/static/dashboard"];

/// The `FileDescriptorSet` buf builds from `contracts/proto`, committed and
/// version-gated. Codegen reads it, and `grpc/reflection.rs` embeds the same
/// bytes, so the types and what the server advertises cannot disagree.
#[cfg(feature = "grpc")]
const DESCRIPTOR: &str = "contracts/descriptor.binpb";

fn main() {
    println!("cargo:rerun-if-env-changed=FLEXIQ_DASHBOARD_ASSETS_DIR");

    #[cfg(feature = "grpc")]
    generate_proto_types();

    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("cargo always sets OUT_DIR"));
    let generated = match locate_assets() {
        Some(root) => {
            println!("cargo:rerun-if-changed={}", root.display());
            let mut files = Vec::new();
            collect_files(&root, &root, &mut files);
            files.sort();
            render_table(&files, &root)
        }
        None => render_table(&[], Path::new("")),
    };

    fs::write(out_dir.join("dashboard_assets.rs"), generated)
        .expect("failed to write the generated asset table");
}

/// Generate the `flexiq.v1` messages, service trait and client from the
/// committed descriptor.
///
/// Compiling the `FileDescriptorSet` rather than the `.proto` files is what
/// keeps `protoc` off the build path entirely — and it means the generated Rust
/// is derived from the exact artifact `scripts/proto-check.sh` gates and the
/// binary serves over reflection. Editing a `.proto` without regenerating the
/// descriptor therefore changes nothing here, which is the same staleness CI's
/// proto job already fails on.
#[cfg(feature = "grpc")]
fn generate_proto_types() {
    use prost::Message;

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
        .build_server(true)
        // The integration tests dial the listener with the generated client,
        // which is also what a Rust consumer of this contract would use.
        .build_client(true)
        // google.rpc.Status is already generated, once, in tonic-types — and
        // that is the crate whose StatusExt attaches the ErrorInfo details the
        // error model requires, so a second copy of the type here would be two
        // spellings of one message inside one process.
        .extern_path(".google.rpc", "::tonic_types::pb")
        .compile_fds(set)
        .expect("failed to generate the flexiq.v1 types");
}

/// Resolve the asset root, preferring an explicit override over the two
/// in-repo build outputs. Returns `None` when nothing has been built.
fn locate_assets() -> Option<PathBuf> {
    if let Ok(explicit) = env::var("FLEXIQ_DASHBOARD_ASSETS_DIR") {
        let path = PathBuf::from(explicit);
        return path.join("index.html").is_file().then_some(path);
    }
    let workspace_root = workspace_root();
    CANDIDATE_DIRS
        .iter()
        .map(|relative| workspace_root.join(relative))
        .find(|path| path.join("index.html").is_file())
}

/// `crates/flexiq-server` → the repository root.
fn workspace_root() -> PathBuf {
    let manifest = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("cargo sets this"));
    manifest
        .parent()
        .and_then(Path::parent)
        .map(Path::to_path_buf)
        .unwrap_or(manifest)
}

/// Depth-first walk collecting every regular file, as a path relative to `root`.
fn collect_files(root: &Path, dir: &Path, out: &mut Vec<String>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_files(root, &path, out);
        } else if let Ok(relative) = path.strip_prefix(root) {
            // Request paths are always `/`-separated, so normalise here rather
            // than at every lookup.
            out.push(relative.to_string_lossy().replace('\\', "/"));
        }
    }
}

fn render_table(files: &[String], root: &Path) -> String {
    let mut source = String::from(
        "/// Every file of the embedded SPA build, as `(request path, bytes)`.\n\
         pub static EMBEDDED_ASSETS: &[(&str, &[u8])] = &[\n",
    );
    for relative in files {
        let absolute = root.join(relative);
        source.push_str(&format!(
            "    ({:?}, include_bytes!({:?})),\n",
            relative,
            absolute.display().to_string()
        ));
    }
    source.push_str("];\n");
    source
}
