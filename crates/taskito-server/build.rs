//! Embed the compiled dashboard SPA into the binary, when one is present.
//!
//! The SPA is a pnpm/vite build that lives outside the cargo tree, so it may
//! simply not exist — `cargo check` in CI never runs pnpm. A missing bundle
//! generates an empty table instead of failing the build; the server then
//! serves the "assets not bundled" page, exactly as the SDK dashboards do.

use std::env;
use std::fs;
use std::path::{Path, PathBuf};

/// Directories searched for a built SPA, first hit wins. `TASKITO_DASHBOARD_ASSETS_DIR`
/// overrides all of them (the deploy image sets it explicitly).
const CANDIDATE_DIRS: [&str; 2] = ["dashboard/dist", "sdks/python/taskito/static/dashboard"];

fn main() {
    println!("cargo:rerun-if-env-changed=TASKITO_DASHBOARD_ASSETS_DIR");

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

/// Resolve the asset root, preferring an explicit override over the two
/// in-repo build outputs. Returns `None` when nothing has been built.
fn locate_assets() -> Option<PathBuf> {
    if let Ok(explicit) = env::var("TASKITO_DASHBOARD_ASSETS_DIR") {
        let path = PathBuf::from(explicit);
        return path.join("index.html").is_file().then_some(path);
    }
    let workspace_root = workspace_root();
    CANDIDATE_DIRS
        .iter()
        .map(|relative| workspace_root.join(relative))
        .find(|path| path.join("index.html").is_file())
}

/// `crates/taskito-server` → the repository root.
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
