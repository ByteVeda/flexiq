//! Writes `contracts/openapi.json`.
//!
//! Invoked by `scripts/proto-check.sh`, which runs it into a scratch file and
//! compares — so this writes the document and nothing else to stdout, and every
//! diagnostic goes to stderr.

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use flexiq_openapi::document;

/// The committed contract, relative to the workspace root.
const DESCRIPTOR: &str = "contracts/descriptor.binpb";

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("flexiq-openapi: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), String> {
    let options = Options::parse(std::env::args().skip(1))?;

    let descriptor = std::fs::read(&options.descriptor).map_err(|error| {
        format!(
            "{} could not be read: {error}",
            options.descriptor.display()
        )
    })?;
    let rendered = document(&descriptor).map_err(|error| error.to_string())?;

    match &options.out {
        Some(path) => std::fs::write(path, rendered)
            .map_err(|error| format!("{} could not be written: {error}", path.display())),
        None => {
            print!("{rendered}");
            Ok(())
        }
    }
}

/// What the binary was asked to do.
struct Options {
    descriptor: PathBuf,
    out: Option<PathBuf>,
}

impl Options {
    fn parse(arguments: impl Iterator<Item = String>) -> Result<Self, String> {
        let mut options = Self {
            descriptor: workspace_root().join(DESCRIPTOR),
            out: None,
        };
        let mut arguments = arguments;
        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "--descriptor" => {
                    options.descriptor = PathBuf::from(value(&argument, &mut arguments)?)
                }
                "--out" => options.out = Some(PathBuf::from(value(&argument, &mut arguments)?)),
                "--help" | "-h" => {
                    println!("{USAGE}");
                    std::process::exit(0);
                }
                other => return Err(format!("unknown argument `{other}`\n\n{USAGE}")),
            }
        }
        Ok(options)
    }
}

const USAGE: &str = "\
usage: flexiq-openapi [--descriptor <path>] [--out <path>]

  --descriptor <path>  the FileDescriptorSet to read (default: contracts/descriptor.binpb)
  --out <path>         where to write the document (default: stdout)";

fn value(flag: &str, arguments: &mut impl Iterator<Item = String>) -> Result<String, String> {
    arguments
        .next()
        .ok_or_else(|| format!("{flag} needs a path\n\n{USAGE}"))
}

/// The workspace root, from where this crate was compiled.
///
/// Baked in at build time, so the binary finds the descriptor whatever
/// directory it is run from — which is what lets the shell script call it
/// without knowing where cargo put it.
fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."))
}
