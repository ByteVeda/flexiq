//! The `fq` entry point.
#![deny(missing_docs)]

use clap::Parser;

use flexiq_cli::cli::Cli;

fn main() {
    // Dispatch arrives with the first command; parsing already gives `--help`
    // and `--version`, which is what this target owes the release wiring.
    let _cli = Cli::parse();
}
