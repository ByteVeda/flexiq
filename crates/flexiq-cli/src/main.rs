//! The `fq` entry point.
#![deny(missing_docs)]

use clap::Parser;

use flexiq_cli::cli::Cli;
use flexiq_cli::error::EXIT_FAILURE;

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    if let Err(error) = flexiq_cli::run(cli).await {
        // `{:#}` walks the anyhow chain, so a transport failure prints the
        // endpoint it was dialling as well as what the socket said.
        eprintln!("error: {error:#}");
        std::process::exit(EXIT_FAILURE);
    }
}
