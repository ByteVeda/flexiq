//! A standalone command line for a FlexiQ server.
//!
//! Speaks `flexiq.v1.ProducerService` over gRPC and links no FlexiQ SDK: there
//! is no storage layer here, no database driver and no language runtime. The
//! three SDK CLIs talk to the database off a DSN, which is the wrong door for
//! an operator holding a scoped API token and no database credential; this one
//! talks to the server.
//!
//! What it can do is therefore bounded by what the wire exposes, which is
//! deliberately less than an SDK CLI can do against a database. The operator
//! documentation lists what is missing and what it is blocked on.
#![deny(missing_docs)]

pub mod args;
pub mod cli;
pub mod commands;
pub mod connect;
pub mod error;
pub mod output;
pub mod pb;

/// Run one invocation: dial the door, then dispatch.
pub async fn run(cli: cli::Cli) -> anyhow::Result<()> {
    let token = connect::token_from_env()?;
    let mut client = connect::connect(&cli.endpoint, &token).await?;
    match &cli.command {
        cli::Command::Enqueue(args) => commands::enqueue::run(&mut client, args, cli.json).await,
        cli::Command::Jobs(command) => commands::jobs::run(&mut client, command, cli.json).await,
        // The remaining arm lands with its command.
        _ => Err(anyhow::anyhow!("not yet wired")),
    }
}
