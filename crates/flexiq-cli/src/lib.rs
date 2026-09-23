//! A standalone command line for a FlexiQ server.
//!
//! Speaks `flexiq.v1.ProducerService` and `flexiq.admin.v1.AdminService` over
//! gRPC and links no FlexiQ SDK: there is no storage layer here, no database
//! driver and no language runtime. The three SDK CLIs talk to the database off
//! a DSN, which is the wrong door for an operator holding a scoped API token
//! and no database credential; this one talks to the server.
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
pub mod safe;
pub mod time;

use cli::Command;
use connect::{connect, connect_admin};

/// Run one invocation: dial the door the verb speaks, then dispatch.
pub async fn run(cli: cli::Cli) -> anyhow::Result<()> {
    let token = connect::token_from_env()?;
    let (endpoint, json) = (cli.endpoint.as_str(), cli.json);
    match &cli.command {
        Command::Enqueue(args) => {
            let mut client = connect(endpoint, &token).await?;
            commands::enqueue::run(&mut client, args, json).await
        }
        Command::Jobs(command) => {
            let mut client = connect(endpoint, &token).await?;
            commands::jobs::run(&mut client, command, json).await
        }
        Command::Queues(args) if args.list => {
            let mut client = connect_admin(endpoint, &token).await?;
            commands::queues::list(&mut client, json).await
        }
        Command::Queues(args) => {
            let mut client = connect(endpoint, &token).await?;
            commands::queues::run(&mut client, args, json).await
        }
        Command::Pause(args) => {
            let mut client = connect_admin(endpoint, &token).await?;
            commands::queues::pause(&mut client, args, json).await
        }
        Command::Resume(args) => {
            let mut client = connect_admin(endpoint, &token).await?;
            commands::queues::resume(&mut client, args, json).await
        }
        Command::Throughput(args) => {
            let mut client = connect_admin(endpoint, &token).await?;
            commands::throughput::run(&mut client, args, json).await
        }
        Command::Workers => {
            let mut client = connect_admin(endpoint, &token).await?;
            commands::workers::run(&mut client, json).await
        }
        Command::Drain(args) => {
            let mut client = connect_admin(endpoint, &token).await?;
            commands::workers::drain(&mut client, args, json).await
        }
        Command::Dlq(command) => {
            let mut client = connect_admin(endpoint, &token).await?;
            commands::dlq::run(&mut client, command, json).await
        }
        Command::Periodic(command) => {
            let mut client = connect_admin(endpoint, &token).await?;
            commands::periodic::run(&mut client, command, json).await
        }
        Command::Overrides(command) => {
            let mut client = connect_admin(endpoint, &token).await?;
            commands::overrides::run(&mut client, command, json).await
        }
    }
}
