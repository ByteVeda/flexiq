//! Command-line shell: read the environment, then run the server.

use anyhow::Result;
use clap::Parser;
use taskito_server::config::{dashboard::scrub_bootstrap_password, Config};
use taskito_server::runtime;

/// Environment variables the server reads, shown in `--help` because there are
/// no flags to document instead.
const ENV_HELP: &str = "\
Configuration (environment only):
  TASKITO_DSN                    storage connection string (required)
  TASKITO_BACKEND                sqlite | postgres | redis (default: from the DSN)
  TASKITO_NAMESPACE              tenant namespace to scope the scheduler to
  TASKITO_QUEUES                 comma-separated queues (default: default)
  TASKITO_WORKERS                dispatch concurrency (default: attached slots)
  TASKITO_MAINTENANCE            on | off — run retention and cleanup (default: on)
  TASKITO_LISTEN                 executor attach address, e.g. 127.0.0.1:7777
                                 or unix:/run/taskito.sock (default: off)
  TASKITO_DASHBOARD              dashboard address, e.g. 127.0.0.1:8080 (default: off)
  TASKITO_DASHBOARD_AUTH         off | session (default: off)
  TASKITO_DASHBOARD_ASSETS       serve the SPA from this directory
  TASKITO_DASHBOARD_METRICS_TOKEN  bearer token for /metrics and /readiness
  TASKITO_ALLOW_INSECURE         1 to allow an unauthenticated off-host dashboard

At least one of TASKITO_LISTEN or TASKITO_DASHBOARD must be set.";

#[derive(Parser)]
#[command(
    name = "taskito-server",
    version,
    about = "Taskito scheduler, executor attach listener, and dashboard",
    after_help = ENV_HELP
)]
struct Cli {}

fn main() -> Result<()> {
    // Default to info so a deployment logs its bind addresses and attachments
    // without anyone having to set RUST_LOG first.
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    Cli::parse();

    let config = Config::from_env()?;
    scrub_bootstrap_password();
    runtime::run(config)
}
