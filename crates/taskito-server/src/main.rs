//! Command-line shell: read the environment, then run the server.

use anyhow::Result;
use clap::Parser;
use taskito_server::config::{
    dashboard::scrub_bootstrap_password, listen::scrub_attach_token, Config,
};
use taskito_server::runtime;

/// Environment variables the server reads, shown in `--help` because there are
/// no flags to document instead.
const ENV_HELP: &str = "\
Configuration (environment only):
  FLEXIQ_DSN                    storage connection string (required, except for
                                 a webhook-only deployment)
  FLEXIQ_BACKEND                sqlite | postgres | redis (default: from the DSN)
  FLEXIQ_NAMESPACE              tenant namespace scoping the scheduler and
                                 every dashboard view (unset = all namespaces)
  FLEXIQ_QUEUES                 comma-separated queues (default: default)
  FLEXIQ_WORKERS                dispatch concurrency (default: attached slots)
  FLEXIQ_MAINTENANCE            on | off — run retention and cleanup (default: on)
  FLEXIQ_LISTEN                 executor attach address, e.g. 127.0.0.1:7777
                                 or unix:/run/taskito.sock (default: off)
  FLEXIQ_ATTACH_TOKEN           shared secret executors present when attaching;
                                 required for a non-loopback FLEXIQ_LISTEN
  FLEXIQ_DASHBOARD              dashboard address, e.g. 127.0.0.1:8080 (default: off)
  FLEXIQ_DASHBOARD_AUTH         off | session (default: off)
  FLEXIQ_DASHBOARD_ASSETS       serve the SPA from this directory
  FLEXIQ_DASHBOARD_METRICS_TOKEN  bearer token for /metrics and /readiness
  FLEXIQ_DASHBOARD_PUBLIC_READINESS  1 to answer /readiness without a
                                 credential, for an orchestrator probe that
                                 cannot carry one (/metrics stays gated)
  FLEXIQ_ALLOW_INSECURE         1 to allow an unauthenticated off-host dashboard
  FLEXIQ_WEBHOOK_LISTEN         admission webhook address for executor sidecar
                                 injection, e.g. 0.0.0.0:9443 (default: off)
  FLEXIQ_WEBHOOK_TLS_CERT       PEM chain the webhook serves; required with it
  FLEXIQ_WEBHOOK_TLS_KEY        PEM key for that chain; required with it

At least one of FLEXIQ_LISTEN, FLEXIQ_DASHBOARD or FLEXIQ_WEBHOOK_LISTEN must
be set. FLEXIQ_DSN is required for all but a webhook-only deployment.";

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
    scrub_attach_token();
    runtime::run(config)
}
