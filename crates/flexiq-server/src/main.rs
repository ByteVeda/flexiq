//! Command-line shell: read the environment, then run the server.

use anyhow::Result;
use clap::{Parser, Subcommand};
use flexiq_server::config::{
    dashboard::scrub_bootstrap_password, listen::scrub_attach_token, Config,
};
use flexiq_server::runtime;
use flexiq_server::tokens::cli::TokenCommand;

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
                                 or unix:/run/flexiq.sock (default: off)
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
  FLEXIQ_GRPC_LISTEN            gRPC producer and executor doors, e.g.
                                 127.0.0.1:50051 or unix:/run/flexiq-grpc.sock
                                 (default: off). Requires FLEXIQ_NAMESPACE and a
                                 build with the `grpc` cargo feature. Callers
                                 present an API token; mint one with
                                 `flexiq-server token create`, and see
                                 `token --help`. Which door a caller reaches is
                                 its token's scope: `produce` or `execute`
  FLEXIQ_GRPC_EXECUTOR_STREAM_MAX_AGE  seconds an executor's attach stream lives
                                 before the scheduler drains it and closes it,
                                 so the executor reconnects and can be placed
                                 elsewhere (default: 1800; 0 never rotates)
  FLEXIQ_GRPC_KEEPALIVE_INTERVAL  seconds between HTTP/2 keepalive pings on an
                                 idle connection (default: 60; 0 sends none)
  FLEXIQ_GRPC_REQUEST_TIMEOUT   seconds one call may take before the listener
                                 answers DEADLINE_EXCEEDED; does not bound an
                                 attach stream (default: 30; 0 is unbounded)
  FLEXIQ_GRPC_MAX_CONCURRENT_REQUESTS  calls one connection may have in flight
                                 (default: 256; 0 is unlimited)

At least one of FLEXIQ_LISTEN, FLEXIQ_DASHBOARD, FLEXIQ_WEBHOOK_LISTEN or
FLEXIQ_GRPC_LISTEN must be set. FLEXIQ_DSN is required for all but a
webhook-only deployment.";

#[derive(Parser)]
#[command(
    name = "flexiq-server",
    version,
    about = "FlexiQ scheduler, executor attach listener, dashboard, and gRPC door",
    after_help = ENV_HELP
)]
struct Cli {
    /// An administrative action to take instead of running the server.
    #[command(subcommand)]
    command: Option<Command>,
}

/// What the binary does when it is not being a server.
#[derive(Subcommand)]
enum Command {
    /// Mint, list and revoke the API tokens the gRPC door accepts.
    #[command(subcommand_help_heading = "Tokens")]
    Token(TokenCommand),
}

fn main() -> Result<()> {
    // Default to info so a deployment logs its bind addresses and attachments
    // without anyone having to set RUST_LOG first.
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    // An administrative action configures itself from the environment but runs
    // no role, so it never reaches `Config::from_env`, which requires one.
    if let Some(Command::Token(command)) = Cli::parse().command {
        return flexiq_server::tokens::cli::run(command);
    }

    let config = Config::from_env()?;
    scrub_bootstrap_password();
    scrub_attach_token();
    runtime::run(config)
}
