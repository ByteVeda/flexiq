//! Command-line shell: read the environment, then run the server.

use anyhow::Result;
use clap::{Parser, Subcommand};
use flexiq_server::config::{
    dashboard::scrub_bootstrap_password, listen::scrub_attach_token,
    push::scrub_push_target_secrets, Config,
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
  FLEXIQ_WORKERS                dispatch concurrency (default: the dispatch
                                 path's slots — what executors advertise, or
                                 FLEXIQ_PUSH_TARGET_CAPACITY under push)
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
                                 its token's scope: `produce` or `execute`.
                                 With FLEXIQ_PUSH_TARGET_URL set, only the
                                 producer door is served — this process dials
                                 out, so there is nothing to attach to
  FLEXIQ_GRPC_EXECUTOR_STREAM_MAX_AGE  seconds an executor's attach stream lives
                                 before the scheduler drains it and closes it,
                                 so the executor reconnects and can be placed
                                 elsewhere (default: 1800; 0 never rotates)
  FLEXIQ_GRPC_KEEPALIVE_INTERVAL  seconds between HTTP/2 keepalive pings on an
                                 idle connection (default: 60; 0 sends none)
  FLEXIQ_GRPC_REQUEST_TIMEOUT   seconds a call may take to produce its response
                                 head before the listener answers
                                 DEADLINE_EXCEEDED. The whole call for a unary
                                 RPC; it bounds no response body, so it closes
                                 no stream — Attach, health Watch and
                                 reflection stay open. Use
                                 FLEXIQ_GRPC_EXECUTOR_STREAM_MAX_AGE for an
                                 attach stream (default: 30; 0 is unbounded)
  FLEXIQ_GRPC_MAX_CONCURRENT_REQUESTS  calls one connection may have in flight
                                 (default: 256; 0 is unlimited)
  FLEXIQ_PUSH_TARGET_URL        where the scheduler POSTs a claimed job, e.g.
                                 https://executor.internal/run (default: off).
                                 Requires a build with the `http-target` cargo
                                 feature; mutually exclusive with FLEXIQ_LISTEN
  FLEXIQ_PUSH_TARGET_CAPACITY   jobs the target may run at once (required — a
                                 push target announces no slots of its own)
  FLEXIQ_PUSH_TARGET_ALLOW      comma-separated hosts and CIDRs the target's
                                 resolved address must match (required);
                                 loopback, link-local and cloud metadata are
                                 refused however this is set
  FLEXIQ_PUSH_TARGET_TIMEOUT    seconds one dispatch may take before it is
                                 abandoned as failed (default: 60)
  FLEXIQ_PUSH_TARGET_CONNECT_TIMEOUT  seconds the connection may take to establish
                                 (default: 5)
  FLEXIQ_PUSH_TARGET_DRAIN      seconds shutdown waits for in-flight
                                 dispatches before abandoning them, and then
                                 again for each to settle — a shutdown runs to
                                 at most twice this (default: 30)
  FLEXIQ_PUSH_TARGET_MAX_REQUEST_BYTES  ceiling on one job's request body (default:
                                 8388608, i.e. 8 MiB)
  FLEXIQ_PUSH_TARGET_MAX_RESPONSE_BYTES  ceiling on one response body read back
                                 (default: 1048576, i.e. 1 MiB)
  FLEXIQ_PUSH_TARGET_AUTH       none | bearer | hmac | oidc | sigv4 (default:
                                 none)
  FLEXIQ_PUSH_TARGET_TOKEN      bearer secret; required for
                                 FLEXIQ_PUSH_TARGET_AUTH=bearer
  FLEXIQ_PUSH_TARGET_HMAC_SECRET  HMAC-SHA256 signing secret; required for
                                 FLEXIQ_PUSH_TARGET_AUTH=hmac
  FLEXIQ_PUSH_TARGET_HMAC_KEY_ID  key identifier sent alongside an HMAC
                                 signature (optional)
  FLEXIQ_PUSH_TARGET_OIDC_SOURCE  google | azure-imds | azure-app-service;
                                 required for FLEXIQ_PUSH_TARGET_AUTH=oidc
  FLEXIQ_PUSH_TARGET_OIDC_AUDIENCE  the aud claim the receiver checks; required
                                 for FLEXIQ_PUSH_TARGET_AUTH=oidc
  FLEXIQ_PUSH_TARGET_AZURE_CLIENT_ID  a user-assigned identity's client id, for
                                 FLEXIQ_PUSH_TARGET_OIDC_SOURCE=azure-imds (at
                                 most one of this, ..._AZURE_OBJECT_ID and
                                 ..._AZURE_MSI_RES_ID may be set)
  FLEXIQ_PUSH_TARGET_AZURE_OBJECT_ID  a user-assigned identity's object id; see
                                 FLEXIQ_PUSH_TARGET_AZURE_CLIENT_ID
  FLEXIQ_PUSH_TARGET_AZURE_MSI_RES_ID  a user-assigned identity's Azure resource id;
                                 see FLEXIQ_PUSH_TARGET_AZURE_CLIENT_ID
  FLEXIQ_PUSH_TARGET_AWS_SOURCE  default-chain | environment | container | imds
                                 (default: default-chain); for
                                 FLEXIQ_PUSH_TARGET_AUTH=sigv4
  FLEXIQ_PUSH_TARGET_AWS_REGION  overrides the region inferred from the target
                                 URL
  FLEXIQ_PUSH_TARGET_AWS_SERVICE  overrides the service inferred from the target
                                 URL

At least one of FLEXIQ_LISTEN, FLEXIQ_DASHBOARD, FLEXIQ_WEBHOOK_LISTEN,
FLEXIQ_GRPC_LISTEN or FLEXIQ_PUSH_TARGET_URL must be set. FLEXIQ_DSN is
required for all but a webhook-only deployment. FLEXIQ_PUSH_TARGET_URL and
FLEXIQ_LISTEN are mutually exclusive — a Worker holds exactly one dispatcher.";

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
    scrub_push_target_secrets();
    runtime::run(config)
}
