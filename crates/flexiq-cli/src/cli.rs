//! The command tree.
//!
//! Parsing only: every subcommand's behaviour lives in [`crate::commands`], so
//! the surface an operator sees is one file and the mapping from flags to wire
//! fields is testable without a server.
//!
//! Flag names follow the Node SDK's CLI wherever the two overlap — `-q/--queue`,
//! `--priority`, `--max-retries`, `--delay-ms`, `--unique-key`, `--json` — so an
//! operator who knows one surface can guess the other. Durations are `*-ms`
//! integers for the same reason: a second spelling of the same value buys
//! nothing.

use clap::{Args, Parser, Subcommand};

/// A standalone command line for a FlexiQ server.
#[derive(Debug, Parser)]
#[command(name = "fq", version, about, long_about = None)]
pub struct Cli {
    /// Where the server's gRPC door listens. The scheme picks the transport:
    /// `http://` plaintext, `https://` TLS, `unix:/path` a Unix socket.
    #[arg(
        long,
        global = true,
        env = "FLEXIQ_ENDPOINT",
        default_value = "http://127.0.0.1:50051"
    )]
    pub endpoint: String,

    /// Print proto3 JSON instead of a table.
    #[arg(long, global = true)]
    pub json: bool,

    /// What to do.
    #[command(subcommand)]
    pub command: Command,
}

/// The top-level verbs.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Submit a job.
    Enqueue(EnqueueArgs),
    /// Read and cancel jobs.
    #[command(subcommand)]
    Jobs(JobsCommand),
    /// Job counts, for one queue or for the whole namespace.
    Queues(QueuesArgs),
}

/// `fq enqueue`.
#[derive(Debug, Args)]
pub struct EnqueueArgs {
    /// The registered task name.
    pub task: String,
    /// Positional arguments. Each is read as JSON, or as a string if it is not
    /// valid JSON, so `3` is a number and `hello` is a string.
    pub args: Vec<String>,
    /// A keyword argument, `name=value`, repeatable. The value follows the same
    /// JSON-then-string rule as a positional.
    #[arg(long = "kw", value_name = "KEY=VALUE")]
    pub kwargs: Vec<String>,
    /// The queue to submit to. Omitted means the server's default.
    #[arg(short, long)]
    pub queue: Option<String>,
    /// Higher runs first.
    #[arg(long)]
    pub priority: Option<i32>,
    /// Attempts after the first.
    #[arg(long)]
    pub max_retries: Option<i32>,
    /// Wait this long before the job becomes eligible to run.
    #[arg(long)]
    pub delay_ms: Option<i64>,
    /// How long one attempt may run before the scheduler reclaims it.
    #[arg(long)]
    pub timeout_ms: Option<i64>,
    /// After this long the job is cancelled instead of dispatched.
    #[arg(long)]
    pub expires_in_ms: Option<i64>,
    /// How long the result is kept after completion.
    #[arg(long)]
    pub result_ttl_ms: Option<i64>,
    /// Dedupe against the queue's active jobs.
    #[arg(long)]
    pub unique_key: Option<String>,
    /// Opaque JSON text, carried through byte for byte.
    #[arg(long)]
    pub metadata: Option<String>,
    /// Free prose stored with the job.
    #[arg(long)]
    pub notes: Option<String>,
    /// A job id this job waits on. Repeatable.
    #[arg(long, value_name = "ID")]
    pub depends_on: Vec<String>,
}

/// `fq jobs`.
#[derive(Debug, Subcommand)]
pub enum JobsCommand {
    /// List jobs, newest first.
    List(JobsListArgs),
    /// Read one job.
    Get(JobsGetArgs),
    /// Request cancellation.
    Cancel(JobsCancelArgs),
}

/// `fq jobs list`.
#[derive(Debug, Args)]
pub struct JobsListArgs {
    /// Only this status. Either the short name (`pending`) or the enum's own
    /// spelling (`JOB_STATUS_PENDING`).
    #[arg(long)]
    pub status: Option<String>,
    /// Only this queue.
    #[arg(short, long)]
    pub queue: Option<String>,
    /// Only this task name.
    #[arg(short, long)]
    pub task: Option<String>,
    /// How many rows to ask for. The server may return fewer.
    #[arg(long)]
    pub limit: Option<i32>,
    /// The `nextPageToken` from a previous call.
    #[arg(long)]
    pub page_token: Option<String>,
}

/// `fq jobs get`.
#[derive(Debug, Args)]
pub struct JobsGetArgs {
    /// The job id.
    pub id: String,
    /// Also fetch the request payload.
    #[arg(long)]
    pub payload: bool,
    /// Also fetch the return value.
    #[arg(long)]
    pub result: bool,
}

/// `fq jobs cancel`.
#[derive(Debug, Args)]
pub struct JobsCancelArgs {
    /// The job id.
    pub id: String,
}

/// `fq queues`.
#[derive(Debug, Args)]
pub struct QueuesArgs {
    /// One queue. Omitted, the counts cover the whole namespace.
    pub queue: Option<String>,
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory;

    use super::Cli;

    /// clap's own consistency checks: duplicate flags, a short that collides, a
    /// required positional after an optional one.
    #[test]
    fn the_tree_is_well_formed() {
        Cli::command().debug_assert();
    }
}
