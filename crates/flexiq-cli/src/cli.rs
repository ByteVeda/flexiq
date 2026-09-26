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
//!
//! Two doors behind one binary. `enqueue`, `jobs`, `tail` and `queues [NAME]` speak
//! `flexiq.v1.ProducerService` and need a `produce` token; everything else
//! speaks `flexiq.admin.v1.AdminService` and needs `inspect` to read and
//! `admin` to change anything.

use clap::{ArgGroup, Args, Parser, Subcommand};

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
    /// Follow jobs, or a queue, as they change state. With ids, exits once
    /// every job is finished; with `--queue`, runs until interrupted.
    /// Reconnects on its own after a dropped connection.
    Tail(TailArgs),
    /// Job counts, for one queue or for the whole namespace. `--list` lists
    /// every queue with whether it is paused.
    Queues(QueuesArgs),
    /// Stop dispatching from a queue. Jobs already running finish.
    Pause(QueueNameArgs),
    /// Resume dispatching from a paused queue.
    Resume(QueueNameArgs),
    /// Jobs finished per queue over a recent window, with per-minute rates.
    Throughput(ThroughputArgs),
    /// The namespace's registered workers and their heartbeats.
    Workers,
    /// Ask one worker to stop claiming, finish its jobs and exit — the stop a
    /// SIGTERM gives it. It reads the request on its next heartbeat.
    Drain(WorkerIdArgs),
    /// Read, replay and delete dead letters.
    #[command(subcommand)]
    Dlq(DlqCommand),
    /// Manage periodic (cron) tasks.
    #[command(subcommand)]
    Periodic(PeriodicCommand),
    /// Manage task and queue overrides. Workers read overrides when they
    /// start, so a change reaches the next worker start, not running ones.
    #[command(subcommand)]
    Overrides(OverridesCommand),
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

/// `fq tail`.
#[derive(Debug, Args)]
pub struct TailArgs {
    /// Job ids to follow, at most 100.
    #[arg(required_unless_present = "queue", conflicts_with = "queue")]
    pub ids: Vec<String>,
    /// Follow every job in this queue instead. Sees only the transitions the
    /// server process you reach handles itself.
    #[arg(short, long)]
    pub queue: Option<String>,
}

/// `fq jobs cancel`.
#[derive(Debug, Args)]
pub struct JobsCancelArgs {
    /// The job id.
    pub id: String,
}

/// `fq queues`.
///
/// `--list` is a flag rather than a `list` subcommand because a subcommand
/// would shadow the queue named `list` in `fq queues list`.
#[derive(Debug, Args)]
pub struct QueuesArgs {
    /// One queue. Omitted, the counts cover the whole namespace.
    #[arg(conflicts_with = "list")]
    pub queue: Option<String>,
    /// List every queue, with its counts and whether it is paused. Speaks the
    /// admin door, so the token needs `inspect` rather than `produce`.
    #[arg(long)]
    pub list: bool,
}

/// A verb that takes one queue name.
#[derive(Debug, Args)]
pub struct QueueNameArgs {
    /// The queue. One with no jobs yet may be paused; the pause holds once
    /// jobs arrive.
    pub queue: String,
}

/// `fq throughput`.
#[derive(Debug, Args)]
pub struct ThroughputArgs {
    /// How far back to count. Omitted is the server's five minutes; above 24
    /// hours is refused.
    #[arg(long)]
    pub window_ms: Option<i64>,
}

/// `fq drain`.
///
/// A top-level verb beside `pause` and `resume`, rather than `fq workers
/// drain`, so `fq workers` stays a bare listing with no subcommand to parse.
#[derive(Debug, Args)]
pub struct WorkerIdArgs {
    /// The worker's id, as `fq workers` lists it.
    pub worker_id: String,
}

/// `fq dlq`.
#[derive(Debug, Subcommand)]
pub enum DlqCommand {
    /// List dead letters, newest first.
    List(DlqListArgs),
    /// Read one dead letter.
    Show(DlqShowArgs),
    /// Enqueue a dead letter again as a fresh job, removing the entry. Not
    /// idempotent: each successful call makes a job.
    Replay(DeadLetterIdArgs),
    /// Delete one dead letter.
    Delete(DeadLetterIdArgs),
    /// Delete dead letters in bulk. Exactly one filter is required, so
    /// purging everything takes an explicit `--all`.
    Purge(DlqPurgeArgs),
}

/// `fq dlq list`.
#[derive(Debug, Args)]
pub struct DlqListArgs {
    /// Rows per page. Omitted is the server's default; the server may cap it.
    #[arg(long)]
    pub page_size: Option<i32>,
    /// The `nextPageToken` from a previous call.
    #[arg(long, conflicts_with = "all")]
    pub page_token: Option<String>,
    /// Follow every page and print them as one listing.
    #[arg(long)]
    pub all: bool,
}

/// `fq dlq show`.
#[derive(Debug, Args)]
pub struct DlqShowArgs {
    /// The dead letter's id — not the original job's.
    pub id: String,
    /// Also fetch the job's payload.
    #[arg(long)]
    pub payload: bool,
}

/// A verb that takes one dead-letter id.
#[derive(Debug, Args)]
pub struct DeadLetterIdArgs {
    /// The dead letter's id — not the original job's.
    pub id: String,
}

/// `fq dlq purge`.
#[derive(Debug, Args)]
#[command(group(
    ArgGroup::new("filter")
        .required(true)
        .args(["task", "before", "older_than_ms", "all"])
))]
pub struct DlqPurgeArgs {
    /// Only entries of this task.
    #[arg(short, long)]
    pub task: Option<String>,
    /// Only entries that dead-lettered before this RFC 3339 instant.
    #[arg(long, value_name = "RFC3339")]
    pub before: Option<String>,
    /// Only entries that dead-lettered more than this long ago.
    #[arg(long, value_name = "MS")]
    pub older_than_ms: Option<i64>,
    /// Every entry in the namespace.
    #[arg(long)]
    pub all: bool,
}

/// `fq periodic`.
#[derive(Debug, Subcommand)]
pub enum PeriodicCommand {
    /// List every periodic task.
    List,
    /// Read one periodic task.
    Show(PeriodicShowArgs),
    /// Create a periodic task, or replace an existing one's definition. A
    /// replace keeps whether the task is paused and when it last ran.
    Put(PeriodicPutArgs),
    /// Delete a periodic task. Jobs it already fired are untouched.
    Delete(PeriodicNameArgs),
    /// Stop a periodic task firing, keeping its definition.
    Pause(PeriodicNameArgs),
    /// Let a paused periodic task fire again.
    Resume(PeriodicNameArgs),
    /// Fire a periodic task now, paused or not. The schedule is unchanged.
    Trigger(PeriodicNameArgs),
}

/// A verb that takes one periodic task name.
#[derive(Debug, Args)]
pub struct PeriodicNameArgs {
    /// The periodic task's name, not the task it enqueues.
    pub name: String,
}

/// `fq periodic show`.
#[derive(Debug, Args)]
pub struct PeriodicShowArgs {
    /// The periodic task's name.
    pub name: String,
    /// Also fetch the payload each firing enqueues.
    #[arg(long)]
    pub payload: bool,
}

/// `fq periodic put`.
#[derive(Debug, Args)]
pub struct PeriodicPutArgs {
    /// The periodic task's name, unique within the namespace.
    pub name: String,
    /// Positional arguments each firing passes. Read as in `fq enqueue`: JSON,
    /// or a string if it is not valid JSON.
    pub args: Vec<String>,
    /// A keyword argument each firing passes, `name=value`, repeatable.
    #[arg(long = "kw", value_name = "KEY=VALUE")]
    pub kwargs: Vec<String>,
    /// The registered task each firing enqueues.
    #[arg(short, long)]
    pub task: String,
    /// Six fields, seconds first: `0 */5 * * * *` is every five minutes.
    #[arg(long)]
    pub cron: String,
    /// The queue each firing enqueues to. Omitted means the default.
    #[arg(short, long)]
    pub queue: Option<String>,
    /// IANA zone the cron expression is read in. Omitted is UTC.
    #[arg(long)]
    pub timezone: Option<String>,
    /// Create the task paused. Ignored when it already exists.
    #[arg(long)]
    pub paused: bool,
}

/// `fq overrides`.
///
/// A worker reads overrides when it starts, so every change here reaches the
/// next worker start and no running worker.
#[derive(Debug, Subcommand)]
pub enum OverridesCommand {
    /// List every task and queue override.
    List,
    /// Replace a task's override. Not a merge: a flag left out is cleared,
    /// not kept. Workers pick the change up when they next start.
    SetTask(SetTaskOverrideArgs),
    /// Remove a task's override, so its declared values apply again.
    ClearTask(TaskNameArgs),
    /// Replace a queue's override. Not a merge: a flag left out is cleared,
    /// not kept. Workers pick the change up when they next start.
    SetQueue(SetQueueOverrideArgs),
    /// Remove a queue's override.
    ClearQueue(QueueNameArgs),
}

/// A verb that takes one task name.
#[derive(Debug, Args)]
pub struct TaskNameArgs {
    /// The registered task name.
    pub task: String,
}

/// `fq overrides set-task`. Every flag is optional, and an omitted one is
/// cleared from the override rather than kept.
#[derive(Debug, Args)]
pub struct SetTaskOverrideArgs {
    /// The registered task name.
    pub task: String,
    /// `<count>/<unit>`, unit one of `s`, `m`, `h`: `100/m`.
    #[arg(long)]
    pub rate_limit: Option<String>,
    /// At most this many running at once.
    #[arg(long)]
    pub max_concurrent: Option<i32>,
    /// Attempts after the first.
    #[arg(long)]
    pub max_retries: Option<i32>,
    /// Base delay between retries.
    #[arg(long)]
    pub retry_backoff_ms: Option<i64>,
    /// How long one attempt may run. Whole seconds, at least one.
    #[arg(long)]
    pub timeout_ms: Option<i64>,
    /// Higher runs first.
    #[arg(long)]
    pub priority: Option<i32>,
    /// Hold this task's jobs without pausing the queue: `true` or `false`.
    #[arg(long, value_name = "BOOL")]
    pub paused: Option<bool>,
}

/// `fq overrides set-queue`. An omitted flag is cleared, not kept.
#[derive(Debug, Args)]
pub struct SetQueueOverrideArgs {
    /// The queue.
    pub queue: String,
    /// `<count>/<unit>`, unit one of `s`, `m`, `h`: `100/m`.
    #[arg(long)]
    pub rate_limit: Option<String>,
    /// At most this many of the queue's jobs running at once.
    #[arg(long)]
    pub max_concurrent: Option<i32>,
}

#[cfg(test)]
mod tests {
    use clap::{CommandFactory, Parser};

    use super::*;

    /// clap's own consistency checks: duplicate flags, a short that collides, a
    /// required positional after an optional one.
    #[test]
    fn the_tree_is_well_formed() {
        Cli::command().debug_assert();
    }

    fn parse(line: &[&str]) -> Result<Cli, clap::Error> {
        Cli::try_parse_from(std::iter::once("fq").chain(line.iter().copied()))
    }

    /// A bare `fq dlq purge` must not mean "everything": the wire reads an
    /// unset filter as every entry, so the CLI makes that an explicit flag.
    #[test]
    fn a_purge_needs_exactly_one_filter() {
        assert!(parse(&["dlq", "purge"]).is_err());
        assert!(parse(&["dlq", "purge", "--all", "--task", "x"]).is_err());
        assert!(parse(&[
            "dlq",
            "purge",
            "--before",
            "2026-01-01T00:00:00Z",
            "--task",
            "x"
        ])
        .is_err());
        for filter in [
            &["--all"][..],
            &["--task", "x"],
            &["--before", "2026-01-01T00:00:00Z"],
            &["--older-than-ms", "60000"],
        ] {
            let line: Vec<_> = ["dlq", "purge"].iter().chain(filter).copied().collect();
            assert!(parse(&line).is_ok(), "{line:?}");
        }
    }

    /// `fq queues NAME` is unchanged; `--list` is the admin listing, and the
    /// two together are a contradiction.
    #[test]
    fn queues_takes_a_name_or_list_but_not_both() {
        let Command::Queues(args) = parse(&["queues", "mail"]).expect("parses").command else {
            panic!("a queues command");
        };
        assert_eq!(args.queue.as_deref(), Some("mail"));
        assert!(!args.list);
        let Command::Queues(args) = parse(&["queues", "--list"]).expect("parses").command else {
            panic!("a queues command");
        };
        assert!(args.list && args.queue.is_none());
        assert!(parse(&["queues", "mail", "--list"]).is_err());
    }

    /// `fq workers` stays a bare listing; draining is its own verb.
    #[test]
    fn drain_takes_one_worker_id() {
        let Command::Drain(args) = parse(&["drain", "w-1"]).expect("parses").command else {
            panic!("a drain command");
        };
        assert_eq!(args.worker_id, "w-1");
        assert!(parse(&["drain"]).is_err());
        assert!(matches!(
            parse(&["workers"]).expect("parses").command,
            Command::Workers
        ));
    }

    #[test]
    fn a_page_token_and_all_are_exclusive() {
        assert!(parse(&["dlq", "list", "--all", "--page-token", "t"]).is_err());
    }

    #[test]
    fn a_task_override_pause_takes_an_explicit_boolean() {
        let Command::Overrides(OverridesCommand::SetTask(args)) =
            parse(&["overrides", "set-task", "send", "--paused", "false"])
                .expect("parses")
                .command
        else {
            panic!("a set-task command");
        };
        assert_eq!(args.paused, Some(false));
        assert!(parse(&["overrides", "set-task", "send", "--paused", "maybe"]).is_err());
    }

    #[test]
    fn periodic_put_takes_arguments_after_the_name() {
        let Command::Periodic(PeriodicCommand::Put(args)) = parse(&[
            "periodic",
            "put",
            "nightly",
            "--task",
            "report",
            "--cron",
            "0 0 3 * * *",
            "a@b.c",
            "3",
            "--kw",
            "k=v",
        ])
        .expect("parses")
        .command
        else {
            panic!("a put command");
        };
        assert_eq!(args.name, "nightly");
        assert_eq!(args.args, ["a@b.c", "3"]);
        assert_eq!(args.kwargs, ["k=v"]);
    }
}
