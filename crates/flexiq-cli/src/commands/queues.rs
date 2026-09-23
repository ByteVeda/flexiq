//! `fq queues`, `fq pause` and `fq resume`.
//!
//! `fq queues [NAME]` is the producer door's `QueueStats`: six counts for one
//! queue or aggregated over the namespace, answerable with a `produce` token.
//! `fq queues --list`, `pause` and `resume` are the admin door's, which can
//! enumerate queues and knows which are paused.

use anyhow::{anyhow, Result};

use super::{emit, refused};
use crate::cli::{QueueNameArgs, QueuesArgs};
use crate::connect::{AdminClient, Client};
use crate::output::admin::{queue_envelope_json, queue_row, QUEUE_COLUMNS};
use crate::{error, output, pb};

/// The two columns of a counts table.
const COLUMNS: [&str; 2] = ["state", "count"];

/// The request. An omitted name leaves the field unset, which the door reads
/// as the whole namespace — a different question from the queue named `""`.
pub fn request(args: &QueuesArgs) -> pb::QueueStatsRequest {
    pb::QueueStatsRequest {
        queue: args.queue.clone(),
    }
}

/// Fetch the counts and print them.
pub async fn run(client: &mut Client, args: &QueuesArgs, json: bool) -> Result<()> {
    let response = client
        .queue_stats(request(args))
        .await
        .map_err(|status| anyhow!("{}", error::describe(&status)))?
        .into_inner();

    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&output::queue_stats_json(&response))?
        );
    } else {
        print!(
            "{}",
            output::table(&COLUMNS, &output::queue_stats_rows(&response))
        );
    }
    Ok(())
}

/// `fq queues --list`: every queue, by name, with whether it is paused.
pub async fn list(client: &mut AdminClient, json: bool) -> Result<()> {
    let response = client
        .list_queues(pb::admin::ListQueuesRequest {})
        .await
        .map_err(refused)?
        .into_inner();
    emit(
        json,
        || output::admin::list_queues_json(&response),
        || {
            let rows: Vec<_> = response.queues.iter().map(queue_row).collect();
            output::table(&QUEUE_COLUMNS, &rows)
        },
    )
}

/// The pause request.
pub fn pause_request(args: &QueueNameArgs) -> pb::admin::PauseQueueRequest {
    pb::admin::PauseQueueRequest {
        queue: args.queue.clone(),
    }
}

/// The resume request.
pub fn resume_request(args: &QueueNameArgs) -> pb::admin::ResumeQueueRequest {
    pb::admin::ResumeQueueRequest {
        queue: args.queue.clone(),
    }
}

/// `fq pause`. Idempotent: pausing a paused queue is the same answer.
pub async fn pause(client: &mut AdminClient, args: &QueueNameArgs, json: bool) -> Result<()> {
    let response = client
        .pause_queue(pause_request(args))
        .await
        .map_err(refused)?
        .into_inner();
    print_queue(response.queue.as_ref(), json)
}

/// `fq resume`. Idempotent, like [`pause`].
pub async fn resume(client: &mut AdminClient, args: &QueueNameArgs, json: bool) -> Result<()> {
    let response = client
        .resume_queue(resume_request(args))
        .await
        .map_err(refused)?
        .into_inner();
    print_queue(response.queue.as_ref(), json)
}

/// The queue after a pause or resume, as JSON or a one-row table.
fn print_queue(queue: Option<&pb::admin::Queue>, json: bool) -> Result<()> {
    emit(
        json,
        || queue_envelope_json(queue),
        || {
            let rows = queue
                .map(|queue| vec![queue_row(queue)])
                .unwrap_or_default();
            output::table(&QUEUE_COLUMNS, &rows)
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_omitted_queue_leaves_the_field_unset() {
        let args = |queue: Option<&str>| QueuesArgs {
            queue: queue.map(str::to_string),
            list: false,
        };
        assert!(request(&args(None)).queue.is_none());
        assert_eq!(request(&args(Some("mail"))).queue.as_deref(), Some("mail"));
    }

    #[test]
    fn pause_and_resume_name_the_queue() {
        let args = QueueNameArgs {
            queue: "mail".into(),
        };
        assert_eq!(pause_request(&args).queue, "mail");
        assert_eq!(resume_request(&args).queue, "mail");
    }
}
