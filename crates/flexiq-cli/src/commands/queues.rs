//! `fq queues` — job counts, for one queue or for the whole namespace.
//!
//! `QueueStats` is the only stats RPC the producer door carries, and it answers
//! with six counts. It cannot enumerate queues and it reports no rates: an
//! unset `queue` means *aggregate the namespace*, not *list what is in it*.
//! Both gaps are #836, and the operator docs say so.

use anyhow::{anyhow, Result};

use crate::cli::QueuesArgs;
use crate::connect::Client;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_omitted_queue_leaves_the_field_unset() {
        assert!(request(&QueuesArgs { queue: None }).queue.is_none());
        assert_eq!(
            request(&QueuesArgs {
                queue: Some("mail".into())
            })
            .queue
            .as_deref(),
            Some("mail")
        );
    }
}
