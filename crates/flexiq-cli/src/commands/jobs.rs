//! `fq jobs` — list, read and cancel.

use anyhow::{anyhow, Result};

use crate::cli::{JobsCancelArgs, JobsCommand, JobsGetArgs, JobsListArgs};
use crate::connect::Client;
use crate::{error, output, pb};

/// Dispatch the three verbs.
pub async fn run(client: &mut Client, command: &JobsCommand, json: bool) -> Result<()> {
    match command {
        JobsCommand::List(args) => list(client, args, json).await,
        JobsCommand::Get(args) => get(client, args, json).await,
        JobsCommand::Cancel(args) => cancel(client, args, json).await,
    }
}

/// The listing request. Every filter is `optional` on the wire, and an unset
/// one means "every value" — which is not the same message as an empty string.
pub fn list_request(args: &JobsListArgs) -> Result<pb::ListJobsRequest> {
    let status = args
        .status
        .as_deref()
        .map(output::parse_status)
        .transpose()?
        .map(|status| status as i32);
    Ok(pb::ListJobsRequest {
        status,
        queue: args.queue.clone(),
        task_name: args.task.clone(),
        // Zero means "the server's default page size", which is what an
        // omitted flag means too.
        page_size: args.limit.unwrap_or_default(),
        page_token: args.page_token.clone().unwrap_or_default(),
    })
}

/// `fq jobs list`.
async fn list(client: &mut Client, args: &JobsListArgs, json: bool) -> Result<()> {
    let response = client
        .list_jobs(list_request(args)?)
        .await
        .map_err(|status| anyhow!("{}", error::describe(&status)))?
        .into_inner();

    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&output::list_jobs_json(&response))?
        );
        return Ok(());
    }

    let rows: Vec<_> = response.jobs.iter().map(output::job_row).collect();
    print!("{}", output::table(&output::JOB_COLUMNS, &rows));
    // The token is opaque and server-issued, so an operator cannot construct
    // the next call from what they can see. Printing the flag spells it out.
    if !response.next_page_token.is_empty() {
        println!("\nnext page: --page-token {}", response.next_page_token);
    }
    Ok(())
}

/// `fq jobs get`.
async fn get(client: &mut Client, args: &JobsGetArgs, json: bool) -> Result<()> {
    let response = client
        .get_job(pb::GetJobRequest {
            job_id: args.id.clone(),
            include_payload: args.payload,
            include_result: args.result,
        })
        .await
        .map_err(|status| anyhow!("{}", error::describe(&status)))?
        .into_inner();
    print_job(response.job.as_ref(), json)
}

/// `fq jobs cancel`.
///
/// Idempotent on the wire: a pending job becomes cancelled, a running one keeps
/// running with `cancelRequested` set, and a terminal one is unchanged.
async fn cancel(client: &mut Client, args: &JobsCancelArgs, json: bool) -> Result<()> {
    let response = client
        .cancel_job(pb::CancelJobRequest {
            job_id: args.id.clone(),
        })
        .await
        .map_err(|status| anyhow!("{}", error::describe(&status)))?
        .into_inner();
    print_job(response.job.as_ref(), json)
}

/// One job, as JSON or as a one-row table.
fn print_job(job: Option<&pb::Job>, json: bool) -> Result<()> {
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&output::job_envelope_json(job))?
        );
        return Ok(());
    }
    let rows = job
        .map(|job| vec![output::job_row(job)])
        .unwrap_or_default();
    print!("{}", output::table(&output::JOB_COLUMNS, &rows));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(status: Option<&str>) -> JobsListArgs {
        JobsListArgs {
            status: status.map(str::to_string),
            queue: None,
            task: None,
            limit: None,
            page_token: None,
        }
    }

    /// An unset filter means "every value" to the door. Sending an empty
    /// string instead would mean "the queue named ''", which matches nothing.
    #[test]
    fn an_unset_filter_stays_unset_rather_than_becoming_empty() {
        let request = list_request(&args(None)).expect("builds");
        assert!(request.status.is_none());
        assert!(request.queue.is_none());
        assert!(request.task_name.is_none());
        assert_eq!(request.page_size, 0);
        assert_eq!(request.page_token, "");
    }

    #[test]
    fn a_short_status_name_reaches_the_wire_as_its_number() {
        let request = list_request(&JobsListArgs {
            status: Some("running".into()),
            queue: Some("mail".into()),
            task: Some("send_email".into()),
            limit: Some(10),
            page_token: Some("t".into()),
        })
        .expect("builds");
        assert_eq!(request.status, Some(pb::JobStatus::Running as i32));
        assert_eq!(request.queue.as_deref(), Some("mail"));
        assert_eq!(request.task_name.as_deref(), Some("send_email"));
        assert_eq!(request.page_size, 10);
        assert_eq!(request.page_token, "t");
    }

    #[test]
    fn an_unknown_status_is_refused_before_the_call() {
        assert!(list_request(&args(Some("gone"))).is_err());
    }
}
