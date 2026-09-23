//! `fq periodic` — manage periodic (cron) tasks.
//!
//! A task is addressed by its periodic name, not by the task it enqueues: two
//! schedules may fire the same task.

use anyhow::Result;

use super::{emit, print_job, refused};
use crate::cli::{PeriodicCommand, PeriodicNameArgs, PeriodicPutArgs, PeriodicShowArgs};
use crate::connect::AdminClient;
use crate::output::admin::{
    empty_json, list_periodic_tasks_json, periodic_row, periodic_task_envelope_json,
    PERIODIC_COLUMNS,
};
use crate::pb::admin::{self as pb, put_periodic_task_request::Body};
use crate::safe::escape;
use crate::{args, output};

/// Dispatch the seven verbs.
pub async fn run(client: &mut AdminClient, command: &PeriodicCommand, json: bool) -> Result<()> {
    match command {
        PeriodicCommand::List => list(client, json).await,
        PeriodicCommand::Show(args) => show(client, args, json).await,
        PeriodicCommand::Put(args) => put(client, args, json).await,
        PeriodicCommand::Delete(args) => delete(client, args, json).await,
        PeriodicCommand::Pause(args) => pause(client, args, json).await,
        PeriodicCommand::Resume(args) => resume(client, args, json).await,
        PeriodicCommand::Trigger(args) => trigger(client, args, json).await,
    }
}

/// `fq periodic list`, by name.
async fn list(client: &mut AdminClient, json: bool) -> Result<()> {
    let response = client
        .list_periodic_tasks(pb::ListPeriodicTasksRequest {})
        .await
        .map_err(refused)?
        .into_inner();
    emit(
        json,
        || list_periodic_tasks_json(&response),
        || {
            let rows: Vec<_> = response.periodic_tasks.iter().map(periodic_row).collect();
            output::table(&PERIODIC_COLUMNS, &rows)
        },
    )
}

/// The read request.
pub fn show_request(args: &PeriodicShowArgs) -> pb::GetPeriodicTaskRequest {
    pb::GetPeriodicTaskRequest {
        name: args.name.clone(),
        include_payload: args.payload,
    }
}

/// `fq periodic show`.
async fn show(client: &mut AdminClient, args: &PeriodicShowArgs, json: bool) -> Result<()> {
    let response = client
        .get_periodic_task(show_request(args))
        .await
        .map_err(refused)?
        .into_inner();
    print_task(response.periodic_task.as_ref(), json)
}

/// The create-or-replace request.
///
/// The arguments go as the `structured` arm, read exactly as `fq enqueue`
/// reads them, so the server encodes each firing's envelope with the one
/// in-tree encoder.
pub fn put_request(cli_args: &PeriodicPutArgs) -> Result<pb::PutPeriodicTaskRequest> {
    Ok(pb::PutPeriodicTaskRequest {
        name: cli_args.name.clone(),
        task_name: cli_args.task.clone(),
        cron: cli_args.cron.clone(),
        // Empty means "default", which is what an omitted flag means too.
        queue: cli_args.queue.clone().unwrap_or_default(),
        body: Some(Body::Structured(args::structured(
            &cli_args.args,
            &cli_args.kwargs,
        )?)),
        start_paused: cli_args.paused,
        timezone: cli_args.timezone.clone(),
    })
}

/// `fq periodic put`.
async fn put(client: &mut AdminClient, args: &PeriodicPutArgs, json: bool) -> Result<()> {
    let response = client
        .put_periodic_task(put_request(args)?)
        .await
        .map_err(refused)?
        .into_inner();
    print_task(response.periodic_task.as_ref(), json)
}

/// The delete request.
pub fn delete_request(args: &PeriodicNameArgs) -> pb::DeletePeriodicTaskRequest {
    pb::DeletePeriodicTaskRequest {
        name: args.name.clone(),
    }
}

/// `fq periodic delete`.
async fn delete(client: &mut AdminClient, args: &PeriodicNameArgs, json: bool) -> Result<()> {
    client
        .delete_periodic_task(delete_request(args))
        .await
        .map_err(refused)?;
    emit(json, empty_json, || {
        format!("deleted {}\n", escape(&args.name))
    })
}

/// The pause request.
pub fn pause_request(args: &PeriodicNameArgs) -> pb::PausePeriodicTaskRequest {
    pb::PausePeriodicTaskRequest {
        name: args.name.clone(),
    }
}

/// `fq periodic pause`.
async fn pause(client: &mut AdminClient, args: &PeriodicNameArgs, json: bool) -> Result<()> {
    let response = client
        .pause_periodic_task(pause_request(args))
        .await
        .map_err(refused)?
        .into_inner();
    print_task(response.periodic_task.as_ref(), json)
}

/// The resume request.
pub fn resume_request(args: &PeriodicNameArgs) -> pb::ResumePeriodicTaskRequest {
    pb::ResumePeriodicTaskRequest {
        name: args.name.clone(),
    }
}

/// `fq periodic resume`.
async fn resume(client: &mut AdminClient, args: &PeriodicNameArgs, json: bool) -> Result<()> {
    let response = client
        .resume_periodic_task(resume_request(args))
        .await
        .map_err(refused)?
        .into_inner();
    print_task(response.periodic_task.as_ref(), json)
}

/// The trigger request.
pub fn trigger_request(args: &PeriodicNameArgs) -> pb::TriggerPeriodicTaskRequest {
    pb::TriggerPeriodicTaskRequest {
        name: args.name.clone(),
    }
}

/// `fq periodic trigger`: prints the job it enqueued. Not idempotent — each
/// call is a job.
async fn trigger(client: &mut AdminClient, args: &PeriodicNameArgs, json: bool) -> Result<()> {
    let response = client
        .trigger_periodic_task(trigger_request(args))
        .await
        .map_err(refused)?
        .into_inner();
    print_job(response.job.as_ref(), json)
}

/// One periodic task, as JSON or as a one-row table with its payload after.
fn print_task(task: Option<&pb::PeriodicTask>, json: bool) -> Result<()> {
    emit(
        json,
        || periodic_task_envelope_json(task),
        || {
            let rows = task
                .map(|task| vec![periodic_row(task)])
                .unwrap_or_default();
            let mut text = output::table(&PERIODIC_COLUMNS, &rows);
            if let Some(payload) = task.and_then(|task| task.payload.as_ref()) {
                text.push_str(&format!("\npayload: {}\n", output::base64(payload)));
            }
            text
        },
    )
}

#[cfg(test)]
mod tests {
    use prost_types::value::Kind;

    use super::*;

    fn put_args() -> PeriodicPutArgs {
        PeriodicPutArgs {
            name: "nightly".into(),
            args: vec!["a@b.c".into(), "3".into()],
            kwargs: vec!["k=v".into()],
            task: "report".into(),
            cron: "0 0 3 * * *".into(),
            queue: None,
            timezone: None,
            paused: false,
        }
    }

    #[test]
    fn every_put_flag_reaches_the_wire() {
        let mut args = put_args();
        args.queue = Some("reports".into());
        args.timezone = Some("Europe/Paris".into());
        args.paused = true;
        let request = put_request(&args).expect("builds");
        assert_eq!(request.name, "nightly");
        assert_eq!(request.task_name, "report");
        assert_eq!(request.cron, "0 0 3 * * *");
        assert_eq!(request.queue, "reports");
        assert_eq!(request.timezone.as_deref(), Some("Europe/Paris"));
        assert!(request.start_paused);
    }

    /// Arguments are read as `fq enqueue` reads them and sent structured.
    #[test]
    fn put_arguments_are_the_structured_arm() {
        let request = put_request(&put_args()).expect("builds");
        let Some(Body::Structured(body)) = request.body else {
            panic!("fq sends the structured arm");
        };
        assert!(matches!(&body.args[0].kind, Some(Kind::StringValue(s)) if s == "a@b.c"));
        assert!(matches!(body.args[1].kind, Some(Kind::NumberValue(n)) if n == 3.0));
        assert!(body.kwargs.contains_key("k"));
    }

    /// Omitted queue and timezone mean the server's defaults, and an omitted
    /// timezone must stay unset rather than become the zone named "".
    #[test]
    fn omitted_queue_and_timezone_are_the_defaults() {
        let request = put_request(&put_args()).expect("builds");
        assert_eq!(request.queue, "");
        assert!(request.timezone.is_none());
        assert!(!request.start_paused);
    }

    #[test]
    fn a_bad_keyword_argument_is_refused_before_the_call() {
        let mut args = put_args();
        args.kwargs = vec!["oops".into()];
        assert!(put_request(&args).is_err());
    }

    #[test]
    fn the_name_verbs_take_the_periodic_name() {
        let name = PeriodicNameArgs {
            name: "nightly".into(),
        };
        assert_eq!(delete_request(&name).name, "nightly");
        assert_eq!(pause_request(&name).name, "nightly");
        assert_eq!(resume_request(&name).name, "nightly");
        assert_eq!(trigger_request(&name).name, "nightly");
        let show = show_request(&PeriodicShowArgs {
            name: "nightly".into(),
            payload: true,
        });
        assert!(show.include_payload);
    }
}
