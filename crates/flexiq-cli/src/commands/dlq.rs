//! `fq dlq` — list, read, replay, delete and purge dead letters.
//!
//! An id here is always the dead letter's own, never the original job's: the
//! two are different strings, and replay and delete take the former.

use anyhow::{anyhow, Result};

use super::{emit, print_job, refused};
use crate::cli::{DeadLetterIdArgs, DlqCommand, DlqListArgs, DlqPurgeArgs, DlqShowArgs};
use crate::connect::AdminClient;
use crate::output;
use crate::output::admin::{
    dead_letter_envelope_json, dead_letter_row, empty_json, list_dead_letters_json, purge_json,
    DEAD_LETTER_COLUMNS,
};
use crate::pb::admin::{self as pb, purge_dead_letters_request::Filter};
use crate::safe::escape;
use crate::time::{instant_at, instant_before};

/// Dispatch the five verbs.
pub async fn run(client: &mut AdminClient, command: &DlqCommand, json: bool) -> Result<()> {
    match command {
        DlqCommand::List(args) => list(client, args, json).await,
        DlqCommand::Show(args) => show(client, args, json).await,
        DlqCommand::Replay(args) => replay(client, args, json).await,
        DlqCommand::Delete(args) => delete(client, args, json).await,
        DlqCommand::Purge(args) => {
            let request = purge_request(args, chrono::Utc::now().timestamp_millis())?;
            purge(client, request, json).await
        }
    }
}

/// The listing request for the first page asked for.
pub fn list_request(args: &DlqListArgs) -> pb::ListDeadLettersRequest {
    pb::ListDeadLettersRequest {
        // Zero is the server's default page size, as an omitted flag means.
        page_size: args.page_size.unwrap_or_default(),
        page_token: args.page_token.clone().unwrap_or_default(),
    }
}

/// `fq dlq list`, one page or, with `--all`, every page as one listing.
async fn list(client: &mut AdminClient, args: &DlqListArgs, json: bool) -> Result<()> {
    let response = if args.all {
        list_all(client, list_request(args)).await?
    } else {
        client
            .list_dead_letters(list_request(args))
            .await
            .map_err(refused)?
            .into_inner()
    };
    emit(
        json,
        || list_dead_letters_json(&response),
        || {
            let rows: Vec<_> = response.dead_letters.iter().map(dead_letter_row).collect();
            let mut text = output::table(&DEAD_LETTER_COLUMNS, &rows);
            // The token is opaque and server-issued, so printing the flag is
            // the only way an operator can ask for the next page.
            if !response.next_page_token.is_empty() {
                text.push_str(&format!(
                    "\nnext page: --page-token {}\n",
                    escape(&response.next_page_token)
                ));
            }
            text
        },
    )
}

/// Follow `next_page_token` to the end, as one response with no token.
///
/// A server that handed back the token it was just given would loop this
/// forever; that is refused rather than trusted.
async fn list_all(
    client: &mut AdminClient,
    mut request: pb::ListDeadLettersRequest,
) -> Result<pb::ListDeadLettersResponse> {
    let mut all = pb::ListDeadLettersResponse::default();
    loop {
        let page = client
            .list_dead_letters(request.clone())
            .await
            .map_err(refused)?
            .into_inner();
        all.dead_letters.extend(page.dead_letters);
        if page.next_page_token.is_empty() {
            return Ok(all);
        }
        if page.next_page_token == request.page_token {
            return Err(anyhow!(
                "the server returned the same page token twice; stopping rather than looping"
            ));
        }
        request.page_token = page.next_page_token;
    }
}

/// The read request.
pub fn show_request(args: &DlqShowArgs) -> pb::GetDeadLetterRequest {
    pb::GetDeadLetterRequest {
        dead_letter_id: args.id.clone(),
        include_payload: args.payload,
    }
}

/// `fq dlq show`: a one-row table, then the error and payload, which do not
/// fit a column.
async fn show(client: &mut AdminClient, args: &DlqShowArgs, json: bool) -> Result<()> {
    let response = client
        .get_dead_letter(show_request(args))
        .await
        .map_err(refused)?
        .into_inner();
    let entry = response.dead_letter.as_ref();
    emit(
        json,
        || dead_letter_envelope_json(entry),
        || {
            let rows = entry
                .map(|entry| vec![dead_letter_row(entry)])
                .unwrap_or_default();
            let mut text = output::table(&DEAD_LETTER_COLUMNS, &rows);
            if let Some(error) = entry.and_then(|entry| entry.error.as_deref()) {
                text.push_str(&format!("\nerror: {}\n", escape(error)));
            }
            if let Some(metadata) = entry.and_then(|entry| entry.metadata.as_deref()) {
                text.push_str(&format!("metadata: {}\n", escape(metadata)));
            }
            if let Some(payload) = entry.and_then(|entry| entry.payload.as_ref()) {
                text.push_str(&format!("payload: {}\n", output::base64(payload)));
            }
            text
        },
    )
}

/// The replay request.
pub fn replay_request(args: &DeadLetterIdArgs) -> pb::ReplayDeadLetterRequest {
    pb::ReplayDeadLetterRequest {
        dead_letter_id: args.id.clone(),
    }
}

/// `fq dlq replay`: prints the new job. Not idempotent — a second call finds
/// no entry.
async fn replay(client: &mut AdminClient, args: &DeadLetterIdArgs, json: bool) -> Result<()> {
    let response = client
        .replay_dead_letter(replay_request(args))
        .await
        .map_err(refused)?
        .into_inner();
    print_job(response.job.as_ref(), json)
}

/// The delete request.
pub fn delete_request(args: &DeadLetterIdArgs) -> pb::DeleteDeadLetterRequest {
    pb::DeleteDeadLetterRequest {
        dead_letter_id: args.id.clone(),
    }
}

/// `fq dlq delete`.
async fn delete(client: &mut AdminClient, args: &DeadLetterIdArgs, json: bool) -> Result<()> {
    client
        .delete_dead_letter(delete_request(args))
        .await
        .map_err(refused)?;
    emit(json, empty_json, || {
        format!("deleted {}\n", escape(&args.id))
    })
}

/// The purge request, with exactly one filter.
///
/// clap already requires one; this refuses too, because on the wire an unset
/// filter means *every entry*, and a caller that builds [`DlqPurgeArgs`] by
/// hand must not get that by omission.
pub fn purge_request(args: &DlqPurgeArgs, now_ms: i64) -> Result<pb::PurgeDeadLettersRequest> {
    let before = instant_at(args.before.as_deref(), "--before")?;
    let older = instant_before(now_ms, args.older_than_ms, "--older-than-ms")?;
    let filter = match (&args.task, before, older, args.all) {
        (Some(task), None, None, false) => Some(Filter::TaskName(task.clone())),
        (None, Some(at), None, false) | (None, None, Some(at), false) => {
            Some(Filter::FailedBefore(at))
        }
        (None, None, None, true) => None,
        _ => {
            return Err(anyhow!(
                "give exactly one of --task, --before, --older-than-ms or --all"
            ))
        }
    };
    Ok(pb::PurgeDeadLettersRequest { filter })
}

/// `fq dlq purge`: prints how many entries went.
async fn purge(
    client: &mut AdminClient,
    request: pb::PurgeDeadLettersRequest,
    json: bool,
) -> Result<()> {
    let response = client
        .purge_dead_letters(request)
        .await
        .map_err(refused)?
        .into_inner();
    emit(
        json,
        || purge_json(&response),
        || format!("purged {}\n", response.purged),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: i64 = 1_757_500_000_000;

    fn purge_args() -> DlqPurgeArgs {
        DlqPurgeArgs {
            task: None,
            before: None,
            older_than_ms: None,
            all: false,
        }
    }

    #[test]
    fn an_omitted_page_is_the_servers_default() {
        let request = list_request(&DlqListArgs {
            page_size: None,
            page_token: None,
            all: false,
        });
        assert_eq!(request.page_size, 0);
        assert_eq!(request.page_token, "");
    }

    #[test]
    fn a_page_reaches_the_wire() {
        let request = list_request(&DlqListArgs {
            page_size: Some(10),
            page_token: Some("t".into()),
            all: false,
        });
        assert_eq!(request.page_size, 10);
        assert_eq!(request.page_token, "t");
    }

    #[test]
    fn show_replay_and_delete_take_the_dead_letters_id() {
        let request = show_request(&DlqShowArgs {
            id: "dl-1".into(),
            payload: true,
        });
        assert_eq!(request.dead_letter_id, "dl-1");
        assert!(request.include_payload);
        let id = DeadLetterIdArgs { id: "dl-2".into() };
        assert_eq!(replay_request(&id).dead_letter_id, "dl-2");
        assert_eq!(delete_request(&id).dead_letter_id, "dl-2");
    }

    #[test]
    fn a_task_filter_is_the_task_arm() {
        let mut args = purge_args();
        args.task = Some("charge".into());
        let request = purge_request(&args, NOW).expect("builds");
        assert_eq!(request.filter, Some(Filter::TaskName("charge".into())));
    }

    #[test]
    fn before_and_older_than_are_the_instant_arm() {
        let mut args = purge_args();
        args.before = Some("2025-09-10T10:26:40Z".into());
        let Some(Filter::FailedBefore(at)) = purge_request(&args, NOW).expect("builds").filter
        else {
            panic!("the instant arm");
        };
        assert_eq!(at.seconds, 1_757_500_000);

        let mut args = purge_args();
        args.older_than_ms = Some(3_600_000);
        let Some(Filter::FailedBefore(at)) = purge_request(&args, NOW).expect("builds").filter
        else {
            panic!("the instant arm");
        };
        assert_eq!(at.seconds, 1_757_496_400);
    }

    /// `--all` is the only way to send no filter, which the door reads as
    /// every entry in the namespace.
    #[test]
    fn only_all_sends_no_filter() {
        let mut args = purge_args();
        args.all = true;
        assert!(purge_request(&args, NOW).expect("builds").filter.is_none());
        assert!(purge_request(&purge_args(), NOW).is_err());
    }

    #[test]
    fn two_filters_are_refused() {
        let mut args = purge_args();
        args.all = true;
        args.task = Some("charge".into());
        assert!(purge_request(&args, NOW).is_err());
    }

    #[test]
    fn an_older_than_that_does_not_fit_is_refused_by_flag_name() {
        let mut args = purge_args();
        args.older_than_ms = Some(i64::MIN);
        let error = purge_request(&args, NOW).expect_err("out of range");
        assert!(error.to_string().contains("--older-than-ms"), "{error}");
    }
}
