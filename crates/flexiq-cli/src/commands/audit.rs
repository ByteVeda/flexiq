//! `fq audit` — read the audit trail back.
//!
//! An `inspect` read on the operator door. A record names a token by its public
//! id — the one `flexiq-server token list` shows — and never by its secret.

use anyhow::{anyhow, Result};

use super::{emit, refused};
use crate::cli::{AuditCommand, AuditListArgs};
use crate::connect::AdminClient;
use crate::output;
use crate::output::admin::{audit_row, list_audit_records_json, AUDIT_COLUMNS};
use crate::pb::admin as pb;
use crate::safe::escape;
use crate::time::instant_at;

/// The kind `--job` stands for.
const JOB: &str = "job";

/// Dispatch the one verb.
pub async fn run(client: &mut AdminClient, command: &AuditCommand, json: bool) -> Result<()> {
    match command {
        AuditCommand::List(args) => list(client, list_request(args)?, args.all, json).await,
    }
}

/// The listing request for the first page asked for.
pub fn list_request(args: &AuditListArgs) -> Result<pb::ListAuditRecordsRequest> {
    let (target_kind, target) = match (&args.job, &args.target, &args.kind) {
        (Some(job), _, _) => (JOB.to_string(), job.clone()),
        (None, Some(target), _) => {
            let (kind, id) = target
                .split_once(':')
                .filter(|(kind, id)| !kind.is_empty() && !id.is_empty())
                .ok_or_else(|| {
                    anyhow!("`--target {target}` is not `kind:id`, e.g. `--target queue:emails`")
                })?;
            (kind.to_string(), id.to_string())
        }
        (None, None, kind) => (kind.clone().unwrap_or_default(), String::new()),
    };
    Ok(pb::ListAuditRecordsRequest {
        // Zero is the server's default page size, as an omitted flag means.
        page_size: args.page_size.unwrap_or_default(),
        page_token: args.page_token.clone().unwrap_or_default(),
        token_id: args.token_id.clone().unwrap_or_default(),
        target_kind,
        target,
        since: instant_at(args.since.as_deref(), "--since")?,
        until: instant_at(args.until.as_deref(), "--until")?,
    })
}

/// `fq audit list`, one page or, with `--all`, every page as one listing.
async fn list(
    client: &mut AdminClient,
    request: pb::ListAuditRecordsRequest,
    all: bool,
    json: bool,
) -> Result<()> {
    let response = if all {
        list_all(client, request).await?
    } else {
        client
            .list_audit_records(request)
            .await
            .map_err(refused)?
            .into_inner()
    };
    emit(
        json,
        || list_audit_records_json(&response),
        || {
            let rows: Vec<_> = response.records.iter().map(audit_row).collect();
            let mut text = output::table(&AUDIT_COLUMNS, &rows);
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

/// Follow `next_page_token` to the end, as one response with no token. A
/// server that hands back the token it was just given is refused, not looped.
async fn list_all(
    client: &mut AdminClient,
    mut request: pb::ListAuditRecordsRequest,
) -> Result<pb::ListAuditRecordsResponse> {
    let mut all = pb::ListAuditRecordsResponse::default();
    loop {
        let page = client
            .list_audit_records(request.clone())
            .await
            .map_err(refused)?
            .into_inner();
        all.records.extend(page.records);
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

#[cfg(test)]
mod tests {
    use super::*;

    fn args() -> AuditListArgs {
        AuditListArgs {
            token_id: None,
            job: None,
            target: None,
            kind: None,
            since: None,
            until: None,
            page_size: None,
            page_token: None,
            all: false,
        }
    }

    #[test]
    fn no_flags_is_the_whole_trail_at_the_servers_page_size() {
        let request = list_request(&args()).expect("builds");
        assert_eq!(request, pb::ListAuditRecordsRequest::default());
    }

    #[test]
    fn job_is_the_job_target() {
        let mut args = args();
        args.job = Some("j-1".into());
        args.token_id = Some("tok".into());
        let request = list_request(&args).expect("builds");
        assert_eq!(request.target_kind, "job");
        assert_eq!(request.target, "j-1");
        assert_eq!(request.token_id, "tok");
    }

    #[test]
    fn target_splits_at_the_first_colon() {
        let mut args = args();
        args.target = Some("queue:emails:eu".into());
        let request = list_request(&args).expect("builds");
        assert_eq!(request.target_kind, "queue");
        assert_eq!(request.target, "emails:eu");
    }

    #[test]
    fn a_target_without_both_halves_is_refused() {
        for target in ["emails", ":emails", "queue:"] {
            let mut args = args();
            args.target = Some(target.into());
            let error = list_request(&args).expect_err(target).to_string();
            assert!(error.contains("kind:id"), "{error}");
        }
    }

    #[test]
    fn kind_alone_filters_by_kind() {
        let mut args = args();
        args.kind = Some("worker".into());
        let request = list_request(&args).expect("builds");
        assert_eq!(request.target_kind, "worker");
        assert_eq!(request.target, "");
    }

    #[test]
    fn since_and_until_are_instants() {
        let mut args = args();
        args.since = Some("2025-09-10T10:26:40Z".into());
        args.until = Some("not a time".into());
        assert!(list_request(&args).is_err(), "a bad instant is refused");
        args.until = None;
        let request = list_request(&args).expect("builds");
        assert_eq!(request.since.map(|at| at.seconds), Some(1_757_500_000));
    }
}
