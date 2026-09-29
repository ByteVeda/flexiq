//! `fq quota` — the namespace's quota (#841).
//!
//! Unlike an override, a quota is not read at worker start: every process
//! serving the namespace enforces a change within a couple of seconds. A set
//! *replaces* the quota — an omitted flag lifts that limit.

use anyhow::Result;

use super::{emit, refused};
use crate::cli::{QuotaCommand, SetQuotaArgs};
use crate::connect::AdminClient;
use crate::output;
use crate::output::admin::{empty_json, quota_envelope_json, quota_rows, QUOTA_COLUMNS};
use crate::pb::admin as pb;

/// Dispatch the three verbs.
pub async fn run(client: &mut AdminClient, command: &QuotaCommand, json: bool) -> Result<()> {
    match command {
        QuotaCommand::Get => get(client, json).await,
        QuotaCommand::Set(args) => set(client, args, json).await,
        QuotaCommand::Clear => clear(client, json).await,
    }
}

/// `fq quota get`: every limit, `-` for an unlimited one.
async fn get(client: &mut AdminClient, json: bool) -> Result<()> {
    let response = client
        .get_namespace_quota(pb::GetNamespaceQuotaRequest {})
        .await
        .map_err(refused)?
        .into_inner();
    print_quota(response.quota.as_ref(), json)
}

/// The quota request. Each omitted flag leaves its limit unset, and on this
/// RPC unset means "unlimited".
pub fn set_request(args: &SetQuotaArgs) -> pb::SetNamespaceQuotaRequest {
    let on_excess = match args.on_excess.as_deref() {
        Some("drop") => pb::QuotaOverflow::Drop,
        Some(_) => pb::QuotaOverflow::Reject,
        None => pb::QuotaOverflow::Unspecified,
    };
    pb::SetNamespaceQuotaRequest {
        quota: Some(pb::NamespaceQuota {
            max_pending: args.max_pending,
            on_excess: on_excess.into(),
            enqueue_rate: args.enqueue_rate.clone(),
            max_running: args.max_running,
            max_archived_rows: args.max_archived_rows,
            max_dead_rows: args.max_dead_rows,
        }),
    }
}

/// `fq quota set`: prints the quota as stored.
async fn set(client: &mut AdminClient, args: &SetQuotaArgs, json: bool) -> Result<()> {
    let response = client
        .set_namespace_quota(set_request(args))
        .await
        .map_err(refused)?
        .into_inner();
    print_quota(response.quota.as_ref(), json)
}

/// `fq quota clear`.
async fn clear(client: &mut AdminClient, json: bool) -> Result<()> {
    client
        .clear_namespace_quota(pb::ClearNamespaceQuotaRequest {})
        .await
        .map_err(refused)?;
    emit(json, empty_json, || {
        "cleared the namespace quota\n".to_string()
    })
}

fn print_quota(quota: Option<&pb::NamespaceQuota>, json: bool) -> Result<()> {
    emit(
        json,
        || quota_envelope_json(quota),
        || {
            let rows = quota.map(quota_rows).unwrap_or_default();
            output::table(&QUOTA_COLUMNS, &rows)
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args() -> SetQuotaArgs {
        SetQuotaArgs {
            max_pending: None,
            on_excess: None,
            enqueue_rate: None,
            max_running: None,
            max_archived_rows: None,
            max_dead_rows: None,
        }
    }

    #[test]
    fn every_flag_reaches_the_wire() {
        let args = SetQuotaArgs {
            max_pending: Some(10),
            on_excess: Some("drop".into()),
            enqueue_rate: Some("5/s".into()),
            max_running: Some(2),
            max_archived_rows: Some(100),
            max_dead_rows: Some(0),
        };
        let quota = set_request(&args).quota.expect("always sent");
        assert_eq!(quota.max_pending, Some(10));
        assert_eq!(quota.on_excess, pb::QuotaOverflow::Drop as i32);
        assert_eq!(quota.enqueue_rate.as_deref(), Some("5/s"));
        assert_eq!(quota.max_running, Some(2));
        assert_eq!(quota.max_archived_rows, Some(100));
        assert_eq!(quota.max_dead_rows, Some(0));
    }

    /// A replace, not a merge: every omitted flag is an unset limit.
    #[test]
    fn an_omitted_flag_is_an_unset_limit() {
        let quota = set_request(&args()).quota.expect("always sent");
        assert_eq!(quota, pb::NamespaceQuota::default());
    }
}
