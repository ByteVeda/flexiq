//! The audit trail, read back for the dashboard's audit page (#995).
//!
//! Admin-only like the token inventory: `auth::gate` lists this path among the
//! admin-read prefixes, because the trail names every credential and user and
//! everything each one touched. Reading it is not itself recorded — only
//! mutations are (`dashboard::audit`).

use axum::extract::State;
use axum::Json;
use serde_json::{json, Value};

use flexiq_core::scheduler::retention::DEFAULT_NAMESPACE;
use flexiq_core::storage::cursor::{decode_cursor, next_cursor};
use flexiq_core::{AuditFilter, AuditRecord, Storage};

use crate::dashboard::blocking::on_storage_api;
use crate::dashboard::error::{ApiError, ApiResult};
use crate::dashboard::query::Params;
use crate::dashboard::state::SharedState;

/// Default page, and the most one page may ask for — the same bounds as
/// `ListAuditRecords`.
const DEFAULT_LIMIT: i64 = 50;
const MAX_LIMIT: i64 = 500;

/// `GET /api/audit-records` — a page of the trail, newest first.
///
/// Filters mirror `ListAuditRecords`: `token_id`, `principal_kind`,
/// `target_kind`, `target`, and `since` (inclusive) / `until` (exclusive) in
/// Unix ms. `after` resumes from a previous page's `next_cursor`.
pub async fn list(State(state): State<SharedState>, params: Params) -> ApiResult<Json<Value>> {
    let filter = filter(&params)?;
    let limit = params.int("limit", DEFAULT_LIMIT)?.clamp(1, MAX_LIMIT);
    let after = params.get("after").map(str::to_string);
    // The writer files records under the default namespace when the dashboard
    // serves none, so the reader must look there too.
    let namespace = state
        .namespace
        .clone()
        .unwrap_or_else(|| DEFAULT_NAMESPACE.to_string());

    let records = on_storage_api(&state, move |storage| {
        let cursor = after
            .as_deref()
            .map(decode_cursor)
            .transpose()
            .map_err(|error| ApiError::BadRequest(error.to_string()))?;
        storage
            .list_audit_after(&namespace, &filter, limit, cursor)
            .map_err(ApiError::from)
    })
    .await?;

    let cursor = next_cursor(&records, limit, |record| (record.at_ms, &record.id));
    Ok(Json(json!({
        "records": records.iter().map(record_json).collect::<Vec<_>>(),
        "next_cursor": cursor,
    })))
}

/// The storage filter a query string asks for; an empty value is no filter.
fn filter(params: &Params) -> ApiResult<AuditFilter> {
    let text = |key: &str| params.get(key).map(str::to_string);
    let bound = |key: &str| -> ApiResult<Option<i64>> {
        Ok(params.get(key).is_some().then_some(params.int(key, 0)?))
    };
    Ok(AuditFilter {
        token_id: text("token_id"),
        principal_kind: text("principal_kind"),
        target_kind: text("target_kind"),
        target: text("target"),
        since_ms: bound("since")?,
        until_ms: bound("until")?,
    })
}

/// One record as the SPA reads it. The namespace is left out: a dashboard only
/// ever lists its own.
fn record_json(record: &AuditRecord) -> Value {
    json!({
        "id": record.id,
        "at": record.at_ms,
        "principal_kind": record.principal_kind,
        "token_id": record.token_id,
        "principal": record.principal,
        "operation": record.operation,
        "target_kind": record.target_kind,
        "target": record.target,
        "outcome": record.outcome,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_filter_reads_from_its_parameter() {
        let params = Params::parse(
            "token_id=abc&principal_kind=token&target_kind=job&target=j1&since=10&until=20",
        );
        assert_eq!(
            filter(&params).expect("parses"),
            AuditFilter {
                token_id: Some("abc".into()),
                principal_kind: Some("token".into()),
                target_kind: Some("job".into()),
                target: Some("j1".into()),
                since_ms: Some(10),
                until_ms: Some(20),
            }
        );
    }

    #[test]
    fn empty_parameters_filter_nothing() {
        let params = Params::parse("token_id=&since=&until=");
        assert_eq!(filter(&params).expect("parses"), AuditFilter::default());
    }

    #[test]
    fn a_malformed_bound_is_refused() {
        for raw in ["since=yesterday", "until=-1"] {
            assert!(
                matches!(filter(&Params::parse(raw)), Err(ApiError::BadRequest(_))),
                "{raw} must be refused"
            );
        }
    }
}
