//! `ListAuditRecords` (#840): reading the trail back.
//!
//! An `inspect` read, like every listing on this door. The trail names token
//! ids and principals but no secret, and an operator who may list workers and
//! dead letters may see who touched them.

use flexiq_core::{AuditFilter, AuditRecord, Storage};
use prost_types::Timestamp;
use tonic::{Response, Status};

use super::Scoped;
use crate::grpc::blocking::on_storage;
use crate::grpc::pb::admin as pb;
use crate::grpc::producer::convert::{millis_from_timestamp, timestamp};
use crate::grpc::producer::cursor::Cursor;
use crate::grpc::producer::reads::page_size;

/// A page of the trail, newest first.
///
/// The page token is the producer door's opaque cursor over the last row's
/// `(at_ms, id)` — the keyset `list_audit_after` resumes from.
pub(crate) async fn list(
    scoped: &Scoped,
    request: pb::ListAuditRecordsRequest,
) -> Result<Response<pb::ListAuditRecordsResponse>, Status> {
    let limit = page_size(request.page_size)?;
    let cursor = match request.page_token.as_str() {
        "" => None,
        token => Some(Cursor::decode(token)?),
    };
    let filter = filter(&request);

    let namespace = scoped.namespace_owned();
    let records = on_storage(scoped.storage(), move |storage| {
        storage.list_audit_after(
            &namespace,
            &filter,
            i64::from(limit),
            cursor
                .as_ref()
                .map(|cursor| (cursor.created_at, cursor.id.as_str())),
        )
    })
    .await?;

    // A full page may have a successor; a short one is the end.
    let next_page_token = (records.len() == limit as usize)
        .then(|| records.last())
        .flatten()
        .map(|record| {
            Cursor {
                created_at: record.at_ms,
                id: record.id.clone(),
            }
            .encode()
        })
        .unwrap_or_default();

    Ok(Response::new(pb::ListAuditRecordsResponse {
        records: records.into_iter().map(to_wire).collect(),
        next_page_token,
    }))
}

/// The storage filter a request asks for. An empty string is an unset field,
/// as everywhere on this wire.
fn filter(request: &pb::ListAuditRecordsRequest) -> AuditFilter {
    let set = |value: &str| (!value.is_empty()).then(|| value.to_string());
    AuditFilter {
        token_id: set(&request.token_id),
        principal_kind: set(&request.principal_kind),
        target_kind: set(&request.target_kind),
        target: set(&request.target),
        since_ms: request.since.as_ref().map(bound_millis),
        until_ms: request.until.as_ref().map(bound_millis),
    }
}

/// A bound in the millisecond units records are stamped in, rounded **up**.
/// A record stamped `m` was made at or after `m` and before `m + 1`, so both
/// `since` (inclusive) and `until` (exclusive) at `m + 0.5` must read as
/// `m + 1`: rounding down would admit a record made before `since`, and drop
/// one made before `until`.
fn bound_millis(value: &Timestamp) -> i64 {
    let floor = millis_from_timestamp(value);
    if value.nanos % NANOS_PER_MILLI == 0 {
        floor
    } else {
        floor.saturating_add(1)
    }
}

const NANOS_PER_MILLI: i32 = 1_000_000;

fn to_wire(record: AuditRecord) -> pb::AuditRecord {
    pb::AuditRecord {
        id: record.id,
        time: Some(timestamp(record.at_ms)),
        token_id: record.token_id,
        principal: record.principal,
        operation: record.operation,
        target_kind: record.target_kind.unwrap_or_default(),
        target: record.target.unwrap_or_default(),
        outcome: record.outcome,
        principal_kind: record.principal_kind,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_strings_are_unset_filters() {
        let request = pb::ListAuditRecordsRequest {
            token_id: "tok".into(),
            principal_kind: "user".into(),
            since: Some(timestamp(1_000)),
            ..Default::default()
        };
        assert_eq!(
            filter(&request),
            AuditFilter {
                token_id: Some("tok".into()),
                principal_kind: Some("user".into()),
                since_ms: Some(1_000),
                ..Default::default()
            }
        );
    }

    #[test]
    fn a_submillisecond_bound_rounds_up_to_the_next_record_stamp() {
        let at = |nanos| Timestamp { seconds: 1, nanos };
        assert_eq!(bound_millis(&at(0)), 1_000);
        assert_eq!(bound_millis(&at(2_000_000)), 1_002);
        assert_eq!(bound_millis(&at(2_000_001)), 1_003);
        assert_eq!(bound_millis(&at(999_999_999)), 2_000);
    }

    #[test]
    fn an_untargeted_record_crosses_the_wire_with_empty_target_fields() {
        let wire = to_wire(AuditRecord {
            id: "r1".into(),
            namespace: "prod".into(),
            at_ms: 1_500,
            principal_kind: "token".into(),
            token_id: "tok".into(),
            principal: "ci".into(),
            operation: "flexiq.v1.ProducerService/Enqueue".into(),
            target_kind: None,
            target: None,
            outcome: "PERMISSION_DENIED".into(),
            access: "write".into(),
        });
        assert_eq!(wire.principal_kind, "token");
        assert_eq!(wire.target_kind, "");
        assert_eq!(wire.target, "");
        assert_eq!(wire.time, Some(timestamp(1_500)));
    }
}
