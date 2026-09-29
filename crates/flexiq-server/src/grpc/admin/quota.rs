//! The namespace quota RPCs (#841): the one document per namespace the core
//! enforces at enqueue, at dispatch and in the retention sweep.
//!
//! The door only reads and replaces the document; it enforces nothing itself,
//! so a quota set here binds every shell serving the namespace alike.

use flexiq_core::{NamespaceQuota, QuotaOverflow};
use tonic::{Response, Status};

use super::Scoped;
use crate::grpc::audit::TargetKind;
use crate::grpc::blocking::on_storage;
use crate::grpc::pb::admin as pb;
use crate::grpc::status::WireError;

/// The namespace's quota; every limit unset when it has none.
pub(crate) async fn get(
    scoped: &Scoped,
) -> Result<Response<pb::GetNamespaceQuotaResponse>, Status> {
    let namespace = scoped.namespace_owned();
    let stored = on_storage(scoped.storage(), move |storage| {
        storage.namespace_quota(Some(&namespace))
    })
    .await?;
    Ok(Response::new(pb::GetNamespaceQuotaResponse {
        quota: Some(to_wire(&stored.unwrap_or_default())),
    }))
}

/// Replace the namespace's quota. A document with every limit unset is
/// cleared rather than stored, so "unlimited" has one spelling.
pub(crate) async fn set(
    scoped: &Scoped,
    request: pb::SetNamespaceQuotaRequest,
) -> Result<Response<pb::SetNamespaceQuotaResponse>, Status> {
    scoped.audit(TargetKind::Namespace, scoped.namespace_owned());
    let quota = from_wire(request.quota.unwrap_or_default())?;
    let namespace = scoped.namespace_owned();
    let stored = quota.clone();
    on_storage(scoped.storage(), move |storage| {
        if stored.is_unlimited() {
            storage.clear_namespace_quota(Some(&namespace)).map(drop)
        } else {
            storage.set_namespace_quota(Some(&namespace), &stored)
        }
    })
    .await?;
    Ok(Response::new(pb::SetNamespaceQuotaResponse {
        quota: Some(to_wire(&quota)),
    }))
}

/// Remove the namespace's quota. Clearing none is not an error: "no quota"
/// is the state asked for.
pub(crate) async fn clear(
    scoped: &Scoped,
) -> Result<Response<pb::ClearNamespaceQuotaResponse>, Status> {
    scoped.audit(TargetKind::Namespace, scoped.namespace_owned());
    let namespace = scoped.namespace_owned();
    on_storage(scoped.storage(), move |storage| {
        storage.clear_namespace_quota(Some(&namespace)).map(drop)
    })
    .await?;
    Ok(Response::new(pb::ClearNamespaceQuotaResponse {}))
}

/// The wire message as the core's document, refusing anything the core would
/// refuse to enforce — a negative cap, a rate it cannot parse — as the
/// caller's mistake rather than a storage fault.
fn from_wire(quota: pb::NamespaceQuota) -> Result<NamespaceQuota, WireError> {
    let on_excess = match pb::QuotaOverflow::try_from(quota.on_excess) {
        Ok(pb::QuotaOverflow::Unspecified | pb::QuotaOverflow::Reject) => QuotaOverflow::Reject,
        Ok(pb::QuotaOverflow::Drop) => QuotaOverflow::Drop,
        Err(_) => {
            return Err(WireError::invalid_request(format!(
                "on_excess {} is not a QuotaOverflow",
                quota.on_excess
            )))
        }
    };
    let quota = NamespaceQuota {
        max_pending: quota.max_pending,
        on_excess,
        enqueue_rate: quota.enqueue_rate,
        max_running: quota.max_running,
        max_archived_rows: quota.max_archived_rows,
        max_dead_rows: quota.max_dead_rows,
    };
    quota
        .validate()
        .map_err(|e| WireError::invalid_request(e.to_string()))?;
    Ok(quota)
}

fn to_wire(quota: &NamespaceQuota) -> pb::NamespaceQuota {
    let on_excess = match quota.on_excess {
        QuotaOverflow::Reject => pb::QuotaOverflow::Reject,
        QuotaOverflow::Drop => pb::QuotaOverflow::Drop,
    };
    pb::NamespaceQuota {
        max_pending: quota.max_pending,
        on_excess: on_excess.into(),
        enqueue_rate: quota.enqueue_rate.clone(),
        max_running: quota.max_running,
        max_archived_rows: quota.max_archived_rows,
        max_dead_rows: quota.max_dead_rows,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_quota_round_trips_the_wire() {
        let quota = NamespaceQuota {
            max_pending: Some(10),
            on_excess: QuotaOverflow::Drop,
            enqueue_rate: Some("5/s".into()),
            max_running: Some(0),
            max_archived_rows: None,
            max_dead_rows: Some(3),
        };
        assert_eq!(from_wire(to_wire(&quota)).unwrap(), quota);
    }

    #[test]
    fn an_unspecified_overflow_reads_as_reject() {
        let quota = from_wire(pb::NamespaceQuota::default()).unwrap();
        assert_eq!(quota.on_excess, QuotaOverflow::Reject);
        assert!(quota.is_unlimited());
    }

    #[test]
    fn what_the_core_would_refuse_is_invalid_argument() {
        for quota in [
            pb::NamespaceQuota {
                max_pending: Some(-1),
                ..Default::default()
            },
            pb::NamespaceQuota {
                enqueue_rate: Some("fast".into()),
                ..Default::default()
            },
            pb::NamespaceQuota {
                on_excess: 9,
                ..Default::default()
            },
        ] {
            let status = Status::from(from_wire(quota).unwrap_err());
            assert_eq!(status.code(), tonic::Code::InvalidArgument);
        }
    }
}
