//! The scaler's in-band credential check.
//!
//! KEDA dials an external scaler with no `authorization` header and no way to
//! add one, so the gate exempts this service (`Requirement::InService`) and the
//! token rides in `scalerMetadata` instead. It is checked here by the same
//! [`Authenticator`] and the same `admit` the auth layer uses, so hashing,
//! expiry, revocation, namespace binding and scope mean exactly what they mean
//! on every other door.

use std::collections::HashMap;

use tonic::metadata::{MetadataMap, MetadataValue};
use tonic::{Request, Status};

use super::metadata;
use crate::grpc::audit::AuditContext;
use crate::grpc::auth::bearer::AUTHORIZATION;
use crate::grpc::auth::layer::admit;
use crate::grpc::auth::{Authenticator, Principal, Scope};
use crate::grpc::status::WireError;

/// Identify the caller of `request` and hold it to `inspect`.
///
/// An `authorization` header wins when present, for a caller such as `grpcurl`
/// that can send one; otherwise the token in `scaler_metadata` is presented as
/// if it had arrived as that header.
pub async fn caller<T>(
    authenticator: &dyn Authenticator,
    request: &Request<T>,
    scaler_metadata: &HashMap<String, String>,
) -> Result<Principal, Status> {
    let principal = if request.metadata().contains_key(AUTHORIZATION) {
        authenticator.authenticate(request.metadata()).await?
    } else {
        authenticator
            .authenticate(&as_header(metadata::token(scaler_metadata))?)
            .await?
    };
    // Before the scope check, as the layer does, so a refusal is attributed.
    if let Some(audit) = AuditContext::of(request.extensions()) {
        audit.identify(&principal);
    }
    admit(principal, Scope::Inspect)
}

/// `token` spelled as the metadata a bearer credential arrives in. No token is
/// an empty map, which the authenticator refuses like any wrong one.
fn as_header(token: Option<&str>) -> Result<MetadataMap, Status> {
    let mut map = MetadataMap::new();
    if let Some(token) = token {
        // A value that cannot be a header cannot be a token either.
        let value = MetadataValue::try_from(format!("Bearer {token}"))
            .map_err(|_| Status::from(WireError::unauthenticated()))?;
        map.insert(AUTHORIZATION, value);
    }
    Ok(map)
}
