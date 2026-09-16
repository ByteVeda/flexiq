//! Outbound HTTP: the egress guard and the client every dialled URL goes
//! through.
//!
//! This is the machinery any outbound path in the crate shares — today push
//! dispatch, later the settle callback — which is why it lives here rather
//! than inside `worker/`.

/// Outbound authentication: the signing seam bearer, HMAC, OIDC and SigV4
/// each plug into.
pub mod auth;
mod client;
mod egress;
mod resolver;
#[cfg(test)]
mod testing;

pub use auth::{AuthError, OutboundAuth, Signer, SigningRequest};
pub use client::DispatchClient;
/// Crate-internal: the response reader is a detail of how this crate dials
/// out, not part of the surface an embedder configures.
pub(crate) use client::{read_bounded, BodyRead};
pub use egress::{EgressPolicy, EgressRefusal};
