//! Outbound HTTP: the egress guard and the client every dialled URL goes
//! through.
//!
//! This is the machinery any outbound path in the crate shares — today push
//! dispatch, later the settle callback — which is why it lives here rather
//! than inside `worker/`.

/// Outbound authentication: the signing seam every scheme — bearer today,
/// HMAC/OIDC/SigV4 in later commits — plugs into.
pub mod auth;
mod client;
mod egress;
mod resolver;
#[cfg(test)]
mod testing;

pub use auth::{AuthError, OutboundAuth, Signer, SigningRequest};
pub use client::DispatchClient;
pub use egress::{EgressPolicy, EgressRefusal};
