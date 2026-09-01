//! Who may call this door, and what they may call.
//!
//! Three pieces, in the order a request meets them:
//!
//! 1. [`gate`] classifies the path — public, authenticated, or authenticated
//!    with a scope. One table, one prefix per proto package, so a new RPC
//!    inherits its package's answer.
//! 2. an [`Authenticator`] turns request metadata into a [`Principal`].
//!    [`SharedSecret`] is the one #716 ships; [`Anonymous`] covers the loopback
//!    door with no credential configured, and #717's token store lands as a
//!    third.
//! 3. [`AuthLayer`] wraps the whole router, applies 1 and 2, and puts the
//!    `Principal` in the request's extensions for the handlers.
//!
//! **This is a shared secret, and a shared secret is not the answer.** It cannot
//! be revoked for one client, it carries no scope, and it leaves no audit
//! trail. It is enough to close the gap between a producer service that exists
//! and a token store that does not, and the [`Principal`] boundary is what
//! makes that store additive: it carries a namespace and a set of scopes from
//! this commit even though the secret behind it grants both scopes and the one
//! namespace the process serves.

pub mod authenticator;
pub mod gate;
pub mod layer;
pub mod principal;
pub mod shared_secret;

use std::sync::Arc;

use flexiq_core::Secret;

pub use authenticator::{Anonymous, Authenticator};
pub use layer::AuthLayer;
pub use principal::{Principal, Scope, ScopeSet};
pub use shared_secret::SharedSecret;

/// The authenticator a configured door uses.
///
/// One function so the choice is made once. `None` is only reachable for a
/// loopback or Unix-socket bind — `config::grpc` refuses any other without a
/// token — so the anonymous case is a boundary that already exists, not one
/// this grants.
pub fn authenticator(token: Option<&Secret>, namespace: &str) -> Arc<dyn Authenticator> {
    match token {
        Some(token) => Arc::new(SharedSecret::new(token.clone(), namespace)),
        None => Arc::new(Anonymous::new(namespace)),
    }
}
