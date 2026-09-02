//! Who may call this door, and what they may call.
//!
//! Three pieces, in the order a request meets them:
//!
//! 1. [`gate`] classifies the path — public, authenticated, or authenticated
//!    with a scope. One table, one prefix per proto package, so a new RPC
//!    inherits its package's answer.
//! 2. an [`Authenticator`] turns request metadata into a [`Principal`].
//!    [`TokenStore`] is the one implementation there is: a presented credential
//!    is looked up in the stored tokens, and there is no other way in.
//! 3. [`AuthLayer`] wraps the whole router, applies 1 and 2, and puts the
//!    `Principal` in the request's extensions for the handlers.
//!
//! **There is no unauthenticated path and no fallback credential.** #716 had
//! two — a shared secret in an environment variable, and an anonymous principal
//! for a loopback bind with no secret configured. Both are gone. A shared secret
//! cannot be revoked for one client and carries no scope, so leaving it in place
//! would have made "a revoked token stops working" true only for clients that
//! did not have it; and an anonymous principal is a credential the network stack
//! issues, which is not a thing this door can reason about. A listener with no
//! token provisioned refuses every call, including on loopback, because a door
//! with no credential configured is a misconfiguration rather than a permission
//! grant.

pub mod authenticator;
pub mod bearer;
pub mod gate;
pub mod layer;
pub mod principal;
pub mod token_store;

pub use authenticator::Authenticator;
pub use layer::AuthLayer;
pub use principal::{Principal, Scope, ScopeSet};
pub use token_store::TokenStore;
