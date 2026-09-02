//! Scoped API tokens: the credential the gRPC door accepts.
//!
//! A token is a named row with a set of [`Scope`]s, a namespace and an expiry,
//! stored hashed and shown to its operator exactly once. It replaces the shared
//! secret #716 shipped, which was one string every client presented: it could
//! not be revoked for one of them, carried no scope, and left no record of who
//! called.
//!
//! **The store is the settings KV, not a table of its own.** Dashboard users,
//! sessions and webhook subscriptions already persist through
//! `Storage::set_setting`, and that is what makes them readable by a SQLite,
//! Postgres *and* Redis deployment without three implementations of one row.
//! `RESERVED_SETTING_PREFIXES` already carries `auth:`, so these rows are hidden
//! from the generic settings API without a core change — a forged token cannot
//! be written through the dashboard's key/value surface.
//!
//! This module is compiled unconditionally. Minting, listing and revoking are
//! things a build without the `grpc` feature still does, because the operator
//! provisioning a credential and the door checking it need not be the same
//! process.

pub mod cli;
pub mod model;
pub mod scope;
pub mod secret;
pub mod store;

pub use model::{ApiToken, NewToken, TokenStatus};
pub use scope::{Scope, ScopeSet};
pub use secret::{MintedToken, PresentedToken};
