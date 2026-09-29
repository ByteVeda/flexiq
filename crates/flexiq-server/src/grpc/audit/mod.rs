//! Which token did what (#840).
//!
//! One record per authorised write this listener answers — token id (never
//! the secret, never its digest), the token's name, namespace, the RPC, what
//! it touched and how it ended — appended to the `audit_log` table.
//!
//! Four pieces, in the order a call meets them:
//!
//! 1. [`AuditLayer`] opens an [`AuditContext`] for a call that needs a write
//!    scope, and closes it into records once the call is answered.
//! 2. The auth layer names the caller in it the moment the credential is
//!    believed, so a scope refusal is still attributed.
//! 3. A handler names what it touched with [`target`].
//! 4. [`AuditSink`] appends the records off the request path, degrading to
//!    the log when it cannot.
//!
//! Why a table rather than a log stream, why writes only, and why the window
//! is the server's are recorded in `tasks/plans/2026-09-29-audit-trail-840.md`.

pub mod context;
pub mod layer;
pub mod metrics;
pub mod retention;
pub mod sink;

pub use context::{target, AuditContext, TargetKind};
pub use layer::AuditLayer;
pub use sink::AuditSink;
