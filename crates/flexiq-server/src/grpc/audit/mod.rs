//! Which token did what (#840), and optionally what it saw (#993).
//!
//! One record per authorised write this listener answers — token id (never
//! the secret, never its digest), the token's name, namespace, the RPC, what
//! it touched and how it ended — appended to the `audit_log` table. Reads are
//! recorded the same way when `FLEXIQ_GRPC_AUDIT_READS` is on, folded by
//! [`dedup`] so polling does not bury the writes.
//!
//! Four pieces, in the order a call meets them:
//!
//! 1. [`AuditLayer`] opens an [`AuditContext`] for a call that needs a write
//!    scope — or a read scope, when reads are on — and closes it into records
//!    once the call is answered.
//! 2. The auth layer names the caller in it the moment the credential is
//!    believed, so a scope refusal is still attributed.
//! 3. A handler names what it touched with [`target`].
//! 4. [`AuditSink`] appends the records off the request path, degrading to
//!    the log when it cannot.
//!
//! The sink, retention and counters are not this door's alone — the dashboard
//! records into the same ones — so they live in [`crate::audit`].
//!
//! Why a table rather than a log stream, why writes only, and why the window
//! is the server's are recorded in `tasks/plans/2026-09-29-audit-trail-840.md`.

pub mod context;
pub mod dedup;
pub mod layer;

pub use crate::audit::{AuditSink, TargetKind};
pub use context::{filter, target, AuditContext};
pub use layer::AuditLayer;
