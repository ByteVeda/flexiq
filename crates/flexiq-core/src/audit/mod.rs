//! The audit trail's record shape and the dashboard recording rule, shared
//! by `flexiq-server` and every SDK shell (#840, #994, #1020).
//!
//! Storage is `Storage::{append_audit, list_audit_after, purge_audit}`; this
//! module decides what goes in a record, so a row reads the same whichever
//! surface wrote it.

pub mod dashboard;
pub mod record;
pub mod recorder;
pub mod target;

pub use record::{outcome_of, records, Access, Actor, PrincipalKind};
pub use recorder::{AuditRecorder, DashboardAudit};
pub use target::TargetKind;
