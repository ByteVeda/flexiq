//! The audit trail's storage side, shared by every surface that records.
//!
//! The gRPC door records token calls (`grpc::audit`); the dashboard records
//! its users' actions and the token command line its mints and revokes
//! (#994). They share what is not specific to how a call arrives: the
//! off-path [`AuditSink`], the retention prune, the counters, and the
//! vocabulary of [`TargetKind`]. None of it needs the `grpc` feature, so a
//! dashboard-only build keeps a trail too.

pub mod metrics;
pub mod record;
pub mod retention;
pub mod sink;
pub mod target;

pub use record::{Actor, PrincipalKind};
pub use sink::AuditSink;
pub use target::TargetKind;
