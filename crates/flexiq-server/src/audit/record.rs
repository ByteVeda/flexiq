//! One action, as the records it leaves — whichever surface took it.
//!
//! The record shape lives in the core (#1020), so the server's records and an
//! SDK dashboard's read the same; this adds only the axum status type.

use axum::http::StatusCode;

pub use flexiq_core::audit::record::{records, Access, Actor, PrincipalKind};

/// The `google.rpc.Code` name an HTTP answer stands for, so a dashboard
/// record's outcome reads like a gRPC one and one filter covers both.
pub fn outcome_of(status: StatusCode) -> &'static str {
    flexiq_core::audit::outcome_of(status.as_u16())
}
