//! The `reason` a `job.cancelled` carries for a job storage archived without
//! running it.
//!
//! Storage writes the same string into the archived row's error, so both come
//! from here: two copies would be free to disagree, and a sink filters on it.

/// A pending job the expiry sweep archived after its TTL passed.
pub const EXPIRED: &str = "expired";

/// A job found expired as the poller went to claim it.
pub const EXPIRED_BEFORE_EXECUTION: &str = "expired before execution";

/// A dependent cancelled because its parent was dead-lettered. The archived
/// row's error also names the parent; the event's reason stays constant.
pub const DEPENDENCY_FAILED: &str = "dependency failed";

/// A dependent cancelled because the job it waited on was cancelled. The
/// archived row's error also names the parent; the event's reason stays
/// constant.
pub const DEPENDENCY_CANCELLED: &str = "dependency cancelled";
