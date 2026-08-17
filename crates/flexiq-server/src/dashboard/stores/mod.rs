//! Feature stores the dashboard owns, all persisted in the settings table.
//!
//! Keeping them in settings rather than in tables of their own is what makes
//! the same rows readable across SQLite, Postgres, and Redis, and by every SDK
//! dashboard pointed at the same backend. The key layouts are a cross-SDK
//! contract, documented per module.

pub mod deliveries;
pub mod kv;
pub mod middleware;
pub mod overrides;
pub mod url_safety;
pub mod webhooks;
