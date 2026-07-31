//! `taskito-server` — the scheduler, executor attach listener, and dashboard,
//! with no language runtime.
//!
//! Task execution lives in the app's own container: executors dial in over the
//! worker frame protocol and run the task bodies, so this image stays small and
//! identical for every SDK. The binary in `main.rs` is a thin shell over these
//! modules; they are public so integration tests can drive the same wiring the
//! process does.

pub mod config;
pub mod dashboard;
pub mod runtime;
