//! Node.js (napi-rs) bindings for the FlexiQ task-queue core.
//!
//! A thin binding shell — peer to the Python shell in `crates/flexiq-python`.
//! All scheduling and storage logic lives in `flexiq-core`; this crate only
//! marshals between JS values and the core and (later) dispatches task
//! execution back into JavaScript.

mod attached_steps;
mod backend;
mod config;
mod convert;
mod dispatcher;
mod error;
mod executor;
mod queue;
mod steps;
mod worker;

pub use executor::{start_executor, JsExecutor};
pub use queue::JsQueue;
pub use steps::JsStepSession;
pub use worker::JsWorker;

/// Settings-key prefixes the dashboard's generic KV surface must hide. Sourced
/// from the core so every shell hides the same keys.
#[napi_derive::napi]
pub fn reserved_setting_prefixes() -> Vec<String> {
    flexiq_core::RESERVED_SETTING_PREFIXES
        .iter()
        .map(|prefix| (*prefix).to_string())
        .collect()
}
