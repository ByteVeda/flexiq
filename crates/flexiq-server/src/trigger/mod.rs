//! Triggers: inbound HTTP requests that become enqueues (#847).
//!
//! A trigger is a configuration object, not code. It names one task, one
//! queue and — through the process — one namespace, says how a request proves
//! who sent it, and maps the request onto the task's arguments. Nothing a
//! request carries can change where the job goes, only what it holds.
//!
//! The listener is the one door this process expects to face the public
//! internet, which is why it is a role of its own rather than a route on the
//! dashboard or the gRPC door: an ingress can expose it without exposing them.

pub mod auth;
pub mod definition;
pub mod document;
pub mod enqueue;
pub mod handler;
pub mod mapping;
pub mod metrics;
pub mod object_store;
pub mod rate;
pub mod server;

pub use server::{router, router_with_keys, serve};
