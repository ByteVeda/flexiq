//! Outbound HTTP: the egress guard and the client every dialled URL goes
//! through.
//!
//! This is the machinery any outbound path in the crate shares — today push
//! dispatch, later the settle callback — which is why it lives here rather
//! than inside `worker/`.

mod client;
mod egress;
mod resolver;

pub use client::DispatchClient;
pub use egress::{EgressPolicy, EgressRefusal};
