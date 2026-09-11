//! One module per subcommand group.
//!
//! Each owns the construction of its own request, as a plain function of the
//! parsed flags, so the mapping from what an operator typed to what goes on the
//! wire is testable without a server. Only the thin `run` around it needs one.

pub mod enqueue;
pub mod jobs;
pub mod queues;
