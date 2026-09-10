//! Durable steps for a running task.
//!
//! A stub until the dispatcher can hand a session over: opening one needs the
//! `(owner, attempt, epoch)` fence, and the fence only exists inside a pool
//! that kept what the scheduler handed it.

/// An open step session.
pub(crate) struct Session;
