//! One encoded call to one task, before it is enqueued.

use std::marker::PhantomData;

use crate::options::enqueue_setters;
use crate::{EnqueueOptions, Task};

/// A task's encoded arguments, with the options to enqueue them.
///
/// Transport-free on purpose. This is the value both the embedded handle and a
/// future remote one accept: a task name, a payload, and a set of options. The
/// handle is where the two doors differ; the call is not.
pub struct TaskCall<T: Task> {
    pub(crate) payload: Vec<u8>,
    pub(crate) options: EnqueueOptions,
    pub(crate) _task: PhantomData<fn() -> T>,
}

impl<T: Task> TaskCall<T> {
    /// Build a call from an already-encoded payload, seeded with the task's
    /// declared defaults.
    ///
    /// `pub`, not `pub(crate)`: `#[flexiq::task]` expands in the *caller's*
    /// crate, so everything the expansion touches has to be reachable from
    /// outside this one. Hidden from the docs because a hand-written [`Task`]
    /// impl is the only other caller.
    #[doc(hidden)]
    pub fn from_args(payload: Vec<u8>) -> Self {
        Self {
            payload,
            options: T::defaults(),
            _task: PhantomData,
        }
    }

    /// The task this call runs.
    pub fn task_name(&self) -> &'static str {
        T::NAME
    }

    enqueue_setters!(.options);
}
