//! One encoded call to one task, before it is enqueued.

use std::marker::PhantomData;

use crate::options::enqueue_setters;
use crate::{EncodeError, EnqueueOptions, Task};

/// A task's encoded arguments, with the options to enqueue them.
///
/// Transport-free on purpose. This is the value both the embedded handle and a
/// future remote one accept: a task name, a payload, and a set of options. The
/// handle is where the two doors differ; the call is not.
pub struct TaskCall<T: Task> {
    /// The encoded arguments, or why they could not be encoded.
    ///
    /// Carried rather than raised because `call(..)` mirrors the task's own
    /// signature and has nowhere to put a `Result` without costing every caller
    /// a second `?` — including inside `vec![..]`, where a batch is built. The
    /// failure surfaces from [`crate::FlexiQ::enqueue`], which already returns
    /// one, naming the task.
    pub(crate) payload: Result<Vec<u8>, EncodeError>,
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
        Self::from_encoded(Ok(payload))
    }

    /// Build a call from an encoding that may have failed.
    ///
    /// What the macro emits: a value whose type satisfies `Serialize` can still
    /// fail to encode — a `u64` past `i64::MAX` has no representation in the
    /// envelope — and a producer should see that as an error rather than a
    /// panic.
    #[doc(hidden)]
    pub fn from_encoded(payload: Result<Vec<u8>, EncodeError>) -> Self {
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
