use pyo3::prelude::*;
use tokio::sync::OwnedSemaphorePermit;

/// An async-dispatch slot, held by the Python coroutine that is using it.
///
/// The pool acquires a permit before handing a job to the Python executor, so the
/// number of async jobs in flight stays bounded. The permit has to outlive the
/// `submit_job` call — dispatch returns as soon as the coroutine is scheduled, long
/// before it runs — so ownership passes to the coroutine frame itself. CPython then
/// reclaims it on *every* exit path, including the ones that report nothing: an
/// escaping `CancelledError`, a loop stopped mid-flight, or a future dropped before
/// the coroutine ever ran. Releasing from the result-report path instead would leak a
/// slot permanently on each of those.
#[pyclass]
pub struct PyJobPermit(Option<OwnedSemaphorePermit>);

impl PyJobPermit {
    pub fn new(permit: OwnedSemaphorePermit) -> Self {
        Self(Some(permit))
    }
}

#[pymethods]
impl PyJobPermit {
    /// Give the slot back now rather than waiting for the frame to be collected.
    /// Idempotent; dropping the handle does the same thing.
    fn release(&mut self) {
        self.0.take();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::Semaphore;

    #[tokio::test]
    async fn drop_without_release_returns_the_permit() {
        let sem = Arc::new(Semaphore::new(1));
        let handle = PyJobPermit::new(sem.clone().acquire_owned().await.unwrap());
        assert_eq!(sem.available_permits(), 0);

        drop(handle);
        assert_eq!(sem.available_permits(), 1);
    }

    #[tokio::test]
    async fn release_is_idempotent() {
        let sem = Arc::new(Semaphore::new(1));
        let mut handle = PyJobPermit::new(sem.clone().acquire_owned().await.unwrap());

        handle.release();
        assert_eq!(sem.available_permits(), 1);

        handle.release();
        assert_eq!(sem.available_permits(), 1);
    }
}
