//! Translate core errors into Java exceptions thrown across the JNI boundary.

use flexiq_core::QueueError;
use jni::JNIEnv;

/// Fully-qualified JNI name (slashes, not dots) of the SDK's exception class.
pub const FLEXIQ_EXCEPTION: &str = "org/byteveda/flexiq/FlexiQException";

/// A deferred Java exception. The binding builds one of these on failure and
/// throws it at the FFI boundary instead of unwinding a Rust panic across it.
#[derive(Debug)]
pub struct BindingError {
    class: &'static str,
    message: String,
}

impl BindingError {
    /// A `FlexiQException` carrying `message`.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            class: FLEXIQ_EXCEPTION,
            message: message.into(),
        }
    }

    /// An exception of a specific class, for a failure whose *class* is the
    /// contract.
    ///
    /// Durable steps are the case this exists for: the core's retry verdict has
    /// to survive the crossing, and JNI can throw any class, so the class names
    /// both the failure and whether the attempt may be retried. Nothing has to
    /// be encoded into the message to carry it (see [`crate::steps`]).
    pub fn with_class(class: &'static str, message: impl Into<String>) -> Self {
        Self {
            class,
            message: message.into(),
        }
    }

    /// Throw this as a pending Java exception. A failed throw means the JVM is
    /// already in a bad state, so there is nothing further to do.
    pub fn throw(&self, env: &mut JNIEnv) {
        let _ = env.throw_new(self.class, &self.message);
    }
}

/// The message alone: the C-ABI surface (`ffi_c`) reports errors as strings,
/// with no exception class to carry.
impl std::fmt::Display for BindingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl From<QueueError> for BindingError {
    fn from(err: QueueError) -> Self {
        Self::new(err.to_string())
    }
}

/// Workflow errors (e.g. DAG validation) reach the boundary as `FlexiQException`.
#[cfg(feature = "workflows")]
impl From<flexiq_workflows::WorkflowError> for BindingError {
    fn from(err: flexiq_workflows::WorkflowError) -> Self {
        Self::new(err.to_string())
    }
}
