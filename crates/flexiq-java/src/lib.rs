//! Java (JNI) bindings for the FlexiQ task-queue core.
//!
//! A thin binding shell — peer to the Python (`flexiq-python`) and Node
//! (`flexiq-node`) shells. All scheduling and storage logic lives in
//! `flexiq-core`; this crate only marshals between the JVM and the core.
//!
//! Each JNI entry point lives in [`queue`] and is named
//! `Java_org_byteveda_flexiq_internal_NativeQueue_<method>` so the JVM links it
//! to the matching `native` method on the `NativeQueue` Java class.

mod backend;
mod convert;
mod dispatcher;
mod error;
mod executor;
mod ffi;
mod ffi_c;
mod handle;
mod jvm;
#[cfg(feature = "mesh")]
mod mesh;
mod queue;
mod worker;
#[cfg(feature = "workflows")]
mod workflows;

use std::ffi::c_void;

use jni::sys::{jint, JNI_VERSION_1_8};
use jni::JavaVM;

/// Cache the `JavaVM` so worker threads can later attach to invoke Java
/// callbacks. The JVM calls this before any native method here.
#[no_mangle]
pub extern "system" fn JNI_OnLoad(vm: JavaVM, _reserved: *mut c_void) -> jint {
    jvm::init(vm);
    JNI_VERSION_1_8
}
