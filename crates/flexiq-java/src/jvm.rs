//! Process-wide `JavaVM` handle, cached at `JNI_OnLoad`.

use jni::JavaVM;
use once_cell::sync::OnceCell;

static JAVA_VM: OnceCell<JavaVM> = OnceCell::new();

/// Store the VM. Called once from `JNI_OnLoad`.
pub fn init(vm: JavaVM) {
    let _ = JAVA_VM.set(vm);
}

/// The cached VM (present after `JNI_OnLoad`). Used by the worker bridge to
/// attach worker threads that invoke Java callbacks.
pub fn vm() -> &'static JavaVM {
    JAVA_VM.get().expect("JNI_OnLoad has not run")
}
