//! Opaque Java `long` <-> heap-boxed Rust value.
//!
//! A handle is a `Box::into_raw` pointer as `jlong`; `close` reclaims it. Java
//! must not use a handle after `close`, nor `close` it concurrently with a call.

use jni::sys::jlong;

/// Box `value` on the heap and return its pointer as an opaque handle.
pub fn into_handle<T>(value: T) -> jlong {
    Box::into_raw(Box::new(value)) as jlong
}

/// Borrow the value behind a handle.
///
/// # Safety
/// `handle` must be a live pointer returned by [`into_handle`] for a `T` and not
/// yet passed to [`drop_handle`].
pub unsafe fn borrow<'a, T>(handle: jlong) -> &'a T {
    &*(handle as *const T)
}

/// Reclaim and drop the value behind a handle.
///
/// # Safety
/// `handle` must be a live pointer returned by [`into_handle`] for a `T`; it is
/// invalid afterward.
pub unsafe fn drop_handle<T>(handle: jlong) {
    drop(Box::from_raw(handle as *mut T));
}
