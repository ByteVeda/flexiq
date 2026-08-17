//! JNI marshalling helpers and the panic-safety boundary.

use std::panic::{catch_unwind, AssertUnwindSafe};

use jni::objects::{JByteArray, JObject, JObjectArray, JString};
use jni::sys::{jbyteArray, jobjectArray, jstring};
use jni::JNIEnv;

use crate::error::BindingError;

/// Run `body`, converting both `BindingError`s and Rust panics into thrown Java
/// exceptions and returning `fallback` in either case.
///
/// A Rust panic must never unwind across the FFI boundary (undefined behaviour),
/// so every JNI entry point routes its work through this guard.
pub fn guard<'local, T>(
    env: &mut JNIEnv<'local>,
    fallback: T,
    body: impl FnOnce(&mut JNIEnv<'local>) -> Result<T, BindingError>,
) -> T {
    match catch_unwind(AssertUnwindSafe(|| body(env))) {
        Ok(Ok(value)) => value,
        Ok(Err(err)) => {
            err.throw(env);
            fallback
        }
        Err(_) => {
            BindingError::new("flexiq native panic").throw(env);
            fallback
        }
    }
}

/// Read a required Java `String` argument into an owned Rust `String`.
pub fn read_string(env: &mut JNIEnv, value: &JString) -> Result<String, BindingError> {
    let java_str = env
        .get_string(value)
        .map_err(|e| BindingError::new(format!("invalid string argument: {e}")))?;
    Ok(java_str.into())
}

/// Read an optional Java `String` argument; `null` becomes `None`.
pub fn read_optional_string(
    env: &mut JNIEnv,
    value: &JString,
) -> Result<Option<String>, BindingError> {
    if value.is_null() {
        Ok(None)
    } else {
        read_string(env, value).map(Some)
    }
}

/// Read a Java `byte[]` argument into an owned `Vec<u8>`.
pub fn read_bytes(env: &mut JNIEnv, value: &JByteArray) -> Result<Vec<u8>, BindingError> {
    env.convert_byte_array(value)
        .map_err(|e| BindingError::new(format!("invalid byte[] argument: {e}")))
}

/// Read a Java `byte[][]` argument into owned bytes per element.
pub fn read_bytes_array(
    env: &mut JNIEnv,
    value: &JObjectArray,
) -> Result<Vec<Vec<u8>>, BindingError> {
    let len = env
        .get_array_length(value)
        .map_err(|e| BindingError::new(format!("invalid byte[][] argument: {e}")))?;
    let mut out = Vec::with_capacity(len as usize);
    for index in 0..len {
        let element = env
            .get_object_array_element(value, index)
            .map_err(|e| BindingError::new(format!("invalid byte[][] element: {e}")))?;
        // Auto-release the per-element local ref each iteration so a large array
        // can't exhaust the JNI local-reference table before we return.
        let element = env.auto_local(JByteArray::from(element));
        out.push(read_bytes(env, &element)?);
    }
    Ok(out)
}

/// Read a Java `String[]` argument into owned strings; a null array is empty.
#[cfg_attr(not(feature = "workflows"), allow(dead_code))]
pub fn read_string_array(
    env: &mut JNIEnv,
    value: &JObjectArray,
) -> Result<Vec<String>, BindingError> {
    if value.is_null() {
        return Ok(Vec::new());
    }
    let len = env
        .get_array_length(value)
        .map_err(|e| BindingError::new(format!("invalid String[] argument: {e}")))?;
    let mut out = Vec::with_capacity(len as usize);
    for index in 0..len {
        let element = env
            .get_object_array_element(value, index)
            .map_err(|e| BindingError::new(format!("invalid String[] element: {e}")))?;
        out.push(read_string(env, &JString::from(element))?);
    }
    Ok(out)
}

/// Build a Java `String` return value from an owned Rust `String`.
pub fn new_string(env: &mut JNIEnv, value: String) -> Result<jstring, BindingError> {
    env.new_string(value)
        .map(|s| s.into_raw())
        .map_err(|e| BindingError::new(format!("failed to allocate Java string: {e}")))
}

/// Build a Java `byte[]` return value from a slice.
pub fn new_bytes(env: &mut JNIEnv, value: &[u8]) -> Result<jbyteArray, BindingError> {
    env.byte_array_from_slice(value)
        .map(|a| a.into_raw())
        .map_err(|e| BindingError::new(format!("failed to allocate byte[]: {e}")))
}

/// Build a Java `String[]` return value from owned strings.
pub fn new_string_array(env: &mut JNIEnv, values: &[String]) -> Result<jobjectArray, BindingError> {
    let class = env
        .find_class("java/lang/String")
        .map_err(|e| BindingError::new(format!("String class lookup failed: {e}")))?;
    let array = env
        .new_object_array(values.len() as i32, &class, JObject::null())
        .map_err(|e| BindingError::new(format!("failed to allocate String[]: {e}")))?;
    for (index, value) in values.iter().enumerate() {
        // Auto-release each element's local ref after storing it in the array, so
        // a large String[] can't exhaust the JNI local-reference table.
        let element = env.auto_local(
            env.new_string(value)
                .map_err(|e| BindingError::new(format!("failed to allocate Java string: {e}")))?,
        );
        env.set_object_array_element(&array, index as i32, &element)
            .map_err(|e| BindingError::new(format!("failed to set String[] element: {e}")))?;
    }
    Ok(array.into_raw())
}
