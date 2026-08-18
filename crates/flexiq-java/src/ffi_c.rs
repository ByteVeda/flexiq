//! C-ABI fast path for the hot byte ops, parallel to the jni-rs surface.
//!
//! These `extern "C"` exports let the Java SDK call `enqueue`, `enqueueMany`, and
//! `getResult` over Project Panama (FFM) on JDK 22+, avoiding the JNI string and
//! `byte[]` marshalling. They are perf-only: the JNI entry points in [`crate::queue`]
//! remain the control plane and the fallback on JDKs without FFM. Both surfaces
//! share one cdylib and operate on the same opaque `QueueHandle` pointer.
//!
//! ## Contract
//! Each function returns an `i32` status — [`OK`], [`ERR`], or (results only)
//! [`ABSENT`] — and, when it produces bytes, writes a freshly heap-allocated
//! buffer's pointer and length through the `out_data`/`out_len` out-params. The
//! caller copies the bytes and must release the buffer with [`flexiq_ffi_free`].
//! On [`ERR`] the buffer holds a UTF-8 error message. Inputs are borrowed for the
//! duration of the call and never freed here. Every body is panic-guarded so a
//! Rust panic can never unwind across the FFI boundary.

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::slice;

use flexiq_core::Storage;

use crate::backend::QueueHandle;
use crate::convert::{build_new_job, debounce_options, EnqueueOptions};

/// The call succeeded; `out` holds the result bytes (job id, ids frame, or result).
pub const OK: i32 = 0;
/// The call failed; `out` holds a UTF-8 error message.
pub const ERR: i32 = 1;
/// `getResult` only: the job has no result yet; `out` is null/empty.
pub const ABSENT: i32 = 2;

/// Borrow `len` bytes at `ptr` as an owned `Vec`; a null or empty input is empty.
///
/// # Safety
/// `ptr` must be valid for `len` bytes for the duration of the call, or null.
unsafe fn read_bytes(ptr: *const u8, len: usize) -> Vec<u8> {
    if ptr.is_null() || len == 0 {
        Vec::new()
    } else {
        slice::from_raw_parts(ptr, len).to_vec()
    }
}

/// Borrow `len` bytes at `ptr` as a UTF-8 `String`.
///
/// # Safety
/// See [`read_bytes`].
unsafe fn read_string(ptr: *const u8, len: usize) -> Result<String, String> {
    String::from_utf8(read_bytes(ptr, len)).map_err(|e| format!("invalid UTF-8 argument: {e}"))
}

/// Hand `bytes` to the caller: write its pointer and length to the out-params and
/// leak the allocation, to be reclaimed by [`flexiq_ffi_free`].
///
/// # Safety
/// `out_data` and `out_len` must be valid, writable pointers.
unsafe fn emit(bytes: Vec<u8>, out_data: *mut *mut u8, out_len: *mut usize) {
    let mut boxed = bytes.into_boxed_slice();
    *out_len = boxed.len();
    *out_data = boxed.as_mut_ptr();
    std::mem::forget(boxed);
}

/// Resolve a producer result into a status code, emitting bytes or the error text.
///
/// # Safety
/// `out_data`/`out_len` must be valid, writable pointers.
unsafe fn finish(
    result: std::thread::Result<Result<Vec<u8>, String>>,
    out_data: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    match result {
        Ok(Ok(bytes)) => {
            emit(bytes, out_data, out_len);
            OK
        }
        Ok(Err(message)) => {
            emit(message.into_bytes(), out_data, out_len);
            ERR
        }
        Err(_) => {
            emit(b"flexiq native panic".to_vec(), out_data, out_len);
            ERR
        }
    }
}

/// Length-prefixed frame: `[count u32][len u32][bytes]...`, all little-endian.
/// Matches the Java side's framing so batch payloads and ids cross as one buffer.
fn frame(items: &[Vec<u8>]) -> Vec<u8> {
    let total = 4 + items.iter().map(|item| 4 + item.len()).sum::<usize>();
    let mut out = Vec::with_capacity(total);
    out.extend_from_slice(&(items.len() as u32).to_le_bytes());
    for item in items {
        out.extend_from_slice(&(item.len() as u32).to_le_bytes());
        out.extend_from_slice(item);
    }
    out
}

/// Decode a [`frame`]d buffer back into its elements.
fn unframe(buf: &[u8]) -> Result<Vec<Vec<u8>>, String> {
    let mut pos = 0usize;
    let take_u32 = |buf: &[u8], pos: &mut usize| -> Result<usize, String> {
        let end = pos.checked_add(4).ok_or("frame length overflow")?;
        let slice = buf.get(*pos..end).ok_or("truncated frame header")?;
        *pos = end;
        Ok(u32::from_le_bytes(slice.try_into().unwrap()) as usize)
    };
    let count = take_u32(buf, &mut pos)?;
    let mut items = Vec::with_capacity(count);
    for _ in 0..count {
        let len = take_u32(buf, &mut pos)?;
        let end = pos.checked_add(len).ok_or("frame element overflow")?;
        let element = buf.get(pos..end).ok_or("truncated frame element")?;
        items.push(element.to_vec());
        pos = end;
    }
    Ok(items)
}

/// `enqueue` — mirrors `NativeQueue.enqueue`. On success `out` is the job id.
///
/// # Safety
/// `handle` must be a live `QueueHandle` pointer; the input pointers must be valid
/// for their lengths; `out_data`/`out_len` must be writable.
#[no_mangle]
pub unsafe extern "C" fn flexiq_ffi_enqueue(
    handle: i64,
    task_ptr: *const u8,
    task_len: usize,
    payload_ptr: *const u8,
    payload_len: usize,
    options_ptr: *const u8,
    options_len: usize,
    out_data: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    let work = AssertUnwindSafe(|| {
        let queue = handle_ref(handle);
        let task = read_string(task_ptr, task_len)?;
        let payload = read_bytes(payload_ptr, payload_len);
        let raw_options = read_string(options_ptr, options_len)?;
        let options: EnqueueOptions = parse_options(&raw_options, "enqueue options")?;
        let unique = options.unique_key.is_some();
        let debounce = debounce_options(&options).map_err(|e| e.to_string())?;
        // `enqueue_debounced` collapses onto the open window; it does no unique
        // dedup, so there is no honest way to satisfy both requests at once.
        if unique && debounce.is_some() {
            return Err(
                "uniqueKey and debounce cannot be combined — they are two different collapse rules"
                    .to_string(),
            );
        }
        let new_job = build_new_job(task, payload, options, queue.namespace.as_deref());
        let job = match debounce {
            Some(debounce) => queue.storage.enqueue_debounced(new_job, debounce),
            None if unique => queue.storage.enqueue_unique(new_job),
            None => queue.storage.enqueue(new_job),
        }
        .map_err(|e| e.to_string())?;
        Ok::<Vec<u8>, String>(job.id.into_bytes())
    });
    finish(catch_unwind(work), out_data, out_len)
}

/// `enqueueMany` — mirrors `NativeQueue.enqueueMany`. `payloads` is a [`frame`]d
/// buffer; `options` is a JSON array. On success `out` is a frame of job ids.
///
/// # Safety
/// See [`flexiq_ffi_enqueue`].
#[no_mangle]
pub unsafe extern "C" fn flexiq_ffi_enqueue_many(
    handle: i64,
    task_ptr: *const u8,
    task_len: usize,
    payloads_ptr: *const u8,
    payloads_len: usize,
    options_ptr: *const u8,
    options_len: usize,
    out_data: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    let work = AssertUnwindSafe(|| {
        let queue = handle_ref(handle);
        let task = read_string(task_ptr, task_len)?;
        let payloads = unframe(&read_bytes(payloads_ptr, payloads_len))?;
        let raw_options = read_string(options_ptr, options_len)?;
        let option_list: Vec<EnqueueOptions> =
            parse_options(&raw_options, "enqueue batch options")?;
        if option_list.len() != payloads.len() {
            return Err("payloads and options must have the same length".to_string());
        }
        let namespace = queue.namespace.as_deref();
        let new_jobs = payloads
            .into_iter()
            .zip(option_list)
            .map(|(payload, options)| {
                // The batch path is one `enqueue_batch_dedup` call with no debounce
                // step, so an entry's key would be written but never honoured — and a
                // plain pending row carrying a debounce key is a slide target for the
                // next debounced enqueue.
                if debounce_options(&options)
                    .map_err(|e| e.to_string())?
                    .is_some()
                {
                    return Err("batch enqueue cannot debounce".to_string());
                }
                Ok(build_new_job(task.clone(), payload, options, namespace))
            })
            .collect::<Result<Vec<_>, String>>()?;
        let ids = crate::backend::enqueue_batch_dedup(&queue.storage, new_jobs)
            .map_err(|e| e.to_string())?;
        let ids: Vec<Vec<u8>> = ids.into_iter().map(String::into_bytes).collect();
        Ok::<Vec<u8>, String>(frame(&ids))
    });
    finish(catch_unwind(work), out_data, out_len)
}

/// `getResult` — mirrors `NativeQueue.getResult`. Returns [`ABSENT`] (with a null
/// `out`) when the job has no result, [`OK`] with the result bytes, or [`ERR`].
///
/// # Safety
/// See [`flexiq_ffi_enqueue`].
#[no_mangle]
pub unsafe extern "C" fn flexiq_ffi_get_result(
    handle: i64,
    job_ptr: *const u8,
    job_len: usize,
    out_data: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    let work = AssertUnwindSafe(|| {
        let queue = handle_ref(handle);
        let id = read_string(job_ptr, job_len)?;
        let result = queue
            .storage
            .get_job(&id, queue.namespace.as_deref())
            .map_err(|e| e.to_string())?
            .and_then(|job| job.result);
        Ok::<Option<Vec<u8>>, String>(result)
    });
    match catch_unwind(work) {
        Ok(Ok(Some(bytes))) => {
            emit(bytes, out_data, out_len);
            OK
        }
        Ok(Ok(None)) => {
            *out_data = std::ptr::null_mut();
            *out_len = 0;
            ABSENT
        }
        Ok(Err(message)) => {
            emit(message.into_bytes(), out_data, out_len);
            ERR
        }
        Err(_) => {
            emit(b"flexiq native panic".to_vec(), out_data, out_len);
            ERR
        }
    }
}

/// Reclaim a buffer previously handed out by one of the producer functions.
///
/// # Safety
/// `ptr`/`len` must be a pair produced by [`emit`] (i.e. returned through an
/// `out_data`/`out_len` pair), and freed at most once.
#[no_mangle]
pub unsafe extern "C" fn flexiq_ffi_free(ptr: *mut u8, len: usize) {
    if !ptr.is_null() && len != 0 {
        drop(Box::from_raw(std::ptr::slice_from_raw_parts_mut(ptr, len)));
    }
}

/// Borrow the queue behind a handle.
///
/// # Safety
/// `handle` must be a live `QueueHandle` pointer (see [`crate::handle`]).
unsafe fn handle_ref<'a>(handle: i64) -> &'a QueueHandle {
    crate::handle::borrow::<QueueHandle>(handle)
}

/// Parse JSON options without the JNI-flavoured `BindingError` (no env to throw on).
fn parse_options<'a, T: serde::Deserialize<'a>>(raw: &'a str, what: &str) -> Result<T, String> {
    serde_json::from_str(raw).map_err(|e| format!("invalid {what} JSON: {e}"))
}
