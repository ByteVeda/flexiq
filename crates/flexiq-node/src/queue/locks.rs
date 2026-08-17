//! Distributed locking over the core storage. Locks are advisory, TTL-bounded,
//! and owner-scoped; expired locks are reaped by the worker's maintenance loop.

use flexiq_core::Storage;
use napi::bindgen_prelude::Result;
use napi_derive::napi;

use super::JsQueue;
use crate::convert::{lock_info_to_js, JsLockInfo};
use crate::error::{invalid_arg, to_napi_err};

#[napi]
impl JsQueue {
    /// Acquire `name` for `ownerId`, expiring after `ttlMs`. Returns false when
    /// another owner already holds a live lock.
    #[napi]
    pub fn acquire_lock(&self, name: String, owner_id: String, ttl_ms: i64) -> Result<bool> {
        if ttl_ms <= 0 {
            return Err(invalid_arg(format!("ttlMs must be > 0 (got {ttl_ms})")));
        }
        self.storage
            .acquire_lock(&name, &owner_id, ttl_ms)
            .map_err(to_napi_err)
    }

    /// Release `name` if held by `ownerId`. Returns false if not the holder.
    #[napi]
    pub fn release_lock(&self, name: String, owner_id: String) -> Result<bool> {
        self.storage
            .release_lock(&name, &owner_id)
            .map_err(to_napi_err)
    }

    /// Extend `name`'s TTL to `ttlMs` if held by `ownerId`. Returns false otherwise.
    #[napi]
    pub fn extend_lock(&self, name: String, owner_id: String, ttl_ms: i64) -> Result<bool> {
        if ttl_ms <= 0 {
            return Err(invalid_arg(format!("ttlMs must be > 0 (got {ttl_ms})")));
        }
        self.storage
            .extend_lock(&name, &owner_id, ttl_ms)
            .map_err(to_napi_err)
    }

    /// Current holder info for `name`, or `null` if free.
    #[napi]
    pub fn get_lock_info(&self, name: String) -> Result<Option<JsLockInfo>> {
        Ok(self
            .storage
            .get_lock_info(&name)
            .map_err(to_napi_err)?
            .map(lock_info_to_js))
    }
}
