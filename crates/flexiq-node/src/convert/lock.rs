//! JS-facing shape for distributed lock info. Timestamps are Unix milliseconds.

use flexiq_core::storage::records::LockInfo;
use napi_derive::napi;

/// JS-facing view of a held distributed lock.
#[napi(object)]
pub struct JsLockInfo {
    pub lock_name: String,
    pub owner_id: String,
    pub acquired_at: i64,
    pub expires_at: i64,
}

pub fn lock_info_to_js(row: LockInfo) -> JsLockInfo {
    JsLockInfo {
        lock_name: row.lock_name,
        owner_id: row.owner_id,
        acquired_at: row.acquired_at,
        expires_at: row.expires_at,
    }
}
