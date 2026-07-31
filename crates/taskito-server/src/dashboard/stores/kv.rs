//! Reading and writing the JSON documents the feature stores keep in the
//! settings table.
//!
//! Every store here persists through `Storage::set_setting` rather than a table
//! of its own: that is what makes the same rows readable by SQLite, Postgres,
//! and Redis deployments, and by every SDK dashboard pointed at the same
//! backend. A malformed document reads as empty rather than failing the
//! request — the dashboard stays usable, and the log says what was skipped.

use serde::de::DeserializeOwned;
use serde::Serialize;
use taskito_core::{Result, Storage};

/// Parse a stored JSON document, or the type's default when it is missing or
/// unreadable.
pub fn read<T: DeserializeOwned + Default>(storage: &impl Storage, key: &str) -> Result<T> {
    let Some(raw) = storage.get_setting(key)? else {
        return Ok(T::default());
    };
    Ok(serde_json::from_str(&raw).unwrap_or_else(|error| {
        log::warn!(
            "setting '{key}' is not the expected JSON document ({error}); treating as empty"
        );
        T::default()
    }))
}

/// Write a JSON document compactly, matching what the SDK dashboards store.
pub fn write<T: Serialize>(storage: &impl Storage, key: &str, value: &T) -> Result<()> {
    let encoded = serde_json::to_string(value)?;
    storage.set_setting(key, &encoded)
}

/// Every setting whose key starts with `prefix`, as `(suffix, raw value)`.
pub fn scan_prefix(storage: &impl Storage, prefix: &str) -> Result<Vec<(String, String)>> {
    let mut matches: Vec<(String, String)> = storage
        .list_settings()?
        .into_iter()
        .filter_map(|(key, value)| {
            key.strip_prefix(prefix)
                .map(|suffix| (suffix.to_string(), value))
        })
        .collect();
    // Settings come back unordered; a stable listing keeps the UI from
    // reshuffling rows between polls.
    matches.sort_by(|(left, _), (right, _)| left.cmp(right));
    Ok(matches)
}
