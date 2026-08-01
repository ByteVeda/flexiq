//! Reading and writing the JSON documents the feature stores keep in the
//! settings table.
//!
//! Every store here persists through `Storage::set_setting` rather than a table
//! of its own: that is what makes the same rows readable by SQLite, Postgres,
//! and Redis deployments, and by every SDK dashboard pointed at the same
//! backend. A malformed document reads as empty rather than failing the
//! request — the dashboard stays usable, and the log says what was skipped.
//!
//! A whole document per key means a read-then-write would drop a concurrent
//! edit wholesale, so anything that mutates one goes through [`update`], which
//! writes conditionally and retries.

use serde::de::DeserializeOwned;
use serde::Serialize;
use taskito_core::{QueueError, Result, Storage};

/// How many times [`update`] re-reads and retries before giving up.
///
/// A losing writer only loses to a writer that won, so the bound has to clear
/// the number of dashboards that could be editing one document at once. Writes
/// here are admin-frequency: losing this many in a row is a fault, not
/// contention worth waiting out.
const MAX_ATTEMPTS: usize = 25;

/// Parse a stored JSON document, or the type's default when it is missing or
/// unreadable.
pub fn read<T: DeserializeOwned + Default>(storage: &impl Storage, key: &str) -> Result<T> {
    Ok(parse(key, storage.get_setting(key)?.as_deref()))
}

/// Write a JSON document compactly, matching what the SDK dashboards store.
///
/// Unconditional: use it only when the new document does not depend on the
/// stored one. Otherwise use [`update`].
pub fn write<T: Serialize>(storage: &impl Storage, key: &str, value: &T) -> Result<()> {
    let encoded = serde_json::to_string(value)?;
    storage.set_setting(key, &encoded)
}

/// Read a document, apply `mutate`, and store it only if nobody else wrote in
/// between; on a lost race, re-read and try again.
///
/// `mutate` runs once per attempt, so it must not do anything but change the
/// document it is handed. Its return value comes back from the winning attempt.
pub fn update<T, R>(
    storage: &impl Storage,
    key: &str,
    mut mutate: impl FnMut(&mut T) -> R,
) -> Result<R>
where
    T: DeserializeOwned + Default + Serialize,
{
    for _ in 0..MAX_ATTEMPTS {
        let stored = storage.get_setting(key)?;
        let mut document: T = parse(key, stored.as_deref());
        // Compared against the document as parsed, not against the raw stored
        // string: on a missing key the raw is `None` while the encoding is the
        // default (`[]`), so comparing to the raw would treat "changed nothing"
        // as a change and write an empty row — which is what a delete for an
        // unknown id used to do.
        let before = serde_json::to_string(&document)?;
        let outcome = mutate(&mut document);
        let encoded = serde_json::to_string(&document)?;
        // A mutation that changed nothing needs no write — which is also what
        // keeps a lookup that matched no row from touching the document.
        if encoded == before {
            return Ok(outcome);
        }
        if storage.set_setting_if(key, stored.as_deref(), &encoded)? {
            return Ok(outcome);
        }
    }
    Err(QueueError::SettingConflict(key.to_string()))
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

/// Decode a stored document, falling back to the default and logging what was
/// skipped.
fn parse<T: DeserializeOwned + Default>(key: &str, raw: Option<&str>) -> T {
    let Some(raw) = raw else {
        return T::default();
    };
    serde_json::from_str(raw).unwrap_or_else(|error| {
        log::warn!(
            "setting '{key}' is not the expected JSON document ({error}); treating as empty"
        );
        T::default()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use taskito_core::storage::sqlite::SqliteStorage;

    fn storage() -> SqliteStorage {
        SqliteStorage::in_memory().expect("in-memory sqlite")
    }

    #[test]
    fn a_no_op_mutation_on_a_missing_key_writes_nothing() {
        // The skip used to compare the new encoding against the *raw* stored
        // string. On a missing key that is `None` while the encoding is the
        // default `[]`, so a mutation that changed nothing still wrote a row —
        // which is how deleting an unknown webhook id created an empty one.
        let storage = storage();

        let found: bool = update(&storage, "missing", |names: &mut Vec<String>| {
            let before = names.len();
            names.retain(|name| name != "absent");
            names.len() != before
        })
        .expect("update");

        assert!(!found, "nothing matched");
        assert_eq!(
            storage.get_setting("missing").expect("read back"),
            None,
            "a mutation that changed nothing must leave no row"
        );
    }

    #[test]
    fn a_no_op_mutation_on_an_existing_key_leaves_it_alone() {
        let storage = storage();
        write(&storage, "present", &vec!["keep".to_string()]).expect("seed");

        update(&storage, "present", |names: &mut Vec<String>| {
            names.retain(|name| name != "absent");
        })
        .expect("update");

        assert_eq!(
            storage
                .get_setting("present")
                .expect("read back")
                .as_deref(),
            Some(r#"["keep"]"#)
        );
    }

    #[test]
    fn a_real_mutation_still_writes() {
        let storage = storage();

        update(&storage, "list", |names: &mut Vec<String>| {
            names.push("added".to_string());
        })
        .expect("update");

        assert_eq!(
            storage.get_setting("list").expect("read back").as_deref(),
            Some(r#"["added"]"#)
        );
    }
}
