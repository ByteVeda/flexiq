//! Per-task middleware disable list.
//!
//! Stored as `middleware:disabled:<task_name>` → JSON array of middleware
//! names. Workers read it on every task invocation, so a toggle takes effect on
//! the next job without a restart — which is also why this process can write it
//! without knowing anything about the middleware itself.

use taskito_core::{Result, Storage};

use crate::dashboard::stores::kv;

/// Settings-key prefix for a task's disable list.
pub const DISABLE_PREFIX: &str = "middleware:disabled:";

fn key(task_name: &str) -> String {
    format!("{DISABLE_PREFIX}{task_name}")
}

/// `(task name, disabled middleware)` for every task with at least one disable.
pub fn list_all(storage: &impl Storage) -> Result<Vec<(String, Vec<String>)>> {
    Ok(kv::scan_prefix(storage, DISABLE_PREFIX)?
        .into_iter()
        .map(|(task_name, raw)| (task_name, parse(&raw)))
        .filter(|(_, disabled)| !disabled.is_empty())
        .collect())
}

/// Middleware disabled for one task.
pub fn get_for(storage: &impl Storage, task_name: &str) -> Result<Vec<String>> {
    Ok(storage
        .get_setting(&key(task_name))?
        .map(|raw| parse(&raw))
        .unwrap_or_default())
}

/// Flip one middleware on or off for a task; returns the new disable list.
///
/// An empty list deletes the row rather than storing `[]`, so a task with
/// nothing disabled leaves no trace in the settings listing.
pub fn set_disabled(
    storage: &impl Storage,
    task_name: &str,
    middleware_name: &str,
    disabled: bool,
) -> Result<Vec<String>> {
    let mut current = get_for(storage, task_name)?;
    if disabled {
        if !current.iter().any(|name| name == middleware_name) {
            current.push(middleware_name.to_string());
        }
    } else {
        current.retain(|name| name != middleware_name);
    }

    if current.is_empty() {
        storage.delete_setting(&key(task_name))?;
    } else {
        kv::write(storage, &key(task_name), &current)?;
    }
    Ok(current)
}

/// Clear every disable for a task — all its middleware fires again.
pub fn clear_for(storage: &impl Storage, task_name: &str) -> Result<bool> {
    storage.delete_setting(&key(task_name))
}

/// Read a stored list, ignoring non-string entries and unreadable documents.
fn parse(raw: &str) -> Vec<String> {
    serde_json::from_str::<Vec<serde_json::Value>>(raw)
        .unwrap_or_default()
        .into_iter()
        .filter_map(|entry| entry.as_str().map(str::to_string))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_string_entries_survive_parsing() {
        assert_eq!(
            parse(r#"["a",1,"b",null]"#),
            vec!["a".to_string(), "b".to_string()]
        );
        assert!(parse("not json").is_empty());
        assert!(parse(r#"{"a":1}"#).is_empty());
    }
}
