//! Per-task middleware disable list.
//!
//! Stored as `middleware:disabled:<task_name>` → JSON array of middleware
//! names. Workers read it on every task invocation, so a toggle takes effect on
//! the next job without a restart — which is also why this process can write it
//! without knowing anything about the middleware itself.

use serde_json::Value;
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
/// An emptied list leaves a `[]` row rather than deleting it. Deleting sat
/// outside the compare-and-set, so a concurrent writer's entry could be added
/// between the swap and the delete and then removed by it — the very lost
/// update the compare-and-set exists to prevent. Nothing reads the difference:
/// [`get_for`] parses `[]` as "nothing disabled", [`list_all`] filters empty
/// lists out, and the key is a reserved prefix, so the generic settings view
/// does not show it either.
pub fn set_disabled(
    storage: &impl Storage,
    task_name: &str,
    middleware_name: &str,
    disabled: bool,
) -> Result<Vec<String>> {
    // Edited as raw JSON so an entry this build would not parse is left alone
    // rather than dropped by the write.
    let current: Vec<String> = kv::update(storage, &key(task_name), |names: &mut Vec<Value>| {
        let already = names
            .iter()
            .any(|name| name.as_str() == Some(middleware_name));
        if disabled {
            if !already {
                names.push(Value::String(middleware_name.to_string()));
            }
        } else {
            names.retain(|name| name.as_str() != Some(middleware_name));
        }
        names
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect()
    })?;

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
