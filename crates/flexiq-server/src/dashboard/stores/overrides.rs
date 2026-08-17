//! Per-task and per-queue runtime overrides.
//!
//! Layout in the settings store: `overrides:task:<task_name>` and
//! `overrides:queue:<queue_name>`, each a JSON object of only the fields an
//! operator actually set. Workers read them at startup, so the contract is
//! deliberately narrow — an unknown field is rejected rather than written,
//! because nothing would ever consume it.

use flexiq_core::{now_millis, Result, Storage};
use serde_json::{json, Map, Value};

use crate::dashboard::error::ApiError;
use crate::dashboard::stores::kv;

/// Settings-key prefix for task overrides.
pub const TASK_PREFIX: &str = "overrides:task:";
/// Settings-key prefix for queue overrides.
pub const QUEUE_PREFIX: &str = "overrides:queue:";

/// Fields a task override may carry.
pub const TASK_FIELDS: [&str; 7] = [
    "rate_limit",
    "max_concurrent",
    "max_retries",
    "retry_backoff",
    "timeout",
    "priority",
    "paused",
];

/// Fields a queue override may carry.
pub const QUEUE_FIELDS: [&str; 3] = ["rate_limit", "max_concurrent", "paused"];

/// Timestamps below this are seconds, not milliseconds — rows written by an
/// older shell. Normalised on read so the UI never renders 1970.
const SECONDS_ERA_THRESHOLD: i64 = 1_000_000_000_000;

/// What an override scope is: which key prefix and which fields it accepts.
#[derive(Debug, Clone, Copy)]
pub enum Scope {
    /// A single task.
    Task,
    /// A whole queue.
    Queue,
}

impl Scope {
    fn prefix(self) -> &'static str {
        match self {
            Self::Task => TASK_PREFIX,
            Self::Queue => QUEUE_PREFIX,
        }
    }

    fn allowed(self) -> &'static [&'static str] {
        match self {
            Self::Task => &TASK_FIELDS,
            Self::Queue => &QUEUE_FIELDS,
        }
    }

    /// Name of the field holding the subject's name in the API response.
    fn name_field(self) -> &'static str {
        match self {
            Self::Task => "task_name",
            Self::Queue => "queue_name",
        }
    }
}

/// One override, as the API returns it: every allowed field present, `null`
/// where unset, so the UI can render a complete form.
pub fn to_api_json(scope: Scope, name: &str, stored: &Map<String, Value>) -> Value {
    let mut body = Map::new();
    body.insert(scope.name_field().into(), json!(name));
    for field in scope.allowed() {
        let value = stored.get(*field).cloned().unwrap_or(Value::Null);
        // `paused` is a flag, not an optional: absent means false.
        let value = if *field == "paused" {
            json!(value.as_bool().unwrap_or(false))
        } else {
            value
        };
        body.insert((*field).into(), value);
    }
    body.insert(
        "updated_at".into(),
        json!(normalise_timestamp(
            stored
                .get("updated_at")
                .and_then(Value::as_i64)
                .unwrap_or(0)
        )),
    );
    Value::Object(body)
}

/// Every stored override in a scope, as `(name, stored fields)`.
pub fn list(scope: Scope, storage: &impl Storage) -> Result<Vec<(String, Map<String, Value>)>> {
    Ok(kv::scan_prefix(storage, scope.prefix())?
        .into_iter()
        .map(|(name, raw)| {
            let fields = serde_json::from_str::<Map<String, Value>>(&raw).unwrap_or_default();
            (name, fields)
        })
        .collect())
}

/// One override's stored fields, or `None` when nothing is set.
pub fn get(scope: Scope, storage: &impl Storage, name: &str) -> Result<Option<Map<String, Value>>> {
    let Some(raw) = storage.get_setting(&format!("{}{name}", scope.prefix()))? else {
        return Ok(None);
    };
    Ok(Some(
        serde_json::from_str::<Map<String, Value>>(&raw).unwrap_or_default(),
    ))
}

/// Merge `patch` into an override and persist it.
///
/// An explicit `null` clears a field rather than storing a null, which is what
/// lets the UI reset one knob back to the decorator default.
pub fn set(
    scope: Scope,
    storage: &impl Storage,
    name: &str,
    patch: &Map<String, Value>,
) -> std::result::Result<Map<String, Value>, ApiError> {
    if name.is_empty() {
        return Err(ApiError::BadRequest("name must not be empty".into()));
    }
    validate(scope, patch)?;

    let merged = kv::update(
        storage,
        &format!("{}{name}", scope.prefix()),
        |stored: &mut Map<String, Value>| {
            stored.remove("updated_at");
            for (field, value) in patch {
                if value.is_null() {
                    stored.remove(field);
                } else {
                    stored.insert(field.clone(), value.clone());
                }
            }
            stored.insert("updated_at".into(), json!(now_millis()));
            stored.clone()
        },
    )?;
    Ok(merged)
}

/// Remove an override entirely.
pub fn clear(scope: Scope, storage: &impl Storage, name: &str) -> Result<bool> {
    storage.delete_setting(&format!("{}{name}", scope.prefix()))
}

/// Reject unknown fields and out-of-range values before anything is written.
fn validate(scope: Scope, patch: &Map<String, Value>) -> std::result::Result<(), ApiError> {
    let unknown: Vec<&str> = patch
        .keys()
        .map(String::as_str)
        .filter(|field| !scope.allowed().contains(field))
        .collect();
    if !unknown.is_empty() {
        return Err(ApiError::BadRequest(format!(
            "unknown override fields: {:?}; allowed: {:?}",
            unknown,
            scope.allowed()
        )));
    }

    for (field, value) in patch {
        if value.is_null() {
            continue;
        }
        match field.as_str() {
            "rate_limit" => {
                let text = value
                    .as_str()
                    .filter(|raw| !raw.is_empty())
                    .ok_or_else(|| {
                        ApiError::BadRequest(
                            "rate_limit must be a non-empty string like '100/m'".into(),
                        )
                    })?;
                if !text.contains('/') {
                    return Err(ApiError::BadRequest(
                        "rate_limit must contain a unit, e.g. '10/s', '100/m', '3600/h'".into(),
                    ));
                }
            }
            "max_concurrent" => require_int(field, value, Some(0))?,
            "max_retries" => require_int(field, value, Some(0))?,
            "timeout" => require_int(field, value, Some(1))?,
            "priority" => require_int(field, value, None)?,
            "retry_backoff" => {
                let number = value
                    .as_f64()
                    .filter(|_| !value.is_boolean())
                    .ok_or_else(|| ApiError::BadRequest("retry_backoff must be a number".into()))?;
                if number < 0.0 {
                    return Err(ApiError::BadRequest("retry_backoff must be >= 0".into()));
                }
            }
            "paused" if !value.is_boolean() => {
                return Err(ApiError::BadRequest("paused must be a boolean".into()))
            }
            _ => {}
        }
    }
    Ok(())
}

fn require_int(
    field: &str,
    value: &Value,
    minimum: Option<i64>,
) -> std::result::Result<(), ApiError> {
    // `is_i64` is false for JSON booleans and floats, which is exactly the
    // distinction the SDK dashboards make.
    let number = value
        .as_i64()
        .ok_or_else(|| ApiError::BadRequest(format!("{field} must be an integer")))?;
    match minimum {
        Some(minimum) if number < minimum => Err(ApiError::BadRequest(format!(
            "{field} must be >= {minimum}"
        ))),
        _ => Ok(()),
    }
}

fn normalise_timestamp(stored: i64) -> i64 {
    if stored > 0 && stored < SECONDS_ERA_THRESHOLD {
        stored * 1_000
    } else {
        stored
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn patch(pairs: &[(&str, Value)]) -> Map<String, Value> {
        pairs
            .iter()
            .map(|(key, value)| ((*key).to_string(), value.clone()))
            .collect()
    }

    #[test]
    fn unknown_fields_are_rejected() {
        let error = validate(Scope::Task, &patch(&[("nonsense", json!(1))]))
            .expect_err("must reject unknown fields");
        assert!(matches!(error, ApiError::BadRequest(message) if message.contains("nonsense")));
    }

    #[test]
    fn queue_scope_rejects_task_only_fields() {
        assert!(validate(Scope::Queue, &patch(&[("timeout", json!(5))])).is_err());
        validate(Scope::Queue, &patch(&[("max_concurrent", json!(5))])).expect("allowed");
    }

    #[test]
    fn value_ranges_are_enforced() {
        assert!(validate(Scope::Task, &patch(&[("timeout", json!(0))])).is_err());
        assert!(validate(Scope::Task, &patch(&[("max_retries", json!(-1))])).is_err());
        assert!(validate(Scope::Task, &patch(&[("paused", json!("yes"))])).is_err());
        assert!(validate(Scope::Task, &patch(&[("rate_limit", json!("100"))])).is_err());
        assert!(validate(Scope::Task, &patch(&[("max_concurrent", json!(true))])).is_err());
        validate(Scope::Task, &patch(&[("rate_limit", json!("100/m"))])).expect("valid");
    }

    #[test]
    fn nulls_pass_validation_because_they_clear_a_field() {
        validate(Scope::Task, &patch(&[("timeout", Value::Null)])).expect("null clears");
    }

    #[test]
    fn the_api_form_fills_in_every_allowed_field() {
        let stored = patch(&[("timeout", json!(30)), ("updated_at", json!(1_700_000_000))]);
        let body = to_api_json(Scope::Task, "send_email", &stored);
        assert_eq!(body["task_name"], json!("send_email"));
        assert_eq!(body["timeout"], json!(30));
        assert_eq!(body["priority"], Value::Null);
        assert_eq!(body["paused"], json!(false));
        // A seconds-era timestamp is normalised to milliseconds.
        assert_eq!(body["updated_at"], json!(1_700_000_000_000i64));
    }
}
