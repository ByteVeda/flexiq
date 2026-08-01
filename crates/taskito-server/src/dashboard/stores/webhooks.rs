//! Webhook subscriptions, stored as one JSON array under
//! `webhooks:subscriptions`.
//!
//! The layout is a cross-SDK contract: snake_case fields, one array, plaintext
//! signing secret. The API never returns the secret — only `has_secret`, plus
//! the value itself exactly once on create and rotate.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use taskito_core::{now_millis, Result, Storage};

use crate::dashboard::security::random_token;
use crate::dashboard::stores::kv;

/// Settings key holding the subscription array.
pub const SUBSCRIPTIONS_KEY: &str = "webhooks:subscriptions";

/// One persisted webhook subscription.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookSubscription {
    /// Stable id, also the delivery-log key suffix.
    pub id: String,
    /// Destination URL.
    pub url: String,
    /// Event types to deliver; empty means all.
    #[serde(default)]
    pub events: Vec<String>,
    /// Task names to deliver for; `None` means all.
    #[serde(default)]
    pub task_filter: Option<Vec<String>>,
    /// Extra request headers.
    #[serde(default)]
    pub headers: std::collections::BTreeMap<String, String>,
    /// HMAC signing secret, never returned by the API.
    #[serde(default)]
    pub secret: Option<String>,
    /// Delivery attempts before giving up.
    #[serde(default = "default_max_retries")]
    pub max_retries: i64,
    /// Per-attempt timeout.
    #[serde(default = "default_timeout")]
    pub timeout_seconds: f64,
    /// Backoff multiplier between attempts.
    #[serde(default = "default_backoff")]
    pub retry_backoff: f64,
    /// Whether deliveries fire at all.
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    /// Operator note.
    #[serde(default)]
    pub description: Option<String>,
    /// Unix milliseconds.
    #[serde(default)]
    pub created_at: i64,
    /// Unix milliseconds.
    #[serde(default)]
    pub updated_at: i64,
}

fn default_max_retries() -> i64 {
    3
}
fn default_timeout() -> f64 {
    10.0
}
fn default_backoff() -> f64 {
    2.0
}
fn default_enabled() -> bool {
    true
}

impl WebhookSubscription {
    /// A new subscription with the store's defaults and timestamps set.
    pub fn new(url: String) -> Self {
        let now = now_millis();
        Self {
            id: uuid::Uuid::new_v4().simple().to_string(),
            url,
            events: Vec::new(),
            task_filter: None,
            headers: std::collections::BTreeMap::new(),
            secret: None,
            max_retries: default_max_retries(),
            timeout_seconds: default_timeout(),
            retry_backoff: default_backoff(),
            enabled: default_enabled(),
            description: None,
            created_at: now,
            updated_at: now,
        }
    }

    /// JSON for the API: the secret is replaced by `has_secret`, and only
    /// revealed when the caller just created or rotated it.
    pub fn to_api_json(&self, reveal_secret: bool) -> Value {
        let mut value = serde_json::to_value(self).unwrap_or(Value::Null);
        if let Some(object) = value.as_object_mut() {
            let secret = object.remove("secret").and_then(|raw| match raw {
                Value::String(text) if !text.is_empty() => Some(text),
                _ => None,
            });
            object.insert("has_secret".into(), Value::Bool(secret.is_some()));
            if reveal_secret {
                if let Some(secret) = secret {
                    object.insert("secret".into(), Value::String(secret));
                }
            }
        }
        value
    }
}

/// A fresh URL-safe signing secret.
pub fn generate_secret() -> String {
    random_token()
}

/// Every subscription, in stored order.
pub fn list_all(storage: &impl Storage) -> Result<Vec<WebhookSubscription>> {
    let rows: Vec<Value> = kv::read(storage, SUBSCRIPTIONS_KEY)?;
    Ok(rows
        .into_iter()
        .filter_map(|row| match serde_json::from_value(row) {
            Ok(subscription) => Some(subscription),
            Err(error) => {
                // One corrupt row must not hide every other subscription.
                log::warn!("skipping unreadable webhook subscription: {error}");
                None
            }
        })
        .collect())
}

/// One subscription by id.
pub fn get(storage: &impl Storage, id: &str) -> Result<Option<WebhookSubscription>> {
    Ok(list_all(storage)?
        .into_iter()
        .find(|subscription| subscription.id == id))
}

/// Append a subscription.
///
/// The stored rows are edited as raw JSON rather than parsed subscriptions, so
/// a row this build cannot read survives an edit to its neighbours.
pub fn create(storage: &impl Storage, subscription: &WebhookSubscription) -> Result<()> {
    let row = serde_json::to_value(subscription)?;
    kv::update(storage, SUBSCRIPTIONS_KEY, |rows: &mut Vec<Value>| {
        rows.push(row.clone());
    })
}

/// Replace a subscription in place, stamping `updated_at`. `None` when the id
/// is unknown.
pub fn replace(
    storage: &impl Storage,
    mut subscription: WebhookSubscription,
) -> Result<Option<WebhookSubscription>> {
    subscription.updated_at = now_millis();
    let row = serde_json::to_value(&subscription)?;
    let replaced = kv::update(
        storage,
        SUBSCRIPTIONS_KEY,
        |rows: &mut Vec<Value>| match rows
            .iter_mut()
            .find(|candidate| row_id(candidate) == Some(subscription.id.as_str()))
        {
            Some(slot) => {
                *slot = row.clone();
                true
            }
            None => false,
        },
    )?;
    Ok(replaced.then_some(subscription))
}

/// Remove a subscription. `false` when the id is unknown.
pub fn delete(storage: &impl Storage, id: &str) -> Result<bool> {
    kv::update(storage, SUBSCRIPTIONS_KEY, |rows: &mut Vec<Value>| {
        let before = rows.len();
        rows.retain(|row| row_id(row) != Some(id));
        rows.len() != before
    })
}

/// The `id` of a stored row, when it has a readable one.
fn row_id(row: &Value) -> Option<&str> {
    row.get("id").and_then(Value::as_str)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_secret_is_url_safe_and_long_enough() {
        let secret = generate_secret();
        assert!(secret.len() >= 43, "32 bytes of entropy, base64url encoded");
        assert!(secret
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'));
        assert_ne!(secret, generate_secret());
    }

    #[test]
    fn the_api_form_hides_the_secret_unless_revealed() {
        let mut subscription = WebhookSubscription::new("https://example.com/hook".into());
        subscription.secret = Some("s3cret".into());

        let hidden = subscription.to_api_json(false);
        assert_eq!(hidden.get("has_secret"), Some(&Value::Bool(true)));
        assert!(hidden.get("secret").is_none());

        let revealed = subscription.to_api_json(true);
        assert_eq!(
            revealed.get("secret").and_then(Value::as_str),
            Some("s3cret")
        );
    }

    #[test]
    fn a_subscription_without_a_secret_reports_none() {
        let subscription = WebhookSubscription::new("https://example.com/hook".into());
        let json = subscription.to_api_json(true);
        assert_eq!(json.get("has_secret"), Some(&Value::Bool(false)));
        assert!(json.get("secret").is_none());
    }

    #[test]
    fn stored_rows_fill_in_the_documented_defaults() {
        let row: WebhookSubscription = serde_json::from_str(
            r#"{"id":"abc","url":"https://example.com/hook","created_at":1,"updated_at":1}"#,
        )
        .expect("a minimal row parses");
        assert_eq!(row.max_retries, 3);
        assert_eq!(row.timeout_seconds, 10.0);
        assert_eq!(row.retry_backoff, 2.0);
        assert!(row.enabled);
        assert!(row.task_filter.is_none());
    }
}
