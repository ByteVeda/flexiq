//! Webhook delivery log: one JSON array per subscription under
//! `webhooks:deliveries:<subscription_id>`.
//!
//! Append-only with FIFO eviction at the per-webhook cap — enough history to
//! debug recent activity, bounded so a busy webhook cannot grow a settings row
//! without limit.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use taskito_core::{now_millis, Result, Storage};

use crate::dashboard::stores::kv;

/// Settings-key prefix for a subscription's delivery log.
pub const DELIVERY_PREFIX: &str = "webhooks:deliveries:";

/// Records kept per webhook before the oldest are evicted.
const MAX_PER_WEBHOOK: usize = 200;

/// Longest response body stored per record.
const RESPONSE_BODY_MAX_BYTES: usize = 2048;

/// Settled state of one delivery attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DeliveryStatus {
    /// Recorded before its first attempt settles.
    Pending,
    /// Accepted by the endpoint.
    Delivered,
    /// Rejected or unreachable, retries may remain.
    Failed,
    /// Out of retries.
    Dead,
}

impl DeliveryStatus {
    /// Wire form, as stored and filtered on.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Delivered => "delivered",
            Self::Failed => "failed",
            Self::Dead => "dead",
        }
    }

    /// Parse a status filter, `None` when it names no known status.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "pending" => Some(Self::Pending),
            "delivered" => Some(Self::Delivered),
            "failed" => Some(Self::Failed),
            "dead" => Some(Self::Dead),
            _ => None,
        }
    }
}

/// One attempted delivery.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliveryRecord {
    /// Record id.
    pub id: String,
    /// Subscription this belongs to.
    pub subscription_id: String,
    /// Event type that triggered it.
    pub event: String,
    /// Body that was sent.
    #[serde(default)]
    pub payload: Map<String, Value>,
    /// Task the event was about, when it was about one.
    #[serde(default)]
    pub task_name: Option<String>,
    /// Job the event was about, when it was about one.
    #[serde(default)]
    pub job_id: Option<String>,
    /// Outcome.
    #[serde(default = "pending")]
    pub status: DeliveryStatus,
    /// Attempts made.
    #[serde(default)]
    pub attempts: i64,
    /// Last HTTP status seen.
    #[serde(default)]
    pub response_code: Option<i64>,
    /// Truncated response body.
    #[serde(default)]
    pub response_body: Option<String>,
    /// Round-trip time of the settled attempt.
    #[serde(default)]
    pub latency_ms: Option<i64>,
    /// Transport-level error, when there was no response at all.
    #[serde(default)]
    pub error: Option<String>,
    /// Unix milliseconds.
    #[serde(default)]
    pub created_at: i64,
    /// Unix milliseconds; unset while pending.
    #[serde(default)]
    pub completed_at: Option<i64>,
}

fn pending() -> DeliveryStatus {
    DeliveryStatus::Pending
}

/// Which records a listing should return.
#[derive(Debug, Default, Clone)]
pub struct DeliveryFilter {
    /// Only this status.
    pub status: Option<DeliveryStatus>,
    /// Only this event type.
    pub event: Option<String>,
    /// Page size.
    pub limit: usize,
    /// Records to skip.
    pub offset: usize,
}

fn key(subscription_id: &str) -> String {
    format!("{DELIVERY_PREFIX}{subscription_id}")
}

/// Every stored record for a subscription, oldest first.
fn load(storage: &impl Storage, subscription_id: &str) -> Result<Vec<DeliveryRecord>> {
    let rows: Vec<Value> = kv::read(storage, &key(subscription_id))?;
    Ok(rows
        .into_iter()
        .filter_map(|row| serde_json::from_value(row).ok())
        .collect())
}

/// A page of records, newest first.
pub fn list_for(
    storage: &impl Storage,
    subscription_id: &str,
    filter: &DeliveryFilter,
) -> Result<Vec<DeliveryRecord>> {
    let mut rows = load(storage, subscription_id)?;
    rows.reverse();
    Ok(rows
        .into_iter()
        .filter(|record| filter.status.is_none_or(|wanted| record.status == wanted))
        .filter(|record| {
            filter
                .event
                .as_ref()
                .is_none_or(|wanted| &record.event == wanted)
        })
        .skip(filter.offset)
        .take(filter.limit)
        .collect())
}

/// How many records a subscription has, before filtering.
pub fn count_for(storage: &impl Storage, subscription_id: &str) -> Result<usize> {
    Ok(load(storage, subscription_id)?.len())
}

/// One record by id.
pub fn get(
    storage: &impl Storage,
    subscription_id: &str,
    delivery_id: &str,
) -> Result<Option<DeliveryRecord>> {
    Ok(load(storage, subscription_id)?
        .into_iter()
        .find(|record| record.id == delivery_id))
}

/// Append a settled attempt, evicting the oldest records past the cap.
pub fn record_attempt(
    storage: &impl Storage,
    mut record: DeliveryRecord,
) -> Result<DeliveryRecord> {
    let now = now_millis();
    record.id = uuid::Uuid::new_v4().simple().to_string();
    record.created_at = now;
    record.completed_at = (record.status != DeliveryStatus::Pending).then_some(now);
    record.response_body = record.response_body.as_deref().map(truncate);

    let mut rows = load(storage, &record.subscription_id)?;
    rows.push(record.clone());
    if rows.len() > MAX_PER_WEBHOOK {
        rows.drain(..rows.len() - MAX_PER_WEBHOOK);
    }
    kv::write(storage, &key(&record.subscription_id), &rows)?;
    Ok(record)
}

/// Drop a subscription's whole log, e.g. when the subscription is deleted.
pub fn delete_for(storage: &impl Storage, subscription_id: &str) -> Result<bool> {
    storage.delete_setting(&key(subscription_id))
}

/// Cut a response body to the stored cap, on a character boundary.
fn truncate(body: &str) -> String {
    if body.len() <= RESPONSE_BODY_MAX_BYTES {
        return body.to_string();
    }
    let mut cut = RESPONSE_BODY_MAX_BYTES;
    while cut > 0 && !body.is_char_boundary(cut) {
        cut -= 1;
    }
    format!("{}…", &body[..cut])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statuses_round_trip_through_their_wire_form() {
        for status in [
            DeliveryStatus::Pending,
            DeliveryStatus::Delivered,
            DeliveryStatus::Failed,
            DeliveryStatus::Dead,
        ] {
            assert_eq!(DeliveryStatus::parse(status.as_str()), Some(status));
            let encoded = serde_json::to_string(&status).expect("serializes");
            assert_eq!(encoded, format!("\"{}\"", status.as_str()));
        }
        assert_eq!(DeliveryStatus::parse("nope"), None);
    }

    #[test]
    fn an_oversized_body_is_truncated_on_a_character_boundary() {
        let body = "é".repeat(RESPONSE_BODY_MAX_BYTES);
        let truncated = truncate(&body);
        assert!(truncated.ends_with('…'));
        assert!(truncated.len() <= RESPONSE_BODY_MAX_BYTES + '…'.len_utf8());
    }

    #[test]
    fn a_short_body_is_left_alone() {
        assert_eq!(truncate("ok"), "ok");
    }
}
