//! Reading a `flexiq.admin.v1` request out of a JSON body or a query string.
//!
//! The rules are [`super::request`]'s, unchanged: serde structs, unknown
//! fields refused, both spellings of every field accepted, and no validation —
//! an empty name or an unparseable cron expression is refused by the handler a
//! gRPC caller reaches, so the two doors cannot disagree about one request.
//!
//! Only the messages a client *writes* are here. A request whose every field is
//! bound by the path (`PauseQueue`, `ClearTaskOverride`, …) or that has no
//! fields at all is built in the handler, not parsed.

use serde::Deserialize;

use super::request::Structured;
use super::wkt::{JsonBytes, JsonDuration, JsonTimestamp};
use crate::grpc::pb::admin as pb;
use crate::grpc::pb::admin::purge_dead_letters_request::Filter;
use crate::grpc::pb::admin::put_periodic_task_request::Body;

/// `GET /v1/admin/throughput` — the window, as a query parameter.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetThroughput {
    /// How far back to count, a duration such as `300s`. Omitting it takes the
    /// server's default.
    #[serde(default)]
    pub window: Option<JsonDuration>,
}

impl GetThroughput {
    /// The request message.
    pub fn into_message(self) -> pb::GetThroughputRequest {
        pb::GetThroughputRequest {
            window: self.window.map(|value| value.0),
        }
    }
}

/// `GET /v1/admin/deadLetters` — the page cursor, as query parameters.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ListDeadLetters {
    /// Rows per page, `pageSize=`. Omitting it takes the server's default.
    #[serde(default, alias = "page_size")]
    pub page_size: Option<i32>,
    /// The previous response's `nextPageToken`, `pageToken=`. Opaque.
    #[serde(default, alias = "page_token")]
    pub page_token: Option<String>,
}

impl ListDeadLetters {
    /// The request message.
    pub fn into_message(self) -> pb::ListDeadLettersRequest {
        pb::ListDeadLettersRequest {
            page_size: self.page_size.unwrap_or_default(),
            page_token: self.page_token.unwrap_or_default(),
        }
    }
}

/// The one query parameter a single dead letter or periodic task takes.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct IncludePayload {
    /// Send the payload back, `includePayload=true`. Off by default.
    #[serde(default, alias = "include_payload")]
    pub include_payload: bool,
}

/// `POST /v1/admin/deadLetters:purge` — a `PurgeDeadLettersRequest`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PurgeDeadLetters {
    /// The `failed_before` arm of `filter`, RFC 3339.
    #[serde(default, alias = "failed_before")]
    pub failed_before: Option<JsonTimestamp>,
    /// The `task_name` arm of `filter`.
    #[serde(default, alias = "task_name")]
    pub task_name: Option<String>,
}

impl PurgeDeadLetters {
    /// The request message. No arm purges the whole namespace, which is the
    /// contract's meaning and not a default this door picked.
    pub fn into_message(self) -> Result<pb::PurgeDeadLettersRequest, String> {
        let filter = match (self.failed_before, self.task_name) {
            (Some(_), Some(_)) => return Err(
                "`failedBefore` and `taskName` are the two arms of one filter; send one of them"
                    .to_string(),
            ),
            (Some(before), None) => Some(Filter::FailedBefore(before.0)),
            (None, Some(task)) => Some(Filter::TaskName(task)),
            (None, None) => None,
        };
        Ok(pb::PurgeDeadLettersRequest { filter })
    }
}

/// `POST /v1/admin/periodicTasks` — a `PutPeriodicTaskRequest`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PutPeriodicTask {
    /// The task's handle in the namespace.
    #[serde(default)]
    pub name: String,
    /// The registered task each firing enqueues, `taskName`.
    #[serde(default, alias = "task_name")]
    pub task_name: String,
    /// Six fields, seconds first.
    #[serde(default)]
    pub cron: String,
    /// Empty, like omitting it, is the default queue.
    #[serde(default)]
    pub queue: String,
    /// The `raw` arm of `body`: the payload envelope, base64.
    #[serde(default)]
    pub raw: Option<JsonBytes>,
    /// The `structured` arm of `body`, read exactly as an enqueue's is.
    #[serde(default)]
    pub structured: Option<Structured>,
    /// Create the task paused, `startPaused`.
    #[serde(default, alias = "start_paused")]
    pub start_paused: bool,
    /// IANA name to read the cron expression in. Omitting it is UTC.
    #[serde(default)]
    pub timezone: Option<String>,
}

impl PutPeriodicTask {
    /// The request message, or the one shape a JSON body can get wrong that the
    /// protobuf encoding cannot: both arms of `body` at once.
    pub fn into_message(self) -> Result<pb::PutPeriodicTaskRequest, String> {
        let body = match (self.raw, self.structured) {
            (Some(_), Some(_)) => {
                return Err(
                    "`raw` and `structured` are the two arms of one field; send one of them"
                        .to_string(),
                )
            }
            (Some(raw), None) => Some(Body::Raw(raw.0)),
            (None, Some(structured)) => Some(Body::Structured(structured.into_message()?)),
            (None, None) => None,
        };
        Ok(pb::PutPeriodicTaskRequest {
            name: self.name,
            task_name: self.task_name,
            cron: self.cron,
            queue: self.queue,
            body,
            start_paused: self.start_paused,
            timezone: self.timezone,
        })
    }
}

/// `POST /v1/admin/tasks/{task_name}/override` — the body *is* the
/// `TaskOverride`, because the binding names it as the body.
///
/// `updateTime` is output only. It is accepted and handed on, and the shared
/// handler ignores it as the contract says — so an object read from
/// `GET /v1/admin/overrides` can be edited and sent straight back, which is the
/// point of AIP-203's "ignore on input" over refusing it.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TaskOverride {
    /// `<count>/<unit>`, `rateLimit`.
    #[serde(default, alias = "rate_limit")]
    pub rate_limit: Option<String>,
    /// `maxConcurrent`.
    #[serde(default, alias = "max_concurrent")]
    pub max_concurrent: Option<i32>,
    /// `maxRetries`.
    #[serde(default, alias = "max_retries")]
    pub max_retries: Option<i32>,
    /// A duration such as `"1.5s"`, `retryBackoff`.
    #[serde(default, alias = "retry_backoff")]
    pub retry_backoff: Option<JsonDuration>,
    /// A duration such as `"30s"`.
    #[serde(default)]
    pub timeout: Option<JsonDuration>,
    /// Higher runs first.
    #[serde(default)]
    pub priority: Option<i32>,
    /// Hold this task's jobs.
    #[serde(default)]
    pub paused: Option<bool>,
    /// Output only; see the type's documentation.
    #[serde(default, alias = "update_time")]
    pub update_time: Option<JsonTimestamp>,
}

impl TaskOverride {
    /// The message.
    pub fn into_message(self) -> pb::TaskOverride {
        pb::TaskOverride {
            rate_limit: self.rate_limit,
            max_concurrent: self.max_concurrent,
            max_retries: self.max_retries,
            retry_backoff: self.retry_backoff.map(|value| value.0),
            timeout: self.timeout.map(|value| value.0),
            priority: self.priority,
            paused: self.paused,
            update_time: self.update_time.map(|value| value.0),
        }
    }
}

/// `POST /v1/admin/queues/{queue}/override` — the body is the `QueueOverride`.
/// `updateTime` is accepted and ignored, as on [`TaskOverride`].
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct QueueOverride {
    /// `<count>/<unit>`, `rateLimit`.
    #[serde(default, alias = "rate_limit")]
    pub rate_limit: Option<String>,
    /// `maxConcurrent`.
    #[serde(default, alias = "max_concurrent")]
    pub max_concurrent: Option<i32>,
    /// Output only.
    #[serde(default, alias = "update_time")]
    pub update_time: Option<JsonTimestamp>,
}

impl QueueOverride {
    /// The message.
    pub fn into_message(self) -> pb::QueueOverride {
        pb::QueueOverride {
            rate_limit: self.rate_limit,
            max_concurrent: self.max_concurrent,
            update_time: self.update_time.map(|value| value.0),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn purge(body: serde_json::Value) -> Result<pb::PurgeDeadLettersRequest, String> {
        serde_json::from_value::<PurgeDeadLetters>(body)
            .map_err(|error| error.to_string())?
            .into_message()
    }

    #[test]
    fn a_purge_takes_one_filter_arm_or_none() {
        assert!(purge(serde_json::json!({}))
            .expect("parses")
            .filter
            .is_none());
        assert!(matches!(
            purge(serde_json::json!({"taskName": "charge"})).expect("parses").filter,
            Some(Filter::TaskName(ref task)) if task == "charge"
        ));
        assert!(matches!(
            purge(serde_json::json!({"failed_before": "2025-09-03T12:26:40Z"}))
                .expect("parses")
                .filter,
            Some(Filter::FailedBefore(_))
        ));
        let error = purge(serde_json::json!({
            "failedBefore": "2025-09-03T12:26:40Z",
            "taskName": "charge"
        }))
        .expect_err("a oneof holds one arm");
        assert!(error.contains("one of them"), "unhelpful message: {error}");
    }

    #[test]
    fn a_periodic_task_reads_structured_arguments_as_an_enqueue_does() {
        let message = serde_json::from_value::<PutPeriodicTask>(serde_json::json!({
            "name": "nightly",
            "taskName": "report",
            "cron": "0 0 3 * * *",
            "structured": {"args": [1], "kwargs": {"k": "v"}},
            "startPaused": true,
            "timezone": "Europe/Berlin"
        }))
        .expect("a well-formed body")
        .into_message()
        .expect("one body arm");
        assert_eq!(message.task_name, "report");
        assert!(message.start_paused);
        let Some(pb::put_periodic_task_request::Body::Structured(args)) = message.body else {
            panic!("the structured arm");
        };
        assert_eq!(args.args.len(), 1);
        assert_eq!(args.kwargs.len(), 1);
    }

    #[test]
    fn an_override_reads_durations_and_passes_update_time_on() {
        let message = serde_json::from_value::<TaskOverride>(serde_json::json!({
            "rateLimit": "100/m",
            "max_concurrent": 2,
            "retryBackoff": "1.5s",
            "timeout": "30s",
            "updateTime": "2025-09-03T12:26:40Z"
        }))
        .expect("a well-formed override")
        .into_message();
        assert_eq!(message.rate_limit.as_deref(), Some("100/m"));
        assert_eq!(message.max_concurrent, Some(2));
        assert_eq!(message.timeout.map(|value| value.seconds), Some(30));
        assert_eq!(
            message
                .retry_backoff
                .map(|value| (value.seconds, value.nanos)),
            Some((1, 500_000_000))
        );
        assert!(message.update_time.is_some());
    }

    #[test]
    fn a_misspelled_override_field_is_refused() {
        assert!(
            serde_json::from_value::<QueueOverride>(serde_json::json!({"paused": true})).is_err()
        );
    }

    #[test]
    fn a_window_reads_from_the_query_string() {
        let query: GetThroughput = serde_urlencoded::from_str("window=300s").expect("parses");
        assert_eq!(
            query.into_message().window.map(|value| value.seconds),
            Some(300)
        );
        assert!(serde_urlencoded::from_str::<GetThroughput>("window=300").is_err());

        let page: ListDeadLetters =
            serde_urlencoded::from_str("page_size=5&pageToken=abc").expect("both spellings");
        assert_eq!(page.page_size, Some(5));
        assert_eq!(page.page_token.as_deref(), Some("abc"));
    }
}
