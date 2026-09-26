//! What `fq tail` prints: one line per `WatchJobsResponse`.
//!
//! A stream has no table to align, so the default rendering is one line per
//! item and `--json` is one compact proto3 JSON object per line, which `jq`
//! reads as a stream.

use serde_json::{Map, Value};

use super::{insert_timestamp, status_name, timestamp_to_json};
use crate::pb::{self, watch_jobs_response::Item};
use crate::safe::escape;

/// One item as a line of text, or `None` for an arm this build does not know.
pub fn watch_line(response: &pb::WatchJobsResponse) -> Option<String> {
    match response.item.as_ref()? {
        Item::NotFoundJobId(id) => Some(format!("{}  not found", escape(id))),
        Item::Transition(t) => {
            let mut line = format!(
                "{}  {}  {}  {}  {} -> {}  attempt {}",
                t.time
                    .as_ref()
                    .and_then(timestamp_to_json)
                    .unwrap_or_default(),
                escape(&t.job_id),
                escape(&t.queue),
                escape(&t.task_name),
                kind_name(t.kind),
                status_name(t.status),
                t.attempt,
            );
            if let Some(reason) = &t.reason {
                line.push_str(&format!("  reason: {}", escape(reason)));
            }
            if let Some(error) = &t.error {
                line.push_str(&format!("  error: {}", escape(error)));
            }
            Some(line)
        }
    }
}

/// `JOB_TRANSITION_KIND_STARTED` as `started`; an unknown value as its number.
fn kind_name(kind: i32) -> String {
    match pb::JobTransitionKind::try_from(kind) {
        Ok(known) => known
            .as_str_name()
            .trim_start_matches("JOB_TRANSITION_KIND_")
            .to_ascii_lowercase(),
        Err(_) => kind.to_string(),
    }
}

/// `WatchJobsResponse` as proto3 JSON.
pub fn watch_json(response: &pb::WatchJobsResponse) -> Value {
    let mut object = Map::new();
    match response.item.as_ref() {
        Some(Item::Transition(t)) => {
            object.insert("transition".to_string(), transition_json(t));
        }
        Some(Item::NotFoundJobId(id)) => {
            object.insert("notFoundJobId".to_string(), id.clone().into());
        }
        None => {}
    }
    if !response.cursor.is_empty() {
        object.insert("cursor".to_string(), response.cursor.clone().into());
    }
    Value::Object(object)
}

fn transition_json(t: &pb::JobTransition) -> Value {
    let mut object = Map::new();
    object.insert("jobId".to_string(), t.job_id.clone().into());
    object.insert("queue".to_string(), t.queue.clone().into());
    object.insert("taskName".to_string(), t.task_name.clone().into());
    object.insert(
        "kind".to_string(),
        match pb::JobTransitionKind::try_from(t.kind) {
            Ok(known) => known.as_str_name().into(),
            Err(_) => t.kind.into(),
        },
    );
    object.insert(
        "status".to_string(),
        match pb::JobStatus::try_from(t.status) {
            Ok(known) => known.as_str_name().into(),
            Err(_) => t.status.into(),
        },
    );
    object.insert("attempt".to_string(), t.attempt.into());
    insert_timestamp(&mut object, "time", t.time.as_ref());
    object.insert("terminal".to_string(), t.terminal.into());
    if let Some(error) = &t.error {
        object.insert("error".to_string(), error.clone().into());
    }
    if let Some(reason) = &t.reason {
        object.insert("reason".to_string(), reason.clone().into());
    }
    if let Some(timed_out) = t.timed_out {
        object.insert("timedOut".to_string(), timed_out.into());
    }
    insert_timestamp(&mut object, "wakeAt", t.wake_at.as_ref());
    Value::Object(object)
}

#[cfg(test)]
mod tests {
    use prost_types::Timestamp;
    use serde_json::json;

    use super::*;

    fn completed() -> pb::WatchJobsResponse {
        pb::WatchJobsResponse {
            item: Some(Item::Transition(pb::JobTransition {
                job_id: "j1".into(),
                queue: "emails".into(),
                task_name: "send\x1b[2J".into(),
                kind: pb::JobTransitionKind::Dead as i32,
                status: pb::JobStatus::Dead as i32,
                attempt: 3,
                time: Some(Timestamp {
                    seconds: 1_700_000_000,
                    nanos: 123_000_000,
                }),
                terminal: true,
                error: Some("boom".into()),
                reason: None,
                timed_out: Some(false),
                wake_at: None,
            })),
            cursor: "c1".into(),
        }
    }

    #[test]
    fn a_transition_is_one_escaped_line() {
        assert_eq!(
            watch_line(&completed()).unwrap(),
            "2023-11-14T22:13:20.123Z  j1  emails  send\\u{1b}[2J  dead -> dead  attempt 3  error: boom"
        );
        let missing = pb::WatchJobsResponse {
            item: Some(Item::NotFoundJobId("nope".into())),
            cursor: String::new(),
        };
        assert_eq!(watch_line(&missing).unwrap(), "nope  not found");
        assert_eq!(watch_line(&pb::WatchJobsResponse::default()), None);
    }

    #[test]
    fn json_is_proto3_json() {
        assert_eq!(
            watch_json(&completed()),
            json!({
                "transition": {
                    "jobId": "j1",
                    "queue": "emails",
                    "taskName": "send\u{1b}[2J",
                    "kind": "JOB_TRANSITION_KIND_DEAD",
                    "status": "JOB_STATUS_DEAD",
                    "attempt": 3,
                    "time": "2023-11-14T22:13:20.123Z",
                    "terminal": true,
                    "error": "boom",
                    "timedOut": false,
                },
                "cursor": "c1",
            })
        );
    }
}
