//! What `fq` prints.
//!
//! Two renderings. The default is an aligned table, built the same way the Node
//! SDK's CLI builds its own — column widths from the widest cell, two spaces
//! between columns, `(none)` for an empty result — so the two surfaces look
//! alike.
//!
//! `--json` is **proto3 JSON**, the same encoding the server's own `/v1` facade
//! returns: `lowerCamelCase` keys, RFC 3339 instants, durations as a decimal
//! number of seconds with an `s`, `bytes` as base64, enums by name, `int64` as
//! a string, and an absent optional omitted rather than written as `null`. One
//! `jq` expression therefore works against `fq jobs get X --json` and against
//! `curl …/v1/jobs/X` alike.
//!
//! That makes this a second implementation of a rendering the server already
//! has, and a second implementation of a shared format drifts unless something
//! holds it. `crates/flexiq-server/tests/grpc_cli.rs` holds it: it renders one
//! `Job` both ways and asserts the two objects are equal.

use anyhow::{anyhow, Result};
use base64::Engine as _;
use chrono::{DateTime, SecondsFormat, Utc};
use prost_types::{Duration as ProtoDuration, Timestamp};
use serde_json::{Map, Value};

use crate::pb;

/// Nanoseconds in a second.
const NANOS_PER_SECOND: u32 = 1_000_000_000;

/// proto3 JSON writes `bytes` in the standard alphabet, padded.
const BASE64: base64::engine::general_purpose::GeneralPurpose =
    base64::engine::general_purpose::STANDARD;

/// The columns of a job listing.
pub const JOB_COLUMNS: [&str; 7] = [
    "id", "queue", "task", "status", "priority", "retries", "created",
];

/// The six short status names, for an error that has to list them.
const SHORT_STATUS_NAMES: [&str; 6] = [
    "pending",
    "running",
    "complete",
    "failed",
    "dead",
    "cancelled",
];

// ── Tables ───────────────────────────────────────────────────────────

/// Render `rows` under `columns`, aligned.
pub fn table(columns: &[&str], rows: &[Vec<String>]) -> String {
    if rows.is_empty() {
        return "(none)\n".to_string();
    }
    let widths: Vec<usize> = columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            rows.iter()
                .filter_map(|row| row.get(index))
                .map(|cell| cell.chars().count())
                .chain(std::iter::once(column.chars().count()))
                .max()
                .unwrap_or_default()
        })
        .collect();

    let render = |cells: &[String]| -> String {
        cells
            .iter()
            .enumerate()
            .map(|(index, cell)| {
                let width = widths.get(index).copied().unwrap_or_default();
                format!("{cell:<width$}")
            })
            .collect::<Vec<_>>()
            .join("  ")
    };

    let mut lines = vec![
        render(&columns.iter().map(|c| (*c).to_string()).collect::<Vec<_>>()),
        widths
            .iter()
            .map(|width| "-".repeat(*width))
            .collect::<Vec<_>>()
            .join("  "),
    ];
    lines.extend(rows.iter().map(|row| render(row)));
    format!("{}\n", lines.join("\n"))
}

/// One job as a row of [`JOB_COLUMNS`].
pub fn job_row(job: &pb::Job) -> Vec<String> {
    vec![
        job.id.clone(),
        job.queue.clone(),
        job.task_name.clone(),
        status_name(job.status),
        job.priority.to_string(),
        format!("{}/{}", job.retry_count, job.max_retries),
        job.created_at
            .as_ref()
            .and_then(timestamp_to_json)
            .unwrap_or_default(),
    ]
}

/// The six queue counters as a two-column table.
pub fn queue_stats_rows(stats: &pb::QueueStatsResponse) -> Vec<Vec<String>> {
    [
        ("pending", stats.pending),
        ("running", stats.running),
        ("completed", stats.completed),
        ("failed", stats.failed),
        ("dead", stats.dead),
        ("cancelled", stats.cancelled),
    ]
    .into_iter()
    .map(|(state, count)| vec![state.to_string(), count.to_string()])
    .collect()
}

// ── Statuses ─────────────────────────────────────────────────────────

/// A status for a table cell: the short name, lowercased.
///
/// An unrecognised value renders as its number, which is what proto3 JSON does
/// and the honest answer — the contract's rule is that an unknown status is
/// **not terminal**, and a reader cannot apply it to a name this build invented.
pub fn status_name(status: i32) -> String {
    match pb::JobStatus::try_from(status) {
        Ok(known) => known
            .as_str_name()
            .trim_start_matches("JOB_STATUS_")
            .to_ascii_lowercase(),
        Err(_) => status.to_string(),
    }
}

/// Read a `--status` filter: the short name or the enum's own spelling, in any
/// case.
pub fn parse_status(text: &str) -> Result<pb::JobStatus> {
    let upper = text.to_ascii_uppercase();
    let full = if upper.starts_with("JOB_STATUS_") {
        upper
    } else {
        format!("JOB_STATUS_{upper}")
    };
    match pb::JobStatus::from_str_name(&full) {
        Some(pb::JobStatus::Unspecified) | None => Err(anyhow!(
            "`{text}` is not a status. Use one of: {}",
            SHORT_STATUS_NAMES.join(", ")
        )),
        Some(known) => Ok(known),
    }
}

// ── proto3 JSON ──────────────────────────────────────────────────────

/// `EnqueueResponse` as proto3 JSON.
pub fn enqueue_json(response: &pb::EnqueueResponse) -> Value {
    let mut object = Map::new();
    if let Some(job) = response.job.as_ref() {
        object.insert("job".to_string(), job_json(job));
    }
    object.insert("deduplicated".to_string(), response.deduplicated.into());
    Value::Object(object)
}

/// `ListJobsResponse` as proto3 JSON.
pub fn list_jobs_json(response: &pb::ListJobsResponse) -> Value {
    Value::Object(Map::from_iter([
        (
            "jobs".to_string(),
            response.jobs.iter().map(job_json).collect::<Value>(),
        ),
        (
            "nextPageToken".to_string(),
            response.next_page_token.clone().into(),
        ),
    ]))
}

/// A response carrying one optional job — `GetJobResponse` and
/// `CancelJobResponse` are the same shape.
pub fn job_envelope_json(job: Option<&pb::Job>) -> Value {
    let mut object = Map::new();
    if let Some(job) = job {
        object.insert("job".to_string(), job_json(job));
    }
    Value::Object(object)
}

/// `QueueStatsResponse` as proto3 JSON. Every counter is an `int64`, so every
/// counter is a string.
pub fn queue_stats_json(stats: &pb::QueueStatsResponse) -> Value {
    Value::Object(Map::from_iter([
        ("pending".to_string(), int64(stats.pending)),
        ("running".to_string(), int64(stats.running)),
        ("completed".to_string(), int64(stats.completed)),
        ("failed".to_string(), int64(stats.failed)),
        ("dead".to_string(), int64(stats.dead)),
        ("cancelled".to_string(), int64(stats.cancelled)),
    ]))
}

/// One `Job` as proto3 JSON.
///
/// Field for field with `flexiq_server::grpc::facade::json::response::job`; the
/// end-to-end suite asserts the two agree.
pub fn job_json(job: &pb::Job) -> Value {
    let mut object = Map::new();
    object.insert("id".to_string(), job.id.clone().into());
    object.insert("queue".to_string(), job.queue.clone().into());
    object.insert("taskName".to_string(), job.task_name.clone().into());
    object.insert("status".to_string(), status_json(job.status));
    object.insert("priority".to_string(), job.priority.into());
    insert_timestamp(&mut object, "createdAt", job.created_at.as_ref());
    insert_timestamp(&mut object, "scheduledAt", job.scheduled_at.as_ref());
    object.insert("retryCount".to_string(), job.retry_count.into());
    object.insert("maxRetries".to_string(), job.max_retries.into());
    if let Some(value) = job.timeout.as_ref() {
        object.insert("timeout".to_string(), duration_to_json(value).into());
    }
    object.insert("cancelRequested".to_string(), job.cancel_requested.into());
    object.insert("hasDeps".to_string(), job.has_deps.into());
    object.insert("namespace".to_string(), job.namespace.clone().into());
    insert_timestamp(&mut object, "startedAt", job.started_at.as_ref());
    insert_timestamp(&mut object, "completedAt", job.completed_at.as_ref());

    // The optional tail. Absent here means absent on the wire: a payload the
    // caller did not ask for and a result the task never produced are both
    // missing keys, and a zero-length one is a present empty string.
    if let Some(payload) = job.payload.as_ref() {
        object.insert("payload".to_string(), BASE64.encode(payload).into());
    }
    if let Some(result) = job.result.as_ref() {
        object.insert("result".to_string(), BASE64.encode(result).into());
    }
    if let Some(error) = job.error.as_ref() {
        object.insert("error".to_string(), error.clone().into());
    }
    if let Some(progress) = job.progress {
        object.insert("progress".to_string(), progress.into());
    }
    if let Some(metadata) = job.metadata.as_ref() {
        object.insert("metadata".to_string(), metadata.clone().into());
    }
    if let Some(notes) = job.notes.as_ref() {
        object.insert("notes".to_string(), notes.clone().into());
    }
    if let Some(unique_key) = job.unique_key.as_ref() {
        object.insert("uniqueKey".to_string(), unique_key.clone().into());
    }
    insert_timestamp(&mut object, "expiresAt", job.expires_at.as_ref());
    if let Some(value) = job.result_ttl.as_ref() {
        object.insert("resultTtl".to_string(), duration_to_json(value).into());
    }
    if let Some(debounce_key) = job.debounce_key.as_ref() {
        object.insert("debounceKey".to_string(), debounce_key.clone().into());
    }
    Value::Object(object)
}

/// A `JobStatus` by name, or by number when this build does not know it.
fn status_json(status: i32) -> Value {
    match pb::JobStatus::try_from(status) {
        Ok(known) => known.as_str_name().into(),
        Err(_) => status.into(),
    }
}

/// An `int64`, as a string.
fn int64(value: i64) -> Value {
    value.to_string().into()
}

/// Write a timestamp field, if the message holds one that renders.
fn insert_timestamp(object: &mut Map<String, Value>, key: &str, value: Option<&Timestamp>) {
    if let Some(text) = value.and_then(timestamp_to_json) {
        object.insert(key.to_string(), text.into());
    }
}

/// An instant as RFC 3339.
///
/// `SecondsFormat::AutoSi` is exactly proto3 JSON's rule — zero, three, six or
/// nine fractional digits, whichever shows every non-zero one. `None` means the
/// message is not a valid `Timestamp`, in which case omitting the field is the
/// only answer that is not actively wrong.
fn timestamp_to_json(value: &Timestamp) -> Option<String> {
    let nanos = u32::try_from(value.nanos).ok()?;
    if nanos >= NANOS_PER_SECOND {
        return None;
    }
    DateTime::<Utc>::from_timestamp(value.seconds, nanos)
        .map(|moment| moment.to_rfc3339_opts(SecondsFormat::AutoSi, true))
}

/// A duration as seconds with an `s`, to zero, three, six or nine decimals.
///
/// The two halves are summed before being split again: a negative duration can
/// arrive as a negative `seconds` beside a positive `nanos`, and formatting
/// that pair where it sits would move the value by a second.
fn duration_to_json(value: &ProtoDuration) -> String {
    let total = i128::from(value.seconds) * i128::from(NANOS_PER_SECOND) + i128::from(value.nanos);
    let sign = if total < 0 { "-" } else { "" };
    let magnitude = total.unsigned_abs();
    let seconds = magnitude / u128::from(NANOS_PER_SECOND);
    let nanos = (magnitude % u128::from(NANOS_PER_SECOND)) as u32;
    if nanos == 0 {
        format!("{sign}{seconds}s")
    } else if nanos.is_multiple_of(1_000_000) {
        format!("{sign}{seconds}.{:03}s", nanos / 1_000_000)
    } else if nanos.is_multiple_of(1_000) {
        format!("{sign}{seconds}.{:06}s", nanos / 1_000)
    } else {
        format!("{sign}{seconds}.{nanos:09}s")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_empty_table_says_so() {
        assert_eq!(table(&["id"], &[]), "(none)\n");
    }

    #[test]
    fn columns_are_padded_to_the_widest_cell() {
        let rendered = table(
            &["id", "queue"],
            &[
                vec!["a".into(), "default".into()],
                vec!["bbbb".into(), "q".into()],
            ],
        );
        let lines: Vec<_> = rendered.lines().collect();
        assert_eq!(lines[0], "id    queue  ");
        assert_eq!(lines[1], "----  -------");
        assert_eq!(lines[2], "a     default");
        assert_eq!(lines[3], "bbbb  q      ");
    }

    #[test]
    fn a_job_renders_as_proto3_json() {
        let job = pb::Job {
            id: "job-1".into(),
            queue: "default".into(),
            task_name: "send_email".into(),
            status: pb::JobStatus::Pending as i32,
            created_at: Some(Timestamp {
                seconds: 1_757_500_000,
                nanos: 0,
            }),
            timeout: Some(ProtoDuration {
                seconds: 30,
                nanos: 0,
            }),
            payload: Some(vec![0x02, 0x82]),
            ..Default::default()
        };
        let rendered = job_json(&job);
        assert_eq!(rendered["taskName"], "send_email");
        assert_eq!(rendered["status"], "JOB_STATUS_PENDING");
        assert_eq!(rendered["createdAt"], "2025-09-10T10:26:40Z");
        assert_eq!(rendered["timeout"], "30s");
        assert_eq!(rendered["payload"], "AoI=");
        // int32 stays a number; only int64 becomes a string.
        assert_eq!(rendered["priority"], 0);
    }

    /// Absent and empty are different answers on `payload` and `result`, so an
    /// absent one must not become `null`.
    #[test]
    fn an_absent_optional_is_omitted_not_null() {
        let job = pb::Job {
            id: "job-1".into(),
            ..Default::default()
        };
        let rendered = job_json(&job);
        assert!(rendered.get("payload").is_none());
        assert!(rendered.get("result").is_none());
        assert!(rendered.get("uniqueKey").is_none());
    }

    #[test]
    fn a_zero_length_payload_is_present_and_empty() {
        let job = pb::Job {
            payload: Some(Vec::new()),
            ..Default::default()
        };
        assert_eq!(job_json(&job)["payload"], "");
    }

    #[test]
    fn a_sub_second_duration_keeps_its_decimals() {
        assert_eq!(
            duration_to_json(&ProtoDuration {
                seconds: 1,
                nanos: 500_000_000
            }),
            "1.500s"
        );
        assert_eq!(
            duration_to_json(&ProtoDuration {
                seconds: 0,
                nanos: 1
            }),
            "0.000000001s"
        );
    }

    #[test]
    fn counts_are_strings_because_they_are_int64() {
        let stats = pb::QueueStatsResponse {
            pending: 3,
            ..Default::default()
        };
        assert_eq!(queue_stats_json(&stats)["pending"], "3");
    }

    #[test]
    fn a_status_reads_short_or_long_and_is_case_insensitive() {
        assert_eq!(
            parse_status("pending").expect("known"),
            pb::JobStatus::Pending
        );
        assert_eq!(parse_status("DEAD").expect("known"), pb::JobStatus::Dead);
        assert_eq!(
            parse_status("JOB_STATUS_CANCELLED").expect("known"),
            pb::JobStatus::Cancelled
        );
        let error = parse_status("gone").expect_err("unknown");
        assert!(error.to_string().contains("pending"), "{error}");
    }

    /// Zero is the proto's `_UNSPECIFIED`, which is not a filter a caller may
    /// ask for.
    #[test]
    fn unspecified_is_not_a_status_a_caller_can_name() {
        assert!(parse_status("unspecified").is_err());
    }

    #[test]
    fn a_table_status_is_the_short_name() {
        assert_eq!(status_name(pb::JobStatus::Complete as i32), "complete");
        assert_eq!(status_name(99), "99");
    }
}
