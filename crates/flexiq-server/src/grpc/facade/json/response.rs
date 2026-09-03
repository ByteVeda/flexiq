//! Writing a `flexiq.v1` response as proto3 JSON.
//!
//! Built as a `serde_json::Value` rather than through a mirror struct, for two
//! reasons that are the same reason twice. Presence is a decision per field
//! here — a `payload` the caller did not ask for is **absent**, not `null` and
//! not `""` — and a 64-bit integer is written as a string, because a JSON
//! number is a double and a job count is not. Both are easy to state and hard
//! to express in one derive.
//!
//! What that costs is the compiler's help: a field left out of one of these
//! functions is not a build error. What replaces it is the test at the bottom,
//! which reads `contracts/descriptor.binpb` and asserts that a fully populated
//! message emits **exactly** the message's own JSON names — so a field added to
//! the `.proto` fails here until it is written, and a key that is not a field
//! fails here immediately.
//!
//! The rule, stated once: a field with explicit presence is written only when
//! it is set; everything else is always written, at its zero if that is what it
//! holds.

use serde_json::{Map, Value};

use super::wkt::{bytes_to_json, duration_to_json, timestamp_to_json};
use crate::grpc::facade::error;
use crate::grpc::pb;

/// `EnqueueResponse`.
pub fn enqueue(response: &pb::EnqueueResponse) -> Value {
    let mut object = Map::new();
    if let Some(value) = response.job.as_ref() {
        object.insert("job".to_string(), job(value));
    }
    object.insert("deduplicated".to_string(), response.deduplicated.into());
    Value::Object(object)
}

/// `EnqueueBatchResponse`, one result per input item in input order.
pub fn enqueue_batch(response: &pb::EnqueueBatchResponse) -> Value {
    let results = response
        .results
        .iter()
        .map(|result| {
            let mut object = Map::new();
            match result.outcome.as_ref() {
                Some(pb::enqueue_batch_item_result::Outcome::Enqueued(item)) => {
                    object.insert("enqueued".to_string(), enqueue(item));
                }
                Some(pb::enqueue_batch_item_result::Outcome::Error(status)) => {
                    // The same object an RPC-level failure carries, so one
                    // failure reads the same whether it arrived alone or as one
                    // item of a batch. Only the `{"error": …}` wrapper differs,
                    // and that belongs to a failed response rather than to a
                    // successful one describing an item that did not land.
                    object.insert("error".to_string(), error::status_json(status));
                }
                None => {}
            }
            Value::Object(object)
        })
        .collect();
    Value::Object(Map::from_iter([("results".to_string(), results)]))
}

/// `GetJobResponse`.
pub fn get_job(response: &pb::GetJobResponse) -> Value {
    let mut object = Map::new();
    if let Some(value) = response.job.as_ref() {
        object.insert("job".to_string(), job(value));
    }
    Value::Object(object)
}

/// `ListJobsResponse`.
pub fn list_jobs(response: &pb::ListJobsResponse) -> Value {
    Value::Object(Map::from_iter([
        (
            "jobs".to_string(),
            response.jobs.iter().map(job).collect::<Value>(),
        ),
        (
            "nextPageToken".to_string(),
            response.next_page_token.clone().into(),
        ),
    ]))
}

/// `CancelJobResponse`.
pub fn cancel_job(response: &pb::CancelJobResponse) -> Value {
    let mut object = Map::new();
    if let Some(value) = response.job.as_ref() {
        object.insert("job".to_string(), job(value));
    }
    Value::Object(object)
}

/// `QueueStatsResponse`. Every counter is an `int64`, so every counter is a
/// string.
pub fn queue_stats(response: &pb::QueueStatsResponse) -> Value {
    Value::Object(Map::from_iter([
        ("pending".to_string(), int64(response.pending)),
        ("running".to_string(), int64(response.running)),
        ("completed".to_string(), int64(response.completed)),
        ("failed".to_string(), int64(response.failed)),
        ("dead".to_string(), int64(response.dead)),
        ("cancelled".to_string(), int64(response.cancelled)),
    ]))
}

/// `SubmitWorkflowResponse`.
pub fn submit_workflow(response: &pb::SubmitWorkflowResponse) -> Value {
    Value::Object(Map::from_iter([(
        "runId".to_string(),
        response.run_id.clone().into(),
    )]))
}

/// `GetWorkflowRunResponse`.
pub fn get_workflow_run(response: &pb::GetWorkflowRunResponse) -> Value {
    let mut object = Map::new();
    if let Some(value) = response.run.as_ref() {
        object.insert("run".to_string(), workflow_run(value));
    }
    object.insert(
        "nodes".to_string(),
        response.nodes.iter().map(workflow_node).collect::<Value>(),
    );
    Value::Object(object)
}

/// One `WorkflowRun`.
fn workflow_run(run: &pb::WorkflowRun) -> Value {
    let mut object = Map::new();
    object.insert("id".to_string(), run.id.clone().into());
    object.insert("definitionId".to_string(), run.definition_id.clone().into());
    object.insert("state".to_string(), workflow_state(run.state));
    insert_timestamp(&mut object, "startedAt", run.started_at.as_ref());
    insert_timestamp(&mut object, "completedAt", run.completed_at.as_ref());
    if let Some(error) = run.error.as_ref() {
        object.insert("error".to_string(), error.clone().into());
    }
    if let Some(parent_run_id) = run.parent_run_id.as_ref() {
        object.insert("parentRunId".to_string(), parent_run_id.clone().into());
    }
    if let Some(parent_node_name) = run.parent_node_name.as_ref() {
        object.insert(
            "parentNodeName".to_string(),
            parent_node_name.clone().into(),
        );
    }
    insert_timestamp(&mut object, "createdAt", run.created_at.as_ref());
    Value::Object(object)
}

/// One `WorkflowNode`.
fn workflow_node(node: &pb::WorkflowNode) -> Value {
    let mut object = Map::new();
    object.insert("name".to_string(), node.name.clone().into());
    object.insert("status".to_string(), workflow_node_status(node.status));
    if let Some(job_id) = node.job_id.as_ref() {
        object.insert("jobId".to_string(), job_id.clone().into());
    }
    insert_timestamp(&mut object, "startedAt", node.started_at.as_ref());
    insert_timestamp(&mut object, "completedAt", node.completed_at.as_ref());
    if let Some(error) = node.error.as_ref() {
        object.insert("error".to_string(), error.clone().into());
    }
    Value::Object(object)
}

/// A `WorkflowState` by name, or by number when this build does not know it.
/// Same rationale as [`status`].
fn workflow_state(state: i32) -> Value {
    match pb::WorkflowState::try_from(state) {
        Ok(known) => known.as_str_name().into(),
        Err(_) => state.into(),
    }
}

/// A `WorkflowNodeStatus` by name, or by number. Same rationale as [`status`].
fn workflow_node_status(status: i32) -> Value {
    match pb::WorkflowNodeStatus::try_from(status) {
        Ok(known) => known.as_str_name().into(),
        Err(_) => status.into(),
    }
}

/// One `Job`.
pub fn job(job: &pb::Job) -> Value {
    let mut object = Map::new();
    object.insert("id".to_string(), job.id.clone().into());
    object.insert("queue".to_string(), job.queue.clone().into());
    object.insert("taskName".to_string(), job.task_name.clone().into());
    object.insert("status".to_string(), status(job.status));
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
        object.insert("payload".to_string(), bytes_to_json(payload).into());
    }
    if let Some(result) = job.result.as_ref() {
        object.insert("result".to_string(), bytes_to_json(result).into());
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
///
/// A number is what proto3 JSON writes for an unrecognised enum value, and it
/// is the honest answer: the reader is told what the field holds and is left to
/// apply the contract's rule, which is that an unknown status is **not
/// terminal**.
fn status(status: i32) -> Value {
    match pb::JobStatus::try_from(status) {
        Ok(known) => known.as_str_name().into(),
        Err(_) => status.into(),
    }
}

/// A 64-bit integer, as a string.
fn int64(value: i64) -> Value {
    value.to_string().into()
}

/// Write a timestamp field, if the message holds one this build can render.
fn insert_timestamp(
    object: &mut Map<String, Value>,
    key: &str,
    value: Option<&prost_types::Timestamp>,
) {
    if let Some(text) = value.and_then(timestamp_to_json) {
        object.insert(key.to_string(), text.into());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::grpc::facade::descriptor;

    /// Every field of `Job` set, so the assertion below is about the whole
    /// message rather than about the half a fixture happened to populate.
    fn populated_job() -> pb::Job {
        pb::Job {
            id: "01924f".to_string(),
            queue: "emails".to_string(),
            task_name: "send_email".to_string(),
            status: pb::JobStatus::Running as i32,
            priority: 5,
            created_at: Some(crate::grpc::producer::convert::timestamp(1_756_900_000_000)),
            scheduled_at: Some(crate::grpc::producer::convert::timestamp(1_756_900_001_000)),
            retry_count: 1,
            max_retries: 3,
            timeout: Some(crate::grpc::producer::convert::duration(30_000)),
            cancel_requested: true,
            has_deps: true,
            namespace: "prod".to_string(),
            started_at: Some(crate::grpc::producer::convert::timestamp(1_756_900_002_000)),
            completed_at: Some(crate::grpc::producer::convert::timestamp(1_756_900_003_000)),
            payload: Some(vec![0x02, 0x82]),
            result: Some(Vec::new()),
            error: Some("{\"errtype\":\"ValueError\"}".to_string()),
            progress: Some(50),
            metadata: Some("{\"a\":1}".to_string()),
            notes: Some("a note".to_string()),
            unique_key: Some("welcome:42".to_string()),
            expires_at: Some(crate::grpc::producer::convert::timestamp(1_756_900_004_000)),
            result_ttl: Some(crate::grpc::producer::convert::duration(3_600_000)),
            debounce_key: Some("burst".to_string()),
        }
    }

    /// The drift check this module exists to make possible: a fully populated
    /// message emits exactly the JSON names the contract gives it. A field
    /// added to the `.proto` and forgotten here fails; a key spelled by hand
    /// and not by the contract fails.
    fn assert_names(message: &str, rendered: &Value) {
        let expected = descriptor::json_names(message);
        let emitted: std::collections::BTreeSet<String> = rendered
            .as_object()
            .expect("a message renders as an object")
            .keys()
            .cloned()
            .collect();
        assert_eq!(emitted, expected, "{message} drifted from the contract");
    }

    #[test]
    fn a_populated_job_carries_every_field_the_contract_names() {
        assert_names("Job", &job(&populated_job()));
    }

    #[test]
    fn every_response_carries_every_field_the_contract_names() {
        let job = populated_job();
        assert_names(
            "EnqueueResponse",
            &enqueue(&pb::EnqueueResponse {
                job: Some(job.clone()),
                deduplicated: true,
            }),
        );
        assert_names(
            "GetJobResponse",
            &get_job(&pb::GetJobResponse {
                job: Some(job.clone()),
            }),
        );
        assert_names(
            "CancelJobResponse",
            &cancel_job(&pb::CancelJobResponse {
                job: Some(job.clone()),
            }),
        );
        assert_names(
            "ListJobsResponse",
            &list_jobs(&pb::ListJobsResponse {
                jobs: vec![job.clone()],
                next_page_token: "cursor".to_string(),
            }),
        );
        assert_names(
            "QueueStatsResponse",
            &queue_stats(&pb::QueueStatsResponse {
                pending: 1,
                running: 2,
                completed: 3,
                failed: 4,
                dead: 5,
                cancelled: 6,
            }),
        );
        assert_names(
            "EnqueueBatchResponse",
            &enqueue_batch(&pb::EnqueueBatchResponse {
                results: Vec::new(),
            }),
        );
    }

    /// A oneof renders one arm at a time, so each case is pinned to the key it
    /// owes. Asserting only that the emitted key is *one of* the legal ones
    /// would pass an `Error` arm that wrote `"enqueued"` — the arm names are
    /// hand-spelled in [`enqueue_batch`], which is exactly the mistake
    /// available to make there. The descriptor check stays beside it, because
    /// nothing else compares those spellings with the contract: the message
    /// above renders an empty `results` list, so the item's keys never reach
    /// [`assert_names`].
    #[test]
    fn a_batch_item_carries_only_names_the_contract_gives_it() {
        let names = descriptor::json_names("EnqueueBatchItemResult");
        for (outcome, expected) in [
            (
                pb::enqueue_batch_item_result::Outcome::Enqueued(pb::EnqueueResponse {
                    job: Some(populated_job()),
                    deduplicated: false,
                }),
                "enqueued",
            ),
            (
                pb::enqueue_batch_item_result::Outcome::Error(tonic_types::pb::Status::default()),
                "error",
            ),
        ] {
            let rendered = enqueue_batch(&pb::EnqueueBatchResponse {
                results: vec![pb::EnqueueBatchItemResult {
                    outcome: Some(outcome),
                }],
            });
            let emitted: std::collections::BTreeSet<String> = rendered["results"][0]
                .as_object()
                .expect("an item renders as an object")
                .keys()
                .cloned()
                .collect();
            assert_eq!(
                emitted,
                std::collections::BTreeSet::from([expected.to_string()]),
                "a oneof renders exactly its own arm"
            );
            assert!(
                emitted.is_subset(&names),
                "EnqueueBatchItemResult drifted from the contract: {emitted:?}"
            );
        }
    }

    /// The distinction the frame protocol already draws and the wire inherits:
    /// a blob nobody asked for is a missing key, and an empty one is present.
    #[test]
    fn an_absent_blob_is_a_missing_key_and_an_empty_one_is_not() {
        let mut job = populated_job();
        job.payload = None;
        job.result = Some(Vec::new());
        let rendered = super::job(&job);
        let object = rendered.as_object().expect("an object");
        assert!(!object.contains_key("payload"));
        assert_eq!(object["result"], Value::String(String::new()));
    }

    #[test]
    fn a_counter_is_a_string_because_a_json_number_is_a_double() {
        let rendered = queue_stats(&pb::QueueStatsResponse {
            pending: 9_007_199_254_740_993,
            ..Default::default()
        });
        assert_eq!(
            rendered["pending"],
            Value::String("9007199254740993".into())
        );
    }

    #[test]
    fn a_status_is_its_name_and_an_unknown_one_is_its_number() {
        let mut job = populated_job();
        assert_eq!(
            super::job(&job)["status"],
            Value::String("JOB_STATUS_RUNNING".into())
        );
        job.status = 99;
        assert_eq!(super::job(&job)["status"], Value::from(99));
    }

    #[test]
    fn a_batch_item_error_is_rendered_the_same_way_an_rpc_failure_is() {
        let failure: tonic_types::pb::Status = crate::grpc::status::WireError::from_queue_error(
            &flexiq_core::error::QueueError::QueueFull {
                queue: "emails".to_string(),
                pending: 11,
                cap: 10,
            },
        )
        .at_index(1)
        .into();
        let rendered = enqueue_batch(&pb::EnqueueBatchResponse {
            results: vec![pb::EnqueueBatchItemResult {
                outcome: Some(pb::enqueue_batch_item_result::Outcome::Error(failure)),
            }],
        });
        let item = &rendered["results"][0]["error"];
        assert_eq!(item["status"], Value::String("RESOURCE_EXHAUSTED".into()));
        assert_eq!(
            item["details"][0]["reason"],
            Value::String("QUEUE_FULL".into())
        );
        assert_eq!(
            item["details"][0]["metadata"]["index"],
            Value::String("1".into())
        );
    }
}
