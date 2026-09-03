//! Reading a `flexiq.v1` request out of a JSON body or a query string.
//!
//! These are serde structs rather than hand-rolled readers, for the error
//! messages: a client that writes `taskname` instead of `taskName` is told so
//! by name, and that is worth more on a door people reach with `curl` than any
//! amount of leniency. Which is why **unknown fields are refused**, exactly as
//! proto3 JSON's default parser refuses them — a typo that is silently ignored
//! enqueues a job the caller did not describe.
//!
//! Every field carries both spellings: the lowerCamelCase name proto3 JSON
//! writes, and the field's own name from the `.proto`. Accepting both is what
//! the specification requires of a parser, and it is why the field *names* are
//! frozen alongside the numbers (design doc D4) — a rename is invisible to
//! binary protobuf and fatal to a client that has only these.
//!
//! Nothing here validates. `task_name` being empty, a `debounce` block with no
//! window, both body arms at once — the first two are refused by the same code
//! that refuses them for a gRPC caller, so the two doors cannot come to
//! different conclusions about one request.

use std::collections::BTreeMap;

use serde::Deserialize;

use super::wkt::{JsonBytes, JsonDuration, JsonInt64, JsonTimestamp, JsonValue};
use crate::grpc::pb;

/// `POST /v1/jobs` — one `EnqueueRequest`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Enqueue {
    #[serde(default, alias = "task_name")]
    pub task_name: String,
    /// The `raw` arm of `body`: the payload envelope, base64.
    #[serde(default)]
    pub raw: Option<JsonBytes>,
    /// The `structured` arm of `body`.
    #[serde(default)]
    pub structured: Option<Structured>,
    #[serde(default)]
    pub options: Option<Options>,
}

impl Enqueue {
    /// The request message, or the one shape a JSON body can get wrong that the
    /// protobuf encoding cannot.
    pub fn into_message(self) -> Result<pb::EnqueueRequest, String> {
        // A oneof holds one arm. In protobuf that is structural; in JSON the
        // arms are sibling keys, so two of them is a body only this door can
        // see and only this door can refuse.
        let body = match (self.raw, self.structured) {
            (Some(_), Some(_)) => {
                return Err(
                    "`raw` and `structured` are the two arms of one field; send one of them"
                        .to_string(),
                )
            }
            (Some(raw), None) => Some(pb::enqueue_request::Body::Raw(raw.0)),
            (None, Some(structured)) => Some(pb::enqueue_request::Body::Structured(
                structured.into_message()?,
            )),
            (None, None) => None,
        };
        Ok(pb::EnqueueRequest {
            task_name: self.task_name,
            body,
            options: self.options.map(Options::into_message).transpose()?,
        })
    }
}

/// `POST /v1/jobs:batchEnqueue` — an `EnqueueBatchRequest`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct EnqueueBatch {
    #[serde(default)]
    pub items: Vec<Enqueue>,
}

impl EnqueueBatch {
    /// The request message.
    pub fn into_message(self) -> Result<pb::EnqueueBatchRequest, String> {
        Ok(pb::EnqueueBatchRequest {
            items: self
                .items
                .into_iter()
                .map(Enqueue::into_message)
                .collect::<Result<_, _>>()?,
        })
    }
}

/// `StructuredArgs`: the call the server encodes into the payload envelope.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Structured {
    #[serde(default)]
    pub args: Vec<JsonValue>,
    #[serde(default)]
    pub kwargs: BTreeMap<String, JsonValue>,
}

impl Structured {
    fn into_message(self) -> Result<pb::StructuredArgs, String> {
        Ok(pb::StructuredArgs {
            args: self.args.into_iter().map(|value| value.0).collect(),
            kwargs: self
                .kwargs
                .into_iter()
                .map(|(key, value)| (key, value.0))
                .collect(),
        })
    }
}

/// `EnqueueOptions`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Options {
    #[serde(default)]
    pub queue: String,
    #[serde(default)]
    pub priority: i32,
    #[serde(default, alias = "max_retries")]
    pub max_retries: i32,
    #[serde(default, alias = "scheduled_at")]
    pub scheduled_at: Option<JsonTimestamp>,
    #[serde(default)]
    pub timeout: Option<JsonDuration>,
    #[serde(default, alias = "unique_key")]
    pub unique_key: Option<String>,
    #[serde(default)]
    pub metadata: Option<String>,
    #[serde(default)]
    pub notes: Option<String>,
    #[serde(default, alias = "depends_on")]
    pub depends_on: Vec<String>,
    #[serde(default, alias = "expires_at")]
    pub expires_at: Option<JsonTimestamp>,
    #[serde(default, alias = "result_ttl")]
    pub result_ttl: Option<JsonDuration>,
    #[serde(default)]
    pub debounce: Option<Debounce>,
}

impl Options {
    fn into_message(self) -> Result<pb::EnqueueOptions, String> {
        Ok(pb::EnqueueOptions {
            queue: self.queue,
            priority: self.priority,
            max_retries: self.max_retries,
            scheduled_at: self.scheduled_at.map(|value| value.0),
            timeout: self.timeout.map(|value| value.0),
            unique_key: self.unique_key,
            metadata: self.metadata,
            notes: self.notes,
            depends_on: self.depends_on,
            expires_at: self.expires_at.map(|value| value.0),
            result_ttl: self.result_ttl.map(|value| value.0),
            debounce: self.debounce.map(Debounce::into_message),
        })
    }
}

/// `Debounce`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Debounce {
    #[serde(default)]
    pub key: String,
    #[serde(default)]
    pub window: Option<JsonDuration>,
    #[serde(default, alias = "max_wait")]
    pub max_wait: Option<JsonDuration>,
    #[serde(default, alias = "replace_payload")]
    pub replace_payload: bool,
    #[serde(default, alias = "max_pending")]
    pub max_pending: Option<JsonInt64>,
}

impl Debounce {
    fn into_message(self) -> pb::Debounce {
        pb::Debounce {
            key: self.key,
            window: self.window.map(|value| value.0),
            max_wait: self.max_wait.map(|value| value.0),
            replace_payload: self.replace_payload,
            max_pending: self.max_pending.map(|value| value.0),
        }
    }
}

/// `GET /v1/jobs/{job_id}` — the two blob switches, as query parameters.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetJob {
    #[serde(default, alias = "include_payload")]
    pub include_payload: bool,
    #[serde(default, alias = "include_result")]
    pub include_result: bool,
}

impl GetJob {
    /// The request message. The id comes from the path, not from the query.
    pub fn into_message(self, job_id: String) -> pb::GetJobRequest {
        pb::GetJobRequest {
            job_id,
            include_payload: self.include_payload,
            include_result: self.include_result,
        }
    }
}

/// `GET /v1/jobs` — the filters and the page cursor, as query parameters.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ListJobs {
    /// The `JobStatus` name, as it is spelled in the enum.
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub queue: Option<String>,
    #[serde(default, alias = "task_name")]
    pub task_name: Option<String>,
    #[serde(default, alias = "page_size")]
    pub page_size: Option<i32>,
    #[serde(default, alias = "page_token")]
    pub page_token: Option<String>,
}

impl ListJobs {
    /// The request message, or the reason the status filter names nothing.
    pub fn into_message(self) -> Result<pb::ListJobsRequest, String> {
        let status = self.status.as_deref().map(status_from_name).transpose()?;
        Ok(pb::ListJobsRequest {
            status,
            queue: self.queue,
            task_name: self.task_name,
            page_size: self.page_size.unwrap_or_default(),
            page_token: self.page_token.unwrap_or_default(),
        })
    }
}

/// `POST /v1/workflows` — a `SubmitWorkflowRequest`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SubmitWorkflow {
    #[serde(default)]
    pub name: String,
    pub graph: Graph,
    #[serde(default, alias = "params_json")]
    pub params_json: Option<String>,
}

impl SubmitWorkflow {
    pub fn into_message(self) -> Result<pb::SubmitWorkflowRequest, String> {
        Ok(pb::SubmitWorkflowRequest {
            name: self.name,
            graph: Some(self.graph.into_message()?),
            params_json: self.params_json,
        })
    }
}

/// `WorkflowGraph`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Graph {
    #[serde(default)]
    pub nodes: Vec<GraphNode>,
    #[serde(default)]
    pub edges: Vec<GraphEdge>,
    #[serde(default, alias = "node_configs")]
    pub node_configs: Vec<NodeConfig>,
}

impl Graph {
    fn into_message(self) -> Result<pb::WorkflowGraph, String> {
        Ok(pb::WorkflowGraph {
            nodes: self
                .nodes
                .into_iter()
                .map(|node| pb::WorkflowGraphNode { name: node.name })
                .collect(),
            edges: self
                .edges
                .into_iter()
                .map(|edge| pb::WorkflowGraphEdge {
                    from: edge.from,
                    to: edge.to,
                })
                .collect(),
            node_configs: self
                .node_configs
                .into_iter()
                .map(NodeConfig::into_message)
                .collect::<Result<_, _>>()?,
        })
    }
}

/// `WorkflowGraphNode`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GraphNode {
    #[serde(default)]
    pub name: String,
}

/// `WorkflowGraphEdge`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GraphEdge {
    #[serde(default)]
    pub from: String,
    #[serde(default)]
    pub to: String,
}

/// `WorkflowNodeConfig`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct NodeConfig {
    #[serde(default)]
    pub name: String,
    #[serde(default, alias = "task_name")]
    pub task_name: String,
    #[serde(default)]
    pub queue: Option<String>,
    #[serde(default)]
    pub raw: Option<JsonBytes>,
    #[serde(default)]
    pub structured: Option<Structured>,
    #[serde(default, alias = "max_retries")]
    pub max_retries: Option<i32>,
    #[serde(default)]
    pub timeout: Option<JsonDuration>,
    #[serde(default)]
    pub priority: Option<i32>,
    #[serde(default)]
    pub condition: Option<String>,
    #[serde(default)]
    pub gate: Option<Gate>,
    #[serde(default)]
    pub cache: Option<Cache>,
    #[serde(default, alias = "fan_out")]
    pub fan_out: Option<FanOut>,
    #[serde(default, alias = "fan_in")]
    pub fan_in: Option<FanIn>,
    #[serde(default, alias = "sub_workflow")]
    pub sub_workflow: Option<SubWorkflow>,
    #[serde(default)]
    pub compensate: Option<String>,
}

impl NodeConfig {
    fn into_message(self) -> Result<pb::WorkflowNodeConfig, String> {
        let body = match (self.raw, self.structured) {
            (Some(_), Some(_)) => {
                return Err(
                    "`raw` and `structured` are the two arms of one field; send one of them"
                        .to_string(),
                )
            }
            (Some(raw), None) => Some(pb::workflow_node_config::Body::Raw(raw.0)),
            (None, Some(structured)) => Some(pb::workflow_node_config::Body::Structured(
                structured.into_message()?,
            )),
            (None, None) => None,
        };
        let condition = self
            .condition
            .as_deref()
            .map(edge_condition_from_name)
            .transpose()?
            .unwrap_or(pb::EdgeCondition::Unspecified as i32);
        Ok(pb::WorkflowNodeConfig {
            name: self.name,
            task_name: self.task_name,
            queue: self.queue,
            body,
            max_retries: self.max_retries,
            timeout: self.timeout.map(|value| value.0),
            priority: self.priority,
            condition,
            gate: self.gate.map(Gate::into_message).transpose()?,
            cache: self.cache.map(Cache::into_message),
            fan_out: self.fan_out.map(FanOut::into_message),
            fan_in: self.fan_in.map(FanIn::into_message),
            sub_workflow: self
                .sub_workflow
                .map(SubWorkflow::into_message)
                .transpose()?,
            compensate: self.compensate,
        })
    }
}

/// `GateConfig`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Gate {
    #[serde(default)]
    pub timeout: Option<JsonDuration>,
    #[serde(default, alias = "on_timeout")]
    pub on_timeout: Option<String>,
    #[serde(default)]
    pub message: Option<String>,
}

impl Gate {
    fn into_message(self) -> Result<pb::GateConfig, String> {
        let on_timeout = self
            .on_timeout
            .as_deref()
            .map(on_timeout_from_name)
            .transpose()?
            .unwrap_or(pb::OnTimeout::Unspecified as i32);
        Ok(pb::GateConfig {
            timeout: self.timeout.map(|value| value.0),
            on_timeout,
            message: self.message,
        })
    }
}

/// `CacheConfig`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Cache {
    #[serde(default)]
    pub ttl: Option<JsonDuration>,
}

impl Cache {
    fn into_message(self) -> pb::CacheConfig {
        pb::CacheConfig {
            ttl: self.ttl.map(|value| value.0),
        }
    }
}

/// `FanOutConfig`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct FanOut {
    #[serde(default, alias = "items_from")]
    pub items_from: Option<String>,
}

impl FanOut {
    fn into_message(self) -> pb::FanOutConfig {
        pb::FanOutConfig {
            items_from: self.items_from,
        }
    }
}

/// `FanInConfig`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct FanIn {
    #[serde(default)]
    pub from: String,
}

impl FanIn {
    fn into_message(self) -> pb::FanInConfig {
        pb::FanInConfig { from: self.from }
    }
}

/// `SubWorkflowSpec`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SubWorkflow {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub version: i32,
    pub graph: Graph,
    #[serde(default, alias = "deferred_node_names")]
    pub deferred_node_names: Vec<String>,
}

impl SubWorkflow {
    fn into_message(self) -> Result<pb::SubWorkflowSpec, String> {
        Ok(pb::SubWorkflowSpec {
            name: self.name,
            version: self.version,
            graph: Some(self.graph.into_message()?),
            deferred_node_names: self.deferred_node_names,
        })
    }
}

/// One `EdgeCondition`, by the name the enum gives it.
fn edge_condition_from_name(name: &str) -> Result<i32, String> {
    pb::EdgeCondition::from_str_name(name)
        .map(|value| value as i32)
        .ok_or_else(|| {
            format!(
                "`{name}` is not an edge condition; one of {}",
                [
                    pb::EdgeCondition::OnSuccess,
                    pb::EdgeCondition::OnFailure,
                    pb::EdgeCondition::Always,
                ]
                .map(|value| value.as_str_name())
                .join(", ")
            )
        })
}

/// One `OnTimeout`, by the name the enum gives it.
fn on_timeout_from_name(name: &str) -> Result<i32, String> {
    pb::OnTimeout::from_str_name(name)
        .map(|value| value as i32)
        .ok_or_else(|| {
            format!(
                "`{name}` is not a gate timeout policy; one of {}",
                [pb::OnTimeout::Approve, pb::OnTimeout::Reject]
                    .map(|value| value.as_str_name())
                    .join(", ")
            )
        })
}

/// One `JobStatus`, by the name the enum gives it.
///
/// The number is accepted too, because proto3 JSON accepts both — but the
/// refusal names the spellings rather than the numbers, since a client reading
/// a response only ever sees names.
fn status_from_name(name: &str) -> Result<i32, String> {
    if let Some(status) = pb::JobStatus::from_str_name(name) {
        return Ok(status as i32);
    }
    if let Ok(number) = name.parse::<i32>() {
        return Ok(number);
    }
    Err(format!(
        "`{name}` is not a job status; one of {}",
        [
            pb::JobStatus::Pending,
            pb::JobStatus::Running,
            pb::JobStatus::Complete,
            pb::JobStatus::Failed,
            pb::JobStatus::Dead,
            pb::JobStatus::Cancelled,
        ]
        .map(|status| status.as_str_name())
        .join(", ")
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn enqueue(body: serde_json::Value) -> Result<pb::EnqueueRequest, String> {
        serde_json::from_value::<Enqueue>(body)
            .map_err(|error| error.to_string())?
            .into_message()
    }

    #[test]
    fn a_camel_case_body_becomes_the_request_message() {
        let message = enqueue(serde_json::json!({
            "taskName": "send_email",
            "raw": "AoKCAWFhoA==",
            "options": {
                "queue": "emails",
                "priority": 5,
                "maxRetries": 3,
                "scheduledAt": "2025-09-03T12:26:40Z",
                "timeout": "30s",
                "uniqueKey": "welcome:42",
                "dependsOn": ["a", "b"],
                "resultTtl": "1.500s",
                "debounce": {"key": "k", "window": "5s", "maxWait": "60s", "maxPending": "10"}
            }
        }))
        .expect("a well-formed body");

        assert_eq!(message.task_name, "send_email");
        assert!(matches!(
            message.body,
            Some(pb::enqueue_request::Body::Raw(ref bytes)) if bytes == &[0x02, 0x82, 0x82, 0x01, 0x61, 0x61, 0xa0]
        ));
        let options = message.options.expect("options");
        assert_eq!(options.queue, "emails");
        assert_eq!(options.max_retries, 3);
        assert_eq!(options.unique_key.as_deref(), Some("welcome:42"));
        assert_eq!(options.depends_on, vec!["a", "b"]);
        assert_eq!(options.debounce.expect("debounce").max_pending, Some(10));
    }

    /// Both spellings, because a parser must accept the field's own name as
    /// well as the JSON one — and a client generating a body from the `.proto`
    /// has only the former.
    #[test]
    fn the_proto_field_names_are_accepted_too() {
        let message = enqueue(serde_json::json!({
            "task_name": "t",
            "raw": "",
            "options": {"max_retries": 2, "unique_key": "u", "depends_on": ["a"], "result_ttl": "1s"}
        }))
        .expect("snake_case is the field's own name");
        assert_eq!(message.task_name, "t");
        let options = message.options.expect("options");
        assert_eq!(options.max_retries, 2);
        assert_eq!(options.unique_key.as_deref(), Some("u"));
        assert!(options.result_ttl.is_some());
    }

    #[test]
    fn a_misspelled_field_is_refused_rather_than_ignored() {
        let error = enqueue(serde_json::json!({"taskname": "t", "raw": ""}))
            .expect_err("an unknown field is not a job description");
        assert!(error.contains("taskname"), "unhelpful message: {error}");
    }

    #[test]
    fn both_body_arms_at_once_are_refused() {
        let error = enqueue(serde_json::json!({
            "taskName": "t",
            "raw": "",
            "structured": {"args": []}
        }))
        .expect_err("a oneof holds one arm");
        assert!(error.contains("one of them"), "unhelpful message: {error}");
    }

    /// Neither arm is left for the shared handler to refuse, so the two doors
    /// give one answer to one mistake.
    #[test]
    fn no_body_arm_is_left_for_the_shared_handler() {
        let message = enqueue(serde_json::json!({"taskName": "t"})).expect("parses");
        assert!(message.body.is_none());
    }

    #[test]
    fn structured_arguments_keep_their_values() {
        let message = enqueue(serde_json::json!({
            "taskName": "t",
            "structured": {"args": [1, "a"], "kwargs": {"k": true}}
        }))
        .expect("a structured body");
        let Some(pb::enqueue_request::Body::Structured(args)) = message.body else {
            panic!("the structured arm");
        };
        assert_eq!(args.args.len(), 2);
        assert_eq!(args.kwargs.len(), 1);
    }

    #[test]
    fn a_status_filter_reads_the_enum_name() {
        let query = ListJobs {
            status: Some("JOB_STATUS_PENDING".to_string()),
            ..Default::default()
        };
        assert_eq!(
            query.into_message().expect("a known status").status,
            Some(pb::JobStatus::Pending as i32)
        );
    }

    #[test]
    fn an_unknown_status_filter_names_the_ones_that_exist() {
        let query = ListJobs {
            status: Some("pending".to_string()),
            ..Default::default()
        };
        let error = query.into_message().expect_err("not a status name");
        assert!(
            error.contains("JOB_STATUS_PENDING"),
            "unhelpful message: {error}"
        );
    }

    #[test]
    fn a_query_string_reads_both_spellings() {
        let camel: GetJob =
            serde_urlencoded::from_str("includePayload=true").expect("camelCase parses");
        let snake: GetJob =
            serde_urlencoded::from_str("include_payload=true").expect("snake_case parses");
        assert!(camel.include_payload && snake.include_payload);
        assert!(!camel.include_result);

        let page: ListJobs =
            serde_urlencoded::from_str("pageSize=25&queue=emails").expect("parses");
        assert_eq!(page.page_size, Some(25));
        assert_eq!(page.queue.as_deref(), Some("emails"));
    }

    #[test]
    fn an_unknown_query_parameter_is_refused() {
        assert!(serde_urlencoded::from_str::<GetJob>("includePayloadd=true").is_err());
    }
}
