//! Marshalling between core types and JS-facing shapes. One submodule per
//! concern; kept out of the logic modules so they read as intent, not plumbing.

mod job;
mod lock;
mod log;
mod ops;
mod outcome;
mod periodic;
mod pubsub;
mod stats;
mod task_config;
#[cfg(feature = "workflows")]
mod workflow;

pub use job::{build_new_job, job_to_js, JsJob, JsJobPage, JsTaskInvocation, JsTaskOutcome};
pub(crate) use job::{DEFAULT_MAX_RETRIES, DEFAULT_PRIORITY, DEFAULT_TIMEOUT_MS};
pub use lock::{lock_info_to_js, JsLockInfo};
pub use log::{log_to_js, JsTaskLog};
pub use ops::{
    circuit_breaker_to_js, replay_to_js, JsCircuitBreaker, JsDagEdge, JsJobDag, JsReplayEntry,
};
pub use outcome::{outcome_to_js, JsOutcome};
pub use periodic::{periodic_to_js, JsPeriodicTask};
pub use pubsub::{
    subscription_to_js, topic_log_stat_to_js, topic_message_to_js, topic_stat_to_js, topic_to_js,
    JsSubscription, JsTopic, JsTopicLogStat, JsTopicMessage, JsTopicStat,
};
pub use stats::{
    dead_job_to_js, job_error_to_js, metric_to_js, stats_to_js, status_code, worker_to_js,
    JsDeadJob, JsDeadJobPage, JsJobError, JsMetric, JsStats, JsWorkerRow,
};
pub use task_config::{queue_codel, queue_config, task_config};
#[cfg(feature = "workflows")]
pub use workflow::{
    node_to_js, run_to_js, JsFanOutCompletion, JsWorkflowAdvance, JsWorkflowNode,
    JsWorkflowNodeRef, JsWorkflowRun, JsWorkflowRunPlan,
};
