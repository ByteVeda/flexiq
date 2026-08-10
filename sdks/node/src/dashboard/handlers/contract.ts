// Map the SDK's camelCase shapes to the snake_case JSON contract the React SPA
// (`dashboard/`) expects. Timestamps are Unix milliseconds throughout.

import type { CircuitBreaker, DeadJob, Job, ReplayEntry, TaskLog, WorkerInfo } from "../../types";
import type { Delivery, Webhook } from "../../webhooks";
import type { WorkflowNode, WorkflowRun } from "../../workflows";

/** Replace header values with a mask so outbound credentials aren't exposed. */
function maskHeaderValues(headers: Record<string, string>): Record<string, string> {
  return Object.fromEntries(Object.keys(headers).map((name) => [name, "***"]));
}

/** Map a webhook to the SPA contract — secret and header values are never exposed. */
export function webhookToContract(webhook: Webhook) {
  return {
    id: webhook.id,
    url: webhook.url,
    events: webhook.events,
    task_filter: webhook.taskFilter ?? null,
    // Mask header values — they may carry outbound credentials. Keep names so
    // the UI can show which headers are configured.
    headers: maskHeaderValues(webhook.headers),
    has_secret: Boolean(webhook.secret),
    max_retries: webhook.maxRetries,
    timeout_seconds: webhook.timeoutMs / 1000,
    retry_backoff: webhook.retryBackoff,
    enabled: webhook.enabled,
    description: webhook.description ?? null,
    created_at: webhook.createdAt,
    updated_at: webhook.updatedAt,
  };
}

/** Map a webhook delivery to the SPA's `WebhookDelivery` contract. */
export function deliveryToContract(delivery: Delivery) {
  return {
    id: delivery.id,
    subscription_id: delivery.webhookId,
    event: delivery.event,
    payload: delivery.payload,
    task_name: delivery.taskName,
    job_id: delivery.jobId,
    status: delivery.status,
    attempts: delivery.attempts,
    response_code: delivery.responseCode,
    response_body: delivery.responseBody,
    latency_ms: delivery.latencyMs,
    error: delivery.error ?? null,
    created_at: delivery.createdAt,
    completed_at: delivery.completedAt,
  };
}

export function jobToContract(job: Job) {
  return {
    id: job.id,
    task_name: job.taskName,
    queue: job.queue,
    status: job.status,
    priority: job.priority,
    progress: job.progress ?? null,
    retry_count: job.retryCount,
    max_retries: job.maxRetries,
    created_at: job.createdAt,
    scheduled_at: job.scheduledAt,
    started_at: job.startedAt ?? null,
    completed_at: job.completedAt ?? null,
    timeout_ms: job.timeoutMs,
    error: job.error ?? null,
    unique_key: job.uniqueKey ?? null,
    metadata: job.metadata ?? null,
    notes: job.notes ?? null,
    namespace: job.namespace ?? null,
  };
}

export function workerToContract(worker: WorkerInfo) {
  return {
    worker_id: worker.workerId,
    queues: worker.queues,
    status: worker.status,
    last_heartbeat: worker.lastHeartbeat,
    registered_at: worker.startedAt ?? worker.lastHeartbeat,
    hostname: worker.hostname ?? null,
    pid: worker.pid ?? null,
    pool_type: worker.poolType ?? null,
    threads: worker.threads,
    tags: worker.tags ?? null,
    sdk: worker.sdk ?? null,
    sdk_version: worker.sdkVersion ?? null,
  };
}

export function workflowRunToContract(run: WorkflowRun) {
  return {
    id: run.id,
    definition_id: run.definitionId,
    state: run.state,
    params: run.params ?? null,
    started_at: run.startedAt ?? null,
    completed_at: run.completedAt ?? null,
    error: run.error ?? null,
    parent_run_id: run.parentRunId ?? null,
    parent_node_name: run.parentNodeName ?? null,
    created_at: run.createdAt,
  };
}

export function workflowNodeToContract(node: WorkflowNode) {
  // saga fields stay null until saga compensation is bound; fan_out_count is
  // real once a node has expanded.
  return {
    node_name: node.nodeName,
    status: node.status,
    job_id: node.jobId ?? null,
    result_hash: node.resultHash ?? null,
    fan_out_count: node.fanOutCount ?? null,
    started_at: node.startedAt ?? null,
    completed_at: node.completedAt ?? null,
    error: node.error ?? null,
    compensation_job_id: node.compensationJobId ?? null,
    compensation_started_at: node.compensationStartedAt ?? null,
    compensation_completed_at: node.compensationCompletedAt ?? null,
    compensation_error: node.compensationError ?? null,
  };
}

export function deadToContract(dead: DeadJob) {
  return {
    id: dead.id,
    original_job_id: dead.originalJobId,
    task_name: dead.taskName,
    queue: dead.queue,
    error: dead.error ?? null,
    retry_count: dead.retryCount,
    failed_at: dead.failedAt,
    metadata: dead.metadata ?? null,
    dlq_retry_count: dead.dlqRetryCount,
  };
}

export function taskLogToContract(log: TaskLog) {
  return {
    job_id: log.jobId,
    task_name: log.taskName,
    level: log.level,
    message: log.message,
    extra: log.extra ?? null,
    logged_at: log.loggedAt,
  };
}

export function circuitBreakerToContract(row: CircuitBreaker) {
  return {
    task_name: row.taskName,
    state: row.state,
    failure_count: row.failureCount,
    last_failure_at: row.lastFailureAt ?? null,
    opened_at: row.openedAt ?? null,
    threshold: row.threshold,
    window_ms: row.windowMs,
    cooldown_ms: row.cooldownMs,
  };
}

export function replayEntryToContract(row: ReplayEntry) {
  return {
    id: row.id,
    original_job_id: row.originalJobId,
    replay_job_id: row.replayJobId,
    replayed_at: row.replayedAt,
    original_error: row.originalError ?? null,
    replay_error: row.replayError ?? null,
  };
}
