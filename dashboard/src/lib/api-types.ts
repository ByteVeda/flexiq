/**
 * Response shapes for the Taskito dashboard API.
 *
 * These mirror the `to_dict()` output of the underlying Rust/Python models.
 * Keep in sync with:
 * - `py_src/taskito/dashboard.py` route handlers
 * - `docs/guide/observability/dashboard-api.md` (documented contract)
 *
 * **Timestamps**: every timestamp field exposed by the dashboard API is Unix
 * milliseconds (the Rust core uses `now_millis()` and the Python aggregators
 * work in ms). Pass them straight to `Date`, `formatRelative`, `formatAbsolute`
 * — never multiply by 1000.
 */

export type JobStatus = "pending" | "running" | "complete" | "failed" | "dead" | "cancelled";

export interface QueueStats {
  pending: number;
  running: number;
  completed: number;
  failed: number;
  dead: number;
  cancelled: number;
}

export type QueueStatsMap = Record<
  string,
  {
    pending: number;
    running: number;
    completed?: number;
    failed?: number;
    dead?: number;
    cancelled?: number;
  }
>;

export interface Job {
  id: string;
  task_name: string;
  queue: string;
  status: JobStatus;
  priority: number;
  progress: number | null;
  retry_count: number;
  max_retries: number;
  /** Unix milliseconds. */
  created_at: number;
  /** Unix milliseconds. */
  scheduled_at: number;
  /** Unix milliseconds; null if the job hasn't started. */
  started_at: number | null;
  /** Unix milliseconds; null if the job hasn't finished. */
  completed_at: number | null;
  timeout_ms: number;
  error: string | null;
  unique_key: string | null;
  metadata: string | null;
  /**
   * Structured notes JSON string (canonical encoding, ≤ 15 top-level fields).
   * Parse with `JSON.parse` for a dict suitable for key/value rendering.
   * `null` when no notes were attached at enqueue time.
   */
  notes: string | null;
}

/**
 * One executor attached to a standalone scheduler.
 *
 * Only `taskito-server` serves `/api/executors`; an SDK dashboard answers 404
 * and the page hides itself.
 */
export interface Executor {
  executor_id: string;
  sdk: string;
  version: string;
  /** Task names it advertises; the scheduler sends it nothing else. */
  tasks: string[];
  slots: number;
  free_slots: number;
  in_flight: number;
  /** Peer label for logs, never a credential. */
  peer: string;
  /** Milliseconds since its last frame. */
  idle_ms: number;
}

export interface ExecutorCapacity {
  executors: number;
  total_slots: number;
  free_slots: number;
}

export interface ExecutorInventory {
  executors: Executor[];
  capacity: ExecutorCapacity;
}

export interface JobError {
  attempt: number;
  error: string;
  /** Unix milliseconds. */
  failed_at: number;
}

export interface TaskLog {
  job_id: string;
  task_name: string;
  level: string;
  message: string;
  extra: string | null;
  /** Unix milliseconds. */
  logged_at: number;
}

export interface ReplayEntry {
  replay_job_id: string;
  /** Unix milliseconds. */
  replayed_at: number;
  original_error: string | null;
  replay_error: string | null;
}

export interface DagNode {
  id: string;
  task_name: string;
  status: JobStatus;
}

export interface DagEdge {
  from: string;
  to: string;
}

export interface DagData {
  nodes: DagNode[];
  edges: DagEdge[];
}

export interface DeadLetter {
  id: string;
  original_job_id: string;
  task_name: string;
  queue: string;
  error: string | null;
  retry_count: number;
  /** Unix milliseconds. */
  failed_at: number;
  dlq_retry_count: number;
}

export interface TaskMetrics {
  count: number;
  success_count: number;
  failure_count: number;
  avg_ms: number;
  p50_ms: number;
  p95_ms: number;
  p99_ms: number;
  min_ms: number;
  max_ms: number;
}

export type MetricsResponse = Record<string, TaskMetrics>;

export interface TimeseriesBucket {
  /** Bucket start; Unix milliseconds. */
  timestamp: number;
  count: number;
  success: number;
  failure: number;
  avg_ms: number;
  p50_ms: number;
  p95_ms: number;
  p99_ms: number;
}

export interface Worker {
  worker_id: string;
  queues: string;
  /** Unix milliseconds. */
  last_heartbeat: number;
  /** Unix milliseconds. */
  registered_at: number;
  tags: string | null;
}

export interface CircuitBreaker {
  task_name: string;
  state: "closed" | "open" | "half_open";
  failure_count: number;
  threshold: number;
  window_ms: number;
  cooldown_ms: number;
  /** Unix milliseconds; null if no failures yet. */
  last_failure_at: number | null;
}

export interface ResourcePoolStats {
  active: number;
  idle: number;
  size: number;
  total_timeouts: number;
}

export interface ResourceStatus {
  name: string;
  scope: string;
  health: string;
  init_duration_ms: number;
  recreations: number;
  depends_on: string[];
  pool?: ResourcePoolStats;
}

export interface ProxyHandlerStats {
  handler: string;
  total_reconstructions: number;
  total_errors: number;
  total_cleanup_errors: number;
  total_checksum_failures: number;
  total_duration_ms: number;
  avg_duration_ms: number;
  max_duration_ms: number;
  p95_duration_ms: number;
}

export type ProxyStats = ProxyHandlerStats[];

export interface InterceptionStats {
  total_intercepts: number;
  total_duration_ms: number;
  avg_duration_ms: number;
  strategy_counts: Record<string, number>;
  max_depth_reached: number;
}

export type WorkflowState =
  | "pending"
  | "running"
  | "paused"
  | "completed"
  | "completed_with_failures"
  | "failed"
  | "cancelled"
  | "compensating"
  | "compensated"
  | "compensation_failed";

export type WorkflowNodeStatus =
  | "pending"
  | "ready"
  | "running"
  | "completed"
  | "failed"
  | "skipped"
  | "waiting_approval"
  | "cache_hit"
  | "compensating"
  | "compensated"
  | "compensation_failed";

export interface WorkflowRun {
  id: string;
  definition_id: string;
  state: WorkflowState;
  params: string | null;
  /** Unix milliseconds; null if not started. */
  started_at: number | null;
  /** Unix milliseconds; null if not completed. */
  completed_at: number | null;
  error: string | null;
  parent_run_id: string | null;
  parent_node_name: string | null;
  /** Unix milliseconds. */
  created_at: number;
}

export interface TopicSummary {
  topic: string;
  subscription_count: number;
  /** Sum of pending + running across every subscription. */
  backlog: number;
  /** Sum of dead-lettered deliveries across every subscription. */
  dead: number;
}

export interface SubscriptionBacklog {
  topic: string;
  subscription: string;
  task_name: string;
  queue: string;
  active: boolean;
  durable: boolean;
  pending: number;
  running: number;
  dead: number;
  /** Age of the oldest pending delivery in ms; null when no pending backlog. */
  oldest_pending_age_ms: number | null;
}

export interface WorkflowNode {
  node_name: string;
  status: WorkflowNodeStatus;
  job_id: string | null;
  result_hash: string | null;
  fan_out_count: number | null;
  /** Unix milliseconds; null if not started. */
  started_at: number | null;
  /** Unix milliseconds; null if not completed. */
  completed_at: number | null;
  error: string | null;
  compensation_job_id: string | null;
  /** Unix milliseconds. */
  compensation_started_at: number | null;
  /** Unix milliseconds. */
  compensation_completed_at: number | null;
  compensation_error: string | null;
}
