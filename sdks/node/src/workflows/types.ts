import type { JsWorkflowAdvance, JsWorkflowNode, JsWorkflowRun } from "../native";

/** A workflow run row (state, timestamps, lineage). */
export type WorkflowRun = JsWorkflowRun;
/** One step of a run, linked to its underlying job. */
export type WorkflowNode = JsWorkflowNode;
/** Result of advancing a node — `finalState` set once the run is terminal. */
export type WorkflowAdvance = JsWorkflowAdvance;

/** Lowercase workflow run states (mirrors the Rust `WorkflowState`). */
export type WorkflowRunState =
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

/**
 * When a step runs, based on its predecessors' outcomes:
 * - `on_success` (default) — every predecessor completed
 * - `on_failure` — at least one predecessor failed (an error handler)
 * - `always` — regardless of predecessor outcomes
 */
export type WorkflowCondition = "on_success" | "on_failure" | "always";

/** Per-step options when building a workflow. */
export interface WorkflowStepOptions {
  /** Predecessor step name(s) this step runs after. */
  after?: string | string[];
  /** Positional args passed to the step's task handler. */
  args?: unknown[];
  /** Queue to run this step on (defaults to the submit-time `queueDefault`). */
  queue?: string;
  maxRetries?: number;
  timeoutMs?: number;
  priority?: number;
  /** Gate this step on its predecessors' outcomes (default `on_success`). */
  condition?: WorkflowCondition;
  /**
   * Rollback task name for saga compensation. If the run fails, this task is run
   * with the step's result as its single argument, in reverse-dependency order.
   */
  compensate?: string;
  /**
   * Reuse this step's result across runs when its task, args, and upstream
   * results are unchanged. `{ ttlMs }` expires the cache. A cacheable step must
   * have at least one predecessor.
   */
  cache?: boolean | { ttlMs?: number };
}

/**
 * A step in a canvas helper ({@link WorkflowBuilder.chain} / `group` / `chord`):
 * a node name plus its task and step options. `after` is managed by the helper.
 */
export interface CanvasStep extends Omit<WorkflowStepOptions, "after"> {
  name: string;
  task: string;
}

/** Common per-step task config shared by every step kind. */
interface StepTaskOptions {
  queue?: string;
  maxRetries?: number;
  timeoutMs?: number;
  priority?: number;
}

/**
 * Options for a fan-out step. The step itself runs no job; at runtime the
 * tracker reads the array result of `itemsFrom` (its sole predecessor by
 * default) and expands one child per item, each running `task` with that item
 * as its single argument.
 */
export interface FanOutStepOptions extends StepTaskOptions {
  /** Predecessor step name(s). At least one is required. */
  after: string | string[];
  /** Task each child runs, once per item. */
  task: string;
  /** Predecessor whose array result supplies the items. Defaults to the sole predecessor. */
  itemsFrom?: string;
}

/**
 * Options for a fan-in step. Collects the results of a fan-out's children into
 * an array and runs `task` with that array as its single argument.
 */
export interface FanInStepOptions extends StepTaskOptions {
  /** The fan-out step to collect. */
  after: string;
  /** Combiner task; receives `[childResult, …]` as its single argument. */
  task: string;
}

/**
 * Options for an approval gate. The gate runs no task: the run pauses at the
 * gate (status `waiting_approval`) until resolved via
 * `queue.workflows.resolveGate` / `approveGate` / `rejectGate`, or until
 * `timeoutMs` elapses (then `onTimeout` decides the outcome).
 */
export interface GateStepOptions {
  /** Predecessor step name(s) the gate runs after. */
  after: string | string[];
  /** Auto-resolve after this many ms (no timeout if omitted). */
  timeoutMs?: number;
  /** What a timeout does — `"approve"` or `"reject"` (default `"reject"`). */
  onTimeout?: "approve" | "reject";
  /** Human-readable reason shown to approvers (recorded in metadata). */
  message?: string;
}

/** snake_case step metadata, matching the Rust `StepMetadata` shape. */
export interface StepMetadataJson {
  task_name: string;
  queue?: string;
  max_retries?: number;
  timeout_ms?: number;
  priority?: number;
  /** Base64 of the serialized args, persisted so the tracker can enqueue deferred nodes. */
  args_template?: string;
  /** JSON `{itemsFrom}` marking a fan-out node. */
  fan_out?: string;
  /** JSON `{from}` marking a fan-in node (the fan-out it collects). */
  fan_in?: string;
  /** Entry condition: `on_success` | `on_failure` | `always`. */
  condition?: string;
  /** JSON `{timeoutMs?, onTimeout?, message?}` marking an approval gate node. */
  gate?: string;
  /** JSON {@link SubWorkflowTransport} marking a sub-workflow node. */
  sub_workflow?: string;
  /** Rollback task name for saga compensation. */
  compensate?: string;
  /** JSON `{ttlMs?}` marking a cacheable node (result reused across runs). */
  cache?: string;
}

/**
 * Options for a sub-workflow step. The step runs no task of its own; at runtime
 * the tracker submits `workflow` as a child run and resolves this node when the
 * child finalizes (success → completed, failure → failed).
 */
export interface SubWorkflowStepOptions {
  /** Predecessor step name(s) the sub-workflow runs after. */
  after: string | string[];
  /** The child workflow to run — build it with `queue.workflows.define(...).build()`. */
  workflow: WorkflowSpec;
}

/**
 * A child workflow flattened to base64/JSON so it round-trips through storage
 * (in a parent node's `sub_workflow` metadata) and can be submitted as a child
 * run by the tracker without re-serializing user args.
 */
export interface SubWorkflowTransport {
  name: string;
  version: number;
  /** base64 of the serialized DAG (`SerializableGraph` JSON). */
  dag: string;
  /** JSON-encoded `Record<string, StepMetadataJson>`. */
  stepMetadata: string;
  /** Node name → base64 of the node's serialized args payload. */
  nodePayloads: Record<string, string>;
  deferredNodeNames: string[];
}

/** A built workflow definition, ready to submit. */
export interface WorkflowSpec {
  name: string;
  version: number;
  nodes: string[];
  edges: Array<{ from: string; to: string }>;
  stepMetadata: Record<string, StepMetadataJson>;
  stepArgs: Record<string, unknown[]>;
  /** Nodes the tracker enqueues on demand (fan-out/fan-in ∪ their descendants). */
  deferredNodeNames: string[];
  /** Child workflows keyed by their sub-workflow node name. */
  subWorkflows?: Record<string, WorkflowSpec>;
}

/** Submit-time options. */
export interface WorkflowSubmitOptions {
  /** Default queue for steps that don't set their own. */
  queueDefault?: string;
  /** Arbitrary params recorded on the run (JSON-encoded). */
  params?: unknown;
  /**
   * W3C carrier (`traceparent`, `tracestate`) every node job carries, so each
   * step's span joins this trace. When omitted, middleware `onWorkflowSubmit`
   * hooks may supply one — the OpenTelemetry middleware passes the active context.
   */
  traceContext?: Record<string, string>;
}

/** Options for {@link WorkflowHandle.wait}. */
export interface WorkflowWaitOptions {
  /** Max time to wait for a terminal state (ms). Default 30000. */
  timeoutMs?: number;
  /** Poll interval (ms). Default 100. */
  pollMs?: number;
}

/** Handle to a submitted run: query status/nodes or await completion. */
export interface WorkflowHandle {
  readonly runId: string;
  /** Current run row, or `undefined` if it has been purged. */
  status(): WorkflowRun | undefined;
  /** The run's step nodes. */
  nodes(): WorkflowNode[];
  /** Resolve once the run reaches a terminal state (or reject on timeout). */
  wait(options?: WorkflowWaitOptions): Promise<WorkflowRun>;
}
