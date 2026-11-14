import { createHash } from "node:crypto";
import type { Emitter } from "../events";
import type { JsOutcome, NativeQueue } from "../native";
import { type Serializer, serializeCall } from "../serializers";
import { createLogger } from "../utils";
import { CACHE_TASK, WorkflowCacheStore } from "./cache";
import { predecessorsOf, successorsOf, transitiveDeferred } from "./plan";
import type { SubWorkflowTransport } from "./types";

const log = createLogger("workflow-tracker");

const DEFAULT_QUEUE = "default";
const DEFAULT_MAX_RETRIES = 3;
const DEFAULT_TIMEOUT_MS = 300_000;

/** Node statuses with no further transitions (mirrors `WorkflowNodeStatus::is_terminal`). */
const TERMINAL_NODE_STATUS = new Set([
  "completed",
  "failed",
  "skipped",
  "cache_hit",
  "compensated",
  "compensation_failed",
]);

/** A fan-out child node name (`parent[3]`) → its parent (`parent`), else null. */
const FAN_OUT_CHILD = /^(.+)\[\d+\]$/;

/** Predecessor outcomes that satisfy an `on_success` condition. */
const SUCCEEDED_STATUS = new Set(["completed", "cache_hit"]);

/** snake_case step metadata as stored in the workflow definition. */
interface StepMeta {
  task_name: string;
  queue?: string;
  max_retries?: number;
  timeout_ms?: number;
  priority?: number;
  args_template?: string;
  fan_out?: string;
  fan_in?: string;
  /** `"on_success"` (default) | `"on_failure"` | `"always"` — gates node entry. */
  condition?: string;
  /** JSON `{timeoutMs?, onTimeout?, message?}` marking an approval gate node. */
  gate?: string;
  /** JSON `SubWorkflowTransport` marking a sub-workflow node. */
  sub_workflow?: string;
  /** Rollback task name run if the workflow fails (saga compensation). */
  compensate?: string;
  /** JSON `{ttlMs?}` marking a cacheable node (result reused across runs). */
  cache?: string;
}

/** Parsed gate config from a node's `gate` metadata. */
interface GateConfig {
  timeoutMs?: number;
  onTimeout?: "approve" | "reject";
  message?: string;
}

/** The reconstructed structure + config of a workflow run. */
interface RunPlan {
  successors: Map<string, string[]>;
  predecessors: Map<string, string[]>;
  deferred: Set<string>;
  meta: Map<string, StepMeta>;
  /** True if any node declares a compensator (the run can run a saga rollback). */
  hasCompensation: boolean;
}

/** Node statuses still awaiting (or undergoing) compensation rollback. */
const COMPENSABLE_STATUS = new Set(["completed", "cache_hit"]);

/** Events a run can end with — narrower than EventName so a wrong
 * name/payload pairing in the lookup below fails typecheck. */
type RunTerminalEvent =
  | "workflow.completed"
  | "workflow.completed_with_failures"
  | "workflow.failed"
  | "workflow.cancelled"
  | "workflow.compensated"
  | "workflow.compensation_failed";

/** Terminal run state (from the finalizer) → the event announcing it. */
const TERMINAL_RUN_EVENTS: Record<string, RunTerminalEvent> = {
  completed: "workflow.completed",
  completed_with_failures: "workflow.completed_with_failures",
  failed: "workflow.failed",
  cancelled: "workflow.cancelled",
};

/** Terminal runs remembered by the emit-once guard before pruning the oldest. */
const REPORTED_RUNS_CAP = 1000;

/**
 * The runtime workflow brain. Driven from the worker's outcome callback, it
 * advances a run as each node-job settles. For plain DAGs the core scheduler
 * already sequences nodes via `depends_on`, so the tracker only records status.
 * For runs with deferred (fan-out / fan-in) nodes it owns the on-demand
 * orchestration: expand fan-outs, collect fan-ins, enqueue downstream nodes,
 * cascade fail-fast, and finalize the run.
 *
 * The run plan is reconstructed from storage (DAG + step metadata), so the
 * tracker works regardless of which process submitted the run.
 *
 * Runs entirely synchronously inside the (sequential) outcome callback: job
 * results are committed before the callback fires, so reads never race.
 */
export class WorkflowTracker {
  private readonly plans = new Map<string, RunPlan>();
  /** Pending gate-timeout timers, keyed `runId\0node` (unref'd). */
  private readonly gateTimers = new Map<string, ReturnType<typeof setTimeout>>();
  /** Cross-run store for cacheable step results. */
  private readonly cache: WorkflowCacheStore;
  /** Runs whose terminal event already fired (bounded, insertion-ordered). */
  private readonly reportedTerminal = new Set<string>();

  constructor(
    private readonly native: NativeQueue,
    private readonly serializer: Serializer,
    /** Per-task payload encoder (serializer + named codecs) so tracker-created
     *  jobs decode under the worker's unconditional codec reversal. */
    private readonly encodeCall: (taskName: string, args: unknown[]) => Uint8Array = (_, args) =>
      serializeCall(this.serializer, args),
    /** Emits workflow lifecycle events; absent on throwaway trackers. */
    private readonly emitter?: Emitter,
  ) {
    this.cache = new WorkflowCacheStore(native);
  }

  /** Handle one worker outcome. A no-op for non-workflow jobs. */
  onOutcome(outcome: JsOutcome): void {
    let succeeded: boolean;
    if (outcome.kind === "success") {
      succeeded = true;
    } else if (outcome.kind === "dead") {
      succeeded = false;
    } else if (outcome.kind === "cancelled") {
      this.settleCancelledJob(outcome);
      return;
    } else {
      return; // retry is not terminal for workflow advancement
    }

    // The worker reads the link off the job it dispatched, so classifying an
    // outcome costs no storage read on this (the JS) thread.
    const link = outcome.workflow;
    if (!link) {
      return; // not a workflow job
    }
    try {
      // A rollback job advances the saga, not the forward run.
      if (link.compensation) {
        this.onCompensationOutcome(link.runId, link.nodeName, succeeded, outcome.error ?? null);
        return;
      }
      this.advance(outcome.jobId, link.runId, link.nodeName, succeeded, outcome.error ?? null);
    } catch (error) {
      // Workflow bookkeeping must never break the worker loop — but a swallowed
      // failure means a stuck run, so it must be visible at the default level.
      log.error(() => `workflow advance for ${outcome.jobId} failed`, error);
    }
  }

  private advance(
    jobId: string,
    runId: string,
    nodeName: string,
    succeeded: boolean,
    error: string | null,
  ): void {
    const plan = this.plan(runId);
    if (!plan) {
      return;
    }
    const managed = plan.deferred.size > 0;

    // Record the node's outcome. For plain DAGs the core also cascades + finalizes.
    const advance = this.native.markWorkflowNodeResult(jobId, succeeded, error, managed);
    // Persist a successful cacheable node's result for future runs (idempotent on
    // a hit, whose job already carries the cached bytes).
    if (succeeded && plan.meta.get(nodeName)?.cache) {
      this.storeCache(runId, nodeName, jobId, plan);
    }
    if (!managed) {
      this.settle(runId, advance?.finalState ?? null);
      return;
    }

    const parent = fanOutParentOf(nodeName);
    if (parent !== null && isFanOut(plan, parent)) {
      this.onFanOutChild(runId, parent, plan);
    } else {
      // Success and failure both flow through successor evaluation: each
      // successor's condition decides whether it runs or is skipped, so an
      // `on_failure` handler fires and an `on_success` step skips after a fault.
      this.evaluateSuccessors(runId, nodeName, plan);
    }

    this.settle(runId, this.native.finalizeRunIfTerminal(runId));
  }

  /**
   * A cancelled node-job cannot advance its run, but it can strand it: once
   * nothing else is in flight, try to finalize so the run still reaches a
   * terminal state. A non-workflow job is left exactly as before (no-op).
   * `workflow.cancelled` is reserved — this SDK has no run-cancellation API
   * today, so it only fires when the finalizer reports a cancelled run.
   */
  private settleCancelledJob(outcome: JsOutcome): void {
    const link = outcome.workflow;
    if (!link || link.compensation) {
      return; // not a forward workflow node
    }
    try {
      this.settle(link.runId, this.native.finalizeRunIfTerminal(link.runId));
    } catch (error) {
      log.error(() => `workflow cancel settle for ${outcome.jobId} failed`, error);
    }
  }

  /**
   * Wrap up after a node settles. A failed run with compensable nodes starts a
   * saga rollback instead of finishing; otherwise the plan is evicted and, if
   * this run is a sub-workflow child, its parent node is resolved.
   */
  private settle(runId: string, finalState: string | null): void {
    if (finalState === "failed" && this.startCompensationIfEligible(runId)) {
      return; // run moved to `compensating`; it finalizes when rollback completes
    }
    if (finalState !== null) {
      const runError = this.native.getWorkflowRun(runId)?.error;
      this.emitRunTerminal(runId, TERMINAL_RUN_EVENTS[finalState], finalState, runError);
    }
    this.evictIfTerminal(runId, finalState);
    if (finalState !== null) {
      this.resolveParentIfChild(runId, finalState);
    }
  }

  /**
   * Emit a run's terminal event exactly once. The guard set is bounded: past
   * REPORTED_RUNS_CAP the oldest remembered run is pruned (insertion order),
   * trading a sliver of dedup memory for a hard memory ceiling.
   */
  private emitRunTerminal(
    runId: string,
    event: RunTerminalEvent | undefined,
    state: string,
    error?: string,
  ): void {
    if (!this.emitter || !event || this.reportedTerminal.has(runId)) {
      return;
    }
    this.reportedTerminal.add(runId);
    if (this.reportedTerminal.size > REPORTED_RUNS_CAP) {
      const oldest = this.reportedTerminal.values().next().value;
      if (oldest !== undefined) {
        this.reportedTerminal.delete(oldest);
      }
    }
    this.emitter.emit(event, { runId, state, error });
  }

  /**
   * If `childRunId` is a sub-workflow child, resolve its parent node (success →
   * complete, failure → fail), advance the parent's successors, and recurse for
   * nested parents. Idempotent: a parent node already resolved is left alone.
   */
  private resolveParentIfChild(childRunId: string, finalState: string): void {
    const child = this.native.getWorkflowRun(childRunId);
    const parentRunId = child?.parentRunId;
    const parentNode = child?.parentNodeName;
    if (!parentRunId || !parentNode) {
      return;
    }
    const parentStatus = this.nodeStatuses(parentRunId).get(parentNode);
    if (parentStatus === undefined || TERMINAL_NODE_STATUS.has(parentStatus)) {
      return; // parent gone, or already resolved (e.g. by a racing worker)
    }
    const succeeded = finalState === "completed";
    this.native.resolveWorkflowGate(
      parentRunId,
      parentNode,
      succeeded,
      succeeded ? null : `sub-workflow ${finalState}`,
    );
    const parentPlan = this.plan(parentRunId);
    if (parentPlan) {
      this.evaluateSuccessors(parentRunId, parentNode, parentPlan);
    }
    this.settle(parentRunId, this.native.finalizeRunIfTerminal(parentRunId));
  }

  /** Whether a node declares a compensator (saga rollback task). */
  private isCompensable(node: string, plan: RunPlan): boolean {
    return Boolean(plan.meta.get(node)?.compensate);
  }

  /**
   * Begin saga compensation for a failed run that has completed compensable
   * nodes: move the run to `compensating` and dispatch the first rollback wave.
   * Returns false (run finalizes normally) when there's nothing to roll back.
   */
  private startCompensationIfEligible(runId: string): boolean {
    const plan = this.plan(runId);
    if (!plan?.hasCompensation) {
      return false;
    }
    const eligible = this.native
      .getWorkflowNodes(runId)
      .some((n) => this.isCompensable(n.nodeName, plan) && COMPENSABLE_STATUS.has(n.status));
    if (!eligible) {
      return false;
    }
    this.native.setWorkflowRunState(runId, "compensating", null, null);
    this.emitter?.emit("workflow.compensating", { runId, state: "compensating" });
    this.advanceCompensation(runId, plan);
    return true;
  }

  /**
   * Drive one step of compensation: dispatch every compensable node that's ready
   * (all its compensable successors already rolled back) in reverse-dependency
   * order, then finalize the run once nothing is in flight. Storage-driven and
   * idempotent — re-derived from node status on every rollback outcome.
   */
  private advanceCompensation(runId: string, plan: RunPlan): void {
    let nodes = this.native.getWorkflowNodes(runId);
    const compensable = (node: { nodeName: string }) => this.isCompensable(node.nodeName, plan);
    const anyFailed = nodes.some((n) => compensable(n) && n.status === "compensation_failed");

    if (!anyFailed) {
      const status = new Map(nodes.map((n) => [n.nodeName, n.status]));
      for (const node of nodes) {
        if (!compensable(node) || !COMPENSABLE_STATUS.has(node.status)) {
          continue;
        }
        if (this.compensationReady(node.nodeName, status, plan)) {
          this.enqueueCompensation(runId, node.nodeName, node.jobId ?? null, plan);
        }
      }
      nodes = this.native.getWorkflowNodes(runId); // re-read after dispatch
    }

    // Still working while any rollback is in flight, or (absent a failure) any
    // compensable node is still awaiting rollback.
    const working = nodes.some((n) => {
      if (!compensable(n)) {
        return false;
      }
      return n.status === "compensating" || (!anyFailed && COMPENSABLE_STATUS.has(n.status));
    });
    if (working) {
      return;
    }

    const failed = nodes.some((n) => compensable(n) && n.status === "compensation_failed");
    const now = Date.now();
    // These terminal writes bypass settle(), so emit here — same emit-once guard.
    if (failed) {
      this.native.setWorkflowRunState(runId, "compensation_failed", "compensation failed", now);
      this.emitRunTerminal(
        runId,
        "workflow.compensation_failed",
        "compensation_failed",
        "compensation failed",
      );
    } else {
      this.native.setWorkflowRunState(runId, "compensated", null, now);
      this.emitRunTerminal(runId, "workflow.compensated", "compensated");
    }
    this.plans.delete(runId);
  }

  /** A node is ready to roll back once all its compensable successors have. */
  private compensationReady(node: string, status: Map<string, string>, plan: RunPlan): boolean {
    for (const succ of plan.successors.get(node) ?? []) {
      if (!this.isCompensable(succ, plan)) {
        continue; // a non-compensable successor did nothing to undo
      }
      const s = status.get(succ);
      if (s !== "compensated" && s !== "compensation_failed") {
        return false;
      }
    }
    return true;
  }

  /** Enqueue a node's rollback job, passing the node's forward result as its arg. */
  private enqueueCompensation(
    runId: string,
    node: string,
    jobId: string | null,
    plan: RunPlan,
  ): void {
    const meta = plan.meta.get(node);
    if (!meta?.compensate) {
      return;
    }
    const job = jobId ? this.native.getJob(jobId) : null;
    const result = job?.result ? this.serializer.deserialize(job.result) : null;
    const payload = Buffer.from(this.encodeCall(meta.compensate, [result]));
    this.native.enqueueCompensation(
      runId,
      node,
      payload,
      meta.compensate,
      meta.queue ?? DEFAULT_QUEUE,
      meta.max_retries ?? DEFAULT_MAX_RETRIES,
      meta.timeout_ms ?? DEFAULT_TIMEOUT_MS,
      meta.priority ?? 0,
    );
    this.emitter?.emit("workflow.node_compensating", { runId, node });
  }

  /** Record a rollback's outcome and advance (or finalize) the saga. */
  private onCompensationOutcome(
    runId: string,
    node: string,
    succeeded: boolean,
    error: string | null,
  ): void {
    const now = Date.now();
    if (succeeded) {
      this.native.setWorkflowNodeCompensated(runId, node, now);
      this.emitter?.emit("workflow.node_compensated", { runId, node });
    } else {
      this.native.setWorkflowNodeCompensationFailed(
        runId,
        node,
        error ?? "compensation failed",
        now,
      );
      this.emitter?.emit("workflow.node_compensation_failed", {
        runId,
        node,
        error: error ?? "compensation failed",
      });
    }
    const plan = this.plan(runId);
    if (plan) {
      this.advanceCompensation(runId, plan);
    }
  }

  /** Drop a finished run's cached plan so the cache doesn't grow unbounded. */
  private evictIfTerminal(runId: string, finalState: string | null): void {
    if (finalState !== null) {
      this.plans.delete(runId);
    }
  }

  /** A fan-out child settled — finalize the parent once all children are terminal. */
  private onFanOutChild(runId: string, parent: string, plan: RunPlan): void {
    const completion = this.native.checkFanOutCompletion(runId, parent);
    if (!completion) {
      return; // children still in flight, or another caller already finalized
    }
    if (completion.succeeded) {
      this.onFanOutParentDone(runId, parent, completion.childJobIds, plan);
    } else {
      // A child failed → the parent is now `failed`; evaluating its successors
      // skips the (on_success) fan-in and anything downstream.
      this.evaluateSuccessors(runId, parent, plan);
    }
  }

  /** A fan-out parent completed successfully — collect into fan-in or move on. */
  private onFanOutParentDone(
    runId: string,
    parent: string,
    childJobIds: string[],
    plan: RunPlan,
  ): void {
    const fanInNode = this.fanInNodeFor(parent, plan);
    if (fanInNode) {
      this.createFanInJob(runId, fanInNode, childJobIds, plan);
    }
    // evaluateSuccessors skips fan-in nodes, so the combiner isn't recreated.
    this.evaluateSuccessors(runId, parent, plan);
  }

  /** Read `itemsFrom`'s array result and expand the fan-out into one child per item. */
  private expandFanOut(runId: string, node: string, plan: RunPlan): void {
    const meta = plan.meta.get(node);
    if (!meta?.fan_out) {
      return;
    }
    const itemsFrom =
      (JSON.parse(meta.fan_out).itemsFrom as string | null) ?? singlePred(plan, node);
    const items = this.readArrayResult(runId, itemsFrom);
    const childNames = items.map((_, i) => `${node}[${i}]`);
    const childPayloads = items.map((item) => Buffer.from(this.encodeCall(meta.task_name, [item])));

    this.native.expandFanOut(
      runId,
      node,
      childNames,
      childPayloads,
      meta.task_name,
      meta.queue ?? DEFAULT_QUEUE,
      meta.max_retries ?? DEFAULT_MAX_RETRIES,
      meta.timeout_ms ?? DEFAULT_TIMEOUT_MS,
      meta.priority ?? 0,
    );

    // Empty fan-out completes the parent immediately, so no child outcome will
    // fire to carry the run forward — do it inline.
    if (items.length === 0) {
      this.onFanOutParentDone(runId, node, [], plan);
    }
  }

  /** Collect the children's results and enqueue the combiner job. */
  private createFanInJob(
    runId: string,
    fanInNode: string,
    childJobIds: string[],
    plan: RunPlan,
  ): void {
    const results = childJobIds.map((id) => {
      const job = this.native.getJob(id);
      if (!job?.result) {
        throw new Error(`fan-in: child job '${id}' has no result`);
      }
      return this.serializer.deserialize(job.result);
    });
    const meta = plan.meta.get(fanInNode);
    if (!meta) {
      return;
    }
    // The combiner receives the whole list as its single positional argument.
    const payload = Buffer.from(this.encodeCall(meta.task_name, [results]));
    this.native.createDeferredJob(
      runId,
      fanInNode,
      payload,
      meta.task_name,
      meta.queue ?? DEFAULT_QUEUE,
      meta.max_retries ?? DEFAULT_MAX_RETRIES,
      meta.timeout_ms ?? DEFAULT_TIMEOUT_MS,
      meta.priority ?? 0,
    );
  }

  /** Enqueue/expand/skip any successor whose predecessors are now all terminal. */
  private evaluateSuccessors(runId: string, nodeName: string, plan: RunPlan): void {
    for (const succ of plan.successors.get(nodeName) ?? []) {
      if (!this.allPredecessorsTerminal(runId, succ, plan)) {
        continue;
      }
      if (!plan.deferred.has(succ)) {
        // The core's fail-fast cascade is suppressed on managed runs, so a
        // static successor of a failed predecessor must be skipped here.
        if (!this.shouldExecute(runId, succ, plan)) {
          this.skipNode(runId, succ, plan);
        }
        continue;
      }
      // Evaluate the condition first: a not-taken branch is skipped (and the
      // skip propagates) before we consider what kind of node it is.
      if (!this.shouldExecute(runId, succ, plan)) {
        this.skipNode(runId, succ, plan);
        continue;
      }
      const meta = plan.meta.get(succ);
      if (meta?.fan_in) {
        continue; // fan-in jobs are created from the fan-out completion path
      }
      if (meta?.gate) {
        this.enterGate(runId, succ, meta);
      } else if (meta?.sub_workflow) {
        this.submitSubWorkflow(runId, succ, meta);
      } else if (meta?.fan_out) {
        this.expandFanOut(runId, succ, plan);
      } else if (meta?.cache) {
        this.cacheOrEnqueue(runId, succ, plan, meta);
      } else {
        this.createDeferredJob(runId, succ, plan);
      }
    }
  }

  /** Submit a node's child workflow as a linked run; mark the node `running`. */
  private submitSubWorkflow(runId: string, node: string, meta: StepMeta): void {
    if (!meta.sub_workflow) {
      return;
    }
    const child: SubWorkflowTransport = JSON.parse(meta.sub_workflow);
    const payloads: Record<string, Buffer> = {};
    for (const [name, b64] of Object.entries(child.nodePayloads)) {
      payloads[name] = Buffer.from(b64, "base64");
    }
    const childRunId = this.native.submitWorkflow(
      child.name,
      child.version,
      Buffer.from(child.dag, "base64"),
      child.stepMetadata,
      payloads,
      null,
      null,
      child.deferredNodeNames,
      runId,
      node,
    );
    this.native.setWorkflowNodeRunning(runId, node);
    this.emitter?.emit("workflow.submitted", {
      runId: childRunId,
      name: child.name,
      parentRunId: runId,
    });
  }

  /** Park a gate node at `waiting_approval` and arm its timeout, if any. */
  private enterGate(runId: string, node: string, meta: StepMeta): void {
    this.native.setWorkflowNodeWaitingApproval(runId, node);
    const gate: GateConfig = meta.gate ? JSON.parse(meta.gate) : {};
    this.emitter?.emit("workflow.gate_reached", { runId, node, message: gate.message });
    if (!gate.timeoutMs || gate.timeoutMs <= 0) {
      return;
    }
    const key = gateKey(runId, node);
    const approved = gate.onTimeout === "approve";
    const timer = setTimeout(() => {
      this.gateTimers.delete(key);
      this.resolveGate(runId, node, approved, approved ? undefined : "gate timeout");
    }, gate.timeoutMs);
    timer.unref(); // never keep the process alive waiting on an approval
    this.gateTimers.set(key, timer);
  }

  /**
   * Approve or reject a waiting gate, then advance (or skip) its successors.
   * Safe to call from any process and idempotent: a gate already resolved (by
   * the other of manual-vs-timeout) is a no-op.
   */
  resolveGate(runId: string, node: string, approved: boolean, error?: string): void {
    this.clearGateTimer(runId, node);
    const current = this.nodeStatuses(runId).get(node);
    if (current === undefined || TERMINAL_NODE_STATUS.has(current)) {
      return; // gone, or already resolved by the timeout / another caller
    }
    this.native.resolveWorkflowGate(runId, node, approved, error ?? null);
    const plan = this.plan(runId);
    if (!plan) {
      return;
    }
    this.evaluateSuccessors(runId, node, plan);
    this.settle(runId, this.native.finalizeRunIfTerminal(runId));
  }

  private clearGateTimer(runId: string, node: string): void {
    const key = gateKey(runId, node);
    const timer = this.gateTimers.get(key);
    if (timer) {
      clearTimeout(timer);
      this.gateTimers.delete(key);
    }
  }

  /** Whether a node's condition is satisfied by its predecessors' outcomes. */
  private shouldExecute(runId: string, node: string, plan: RunPlan): boolean {
    const condition = plan.meta.get(node)?.condition ?? "on_success";
    const preds = plan.predecessors.get(node) ?? [];
    if (preds.length === 0 || condition === "always") {
      return true;
    }
    const status = this.nodeStatuses(runId);
    const predStatuses = preds.map((p) => status.get(p) ?? "");
    if (condition === "on_failure") {
      return predStatuses.some((s) => s === "failed");
    }
    return predStatuses.every((s) => SUCCEEDED_STATUS.has(s)); // on_success
  }

  /** Mark a not-taken node skipped, then propagate the skip to its successors. */
  private skipNode(runId: string, node: string, plan: RunPlan): void {
    this.native.skipWorkflowNode(runId, node);
    this.evaluateSuccessors(runId, node, plan);
  }

  /** Enqueue a plain deferred node using its persisted args. */
  private createDeferredJob(runId: string, node: string, plan: RunPlan): void {
    const meta = plan.meta.get(node);
    if (!meta) {
      return;
    }
    const payload = meta.args_template
      ? Buffer.from(meta.args_template, "base64")
      : Buffer.from(this.encodeCall(meta.task_name, []));
    this.native.createDeferredJob(
      runId,
      node,
      payload,
      meta.task_name,
      meta.queue ?? DEFAULT_QUEUE,
      meta.max_retries ?? DEFAULT_MAX_RETRIES,
      meta.timeout_ms ?? DEFAULT_TIMEOUT_MS,
      meta.priority ?? 0,
    );
  }

  /**
   * Run a cacheable node — or, on a cache hit, enqueue the built-in cache task
   * carrying the stored result so the node completes through the normal path
   * (no downstream special-casing). A miss runs the real task.
   */
  private cacheOrEnqueue(runId: string, node: string, plan: RunPlan, meta: StepMeta): void {
    let ttlMs: number | undefined;
    if (meta.cache) {
      try {
        ttlMs = (JSON.parse(meta.cache) as { ttlMs?: number }).ttlMs;
      } catch {
        // Corrupt cache metadata must not stall the run: skip the cache and
        // run the real task so deferred successors still get enqueued.
        this.createDeferredJob(runId, node, plan);
        return;
      }
    }
    const cached = this.cache.get(this.cacheKey(runId, node, plan), ttlMs);
    if (!cached) {
      this.createDeferredJob(runId, node, plan); // miss → run the real task
      return;
    }
    const value = this.serializer.deserialize(cached);
    // CACHE_TASK is internal and has no per-task codecs; keep the bare wire shape.
    const payload = Buffer.from(serializeCall(this.serializer, [value]));
    this.native.createDeferredJob(
      runId,
      node,
      payload,
      CACHE_TASK,
      meta.queue ?? DEFAULT_QUEUE,
      0, // a cache return never retries
      meta.timeout_ms ?? DEFAULT_TIMEOUT_MS,
      meta.priority ?? 0,
    );
  }

  /** Store a node's job result under its content-addressed cache key. */
  private storeCache(runId: string, node: string, jobId: string, plan: RunPlan): void {
    const job = this.native.getJob(jobId);
    if (job?.result) {
      this.cache.set(this.cacheKey(runId, node, plan), Buffer.from(job.result));
    }
  }

  /** Content-addressed key: task + args + each predecessor's result hash (the dirty set). */
  private cacheKey(runId: string, node: string, plan: RunPlan): string {
    const meta = plan.meta.get(node);
    const hash = createHash("sha256");
    hash.update(meta?.task_name ?? "");
    hash.update("\0");
    hash.update(meta?.args_template ?? "");
    for (const pred of [...(plan.predecessors.get(node) ?? [])].sort()) {
      hash.update("\0");
      hash.update(pred);
      hash.update(this.resultHashOf(runId, pred));
    }
    return hash.digest("hex");
  }

  /** SHA-256 of a node's job-result bytes, or `""` when it has no result. */
  private resultHashOf(runId: string, node: string): string {
    const ref = this.native.getWorkflowNodes(runId).find((n) => n.nodeName === node);
    const job = ref?.jobId ? this.native.getJob(ref.jobId) : null;
    return job?.result ? createHash("sha256").update(Buffer.from(job.result)).digest("hex") : "";
  }

  private allPredecessorsTerminal(runId: string, node: string, plan: RunPlan): boolean {
    const preds = plan.predecessors.get(node) ?? [];
    if (preds.length === 0) {
      return true;
    }
    const status = this.nodeStatuses(runId);
    return preds.every((p) => TERMINAL_NODE_STATUS.has(status.get(p) ?? ""));
  }

  /** The combiner-input node that collects a given fan-out, if any. */
  private fanInNodeFor(parent: string, plan: RunPlan): string | null {
    for (const succ of plan.successors.get(parent) ?? []) {
      const meta = plan.meta.get(succ);
      if (meta?.fan_in) {
        const from = JSON.parse(meta.fan_in).from as string | undefined;
        if (!from || from === parent) {
          return succ;
        }
      }
    }
    return null;
  }

  /** Deserialize the array result of a node's job (its sole positional arg shape). */
  private readArrayResult(runId: string, nodeName: string): unknown[] {
    const node = this.native.getWorkflowNodes(runId).find((n) => n.nodeName === nodeName);
    if (!node?.jobId) {
      throw new Error(`fan-out source '${nodeName}' has no job`);
    }
    const job = this.native.getJob(node.jobId);
    if (!job?.result) {
      throw new Error(`fan-out source '${nodeName}' has no result`);
    }
    const value = this.serializer.deserialize(job.result);
    if (!Array.isArray(value)) {
      throw new Error(`fan-out source '${nodeName}' must return an array`);
    }
    return value;
  }

  private nodeStatuses(runId: string): Map<string, string> {
    return new Map(this.native.getWorkflowNodes(runId).map((n) => [n.nodeName, n.status]));
  }

  /** Load + cache the run plan from storage. Returns null if the run is gone. */
  private plan(runId: string): RunPlan | null {
    const cached = this.plans.get(runId);
    if (cached) {
      return cached;
    }
    const raw = this.native.getWorkflowRunPlan(runId);
    if (!raw) {
      return null;
    }
    const graph = JSON.parse(raw.dag) as { edges?: Array<{ from: string; to: string }> };
    const edges = graph.edges ?? [];
    const meta = new Map<string, StepMeta>(
      Object.entries(JSON.parse(raw.stepMetadata) as Record<string, StepMeta>),
    );
    const successors = successorsOf(edges);
    const seeds = [...meta]
      .filter(
        ([, m]) => m.fan_out || m.fan_in || m.condition || m.gate || m.sub_workflow || m.cache,
      )
      .map(([name]) => name);
    const plan: RunPlan = {
      successors,
      predecessors: predecessorsOf(edges),
      deferred: transitiveDeferred(seeds, successors),
      meta,
      hasCompensation: [...meta.values()].some((m) => m.compensate),
    };
    this.plans.set(runId, plan);
    return plan;
  }
}

/** Stable key for a run's gate node in the timer map. */
function gateKey(runId: string, node: string): string {
  return `${runId} ${node}`;
}

/** `parent[3]` → `parent`; a non-child name → null. */
function fanOutParentOf(nodeName: string): string | null {
  return FAN_OUT_CHILD.exec(nodeName)?.[1] ?? null;
}

function isFanOut(plan: RunPlan, node: string): boolean {
  return Boolean(plan.meta.get(node)?.fan_out);
}

function singlePred(plan: RunPlan, node: string): string {
  const preds = plan.predecessors.get(node) ?? [];
  const [only] = preds;
  if (preds.length !== 1 || only === undefined) {
    throw new Error(
      `fan-out '${node}' needs exactly one predecessor or an explicit itemsFrom (has ${preds.length})`,
    );
  }
  return only;
}
