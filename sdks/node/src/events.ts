import { createLogger } from "./utils";

const log = createLogger("events");

/**
 * The cross-SDK event taxonomy — the single source of truth for what this
 * queue emits and what a webhook can subscribe to. Nearly every name is
 * emitted here; the few reserved contract entries are noted inline.
 */
export const EVENT_NAMES = [
  "job.enqueued",
  "job.completed",
  "job.failed",
  "job.retrying",
  "job.dead",
  "job.cancelled",
  "worker.started",
  "worker.online",
  "worker.stopped",
  "worker.offline",
  "worker.unhealthy",
  "queue.paused",
  "queue.resumed",
  "workflow.submitted",
  "workflow.completed",
  "workflow.completed_with_failures",
  "workflow.failed",
  "workflow.cancelled",
  "workflow.gate_reached",
  "workflow.compensating",
  "workflow.compensated",
  "workflow.compensation_failed",
  "workflow.node_compensating",
  "workflow.node_compensated",
  "workflow.node_compensation_failed",
  "predicate.rejected",
  "predicate.skipped",
  "predicate.deferred",
  // Reserved: a dispatch-time predicate cancelling an already-enqueued job.
  // This SDK gates only at enqueue, where a terminal skip is
  // `predicate.skipped` (no job exists yet), so nothing emits it here.
  "predicate.cancelled",
] as const;

/** A queue event name. */
export type EventName = (typeof EVENT_NAMES)[number];

/** Payload for a job lifecycle event. */
export interface OutcomeEvent {
  jobId: string;
  taskName: string;
  queue?: string;
  error?: string;
  retryCount?: number;
  timedOut?: boolean;
  /** How long the job ran, in ms. Absent when nothing measured the run. */
  durationMs?: number;
}

/** Payload for `job.enqueued`. */
export interface EnqueuedEvent {
  jobId: string;
  taskName: string;
  queue: string;
}

/** Payload for `worker.started` / `worker.online` / `worker.stopped`. */
export interface WorkerEvent {
  workerId: string;
  /** The queues the worker consumes; absent when not configured (all defaults). */
  queues?: string[];
}

/** Payload for `worker.unhealthy` — one per resource turning unhealthy. */
export interface WorkerUnhealthyEvent {
  workerId: string;
  resource: string;
}

/** Payload for `queue.paused` / `queue.resumed`. */
export interface QueueEvent {
  queue: string;
}

/** Payload for workflow run lifecycle events. */
export interface WorkflowEvent {
  runId: string;
  name?: string;
  state?: string;
  error?: string;
  /** Set when the run was submitted as a sub-workflow of another run. */
  parentRunId?: string;
}

/** Payload for `workflow.gate_reached`. */
export interface GateEvent {
  runId: string;
  node: string;
  message?: string;
}

/** Payload for per-node saga compensation events. */
export interface NodeCompensationEvent {
  runId: string;
  node: string;
  error?: string;
}

/** Payload for the `predicate.*` events. */
export interface PredicateEvent {
  taskName: string;
  /** The gate's reason, when it gave one (`predicate.rejected` / `.skipped`). */
  reason?: string;
  /** How long the enqueue was held back (`predicate.deferred` only). */
  delayMs?: number;
}

/** Event name → payload type, for every {@link EventName}. */
export interface EventMap {
  "job.enqueued": EnqueuedEvent;
  "job.completed": OutcomeEvent;
  "job.failed": OutcomeEvent;
  "job.retrying": OutcomeEvent;
  "job.dead": OutcomeEvent;
  "job.cancelled": OutcomeEvent;
  "worker.started": WorkerEvent;
  "worker.online": WorkerEvent;
  "worker.stopped": WorkerEvent;
  "worker.offline": WorkerEvent;
  "worker.unhealthy": WorkerUnhealthyEvent;
  "queue.paused": QueueEvent;
  "queue.resumed": QueueEvent;
  "workflow.submitted": WorkflowEvent;
  "workflow.completed": WorkflowEvent;
  "workflow.completed_with_failures": WorkflowEvent;
  "workflow.failed": WorkflowEvent;
  "workflow.cancelled": WorkflowEvent;
  "workflow.gate_reached": GateEvent;
  "workflow.compensating": WorkflowEvent;
  "workflow.compensated": WorkflowEvent;
  "workflow.compensation_failed": WorkflowEvent;
  "workflow.node_compensating": NodeCompensationEvent;
  "workflow.node_compensated": NodeCompensationEvent;
  "workflow.node_compensation_failed": NodeCompensationEvent;
  "predicate.rejected": PredicateEvent;
  "predicate.skipped": PredicateEvent;
  "predicate.deferred": PredicateEvent;
  "predicate.cancelled": PredicateEvent;
}

/**
 * Union of every event payload. The mapped form doubles as a compile-time
 * exhaustiveness guard: a name added to {@link EVENT_NAMES} without an
 * {@link EventMap} entry makes `EventMap[E]` unindexable and fails typecheck.
 */
export type EventPayload = { [E in EventName]: EventMap[E] }[EventName];

/** Worker outcome kind → the event it emits. */
export const OUTCOME_KIND_EVENTS = {
  success: "job.completed",
  retry: "job.retrying",
  dead: "job.dead",
  cancelled: "job.cancelled",
} as const satisfies Record<string, EventName>;

/** Handler for one event. Defaults to `job.completed` for back-compat. */
export type EventHandler<E extends EventName = "job.completed"> = (event: EventMap[E]) => void;

/**
 * A minimal typed event emitter. Listener errors are isolated — they never
 * break sibling listeners or the worker — but each one is logged at debug so a
 * misbehaving listener isn't invisible.
 */
export class Emitter {
  private readonly handlers = new Map<EventName, Set<(event: EventPayload) => void>>();

  on<E extends EventName>(event: E, handler: (event: EventMap[E]) => void): void {
    const set = this.handlers.get(event) ?? new Set<(event: EventPayload) => void>();
    set.add(handler as (event: EventPayload) => void);
    this.handlers.set(event, set);
  }

  off<E extends EventName>(event: E, handler: (event: EventMap[E]) => void): void {
    this.handlers.get(event)?.delete(handler as (event: EventPayload) => void);
  }

  emit<E extends EventName>(event: E, payload: EventMap[E]): void {
    const set = this.handlers.get(event);
    if (!set) {
      return;
    }
    for (const handler of set) {
      try {
        // Promise.resolve captures async listeners' rejections (else they
        // crash the process as unhandled rejections).
        void Promise.resolve(handler(payload)).catch((error) => {
          // A listener rejecting must not break other listeners or the worker.
          log.debug(() => `listener for "${event}" rejected`, error);
        });
      } catch (error) {
        // A listener throwing must not break other listeners or the worker.
        log.debug(() => `listener for "${event}" threw`, error);
      }
    }
  }
}
