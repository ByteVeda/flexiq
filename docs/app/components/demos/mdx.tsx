import { DocDemo } from "./doc-demo";

/**
 * The interactive demos as MDX components, one barrel in the shape of
 * `@/components/diagrams` so a page can write the bare tag with no import.
 *
 * The frame itself is {@link DocDemo}, which the landing page also renders —
 * this file is only the name each demo answers to inside MDX.
 */

/** Chunked upload streaming progress back to the caller. */
export function ProgressDemo() {
  return <DocDemo id="progress" />;
}

/** A token bucket pacing dispatch against a provider limit. */
export function RateLimitDemo() {
  return <DocDemo id="ratelimit" />;
}

/** Attempts, backoff and the hand-off to the dead-letter queue. */
export function RecoveryDemo() {
  return <DocDemo id="recovery" />;
}

/** Worker-pool size against throughput and latency. */
export function ScalingDemo() {
  return <DocDemo id="scaling" />;
}

/** A workflow DAG running, with per-node dependencies and status. */
export function WorkflowDemo() {
  return <DocDemo id="workflow" />;
}

/** A multi-step process failing and compensating backwards. */
export function SagaDemo() {
  return <DocDemo id="saga" />;
}

/**
 * Routing keys fanning into the pool built to run them (gpu / default / email).
 *
 * Named for what it shows rather than `MeshDemo`, which is already taken by the
 * work-stealing diagram in `@/components/diagrams` — both land in the same MDX
 * component map, so the identifiers cannot repeat.
 */
export function TaskAffinityDemo() {
  return <DocDemo id="mesh" />;
}

/** One request walked through `flexiq-server`'s producer door, stage by stage. */
export function ServerDoorDemo() {
  return <DocDemo id="serverdoor" />;
}
