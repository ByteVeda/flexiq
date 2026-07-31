import type { Executor } from "@/lib/api-types";

/** An executor is "quiet" once no frame has arrived for this long. */
export const EXECUTOR_QUIET_AFTER_MS = 30_000;

/**
 * Slots currently occupied. Derived rather than reported: `free_slots` is what
 * the executor advertises, and the difference is what an operator reads as
 * load.
 */
export function busySlots(executor: Executor): number {
  return Math.max(0, executor.slots - executor.free_slots);
}

/** Whether an executor has gone quiet — attached, but not heard from. */
export function isExecutorQuiet(executor: Executor): boolean {
  return executor.idle_ms > EXECUTOR_QUIET_AFTER_MS;
}

/** Fraction of advertised capacity in use, 0–1. Zero slots reads as idle. */
export function utilization(executors: Executor[]): number {
  const total = executors.reduce((sum, executor) => sum + executor.slots, 0);
  if (total === 0) return 0;
  const busy = executors.reduce((sum, executor) => sum + busySlots(executor), 0);
  return busy / total;
}
