import { describe, expect, it } from "vitest";
import type { Executor } from "@/lib/api-types";
import { busySlots, EXECUTOR_QUIET_AFTER_MS, isExecutorQuiet, utilization } from "./utils";

function executor(overrides: Partial<Executor> = {}): Executor {
  return {
    executor_id: "worker-1",
    sdk: "python",
    version: "0.21.0",
    tasks: ["send_email"],
    slots: 4,
    free_slots: 4,
    in_flight: 0,
    peer: "tcp:10.0.0.2:53124",
    idle_ms: 0,
    ...overrides,
  };
}

describe("busySlots", () => {
  it("is what the executor advertises minus what it has left", () => {
    expect(busySlots(executor({ slots: 4, free_slots: 1 }))).toBe(3);
    expect(busySlots(executor({ slots: 4, free_slots: 4 }))).toBe(0);
  });

  it("never goes negative on an inconsistent report", () => {
    // A heartbeat racing a dispatch can report more free than total; the UI
    // must not render "-1 / 4".
    expect(busySlots(executor({ slots: 2, free_slots: 5 }))).toBe(0);
  });
});

describe("isExecutorQuiet", () => {
  it("flags an executor that has stopped sending frames", () => {
    expect(isExecutorQuiet(executor({ idle_ms: 0 }))).toBe(false);
    expect(isExecutorQuiet(executor({ idle_ms: EXECUTOR_QUIET_AFTER_MS }))).toBe(false);
    expect(isExecutorQuiet(executor({ idle_ms: EXECUTOR_QUIET_AFTER_MS + 1 }))).toBe(true);
  });
});

describe("utilization", () => {
  it("is the share of advertised slots in use", () => {
    expect(
      utilization([executor({ slots: 4, free_slots: 2 }), executor({ slots: 4, free_slots: 4 })]),
    ).toBeCloseTo(0.25);
  });

  it("reads as idle rather than NaN when nothing is attached", () => {
    expect(utilization([])).toBe(0);
    expect(utilization([executor({ slots: 0, free_slots: 0 })])).toBe(0);
  });
});
