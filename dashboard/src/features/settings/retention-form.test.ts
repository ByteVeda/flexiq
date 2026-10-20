import { describe, expect, it } from "vitest";
import { daysFromWindows, shortenedTables, windowsFromDays } from "./retention-form";
import type { RetentionWindows } from "./types";

const DAY = 86_400_000;

const windows = (overrides: Partial<RetentionWindows> = {}): RetentionWindows => ({
  task_logs_ttl_ms: 3 * DAY,
  archived_jobs_ttl_ms: 7 * DAY,
  job_errors_ttl_ms: 7 * DAY,
  task_metrics_ttl_ms: 7 * DAY,
  dead_letter_ttl_ms: null,
  ...overrides,
});

describe("daysFromWindows / windowsFromDays", () => {
  it("round-trips whole days and keep-forever", () => {
    const days = daysFromWindows(windows());
    expect(days.task_logs_ttl_ms).toBe("3");
    expect(days.dead_letter_ttl_ms).toBe("");
    expect(windowsFromDays(days)).toEqual({ ok: true, windows: windows() });
  });

  it("keeps a sub-day window as a decimal", () => {
    const days = daysFromWindows(windows({ task_logs_ttl_ms: 12 * 3_600_000 }));
    expect(days.task_logs_ttl_ms).toBe("0.5");
  });

  it("round-trips windows two decimals cannot hold", () => {
    for (const ms of [3_600_000, 5 * 3_600_000, DAY + 1]) {
      const current = windows({ task_logs_ttl_ms: ms });
      expect(windowsFromDays(daysFromWindows(current))).toEqual({ ok: true, windows: current });
    }
    expect(daysFromWindows(windows({ task_logs_ttl_ms: 3_600_000 })).task_logs_ttl_ms).toBe(
      "0.04166667",
    );
  });

  it("refuses what the server refuses", () => {
    const days = daysFromWindows(windows());
    expect(windowsFromDays({ ...days, task_logs_ttl_ms: "0" })).toMatchObject({
      ok: false,
      key: "task_logs_ttl_ms",
    });
    expect(windowsFromDays({ ...days, dead_letter_ttl_ms: "soon" })).toMatchObject({
      ok: false,
      key: "dead_letter_ttl_ms",
    });
  });
});

describe("shortenedTables", () => {
  it("names tables that would lose history sooner", () => {
    const current = windows();
    const next = windows({ archived_jobs_ttl_ms: DAY, dead_letter_ttl_ms: 30 * DAY });
    expect(shortenedTables(current, next)).toEqual(["archived_jobs_ttl_ms", "dead_letter_ttl_ms"]);
  });

  it("ignores lengthening and keep-forever", () => {
    const current = windows();
    const next = windows({ task_logs_ttl_ms: null, archived_jobs_ttl_ms: 30 * DAY });
    expect(shortenedTables(current, next)).toEqual([]);
  });
});
