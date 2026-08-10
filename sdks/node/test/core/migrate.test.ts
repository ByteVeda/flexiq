// A deployment that gates DDL opens unmigrated and applies the schema itself.
//
// Until `migrate` has run there are no tables, so every query fails — that is
// the gate working, not a fault.

import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { Queue } from "../../src/index";

let tempDir: string | undefined;

afterEach(() => {
  if (tempDir) {
    // Best effort: the native SQLite handle is released on GC, which JS cannot
    // force, and Windows refuses to unlink a file that still has one open.
    try {
      rmSync(tempDir, { recursive: true, force: true, maxRetries: 5, retryDelay: 50 });
    } catch {
      // the OS reclaims it
    }
    tempDir = undefined;
  }
});

function dbPath(): string {
  tempDir ??= mkdtempSync(join(tmpdir(), "taskito-migrate-"));
  return join(tempDir, "q.db");
}

describe("explicit migrate", () => {
  it("applies the schema a gated open withheld", async () => {
    const queue = new Queue({ dbPath: dbPath(), autoMigrate: false });

    await expect(queue.stats()).rejects.toThrow(/no such table/);

    const report = queue.migrate();
    expect(report.applied.length).toBeGreaterThan(0);
    expect(report.workflowApplied.length).toBeGreaterThan(0);
    expect(report.schemaless).toBe(false);
    await queue.stats();

    const again = queue.migrate();
    expect(again.applied).toEqual([]);
    expect(again.workflowApplied).toEqual([]);
    expect(again.archivedJobs).toBe(0);
  });

  it("leaves only the workflow tables for an auto-migrated queue", () => {
    // Opening applies the core schema; workflow tables are built on first
    // workflow use, so an explicit migrate is what brings them forward.
    const queue = new Queue({ dbPath: dbPath() });

    const report = queue.migrate();
    expect(report.applied).toEqual([]);
    expect(report.workflowApplied.length).toBeGreaterThan(0);

    expect(queue.migrate().workflowApplied).toEqual([]);
  });
});
