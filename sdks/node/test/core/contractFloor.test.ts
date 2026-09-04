// The storage carries the lowest contract level a process may speak.
//
// A build below that floor must refuse to open rather than join a deployment
// and misread rows its contract never described.

import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { Queue } from "../../src/index";

const CONTRACT_FLOOR_SETTING = "contract:min_sdk";

let tempDir: string | undefined;

afterEach(() => {
  if (tempDir) {
    // Best effort: the queue's native SQLite handle is released on GC, which
    // JS cannot force, and Windows refuses to unlink a file that still has one.
    try {
      rmSync(tempDir, { recursive: true, force: true, maxRetries: 5, retryDelay: 50 });
    } catch {
      // the OS reclaims it
    }
    tempDir = undefined;
  }
});

function openQueue(): { queue: Queue; dbPath: string } {
  tempDir ??= mkdtempSync(join(tmpdir(), "flexiq-contract-"));
  const dbPath = join(tempDir, "q.db");
  return { queue: new Queue({ dbPath }), dbPath };
}

describe("contract floor", () => {
  it("records the level that created the database", () => {
    // Creating the schema is the one moment the floor moves on its own: a
    // database that did not exist has no older process to lock out, and this
    // is what lets durable steps work without an operator finding the dial.
    const { queue } = openQueue();
    expect(queue.getSetting(CONTRACT_FLOOR_SETTING)).not.toBeNull();
    expect(queue.minContract()).toBeGreaterThanOrEqual(1);
  });

  it("leaves an unraised floor unwritten", () => {
    // A database an earlier release created carries no row, and opening never
    // adds one — the dial stays the operator's on an existing deployment.
    const { queue } = openQueue();
    queue.deleteSetting(CONTRACT_FLOOR_SETTING);

    expect(queue.getSetting(CONTRACT_FLOOR_SETTING)).toBeNull();
    expect(queue.minContract()).toBeGreaterThanOrEqual(1);
  });

  it("still opens storage whose floor is exactly this build", () => {
    const { queue, dbPath } = openQueue();
    queue.setMinContract(queue.minContract());

    expect(new Queue({ dbPath }).minContract()).toBe(queue.minContract());
  });

  it("refuses to open storage that requires a newer build", () => {
    const { queue, dbPath } = openQueue();
    const unreachable = queue.minContract() + 1;
    // Written through the raw setting: `setMinContract` rejects a level this
    // build cannot speak, which is what the next test exercises.
    queue.setSetting(CONTRACT_FLOOR_SETTING, String(unreachable));

    expect(() => new Queue({ dbPath })).toThrow(new RegExp(`contract ${unreachable}`));
  });

  it("rejects a floor this build cannot speak", () => {
    const { queue } = openQueue();
    const before = queue.minContract();

    expect(() => queue.setMinContract(before + 1)).toThrow(/lock it out/);
    expect(queue.minContract()).toBe(before);
  });
});
