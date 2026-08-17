import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { Queue } from "../src/index";

function newQueue(): Queue {
  const dbPath = join(mkdtempSync(join(tmpdir(), "flexiq-node-")), "queue.db");
  return new Queue({ dbPath });
}

describe("N-API boundary validation", () => {
  it("rejects a zero pool size instead of panicking r2d2", () => {
    const dbPath = join(mkdtempSync(join(tmpdir(), "flexiq-node-")), "queue.db");
    expect(() => new Queue({ dbPath, poolSize: 0 })).toThrow(/poolSize/);
  });

  it("rejects negative enqueue options", () => {
    const queue = newQueue();
    expect(() => queue.enqueue("t", [], { maxRetries: -1 })).toThrow(/maxRetries/);
    expect(() => queue.enqueue("t", [], { timeoutMs: -1 })).toThrow(/timeoutMs/);
    expect(() => queue.enqueue("t", [], { delayMs: -1 })).toThrow(/delayMs/);
  });

  it("rejects negative pagination on listJobs", async () => {
    const queue = newQueue();
    await expect(queue.listJobs({ limit: -1 })).rejects.toThrow(/limit/);
    await expect(queue.listJobs({ offset: -1 })).rejects.toThrow(/offset/);
  });

  it("rejects an unknown status filter rather than returning everything", async () => {
    const queue = newQueue();
    await expect(queue.listJobs({ status: "nope" })).rejects.toThrow(/unknown status/);
  });

  it("rejects negative dead-letter pagination and purge cutoffs", async () => {
    const queue = newQueue();
    await expect(queue.deadLetters(-1)).rejects.toThrow(/limit/);
    await expect(queue.purgeDead(-1)).rejects.toThrow(/olderThanMs/);
    await expect(queue.purgeCompleted(-1)).rejects.toThrow(/olderThanMs/);
  });

  it("fails fast on a malformed task rate limit instead of disabling it", () => {
    const queue = newQueue();
    // Cast past the typed RateLimit template: we're testing the runtime guard
    // for malformed strings that untyped JS callers can still pass.
    queue.task("rated", () => "ok", { rateLimit: "not-a-rate" as `${number}/s` });
    expect(() => queue.runWorker()).toThrow(/rateLimit/);
  });
});
